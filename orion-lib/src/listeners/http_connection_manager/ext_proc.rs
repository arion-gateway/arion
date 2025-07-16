use crate::body::poly_body::BodySender;
use crate::event_error::EventFailure;
use crate::{
    body::{body_with_metrics::BodyWithMetrics, response_flags::ResponseFlags},
    clusters::clusters_manager::{self, RoutingContext},
    listeners::{http_connection_manager::FilterDecision, synthetic_http_response::SyntheticHttpResponse},
    Error, PolyBody,
};
use bytes::Bytes;
use futures::StreamExt;
use http::{Method, Request, Response};
use http_body_util::{BodyExt, BodyStream, Empty, Full};
use orion_configuration::config::{
    cluster::ClusterSpecifier,
    network_filters::http_connection_manager::http_filters::{
        ext_proc::{
            BodyProcessingMode, ExternalProcessor as ExternalProcessorConfig, GrpcServiceSpecifier,
            HeaderForwardingRules, HeaderMutationRules, HeaderProcessingMode, ProcessingMode, RouteCacheAction,
            TrailerProcessingMode,
        },
        ExtProcPerRoute,
    },
};
use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::common_response::ResponseStatus;
use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::core::v3::{header_value_option::HeaderAppendAction, HeaderMap, HeaderValue},
        extensions::filters::http::ext_proc::v3::ProcessingMode as EnvoyProcessingMode,
        service::ext_proc::v3::{
            body_mutation::Mutation, external_processor_client::ExternalProcessorClient,
            processing_request::Request as ProcessingRequestType,
            processing_response::Response as ProcessingResponseType, BodyResponse, HeaderMutation, HeadersResponse,
            HttpBody, HttpHeaders, HttpTrailers, ImmediateResponse, ProcessingRequest, ProcessingResponse,
            ProtocolConfiguration, TrailersResponse,
        },
    },
    google,
    tonic::codec::Streaming,
};
use orion_format::types::ResponseFlags as FmtResponseFlags;

use std::collections::HashMap;
use std::{sync::Arc, time::Duration};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, warn};

#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExternalProcessor {
    ext_proc_worker: Option<mpsc::Sender<ProcessingTask>>,
    worker_config: Arc<ExternalProcessingWorkerConfig>,
    forward_rules: Option<Arc<HeaderForwardingRules>>,
    sending_request_headers: bool,
    sending_request_body: bool,
    sending_response_headers: bool,
    sending_response_body: bool,
}

impl From<ExternalProcessorConfig> for ExternalProcessor {
    fn from(initial_config: ExternalProcessorConfig) -> Self {
        Self::from((initial_config, None))
    }
}

impl From<(ExternalProcessorConfig, Option<ExtProcPerRoute>)> for ExternalProcessor {
    fn from((initial_config, per_route_config): (ExternalProcessorConfig, Option<ExtProcPerRoute>)) -> Self {
        let forward_rules = initial_config.forward_rules.clone().map(Arc::new);
        let worker_config = ExternalProcessingWorkerConfig::from((initial_config, per_route_config));
        let sending_request_headers =
            !matches!(worker_config.processing_mode.request_header_mode, HeaderProcessingMode::Skip);
        let sending_request_body = !matches!(worker_config.processing_mode.request_body_mode, BodyProcessingMode::None);
        let sending_response_headers =
            !matches!(worker_config.processing_mode.request_header_mode, HeaderProcessingMode::Skip);
        let sending_response_body =
            !matches!(worker_config.processing_mode.request_body_mode, BodyProcessingMode::None);
        Self {
            ext_proc_worker: None,
            worker_config: Arc::new(worker_config),
            forward_rules,
            sending_request_headers,
            sending_request_body,
            sending_response_headers,
            sending_response_body,
        }
    }
}

impl Drop for ExternalProcessor {
    fn drop(&mut self) {
        if let Some(sender) = self.ext_proc_worker.take() {
            drop(sender);
        }
    }
}

impl From<(ExternalProcessorConfig, Option<ExtProcPerRoute>)> for ExternalProcessingWorkerConfig {
    fn from((config, per_route_config): (ExternalProcessorConfig, Option<ExtProcPerRoute>)) -> Self {
        let mut processing_mode = config.processing_mode.clone().unwrap_or(ProcessingMode::default());
        let mut grpc_service = config.grpc_service;
        let mut failure_mode_allow = config.failure_mode_allow;
        if let Some(per_route) = per_route_config {
            if !per_route.disabled {
                if let Some(overrides) = per_route.overrides {
                    if let Some(override_processing_mode) = overrides.processing_mode {
                        processing_mode = override_processing_mode;
                    }
                    if let Some(override_grpc_service) = overrides.grpc_service {
                        grpc_service = override_grpc_service;
                    }
                    if let Some(override_failure_mode_allow) = overrides.failure_mode_allow {
                        failure_mode_allow = override_failure_mode_allow;
                    }
                }
            }
        }
        Self {
            grpc_service_specifier: grpc_service.clone().service_specifier,
            message_timeout: config.message_timeout.unwrap_or(Duration::from_millis(200)),
            max_message_timeout: config.max_message_timeout,
            observability_mode: config.observability_mode,
            failure_mode_allow,
            disable_immediate_response: config.disable_immediate_response,
            mutation_rules: config.mutation_rules.unwrap_or_default(),
            processing_mode,
            allowed_override_modes: config.allowed_override_modes,
            allow_mode_override: config.allow_mode_override,
            route_cache_action: config.route_cache_action,
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
        }
    }
}

impl ExternalProcessor {
    pub async fn apply_request(&mut self, request: &mut Request<BodyWithMetrics<PolyBody>>) -> FilterDecision {
        if !self.sending_request_headers && !self.sending_request_body {
            return FilterDecision::Continue;
        }

        let (response_tx, response_rx) = oneshot::channel();
        let body: PolyBody = std::mem::take(&mut request.body_mut().inner);
        let processing_data = if self.sending_request_headers {
            let http_headers = self.build_http_headers(request.headers(), !self.sending_request_body);
            ProcessingData::Request(http_headers, body)
        } else {
            ProcessingData::RequestBody(body)
        };
        let processing_message =
            ProcessingTask { data: processing_data, reply_channel: response_tx, http_version: request.version() };

        let worker_channel = match self.get_worker_channel().await {
            Ok(channel) => channel,
            Err(e) => {
                return self.on_filter_error(
                    "External processor unavailable for request processing",
                    Some(e),
                    request.version(),
                );
            },
        };
        if worker_channel.send(processing_message).await.is_err() {
            return self.on_filter_error(
                "Failed to schedule sending request data to external processor",
                None,
                request.version(),
            );
        }
        match response_rx.await {
            Ok(ProcessingStatus::HaltedOnError { restore_body }) => {
                if let Some(body) = restore_body {
                    request.body_mut().inner = body;
                }
                self.sending_response_headers = false;
                self.sending_response_body = false;
                FilterDecision::Continue
            },
            Ok(ProcessingStatus::EndWithDirectResponse(direct_response)) => {
                FilterDecision::DirectResponse(direct_response)
            },
            Ok(ProcessingStatus::RequestIsReady {
                header_modifications,
                body_replacement,
                override_sending_response_headers,
                override_sending_response_body,
                clear_route_cache,
            }) => {
                if let Some(header_modifications) = header_modifications {
                    if let Err(e) = apply_header_mutations(
                        request.headers_mut(),
                        &header_modifications,
                        Some(&self.worker_config.mutation_rules),
                    ) {
                        return self.on_filter_error(
                            "Invalid header modifications received from external processor",
                            Some(e),
                            request.version(),
                        );
                    }
                }
                if let Some(body_replacement) = body_replacement {
                    *request.method_mut() = Method::POST;
                    request.body_mut().inner = body_replacement;
                }
                if let Some(override_value) = override_sending_response_headers {
                    self.sending_response_headers = override_value;
                }
                if let Some(override_value) = override_sending_response_body {
                    self.sending_response_body = override_value;
                }
                if clear_route_cache {
                    return FilterDecision::Reroute;
                }
                FilterDecision::Continue
            },
            _ => self.on_filter_error("External processor failed during request processing", None, request.version()),
        }
    }

    pub async fn apply_response(&mut self, response: &mut Response<PolyBody>) -> FilterDecision {
        if !self.sending_response_headers && !self.sending_response_body {
            return FilterDecision::Continue;
        }

        let (response_tx, response_rx) = oneshot::channel();
        let body: PolyBody = std::mem::take(response.body_mut());
        let processing_data = if self.sending_response_headers {
            let http_headers = self.build_http_headers(response.headers(), !self.sending_response_body);
            ProcessingData::Response(http_headers, body)
        } else {
            ProcessingData::ResponseBody(body)
        };
        let processing_message =
            ProcessingTask { data: processing_data, reply_channel: response_tx, http_version: response.version() };

        let worker_channel = match self.get_worker_channel().await {
            Ok(channel) => channel,
            Err(e) => {
                return self.on_filter_error(
                    "External processor unavailable for response processing",
                    Some(e),
                    response.version(),
                );
            },
        };
        if worker_channel.send(processing_message).await.is_err() {
            return self.on_filter_error(
                "Failed to schedule sending response data to external processor",
                None,
                response.version(),
            );
        }
        match response_rx.await {
            Ok(ProcessingStatus::HaltedOnError { restore_body }) => {
                if let Some(body) = restore_body {
                    *response.body_mut() = body;
                }
                FilterDecision::Continue
            },
            Ok(ProcessingStatus::EndWithDirectResponse(direct_response)) => {
                FilterDecision::DirectResponse(direct_response)
            },
            Ok(ProcessingStatus::ResponseIsReady { header_modifications, body_replacement }) => {
                if let Some(header_modifications) = header_modifications {
                    if let Err(e) = apply_header_mutations(
                        response.headers_mut(),
                        &header_modifications,
                        Some(&self.worker_config.mutation_rules),
                    ) {
                        return self.on_filter_error(
                            "Invalid header modifications received from external processor",
                            Some(e),
                            response.version(),
                        );
                    }
                }
                if let Some(body_replacement) = body_replacement {
                    *response.body_mut() = body_replacement;
                }
                FilterDecision::Continue
            },
            _ => self.on_filter_error("External processor failed during response processing", None, response.version()),
        }
    }

    fn on_filter_error(&mut self, msg: &str, error: Option<Error>, http_version: http::Version) -> FilterDecision {
        if let Some(err) = error {
            error!("{msg}: {err}");
        } else {
            error!("{msg}");
        }
        if self.worker_config.failure_mode_allow {
            self.sending_request_body = false;
            self.sending_response_headers = false;
            self.sending_response_body = false;
            FilterDecision::Continue
        } else {
            FilterDecision::DirectResponse(
                SyntheticHttpResponse::internal_error_with_msg(
                    msg,
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE),
                )
                .into_response(http_version),
            )
        }
    }

    async fn get_worker_channel(&mut self) -> Result<&mpsc::Sender<ProcessingTask>, Error> {
        if let Some(ref sender) = self.ext_proc_worker {
            Ok(sender)
        } else {
            let bidi_stream = ExternalProcessingWorker::connect(&self.worker_config.grpc_service_specifier).await?;
            let (sender, receiver) = mpsc::channel::<ProcessingTask>(4);
            let worker = ExternalProcessingWorker::new(self.worker_config.clone(), bidi_stream);
            tokio::spawn(worker.start(receiver));
            Ok(self.ext_proc_worker.insert(sender))
        }
    }

    fn should_forward_header(&self, header_name: &str) -> bool {
        if let Some(forward_rules) = &self.forward_rules {
            if !forward_rules.disallowed_headers.is_empty() {
                return !forward_rules.disallowed_headers.iter().any(|m| m.matches(header_name));
            }
            if !forward_rules.allowed_headers.is_empty() {
                return forward_rules.allowed_headers.iter().any(|m| m.matches(header_name));
            }
        }
        true
    }

    fn build_http_headers(&self, headers: &http::HeaderMap, end_of_stream: bool) -> HttpHeaders {
        let mut header_values = Vec::new();
        for (name, value) in headers {
            let header_name = name.as_str();
            if self.should_forward_header(header_name) {
                let header_value = if let Ok(value_str) = value.to_str() {
                    HeaderValue {
                        key: header_name.to_string(),
                        value: value_str.to_string(),
                        raw_value: Vec::default(),
                    }
                } else {
                    HeaderValue {
                        key: header_name.to_string(),
                        value: String::default(),
                        raw_value: value.as_bytes().to_vec(),
                    }
                };
                header_values.push(header_value);
            }
        }
        let header_map = HeaderMap { headers: header_values };
        HttpHeaders { headers: Some(header_map), attributes: HashMap::default(), end_of_stream }
    }
}

fn apply_header_mutations(
    headers: &mut http::HeaderMap,
    mutation: &HeaderMutation,
    mutation_rules: Option<&HeaderMutationRules>,
) -> Result<(), Error> {
    for header_to_remove in &mutation.remove_headers {
        if let Some(rules) = mutation_rules {
            if !rules.is_modification_permitted(header_to_remove) {
                if rules.disallow_is_error {
                    return Err(Error::from(format!(
                        "Header removal not permitted by configuration: {header_to_remove}"
                    )));
                }
                continue;
            }
        }
        if let Ok(header_name) = http::HeaderName::from_bytes(header_to_remove.as_bytes()) {
            headers.remove(&header_name);
        }
    }
    for header_to_set in &mutation.set_headers {
        let Some(header) = &header_to_set.header else { continue };
        if let Some(rules) = mutation_rules {
            if !rules.is_modification_permitted(&header.key) {
                if rules.disallow_is_error {
                    return Err(Error::from(format!(
                        "Header modification not permitted by configuration: {}",
                        header.key
                    )));
                }
                continue;
            }
        }
        let Ok(header_name) = http::HeaderName::from_bytes(header.key.as_bytes()) else { continue };
        let header_value = if header.raw_value.is_empty() {
            http::HeaderValue::from_str(&header.value)
        } else {
            http::HeaderValue::from_bytes(&header.raw_value)
        };
        let Ok(header_value) = header_value else { continue };
        match header_to_set.append_action() {
            HeaderAppendAction::AppendIfExistsOrAdd => {
                headers.append(header_name, header_value);
            },
            HeaderAppendAction::AddIfAbsent => {
                if !headers.contains_key(&header_name) {
                    headers.append(header_name, header_value);
                }
            },
            HeaderAppendAction::OverwriteIfExistsOrAdd => {
                headers.insert(header_name, header_value);
            },
            HeaderAppendAction::OverwriteIfExists => {
                if headers.contains_key(&header_name) {
                    headers.insert(header_name, header_value);
                }
            },
        }
    }
    Ok(())
}

struct ProcessingTask {
    data: ProcessingData,
    reply_channel: oneshot::Sender<ProcessingStatus>,
    http_version: http::Version,
}

enum ProcessingData {
    Request(HttpHeaders, PolyBody),
    RequestBody(PolyBody),
    Response(HttpHeaders, PolyBody),
    ResponseBody(PolyBody),
}

enum ProcessingStatus {
    RequestIsReady {
        header_modifications: Option<HeaderMutation>,
        body_replacement: Option<PolyBody>,
        override_sending_response_headers: Option<bool>,
        override_sending_response_body: Option<bool>,
        clear_route_cache: bool,
    },
    ResponseIsReady {
        header_modifications: Option<HeaderMutation>,
        body_replacement: Option<PolyBody>,
    },
    HaltedOnError {
        restore_body: Option<PolyBody>,
    },
    EndWithDirectResponse(Response<PolyBody>),
}

struct BidiStream {
    external_sender: mpsc::Sender<ProcessingRequest>,
    inbound_responses: Streaming<ProcessingResponse>,
}

struct TimeoutState {
    active: bool,
    duration: Duration,
    extended: bool,
}

struct ExternalProcessingWorker {
    config: Arc<ExternalProcessingWorkerConfig>,
    stream: BidiStream,
    request_processing: RequestProcessing,
    response_processing: ResponseProcessing,
    handshake: Option<ProtocolConfiguration>,
    timeout_state: TimeoutState,
}

impl ExternalProcessingWorker {
    fn new(config: Arc<ExternalProcessingWorkerConfig>, bidi_stream: BidiStream) -> Self {
        let request_processing = RequestProcessing::from((&*config, bidi_stream.external_sender.clone()));
        let response_processing = ResponseProcessing::from((&*config, bidi_stream.external_sender.clone()));
        let handshake = Some(ProtocolConfiguration {
            request_body_mode: config.processing_mode.request_body_mode as i32,
            response_body_mode: config.processing_mode.response_body_mode as i32,
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
        });
        let message_timeout = config.message_timeout;
        Self {
            config,
            stream: bidi_stream,
            request_processing,
            response_processing,
            handshake,
            timeout_state: TimeoutState { active: false, duration: message_timeout, extended: false },
        }
    }

    async fn connect(grpc_service_specifier: &GrpcServiceSpecifier) -> Result<BidiStream, Error> {
        let (request_sender, request_receiver) = mpsc::channel::<ProcessingRequest>(4);
        let request_stream = ReceiverStream::new(request_receiver);

        let response_stream = match grpc_service_specifier {
            GrpcServiceSpecifier::Cluster(cluster_name) => {
                let cluster_spec = ClusterSpecifier::Cluster(cluster_name.clone());
                let cluster_id = clusters_manager::resolve_cluster(&cluster_spec).ok_or_else(|| {
                    Error::from(format!("Failed to resolve cluster '{cluster_name}' for external processor"))
                })?;
                let grpc_service = clusters_manager::get_grpc_connection(cluster_id, RoutingContext::None)?;
                let mut client = ExternalProcessorClient::new(grpc_service);
                client
                    .process(request_stream)
                    .await
                    .map_err(|e| Error::from(format!("Failed to establish external processor stream: {e}")))?
                    .into_inner()
            },
            GrpcServiceSpecifier::GoogleGrpc(google_grpc) => {
                let mut client =
                    ExternalProcessorClient::connect(google_grpc.target_uri.clone()).await.map_err(|e| {
                        Error::from(format!("Failed to connect to external processor (GoogleGrpc endpoint): {e}"))
                    })?;
                client
                    .process(request_stream)
                    .await
                    .map_err(|e| Error::from(format!("Failed to establish external processor stream: {e}")))?
                    .into_inner()
            },
        };

        Ok(BidiStream { external_sender: request_sender, inbound_responses: response_stream })
    }

    #[allow(clippy::too_many_lines)]
    async fn start(mut self, mut processing_request_channel: mpsc::Receiver<ProcessingTask>) {
        loop {
            tokio::select! {
                processing_directive = processing_request_channel.recv() => {
                    match processing_directive {
                        Some(ProcessingTask{ data: ProcessingData::Request(headers, body), reply_channel, http_version}) => {
                            let expecting_external_response = self.request_processing.process_request(
                                headers,
                                body,
                                reply_channel,
                                http_version,
                                self.handshake.take(),
                            ).await;
                            self.timeout_state.active = expecting_external_response;
                        }
                        Some(ProcessingTask{ data: ProcessingData::RequestBody(body), reply_channel, http_version}) => {
                            let expecting_external_response = self.request_processing.process_body(
                                body,
                                reply_channel,
                                Some(http_version),
                                self.handshake.take(),
                            ).await;
                            self.timeout_state.active = expecting_external_response;
                        }
                        Some(ProcessingTask{ data: ProcessingData::Response(headers, body), reply_channel, http_version}) => {
                            let expecting_external_response = self.response_processing.process_response(
                                headers,
                                body,
                                reply_channel,
                                http_version,
                                self.handshake.take(),
                            ).await;
                            self.timeout_state.active = expecting_external_response;
                        }
                        Some(ProcessingTask{ data: ProcessingData::ResponseBody(body), reply_channel, http_version}) => {
                            let expecting_external_response = self.response_processing.process_body(
                                body,
                                reply_channel,
                                Some(http_version),
                                self.handshake.take(),
                            ).await;
                            self.timeout_state.active = expecting_external_response;
                        }
                        _ => break,
                    }
                },

                external_processing_response = self.stream.inbound_responses.message() => {
                    match external_processing_response {
                        Ok(Some(ProcessingResponse { override_message_timeout: Some(extended_timeout), ..})) => {
                            if !self.handle_timeout_extension(extended_timeout) {
                                break;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ImmediateResponse(response_attempt)), ..})) => {
                            if self.config.disable_immediate_response {
                                let msg = "External processor attempted to send immediate response - which is disabled by config";
                                warn!("{msg}");
                                self.response_processing.exit_on_error(msg, self.config.failure_mode_allow);
                                self.request_processing.exit_on_error(msg, self.config.failure_mode_allow);
                            } else {
                                let response = self.build_direct_response(&response_attempt);
                                let status = ProcessingStatus::EndWithDirectResponse(response);
                                if self.response_processing.is_awaiting_reply() {
                                    self.response_processing.exit_with_status(status);
                                } else {
                                    self.request_processing.exit_with_status(status);
                                }
                            }
                            break;
                        },
                        Ok(Some(ProcessingResponse { mode_override, response: Some(ProcessingResponseType::RequestHeaders(headers_response)), ..})) => {
                            if self.config.allow_mode_override {
                                if let Some(overrides) = mode_override {
                                    self.request_processing.apply_mode_overrides(&overrides, &self.config.allowed_override_modes);
                                    self.response_processing.apply_mode_overrides(&overrides, &self.config.allowed_override_modes);
                                }
                            }
                            let wants_response_headers = self.response_processing.is_header_processing_planned();
                            let wants_response_body = self.response_processing.is_body_processing_planned();
                            let expecting_followup = self.request_processing.handle_headers_response(
                                headers_response,
                                &self.config.route_cache_action,
                                wants_response_headers,
                                wants_response_body
                            ).await;
                            self.timeout_state.active = expecting_followup;
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::RequestBody(body_response)), ..})) => {
                            let expecting_followup = self.request_processing.handle_body_response(body_response, &self.config.route_cache_action).await;
                            self.timeout_state.active = expecting_followup;
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::RequestTrailers(trailers_response)), ..})) => {
                            self.request_processing.handle_trailers_response(trailers_response).await;
                            self.timeout_state.active = self.response_processing.is_awaiting_reply();
                        },
                        Ok(Some(ProcessingResponse { mode_override, response: Some(ProcessingResponseType::ResponseHeaders(headers_response)), ..})) => {
                            if self.config.allow_mode_override {
                                if let Some(overrides) = mode_override {
                                    self.response_processing.apply_mode_overrides(&overrides, &self.config.allowed_override_modes);
                                }
                            }
                            let expecting_followup = self.response_processing.handle_headers_response(headers_response).await;
                            self.timeout_state.active = expecting_followup;
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ResponseBody(body_response)), ..})) => {
                            let expecting_followup = self.response_processing.handle_body_response(body_response).await;
                            self.timeout_state.active = expecting_followup;
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ResponseTrailers(trailers_response)), ..})) => {
                            self.response_processing.handle_trailers_response(trailers_response).await;
                            self.timeout_state.active = false;
                        },
                        Err(e) => {
                            let msg = "External processor gRPC error";
                            error!("{msg}: {e}");
                            if self.response_processing.is_awaiting_reply() {
                                self.response_processing.exit_on_error(msg, self.config.failure_mode_allow);
                            } else {
                                self.request_processing.exit_on_error(msg, self.config.failure_mode_allow);
                            }
                            break;
                        },
                        _ => {
                            break;
                        }
                    }
                },

                request_body_frame = &mut self.request_processing.body_context.body_stream.next(), if self.request_processing.is_accepting_body_data() => {
                    match request_body_frame {
                        Some(Ok(frame)) => {
                            if let Some(data) = frame.data_ref() {
                                if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
                                    let expecting_response = self.request_processing.handle_body_chunk(buffered, false).await;
                                    self.timeout_state.active = expecting_response;
                                }
                                self.request_processing.body_context.buffered_chunk = Some(data.clone());
                            } else if let Some(trailers) = frame.trailers_ref() {
                                if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
                                    let expecting_response = self.request_processing.handle_body_chunk(buffered, true).await;
                                    self.timeout_state.active = expecting_response;
                                }
                                self.request_processing.body_context.trailers = Some(trailers.clone());
                            }
                        },
                        Some(Err(_err)) => {
                            self.request_processing.exit_on_error("Error occured when streaming request body for external processing", self.config.failure_mode_allow);
                        },
                        None => {
                            if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
                                let expecting_response = self.request_processing.handle_body_chunk(buffered, true).await;
                                self.timeout_state.active = expecting_response;
                            }
                            if let Some(trailers) = self.request_processing.body_context.trailers.take() {
                                let expecting_response = self.request_processing.process_trailers(Some(trailers), None, None, None).await;
                                self.timeout_state.active = expecting_response;
                            }
                        }
                    }
                },

                response_body_frame = &mut self.response_processing.body_context.body_stream.next(), if self.response_processing.is_accepting_body_data() => {
                    match response_body_frame {
                        Some(Ok(frame)) => {
                            if let Some(data) = frame.data_ref() {
                                if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
                                    let expecting_response = self.response_processing.handle_body_chunk(buffered, false).await;
                                    self.timeout_state.active = expecting_response;
                                }
                                self.response_processing.body_context.buffered_chunk = Some(data.clone());
                            } else if let Some(trailers) = frame.trailers_ref() {
                                if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
                                    let expecting_response = self.response_processing.handle_body_chunk(buffered, true).await;
                                    self.timeout_state.active = expecting_response;
                                }
                                self.response_processing.body_context.trailers = Some(trailers.clone());
                            }
                        },
                        Some(Err(_err)) => {
                            self.request_processing.exit_on_error("Error occured when streaming response body for external processing", self.config.failure_mode_allow);
                        },
                        None => {
                            if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
                                let _ = self.response_processing.handle_body_chunk(buffered, true).await;
                            }
                            if let Some(trailers) = self.response_processing.body_context.trailers.take() {
                                let _ = self.response_processing.process_trailers(Some(trailers), None, None, None).await;
                            }
                            self.timeout_state.active = false;
                        }
                    }
                },

                () = pingora_timeout::fast_timeout::fast_sleep(self.timeout_state.duration), if self.timeout_state.active => {
                    self.request_processing.exit_on_timeout(self.config.failure_mode_allow);
                    self.response_processing.exit_on_timeout(self.config.failure_mode_allow);
                    break;
                }


            }
        }
    }

    #[allow(clippy::cast_sign_loss)]
    fn handle_timeout_extension(&mut self, extended_timeout: google::protobuf::Duration) -> bool {
        if self.timeout_state.extended {
            let msg = "External processor attempted multiple timeout extensions";
            self.request_processing.exit_on_error(msg, self.config.failure_mode_allow);
            self.response_processing.exit_on_error(msg, self.config.failure_mode_allow);
            return false;
        }
        let mut timeout_duration =
            Duration::from_secs(extended_timeout.seconds as u64) + Duration::from_nanos(extended_timeout.nanos as u64);
        if timeout_duration < Duration::from_millis(1) {
            warn!("External processor: override_message_timeout must be >= 1ms");
            timeout_duration = self.config.message_timeout;
        }
        if let Some(max_timeout) = self.config.max_message_timeout {
            if timeout_duration > max_timeout {
                warn!("External processor: attempted to override message timeout to value > max_message_timeout (defaulting to max_message_timeout)");
                timeout_duration = max_timeout;
            }
        }
        self.timeout_state.duration = timeout_duration;
        self.timeout_state.extended = true;
        true
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn build_direct_response(&mut self, response_attempt: &ImmediateResponse) -> Response<PolyBody> {
        let status = response_attempt
            .status
            .as_ref()
            .and_then(|s| http::StatusCode::from_u16(s.code as u16).ok())
            .unwrap_or(http::StatusCode::OK);
        let body_bytes = Bytes::copy_from_slice(&response_attempt.body);
        let body = Full::new(body_bytes);
        let mut response = Response::new(crate::PolyBody::from(body));
        *response.status_mut() = status;
        if let Some(header_mutation) = &response_attempt.headers {
            let _ = apply_header_mutations(response.headers_mut(), header_mutation, Some(&self.config.mutation_rules));
        }
        if let Some(grpc_status) = &response_attempt.grpc_status {
            if let Ok(status_value) = http::HeaderValue::from_str(&grpc_status.status.to_string()) {
                response.headers_mut().insert("grpc-status", status_value);
            }
        }
        response
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
struct ExternalProcessingWorkerConfig {
    grpc_service_specifier: GrpcServiceSpecifier,
    message_timeout: Duration,
    max_message_timeout: Option<Duration>,
    observability_mode: bool,
    failure_mode_allow: bool,
    disable_immediate_response: bool,
    mutation_rules: HeaderMutationRules,
    processing_mode: ProcessingMode,
    allowed_override_modes: Vec<ProcessingMode>,
    allow_mode_override: bool,
    route_cache_action: RouteCacheAction,
    send_body_without_waiting_for_header_response: bool,
}

#[derive(Debug, Clone, Default)]
enum ProcessingState {
    ObservabilityMode,
    ObservabilityModeStreamingBody,
    WaitingForHeadersInput,
    WaitingForHeadersReply,
    WaitingForBodyInput,
    WaitingForBodyReply,
    StreamingBody,
    StreamingBodyWaitingForReply,
    FullDuplexStreamingBody,
    ProcessingTrailers,
    #[default]
    Idle,
}

struct BodyContext {
    body: Option<PolyBody>,
    body_stream: BodyStream<PolyBody>,
    body_sender: Option<BodySender>,
    body_mode: BodyProcessingMode,
    trailer_mode: TrailerProcessingMode,
    trailers: Option<http::HeaderMap>,
    buffered_chunk: Option<Bytes>,
}

impl BodyContext {
    fn new(body_mode: BodyProcessingMode, trailer_mode: TrailerProcessingMode) -> Self {
        Self {
            body: None,
            body_stream: BodyStream::new(PolyBody::from(Empty::<Bytes>::default())),
            body_sender: None,
            body_mode,
            trailer_mode,
            trailers: None,
            buffered_chunk: None,
        }
    }
    fn start_streaming(&mut self) {
        if let Some(body) = self.body.take() {
            self.body_stream = BodyStream::new(body);
            let (new_body, sender) = PolyBody::channel(8);
            self.body = Some(new_body);
            self.body_sender = Some(BodySender::new(sender));
        }
    }

    async fn make_new_body_channel(&mut self, data: Bytes) -> Result<(), ()> {
        let (new_body, sender) = PolyBody::channel(2);
        self.body = Some(new_body);
        let sender = BodySender::new(sender);
        if (sender.send_data(data).await).is_err() {
            return Err(());
        }
        self.body_sender = Some(sender);
        Ok(())
    }
}

struct RequestProcessing {
    state: ProcessingState,
    body_context: BodyContext,
    partial_reply: Option<ProcessingStatus>,
    reply_channel: Option<oneshot::Sender<ProcessingStatus>>,
    http_version: Option<http::Version>,
    external_sender: mpsc::Sender<ProcessingRequest>,
    send_body_without_waiting_for_header_response: bool,
    failure_mode_allow: bool,
}

impl From<(&ExternalProcessingWorkerConfig, mpsc::Sender<ProcessingRequest>)> for RequestProcessing {
    fn from((config, external_sender): (&ExternalProcessingWorkerConfig, mpsc::Sender<ProcessingRequest>)) -> Self {
        let processing_mode = &config.processing_mode;
        let initial_state = match (processing_mode, config.observability_mode) {
            (_, true) => ProcessingState::ObservabilityMode,
            (
                ProcessingMode {
                    request_header_mode: HeaderProcessingMode::Default | HeaderProcessingMode::Send, ..
                },
                _,
            ) => ProcessingState::WaitingForHeadersInput,
            (
                ProcessingMode {
                    request_body_mode:
                        BodyProcessingMode::Buffered
                        | BodyProcessingMode::BufferedPartial
                        | BodyProcessingMode::Streamed
                        | BodyProcessingMode::FullDuplexStreamed,
                    ..
                }
                | ProcessingMode { request_trailer_mode: TrailerProcessingMode::Send, .. },
                _,
            ) => ProcessingState::WaitingForBodyInput,
            (_, _) => ProcessingState::Idle,
        };
        Self {
            state: initial_state,
            body_context: BodyContext::new(processing_mode.request_body_mode, processing_mode.request_trailer_mode),
            partial_reply: None,
            reply_channel: None,
            http_version: None,
            external_sender,
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
            failure_mode_allow: config.failure_mode_allow,
        }
    }
}

impl RequestProcessing {
    fn apply_mode_overrides(&mut self, envoy_mode: &EnvoyProcessingMode, allowed_override_modes: &[ProcessingMode]) {
        if matches!(self.state, ProcessingState::WaitingForHeadersReply) {
            if let Ok(mode) = BodyProcessingMode::try_from(envoy_mode.request_body_mode) {
                if allowed_override_modes.iter().any(|allowed| allowed.request_body_mode == mode) {
                    self.body_context.body_mode = mode;
                }
            }
            if let Ok(mode) = TrailerProcessingMode::try_from(envoy_mode.request_trailer_mode) {
                if allowed_override_modes.iter().any(|allowed| allowed.request_trailer_mode == mode) {
                    self.body_context.trailer_mode = mode;
                }
            }
        }
    }

    async fn process_request(
        &mut self,
        headers: HttpHeaders,
        body: PolyBody,
        reply_channel: oneshot::Sender<ProcessingStatus>,
        http_version: http::Version,
        handshake: Option<ProtocolConfiguration>,
    ) -> bool {
        self.reply_channel = Some(reply_channel);
        self.http_version = Some(http_version);
        self.body_context.body = Some(body);
        match &self.state {
            ProcessingState::ObservabilityMode
                if matches!(self.body_context.body_mode, BodyProcessingMode::Streamed) =>
            {
                let processing_request = ProcessingRequest {
                    request: Some(ProcessingRequestType::RequestHeaders(headers)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: true,
                    protocol_config: handshake,
                };
                let _ = self.external_sender.send(processing_request).await;
                self.body_context.start_streaming();
                let status = ProcessingStatus::RequestIsReady {
                    header_modifications: None,
                    body_replacement: self.body_context.body.take(),
                    override_sending_response_headers: None,
                    override_sending_response_body: None,
                    clear_route_cache: false,
                };
                self.state = ProcessingState::ObservabilityModeStreamingBody;
                self.exit_with_status(status);
                false
            },
            ProcessingState::ObservabilityMode => {
                let processing_request = ProcessingRequest {
                    request: Some(ProcessingRequestType::RequestHeaders(headers)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: true,
                    protocol_config: handshake,
                };
                let _ = self.external_sender.send(processing_request).await;
                let status = ProcessingStatus::RequestIsReady {
                    header_modifications: None,
                    body_replacement: self.body_context.body.take(),
                    override_sending_response_headers: None,
                    override_sending_response_body: None,
                    clear_route_cache: false,
                };
                self.state = ProcessingState::Idle;
                self.exit_with_status(status);
                false
            },
            ProcessingState::WaitingForHeadersInput
                if (self.send_body_without_waiting_for_header_response
                    && matches!(self.body_context.body_mode, BodyProcessingMode::Streamed)) =>
            {
                let processing_request = ProcessingRequest {
                    request: Some(ProcessingRequestType::RequestHeaders(headers)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: false,
                    protocol_config: handshake,
                };
                if self.external_sender.send(processing_request).await.is_err() {
                    self.exit_on_error("Lost connection to external processor", self.failure_mode_allow);
                    return false;
                }
                self.body_context.start_streaming();
                self.state = ProcessingState::StreamingBody;
                true
            },
            ProcessingState::WaitingForHeadersInput => {
                let processing_request = ProcessingRequest {
                    request: Some(ProcessingRequestType::RequestHeaders(headers)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: false,
                    protocol_config: handshake,
                };
                if self.external_sender.send(processing_request).await.is_err() {
                    self.exit_on_error("Lost connection to external processor", self.failure_mode_allow);
                    return false;
                }
                self.state = ProcessingState::WaitingForHeadersReply;
                true
            },
            _ => false,
        }
    }

    async fn handle_headers_response(
        &mut self,
        response: HeadersResponse,
        route_cache_action: &RouteCacheAction,
        wants_response_headers: bool,
        wants_response_body: bool,
    ) -> bool {
        match &self.state {
            ProcessingState::WaitingForHeadersReply | ProcessingState::StreamingBody => {
                if let Some(response_data) = response.response {
                    let should_clear_route_cache = match route_cache_action {
                        RouteCacheAction::Clear => true,
                        RouteCacheAction::Retain => false,
                        RouteCacheAction::Default => response_data.clear_route_cache,
                    };
                    let embedded_status =
                        ResponseStatus::try_from(response_data.status).unwrap_or(ResponseStatus::Continue);
                    if matches!(embedded_status, ResponseStatus::ContinueAndReplace) {
                        let body_replacement =
                            match response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation) {
                                Some(Mutation::Body(bytes)) => {
                                    Some(PolyBody::from(Full::new(Bytes::copy_from_slice(&bytes))))
                                },
                                Some(Mutation::ClearBody(true)) => Some(PolyBody::from(Empty::<Bytes>::default())),
                                Some(Mutation::ClearBody(false)) | None => self.body_context.body.take(),
                                Some(Mutation::StreamedResponse(_)) => {
                                    self.exit_on_error(
                                    "StreamedResponse mutation not supported in response to header processing request",
                                    self.failure_mode_allow,
                                );
                                    return false;
                                },
                            };
                        let status = ProcessingStatus::RequestIsReady {
                            header_modifications: response_data.header_mutation,
                            body_replacement,
                            override_sending_response_headers: Some(wants_response_headers),
                            override_sending_response_body: Some(wants_response_body),
                            clear_route_cache: should_clear_route_cache,
                        };
                        if let Some(reply_channel) = self.reply_channel.take() {
                            let _ = reply_channel.send(status);
                        }
                        return false;
                    }
                    if let Some(reply_channel) = self.reply_channel.take() {
                        let mut status = ProcessingStatus::RequestIsReady {
                            header_modifications: response_data.header_mutation,
                            body_replacement: None,
                            override_sending_response_headers: Some(wants_response_headers),
                            override_sending_response_body: Some(wants_response_body),
                            clear_route_cache: should_clear_route_cache,
                        };
                        if self.is_body_processing_planned() {
                            self.state = ProcessingState::WaitingForBodyInput;
                            self.partial_reply = Some(status);
                            if let Some(body) = self.body_context.body.take() {
                                return self.process_body(body, reply_channel, None, None).await;
                            }
                        } else {
                            if let ProcessingStatus::RequestIsReady { ref mut body_replacement, .. } = status {
                                *body_replacement = self.body_context.body.take();
                            }
                            let _ = reply_channel.send(status);
                            return false;
                        }
                    }
                }
                false
            },
            _ => false,
        }
    }

    async fn process_body(
        &mut self,
        body: PolyBody,
        reply_channel: oneshot::Sender<ProcessingStatus>,
        http_version: Option<http::Version>,
        handshake: Option<ProtocolConfiguration>,
    ) -> bool {
        self.reply_channel = Some(reply_channel);
        if let Some(http_version) = http_version {
            self.http_version = Some(http_version);
        }
        match &self.state {
            ProcessingState::WaitingForBodyInput => match self.body_context.body_mode {
                BodyProcessingMode::Buffered | BodyProcessingMode::BufferedPartial => {
                    let Ok(collected_body) = body.collect().await else {
                        self.exit_on_error(
                            "Failed to collect request body bytes for external processing",
                            self.failure_mode_allow,
                        );
                        return false;
                    };
                    if matches!(self.body_context.trailer_mode, TrailerProcessingMode::Send) {
                        if let Some(trailers) = collected_body.trailers() {
                            self.body_context.trailers = Some(trailers.clone());
                        }
                    }
                    let body_bytes = collected_body.to_bytes();
                    let http_body = HttpBody { body: body_bytes.to_vec(), end_of_stream: true };
                    let processing_request = ProcessingRequest {
                        request: Some(ProcessingRequestType::RequestBody(http_body)),
                        metadata_context: None,
                        attributes: HashMap::default(),
                        observability_mode: false,
                        protocol_config: handshake,
                    };
                    if self.external_sender.send(processing_request).await.is_err() {
                        self.exit_on_error("Lost connection to external processor", self.failure_mode_allow);
                        return false;
                    }
                    self.state = ProcessingState::WaitingForBodyReply;
                    true
                },
                BodyProcessingMode::Streamed | BodyProcessingMode::FullDuplexStreamed => {
                    self.body_context.body = Some(body);
                    if matches!(self.body_context.body_mode, BodyProcessingMode::Streamed) {
                        self.state = ProcessingState::StreamingBody;
                    } else {
                        self.state = ProcessingState::FullDuplexStreamingBody;
                    }
                    self.body_context.start_streaming();
                    let mut status = self.partial_reply.take().unwrap_or(ProcessingStatus::RequestIsReady {
                        header_modifications: None,
                        body_replacement: None,
                        override_sending_response_headers: None,
                        override_sending_response_body: None,
                        clear_route_cache: false,
                    });
                    if let ProcessingStatus::RequestIsReady { ref mut body_replacement, .. } = status {
                        *body_replacement = self.body_context.body.take();
                    }
                    if let Some(channel) = self.reply_channel.take() {
                        let _ = channel.send(status);
                    }
                    false
                },
                BodyProcessingMode::None if matches!(self.body_context.trailer_mode, TrailerProcessingMode::Send) => {
                    let Ok(collected_body) = body.collect().await else {
                        self.exit_on_error(
                            "Failed to collect request body trailers for external processing",
                            self.failure_mode_allow,
                        );
                        return false;
                    };
                    let trailers = collected_body.trailers().cloned();
                    let body_bytes = collected_body.to_bytes();
                    if (self.body_context.make_new_body_channel(body_bytes).await).is_err() {
                        self.exit_on_error(
                            "Failed to prepare body trailers for external processing",
                            self.failure_mode_allow,
                        );
                        return false;
                    }
                    if let Some(reply_channel) = self.reply_channel.take() {
                        return self
                            .process_trailers(trailers, Some(reply_channel), self.http_version, handshake)
                            .await;
                    }
                    false
                },
                BodyProcessingMode::None => false,
            },
            _ => false,
        }
    }

    async fn handle_body_chunk(&mut self, data: Bytes, end_of_stream: bool) -> bool {
        let http_body = HttpBody { body: data.to_vec(), end_of_stream };
        let processing_request = ProcessingRequest {
            request: Some(ProcessingRequestType::RequestBody(http_body)),
            metadata_context: None,
            attributes: HashMap::default(),
            observability_mode: matches!(self.state, ProcessingState::ObservabilityMode),
            protocol_config: None,
        };
        if self.external_sender.send(processing_request).await.is_err() {
            self.exit_on_error("Lost connection to external processor", self.failure_mode_allow);
            return false;
        }
        match &self.state {
            ProcessingState::ObservabilityMode => {
                if let Some(sender) = &self.body_context.body_sender {
                    let _ = sender.send_data(data).await;
                }
                false
            },
            ProcessingState::StreamingBody => {
                self.state = ProcessingState::StreamingBodyWaitingForReply;
                true
            },
            _ => false,
        }
    }

    async fn handle_body_response(
        &mut self,
        body_response: BodyResponse,
        route_cache_action: &RouteCacheAction,
    ) -> bool {
        match &self.state {
            ProcessingState::WaitingForBodyReply => {
                if let Some(response_data) = body_response.response {
                    let body_replacement = match response_data
                        .body_mutation
                        .and_then(|body_mutation| body_mutation.mutation)
                    {
                        Some(Mutation::Body(bytes)) => Some(PolyBody::from(Full::new(Bytes::copy_from_slice(&bytes)))),
                        Some(Mutation::ClearBody(_)) | None => Some(PolyBody::from(Empty::<Bytes>::default())),
                        Some(Mutation::StreamedResponse(_)) => {
                            self.exit_on_error(
                                "StreamedResponse mutation not supported in response to buffered processing request",
                                self.failure_mode_allow,
                            );
                            return false;
                        },
                    };
                    let mut status = self.partial_reply.take().unwrap_or(ProcessingStatus::RequestIsReady {
                        header_modifications: None,
                        body_replacement: None,
                        override_sending_response_headers: None,
                        override_sending_response_body: None,
                        clear_route_cache: false,
                    });
                    if let ProcessingStatus::RequestIsReady { body_replacement: ref mut body, .. } = status {
                        *body = body_replacement;
                    }
                    if let Some(header_modifications) = response_data.header_mutation {
                        let should_clear_route_cache = match route_cache_action {
                            RouteCacheAction::Clear => true,
                            RouteCacheAction::Retain => false,
                            RouteCacheAction::Default => response_data.clear_route_cache,
                        };
                        if let ProcessingStatus::RequestIsReady {
                            header_modifications: ref mut headers,
                            ref mut clear_route_cache,
                            ..
                        } = status
                        {
                            *headers = Some(header_modifications);
                            *clear_route_cache = should_clear_route_cache;
                        }
                    }
                    let embedded_status =
                        ResponseStatus::try_from(response_data.status).unwrap_or(ResponseStatus::Continue);
                    if let Some(reply_channel) = self.reply_channel.take() {
                        if matches!(embedded_status, ResponseStatus::ContinueAndReplace)
                            || self.body_context.trailers.is_none()
                        {
                            let _ = reply_channel.send(status);
                            return false;
                        }
                        self.partial_reply = Some(status);
                        let trailers = self.body_context.trailers.take();
                        return self.process_trailers(trailers, Some(reply_channel), self.http_version, None).await;
                    }
                }
                false
            },
            ProcessingState::StreamingBodyWaitingForReply | ProcessingState::FullDuplexStreamingBody => {
                if let Some(response_data) = body_response.response {
                    let body_mutation = response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation);
                    if let Some(Mutation::StreamedResponse(streamed_response)) = body_mutation {
                        if let Some(sender) = &self.body_context.body_sender {
                            let _ = sender.send_data(streamed_response.body.into()).await;
                        }
                        if streamed_response.end_of_stream {
                            if matches!(self.body_context.trailer_mode, TrailerProcessingMode::Send) {
                                self.state = ProcessingState::ProcessingTrailers;
                            } else {
                                self.state = ProcessingState::Idle;
                            }
                        } else {
                            self.state = ProcessingState::StreamingBody;
                        }
                    }
                }
                true
            },
            _ => false,
        }
    }

    async fn process_trailers(
        &mut self,
        trailers: Option<http::HeaderMap>,
        reply_channel: Option<oneshot::Sender<ProcessingStatus>>,
        http_version: Option<http::Version>,
        handshake: Option<ProtocolConfiguration>,
    ) -> bool {
        if let Some(reply_channel) = reply_channel {
            self.reply_channel = Some(reply_channel);
        }
        if let Some(http_version) = http_version {
            self.http_version = Some(http_version);
        }
        if let Some(trailers) = trailers {
            let mut header_values = Vec::new();
            for (name, value) in &trailers {
                let header_name = name.as_str();
                let header_value = if let Ok(value_str) = value.to_str() {
                    HeaderValue {
                        key: header_name.to_string(),
                        value: value_str.to_string(),
                        raw_value: Vec::default(),
                    }
                } else {
                    HeaderValue {
                        key: header_name.to_string(),
                        value: String::default(),
                        raw_value: value.as_bytes().to_vec(),
                    }
                };
                header_values.push(header_value);
            }
            let trailers_to_send = HeaderMap { headers: header_values };
            let processing_request = ProcessingRequest {
                request: Some(ProcessingRequestType::RequestTrailers(HttpTrailers {
                    trailers: Some(trailers_to_send),
                })),
                metadata_context: None,
                attributes: HashMap::default(),
                observability_mode: matches!(self.state, ProcessingState::ObservabilityMode),
                protocol_config: handshake,
            };
            if self.external_sender.send(processing_request).await.is_err() {
                self.exit_on_error("Lost connection to external processor", self.failure_mode_allow);
                return false;
            }
            if !matches!(self.state, ProcessingState::ObservabilityMode) {
                self.state = ProcessingState::ProcessingTrailers;
            }
            return true;
        }
        false
    }

    async fn handle_trailers_response(&mut self, trailers_response: TrailersResponse) {
        if let Some(mut trailers) = self.body_context.trailers.take() {
            if !matches!(self.state, ProcessingState::ObservabilityMode) {
                if let Some(trailers_updates) = trailers_response.header_mutation {
                    let _ = apply_header_mutations(&mut trailers, &trailers_updates, None);
                }
            }
            match &self.state {
                ProcessingState::ProcessingTrailers => {
                    if let Some(sender) = &self.body_context.body_sender {
                        let _ = sender.send_trailers(trailers).await;
                    }
                    if let Some(reply_channel) = self.reply_channel.take() {
                        let status = self.partial_reply.take().unwrap_or(ProcessingStatus::RequestIsReady {
                            header_modifications: None,
                            body_replacement: self.body_context.body.take(),
                            override_sending_response_headers: None,
                            override_sending_response_body: None,
                            clear_route_cache: false,
                        });
                        let _ = reply_channel.send(status);
                        self.state = ProcessingState::Idle;
                    }
                },
                ProcessingState::ObservabilityMode => {
                    if let Some(sender) = &self.body_context.body_sender {
                        let _ = sender.send_trailers(trailers).await;
                    }
                    self.state = ProcessingState::Idle;
                },
                _ => {},
            }
        }
    }

    fn is_body_processing_planned(&self) -> bool {
        !matches!(self.body_context.body_mode, BodyProcessingMode::None)
    }

    fn is_accepting_body_data(&self) -> bool {
        matches!(
            self.state,
            ProcessingState::ObservabilityModeStreamingBody
                | ProcessingState::StreamingBody
                | ProcessingState::FullDuplexStreamingBody
        )
    }

    fn exit_on_timeout(&mut self, failure_mode_allow: bool) {
        let status = if failure_mode_allow {
            ProcessingStatus::HaltedOnError { restore_body: self.body_context.body.take() }
        } else {
            let http_version = self.http_version.unwrap_or(http::Version::HTTP_11);
            ProcessingStatus::EndWithDirectResponse(
                SyntheticHttpResponse::gateway_timeout(
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::UPSTREAM_REQUEST_TIMEOUT),
                )
                .into_response(http_version),
            )
        };
        if let Some(channel) = self.reply_channel.take() {
            let _ = channel.send(status);
        } else {
            warn!("Request [async] processing has timed out waiting on reply from external processor, but filter has already continued");
        }
        self.state = ProcessingState::Idle;
    }

    fn exit_on_error(&mut self, msg: &str, failure_mode_allow: bool) {
        let status = if failure_mode_allow {
            ProcessingStatus::HaltedOnError { restore_body: self.body_context.body.take() }
        } else {
            let http_version = self.http_version.unwrap_or(http::Version::HTTP_11);
            ProcessingStatus::EndWithDirectResponse(
                SyntheticHttpResponse::internal_error_with_msg(
                    msg,
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::UPSTREAM_REQUEST_TIMEOUT),
                )
                .into_response(http_version),
            )
        };
        if let Some(channel) = self.reply_channel.take() {
            let _ = channel.send(status);
        }
        self.state = ProcessingState::Idle;
    }

    fn exit_with_status(&mut self, status: ProcessingStatus) {
        if let Some(channel) = self.reply_channel.take() {
            let _ = channel.send(status);
        }
    }
}

struct ResponseProcessing {
    state: ProcessingState,
    body_context: BodyContext,
    partial_reply: Option<ProcessingStatus>,
    reply_channel: Option<oneshot::Sender<ProcessingStatus>>,
    header_mode: HeaderProcessingMode,
    http_version: Option<http::Version>,
    external_sender: mpsc::Sender<ProcessingRequest>,
    failure_mode_allow: bool,
}

impl From<(&ExternalProcessingWorkerConfig, mpsc::Sender<ProcessingRequest>)> for ResponseProcessing {
    fn from((config, external_sender): (&ExternalProcessingWorkerConfig, mpsc::Sender<ProcessingRequest>)) -> Self {
        let processing_mode = &config.processing_mode;
        let initial_state = match (processing_mode, config.observability_mode) {
            (_, true) => ProcessingState::ObservabilityMode,
            (
                ProcessingMode {
                    response_header_mode: HeaderProcessingMode::Default | HeaderProcessingMode::Send, ..
                },
                _,
            ) => ProcessingState::WaitingForHeadersInput,
            (
                ProcessingMode {
                    response_body_mode:
                        BodyProcessingMode::Buffered
                        | BodyProcessingMode::BufferedPartial
                        | BodyProcessingMode::Streamed
                        | BodyProcessingMode::FullDuplexStreamed,
                    ..
                }
                | ProcessingMode { response_trailer_mode: TrailerProcessingMode::Send, .. },
                _,
            ) => ProcessingState::WaitingForBodyInput,
            (_, _) => ProcessingState::Idle,
        };
        Self {
            state: initial_state,
            body_context: BodyContext::new(processing_mode.response_body_mode, processing_mode.response_trailer_mode),
            partial_reply: None,
            reply_channel: None,
            header_mode: processing_mode.response_header_mode,
            http_version: None,
            external_sender,
            failure_mode_allow: config.failure_mode_allow,
        }
    }
}

impl ResponseProcessing {
    fn apply_mode_overrides(&mut self, envoy_mode: &EnvoyProcessingMode, allowed_override_modes: &[ProcessingMode]) {
        let inactive_state = matches!(
            self.state,
            ProcessingState::WaitingForHeadersInput | ProcessingState::WaitingForBodyInput | ProcessingState::Idle
        );
        if inactive_state {
            if let Ok(mode) = HeaderProcessingMode::try_from(envoy_mode.response_header_mode) {
                if mode != HeaderProcessingMode::Default
                    && allowed_override_modes.iter().any(|allowed| allowed.response_header_mode == mode)
                {
                    self.header_mode = mode;
                    if matches!(self.header_mode, HeaderProcessingMode::Default | HeaderProcessingMode::Send) {
                        self.state = ProcessingState::WaitingForHeadersInput;
                    }
                }
            }
        }
        if let Ok(mode) = BodyProcessingMode::try_from(envoy_mode.response_body_mode) {
            if allowed_override_modes.iter().any(|allowed| allowed.response_body_mode == mode) {
                self.body_context.body_mode = mode;
                if !matches!(self.state, ProcessingState::WaitingForHeadersInput)
                    && matches!(
                        self.body_context.body_mode,
                        BodyProcessingMode::Buffered
                            | BodyProcessingMode::BufferedPartial
                            | BodyProcessingMode::Streamed
                            | BodyProcessingMode::FullDuplexStreamed
                    )
                {
                    self.state = ProcessingState::WaitingForBodyInput;
                }
            }
        }
        if let Ok(mode) = TrailerProcessingMode::try_from(envoy_mode.response_trailer_mode) {
            if allowed_override_modes.iter().any(|allowed| allowed.response_trailer_mode == mode) {
                self.body_context.trailer_mode = mode;
            }
        }
    }

    async fn process_response(
        &mut self,
        headers: HttpHeaders,
        body: PolyBody,
        reply_channel: oneshot::Sender<ProcessingStatus>,
        http_version: http::Version,
        handshake: Option<ProtocolConfiguration>,
    ) -> bool {
        self.reply_channel = Some(reply_channel);
        self.http_version = Some(http_version);
        self.body_context.body = Some(body);
        match &self.state {
            ProcessingState::ObservabilityMode
                if matches!(self.body_context.body_mode, BodyProcessingMode::Streamed) =>
            {
                let processing_request = ProcessingRequest {
                    request: Some(ProcessingRequestType::ResponseHeaders(headers)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: true,
                    protocol_config: handshake,
                };
                let _ = self.external_sender.send(processing_request).await;
                self.body_context.start_streaming();
                let status = ProcessingStatus::ResponseIsReady {
                    header_modifications: None,
                    body_replacement: self.body_context.body.take(),
                };
                self.state = ProcessingState::ObservabilityModeStreamingBody;
                self.exit_with_status(status);
                false
            },
            ProcessingState::ObservabilityMode => {
                let processing_request = ProcessingRequest {
                    request: Some(ProcessingRequestType::ResponseHeaders(headers)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: true,
                    protocol_config: handshake,
                };
                let _ = self.external_sender.send(processing_request).await;
                let status = ProcessingStatus::ResponseIsReady {
                    header_modifications: None,
                    body_replacement: self.body_context.body.take(),
                };
                self.state = ProcessingState::Idle;
                self.exit_with_status(status);
                false
            },
            ProcessingState::WaitingForHeadersInput => {
                let processing_request = ProcessingRequest {
                    request: Some(ProcessingRequestType::ResponseHeaders(headers)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: false,
                    protocol_config: handshake,
                };
                if self.external_sender.send(processing_request).await.is_err() {
                    self.exit_on_error("Lost connection to external processor", self.failure_mode_allow);
                    return false;
                }
                self.state = ProcessingState::WaitingForHeadersReply;
                true
            },
            _ => false,
        }
    }

    async fn handle_headers_response(&mut self, response: HeadersResponse) -> bool {
        match &self.state {
            ProcessingState::WaitingForHeadersReply | ProcessingState::StreamingBody => {
                if let Some(response_data) = response.response {
                    let embedded_status =
                        ResponseStatus::try_from(response_data.status).unwrap_or(ResponseStatus::Continue);
                    if matches!(embedded_status, ResponseStatus::ContinueAndReplace) {
                        let body_replacement =
                            match response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation) {
                                Some(Mutation::Body(bytes)) => {
                                    Some(PolyBody::from(Full::new(Bytes::copy_from_slice(&bytes))))
                                },
                                Some(Mutation::ClearBody(true)) => Some(PolyBody::from(Empty::<Bytes>::default())),
                                Some(Mutation::ClearBody(false)) | None => self.body_context.body.take(),
                                Some(Mutation::StreamedResponse(_)) => {
                                    self.exit_on_error(
                                    "StreamedResponse mutation not supported in response to header processing request",
                                    self.failure_mode_allow,
                                );
                                    return false;
                                },
                            };
                        let status = ProcessingStatus::ResponseIsReady {
                            header_modifications: response_data.header_mutation,
                            body_replacement,
                        };
                        if let Some(reply_channel) = self.reply_channel.take() {
                            let _ = reply_channel.send(status);
                        }
                        return false;
                    }
                    let mut status = ProcessingStatus::ResponseIsReady {
                        header_modifications: response_data.header_mutation,
                        body_replacement: None,
                    };
                    if self.is_body_processing_planned() {
                        self.state = ProcessingState::WaitingForBodyInput;
                        self.partial_reply = Some(status);
                        if let Some(body) = self.body_context.body.take() {
                            if let Some(reply_channel) = self.reply_channel.take() {
                                return self.process_body(body, reply_channel, None, None).await;
                            }
                        }
                    } else {
                        if let ProcessingStatus::ResponseIsReady { ref mut body_replacement, .. } = status {
                            *body_replacement = self.body_context.body.take();
                        }
                        if let Some(reply_channel) = self.reply_channel.take() {
                            let _ = reply_channel.send(status);
                            return false;
                        }
                    }
                }
                false
            },
            _ => false,
        }
    }

    async fn process_body(
        &mut self,
        body: PolyBody,
        reply_channel: oneshot::Sender<ProcessingStatus>,
        http_version: Option<http::Version>,
        handshake: Option<ProtocolConfiguration>,
    ) -> bool {
        self.reply_channel = Some(reply_channel);
        if let Some(http_version) = http_version {
            self.http_version = Some(http_version);
        }
        match &self.state {
            ProcessingState::WaitingForBodyInput => match self.body_context.body_mode {
                BodyProcessingMode::Buffered | BodyProcessingMode::BufferedPartial => {
                    let Ok(collected_body) = body.collect().await else {
                        self.exit_on_error(
                            "Failed to collect response body bytes for external processing",
                            self.failure_mode_allow,
                        );
                        return false;
                    };
                    if matches!(self.body_context.trailer_mode, TrailerProcessingMode::Send) {
                        if let Some(trailers) = collected_body.trailers() {
                            self.body_context.trailers = Some(trailers.clone());
                        }
                    }
                    let body_bytes = collected_body.to_bytes();
                    let http_body = HttpBody { body: body_bytes.to_vec(), end_of_stream: true };
                    let processing_request = ProcessingRequest {
                        request: Some(ProcessingRequestType::ResponseBody(http_body)),
                        metadata_context: None,
                        attributes: HashMap::default(),
                        observability_mode: false,
                        protocol_config: handshake,
                    };
                    if self.external_sender.send(processing_request).await.is_err() {
                        self.exit_on_error("Lost connection to external processor", self.failure_mode_allow);
                        return false;
                    }
                    self.state = ProcessingState::WaitingForBodyReply;
                    true
                },
                BodyProcessingMode::Streamed | BodyProcessingMode::FullDuplexStreamed => {
                    self.body_context.body = Some(body);
                    if matches!(self.body_context.body_mode, BodyProcessingMode::Streamed) {
                        self.state = ProcessingState::StreamingBody;
                    } else {
                        self.state = ProcessingState::FullDuplexStreamingBody;
                    }
                    self.body_context.start_streaming();
                    let mut status = self.partial_reply.take().unwrap_or(ProcessingStatus::ResponseIsReady {
                        header_modifications: None,
                        body_replacement: None,
                    });
                    if let ProcessingStatus::ResponseIsReady { ref mut body_replacement, .. } = status {
                        *body_replacement = self.body_context.body.take();
                    }
                    if let Some(channel) = self.reply_channel.take() {
                        let _ = channel.send(status);
                    }
                    false
                },
                BodyProcessingMode::None if matches!(self.body_context.trailer_mode, TrailerProcessingMode::Send) => {
                    let Ok(collected_body) = body.collect().await else {
                        self.exit_on_error(
                            "Failed to collect response body trailers for external processing",
                            self.failure_mode_allow,
                        );
                        return false;
                    };
                    let trailers = collected_body.trailers().cloned();
                    let body_bytes = collected_body.to_bytes();
                    if self.body_context.make_new_body_channel(body_bytes).await.is_err() {
                        self.exit_on_error(
                            "Failed to prepare response body trailers for external processing",
                            self.failure_mode_allow,
                        );
                        return false;
                    }
                    if let Some(reply_channel) = self.reply_channel.take() {
                        return self
                            .process_trailers(trailers, Some(reply_channel), self.http_version, handshake)
                            .await;
                    }
                    false
                },
                BodyProcessingMode::None => false,
            },
            _ => false,
        }
    }

    async fn handle_body_chunk(&mut self, data: Bytes, end_of_stream: bool) -> bool {
        let http_body = HttpBody { body: data.to_vec(), end_of_stream };
        let processing_request = ProcessingRequest {
            request: Some(ProcessingRequestType::ResponseBody(http_body)),
            metadata_context: None,
            attributes: HashMap::default(),
            observability_mode: matches!(self.state, ProcessingState::ObservabilityMode),
            protocol_config: None,
        };
        if self.external_sender.send(processing_request).await.is_err() {
            self.exit_on_error("Lost connection to external processor", self.failure_mode_allow);
            return false;
        }
        match &self.state {
            ProcessingState::ObservabilityMode => {
                if let Some(sender) = &self.body_context.body_sender {
                    let _ = sender.send_data(data).await;
                    if end_of_stream {
                        if let Some(trailers) = self.body_context.trailers.take() {
                            let _ = sender.send_trailers(trailers).await;
                        }
                        self.state = ProcessingState::Idle;
                    }
                }
                false
            },
            ProcessingState::StreamingBody => {
                self.state = ProcessingState::StreamingBodyWaitingForReply;
                true
            },
            _ => false,
        }
    }

    async fn handle_body_response(&mut self, body_response: BodyResponse) -> bool {
        match &self.state {
            ProcessingState::WaitingForBodyReply => {
                if let Some(response_data) = body_response.response {
                    let body_replacement = match response_data
                        .body_mutation
                        .and_then(|body_mutation| body_mutation.mutation)
                    {
                        Some(Mutation::Body(bytes)) => Some(PolyBody::from(Full::new(Bytes::copy_from_slice(&bytes)))),
                        Some(Mutation::ClearBody(_)) | None => Some(PolyBody::from(Empty::<Bytes>::default())),
                        Some(Mutation::StreamedResponse(_)) => {
                            self.exit_on_error(
                                "StreamedResponse mutation not supported in response to buffered processing request",
                                self.failure_mode_allow,
                            );
                            return false;
                        },
                    };
                    let mut status = self.partial_reply.take().unwrap_or(ProcessingStatus::ResponseIsReady {
                        header_modifications: None,
                        body_replacement: None,
                    });
                    if let ProcessingStatus::ResponseIsReady { body_replacement: ref mut body, .. } = status {
                        *body = body_replacement;
                    }
                    if let Some(header_modifications) = response_data.header_mutation {
                        if let ProcessingStatus::ResponseIsReady { header_modifications: ref mut headers, .. } = status
                        {
                            *headers = Some(header_modifications);
                        }
                    }
                    let embedded_status =
                        ResponseStatus::try_from(response_data.status).unwrap_or(ResponseStatus::Continue);
                    if let Some(reply_channel) = self.reply_channel.take() {
                        if matches!(embedded_status, ResponseStatus::ContinueAndReplace)
                            || self.body_context.trailers.is_none()
                        {
                            let _ = reply_channel.send(status);
                            return false;
                        }
                        self.partial_reply = Some(status);
                        let trailers = self.body_context.trailers.take();
                        return self.process_trailers(trailers, Some(reply_channel), self.http_version, None).await;
                    }
                }
                false
            },
            ProcessingState::StreamingBodyWaitingForReply | ProcessingState::FullDuplexStreamingBody => {
                if let Some(response_data) = body_response.response {
                    let body_mutation = response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation);
                    if let Some(Mutation::StreamedResponse(streamed_response)) = body_mutation {
                        if let Some(sender) = &self.body_context.body_sender {
                            let _ = sender.send_data(streamed_response.body.into()).await;
                        }
                        if streamed_response.end_of_stream {
                            if matches!(self.body_context.trailer_mode, TrailerProcessingMode::Send) {
                                self.state = ProcessingState::ProcessingTrailers;
                            } else {
                                self.state = ProcessingState::Idle;
                            }
                        } else {
                            self.state = ProcessingState::StreamingBody;
                        }
                    }
                }
                true
            },
            _ => false,
        }
    }

    async fn process_trailers(
        &mut self,
        trailers: Option<http::HeaderMap>,
        reply_channel: Option<oneshot::Sender<ProcessingStatus>>,
        http_version: Option<http::Version>,
        handshake: Option<ProtocolConfiguration>,
    ) -> bool {
        if let Some(reply_channel) = reply_channel {
            self.reply_channel = Some(reply_channel);
        }
        if let Some(http_version) = http_version {
            self.http_version = Some(http_version);
        }
        if let Some(trailers) = trailers {
            let mut header_values = Vec::new();
            for (name, value) in &trailers {
                let header_name = name.as_str();
                let header_value = if let Ok(value_str) = value.to_str() {
                    HeaderValue {
                        key: header_name.to_string(),
                        value: value_str.to_string(),
                        raw_value: Vec::default(),
                    }
                } else {
                    HeaderValue {
                        key: header_name.to_string(),
                        value: String::default(),
                        raw_value: value.as_bytes().to_vec(),
                    }
                };
                header_values.push(header_value);
            }
            let trailers_to_send = HeaderMap { headers: header_values };
            let processing_request = ProcessingRequest {
                request: Some(ProcessingRequestType::ResponseTrailers(HttpTrailers {
                    trailers: Some(trailers_to_send),
                })),
                metadata_context: None,
                attributes: HashMap::default(),
                observability_mode: matches!(self.state, ProcessingState::ObservabilityMode),
                protocol_config: handshake,
            };
            if self.external_sender.send(processing_request).await.is_err() {
                self.exit_on_error("Lost connection to external processor", self.failure_mode_allow);
                return false;
            }
            if matches!(self.state, ProcessingState::ObservabilityMode) {
                self.state = ProcessingState::Idle;
            } else {
                self.state = ProcessingState::ProcessingTrailers;
            }
            return true;
        }
        false
    }

    async fn handle_trailers_response(&mut self, trailers_response: TrailersResponse) {
        if let Some(mut trailers) = self.body_context.trailers.take() {
            if !matches!(self.state, ProcessingState::ObservabilityMode) {
                if let Some(trailers_updates) = trailers_response.header_mutation {
                    let _ = apply_header_mutations(&mut trailers, &trailers_updates, None);
                }
            }
            match &self.state {
                ProcessingState::ProcessingTrailers => {
                    if let Some(sender) = &self.body_context.body_sender {
                        let _ = sender.send_trailers(trailers).await;
                    }
                    if let Some(reply_channel) = self.reply_channel.take() {
                        let status = self.partial_reply.take().unwrap_or(ProcessingStatus::ResponseIsReady {
                            header_modifications: None,
                            body_replacement: self.body_context.body.take(),
                        });
                        let _ = reply_channel.send(status);
                        self.state = ProcessingState::Idle;
                    }
                },
                &ProcessingState::ObservabilityMode => {
                    if let Some(sender) = &self.body_context.body_sender {
                        let _ = sender.send_trailers(trailers).await;
                    }
                    self.state = ProcessingState::Idle;
                },
                _ => {},
            }
        }
    }

    fn is_awaiting_reply(&self) -> bool {
        self.reply_channel.is_some()
    }

    fn is_header_processing_planned(&self) -> bool {
        !matches!(self.header_mode, HeaderProcessingMode::Skip)
    }

    fn is_body_processing_planned(&self) -> bool {
        !matches!(self.body_context.body_mode, BodyProcessingMode::None)
    }

    fn is_accepting_body_data(&self) -> bool {
        matches!(
            self.state,
            ProcessingState::ObservabilityModeStreamingBody
                | ProcessingState::StreamingBody
                | ProcessingState::FullDuplexStreamingBody
        )
    }

    fn exit_on_timeout(&mut self, failure_mode_allow: bool) {
        let status = if failure_mode_allow {
            ProcessingStatus::HaltedOnError { restore_body: self.body_context.body.take() }
        } else {
            let http_version = self.http_version.unwrap_or(http::Version::HTTP_11);
            ProcessingStatus::EndWithDirectResponse(
                SyntheticHttpResponse::gateway_timeout(
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE),
                )
                .into_response(http_version),
            )
        };
        if let Some(channel) = self.reply_channel.take() {
            let _ = channel.send(status);
        } else {
            warn!("Response processing has timed out waiting on reply from external processor, but filter has already continued");
        }
        self.state = ProcessingState::Idle;
    }

    fn exit_on_error(&mut self, msg: &str, failure_mode_allow: bool) {
        let status = if failure_mode_allow {
            ProcessingStatus::HaltedOnError { restore_body: self.body_context.body.take() }
        } else {
            let http_version = self.http_version.unwrap_or(http::Version::HTTP_11);
            ProcessingStatus::EndWithDirectResponse(
                SyntheticHttpResponse::internal_error_with_msg(
                    msg,
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE),
                )
                .into_response(http_version),
            )
        };
        if let Some(channel) = self.reply_channel.take() {
            let _ = channel.send(status);
        }
        self.state = ProcessingState::Idle;
    }

    fn exit_with_status(&mut self, status: ProcessingStatus) {
        if let Some(channel) = self.reply_channel.take() {
            let _ = channel.send(status);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::body::{body_with_metrics::BodyWithMetrics, response_flags::BodyKind};
    use http::{Method, Request, Version};
    use http_body_util::{BodyExt, Empty, Full};
    use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
        BodyProcessingMode, ExternalProcessor as ExternalProcessorConfig, GoogleGrpc, GrpcService,
        GrpcServiceSpecifier, HeaderProcessingMode, ProcessingMode, RouteCacheAction, TrailerProcessingMode,
    };
    use orion_data_plane_api::envoy_data_plane_api::{
        envoy::{
            config::core::v3::{
                header_value_option::HeaderAppendAction, HeaderValue as EnvoyHeaderValue, HeaderValueOption,
            },
            service::ext_proc::v3::{
                body_mutation::Mutation,
                common_response::ResponseStatus,
                external_processor_server::{ExternalProcessor as ExternalProcessorService, ExternalProcessorServer},
                processing_response::Response as ProcessingResponseType,
                BodyMutation, BodyResponse, CommonResponse, HeaderMutation, HeadersResponse, ProcessingRequest,
                ProcessingResponse, StreamedBodyResponse,
            },
        },
        tonic::{
            async_trait, transport::Server, Request as TonicRequest, Response as TonicResponse, Status, Streaming,
        },
    };
    use std::{
        collections::VecDeque,
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};

    #[derive(Debug, Clone)]
    pub struct MockExternalProcessorState {
        responses: Arc<Mutex<VecDeque<ProcessingResponse>>>,
    }

    impl MockExternalProcessorState {
        pub fn new() -> Self {
            Self { responses: Arc::new(Mutex::new(VecDeque::new())) }
        }
        pub fn add_response(self, response: ProcessingResponse) -> Self {
            self.responses.lock().unwrap().push_back(response);
            self
        }
        pub fn get_next_response(&self) -> Option<ProcessingResponse> {
            self.responses.lock().unwrap().pop_front()
        }
    }

    #[derive(Debug)]
    pub struct MockExternalProcessor {
        state: MockExternalProcessorState,
    }

    impl MockExternalProcessor {
        pub fn new(state: MockExternalProcessorState) -> Self {
            Self { state }
        }
    }

    #[async_trait]
    impl ExternalProcessorService for MockExternalProcessor {
        type ProcessStream = ReceiverStream<Result<ProcessingResponse, Status>>;

        async fn process(
            &self,
            request: TonicRequest<Streaming<ProcessingRequest>>,
        ) -> Result<TonicResponse<Self::ProcessStream>, Status> {
            let mut inbound = request.into_inner();
            let state = self.state.clone();
            let (tx, rx) = tokio::sync::mpsc::channel(4);
            tokio::spawn(async move {
                while let Some(_req) = inbound.message().await.unwrap_or(None) {
                    if let Some(response) = state.get_next_response() {
                        if tx.send(Ok(response)).await.is_err() {
                            break;
                        }
                    }
                }
            });
            let output_stream = ReceiverStream::new(rx);
            Ok(TonicResponse::new(output_stream))
        }
    }

    async fn start_mock_server(state: MockExternalProcessorState) -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let socket_addr = listener.local_addr().unwrap();
        let incoming = TcpListenerStream::new(listener);
        let mock_service = MockExternalProcessor::new(state);
        let server =
            Server::builder().add_service(ExternalProcessorServer::new(mock_service)).serve_with_incoming(incoming);
        tokio::spawn(server);
        socket_addr
    }

    fn create_test_request(headers: Vec<(&str, &str)>, body: &str) -> Request<BodyWithMetrics<PolyBody>> {
        let mut req = Request::builder().method(Method::GET).uri("http://example.com/test").version(Version::HTTP_11);
        for (name, value) in headers {
            req = req.header(name, value);
        }
        let body = if body.is_empty() {
            PolyBody::from(Empty::<bytes::Bytes>::new())
        } else {
            PolyBody::from(Full::new(bytes::Bytes::from(body.to_string())))
        };
        req.body(BodyWithMetrics::new(BodyKind::Request, body, |_, _, _| {})).unwrap()
    }

    fn create_config_for_mock_server(
        server_addr: SocketAddr,
        processing_mode: ProcessingMode,
        observability_mode: bool,
    ) -> ExternalProcessorConfig {
        ExternalProcessorConfig {
            grpc_service: GrpcService {
                service_specifier: GrpcServiceSpecifier::GoogleGrpc(GoogleGrpc {
                    target_uri: format!("http://{server_addr}"),
                }),
                timeout: Some(Duration::from_secs(1)),
            },
            processing_mode: Some(processing_mode),
            observability_mode,
            failure_mode_allow: true,
            disable_immediate_response: false,
            forward_rules: None,
            mutation_rules: None,
            message_timeout: Some(Duration::from_millis(200)),
            max_message_timeout: None,
            allowed_override_modes: vec![],
            allow_mode_override: false,
            route_cache_action: RouteCacheAction::Default,
            send_body_without_waiting_for_header_response: false,
            deferred_close_timeout: None,
        }
    }

    fn create_headers_response(headers: Vec<(&str, &str)>, status: i32) -> ProcessingResponse {
        let header_mutation = if headers.is_empty() {
            None
        } else {
            Some(HeaderMutation {
                set_headers: headers
                    .into_iter()
                    .map(|(key, value)| HeaderValueOption {
                        header: Some(EnvoyHeaderValue {
                            key: key.to_string(),
                            value: value.to_string(),
                            raw_value: vec![],
                        }),
                        #[allow(deprecated)]
                        append: None,
                        append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
                        keep_empty_value: false,
                    })
                    .collect(),
                remove_headers: vec![],
            })
        };
        ProcessingResponse {
            response: Some(ProcessingResponseType::RequestHeaders(HeadersResponse {
                response: Some(CommonResponse {
                    status,
                    header_mutation,
                    body_mutation: None,
                    trailers: None,
                    clear_route_cache: false,
                }),
            })),
            mode_override: None,
            dynamic_metadata: None,
            override_message_timeout: None,
        }
    }

    fn create_body_response(body_data: Option<Vec<u8>>, headers: Vec<(&str, &str)>, status: i32) -> ProcessingResponse {
        let header_mutation = if headers.is_empty() {
            None
        } else {
            Some(HeaderMutation {
                set_headers: headers
                    .into_iter()
                    .map(|(key, value)| HeaderValueOption {
                        header: Some(EnvoyHeaderValue {
                            key: key.to_string(),
                            value: value.to_string(),
                            raw_value: vec![],
                        }),
                        #[allow(deprecated)]
                        append: None,
                        append_action: HeaderAppendAction::OverwriteIfExistsOrAdd as i32,
                        keep_empty_value: false,
                    })
                    .collect(),
                remove_headers: vec![],
            })
        };
        let body_mutation = body_data.map(|data| BodyMutation { mutation: Some(Mutation::Body(data)) });
        ProcessingResponse {
            response: Some(ProcessingResponseType::RequestBody(BodyResponse {
                response: Some(CommonResponse {
                    status,
                    header_mutation,
                    body_mutation,
                    trailers: None,
                    clear_route_cache: false,
                }),
            })),
            mode_override: None,
            dynamic_metadata: None,
            override_message_timeout: None,
        }
    }

    fn create_streamed_body_response(body_data: Vec<u8>, end_of_stream: bool, status: i32) -> ProcessingResponse {
        let body_mutation = Some(BodyMutation {
            mutation: Some(Mutation::StreamedResponse(StreamedBodyResponse { body: body_data, end_of_stream })),
        });
        ProcessingResponse {
            response: Some(ProcessingResponseType::RequestBody(BodyResponse {
                response: Some(CommonResponse {
                    status,
                    header_mutation: None,
                    body_mutation,
                    trailers: None,
                    clear_route_cache: false,
                }),
            })),
            mode_override: None,
            dynamic_metadata: None,
            override_message_timeout: None,
        }
    }

    #[tokio::test]
    async fn test_header_mutation() {
        let mock_state = MockExternalProcessorState::new().add_response(create_headers_response(
            vec![("x-processed", "true"), ("x-custom-header", "custom-value")],
            ResponseStatus::Continue as i32,
        ));
        let server_addr = start_mock_server(mock_state).await;
        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Send,
            request_body_mode: BodyProcessingMode::None,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let config = create_config_for_mock_server(server_addr, processing_mode, false);
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = create_test_request(vec![("content-type", "application/json")], "");
        let result = ext_proc.apply_request(&mut request).await;

        assert!(matches!(result, FilterDecision::Continue));
        assert_eq!(request.headers().get("x-processed").unwrap(), "true");
        assert_eq!(request.headers().get("x-custom-header").unwrap(), "custom-value");
        assert_eq!(request.headers().get("content-type").unwrap(), "application/json");
    }

    #[tokio::test]
    async fn test_body_buffered_continue_and_replace() {
        let new_body = "modified body content";
        let mock_state = MockExternalProcessorState::new()
            .add_response(create_headers_response(vec![], ResponseStatus::Continue as i32))
            .add_response(create_body_response(
                Some(new_body.as_bytes().to_vec()),
                vec![("y-custom-header", "true")],
                ResponseStatus::ContinueAndReplace as i32,
            ));
        let server_addr = start_mock_server(mock_state).await;
        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Send,
            request_body_mode: BodyProcessingMode::Buffered,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let config = create_config_for_mock_server(server_addr, processing_mode, false);
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = create_test_request(vec![], "original body");
        let result = ext_proc.apply_request(&mut request).await;

        assert!(matches!(result, FilterDecision::Continue));
        assert_eq!(request.method(), Method::POST);
        assert_eq!(request.headers().get("y-custom-header").unwrap(), "true");
        let body_bytes = std::mem::take(&mut request.body_mut().inner).collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes, new_body.as_bytes());
    }

    #[tokio::test]
    async fn test_body_streaming_mode() {
        let mock_state = MockExternalProcessorState::new()
            .add_response(create_headers_response(
                vec![("x-stream-processed", "true"), ("y-custom-header", "true")],
                ResponseStatus::Continue as i32,
            ))
            .add_response(create_streamed_body_response(
                "body data from external processor".as_bytes().to_vec(),
                true,
                ResponseStatus::Continue as i32,
            ));
        let server_addr = start_mock_server(mock_state).await;
        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Send,
            request_body_mode: BodyProcessingMode::Streamed,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            request_trailer_mode: TrailerProcessingMode::Send,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let config = create_config_for_mock_server(server_addr, processing_mode, false);
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = create_test_request(vec![], "streaming body data");
        let result = ext_proc.apply_request(&mut request).await;

        assert!(matches!(result, FilterDecision::Continue));
        assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");
        let body_bytes = std::mem::take(&mut request.body_mut().inner).collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes, "body data from external processor".as_bytes());
    }

    #[tokio::test]
    async fn test_observability_mode() {
        let mock_state = MockExternalProcessorState::new();
        let server_addr = start_mock_server(mock_state).await;
        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Send,
            request_body_mode: BodyProcessingMode::None,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let config = create_config_for_mock_server(server_addr, processing_mode, true);
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = create_test_request(vec![("original-header", "original-value")], "");
        let original_headers = request.headers().clone();
        let result = ext_proc.apply_request(&mut request).await;

        assert!(matches!(result, FilterDecision::Continue));
        assert_eq!(request.headers(), &original_headers);
        assert!(request.headers().get("x-should-not-apply").is_none());
    }
}
