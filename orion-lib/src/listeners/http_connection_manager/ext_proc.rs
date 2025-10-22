mod common_state;
mod mutation;
mod request_proc;
mod response_proc;
mod worker_config;

use crate::event_error::EventFailure;
use crate::listeners::http_connection_manager::ext_proc::common_state::ExtProcStatus;
use crate::listeners::http_connection_manager::ext_proc::common_state::State;
use crate::listeners::http_connection_manager::ext_proc::mutation::apply_header_mutations;
use crate::listeners::http_connection_manager::ext_proc::request_proc::RequestProcessing;
use crate::listeners::http_connection_manager::ext_proc::response_proc::ResponseProcessing;
use crate::listeners::http_connection_manager::ext_proc::worker_config::ExternalProcessingWorkerConfig;
use crate::{
    body::{body_with_metrics::BodyWithMetrics, response_flags::ResponseFlags},
    clusters::clusters_manager::{self, RoutingContext},
    listeners::{http_connection_manager::FilterDecision, synthetic_http_response::SyntheticHttpResponse},
    Error, PolyBody,
};
use bytes::Bytes;
use futures::{future::Either, StreamExt};
use http::header::CONTENT_LENGTH;
use http::{Request, Response};
use http_body::Body;
use http_body_util::Full;
use orion_configuration::config::{
    cluster::ClusterSpecifier,
    network_filters::http_connection_manager::http_filters::{
        ext_proc::{
            BodyProcessingMode, ExternalProcessor as ExternalProcessorConfig, GrpcServiceSpecifier,
            HeaderForwardingRules, HeaderProcessingMode, ProcessingMode,
        },
        ExtProcPerRoute,
    },
};
use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::core::v3::{HeaderMap, HeaderValue},
        service::ext_proc::v3::{
            external_processor_client::ExternalProcessorClient,
            processing_response::Response as ProcessingResponseType, HttpHeaders, ImmediateResponse, ProcessingRequest,
            ProcessingResponse, ProtocolConfiguration,
        },
    },
    google,
    tonic::{codec::Streaming, Status},
};
use orion_format::types::ResponseFlags as FmtResponseFlags;
use pingora_timeout::fast_timeout;
use std::collections::HashMap;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, warn};

use crate::listeners::http_connection_manager::ext_proc::common_state::{ObservabilityState, ProcessingState};

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
            !matches!(worker_config.processing_mode.response_header_mode, HeaderProcessingMode::Skip);
        let sending_response_body =
            !matches!(worker_config.processing_mode.response_body_mode, BodyProcessingMode::None);
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
            debug!(target: "ext_proc", "Dropping ExternalProcessor, close sender");
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
        let body: PolyBody = std::mem::take(&mut request.body_mut().inner);

        let processing_data = if self.sending_request_headers {
            let http_headers =
                self.build_http_headers(request.headers(), !self.sending_request_body || body.is_end_stream());
            ProcessingData::Request(http_headers, body)
        } else {
            if body.is_end_stream() {
                return FilterDecision::Continue;
            }
            ProcessingData::RequestBody(body)
        };

        let ver = request.version();
        let Ok(response_rx) = self.send_processing_data(processing_data, ver).await else {
            return self.on_filter_error("Failed to schedule sending request data to external processor", None, ver);
        };

        match response_rx.await {
            Ok(ExtProcStatus::HaltedOnError { restore_body }) => {
                if let Some(body) = restore_body {
                    request.body_mut().inner = body;
                }
                self.sending_response_headers = false;
                self.sending_response_body = false;
                FilterDecision::Continue
            },
            Ok(ExtProcStatus::EndWithDirectResponse(direct_response)) => {
                FilterDecision::DirectResponse(direct_response)
            },
            Ok(ExtProcStatus::RequestIsReady {
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
                    request.body_mut().inner = body_replacement;
                    request.headers_mut().remove(CONTENT_LENGTH);
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
            Ok(ExtProcStatus::ResponseIsReady { header_modifications: _, body_replacement: _ }) => self
                .on_filter_error(
                    "Unexpected response from external processor during request processing",
                    None,
                    request.version(),
                ),
            Err(e) => self.on_filter_error(
                "External processor failed during request processing",
                Some(e.into()),
                request.version(),
            ),
        }
    }

    pub async fn apply_response(&mut self, response: &mut Response<PolyBody>) -> FilterDecision {
        if !self.sending_response_headers && !self.sending_response_body {
            return FilterDecision::Continue;
        }
        let body: PolyBody = std::mem::take(response.body_mut());
        let processing_data = if self.sending_response_headers {
            let http_headers =
                self.build_http_headers(response.headers(), !self.sending_response_body || body.is_end_stream());
            ProcessingData::Response(http_headers, body)
        } else {
            if body.is_end_stream() {
                return FilterDecision::Continue;
            }
            ProcessingData::ResponseBody(body)
        };

        let ver = response.version();
        let Ok(response_rx) = self.send_processing_data(processing_data, ver).await else {
            return self.on_filter_error("Failed to schedule sending request data to external processor", None, ver);
        };

        match response_rx.await {
            Ok(ExtProcStatus::HaltedOnError { restore_body }) => {
                if let Some(body) = restore_body {
                    *response.body_mut() = body;
                }
                FilterDecision::Continue
            },
            Ok(ExtProcStatus::EndWithDirectResponse(direct_response)) => {
                FilterDecision::DirectResponse(direct_response)
            },
            Ok(ExtProcStatus::ResponseIsReady { header_modifications, body_replacement }) => {
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
                    response.headers_mut().remove(CONTENT_LENGTH);
                }
                FilterDecision::Continue
            },
            Ok(ExtProcStatus::RequestIsReady {
                header_modifications: _,
                body_replacement: _,
                override_sending_response_headers: _,
                override_sending_response_body: _,
                clear_route_cache: _,
            }) => self.on_filter_error(
                "Unexpected request from external processor during response processing",
                None,
                response.version(),
            ),
            Err(e) => self.on_filter_error(
                "External processor failed during response processing",
                Some(e.into()),
                response.version(),
            ),
        }
    }

    async fn send_processing_data(
        &mut self,
        data: ProcessingData,
        ver: http::Version,
    ) -> Result<oneshot::Receiver<ExtProcStatus>, SendError<ProcessingTask>> {
        let (response_tx, response_rx) = oneshot::channel();
        let processing_message = ProcessingTask { data, reply_channel: response_tx, http_version: ver };

        let worker_channel = self.get_worker_channel();
        worker_channel.send(processing_message).await.map(|_| response_rx)
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

    fn get_worker_channel(&mut self) -> &mpsc::Sender<ProcessingTask> {
        if let Some(ref sender) = self.ext_proc_worker {
            sender
        } else {
            let (sender, receiver) = mpsc::channel::<ProcessingTask>(12);
            if self.worker_config.observability_mode {
                let worker = ExternalProcessingWorker::<ObservabilityState>::new(Arc::clone(&self.worker_config));
                tokio::spawn(worker.ext_proc_loop(receiver));
            } else {
                let worker = ExternalProcessingWorker::<ProcessingState>::new(Arc::clone(&self.worker_config));
                tokio::spawn(worker.ext_proc_loop(receiver));
            }
            self.ext_proc_worker.insert(sender)
        }
    }

    #[inline]
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
        let mut header_values = Vec::with_capacity(headers.len());
        for (name, value) in headers {
            let header_name = name.as_str();
            if self.should_forward_header(header_name) {
                let header_value = if let Ok(value_str) = value.to_str() {
                    HeaderValue { key: header_name.to_owned(), value: value_str.to_owned(), raw_value: Vec::default() }
                } else {
                    HeaderValue {
                        key: header_name.to_owned(),
                        value: String::default(),
                        raw_value: value.as_bytes().into(),
                    }
                };
                header_values.push(header_value);
            }
        }

        HttpHeaders {
            headers: Some(HeaderMap { headers: header_values }),
            attributes: HashMap::default(),
            end_of_stream,
        }
    }
}

#[derive(Debug)]
struct ProcessingTask {
    data: ProcessingData,
    reply_channel: oneshot::Sender<ExtProcStatus>,
    http_version: http::Version,
}

#[derive(Debug)]
enum ProcessingData {
    Request(HttpHeaders, PolyBody),
    RequestBody(PolyBody),
    Response(HttpHeaders, PolyBody),
    ResponseBody(PolyBody),
}

struct BidiStream {
    external_sender: mpsc::Sender<ProcessingRequest>,
    inbound_responses: Streaming<ProcessingResponse>,
}

struct TimeoutState {
    duration: Duration,
    active: bool,
    extended: bool,
}

struct ExternalProcessingWorker<S: State> {
    config: Arc<ExternalProcessingWorkerConfig>,
    stream: Option<BidiStream>,
    request_processing: RequestProcessing<S>,
    response_processing: ResponseProcessing<S>,
    handshake: Option<ProtocolConfiguration>,
    timeout_state: TimeoutState,
}

impl ExternalProcessingWorker<ProcessingState> {
    fn new(config: Arc<ExternalProcessingWorkerConfig>) -> Self {
        let request_processing = RequestProcessing::<ProcessingState>::from(&*config);
        let response_processing = ResponseProcessing::<ProcessingState>::from(&*config);
        let handshake = Some(ProtocolConfiguration {
            request_body_mode: config.processing_mode.request_body_mode as i32,
            response_body_mode: config.processing_mode.response_body_mode as i32,
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
        });
        let message_timeout = config.message_timeout;
        Self {
            config,
            stream: None,
            request_processing,
            response_processing,
            handshake,
            timeout_state: TimeoutState { active: false, duration: message_timeout, extended: false },
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn ext_proc_loop(mut self, mut processing_request_channel: mpsc::Receiver<ProcessingTask>) {
        // The following label is not strictly necessary, but it makes it clearer what is being exited at the break point.
        // It also makes it easier to locate subsequent exit points.
        debug!(target: "ext_proc", "--- BEGIN ---");
        'transaction_loop: loop {
            tokio::select! {
                outbond_processing_task = processing_request_channel.recv() => {
                    match outbond_processing_task {
                        Some(ProcessingTask{ data: ProcessingData::Request(headers, body), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "Processing new request ->");
                            let outbound = self
                                .request_processing
                                .process_request(headers, body, reply_channel, http_version);
                            self.forward_to_external_processor(outbound).await;
                        }
                        Some(ProcessingTask{ data: ProcessingData::RequestBody(body), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "Processing new request (body only) ->");
                            let outbound = self
                                .request_processing
                                .process_body(body, reply_channel, Some(http_version)).await;
                            self.forward_to_external_processor(outbound).await;
                        }
                        Some(ProcessingTask{ data: ProcessingData::Response(headers, body), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "Processing new response ->");
                            let outbound = self
                                .response_processing
                                .process_response(headers, body, reply_channel, http_version);
                            self.forward_to_external_processor(outbound).await;
                        }
                        Some(ProcessingTask{ data: ProcessingData::ResponseBody(body), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "Processing new response (body only) ->");
                            let outbound = self
                                .response_processing
                                .process_body(body, reply_channel, Some(http_version)).await;
                            self.forward_to_external_processor(outbound).await;
                        }
                        _ => {
                            debug!(target: "ext_proc", "Channel received closed!");
                            break 'transaction_loop
                        },
                    }
                },

                indbound_processing_response = if let Some(stream) = self.stream.as_mut() {
                        Either::Left(stream.inbound_responses.message())
                    } else {
                        Either::Right(std::future::pending::<Result<Option<ProcessingResponse>, Status>>())
                    } => {
                    match indbound_processing_response {
                        Ok(Some(ProcessingResponse { override_message_timeout: Some(extended_timeout), ..})) => {
                            debug!(target: "ext_proc", "<- External processor requested timeout extension: {extended_timeout:?}");
                            if !self.handle_timeout_extension(extended_timeout) {
                                break 'transaction_loop;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ImmediateResponse(response_attempt)), ..})) => {
                            debug!(target: "ext_proc", "<- External processor sent immediate response");
                            if self.config.disable_immediate_response {
                                let msg = "External processor attempted to send immediate response - which is disabled by config";
                                warn!("{msg}");
                                self.response_processing.exit_on_error(msg, self.config.failure_mode_allow);
                                self.request_processing.exit_on_error(msg, self.config.failure_mode_allow);
                            } else {
                                let response = self.build_direct_response(&response_attempt);
                                let status = ExtProcStatus::EndWithDirectResponse(response);
                                if self.response_processing.is_awaiting_reply() {
                                    self.response_processing.exit_with_status(status);
                                } else {
                                    self.request_processing.exit_with_status(status);
                                }
                            }
                            break 'transaction_loop;
                        },
                        Ok(Some(ProcessingResponse { mode_override, response: Some(ProcessingResponseType::RequestHeaders(headers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- External processor sent request headers response");
                            if self.config.allow_mode_override {
                                if let Some(overrides) = mode_override {
                                    self.request_processing.apply_mode_overrides(&overrides, &self.config.allowed_override_modes);
                                    self.response_processing.apply_mode_overrides(&overrides, &self.config.allowed_override_modes);
                                }
                            }
                            let wants_response_headers = self.response_processing.is_header_processing_planned();
                            let wants_response_body = self.response_processing.is_body_processing_planned();
                            let outbound = self.request_processing.handle_headers_response(
                                headers_response,
                                &self.config.route_cache_action,
                                wants_response_headers,
                                wants_response_body
                            ).await;
                            self.forward_to_external_processor(outbound).await;
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::RequestBody(body_response)), ..})) => {
                            debug!(target: "ext_proc", "<- External processor sent request body response");
                            let empty_response = body_response.response.is_none();
                            let outbound = self
                                .request_processing
                                .handle_body_response(body_response, &self.config.route_cache_action).await;
                            self.forward_to_external_processor(outbound).await;
                            if empty_response {
                                break 'transaction_loop;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::RequestTrailers(trailers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- External processor sent request trailers response");
                            let outbound = self.request_processing.handle_trailers_response(trailers_response).await;
                            self.forward_to_external_processor(outbound).await;
                        },
                        Ok(Some(ProcessingResponse { mode_override, response: Some(ProcessingResponseType::ResponseHeaders(headers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- External processor sent response headers response");
                            if self.config.allow_mode_override {
                                if let Some(overrides) = mode_override {
                                    self.response_processing.apply_mode_overrides(&overrides, &self.config.allowed_override_modes);
                                }
                            }
                            let outbound = self.response_processing.handle_headers_response(headers_response).await;
                            self.forward_to_external_processor(outbound).await;
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ResponseBody(body_response)), ..})) => {
                            debug!(target: "ext_proc", "<- External processor sent response body response");
                            let empty_response = body_response.response.is_none();
                            let outbound = self.response_processing.handle_body_response(body_response).await;
                            self.forward_to_external_processor(outbound).await;
                            if empty_response {
                                break 'transaction_loop;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ResponseTrailers(trailers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- External processor sent response trailers response");
                            let outbound =
                                self.response_processing.handle_trailers_response(trailers_response).await;
                            self.forward_to_external_processor(outbound).await;
                        },
                        Ok(Some(r)) => {
                            debug!(target: "ext_proc", "<- External processor sent noop response {r:?}");
                            self.response_processing.handle_noop_response();
                            let wants_response_headers = self.response_processing.is_header_processing_planned();
                            let wants_response_body = self.response_processing.is_body_processing_planned();
                            self.request_processing.handle_noop_response(wants_response_headers, wants_response_body);
                        }
                        Err(e) => {
                            let msg = "External processor gRPC error";
                            error!("{msg}: {e}");
                            if self.response_processing.is_awaiting_reply() {
                                self.response_processing.exit_on_error(msg, self.config.failure_mode_allow);
                            } else {
                                self.request_processing.exit_on_error(msg, self.config.failure_mode_allow);
                            }
                            break 'transaction_loop;
                        },
                        _ => {
                            debug!(target: "ext_proc", "<- External processor closed the stream");
                            break 'transaction_loop;
                        }
                    }
                },

                request_body_frame = &mut self.request_processing.body_context.outbound_body_stream.next(), if self.request_processing.is_accepting_body_data() => {
                    match request_body_frame {
                        Some(Ok(frame)) => {
                            debug!(target: "ext_proc", "Received request body frame ->");
                            if let Some(data) = frame.data_ref() {
                                if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
                                    let outbound =
                                        self.request_processing.handle_body_chunk(buffered, false).await;
                                    self.forward_to_external_processor(outbound).await;
                                }
                                self.request_processing.body_context.buffered_chunk = Some(data.clone());
                            } else if let Some(trailers) = frame.trailers_ref() {
                                if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
                                    let outbound =
                                        self.request_processing.handle_body_chunk(buffered, true).await;
                                    self.forward_to_external_processor(outbound).await;
                                }
                                self.request_processing.body_context.trailers = Some(trailers.clone());
                            }
                        },
                        Some(Err(_err)) => {
                            self.request_processing.exit_on_error("Error occured when streaming request body for external processing", self.config.failure_mode_allow);
                        },
                        None => {
                            debug!(target: "ext_proc", "Request body stream ended ->");
                            if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
                                let outbound =
                                    self.request_processing.handle_body_chunk(buffered, true).await;
                                self.forward_to_external_processor(outbound).await;
                            }

                            if let Some(trailers) = self.request_processing.body_context.trailers.take() {
                                let outbound =
                                    self.request_processing.process_trailers(Some(trailers), None, None);
                                self.forward_to_external_processor(outbound).await;
                            }
                        }
                    }
                },

                response_body_frame = &mut self.response_processing.body_context.outbound_body_stream.next(), if self.response_processing.is_accepting_body_data() => {
                    match response_body_frame {
                        Some(Ok(frame)) => {
                            debug!(target: "ext_proc", "Received response body frame ->");
                            if let Some(data) = frame.data_ref() {
                                if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
                                    let outbound =
                                        self.response_processing.handle_body_chunk(buffered, false).await;
                                    self.forward_to_external_processor(outbound).await;
                                }
                                self.response_processing.body_context.buffered_chunk = Some(data.clone());
                            } else if let Some(trailers) = frame.trailers_ref() {
                                if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
                                    let outbound =
                                        self.response_processing.handle_body_chunk(buffered, true).await;
                                    self.forward_to_external_processor(outbound).await;
                                }
                                self.response_processing.body_context.trailers = Some(trailers.clone());
                            }
                        },
                        Some(Err(_err)) => {
                            self.response_processing.exit_on_error("Error occured when streaming response body for external processing", self.config.failure_mode_allow);
                        },
                        None => {
                            debug!(target: "ext_proc", "Response body stream ended ->");
                            if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
                                let outbound = self.response_processing.handle_body_chunk(buffered, true).await;
                                self.forward_to_external_processor(outbound).await;
                            }

                            if let Some(trailers) = self.response_processing.body_context.trailers.take() {
                                let outbound =
                                    self.response_processing.process_trailers(Some(trailers), None, None);
                                self.forward_to_external_processor(outbound).await;
                            }
                        }
                    }
                },

                () = fast_timeout::fast_sleep(self.timeout_state.duration), if self.timeout_state.active => {
                    debug!(target: "ext_proc", "transaction timeout!");
                    self.request_processing.exit_on_timeout(self.config.failure_mode_allow);
                    self.response_processing.exit_on_timeout(self.config.failure_mode_allow);
                    break 'transaction_loop;
                }
            }
        }

        debug!(target: "ext_proc", "--- END ---");
    }
}

impl ExternalProcessingWorker<ObservabilityState> {
    fn new(config: Arc<ExternalProcessingWorkerConfig>) -> Self {
        let request_processing = RequestProcessing::<ObservabilityState>::from(&*config);
        let response_processing = ResponseProcessing::<ObservabilityState>::from(&*config);

        let handshake = Some(ProtocolConfiguration {
            request_body_mode: config.processing_mode.request_body_mode as i32,
            response_body_mode: config.processing_mode.response_body_mode as i32,
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
        });
        let message_timeout = config.message_timeout;
        Self {
            config,
            stream: None,
            request_processing,
            response_processing,
            handshake,
            timeout_state: TimeoutState { active: false, duration: message_timeout, extended: false },
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn ext_proc_loop(mut self, mut processing_request_channel: mpsc::Receiver<ProcessingTask>) {
        // The following label is not strictly necessary, but it makes it clearer what is being exited at the break point.
        // It also makes it easier to locate subsequent exit points.
        debug!(target: "ext_proc", "--- BEGIN ---");
        'transaction_loop: loop {
            tokio::select! {
                outbond_processing_task = processing_request_channel.recv() => {
                    match outbond_processing_task {
                        Some(ProcessingTask{ data: ProcessingData::Request(headers, body), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "Processing new request ->");
                            let outbound = self
                                .request_processing
                                .process_request(headers, body, reply_channel, http_version);
                            self.forward_to_external_processor(outbound).await;
                        }
                        Some(ProcessingTask{ data: ProcessingData::RequestBody(body), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "Processing new request (body only) ->");
                            let outbound = self
                                .request_processing
                                .process_body(body, reply_channel, Some(http_version)).await;
                            self.forward_to_external_processor(outbound).await;
                        }
                        Some(ProcessingTask{ data: ProcessingData::Response(headers, body), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "Processing new response ->");
                            let outbound = self
                                .response_processing
                                .process_response(headers, body, reply_channel, http_version);
                            self.forward_to_external_processor(outbound).await;
                        }
                        Some(ProcessingTask{ data: ProcessingData::ResponseBody(body), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "Processing new response (body only) ->");
                            let outbound = self
                                .response_processing
                                .process_body(body, reply_channel, Some(http_version)).await;
                            self.forward_to_external_processor(outbound).await;
                        }
                        _ => {
                            debug!(target: "ext_proc", "Channel received closed!");
                            break 'transaction_loop
                        },
                    }
                },

                request_body_frame = &mut self.request_processing.body_context.outbound_body_stream.next(), if self.request_processing.is_accepting_body_data() => {
                    match request_body_frame {
                        Some(Ok(frame)) => {
                            debug!(target: "ext_proc", "Received request body frame ->");
                            if let Some(data) = frame.data_ref() {
                                if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
                                    let outbound =
                                        self.request_processing.handle_body_chunk(buffered, false).await;
                                    self.forward_to_external_processor(outbound).await;
                                }
                                self.request_processing.body_context.buffered_chunk = Some(data.clone());
                            } else if let Some(trailers) = frame.trailers_ref() {
                                if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
                                    let outbound =
                                        self.request_processing.handle_body_chunk(buffered, true).await;
                                    self.forward_to_external_processor(outbound).await;
                                }
                                self.request_processing.body_context.trailers = Some(trailers.clone());
                            }
                        },
                        Some(Err(_err)) => {
                            self.request_processing.exit_on_error("Error occured when streaming request body for external processing", self.config.failure_mode_allow);
                        },
                        None => {
                            debug!(target: "ext_proc", "Request body stream ended ->");
                            if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
                                let outbound =
                                    self.request_processing.handle_body_chunk(buffered, true).await;
                                self.forward_to_external_processor(outbound).await;
                            }

                            if let Some(trailers) = self.request_processing.body_context.trailers.take() {
                                let outbound =
                                    self.request_processing.process_trailers(Some(trailers), None, None);
                                self.forward_to_external_processor(outbound).await;
                            }
                        }
                    }
                },

                response_body_frame = &mut self.response_processing.body_context.outbound_body_stream.next(), if self.response_processing.is_accepting_body_data() => {
                    match response_body_frame {
                        Some(Ok(frame)) => {
                            debug!(target: "ext_proc", "Received response body frame ->");
                            if let Some(data) = frame.data_ref() {
                                if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
                                    let outbound =
                                        self.response_processing.handle_body_chunk(buffered, false).await;
                                    self.forward_to_external_processor(outbound).await;
                                }
                                self.response_processing.body_context.buffered_chunk = Some(data.clone());
                            } else if let Some(trailers) = frame.trailers_ref() {
                                if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
                                    let outbound =
                                        self.response_processing.handle_body_chunk(buffered, true).await;
                                    self.forward_to_external_processor(outbound).await;
                                }
                                self.response_processing.body_context.trailers = Some(trailers.clone());
                            }
                        },
                        Some(Err(_err)) => {
                            self.response_processing.exit_on_error("Error occured when streaming response body for external processing", self.config.failure_mode_allow);
                        },
                        None => {
                            debug!(target: "ext_proc", "Response body stream ended ->");
                            if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
                                let outbound = self.response_processing.handle_body_chunk(buffered, true).await;
                                self.forward_to_external_processor(outbound).await;
                            }

                            if let Some(trailers) = self.response_processing.body_context.trailers.take() {
                                let outbound =
                                    self.response_processing.process_trailers(Some(trailers), None, None);
                                self.forward_to_external_processor(outbound).await;
                            }
                        }
                    }
                },
            }
        }

        debug!(target: "ext_proc", "--- END ---");
    }
}

impl<S: State + Default> ExternalProcessingWorker<S> {
    async fn connect(
        grpc_service_specifier: &GrpcServiceSpecifier,
        first_request: ProcessingRequest,
    ) -> Result<BidiStream, Error> {
        // Create a channel to send requests to the gRPC stream.
        let (request_sender, mut request_receiver) = mpsc::channel::<ProcessingRequest>(4);
        let request_stream = async_stream::stream! {
            yield first_request;
            while let Some(message) = request_receiver.recv().await {
                yield message
            }
        };
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

        Ok::<_, Error>(BidiStream { external_sender: request_sender, inbound_responses: response_stream })
    }

    async fn get_bidi_stream(&mut self, pending_request: &mut Option<ProcessingRequest>) -> Result<&BidiStream, Error> {
        if let Some(ref stream) = self.stream {
            Ok(stream)
        } else {
            let first_request = pending_request.take().ok_or_else(|| {
                Error::from("Internal error: attempted to establish bidi stream with external processor without a ProcessingRequest")
            })?;
            let stream = Self::connect(&self.config.grpc_service_specifier, first_request).await?;
            Ok(self.stream.insert(stream))
        }
    }

    async fn forward_to_external_processor(&mut self, mut request_opt: Option<ProcessingRequest>) {
        if request_opt.is_none() {
            self.timeout_state.active = false;
            return;
        }

        if let Some(ref mut req) = request_opt {
            req.protocol_config = self.handshake.take();
        }

        let stream = match self.get_bidi_stream(&mut request_opt).await {
            Err(err) => {
                error!("Failed to establish bidi stream with external processor: {err}");
                self.response_processing
                    .exit_on_error("External processor unavailable", self.config.failure_mode_allow);
                self.request_processing.exit_on_error("External processor unavailable", self.config.failure_mode_allow);
                return;
            },
            Ok(stream) => stream,
        };
        let send_outcome = match request_opt {
            Some(request) => Some(stream.external_sender.send(request).await),
            None => None,
        };

        match send_outcome {
            Some(Err(err)) => {
                error!("External processor is unavailable: {err}");
                self.response_processing
                    .exit_on_error("Lost connection to external processor", self.config.failure_mode_allow);
                self.request_processing
                    .exit_on_error("Lost connection to external processor", self.config.failure_mode_allow);
                self.timeout_state.active = false;
            },
            _ => {
                self.timeout_state.active = !self.config.observability_mode;
            },
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
            let (tx, rx) = tokio::sync::mpsc::channel(16);
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
                            key: key.to_owned(),
                            value: value.to_owned(),
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
                            key: key.to_owned(),
                            value: value.to_owned(),
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
                Some(new_body.as_bytes().into()),
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
        assert_eq!(request.method(), Method::GET);
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
                "body data from external processor".as_bytes().into(),
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
