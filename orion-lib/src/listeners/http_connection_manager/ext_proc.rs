mod status;
mod kind;
mod mutation;
mod r#override;
mod processing;
mod worker_config;

use crate::body::channel_body::{ChannelBody, FrameBridge};
use crate::body::poly_body::TrailersType;
use crate::event_error::EventFailure;
use crate::listeners::http_connection_manager::ext_proc::status::Action;
use crate::listeners::http_connection_manager::ext_proc::status::ProcessingStatus;
use crate::listeners::http_connection_manager::ext_proc::status::ReadyStatus;
use crate::listeners::http_connection_manager::ext_proc::mutation::apply_header_mutations;
use crate::listeners::http_connection_manager::ext_proc::r#override::{OverridableBodyMode, OverridableGlobalModes};
use crate::listeners::http_connection_manager::ext_proc::processing::RequestProcessing;
use crate::listeners::http_connection_manager::ext_proc::processing::ResponseProcessing;
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
use http_body::{Body, Frame};
use http_body_util::combinators::WithTrailers;
use http_body_util::BodyExt;
use http_body_util::Collected;
use http_body_util::Full;
use hyper::body::Incoming;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::TrailerProcessingMode;
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
use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HeaderMutation;
use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::core::v3::{HeaderMap, HeaderValue},
        service::ext_proc::v3::{
            external_processor_client::ExternalProcessorClient,
            processing_response::Response as ProcessingResponseType, ImmediateResponse, ProcessingRequest,
            ProcessingResponse, ProtocolConfiguration,
        },
    },
    google,
    tonic::{codec::Streaming, Status},
};
use orion_format::types::ResponseFlags as FmtResponseFlags;
use pingora_timeout::fast_timeout;
use smallvec::SmallVec;
use std::convert::Infallible;
use std::future::ready;
use std::future::Ready;
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, warn};

#[derive(Debug, Clone)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExternalProcessor {
    ext_proc_worker: Option<mpsc::Sender<ProcessingTask>>,
    worker_config: Arc<ExternalProcessingWorkerConfig>,
    forward_rules: Option<Arc<HeaderForwardingRules>>,
    overridable_modes: Arc<OverridableGlobalModes>,
    //sending_request_headers: bool,
    //sending_request_body: bool,
    //sending_request_trailers: bool,
    //sending_response_headers: bool,
    //sending_response_body: bool,
    //sending_response_trailers: bool,
}

impl From<ExternalProcessorConfig> for ExternalProcessor {
    fn from(initial_config: ExternalProcessorConfig) -> Self {
        debug!(target: "ext_proc", "From<ExternalProcessorConfig> for ExternalProcessor");
        Self::from((initial_config, None))
    }
}

impl From<(ExternalProcessorConfig, Option<ExtProcPerRoute>)> for ExternalProcessor {
    fn from((initial_config, per_route_config): (ExternalProcessorConfig, Option<ExtProcPerRoute>)) -> Self {
        debug!(target: "ext_proc", "From<ExternalProcessorConfig, ExtProcPerRoute> for ExternalProcessor");
        let forward_rules = initial_config.forward_rules.clone().map(Arc::new);
        let worker_config = ExternalProcessingWorkerConfig::from((initial_config, per_route_config));

        //let sending_request_headers =
        //    !matches!(worker_config.processing_mode.request_header_mode, HeaderProcessingMode::Skip);
        //let sending_request_body = !matches!(worker_config.processing_mode.request_body_mode, BodyProcessingMode::None);
        //let sending_request_trailers =
        //    !matches!(worker_config.processing_mode.request_trailer_mode, TrailerProcessingMode::Skip);

        //let sending_response_headers =
        //    !matches!(worker_config.processing_mode.response_header_mode, HeaderProcessingMode::Skip);
        //let sending_response_body =
        //    !matches!(worker_config.processing_mode.response_body_mode, BodyProcessingMode::None);
        //let sending_response_trailers =
        //    !matches!(worker_config.processing_mode.response_trailer_mode, TrailerProcessingMode::Skip);

        let overridable_global_modes = Arc::new(OverridableGlobalModes::from(&worker_config));

        Self {
            ext_proc_worker: None,
            worker_config: Arc::new(worker_config),
            forward_rules,
            overridable_modes: overridable_global_modes,
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
        debug!(target: "ext_proc", "From<(ExternalProcessorConfig, Option<ExtProcPerRoute>)> for ExternalProcessingWorkerConfig");
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

macro_rules! run_action {
    // $self: The 'self' instance.
    // $processor: The specific processing struct (e.g., self.request_processing or self.response_processing).
    // $action: The Action to process.
    // $ctx: A string literal context for logging.
    ($self:ident, $processor:expr, $action:expr, $ctx:expr) => {
        match $action {
            Action::Send(outbound) => {
                debug!(target: "ext_proc", "{ctx} @{typ}: forward {outbound:?}",
                    ctx = $ctx,
                    typ = stringify!($processor),
                    outbound = outbound);

                $self.forward_to_external_processor(outbound).await;
            },
            Action::Return(status) => {
                $self.timeout_state.active = false;

                if let Some(reply_channel) = $processor.reply_channel.take() {
                    debug!(target: "ext_proc", "{ctx} @{typ}: status -> {status:?}",
                        ctx = $ctx,
                        typ = stringify!($processor),
                        status = status);

                    let _ = reply_channel.send(status);
                }
            },
        }
    };
}

impl ExternalProcessor {
    #[allow(clippy::too_many_lines)]
    pub async fn apply_request(&mut self, request: &mut Request<BodyWithMetrics<PolyBody>>) -> FilterDecision {
        if !self.overridable_modes.request.should_process_headers()
            && !self.overridable_modes.request.should_process_body()
            && !self.overridable_modes.request.should_process_trailers()
        {
            return FilterDecision::Continue;
        }

        let mut ext_proc_headers = None;

        if self.overridable_modes.request.should_process_headers() {
            debug!(target: "ext_proc", "request processing headers");
            ext_proc_headers = Some(self.filter_header_map(request.headers()));
        }

        let body: PolyBody = std::mem::take(&mut request.body_mut().inner);

        let ext_proc_frame_bridge = match (
            self.overridable_modes.request.body_mode(),
            self.overridable_modes.request.trailer_mode(),
        ) {
            (OverridableBodyMode::None, trailers_mode) => {
                // event though body processing is None and trailers processing is Skip, we have to
                // create the bridge, to allow ext_proc mutate the body with ContinueAndReplace action.
                debug!(target: "ext_proc", "request processing body(Streamed) and trailers:{trailers_mode:?} => {body:?}");
                let (new_body, bridge) = ChannelBody::new(body);
                request.body_mut().inner = PolyBody::from(new_body);
                bridge
            },
            (OverridableBodyMode::Streamed | OverridableBodyMode::FullDuplexStreamed, trailers_mode) => {
                debug!(target: "ext_proc", "request processing body(Streamed) and trailers:{trailers_mode:?} => {body:?}");
                let (new_body, bridge) = ChannelBody::new(body);
                request.body_mut().inner = PolyBody::from(new_body);
                bridge
            },
            (OverridableBodyMode::Buffered | OverridableBodyMode::BufferedPartial, trailers_mode) => {
                debug!(target: "ext_proc", "request processing body(Buffered) with trailers:{trailers_mode:?} => {body:?}");
                let Ok(collected) = body.collect().await else {
                    return self.on_filter_error(
                        "Failed to collect request body for external processing",
                        None,
                        request.version(),
                    );
                };

                let buffered = Self::collect_to_single_chunk(collected).await;

                let (new_body, bridge) = ChannelBody::new(buffered);
                request.body_mut().inner = PolyBody::from(new_body);
                bridge
            },
        };

        debug!(target: "ext_proc", "request headers: {ext_proc_headers:?}");
        debug!(target: "ext_proc", "request body: {ext_proc_frame_bridge:?}");

        let processing_data = ProcessingData::Request(ext_proc_headers, ext_proc_frame_bridge);

        let ver = request.version();
        let Ok(response_rx) = self.send_processing_data(processing_data, ver).await else {
            return self.on_filter_error("Failed to schedule sending request data to external processor", None, ver);
        };

        match response_rx.await {
            Ok(ProcessingStatus::HaltedOnError) => FilterDecision::Continue,
            Ok(ProcessingStatus::EndWithDirectResponse(direct_response)) => {
                FilterDecision::DirectResponse(direct_response)
            },
            Ok(ProcessingStatus::RequestReady(ReadyStatus { clear_route_cache, headers_modifications })) => {
                if let Some(headers_modifications) = headers_modifications {
                    debug!(target: "ext_proc", "applying headers mutation...");
                    if let Err(e) = apply_header_mutations(
                        request.headers_mut(),
                        &headers_modifications,
                        Some(&self.worker_config.mutation_rules),
                    ) {
                        return self.on_filter_error(
                            "Invalid header modifications received from external processor",
                            Some(e),
                            request.version(),
                        );
                    }
                }

                // at this point it's hard to know if body replacement is being requested or not.
                // So we just remove the content-length header to be safe.

                request.headers_mut().remove(CONTENT_LENGTH);

                if clear_route_cache {
                    return FilterDecision::Reroute;
                }
                FilterDecision::Continue
            },
            Ok(ProcessingStatus::ResponseReady(ReadyStatus { headers_modifications, clear_route_cache })) => {
                todo!()
            },
            Err(e) => self.on_filter_error(
                format!("External processor request processing: {e:?}").as_str(),
                Some(e.into()),
                request.version(),
            ),
        }
    }

    async fn collect_to_single_chunk(original: Collected<Bytes>) -> Collected<Bytes> {
        let trailers = original.trailers().cloned();
        let aggregated_bytes = original.to_bytes();
        let new_body = Full::new(aggregated_bytes);

        let trailer_future = async move { trailers.map(Ok::<_, Infallible>) };

        let body_with_trailers = new_body.with_trailers(trailer_future);
        body_with_trailers.collect().await.unwrap()
    }

    #[inline]
    fn dup_and_split(
        &self,
        body: Collected<Bytes>,
    ) -> (WithTrailers<Full<Bytes>, Ready<TrailersType>>, Full<Bytes>, Option<http::HeaderMap>) {
        let trailers = body.trailers().cloned();
        let trailers2 = trailers.clone();
        let bytes = body.to_bytes();
        let bytes2 = bytes.clone();
        (Full::new(bytes).with_trailers(ready(trailers.map(Ok::<_, Infallible>))), Full::new(bytes2), trailers2)
    }

    pub async fn apply_response(&mut self, response: &mut Response<PolyBody>) -> FilterDecision {
        if !self.overridable_modes.response.should_process_headers()
            && !self.overridable_modes.response.should_process_body()
            && !self.overridable_modes.response.should_process_trailers()
        {
            return FilterDecision::Continue;
        }

        todo!()
        //let is_empty_body = response.body().is_end_stream();

        //let mut ext_proc_headers = None;
        //let mut ext_proc_body = None;
        //let mut ext_proc_trailers = None;

        //if self.sending_response_headers {
        //    debug!(target: "ext_proc", "response processing HEADERS(true)");
        //    let envoy_headers = self.build_envoy_data_plane_api_header_map(response.headers(), false);
        //    ext_proc_headers = Some(Self::into_http_headers(envoy_headers, is_empty_body));
        //}

        //if !is_empty_body {
        //    let body: PolyBody = std::mem::take(response.body_mut());
        //    match (self.sending_response_body, self.sending_response_trailers) {
        //        (true, send_trailers) => {
        //            debug!(target: "ext_proc", "response processing BODY(true) and TRAILERS({send_trailers})");

        //            let Ok(collected) = body.collect().await else {
        //                return self.on_filter_error(
        //                    "Failed to collect response body for external processing",
        //                    None,
        //                    response.version(),
        //                );
        //            };

        //            let (orig_body, body2, trailers2) = self.dup_and_split(collected);
        //            *response.body_mut() = PolyBody::from(orig_body);
        //            ext_proc_body = Some(PolyBody::from(body2));
        //            ext_proc_trailers = trailers2;
        //        },
        //        (false, true) => {
        //            debug!(target: "ext_proc", "request processing TRAILERS(true) only");
        //            let Ok(collected) = body.collect().await else {
        //                return self.on_filter_error(
        //                    "Failed to collect request body for external processing",
        //                    None,
        //                    response.version(),
        //                );
        //            };

        //            ext_proc_body = Some(PolyBody::from(Empty::new()));
        //            ext_proc_trailers = collected.trailers().cloned();
        //            *response.body_mut() = PolyBody::from(collected);
        //        },
        //        (false, false) => {
        //            *response.body_mut() = body;
        //        },
        //    }
        //} else {
        //    debug!(target: "ext_proc", "response processing no BODY and no TRAILERS");
        //}

        //debug!(target: "ext_proc", "response: headers: {ext_proc_headers:?} - body: {ext_proc_body:?} - trailers: {ext_proc_trailers:?}");

        //if ext_proc_headers.is_none() && ext_proc_body.is_none() && ext_proc_trailers.is_none() {
        //    return FilterDecision::Continue;
        //}

        //let processing_data = ProcessingData::Response(
        //    ext_proc_headers,
        //    ext_proc_body,
        //    ext_proc_trailers.map(|hm| Self::into_http_headers(hm, true)),
        //);

        //let ver = response.version();
        //let Ok(response_rx) = self.send_processing_data(processing_data, ver).await else {
        //    return self.on_filter_error("Failed to schedule sending request data to external processor", None, ver);
        //};

        //match response_rx.await {
        //    Ok(ExtProcStatus::HaltedOnError { restore_body }) => {
        //        if let Some(body) = restore_body {
        //            *response.body_mut() = body;
        //        }
        //        FilterDecision::Continue
        //    },
        //    Ok(ExtProcStatus::EndWithDirectResponse(direct_response)) => {
        //        FilterDecision::DirectResponse(direct_response)
        //    },
        //    Ok(ExtProcStatus::ResponseIsReady { header_modifications, body_replacement }) => {
        //        if let Some(header_modifications) = header_modifications {
        //            if let Err(e) = apply_header_mutations(
        //                response.headers_mut(),
        //                &header_modifications,
        //                Some(&self.worker_config.mutation_rules),
        //            ) {
        //                return self.on_filter_error(
        //                    "Invalid header modifications received from external processor",
        //                    Some(e),
        //                    response.version(),
        //                );
        //            }
        //        }
        //        if let Some(body_replacement) = body_replacement {
        //            *response.body_mut() = body_replacement;
        //            response.headers_mut().remove(CONTENT_LENGTH);
        //        }
        //        FilterDecision::Continue
        //    },
        //    Ok(ExtProcStatus::RequestIsReady {
        //        header_modifications: _,
        //        body_replacement: _,
        //        override_sending_response_headers: _,
        //        override_sending_response_body: _,
        //        clear_route_cache: _,
        //    }) => self.on_filter_error(
        //        "Unexpected request from external processor during response processing",
        //        None,
        //        response.version(),
        //    ),
        //    Err(e) => self.on_filter_error(
        //        "External processor failed during response processing",
        //        Some(e.into()),
        //        response.version(),
        //    ),
        //}
    }

    async fn send_processing_data(
        &mut self,
        data: ProcessingData,
        ver: http::Version,
    ) -> Result<oneshot::Receiver<ProcessingStatus>, SendError<ProcessingTask>> {
        let (response_tx, response_rx) = oneshot::channel();
        let processing_message = ProcessingTask { data, reply_channel: response_tx, http_version: ver };

        let worker_channel = self.get_worker_channel();
        worker_channel.send(processing_message).await.map(|_| response_rx)
    }

    fn on_filter_error(&mut self, msg: &str, error: Option<Error>, http_version: http::Version) -> FilterDecision {
        if let Some(err) = error {
            error!(target: "ext_proc","{msg}: {err}");
        } else {
            error!(target: "ext_proc", "{msg}");
        }
        if self.worker_config.failure_mode_allow {
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

            // replace the internal overridable modes blueprint with a new spawned instance for the worker
            //

            let overridable_modes = Arc::new(self.overridable_modes.spawn());
            self.overridable_modes = Arc::clone(&overridable_modes);

            if self.worker_config.observability_mode {
                let worker = ExternalProcessingWorker::<kind::Observability>::new(
                    Arc::clone(&self.worker_config),
                    overridable_modes,
                );
                tokio::spawn(worker.ext_proc_loop(receiver));
            } else {
                let worker =
                    ExternalProcessingWorker::<kind::Processing>::new(Arc::clone(&self.worker_config), overridable_modes);
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

    fn filter_header_map(&self, headers: &http::HeaderMap) -> http::HeaderMap {
        let mut filtered_headers = http::HeaderMap::new();
        for (name, value) in headers {
            let header_name = name.as_str();
            if self.should_forward_header(header_name) {
                filtered_headers.append(name.clone(), value.clone());
            }
        }
        filtered_headers
    }
}

struct EnvoyHeaderMap(HeaderMap);
impl From<&http::HeaderMap> for EnvoyHeaderMap {
    fn from(headers: &http::HeaderMap) -> Self {
        let mut headers_vec = Vec::with_capacity(headers.len());
        for (name, value) in headers {
            let header_name = name.as_str();
            let header_value = if let Ok(value_str) = value.to_str() {
                HeaderValue { key: header_name.to_owned(), value: value_str.to_owned(), raw_value: Vec::default() }
            } else {
                HeaderValue {
                    key: header_name.to_owned(),
                    value: String::default(),
                    raw_value: value.as_bytes().into(),
                }
            };
            headers_vec.push(header_value);
        }
        EnvoyHeaderMap(HeaderMap { headers: headers_vec })
    }
}

impl From<EnvoyHeaderMap> for http::HeaderMap {
    fn from(envoy_headers: EnvoyHeaderMap) -> Self {
        let mut headers = http::HeaderMap::with_capacity(envoy_headers.0.headers.len());

        for header in envoy_headers.0.headers {
            let header_name = match http::header::HeaderName::from_bytes(header.key.as_bytes()) {
                Ok(name) => name,
                Err(_) => continue,
            };

            let header_value = if !header.value.is_empty() {
                match http::header::HeaderValue::from_maybe_shared(header.value) {
                    Ok(value) => value,
                    Err(_) => continue,
                }
            } else {
                match http::header::HeaderValue::from_maybe_shared(header.raw_value) {
                    Ok(value) => value,
                    Err(_) => continue,
                }
            };

            headers.append(header_name, header_value);
        }

        headers
    }
}

#[derive(Debug)]
struct ProcessingTask {
    data: ProcessingData,
    reply_channel: oneshot::Sender<ProcessingStatus>,
    http_version: http::Version,
}

#[derive(Debug)]
enum ProcessingData {
    Request(Option<http::HeaderMap>, FrameBridge),
    Response(Option<http::HeaderMap>, FrameBridge),
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

struct ExternalProcessingWorker<S: kind::Mode> {
    config: Arc<ExternalProcessingWorkerConfig>,
    bidi_stream: Option<BidiStream>,
    request_processing: RequestProcessing<S>,
    response_processing: ResponseProcessing<S>,
    handshake: Option<ProtocolConfiguration>,
    timeout_state: TimeoutState,
    overridable_modes: Arc<OverridableGlobalModes>,
}

fn clone_frame(frame: &Frame<Bytes>) -> Frame<Bytes> {
    if let Some(data) = frame.data_ref() {
        Frame::data(data.clone())
    } else if let Some(trailers) = frame.trailers_ref() {
        Frame::trailers(trailers.clone())
    } else {
        // empty frame as fallback
        Frame::data(Bytes::new())
    }
}

impl ExternalProcessingWorker<kind::Processing> {
    fn new(config: Arc<ExternalProcessingWorkerConfig>, overridable_global_modes: Arc<OverridableGlobalModes>) -> Self {
        let request_processing = RequestProcessing::<kind::Processing>::from(&*config);
        let response_processing = ResponseProcessing::<kind::Processing>::from(&*config);
        let handshake = Some(ProtocolConfiguration {
            request_body_mode: config.processing_mode.request_body_mode as i32,
            response_body_mode: config.processing_mode.response_body_mode as i32,
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
        });
        let message_timeout = config.message_timeout;
        Self {
            config,
            bidi_stream: None,
            request_processing,
            response_processing,
            handshake,
            timeout_state: TimeoutState { active: false, duration: message_timeout, extended: false },
            overridable_modes: overridable_global_modes,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn ext_proc_loop(mut self, mut processing_request_channel: mpsc::Receiver<ProcessingTask>) {
        // The following label is not strictly necessary, but it makes it clearer what is being exited at the break point.
        // It also makes it easier to locate subsequent exit points.
        debug!(target: "ext_proc", "--- BEGIN ---");

        let mut parked_frame: Option<Frame<Bytes>> = None;
        let mut sent_frames: SmallVec<[Frame<Bytes>; 2]> = SmallVec::new();

        'transaction_loop: loop {
            tokio::select! {
                outbond_processing_task = processing_request_channel.recv() => {
                    debug!(target: "ext_proc", "PROCESING TASK:");
                    match outbond_processing_task {
                        Some(ProcessingTask{ data: ProcessingData::Request(headers, frame_bridge), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "Processing new request");

                            let action = self.request_processing.process(headers, frame_bridge, reply_channel, http_version, &self.overridable_modes).await;
                            run_action!(self, self.request_processing, action, "process_request");
                        }
                        // Some(ProcessingTask{ data: ProcessingData::Response(headers, body, trailers), reply_channel, http_version}) => {
                        //     debug!(target: "ext_proc", "Processing new response");
                        //     let action = self .response_processing.process(headers, body, trailers, reply_channel, http_version).await;
                        //     run_action!(self, self.response_processing, action, "process_response");
                        // }
                        _ => {
                            debug!(target: "ext_proc", "Worker channel closed!");
                            break 'transaction_loop
                        },
                    }
                },

                inbound_processing_response = if let Some(stream) = self.bidi_stream.as_mut() {
                        Either::Left(stream.inbound_responses.message())
                    } else {
                        Either::Right(std::future::pending::<Result<Option<ProcessingResponse>, Status>>())
                    } => {
                    debug!(target: "ext_proc", "INBOUND PROCESSING RESPONSE:");

                    match inbound_processing_response {
                        Ok(Some(ProcessingResponse { override_message_timeout: Some(extended_timeout), ..})) => {
                            debug!(target: "ext_proc", "<- Requested timeout extension received: {extended_timeout:?}");
                            if !self.handle_timeout_extension(extended_timeout) {
                                debug!(target: "ext_proc", "Invalid timeout extension - closing stream");
                                break 'transaction_loop;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ImmediateResponse(response_attempt)), ..})) => {
                            debug!(target: "ext_proc", "<- Immediate response received");
                            if self.config.disable_immediate_response {
                                let msg = "External processor attempted to send immediate response - which is disabled by config";
                                warn!("{msg}");

                                if let Some(reply_channel) = self.request_processing.reply_channel.take() {
                                    let _ = reply_channel.send(ProcessingStatus::HaltedOnError);
                                }
                                if let Some(reply_channel) = self.response_processing.reply_channel.take() {
                                    let _ = reply_channel.send(ProcessingStatus::HaltedOnError);
                                }
                                // TODO
                                //self.response_processing.exit_on_error(msg, self.config.failure_mode_allow);
                                //self.request_processing.status_error(msg, self.config.failure_mode_allow);
                            } else {
                                let response = self.build_direct_response(&response_attempt);
                                let status = ProcessingStatus::EndWithDirectResponse(response);
                                if self.response_processing.is_awaiting_reply() {
                                    if let Some(reply_channel) = self.response_processing.reply_channel.take() {
                                        let _ = reply_channel.send(status);
                                    }
                                } else {
                                    if let Some(reply_channel) = self.request_processing.reply_channel.take() {
                                        let _ = reply_channel.send(status);
                                    }
                                }
                            }
                            debug!(target: "ext_proc", "Immediate response processed - closing stream");
                            break 'transaction_loop;
                        },
                        Ok(Some(ProcessingResponse { mode_override, response: Some(ProcessingResponseType::RequestHeaders(headers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- Request header response received");
                            if self.config.allow_mode_override {
                                if let Some(overrides) = mode_override {
                                    self.request_processing.apply_mode_overrides(&overrides, &self.config.allowed_override_modes, &self.overridable_modes);
                                    self.response_processing.apply_mode_overrides(&overrides, &self.config.allowed_override_modes, &self.overridable_modes);
                                }
                            }

                            let action = self.request_processing.handle_headers_response(
                                headers_response,
                                &self.config.route_cache_action,
                                &self.overridable_modes,
                            ).await;

                            run_action!(self, self.request_processing, action, "handle_headers_response");
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::RequestBody(body_response)), ..})) => {
                            debug!(target: "ext_proc", "<- Request body response received");

                            let action = self
                                .request_processing
                                .handle_body_response(&mut sent_frames, body_response, Some(&self.config.route_cache_action)).await;

                            run_action!(self, self.request_processing, action, "body_response");

                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::RequestTrailers(trailers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- Request trailers response received");
                            let action = self.request_processing.handle_trailers_response(trailers_response).await;
                            run_action!(self, self.request_processing, action, "handle_trailers_response");
                        },
                        Ok(Some(ProcessingResponse { mode_override, response: Some(ProcessingResponseType::ResponseHeaders(headers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- Response headers response received");
                            if self.config.allow_mode_override {
                                if let Some(overrides) = mode_override {
                                    self.response_processing.apply_mode_overrides(&overrides, &self.config.allowed_override_modes, &self.overridable_modes);
                                }
                            }

                            let action = self.response_processing.handle_headers_response(
                                headers_response,
                                &self.config.route_cache_action,
                                &self.overridable_modes,
                            ).await;
                            run_action!(self, self.response_processing, action, "handle_headers_response");
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ResponseBody(body_response)), ..})) => {
                            debug!(target: "ext_proc", "<- Response body response received");
                            let empty_response = body_response.response.is_none();
                            let action = self.response_processing.handle_body_response(&mut sent_frames, body_response, None).await;
                            run_action!(self, self.response_processing, action, "handle_body_response");

                            if empty_response {
                                debug!(target: "ext_proc", "Response body response contained no response - closing stream");
                                break 'transaction_loop;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ResponseTrailers(trailers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- Response trailers response received");
                            let action = self.response_processing.handle_trailers_response(trailers_response).await;
                            run_action!(self, self.response_processing, action, "handle_trailers_response");
                        },
                        Ok(Some(r)) => {
                            debug!(target: "ext_proc", "<- Noop response received {r:?}");
                            let action = self.response_processing.handle_noop_response(ProcessingStatus::ResponseReady);
                            run_action!(self, self.response_processing, action, "handle_noop_response");
                            let action = self.request_processing.handle_noop_response(ProcessingStatus::RequestReady);
                            run_action!(self, self.request_processing, action, "handle_noop_response");
                        }
                        Err(e) => {
                            let msg = "external processor gRPC error";
                            error!(target: "ext_proc", "<- {msg}: {e}");
                            if self.response_processing.is_awaiting_reply() {
                                let status = self.response_processing.status_error(msg, self.config.failure_mode_allow);
                                if let Some(reply_channel) = self.response_processing.reply_channel.take() {
                                    let _ = reply_channel.send(status);
                                }
                            } else {
                                let status = self.request_processing.status_error(msg, self.config.failure_mode_allow);
                                if let Some(reply_channel) = self.request_processing.reply_channel.take() {
                                    let _ = reply_channel.send(status);
                                }
                            }
                            debug!(target: "ext_proc", "gRPC error processed - closing stream");
                            break 'transaction_loop;
                        },
                        _ => {
                            debug!(target: "ext_proc", "Stream closed by the external processor - closing stream");
                            break 'transaction_loop;
                        }
                    }
                },

                outbout_request_body_frame =
                    async { self.request_processing.frame_bridge.next().await }, if self.request_processing.streaming_body_enabled => {

                    debug!(target: "ext_proc", "OUTBOUND REQUEST BODY FRAME:");
                    match outbout_request_body_frame {
                        Some(Ok(current_frame)) => {
                            debug!(target: "ext_proc", "Sending body chunk of request ({})",  if current_frame.is_data() { "DATA" } else { "TRAILERS" });

                            // 1. send previous parked frame...
                            //
                            if let Some(prev_frame) = parked_frame.take() {
                                let action = self.request_processing.handle_body_chunk(clone_frame(&prev_frame), false).await;
                                run_action!(self, self.request_processing, action, "handle_body_chunk (parked)");
                                // save a copy of the frame to inject into the body bridge later
                                sent_frames.push(prev_frame);
                            }

                            // 2. process the current frame...
                            //
                            if current_frame.is_data() {
                                // This is a DATA, let's park it if is supposed to be streamed
                                if self.overridable_modes.request.should_process_body() {
                                    parked_frame = Some(current_frame);
                                    debug!(target: "ext_proc", "Parking body chunk of request (streamed)!");
                                } else {
                                    _ = self.request_processing.frame_bridge.inject_frame(Ok(current_frame)).await;
                                }

                            } else {
                                // This is TRAILERS. it is the last frame.
                                if self.overridable_modes.request.should_process_trailers() {
                                    parked_frame = Some(current_frame);
                                    debug!(target: "ext_proc", "Parking body chunk of request (streamed)!");
                                } else {
                                    _ = self.request_processing.frame_bridge.inject_frame(Ok(current_frame)).await;
                                }
                            }
                        },
                        Some(Err(_err)) => {
                            debug!(target: "ext_proc", "Error occured when streaming request body to external processing");
                            self.request_processing.status_error("Error occured when streaming request body to external processing", self.config.failure_mode_allow);
                        },
                        None => {
                            debug!(target: "ext_proc", "Request body stream ended!");
                            if let Some(current_frame) = parked_frame.take() {
                                debug!(target: "ext_proc", "Sending last body chunk of request (stubbed)!");
                                let action = self.request_processing.handle_body_chunk(clone_frame(&current_frame), true).await;
                                run_action!(self, self.request_processing, action, "handle_body_chunk (end)");
                                sent_frames.push(current_frame);
                            }

                            self.request_processing.end_of_stream = true;
                            self.request_processing.streaming_body_enabled = false;

                            if sent_frames.is_empty() {
                                debug!(target: "ext_proc", "frame brige closed!");
                                self.request_processing.frame_bridge.close().await;
                            }
                        }
                    }
                },

                () = fast_timeout::fast_sleep(self.timeout_state.duration), if self.timeout_state.active => {
                    debug!(target: "ext_proc", "loop: FAST TIMEOUT..");

                    let status = self.request_processing.status_timeout(self.config.failure_mode_allow);
                    if let Some(reply_channel) = self.request_processing.reply_channel.take() {
                        let _ = reply_channel.send(status);
                    }

                    let status = self.response_processing.status_timeout(self.config.failure_mode_allow);
                    if let Some(reply_channel) = self.response_processing.reply_channel.take() {
                        let _ = reply_channel.send(status);
                    }
                }
            }
        }

        debug!(target: "ext_proc", "--- END ---");
    }
}

impl ExternalProcessingWorker<kind::Observability> {
    fn new(config: Arc<ExternalProcessingWorkerConfig>, overridable_global_modes: Arc<OverridableGlobalModes>) -> Self {
        let request_processing = RequestProcessing::<kind::Observability>::from(&*config);
        let response_processing = ResponseProcessing::<kind::Observability>::from(&*config);

        let handshake = Some(ProtocolConfiguration {
            request_body_mode: config.processing_mode.request_body_mode as i32,
            response_body_mode: config.processing_mode.response_body_mode as i32,
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
        });
        let message_timeout = config.message_timeout;
        Self {
            config,
            bidi_stream: None,
            request_processing,
            response_processing,
            handshake,
            timeout_state: TimeoutState { active: false, duration: message_timeout, extended: false },
            overridable_modes: overridable_global_modes,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn ext_proc_loop(mut self, mut processing_request_channel: mpsc::Receiver<ProcessingTask>) {
        todo!()
        // // The following label is not strictly necessary, but it makes it clearer what is being exited at the break point.
        // // It also makes it easier to locate subsequent exit points.
        // debug!(target: "ext_proc", "--- BEGIN ---");
        // 'transaction_loop: loop {
        //     tokio::select! {
        //         outbond_processing_task = processing_request_channel.recv() => {
        //             match outbond_processing_task {
        //                 Some(ProcessingTask{ data: ObservabilityData::Request(headers, body), reply_channel, http_version}) => {
        //                     debug!(target: "ext_proc", "Processing new request ->");
        //                     let outbound = self
        //                         .request_processing
        //                         .process_request(headers, body, reply_channel, http_version);
        //                     self.forward_to_external_processor(outbound).await;

        //                     // FIXME :::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

        //                     if self.request_processing.is_body_processing_planned() && self.request_processing.body_context.body.is_some() {
        //                         if let Some(body) = self.request_processing.body_context.body.take() {
        //                             if let Some(reply_channel) = self.request_processing.reply_channel.take() {
        //                                 let outbound = self
        //                                     .request_processing
        //                                     .process_body(body, reply_channel, Some(http_version)).await;
        //                                 self.forward_to_external_processor(outbound).await;
        //                             }
        //                         }
        //                     }
        //                 }
        //                 Some(ProcessingTask{ data: ObservabilityData::RequestBody(body), reply_channel, http_version}) => {
        //                     debug!(target: "ext_proc", "Processing new request (body only) ->");
        //                     let outbound = self
        //                         .request_processing
        //                         .process_body(body, reply_channel, Some(http_version)).await;
        //                     self.forward_to_external_processor(outbound).await;
        //                 }
        //                 Some(ProcessingTask{ data: ObservabilityData::Response(headers, body), reply_channel, http_version}) => {
        //                     debug!(target: "ext_proc", "Processing new response ->");
        //                     let outbound = self
        //                         .response_processing
        //                         .process_response(headers, body, reply_channel, http_version);
        //                     self.forward_to_external_processor(outbound).await;
        //                 }
        //                 Some(ProcessingTask{ data: ObservabilityData::ResponseBody(body), reply_channel, http_version}) => {
        //                     debug!(target: "ext_proc", "Processing new response (body only) ->");
        //                     let outbound = self
        //                         .response_processing
        //                         .process_body(body, reply_channel, Some(http_version)).await;
        //                     self.forward_to_external_processor(outbound).await;
        //                 }
        //                 _ => {
        //                     debug!(target: "ext_proc", "Channel received closed!");
        //                     break 'transaction_loop
        //                 },
        //             }
        //         },

        //         request_body_frame = &mut self.request_processing.body_context.outbound_body_stream.next(), if self.request_processing.is_accepting_body_data() => {
        //            match request_body_frame {
        //                Some(Ok(frame)) => {
        //                    debug!(target: "ext_proc", "Received request body frame ->");
        //                    if let Some(data) = frame.data_ref() {
        //                        if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
        //                            let outbound =
        //                                self.request_processing.handle_body_chunk(buffered, false).await;
        //                            self.forward_to_external_processor(outbound).await;
        //                        }
        //                        self.request_processing.body_context.buffered_chunk = Some(data.clone());
        //                    } else if let Some(trailers) = frame.trailers_ref() {
        //                        if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
        //                            let outbound =
        //                                self.request_processing.handle_body_chunk(buffered, true).await;
        //                            self.forward_to_external_processor(outbound).await;
        //                        }
        //                        self.request_processing.body_context.trailers = Some(trailers.clone());
        //                    }
        //                },
        //                Some(Err(_err)) => {
        //                    self.request_processing.exit_on_error("Error occured when streaming request body for external processing", self.config.failure_mode_allow);
        //                },
        //                None => {
        //                    debug!(target: "ext_proc", "Request body stream ended ->");
        //                    if let Some(buffered) = self.request_processing.body_context.buffered_chunk.take() {
        //                        let outbound =
        //                            self.request_processing.handle_body_chunk(buffered, true).await;
        //                        self.forward_to_external_processor(outbound).await;
        //                    }

        //                    if let Some(trailers) = self.request_processing.body_context.trailers.take() {
        //                        let outbound =
        //                            self.request_processing.process_trailers(Some(trailers), None, None);
        //                        self.forward_to_external_processor(outbound).await;
        //                    }
        //                }
        //            }
        //         },

        //         response_body_frame = &mut self.response_processing.body_context.outbound_body_stream.next(), if self.response_processing.is_accepting_body_data() => {
        //            match response_body_frame {
        //                Some(Ok(frame)) => {
        //                    debug!(target: "ext_proc", "Received response body frame ->");
        //                    if let Some(data) = frame.data_ref() {
        //                        if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
        //                            let outbound =
        //                                self.response_processing.handle_body_chunk(buffered, false).await;
        //                            self.forward_to_external_processor(outbound).await;
        //                        }
        //                        self.response_processing.body_context.buffered_chunk = Some(data.clone());
        //                    } else if let Some(trailers) = frame.trailers_ref() {
        //                        if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
        //                            let outbound =
        //                                self.response_processing.handle_body_chunk(buffered, true).await;
        //                            self.forward_to_external_processor(outbound).await;
        //                        }
        //                        self.response_processing.body_context.trailers = Some(trailers.clone());
        //                    }
        //                },
        //                Some(Err(_err)) => {
        //                    self.response_processing.exit_on_error("Error occured when streaming response body for external processing", self.config.failure_mode_allow);
        //                },
        //                None => {
        //                    debug!(target: "ext_proc", "Response body stream ended ->");
        //                    if let Some(buffered) = self.response_processing.body_context.buffered_chunk.take() {
        //                        let outbound = self.response_processing.handle_body_chunk(buffered, true).await;
        //                        self.forward_to_external_processor(outbound).await;
        //                    }

        //                    if let Some(trailers) = self.response_processing.body_context.trailers.take() {
        //                        let outbound =
        //                            self.response_processing.process_trailers(Some(trailers), None, None);
        //                        self.forward_to_external_processor(outbound).await;
        //                    }
        //                }
        //            }
        //         },
        //     }
        // }

        // debug!(target: "ext_proc", "--- END ---");
    }
}

impl<S: kind::Mode + Default> ExternalProcessingWorker<S> {
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
        if let Some(ref stream) = self.bidi_stream {
            Ok(stream)
        } else {
            let first_request = pending_request.take().ok_or_else(|| {
                Error::from("Internal error: attempted to establish bidi stream with external processor without a ProcessingRequest")
            })?;
            let stream = Self::connect(&self.config.grpc_service_specifier, first_request).await?;
            Ok(self.bidi_stream.insert(stream))
        }
    }

    async fn forward_to_external_processor(&mut self, mut request: ProcessingRequest) {
        request.protocol_config = self.handshake.take();
        let mut request_opt = Some(request);

        let stream = match self.get_bidi_stream(&mut request_opt).await {
            Err(err) => {
                self.response_processing
                    .status_error(format!("External processor: {err:?}").as_str(), self.config.failure_mode_allow);
                self.request_processing
                    .status_error(format!("External processor: {err:?}").as_str(), self.config.failure_mode_allow);
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
                error!(target: "ext_proc", "External processor is unavailable: {err}");
                if let Some(reply_channel) = self.response_processing.reply_channel.take() {
                    let _ = reply_channel.send(
                        self.response_processing
                            .status_error("Lost connection to external processor", self.config.failure_mode_allow),
                    );
                }

                if let Some(reply_channel) = self.request_processing.reply_channel.take() {
                    let _ = reply_channel.send(
                        self.request_processing
                            .status_error("Lost connection to external processor", self.config.failure_mode_allow),
                    );
                }

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
            if let Some(reply_channel) = self.response_processing.reply_channel.take() {
                let _ = reply_channel.send(self.response_processing.status_error(msg, self.config.failure_mode_allow));
            }

            if let Some(reply_channel) = self.request_processing.reply_channel.take() {
                let _ = reply_channel.send(self.request_processing.status_error(msg, self.config.failure_mode_allow));
            }

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
                ProcessingResponse, StreamedBodyResponse, TrailersResponse,
            },
        },
        tonic::{
            async_trait, transport::Server, Request as TonicRequest, Response as TonicResponse, Status, Streaming,
        },
    };
    use std::{
        collections::VecDeque,
        net::SocketAddr,
        str::FromStr,
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

    fn create_test_request(
        headers: Vec<(&str, &str)>,
        body: &str,
        trailers: Vec<(&str, &str)>,
    ) -> Request<BodyWithMetrics<PolyBody>> {
        let mut req = Request::builder().method(Method::GET).uri("http://example.com/test").version(Version::HTTP_11);
        for (name, value) in headers {
            req = req.header(name, value);
        }

        let trailers_map = if !trailers.is_empty() {
            let mut map = http::header::HeaderMap::new();
            for (name, value) in trailers {
                map.append(
                    http::header::HeaderName::from_str(name).unwrap(),
                    http::header::HeaderValue::from_str(value).unwrap().clone(),
                );
            }
            map
        } else {
            http::HeaderMap::default()
        };

        let body = if body.is_empty() {
            if trailers_map.is_empty() {
                PolyBody::from(Empty::<bytes::Bytes>::new())
            } else {
                PolyBody::from(
                    Empty::<bytes::Bytes>::new().with_trailers(ready(Some(trailers_map).map(Ok::<_, Infallible>))),
                )
            }
        } else {
            if trailers_map.is_empty() {
                PolyBody::from(Full::new(bytes::Bytes::from(body.to_string())))
            } else {
                PolyBody::from(
                    Full::new(bytes::Bytes::from(body.to_string()))
                        .with_trailers(ready(Some(trailers_map).map(Ok::<_, Infallible>))),
                )
            }
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

    #[inline]
    fn create_header_mutation(headers: Vec<(&str, &str)>) -> Option<HeaderMutation> {
        if headers.is_empty() {
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
        }
    }

    #[inline]
    fn create_body_mutation(body_data: Option<Vec<u8>>, end_of_stream: Option<bool>) -> Option<BodyMutation> {
        if end_of_stream.is_some() {
            body_data.map(|body| BodyMutation {
                mutation: Some(Mutation::StreamedResponse(StreamedBodyResponse {
                    body,
                    end_of_stream: end_of_stream.unwrap(),
                })),
            })
        } else {
            body_data.map(|data| BodyMutation { mutation: Some(Mutation::Body(data)) })
        }
    }

    #[inline]
    fn create_trailer_mutation(trailers: Vec<(&str, &str)>) -> Option<HeaderMutation> {
        create_header_mutation(trailers)
    }

    fn create_headers_response(
        body_data: Option<Vec<u8>>,
        headers: Vec<(&str, &str)>,
        status: i32,
        end_of_stream: Option<bool>,
    ) -> ProcessingResponse {
        let header_mutation = create_header_mutation(headers);
        let body_mutation = create_body_mutation(body_data, end_of_stream);
        ProcessingResponse {
            response: Some(ProcessingResponseType::RequestHeaders(HeadersResponse {
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

    fn create_body_response(
        body_data: Option<Vec<u8>>,
        headers: Vec<(&str, &str)>,
        status: i32,
        end_of_stream: Option<bool>,
    ) -> ProcessingResponse {
        let header_mutation = create_header_mutation(headers);
        let body_mutation = create_body_mutation(body_data, end_of_stream);
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

    fn create_trailers_response(trailers: Vec<(&str, &str)>) -> ProcessingResponse {
        let trailer_mutation = create_trailer_mutation(trailers);
        ProcessingResponse {
            response: Some(ProcessingResponseType::RequestTrailers(TrailersResponse {
                header_mutation: trailer_mutation,
            })),
            mode_override: None,
            dynamic_metadata: None,
            override_message_timeout: None,
        }
    }

    static HEADER_PROCESSING_MODE: [HeaderProcessingMode; 3] =
        [HeaderProcessingMode::Default, HeaderProcessingMode::Send, HeaderProcessingMode::Skip];
    static BODY_PROCESSING_MODE: [BodyProcessingMode; 5] = [
        BodyProcessingMode::None,
        BodyProcessingMode::Streamed,
        BodyProcessingMode::Buffered,
        BodyProcessingMode::BufferedPartial,
        BodyProcessingMode::FullDuplexStreamed,
    ];
    static TRAILER_PROCESSING_MODE: [TrailerProcessingMode; 2] =
        [TrailerProcessingMode::Skip, TrailerProcessingMode::Send];

    fn generate_all_request_processing_mode_configurations() -> Vec<ProcessingMode> {
        HEADER_PROCESSING_MODE
            .iter()
            .flat_map(|&header_mode| {
                BODY_PROCESSING_MODE.iter().flat_map(move |&body_mode| {
                    TRAILER_PROCESSING_MODE.iter().map(move |&trailer_mode| ProcessingMode {
                        request_header_mode: header_mode,
                        request_body_mode: body_mode,
                        request_trailer_mode: trailer_mode,
                        response_header_mode: HeaderProcessingMode::Skip,
                        response_body_mode: BodyProcessingMode::None,
                        response_trailer_mode: TrailerProcessingMode::Skip,
                    })
                })
            })
            .collect()
    }

    fn generate_all_response_processing_mode_configurations() -> Vec<ProcessingMode> {
        HEADER_PROCESSING_MODE
            .iter()
            .flat_map(|&header_mode| {
                BODY_PROCESSING_MODE.iter().flat_map(move |&body_mode| {
                    TRAILER_PROCESSING_MODE.iter().map(move |&trailer_mode| ProcessingMode {
                        request_header_mode: HeaderProcessingMode::Skip,
                        request_body_mode: BodyProcessingMode::None,
                        request_trailer_mode: TrailerProcessingMode::Skip,
                        response_header_mode: header_mode,
                        response_body_mode: body_mode,
                        response_trailer_mode: trailer_mode,
                    })
                })
            })
            .collect()
    }

    #[tokio::test]
    async fn test_header_mutation() {
        let mock_state = MockExternalProcessorState::new().add_response(create_headers_response(
            None,
            vec![("x-processed", "true"), ("x-custom-header", "custom-value")],
            ResponseStatus::Continue as i32,
            None,
        ));
        let server_addr = start_mock_server(mock_state).await;
        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Send,
            request_body_mode: BodyProcessingMode::None,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let config = create_config_for_mock_server(server_addr, processing_mode, false);
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = create_test_request(vec![("content-type", "application/json")], "", vec![]);
        let result = ext_proc.apply_request(&mut request).await;

        assert!(matches!(result, FilterDecision::Continue));
        assert_eq!(request.headers().get("x-processed").unwrap(), "true");
        assert_eq!(request.headers().get("x-custom-header").unwrap(), "custom-value");
        assert_eq!(request.headers().get("content-type").unwrap(), "application/json");
    }

    #[tokio::test]
    async fn test_trailer_mutation() {
        let mock_state = MockExternalProcessorState::new().add_response(create_trailers_response(vec![
            ("x-processed", "true"),
            ("x-custom-trailer", "modified-value"),
        ]));
        let server_addr = start_mock_server(mock_state).await;
        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Send,
            request_body_mode: BodyProcessingMode::Buffered,
            request_trailer_mode: TrailerProcessingMode::Send,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let config = create_config_for_mock_server(server_addr, processing_mode, false);
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = create_test_request(
            vec![("content-type", "application/json")],
            "",
            vec![("x-custom-trailer", "original-value")],
        );

        let result = ext_proc.apply_request(&mut request).await;
        let trailers =
            std::mem::take(&mut request.body_mut().inner).collect().await.unwrap().trailers().map(|t| t.clone());

        assert!(trailers.is_some());
        assert!(matches!(result, FilterDecision::Continue));
        assert_eq!(request.headers().get("x-processed").unwrap(), "true");
        assert_eq!(trailers.unwrap().get("x-custom-trailer").unwrap(), "modified-value");
        assert_eq!(request.headers().get("content-type").unwrap(), "application/json");
    }

    #[tokio::test]
    async fn test_body_buffered_continue_and_replace_on_headers_response() {
        let new_body = "modified body content";
        let mock_state = MockExternalProcessorState::new().add_response(create_headers_response(
            Some(new_body.as_bytes().into()),
            vec![("y-custom-header", "true")],
            ResponseStatus::ContinueAndReplace as i32,
            None,
        ));
        let server_addr = start_mock_server(mock_state).await;
        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Send,
            request_body_mode: BodyProcessingMode::Buffered,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let config = create_config_for_mock_server(server_addr, processing_mode, false);
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = create_test_request(vec![], "original body", vec![]);
        let result = ext_proc.apply_request(&mut request).await;

        assert!(matches!(result, FilterDecision::Continue));
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.headers().get("y-custom-header").unwrap(), "true");
        let body_bytes = std::mem::take(&mut request.body_mut().inner).collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes, new_body.as_bytes());
    }

    #[tokio::test]
    async fn test_body_buffered_continue_and_replace_on_body_response() {
        let new_body = "modified body content";
        let mock_state = MockExternalProcessorState::new()
            .add_response(create_headers_response(None, vec![], ResponseStatus::Continue as i32, None))
            .add_response(create_body_response(
                Some(new_body.as_bytes().into()),
                vec![("y-custom-header", "true")],
                ResponseStatus::ContinueAndReplace as i32,
                None,
            ));
        let server_addr = start_mock_server(mock_state).await;
        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Send,
            request_body_mode: BodyProcessingMode::Buffered,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let config = create_config_for_mock_server(server_addr, processing_mode, false);
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = create_test_request(vec![], "original body", vec![]);
        let result = ext_proc.apply_request(&mut request).await;

        assert!(matches!(result, FilterDecision::Continue));
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.headers().get("y-custom-header").unwrap(), "true");
        let body_bytes = std::mem::take(&mut request.body_mut().inner).collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes, new_body.as_bytes());
    }

    #[tokio::test]
    async fn test_body_buffered_mode() {
        let mock_state = MockExternalProcessorState::new()
            .add_response(create_headers_response(
                None,
                vec![("x-stream-processed", "true"), ("y-custom-header", "true")],
                ResponseStatus::Continue as i32,
                None,
            ))
            .add_response(create_body_response(
                Some("body data from external processor".as_bytes().into()),
                vec![],
                ResponseStatus::Continue as i32,
                None,
            ));
        let server_addr = start_mock_server(mock_state).await;
        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Skip,
            request_body_mode: BodyProcessingMode::None,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let config = create_config_for_mock_server(server_addr, processing_mode, false);
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = create_test_request(vec![], "buffered body data", vec![]);
        let result = ext_proc.apply_request(&mut request).await;

        assert!(matches!(result, FilterDecision::Continue));
        assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");
        let body_bytes = std::mem::take(&mut request.body_mut().inner).collect().await.unwrap().to_bytes();
        assert_eq!(body_bytes, "body data from external processor".as_bytes());
    }

    #[tokio::test]
    async fn test_body_streaming_mode() {
        let mock_state = MockExternalProcessorState::new()
            .add_response(create_headers_response(
                None,
                vec![("x-stream-processed", "true"), ("y-custom-header", "true")],
                ResponseStatus::Continue as i32,
                // even though we will stream the body, no body modification is
                // perfomed in the headers response so no need to set the flag
                None,
            ))
            .add_response(create_body_response(
                Some("body data from external processor".as_bytes().into()),
                vec![],
                ResponseStatus::Continue as i32,
                Some(true),
            ));
        let server_addr = start_mock_server(mock_state).await;
        let processing_mode = ProcessingMode {
            request_header_mode: HeaderProcessingMode::Send,
            request_body_mode: BodyProcessingMode::Streamed,
            request_trailer_mode: TrailerProcessingMode::Skip,
            response_header_mode: HeaderProcessingMode::Skip,
            response_body_mode: BodyProcessingMode::None,
            response_trailer_mode: TrailerProcessingMode::Skip,
        };

        let config = create_config_for_mock_server(server_addr, processing_mode, false);
        let mut ext_proc = ExternalProcessor::from(config);

        let mut request = create_test_request(vec![], "streaming body data", vec![]);
        println!("request before filter: {request:?}");
        let result = ext_proc.apply_request(&mut request).await;
        println!("request after filter: {request:?}");

        assert!(matches!(result, FilterDecision::Continue));
        assert_eq!(request.headers().get("x-stream-processed").unwrap(), "true");
        let body_bytes = std::mem::take(&mut request.body_mut().inner).collect().await.unwrap().to_bytes();
        println!("body_bytes: {body_bytes:?}");
        assert_eq!(body_bytes, "body data from external processor".as_bytes());
    }

    //#[tokio::test]
    //async fn test_observability_mode() {
    //    let mock_state = MockExternalProcessorState::new();
    //    let server_addr = start_mock_server(mock_state).await;
    //    let processing_mode = ProcessingMode {
    //        request_header_mode: HeaderProcessingMode::Send,
    //        request_body_mode: BodyProcessingMode::None,
    //        response_header_mode: HeaderProcessingMode::Skip,
    //        response_body_mode: BodyProcessingMode::None,
    //        request_trailer_mode: TrailerProcessingMode::Skip,
    //        response_trailer_mode: TrailerProcessingMode::Skip,
    //    };

    //    let config = create_config_for_mock_server(server_addr, processing_mode, true);
    //    let mut ext_proc = ExternalProcessor::from(config);

    //    let mut request = create_test_request(vec![("original-header", "original-value")], "");
    //    let original_headers = request.headers().clone();
    //    let result = ext_proc.apply_request(&mut request).await;

    //    assert!(matches!(result, FilterDecision::Continue));
    //    assert_eq!(request.headers(), &original_headers);
    //    assert!(request.headers().get("x-should-not-apply").is_none());
    //}
}
