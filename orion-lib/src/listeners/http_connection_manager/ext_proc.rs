mod kind;
mod mutation;
mod r#override;
mod processing;
mod pseudo_header;
pub mod status;
#[cfg(test)]
mod tests;
mod worker_config;

use crate::body::channel_body::{BodyType, ChannelBody, FrameBridge};
use crate::body::timeout_body::TimeoutBody;
use crate::event_error::EventFailure;
use crate::listeners::http_connection_manager::ext_proc::kind::{MessageType, RequestMsg, ResponseMsg};
use crate::{OrionRequestBody, OrionResponseBody};
use http_body_util::{BodyExt, Collected, LengthLimitError, Limited};
#[cfg(feature = "metrics")]
use orion_metrics::metrics::custom::CUSTOM_METRICS;

use crate::listeners::http_connection_manager::ext_proc::mutation::{
    apply_request_header_mutations, apply_response_header_mutations,
};
use crate::listeners::http_connection_manager::ext_proc::processing::RequestProcessing;
use crate::listeners::http_connection_manager::ext_proc::processing::ResponseProcessing;
use crate::listeners::http_connection_manager::ext_proc::r#override::{OverridableBodyMode, OverridableGlobalModes};
use crate::listeners::http_connection_manager::ext_proc::status::ProcessingStatus;
use crate::listeners::http_connection_manager::ext_proc::status::ReadyStatus;
use crate::listeners::http_connection_manager::ext_proc::worker_config::ExternalProcessingWorkerConfig;
use crate::utils::truncated_debug::TruncatedDebug;
use crate::{
    body::response_flags::ResponseFlags,
    clusters::clusters_manager::{self, RoutingContext},
    listeners::{http_filters::FilterDecision, synthetic_http_response::SyntheticHttpResponse},
    Error, PolyBody,
};
use bytes::Bytes;
use const_str::parse;
use futures::{future::Either, StreamExt};
use http::header::CONTENT_LENGTH;
use http::{Request, Response, StatusCode};
use http_body::{Body, Frame};
use http_body_util::Full;
use orion_configuration::config::{
    cluster::ClusterSpecifier,
    network_filters::http_connection_manager::http_filters::{
        ext_proc::{
            ExternalProcessor as ExternalProcessorConfig, GrpcServiceSpecifier, HeaderForwardingRules, ProcessingMode,
        },
        ExtProcPerRoute,
    },
};
use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::core::v3::{HeaderMap as ProstHeaderMap, HeaderValue},
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
use scopeguard::defer;
use std::convert::Infallible;
use std::future::ready;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU32, Ordering};
use std::{sync::Arc, time::Duration};
use tokio::sync::mpsc::error::SendError;
use tokio::sync::{mpsc, oneshot, Semaphore};
use tracing::{debug, info, warn};

/// The total number of frames to prefetch before sending the request to the upstream service.
const CHANNEL_BODY_PREFETCH_FRAMES: NonZeroUsize = {
    let val = parse!(
        match option_env!("CHANNEL_BODY_PREFETCH_FRAMES") {
            Some(s) => s,
            None => "4",
        },
        usize
    );

    // Evaluates safely at compile time, panicking during the build if val is 0
    NonZeroUsize::new(val).expect("CHANNEL_BODY_PREFETCH_FRAMES must be greater than 0")
};

/// The maximum number of bytes to buffer in memory for the request body in buffered mode.
const EXT_PROC_BUFFERED_BODY_LIMIT: usize = parse!(
    match option_env!("EXT_PROC_BUFFERED_BODY_LIMIT") {
        Some(s) => s,
        None => "104857600", // 100 * 1024 * 1024
    },
    usize
);

/// The merge window for merging frames in buffered mode in microseconds.
const EXT_PROC_MERGE_WINDOW: Duration = Duration::from_micros(parse!(
    match option_env!("EXT_PROC_MERGE_WINDOW") {
        Some(s) => s,
        None => "100",
    },
    u64
));

/// The maximum number of frames to merge.
const EXT_PROC_FRAME_MERGE_LIMIT: u32 = parse!(
    match option_env!("EXT_PROC_FRAME_MERGE_LIMIT") {
        Some(v) => v,
        None => "4",
    },
    u32
);

/// The number of max concurrent `ext_proc` requests per core. This limits the number of concurrent requests to avoid
/// overloading the external processor and spawning too many tasks.
const EXT_PROC_MAX_CONCURRENT_REQUESTS: usize = parse!(
    match option_env!("EXT_PROC_MAX_CONCURRENT_REQUESTS") {
        Some(s) => s,
        None => "24",
    },
    usize
);

thread_local! {
    static EXT_PROC_CONCURRENT_PERMIT: Arc<Semaphore> = Arc::new(Semaphore::new(EXT_PROC_MAX_CONCURRENT_REQUESTS));
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum ExtProcError {
    #[error("Timeout Error: {0}")]
    Timeout(&'static str),
    #[error("Unsupported Response Type: {0}")]
    UnsupportedResponseType(String),
    #[error("Connection closed by remote GRPC")]
    UnexpectedEof,
    #[error("GRPC error: {0}")]
    GrpcError(String),
}

#[derive(Debug)]
pub struct ExternalProcessorInner {
    worker_config: ExternalProcessingWorkerConfig,
    forward_rules: Option<HeaderForwardingRules>,
}

#[derive(Debug, Clone)]
pub struct ExternalProcessor {
    ext_proc_worker: Option<mpsc::Sender<ProcessingTask>>,
    overridable_modes: Arc<OverridableGlobalModes>, // blueprint copy shared with with the worker.
    inner: Arc<ExternalProcessorInner>,             // shared with sessions.
}

// Extended External Processor Configuration, specific to Orion
#[derive(Debug, Clone)]
pub struct ExternalProcessorConfigExt {
    pub frame_merge_limit: u32,
    pub frame_merge_window: Duration,
}

impl From<ExternalProcessorConfig> for ExternalProcessor {
    fn from(initial_config: ExternalProcessorConfig) -> Self {
        debug!(target: "ext_proc", "From<ExternalProcessorConfig> for ExternalProcessor");
        Self::from((initial_config, None, None))
    }
}

impl From<(ExternalProcessorConfig, Option<ExtProcPerRoute>, Option<ExternalProcessorConfigExt>)>
    for ExternalProcessor
{
    fn from(
        (initial_config, per_route_config, ext_config): (
            ExternalProcessorConfig,
            Option<ExtProcPerRoute>,
            Option<ExternalProcessorConfigExt>,
        ),
    ) -> Self {
        debug!(target: "ext_proc", "From<ExternalProcessorConfig, Option<ExtProcPerRoute>, Option<ExternalProcessorConfigExt>> for ExternalProcessor");
        // dump the value of all variables configured via environment variables
        info!(target: "ext_proc", "const CHANNEL_BODY_PREFETCH_FRAMES: {CHANNEL_BODY_PREFETCH_FRAMES}");
        info!(target: "ext_proc", "const EXT_PROC_BUFFERED_BODY_LIMIT: {EXT_PROC_BUFFERED_BODY_LIMIT}");
        info!(target: "ext_proc", "const EXT_PROC_MERGE_WINDOW: {EXT_PROC_MERGE_WINDOW:?}");
        info!(target: "ext_proc", "const EXT_PROC_FRAME_MERGE_LIMIT: {EXT_PROC_FRAME_MERGE_LIMIT}");
        info!(target: "ext_proc", "const EXT_PROC_MAX_CONCURRENT_REQUESTS: {EXT_PROC_MAX_CONCURRENT_REQUESTS}");
        let forward_rules = initial_config.forward_rules.clone();
        let worker_config = ExternalProcessingWorkerConfig::from((initial_config, per_route_config, ext_config));
        let overridable_modes = Arc::new(OverridableGlobalModes::from(&worker_config));

        let inner = Arc::new(ExternalProcessorInner { worker_config, forward_rules });

        Self { ext_proc_worker: None, inner, overridable_modes }
    }
}

impl Drop for ExternalProcessor {
    fn drop(&mut self) {
        if let Some(sender) = self.ext_proc_worker.take() {
            debug!(target: "ext_proc", "ExternalProcessor::drop (closing sender)");
            drop(sender);
        }
    }
}

impl From<(ExternalProcessorConfig, Option<ExtProcPerRoute>, Option<ExternalProcessorConfigExt>)>
    for ExternalProcessingWorkerConfig
{
    fn from(
        (config, per_route_config, ext_proc_config_ext): (
            ExternalProcessorConfig,
            Option<ExtProcPerRoute>,
            Option<ExternalProcessorConfigExt>,
        ),
    ) -> Self {
        debug!(target: "ext_proc", "From<(ExternalProcessorConfig, Option<ExtProcPerRoute>, Option<ExternalProcessorConfigExt>)> for ExternalProcessingWorkerConfig");
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
        let max_receive_message_length = match &grpc_service.service_specifier {
            GrpcServiceSpecifier::Cluster(cluster_grpc) => {
                cluster_grpc.max_receive_message_length.map(|v| v as usize).unwrap_or(EXT_PROC_BUFFERED_BODY_LIMIT)
            },
            GrpcServiceSpecifier::GoogleGrpc(_) => EXT_PROC_BUFFERED_BODY_LIMIT,
        };

        Self {
            grpc_service_specifier: grpc_service.clone().service_specifier,
            message_timeout: config.message_timeout.unwrap_or(Duration::from_millis(200)),
            max_message_timeout: config.max_message_timeout,
            observability_mode: config.observability_mode,
            failure_mode_allow,
            disable_immediate_response: config.disable_immediate_response,
            mutation_rules: config.mutation_rules,
            processing_mode,
            allowed_override_modes: config.allowed_override_modes,
            allow_mode_override: config.allow_mode_override,
            route_cache_action: config.route_cache_action,
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
            frame_merge_limit: ext_proc_config_ext
                .as_ref()
                .map(|e| e.frame_merge_limit)
                .unwrap_or(EXT_PROC_FRAME_MERGE_LIMIT),
            frame_merge_window: ext_proc_config_ext
                .as_ref()
                .map(|e| e.frame_merge_window)
                .unwrap_or(EXT_PROC_MERGE_WINDOW),
            max_receive_message_length,
        }
    }
}

impl ExternalProcessor {
    // aggregate frames of Collected in a single buffer (frame), returning
    // as a Collected<Bytes> along with original trailers.
    async fn to_buffered(original: Collected<Bytes>) -> (Collected<Bytes>, usize) {
        let trailers = original.trailers().cloned().map(Ok::<_, Infallible>);
        let aggregated_bytes = original.to_bytes();
        let body_len = aggregated_bytes.len();
        // e is Infallible, the compiler is able to optimize that branch out.
        (
            Full::new(aggregated_bytes).with_trailers(ready(trailers)).collect().await.unwrap_or_else(|e| match e {}),
            body_len,
        )
    }

    #[allow(clippy::too_many_lines)]
    pub async fn apply_request_prepare_processing_data(
        &mut self,
        request: &mut Request<OrionRequestBody>,
    ) -> Result<ProcessingData, FilterDecision> {
        let modes = &self.overridable_modes.request;
        let process_headers = modes.should_process_headers();
        let process_body = modes.should_process_body();
        let process_trailers = modes.should_process_trailers();

        if !process_headers && !process_body && !process_trailers {
            return Err(FilterDecision::Continue);
        }

        let mut ext_proc_headers = None;

        if process_headers {
            debug!(target: "ext_proc", "request processing headers");
            let uri = request.uri();

            let mut headers_vec = Vec::with_capacity(request.headers().len() + 4);

            headers_vec.push(HeaderValue {
                key: pseudo_header::METHOD.to_owned(),
                value: request.method().as_str().to_owned(),
                raw_value: vec![],
            });
            if let Some(scheme) = uri.scheme() {
                headers_vec.push(HeaderValue {
                    key: pseudo_header::SCHEME.to_owned(),
                    value: scheme.as_str().to_owned(),
                    raw_value: vec![],
                });
            }
            if let Some(authority) = uri.authority() {
                headers_vec.push(HeaderValue {
                    key: pseudo_header::AUTHORITY.to_owned(),
                    value: authority.as_str().to_owned(),
                    raw_value: vec![],
                });
            }
            headers_vec.push(HeaderValue {
                key: pseudo_header::PATH.to_owned(),
                value: uri.path_and_query().map_or(uri.path(), |f| f.as_str()).to_owned(),
                raw_value: vec![],
            });

            for (name, value) in request.headers() {
                if self.should_forward_header(name.as_str()) {
                    let header_name = name.as_str();
                    let header_value = if let Ok(value_str) = value.to_str() {
                        HeaderValue { key: header_name.to_owned(), value: value_str.to_owned(), raw_value: Vec::new() }
                    } else {
                        HeaderValue {
                            key: header_name.to_owned(),
                            value: String::default(),
                            raw_value: value.as_bytes().into(),
                        }
                    };
                    headers_vec.push(header_value);
                }
            }

            ext_proc_headers = Some(EnvoyHeaderMap(ProstHeaderMap { headers: headers_vec }));
        }

        let body: PolyBody = std::mem::take(&mut request.body_mut().inner.inner);

        let ext_proc_frame_bridge = match (modes.body_mode(), modes.trailer_mode()) {
            (OverridableBodyMode::None, trailers_mode) => {
                // event though body processing is None and trailers processing is Skip, we have to
                // create the bridge, to allow ext_proc mutate the body with ContinueAndReplace action.
                debug!(target: "ext_proc", "request processing body(None) and trailers:{trailers_mode:?}");
                let (new_body, bridge) = ChannelBody::new(body, None, CHANNEL_BODY_PREFETCH_FRAMES);
                request.body_mut().inner.inner = PolyBody::from(new_body);
                bridge
            },
            (OverridableBodyMode::Streamed | OverridableBodyMode::FullDuplexStreamed, trailers_mode) => {
                debug!(target: "ext_proc", "request processing body(Streamed) and trailers:{trailers_mode:?}");
                let (new_body, bridge) = ChannelBody::new(body, None, CHANNEL_BODY_PREFETCH_FRAMES);
                request.body_mut().inner.inner = PolyBody::from(new_body);
                bridge
            },
            (OverridableBodyMode::Buffered | OverridableBodyMode::BufferedPartial, trailers_mode) => {
                debug!(target: "ext_proc", "request processing body(Buffered) with trailers:{trailers_mode:?}");
                if body.is_end_stream() {
                    let (new_body, bridge) =
                        ChannelBody::new(body, Some(BodyType::Empty), CHANNEL_BODY_PREFETCH_FRAMES);
                    request.body_mut().inner.inner = PolyBody::from(new_body);
                    bridge
                } else {
                    let body = Limited::new(body, self.inner.worker_config.max_receive_message_length);
                    let collected = match body.collect().await {
                        Ok(collected) => collected,
                        Err(e) => {
                            if e.downcast_ref::<LengthLimitError>().is_some() {
                                return Err(self.on_filter_error(
                                    &format!("Request body: {e}"),
                                    None,
                                    request.version(),
                                    Some(StatusCode::PAYLOAD_TOO_LARGE),
                                ));
                            }
                            return Err(self.on_filter_error(
                                &format!("Error collecting request body: {e}"),
                                None,
                                request.version(),
                                None,
                            ));
                        },
                    };

                    let (buffered, body_len) = Self::to_buffered(collected).await;
                    let has_trailers = buffered.trailers().is_some_and(|t| !t.is_empty());

                    let body_type = match (body_len > 0, has_trailers) {
                        (true, true) => BodyType::BodyAndTrailers,
                        (true, false) => BodyType::Body,
                        (false, true) => BodyType::Trailers,
                        (false, false) => BodyType::Empty,
                    };

                    let (new_body, bridge) = ChannelBody::new(buffered, Some(body_type), CHANNEL_BODY_PREFETCH_FRAMES);
                    request.body_mut().inner.inner = PolyBody::from(new_body);
                    bridge
                }
            },
        };

        debug!(target: "ext_proc", "request headers: {ext_proc_headers:?}");
        debug!(target: "ext_proc", "request body: {ext_proc_frame_bridge:?}");

        Ok(ProcessingData::Request(ext_proc_headers, ext_proc_frame_bridge))
    }

    fn apply_modification_on_request(
        &mut self,
        request: &mut Request<OrionRequestBody>,
        status: ProcessingStatus,
    ) -> FilterDecision {
        debug!(target: "ext_proc", "******************** apply modifications on request ********************");
        match status {
            ProcessingStatus::HaltedOnError => {
                debug!(target: "ext_proc", "apply_request: HaltedOnError...");
                FilterDecision::Continue
            },
            ProcessingStatus::EndWithDirectResponse(direct_response) => {
                debug!(target: "ext_proc", "apply_request: DirectResponse...");
                FilterDecision::DirectResponse(direct_response)
            },
            ProcessingStatus::RequestReady(ReadyStatus { clear_route_cache, headers_modifications }) => {
                debug!(target: "ext_proc", "apply_request: RequestReady Status{{ clear_route_cache:{clear_route_cache:?}, headers_modifications:{headers_modifications:?} }}...");
                if let Some(headers_modifications) = headers_modifications {
                    debug!(target: "ext_proc", "applying headers mutation: {headers_modifications:?}");
                    if let Err(e) = apply_request_header_mutations(
                        request,
                        headers_modifications,
                        self.inner.worker_config.mutation_rules.as_ref(),
                    ) {
                        return self.on_filter_error(
                            "Invalid header modifications received from external processor",
                            Some(e),
                            request.version(),
                            None,
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
            ProcessingStatus::ResponseReady(ReadyStatus { .. }) => {
                warn!(target: "ext_proc", "apply_request: unexpected ResponseReady!");
                self.on_filter_error(
                    "Unexpected ResponseReady status received during request processing",
                    None,
                    request.version(),
                    None,
                )
            },
        }
    }

    pub async fn apply_request(&mut self, request: &mut Request<OrionRequestBody>) -> FilterDecision {
        let processing_data = match self.apply_request_prepare_processing_data(request).await {
            Ok(data) => data,
            Err(decision) => return decision,
        };

        let ver = request.version();

        // Acquire permit before proceeding. This reduces the pressure on Tokio, reducing the number of tasks spawned.
        // Note: It's safe the call unwrap here, since the semaphore is never closed explicitly.
        #[allow(clippy::unwrap_used)]
        let _permit = EXT_PROC_CONCURRENT_PERMIT.with(Clone::clone).acquire_owned().await.unwrap();

        let Ok(response_rx) = self.send_processing_data(processing_data, ver).await else {
            if self.inner.worker_config.failure_mode_allow {
                return FilterDecision::Continue;
            }
            return self.on_filter_error(
                "Failed to schedule sending request data to external processor",
                None,
                ver,
                None,
            );
        };

        let res = match response_rx.await {
            Ok(status) => self.apply_modification_on_request(request, status),
            Err(e) => self.on_filter_error(
                format!("External processor: {e:?}").as_str(),
                Some(e.into()),
                request.version(),
                None,
            ),
        };

        #[cfg(feature = "metrics")]
        if let Some(custom_metrics) = CUSTOM_METRICS.get() {
            use crate::metrics;
            use opentelemetry::KeyValue;
            use orion_metrics::metrics::custom::MetricsHook;

            let mut attrs = smallvec::SmallVec::<[KeyValue; 2]>::new();
            if let Some(custom_keys) = metrics::CUSTOM_KEYS.get() {
                for key in custom_keys {
                    if let Some(source) = key.source() {
                        if let Some(id) = metrics::extract_custom_partition_key(request.headers(), Some(source)) {
                            attrs.push(KeyValue::new(key.attribute_name().unwrap_or("custom"), id));
                        }
                    }
                }
            }
            custom_metrics.with_headers(MetricsHook::ExtProcRequest, request.headers(), attrs.as_slice());
        }

        debug!(target: "ext_proc", "apply_request completed: {res:?}!");
        request.body_mut().inner.inner.prefetch_frames().await;
        res
    }

    #[allow(clippy::too_many_lines)]
    pub async fn apply_response_prepare_processing_data(
        &mut self,
        response: &mut Response<OrionResponseBody>,
    ) -> Result<ProcessingData, FilterDecision> {
        let modes = &self.overridable_modes.response;
        let process_headers = modes.should_process_headers();
        let process_body = modes.should_process_body();
        let process_trailers = modes.should_process_trailers();

        if !process_headers && !process_body && !process_trailers {
            return Err(FilterDecision::Continue);
        }

        let mut ext_proc_headers = None;

        if process_headers {
            debug!(target: "ext_proc", "response processing headers");

            let mut headers_vec = Vec::with_capacity(response.headers().len() + 1);
            headers_vec.push(HeaderValue {
                key: pseudo_header::STATUS.to_owned(),
                value: response.status().as_u16().to_string(),
                raw_value: vec![],
            });

            for (name, value) in response.headers() {
                if self.should_forward_header(name.as_str()) {
                    let header_name = name.as_str();
                    let header_value = if let Ok(value_str) = value.to_str() {
                        HeaderValue { key: header_name.to_owned(), value: value_str.to_owned(), raw_value: Vec::new() }
                    } else {
                        HeaderValue {
                            key: header_name.to_owned(),
                            value: String::default(),
                            raw_value: value.as_bytes().into(),
                        }
                    };
                    headers_vec.push(header_value);
                }
            }

            ext_proc_headers = Some(EnvoyHeaderMap(ProstHeaderMap { headers: headers_vec }));
        }

        let body: PolyBody = std::mem::take(&mut response.body_mut().inner);

        let ext_proc_frame_bridge = match (modes.body_mode(), modes.trailer_mode()) {
            (OverridableBodyMode::None, trailers_mode) => {
                // event though body processing is None and trailers processing is Skip, we have to
                // create the bridge, to allow ext_proc mutate the body with ContinueAndReplace action.
                debug!(target: "ext_proc", "response processing body(None) and trailers:{trailers_mode:?}");
                let (new_body, bridge) = ChannelBody::new(body, None, CHANNEL_BODY_PREFETCH_FRAMES);
                response.body_mut().inner = PolyBody::from(new_body);
                bridge
            },
            (OverridableBodyMode::Streamed | OverridableBodyMode::FullDuplexStreamed, trailers_mode) => {
                debug!(target: "ext_proc", "response processing body(Streamed) and trailers:{trailers_mode:?}");
                let (new_body, bridge) = ChannelBody::new(body, None, CHANNEL_BODY_PREFETCH_FRAMES);
                response.body_mut().inner = PolyBody::from(new_body);
                bridge
            },
            (OverridableBodyMode::Buffered | OverridableBodyMode::BufferedPartial, trailers_mode) => {
                debug!(target: "ext_proc", "response processing body(Buffered) with trailers:{trailers_mode:?}");
                if body.is_end_stream() {
                    let (new_body, bridge) =
                        ChannelBody::new(body, Some(BodyType::Empty), CHANNEL_BODY_PREFETCH_FRAMES);
                    response.body_mut().inner = PolyBody::from(new_body);
                    bridge
                } else {
                    let body = Limited::new(body, self.inner.worker_config.max_receive_message_length);
                    let collected = match body.collect().await {
                        Ok(collected) => collected,
                        Err(e) => {
                            if e.downcast_ref::<LengthLimitError>().is_some() {
                                return Err(self.on_filter_error(
                                    &format!("Response body: {e}"),
                                    None,
                                    response.version(),
                                    Some(StatusCode::PAYLOAD_TOO_LARGE),
                                ));
                            }
                            return Err(self.on_filter_error(
                                &format!("Error collecting response body: {e}"),
                                None,
                                response.version(),
                                None,
                            ));
                        },
                    };

                    let (buffered, body_len) = Self::to_buffered(collected).await;
                    let has_trailers = buffered.trailers().is_some_and(|t| !t.is_empty());

                    let body_type = match (body_len > 0, has_trailers) {
                        (true, true) => BodyType::BodyAndTrailers,
                        (true, false) => BodyType::Body,
                        (false, true) => BodyType::Trailers,
                        (false, false) => BodyType::Empty,
                    };

                    let (new_body, bridge) = ChannelBody::new(buffered, Some(body_type), CHANNEL_BODY_PREFETCH_FRAMES);
                    response.body_mut().inner = PolyBody::from(new_body);
                    bridge
                }
            },
        };

        debug!(target: "ext_proc", "response headers: {ext_proc_headers:?}");
        debug!(target: "ext_proc", "response body: {ext_proc_frame_bridge:?}");

        Ok(ProcessingData::Response(ext_proc_headers, ext_proc_frame_bridge))
    }

    fn apply_modification_on_response(
        &mut self,
        response: &mut Response<OrionResponseBody>,
        status: ProcessingStatus,
    ) -> FilterDecision {
        debug!(target: "ext_proc", "******************** apply modifications on response ********************");
        match status {
            ProcessingStatus::HaltedOnError => {
                debug!(target: "ext_proc", "apply_response: HaltedOnError...");
                FilterDecision::Continue
            },
            ProcessingStatus::EndWithDirectResponse(direct_response) => {
                debug!(target: "ext_proc", "apply_response: DirectResponse...");
                FilterDecision::DirectResponse(direct_response)
            },
            ProcessingStatus::ResponseReady(ReadyStatus { clear_route_cache, headers_modifications }) => {
                debug!(target: "ext_proc", "apply_request: ResponseReady Status{{ clear_route_cache:{clear_route_cache:?}, headers_modifications:{headers_modifications:?} }}...");
                if let Some(headers_modifications) = headers_modifications {
                    debug!(target: "ext_proc", "applying headers mutation: {headers_modifications:?}");
                    if let Err(e) = apply_response_header_mutations(
                        response,
                        headers_modifications,
                        self.inner.worker_config.mutation_rules.as_ref(),
                    ) {
                        return self.on_filter_error(
                            "Invalid header modifications received from external processor",
                            Some(e),
                            response.version(),
                            None,
                        );
                    }
                }

                response.headers_mut().remove(CONTENT_LENGTH);

                if clear_route_cache {
                    return FilterDecision::Reroute;
                }
                FilterDecision::Continue
            },
            ProcessingStatus::RequestReady(ReadyStatus { .. }) => {
                warn!(target: "ext_proc", "apply_response: unexpected RequestReady!");
                self.on_filter_error(
                    "Unexpected RequestReady status received during response processing",
                    None,
                    response.version(),
                    None,
                )
            },
        }
    }

    pub async fn apply_response(&mut self, response: &mut Response<OrionResponseBody>) -> FilterDecision {
        let processing_data = match self.apply_response_prepare_processing_data(response).await {
            Ok(data) => data,
            Err(decision) => return decision,
        };

        let ver = response.version();

        // Acquire permit before proceeding. This reduces the pressure on Tokio, reducing the number of tasks spawned.
        // Note: It's safe the call unwrap here, since the semaphore is never closed explicitly.
        #[allow(clippy::unwrap_used)]
        let _permit = EXT_PROC_CONCURRENT_PERMIT.with(Clone::clone).acquire_owned().await.unwrap();

        let Ok(response_rx) = self.send_processing_data(processing_data, ver).await else {
            if self.inner.worker_config.failure_mode_allow {
                return FilterDecision::Continue;
            }
            return self.on_filter_error(
                "Failed to schedule sending response data to external processor",
                None,
                ver,
                None,
            );
        };

        let res = match response_rx.await {
            Ok(status) => self.apply_modification_on_response(response, status),
            Err(e) => self.on_filter_error(
                format!("External processor response processing: {e:?}").as_str(),
                Some(e.into()),
                response.version(),
                None,
            ),
        };

        #[cfg(feature = "metrics")]
        if let Some(custom_metrics) = CUSTOM_METRICS.get() {
            use crate::metrics;
            use opentelemetry::KeyValue;
            use orion_metrics::metrics::custom::MetricsHook;

            let mut attrs = smallvec::SmallVec::<[KeyValue; 2]>::new();
            if let Some(custom_keys) = metrics::CUSTOM_KEYS.get() {
                for key in custom_keys {
                    if let Some(source) = key.source() {
                        if let Some(id) = metrics::extract_custom_partition_key(response.headers(), Some(source)) {
                            attrs.push(KeyValue::new(key.attribute_name().unwrap_or("custom"), id));
                        }
                    }
                }
            }
            custom_metrics.with_headers(MetricsHook::ExtProcResponse, response.headers(), attrs.as_slice());
        }

        debug!(target: "ext_proc", "apply_response completed: {res:?}!");

        // Delay sending the response until the first frame is ready. This ensures
        // better performance when streaming bodies from the external processor.
        // It works around a limitation in Tokio and Hyper, which perform poorly
        // when the response body is not immediately available. By waiting for
        // the first frame, single-frame responses avoid unnecessary polling
        // cycles.

        response.body_mut().inner.prefetch_frames().await;
        res
    }

    pub async fn send_processing_data(
        &mut self,
        data: ProcessingData,
        ver: http::Version,
    ) -> Result<oneshot::Receiver<ProcessingStatus>, SendError<ProcessingTask>> {
        let (response_tx, response_rx) = oneshot::channel();
        let processing_message = ProcessingTask { data, reply_channel: response_tx, http_version: ver };
        let worker_channel = self.get_worker_channel();
        worker_channel.send(processing_message).await?;
        Ok(response_rx)
    }

    pub fn on_filter_error(
        &mut self,
        msg: &str,
        error: Option<Error>,
        http_version: http::Version,
        status_code: Option<StatusCode>,
    ) -> FilterDecision {
        if let Some(err) = error {
            info!(target: "ext_proc","{msg}: {err}");
        } else {
            info!(target: "ext_proc", "{msg}");
        }
        if self.inner.worker_config.failure_mode_allow {
            FilterDecision::Continue
        } else {
            FilterDecision::DirectResponse(Box::new(
                SyntheticHttpResponse::custom_error(
                    status_code.unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                    Some(msg.to_owned().into()),
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE),
                )
                .into_response(http_version),
            ))
        }
    }

    fn get_worker_channel(&mut self) -> &mpsc::Sender<ProcessingTask> {
        if let Some(ref sender) = self.ext_proc_worker {
            return sender;
        }

        let (sender, receiver) = mpsc::channel::<ProcessingTask>(4);

        // replace the internal overridable modes blueprint with a new spawned instance for the worker
        //
        let overridable_modes = Arc::new(self.overridable_modes.spawn());
        self.overridable_modes = Arc::clone(&overridable_modes);

        if self.inner.worker_config.observability_mode {
            let worker =
                ExternalProcessingWorker::<kind::Observability>::new(Arc::clone(&self.inner), overridable_modes);
            tokio::spawn(worker.observability_loop(receiver));
        } else {
            let worker = ExternalProcessingWorker::<kind::Processing>::new(Arc::clone(&self.inner), overridable_modes);
            tokio::spawn(worker.processing_loop(receiver));
        }
        self.ext_proc_worker.insert(sender)
    }

    #[inline]
    fn should_forward_header(&self, header_name: &str) -> bool {
        if let Some(forward_rules) = &self.inner.forward_rules {
            if !forward_rules.disallowed_headers.is_empty() {
                return !forward_rules.disallowed_headers.iter().any(|m| m.matches(header_name));
            }
            if !forward_rules.allowed_headers.is_empty() {
                return forward_rules.allowed_headers.iter().any(|m| m.matches(header_name));
            }
        }
        true
    }
}

pub struct EnvoyHeaderMap(pub ProstHeaderMap);

impl std::fmt::Debug for EnvoyHeaderMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EnvoyHeaderMap").field("headers", &self.0.headers).finish()
    }
}

impl From<EnvoyHeaderMap> for http::HeaderMap {
    fn from(envoy_headers: EnvoyHeaderMap) -> Self {
        let mut headers = http::HeaderMap::with_capacity(envoy_headers.0.headers.len());

        for header in envoy_headers.0.headers {
            let Ok(header_name) = http::header::HeaderName::from_bytes(header.key.as_bytes()) else { continue };

            let header_value = if header.value.is_empty() {
                match http::header::HeaderValue::from_maybe_shared(header.raw_value) {
                    Ok(value) => value,
                    Err(_) => continue,
                }
            } else {
                match http::header::HeaderValue::from_maybe_shared(header.value) {
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
pub struct ProcessingTask {
    data: ProcessingData,
    reply_channel: oneshot::Sender<ProcessingStatus>,
    http_version: http::Version,
}

#[derive(Debug)]
pub enum ProcessingData {
    Request(Option<EnvoyHeaderMap>, FrameBridge),
    Response(Option<EnvoyHeaderMap>, FrameBridge),
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
    inner: Arc<ExternalProcessorInner>,
    bidi_stream: Option<BidiStream>,
    request_processing: RequestProcessing<S>,
    response_processing: ResponseProcessing<S>,
    handshake: Option<ProtocolConfiguration>,
    timeout_state: TimeoutState,
    overridable_modes: Arc<OverridableGlobalModes>,
}

#[inline]
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

#[allow(dead_code)]
enum MergeResult {
    Retry(u32),
    Error(u32),
    None(u32),
}

static TOTAL_WORKERS: AtomicU32 = AtomicU32::new(0);

impl ExternalProcessingWorker<kind::Processing> {
    fn new(inner: Arc<ExternalProcessorInner>, overridable_global_modes: Arc<OverridableGlobalModes>) -> Self {
        let request_processing = RequestProcessing::<kind::Processing>::from(&inner.worker_config);
        let response_processing = ResponseProcessing::<kind::Processing>::from(&inner.worker_config);
        let handshake = Some(ProtocolConfiguration {
            request_body_mode: inner.worker_config.processing_mode.request_body_mode as i32,
            response_body_mode: inner.worker_config.processing_mode.response_body_mode as i32,
            send_body_without_waiting_for_header_response: inner
                .worker_config
                .send_body_without_waiting_for_header_response,
        });
        let message_timeout = inner.worker_config.message_timeout;
        Self {
            inner,
            bidi_stream: None,
            request_processing,
            response_processing,
            handshake,
            timeout_state: TimeoutState { active: false, duration: message_timeout, extended: false },
            overridable_modes: overridable_global_modes,
        }
    }

    async fn recover_or_failure(&mut self, err: ExtProcError, log_msg: &str) {
        let proof_request = self.request_processing.make_proof().unwrap_or_else(|| {
            if let ExtProcError::Timeout(_) = err {
                let status = self.request_processing.status_timeout(self.inner.worker_config.failure_mode_allow);
                self.request_processing.return_status(status, "recover_or_failure: timeout")
            } else {
                let status = self.request_processing.status_error(log_msg, self.inner.worker_config.failure_mode_allow);
                self.request_processing.return_status(status, "recover_or_failure: error")
            }
        });

        let proof_response = self.response_processing.make_proof().unwrap_or_else(|| {
            if let ExtProcError::Timeout(_) = err {
                let status = self.response_processing.status_timeout(self.inner.worker_config.failure_mode_allow);
                self.response_processing.return_status(status, "recover_or_failure: timeout")
            } else {
                let status =
                    self.response_processing.status_error(log_msg, self.inner.worker_config.failure_mode_allow);
                self.response_processing.return_status(status, "recover_or_failure: error")
            }
        });

        if self.inner.worker_config.failure_mode_allow {
            info!(target: "ext_proc", "{} - continue (failure_mode_allow is true)", log_msg);
            let frames = std::mem::take(&mut self.request_processing.inflight_frames);
            let trailers = std::mem::take(&mut self.request_processing.parked_trailers);
            self.request_processing
                .frame_bridge
                .drain_and_close(proof_request, frames, trailers, Some(&mut self.timeout_state.active))
                .await;
            let frames = std::mem::take(&mut self.response_processing.inflight_frames);
            let trailers = std::mem::take(&mut self.response_processing.parked_trailers);
            self.response_processing
                .frame_bridge
                .drain_and_close(proof_response, frames, trailers, Some(&mut self.timeout_state.active))
                .await;
        } else {
            info!(target: "ext_proc", "{} - abort (failure_mode_allow is false)", log_msg);
            _ = self.request_processing.frame_bridge.inject_frame(Err(Box::new(err.clone())), proof_request).await;
            self.request_processing.frame_bridge.close(Some(&mut self.timeout_state.active));
            _ = self.response_processing.frame_bridge.inject_frame(Err(Box::new(err.clone())), proof_response).await;
            self.response_processing.frame_bridge.close(Some(&mut self.timeout_state.active));
        }

        self.request_processing.set_streaming_body(false);
        self.response_processing.set_streaming_body(false);
    }

    #[allow(clippy::too_many_lines)]
    async fn processing_loop(mut self, mut processing_request_channel: mpsc::Receiver<ProcessingTask>) {
        debug!(target: "ext_proc", "===== BEGIN ({}) =====", TOTAL_WORKERS.fetch_add(1, Ordering::Relaxed)+1);

        defer! {
            debug!(target: "ext_proc", "===== END ({}) =====", TOTAL_WORKERS.fetch_sub(1, Ordering::Relaxed)-1);
        }

        let mut request_body_to_ext_proc_complete = false;
        let mut response_body_to_ext_proc_complete = false;

        'transaction_loop: loop {
            let streaming_enabled =
                self.request_processing.streaming_body_enabled() || self.response_processing.streaming_body_enabled();
            let outbound_req_enabled =
                self.request_processing.streaming_body_enabled() && !request_body_to_ext_proc_complete;
            let outbound_resp_enabled =
                self.response_processing.streaming_body_enabled() && !response_body_to_ext_proc_complete;

            debug!(target: "ext_proc", "----- select! [ process_task:{} inbound:true outbound_req:{outbound_req_enabled} outbound_resp:{outbound_resp_enabled} timeout:{} ] -----",
                !streaming_enabled, self.timeout_state.active);

            tokio::select! {
                outbound_processing_request = processing_request_channel.recv(), if !streaming_enabled => {
                    match outbound_processing_request {
                        Some(ProcessingTask{ data: ProcessingData::Request(headers, frame_bridge), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "-> starting processing request...");
                            if let Some(proc_req) = self.request_processing.process(headers, frame_bridge, reply_channel, http_version, &self.overridable_modes).await {
                                debug!(target: "ext_proc", "processing_request -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                self.forward_to_external_processor(proc_req).await;
                            }
                        }
                        Some(ProcessingTask{ data: ProcessingData::Response(headers, frame_bridge), reply_channel, http_version}) => {
                            debug!(target: "ext_proc", "-> starting processing response...");
                            if let Some(proc_req) = self.response_processing.process(headers, frame_bridge, reply_channel, http_version, &self.overridable_modes).await {
                                debug!(target: "ext_proc", "processing_response -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                self.forward_to_external_processor(proc_req).await;
                            }
                        }
                        _ => {
                            debug!(target: "ext_proc", ">> worker channel closed!");
                            break 'transaction_loop
                        },
                    }
                },

                inbound_processing_response = if let Some(stream) = self.bidi_stream.as_mut() {
                        Either::Left(stream.inbound_responses.message())
                    } else {
                        Either::Right(std::future::pending::<Result<Option<ProcessingResponse>, Status>>())
                    } => {

                    debug!(target: "ext_proc", "<= inbound processing response: {inbound_processing_response:?}");

                    match inbound_processing_response {
                        Ok(Some(ProcessingResponse { override_message_timeout: Some(extended_timeout), ..})) => {
                            debug!(target: "ext_proc", "<- timeout extension received: {extended_timeout:?}");
                            if !self.handle_timeout_extension(extended_timeout) {
                                debug!(target: "ext_proc", "invalid timeout extension - closing stream");
                                break 'transaction_loop;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ImmediateResponse(mut response_attempt)), ..})) => {
                            debug!(target: "ext_proc", "<- ImmediateResponse received");

                            if self.inner.worker_config.disable_immediate_response { // disabled
                                if self.inner.worker_config.failure_mode_allow {
                                    // simply continue with the current processing, either with request or response...

                                    if self.request_processing.frame_bridge.is_open() {
                                        let proof = self.request_processing.make_proof().unwrap_or_else(|| {
                                            self.request_processing.return_status(ProcessingStatus::ready::<RequestMsg>(), "disabled immediate response, with failure mode allowed")
                                        });

                                        self.request_processing.frame_bridge.drain_and_inject(proof).await;
                                        self.request_processing.frame_bridge.close(None);
                                    } else if self.response_processing.frame_bridge.is_open() {
                                        let proof = self.response_processing.make_proof().unwrap_or_else(|| {
                                            self.response_processing.return_status(ProcessingStatus::ready::<ResponseMsg>(), "disabled immediate response, with failure mode allowed")
                                        });

                                        self.response_processing.frame_bridge.drain_and_inject(proof).await;
                                        self.response_processing.frame_bridge.close(None);
                                    }
                                } else {
                                    // failure, immediate response is disable. return an error and/or abort the body processing
                                    let proof_request = self.request_processing.make_proof().unwrap_or_else(|| {
                                        let status = self.request_processing.status_internal_error("ext_proc returned an immediate response despite being disabled by configuration");
                                        self.request_processing.return_status(status, "immediate_response_disabled")
                                    });

                                    let proof_response = self.response_processing.make_proof().unwrap_or_else(|| {
                                        let status = self.response_processing.status_internal_error("ext_proc returned an immediate response despite being disabled by configuration");
                                        self.response_processing.return_status(status, "immediate_response_disabled")
                                    });

                                    _ = self.request_processing.frame_bridge.inject_frame(Err(Box::new(ExtProcError::Timeout("immediate response disabled"))), proof_request).await;
                                    _ = self.response_processing.frame_bridge.inject_frame(Err(Box::new(ExtProcError::Timeout("immediate response disabled"))), proof_response).await;
                                }

                            } else { // enabled
                                match self.can_handle_immediate_response() {
                                    Some(MessageType::Request) => {
                                        let direct_response = self.build_direct_response(&mut response_attempt);
                                        let proof = self.request_processing.make_proof().unwrap_or_else(|| {
                                            let status = ProcessingStatus::EndWithDirectResponse(Box::new(direct_response));
                                            self.request_processing.return_status(status, "immediate_response on request")
                                        });

                                        let frames = std::mem::take(&mut self.request_processing.inflight_frames);
                                        let trailers = std::mem::take(&mut self.request_processing.parked_trailers);
                                        self.request_processing.frame_bridge.drain_and_close(proof, frames, trailers, Some(&mut self.timeout_state.active)).await;
                                        self.request_processing.set_streaming_body(false);
                                    }
                                    Some(MessageType::Response) => {
                                        let direct_response = self.build_direct_response(&mut response_attempt);
                                        let proof = self.response_processing.make_proof().unwrap_or_else(|| {
                                            let status = ProcessingStatus::EndWithDirectResponse(Box::new(direct_response));
                                            self.response_processing.return_status(status, "immediate_response on response")
                                        });

                                        let frames = std::mem::take(&mut self.response_processing.inflight_frames);
                                        let trailers = std::mem::take(&mut self.request_processing.parked_trailers);
                                        self.response_processing.frame_bridge.drain_and_close(proof, frames, trailers, Some(&mut self.timeout_state.active)).await;
                                        self.response_processing.set_streaming_body(false);
                                    }
                                    None => {
                                        // immediate response can no longer be handled.
                                        // let's return an error if processing channel is still available and stop processing
                                        // request/response

                                        let _proof_request = self.request_processing.make_proof().unwrap_or_else(|| {
                                            self.request_processing.return_status(ProcessingStatus::HaltedOnError, "immediate_response (failure)")
                                        });

                                        let _proof_response = self.response_processing.make_proof().unwrap_or_else(|| {
                                            self.response_processing.return_status(ProcessingStatus::HaltedOnError, "immediate_response (failure)")
                                        });

                                        self.request_processing.frame_bridge.close(Some(&mut self.timeout_state.active));
                                        self.response_processing.frame_bridge.close(Some(&mut self.timeout_state.active));

                                        self.request_processing.set_streaming_body(false);
                                        self.response_processing.set_streaming_body(false);
                                    }
                                }
                            }

                            break 'transaction_loop;
                        },
                        Ok(Some(ProcessingResponse { mode_override, response: Some(ProcessingResponseType::RequestHeaders(headers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- RequestHeaders response received");
                            if self.inner.worker_config.allow_mode_override {
                                if let Some(overrides) = mode_override {
                                    self.request_processing.apply_mode_overrides(&overrides, &self.inner.worker_config.allowed_override_modes, &self.overridable_modes);
                                    self.response_processing.apply_mode_overrides(&overrides, &self.inner.worker_config.allowed_override_modes, &self.overridable_modes);
                                }
                            }

                            let proc_req = self.request_processing.handle_headers_response(
                                headers_response,
                                &self.inner.worker_config.route_cache_action,
                                &self.overridable_modes,
                                &mut self.timeout_state.active,
                            ).await;

                            if let Some(proc_req) = proc_req {
                                debug!(target: "ext_proc", "handle_headers_response -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                self.forward_to_external_processor(proc_req).await;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::RequestBody(body_response)), ..})) => {
                            debug!(target: "ext_proc", "<- RequestBody response received");

                            let proc_req = self
                                .request_processing
                                .handle_body_response(body_response, Some(&self.inner.worker_config.route_cache_action), &mut self.timeout_state.active).await;

                            if let Some(proc_req) = proc_req {
                                debug!(target: "ext_proc", "handle_body_response -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                 self.forward_to_external_processor(proc_req).await;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::RequestTrailers(trailers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- RequestTrailers response received");
                            if let Some(proc_req) = self.request_processing.handle_trailers_response(trailers_response, &mut self.timeout_state.active).await {
                                debug!(target: "ext_proc", "handle_trailers_response -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                self.forward_to_external_processor(proc_req).await;
                            }
                        },
                        Ok(Some(ProcessingResponse { mode_override, response: Some(ProcessingResponseType::ResponseHeaders(headers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- ResponseHeaders response received");
                            if self.inner.worker_config.allow_mode_override {
                                if let Some(overrides) = mode_override {
                                    self.response_processing.apply_mode_overrides(&overrides, &self.inner.worker_config.allowed_override_modes, &self.overridable_modes);
                                }
                            }

                            let proc_req = self.response_processing.handle_headers_response(
                                headers_response,
                                &self.inner.worker_config.route_cache_action,
                                &self.overridable_modes,
                                &mut self.timeout_state.active,
                            ).await;

                            if let Some(proc_req) = proc_req {
                                debug!(target: "ext_proc", "handle_headers_response -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                self.forward_to_external_processor(proc_req).await;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ResponseBody(body_response)), ..})) => {
                            debug!(target: "ext_proc", "<- ResponseBody response received");
                            let empty_response = body_response.response.is_none();

                            if let Some(proc_req) = self.response_processing.handle_body_response(body_response, None, &mut self.timeout_state.active).await {
                                debug!(target: "ext_proc", "handle_body_response -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                self.forward_to_external_processor(proc_req).await;
                            }

                            if empty_response {
                                debug!(target: "ext_proc", "response body response contained no response - closing stream");
                                break 'transaction_loop;
                            }
                        },
                        Ok(Some(ProcessingResponse { response: Some(ProcessingResponseType::ResponseTrailers(trailers_response)), ..})) => {
                            debug!(target: "ext_proc", "<- ResponseTrailers response received");
                            if let Some(proc_req) = self.response_processing.handle_trailers_response(trailers_response, &mut self.timeout_state.active).await {
                                debug!(target: "ext_proc", "handle_trailers_response -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                self.forward_to_external_processor(proc_req).await;
                            }
                        },
                        Ok(Some(r)) => {
                            let msg = format!("unsupported response message received: {r:?}");
                            self.recover_or_failure(ExtProcError::UnsupportedResponseType(format!("{r:?}")), &msg).await;
                            break 'transaction_loop;
                        }
                        Ok(None) => {
                            let msg = "stream closed by the external processor";
                            self.recover_or_failure(ExtProcError::UnexpectedEof, msg).await;
                            break 'transaction_loop;
                        },
                        Err(e) => {
                            let msg = format!("gRPC error received from external processor: {}", e.message());
                            self.recover_or_failure(ExtProcError::GrpcError(e.message().into()), &msg).await;
                            break 'transaction_loop;
                        }
                    }
                },

                outbound_request_body_frame = self.request_processing.frame_bridge.next(), if outbound_req_enabled => {
                    debug!(target: "ext_proc", "outbound request body frame: {:?}", TruncatedDebug::<_,1024>(&outbound_request_body_frame));
                    match outbound_request_body_frame {
                        Some(Ok(frame)) => {
                            let body_mode = self.overridable_modes.request.body_mode();

                            debug!(target: "ext_proc", "outbound request body frame: buffering frame...");
                            if let Some(frame_to_send) = self.request_processing.frames_buffer.push(frame, tokio::time::Instant::now()) {
                                // invariant: frame_to_send is always a DATA frame at this point. TRAILERS are sent later.
                                if let OverridableBodyMode::None = body_mode { // body processing is disabled, just inject back the frame
                                    let proof = self.request_processing.make_proof().unwrap_or_else(|| {
                                        let status = ProcessingStatus::RequestReady(ReadyStatus::default());
                                        self.request_processing.return_status(status, "proof for frame injection when body processing is disabled")
                                    });

                                    debug!(target: "ext_proc", "outbound request body frame: injecting the frame DATA into the body");
                                    _ = self.request_processing.frame_bridge.inject_frame(Ok(frame_to_send), proof).await;
                                } else { // send the merged frame to ext_proc and park a copy for later injection
                                    debug!(target: "ext_proc", "outbound request body frame: sending body chunk of request ({})",  if frame_to_send.is_data() { "DATA" } else { "TRAILERS" });
                                    if let Some(proc_req) = self.request_processing.handle_outgoing_body_chunk(clone_frame(&frame_to_send), false) {
                                        debug!(target: "ext_proc", "handle_outgoing_body_chunk -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                        self.forward_to_external_processor(proc_req).await;
                                    }
                                    // save a copy of the frame to inject into the body bridge later
                                    self.request_processing.inflight_frames.push(frame_to_send);
                                }
                            }
                        },
                        Some(Err(_err)) => {
                            request_body_to_ext_proc_complete = true;
                            debug!(target: "ext_proc", "outbound request body frame: error occurred when streaming request body to external processing");
                            self.request_processing.status_error("error occurred when streaming request body to external processing", self.inner.worker_config.failure_mode_allow);
                        },
                        None => {
                            request_body_to_ext_proc_complete = true;
                            debug!(target: "ext_proc", "outbound request body frame: request body stream completing...");

                            while let Some(last_frame) = self.request_processing.frames_buffer.take() {
                                if last_frame.is_data() { // DATA
                                  if self.overridable_modes.request.should_process_body() {
                                      debug!(target: "ext_proc", "outbound request body frame: sending the last body chunk of request");
                                      let end_of_stream = !self.request_processing.frames_buffer.has_trailers();
                                      if let Some(proc_req) = self.request_processing.handle_outgoing_body_chunk(clone_frame(&last_frame), end_of_stream) {
                                        debug!(target: "ext_proc", "handle_outgoing_body_chunk -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                        self.forward_to_external_processor(proc_req).await;
                                      }
                                      // save a copy of the frame to inject into the body bridge later
                                      self.request_processing.inflight_frames.push(last_frame);
                                  } else {
                                      debug!(target: "ext_proc", "outbound request body frame: injecting the last body chunk of request into the body");
                                      let proof = self.request_processing.make_proof().unwrap_or_else(|| {
                                          let status = ProcessingStatus::RequestReady(ReadyStatus::default());
                                          self.request_processing.return_status(status, "proof for frame injection at end of stream when body processing is disabled")
                                      });

                                      _ = self.request_processing.frame_bridge.inject_frame(Ok(last_frame), proof).await;
                                  }
                                } else { // TRAILERS
                                    if self.overridable_modes.request.should_process_trailers() {
                                        debug!(target: "ext_proc", "outbound request body frame: sending the last body chunk of request");
                                        if let Some(proc_req) = self.request_processing.handle_outgoing_body_chunk(clone_frame(&last_frame), true) {
                                            debug!(target: "ext_proc", "handle_outgoing_body_chunk (last frame sent) -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                            self.forward_to_external_processor(proc_req).await;
                                        }
                                        // save a copy of the frame to inject into the body bridge later
                                        self.request_processing.inflight_frames.push(last_frame);
                                    } else {
                                        debug!(target: "ext_proc", "outbound request body frame: parking the trailers chunk to avoid out-of-order delivery");
                                        self.request_processing.parked_trailers = Some(last_frame);
                                    }
                                }
                            }


                            if self.request_processing.inflight_frames.is_empty() {
                                let proof = self.request_processing.make_proof().unwrap_or_else(|| {
                                    let status = ProcessingStatus::RequestReady(ReadyStatus::default());
                                    self.request_processing.return_status(status, "proof for frame injection when inflight frames are empty")
                                });

                                debug!(target: "ext_proc", "outbound request body frame: frame bridge closed (request body)!");
                                let trailers = std::mem::take(&mut self.request_processing.parked_trailers);
                                self.request_processing.frame_bridge.drain_and_close(proof, std::iter::empty(), trailers, Some(&mut self.timeout_state.active)).await;
                            }
                            self.request_processing.set_streaming_body(false);
                        }
                    }
                },

                outbound_response_body_frame = self.response_processing.frame_bridge.next(), if outbound_resp_enabled => {
                    debug!(target: "ext_proc", "outbound response body frame: {:?}", TruncatedDebug::<_,1024>(&outbound_response_body_frame));
                    match outbound_response_body_frame {
                        Some(Ok(frame)) => {
                            let body_mode = self.overridable_modes.response.body_mode();

                            debug!(target: "ext_proc", "outbound response body frame: buffering frame...");
                            if let Some(frame_to_send) = self.response_processing.frames_buffer.push(frame, tokio::time::Instant::now()) {
                                // invariant: frame_to_send is always a DATA frame at this point. TRAILERS are sent later.
                                if let OverridableBodyMode::None = body_mode { // body processing is disabled, just inject back the frame
                                    debug!(target: "ext_proc", "outbound response body frame: injecting the frame DATA into the body");
                                    let proof = self.response_processing.make_proof().unwrap_or_else(|| {
                                        let status = ProcessingStatus::ResponseReady(ReadyStatus::default());
                                        self.response_processing.return_status(status, "proof for frame injection when body processing is disabled")
                                    });
                                    _ = self.response_processing.frame_bridge.inject_frame(Ok(frame_to_send), proof).await;
                                } else { // send the merged frame to ext_proc and park a copy for later injection
                                    debug!(target: "ext_proc", "outbound response body frame: sending body chunk of response ({})",  if frame_to_send.is_data() { "DATA" } else { "TRAILERS" });
                                    if let Some(proc_req) = self.response_processing.handle_outgoing_body_chunk(clone_frame(&frame_to_send), false) {
                                        debug!(target: "ext_proc", "handle_outgoing_body_chunk (merged frame sent) -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                        self.forward_to_external_processor(proc_req).await;
                                    }
                                    // save a copy of the frame to inject into the body bridge later
                                    self.response_processing.inflight_frames.push(frame_to_send);
                                }
                            }
                        },
                        Some(Err(_err)) => {
                            response_body_to_ext_proc_complete = true;
                            debug!(target: "ext_proc", "outbound response body frame: error occurred when streaming response body to external processing");
                            self.response_processing.status_error("outbound response body frame: error occurred when streaming response body to external processing", self.inner.worker_config.failure_mode_allow);
                        },
                        None => {
                            response_body_to_ext_proc_complete = true;
                            debug!(target: "ext_proc", "outbound response body frame: response body stream completing...");

                            while let Some(last_frame) = self.response_processing.frames_buffer.take() {
                                if last_frame.is_data() { // DATA
                                  if self.overridable_modes.response.should_process_body() {
                                      debug!(target: "ext_proc", "outbound response body frame: sending the last body chunk of response");
                                      let end_of_stream = !self.response_processing.frames_buffer.has_trailers();
                                      if let Some(proc_req) = self.response_processing.handle_outgoing_body_chunk(clone_frame(&last_frame), end_of_stream) {
                                        debug!(target: "ext_proc", "handle_outgoing_body_chunk (last frame sent) -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                        self.forward_to_external_processor(proc_req).await;
                                      }
                                      // save a copy of the frame to inject into the body bridge later
                                      self.response_processing.inflight_frames.push(last_frame);
                                  } else {
                                      debug!(target: "ext_proc", "outbound response body frame: injecting the last body chunk of response into the body");
                                      let proof = self.response_processing.make_proof().unwrap_or_else(|| {
                                          let status = ProcessingStatus::ResponseReady(ReadyStatus::default());
                                          self.response_processing.return_status(status, "proof for frame injection at end of stream")
                                      });

                                      _ = self.response_processing.frame_bridge.inject_frame(Ok(last_frame), proof).await;
                                  }
                                } else { // TRAILERS
                                    if self.overridable_modes.response.should_process_trailers() {
                                        debug!(target: "ext_proc", "outbound response body frame: sending the last body chunk of response");
                                        if let Some(proc_req) = self.response_processing.handle_outgoing_body_chunk(clone_frame(&last_frame), true) {
                                            debug!(target: "ext_proc", "handle_outgoing_body_chunk (last frame sent) -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                            self.forward_to_external_processor(proc_req).await;
                                        }
                                        // save a copy of the frame to inject into the body bridge later
                                        self.response_processing.inflight_frames.push(last_frame);
                                    } else {
                                        debug!(target: "ext_proc", "outbound response body frame: parking the trailers chunk to avoid out-of-order delivery");
                                        self.response_processing.parked_trailers = Some(last_frame);
                                    }
                                }
                            }


                            if self.response_processing.inflight_frames.is_empty() {
                                debug!(target: "ext_proc", "outbound response body frame: frame bridge closed (response body)!");
                                let proof = self.response_processing.make_proof().unwrap_or_else(|| {
                                    let status = ProcessingStatus::ResponseReady(ReadyStatus::default());
                                    self.response_processing.return_status(status, "proof for frame injection at end of stream when inflight frames are empty")
                                });

                                let trailers = std::mem::take(&mut self.response_processing.parked_trailers);
                                self.response_processing.frame_bridge.drain_and_close(proof, std::iter::empty(), trailers, Some(&mut self.timeout_state.active)).await;
                            }
                            self.response_processing.set_streaming_body(false);
                        }
                    }
                },

                () = fast_timeout::fast_sleep(self.timeout_state.duration), if self.timeout_state.active => {
                    let msg = "processing_loop: message timeout";
                    self.recover_or_failure(ExtProcError::Timeout("message timeout"), msg).await;
                    break 'transaction_loop;
                }
            }
        }
    }
}

impl ExternalProcessingWorker<kind::Observability> {
    fn new(inner: Arc<ExternalProcessorInner>, overridable_global_modes: Arc<OverridableGlobalModes>) -> Self {
        let request_processing = RequestProcessing::<kind::Observability>::from(&inner.worker_config);
        let response_processing = ResponseProcessing::<kind::Observability>::from(&inner.worker_config);

        let handshake = Some(ProtocolConfiguration {
            request_body_mode: inner.worker_config.processing_mode.request_body_mode as i32,
            response_body_mode: inner.worker_config.processing_mode.response_body_mode as i32,
            send_body_without_waiting_for_header_response: inner
                .worker_config
                .send_body_without_waiting_for_header_response,
        });
        let message_timeout = inner.worker_config.message_timeout;
        Self {
            inner,
            bidi_stream: None,
            request_processing,
            response_processing,
            handshake,
            timeout_state: TimeoutState { active: false, duration: message_timeout, extended: false },
            overridable_modes: overridable_global_modes,
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn observability_loop(mut self, mut processing_request_channel: mpsc::Receiver<ProcessingTask>) {
        debug!(target: "ext_proc", "===== Observability BEGIN =====");
        defer! {
            debug!(target: "ext_proc", "===== Observability END =====");
        }

        let mut request_body_to_ext_proc_complete = false;
        let mut response_body_to_ext_proc_complete = false;

        'transaction_loop: loop {
            let streaming_enabled =
                self.request_processing.streaming_body_enabled() || self.response_processing.streaming_body_enabled();
            let outbound_req_enabled =
                self.request_processing.streaming_body_enabled() && !request_body_to_ext_proc_complete;
            let outbound_resp_enabled =
                self.response_processing.streaming_body_enabled() && !response_body_to_ext_proc_complete;

            debug!(target: "ext_proc", "----- select! [ process_task:{} inbound:true outbound_req:{outbound_req_enabled} outbound_resp:{outbound_resp_enabled} timeout:{} ] -----",
                !streaming_enabled, self.timeout_state.active);

            tokio::select! {
                outbound_processing_request = processing_request_channel.recv(), if !streaming_enabled => {
                    debug!(target: "ext_proc", "processing {outbound_processing_request:?}...");
                    match outbound_processing_request {
                        Some(ProcessingTask{ data: ProcessingData::Request(headers, frame_bridge), reply_channel, http_version}) => {
                            if let Some(proc_req) = self.request_processing.process(headers, frame_bridge, reply_channel, http_version, &self.overridable_modes).await {
                                debug!(target: "ext_proc", "processing_request -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                               self.forward_to_external_processor(proc_req).await;
                            }
                        }
                        Some(ProcessingTask{ data: ProcessingData::Response(headers, frame_bridge), reply_channel, http_version}) => {
                            if let Some(proc_req) = self.response_processing.process(headers, frame_bridge, reply_channel, http_version, &self.overridable_modes).await {
                                debug!(target: "ext_proc", "processing_response -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                self.forward_to_external_processor(proc_req).await;
                            }
                        }
                        _ => {
                            debug!(target: "ext_proc", ">> worker channel closed!");
                            break 'transaction_loop
                        },
                    }
                },

                outbound_request_body_frame = self.request_processing.frame_bridge.next(), if outbound_req_enabled => {
                    debug!(target: "ext_proc", "outbound request body frame: {:?}", TruncatedDebug::<_,1024>(&outbound_request_body_frame));
                    match outbound_request_body_frame {
                        Some(Ok(frame)) => {
                            debug!(target: "ext_proc", "outbound request body frame: buffering frame...");
                            if let Some(frame_to_send) = self.request_processing.frames_buffer.push(frame, tokio::time::Instant::now()) {

                                let proof = self.request_processing.make_proof().unwrap_or_else(|| {
                                    let status = ProcessingStatus::RequestReady(ReadyStatus::default());
                                    self.request_processing.return_status(status, "proof for frame injection when body processing is enabled")
                                });

                                if self.overridable_modes.request.should_process_body() {
                                    _ = self.request_processing.frame_bridge.inject_frame(Ok(clone_frame(&frame_to_send)), proof).await;
                                    debug!(target: "ext_proc", "outbound request body frame: sending body chunk of request ({})",  if frame_to_send.is_data() { "DATA" } else { "TRAILERS" });
                                    if let Some(proc_req) = self.request_processing.handle_outgoing_body_chunk(frame_to_send, false) {
                                        debug!(target: "ext_proc", "handle_outgoing_body_chunk (merged frame sent) -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                        self.forward_to_external_processor(proc_req).await;
                                    }
                                } else {
                                    debug!(target: "ext_proc", "outbound request body frame: injecting the frame DATA into the body");
                                    _ = self.request_processing.frame_bridge.inject_frame(Ok(frame_to_send), proof).await;
                                }
                            }
                        },
                        Some(Err(_err)) => {
                            request_body_to_ext_proc_complete = true;
                            debug!(target: "ext_proc", "error occurred when streaming request body to external processing");
                            self.request_processing.status_error("error occurred when streaming request body to external processing", self.inner.worker_config.failure_mode_allow);
                        },
                        None => {
                            request_body_to_ext_proc_complete = true;
                            debug!(target: "ext_proc", "request body stream completing...");

                            let proof = self.request_processing.make_proof().unwrap_or_else(|| {
                                let status = ProcessingStatus::RequestReady(ReadyStatus::default());
                                self.request_processing.return_status(status, "proof for frame injection at end of stream when processing body chunks")
                            });

                            while let Some(last_frame) = self.request_processing.frames_buffer.take() {
                                if last_frame.is_data() { // DATA
                                  if self.overridable_modes.request.should_process_body() {
                                      debug!(target: "ext_proc", "sending the last data chunk of request");
                                      _ = self.request_processing.frame_bridge.inject_frame(Ok(clone_frame(&last_frame)), proof).await;
                                      let end_of_stream = !self.request_processing.frames_buffer.has_trailers();
                                      if let Some(proc_req) = self.request_processing.handle_outgoing_body_chunk(last_frame, end_of_stream) {
                                        debug!(target: "ext_proc", "handle_outgoing_body_chunk (last data frame sent) -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                        self.forward_to_external_processor(proc_req).await;
                                      }
                                  } else {
                                      debug!(target: "ext_proc", "injecting the last data chunk of request into the body");
                                      _ = self.request_processing.frame_bridge.inject_frame(Ok(last_frame), proof).await;
                                  }
                                } else { // TRAILERS
                                    if self.overridable_modes.request.should_process_trailers() {
                                        debug!(target: "ext_proc", "sending trailers chunk of request");
                                        _ = self.request_processing.frame_bridge.inject_frame(Ok(clone_frame(&last_frame)), proof).await;
                                        if let Some(proc_req) = self.request_processing.handle_outgoing_body_chunk(last_frame, true) {
                                            debug!(target: "ext_proc", "handle_outgoing_body_chunk (trailer frame sent) -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                            self.forward_to_external_processor(proc_req).await;
                                        }
                                    } else {
                                        debug!(target: "ext_proc", "injecting the trailers chunk of request into the body");
                                        _ = self.request_processing.frame_bridge.inject_frame(Ok(last_frame), proof).await;
                                    }
                                }
                            }

                            let proof = self.request_processing.make_proof().unwrap_or_else(|| {
                                let status = ProcessingStatus::RequestReady(ReadyStatus::default());
                                self.request_processing.return_status(status, "request_streaming_completed")
                            });

                            let trailers = std::mem::take(&mut self.request_processing.parked_trailers);
                            self.request_processing.frame_bridge.drain_and_close(proof, std::iter::empty(), trailers, Some(&mut self.timeout_state.active)).await;
                            self.request_processing.set_streaming_body(false);
                        }
                    }
                },

                outbound_response_body_frame = self.response_processing.frame_bridge.next(), if outbound_resp_enabled => {
                    debug!(target: "ext_proc", "outbound response body frame: {:?}", TruncatedDebug::<_,1024>(&outbound_response_body_frame));
                    match outbound_response_body_frame {
                        Some(Ok(frame)) => {
                            debug!(target: "ext_proc", "outbound response body frame: buffering frame...");
                            if let Some(frame_to_send) = self.response_processing.frames_buffer.push(frame, tokio::time::Instant::now()) {
                                let proof = self.response_processing.make_proof().unwrap_or_else(|| {
                                    let status = ProcessingStatus::ResponseReady(ReadyStatus::default());
                                    self.response_processing.return_status(status, "proof for frame injection when body processing is enabled")
                                });

                                if self.overridable_modes.response.should_process_body() {
                                    _ = self.response_processing.frame_bridge.inject_frame(Ok(clone_frame(&frame_to_send)), proof).await;
                                    debug!(target: "ext_proc", "outbound response body frame: sending body chunk of response ({})",  if frame_to_send.is_data() { "DATA" } else { "TRAILERS" });
                                    if let Some(proc_req) = self.response_processing.handle_outgoing_body_chunk(frame_to_send, false) {
                                        debug!(target: "ext_proc", "handle_outgoing_body_chunk (merged frame sent) -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                        self.forward_to_external_processor(proc_req).await;
                                    }
                                } else {
                                    debug!(target: "ext_proc", "outbound response body frame: injecting the frame DATA into the body");
                                    _ = self.response_processing.frame_bridge.inject_frame(Ok(frame_to_send), proof).await;
                                }
                            }
                        },
                        Some(Err(_err)) => {
                            response_body_to_ext_proc_complete = true;
                            debug!(target: "ext_proc", "error occurred when streaming response body to external processing");
                            self.response_processing.status_error("error occurred when streaming response body to external processing", self.inner.worker_config.failure_mode_allow);
                        },
                        None => {
                            response_body_to_ext_proc_complete = true;
                            debug!(target: "ext_proc", "response body stream completing...");

                            let proof = self.response_processing.make_proof().unwrap_or_else(|| {
                                let status = ProcessingStatus::ResponseReady(ReadyStatus::default());
                                self.response_processing.return_status(status, "proof for frame injection at end of stream when processing body chunks")
                            });

                            while let Some(last_frame) = self.response_processing.frames_buffer.take() {
                                if last_frame.is_data() { // DATA
                                  if self.overridable_modes.response.should_process_body() {
                                      debug!(target: "ext_proc", "sending the last data chunk of response");
                                      _ = self.response_processing.frame_bridge.inject_frame(Ok(clone_frame(&last_frame)), proof).await;
                                      let end_of_stream = !self.response_processing.frames_buffer.has_trailers();
                                      if let Some(proc_req) = self.response_processing.handle_outgoing_body_chunk(last_frame, end_of_stream) {
                                        debug!(target: "ext_proc", "handle_outgoing_body_chunk (last data frame sent) -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                        self.forward_to_external_processor(proc_req).await;
                                      }
                                  } else {
                                      debug!(target: "ext_proc", "injecting the last data chunk of response into the body");
                                      _ = self.response_processing.frame_bridge.inject_frame(Ok(last_frame), proof).await;
                                  }
                                } else { // TRAILERS
                                    if self.overridable_modes.response.should_process_trailers() {
                                        debug!(target: "ext_proc", "sending trailers chunk of response");
                                        _ = self.response_processing.frame_bridge.inject_frame(Ok(clone_frame(&last_frame)), proof).await;
                                        if let Some(proc_req) = self.response_processing.handle_outgoing_body_chunk(last_frame, true) {
                                            debug!(target: "ext_proc", "handle_outgoing_body_chunk (trailer frame sent) -> forward {outbound:?}", outbound = TruncatedDebug::<_,1024>(&proc_req));
                                            self.forward_to_external_processor(proc_req).await;
                                        }
                                    } else {
                                        debug!(target: "ext_proc", "injecting the trailers chunk of response into the body");
                                        _ = self.response_processing.frame_bridge.inject_frame(Ok(last_frame), proof).await;
                                    }
                                }
                            }

                            let trailers = std::mem::take(&mut self.response_processing.parked_trailers);
                            self.response_processing.frame_bridge.drain_and_close(proof, std::iter::empty(), trailers, Some(&mut self.timeout_state.active)).await;
                            self.response_processing.set_streaming_body(false);
                        }
                    }
                },

                () = std::future::ready(()), if streaming_enabled && !outbound_req_enabled && !outbound_resp_enabled => {
                    debug!(target: "ext_proc", "no outbound request or response enabled (observability terminating...)");
                    break 'transaction_loop;
                }
            }
        }
    }
}

impl<S: kind::Mode + Default> ExternalProcessingWorker<S> {
    async fn connect(
        grpc_service_specifier: &GrpcServiceSpecifier,
        max_receive_message_length: usize,
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
            GrpcServiceSpecifier::Cluster(cluster_grpc) => {
                let cluster_spec = ClusterSpecifier::Cluster(cluster_grpc.cluster_name.clone());
                let cluster_id = clusters_manager::resolve_cluster(&cluster_spec, None).ok_or_else(|| {
                    Error::from(format!(
                        "Failed to resolve cluster '{}' for external processor",
                        cluster_grpc.cluster_name
                    ))
                })?;
                let grpc_service = clusters_manager::get_grpc_connection(cluster_id, RoutingContext::None)?;
                let mut client =
                    ExternalProcessorClient::new(grpc_service).max_decoding_message_size(max_receive_message_length);

                client
                    .process(request_stream)
                    .await
                    .map_err(|e| Error::from(format!("Failed to establish external processor stream: {e}")))?
                    .into_inner()
            },
            GrpcServiceSpecifier::GoogleGrpc(google_grpc) => {
                let mut client = ExternalProcessorClient::connect(google_grpc.target_uri.clone())
                    .await
                    .map_err(|e| {
                        Error::from(format!("Failed to connect to external processor (GoogleGrpc endpoint): {e}"))
                    })?
                    .max_decoding_message_size(max_receive_message_length);

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
            let stream = Self::connect(
                &self.inner.worker_config.grpc_service_specifier,
                self.inner.worker_config.max_receive_message_length,
                first_request,
            )
            .await?;
            Ok(self.bidi_stream.insert(stream))
        }
    }

    async fn forward_to_external_processor(&mut self, mut request: ProcessingRequest) {
        request.protocol_config = self.handshake.take();
        let mut request_opt = Some(request);

        let stream = match self.get_bidi_stream(&mut request_opt).await {
            Err(err) => {
                self.response_processing.status_error(
                    format!("External processor: {err:?}").as_str(),
                    self.inner.worker_config.failure_mode_allow,
                );
                self.request_processing.status_error(
                    format!("External processor: {err:?}").as_str(),
                    self.inner.worker_config.failure_mode_allow,
                );
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
                info!(target: "ext_proc", "External processor is unavailable: {err}");
                if let Some(reply_channel) = self.response_processing.reply_channel.take() {
                    let _ = reply_channel
                        .send(self.response_processing.status_error(
                            "Lost connection to external processor",
                            self.inner.worker_config.failure_mode_allow,
                        ))
                        .ok();
                }

                if let Some(reply_channel) = self.request_processing.reply_channel.take() {
                    let _ = reply_channel
                        .send(self.request_processing.status_error(
                            "Lost connection to external processor",
                            self.inner.worker_config.failure_mode_allow,
                        ))
                        .ok();
                }

                self.timeout_state.active = false;
            },
            _ => {
                // enable timeout, if not in observability mode
                self.timeout_state.active = !self.inner.worker_config.observability_mode;
            },
        }
    }

    #[allow(clippy::cast_sign_loss)]
    fn handle_timeout_extension(&mut self, extended_timeout: google::protobuf::Duration) -> bool {
        if self.timeout_state.extended {
            let msg = "External processor attempted multiple timeout extensions";
            if let Some(reply_channel) = self.response_processing.reply_channel.take() {
                let _ = reply_channel
                    .send(self.response_processing.status_error(msg, self.inner.worker_config.failure_mode_allow))
                    .ok();
            }

            if let Some(reply_channel) = self.request_processing.reply_channel.take() {
                let _ = reply_channel
                    .send(self.request_processing.status_error(msg, self.inner.worker_config.failure_mode_allow))
                    .ok();
            }

            return false;
        }
        let mut timeout_duration =
            Duration::from_secs(extended_timeout.seconds as u64) + Duration::from_nanos(extended_timeout.nanos as u64);
        if timeout_duration < Duration::from_millis(1) {
            info!(target:"ext_proc", "External processor: override_message_timeout must be >= 1ms");
            timeout_duration = self.inner.worker_config.message_timeout;
        }
        if let Some(max_timeout) = self.inner.worker_config.max_message_timeout {
            if timeout_duration > max_timeout {
                info!(target:"ext_proc", "External processor: attempted to override message timeout to value > max_message_timeout (defaulting to max_message_timeout)");
                timeout_duration = max_timeout;
            }
        }
        self.timeout_state.duration = timeout_duration;
        self.timeout_state.extended = true;
        true
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn build_direct_response(&mut self, response_attempt: &mut ImmediateResponse) -> Response<OrionResponseBody> {
        let status = response_attempt
            .status
            .as_ref()
            .and_then(|s| http::StatusCode::from_u16(s.code as u16).ok())
            .unwrap_or(http::StatusCode::OK);
        let body_bytes = Bytes::from(std::mem::take(&mut response_attempt.body));
        let body = Full::new(body_bytes);
        let mut response = Response::new(TimeoutBody::new(None, PolyBody::from(body)));
        *response.status_mut() = status;
        if let Some(header_mutation) = response_attempt.headers.take() {
            let _ = apply_response_header_mutations(
                &mut response,
                header_mutation,
                self.inner.worker_config.mutation_rules.as_ref(),
            )
            .ok();
        }
        if let Some(grpc_status) = &response_attempt.grpc_status {
            if let Ok(status_value) = http::HeaderValue::from_str(&grpc_status.status.to_string()) {
                response.headers_mut().insert("grpc-status", status_value);
            }
        }
        response
    }

    fn can_handle_immediate_response(&self) -> Option<MessageType> {
        if self.request_processing.reply_channel.is_some() {
            return Some(MessageType::Request);
        }

        if self.response_processing.reply_channel.is_some() {
            return Some(MessageType::Response);
        }

        None
    }
}
