use crate::body::channel_body::FrameBridge;
use crate::event_error::EventFailure;
use crate::listeners::http_connection_manager::ext_proc::mutation::apply_trailer_mutations;
use crate::listeners::http_connection_manager::ext_proc::pseudo_header::CombinedHeaderMap;
use crate::listeners::http_connection_manager::ext_proc::r#override::{
    OverridableBodyMode, OverridableGlobalModes, OverridableModeSelector,
};
use crate::listeners::http_connection_manager::ext_proc::status::{Action, ProcessingStatus, ReadyStatus};
use crate::listeners::http_connection_manager::ext_proc::worker_config::ExternalProcessingWorkerConfig;
use crate::listeners::http_connection_manager::ext_proc::EnvoyHeaderMap;
use crate::listeners::http_connection_manager::ext_proc::{kind, r#override};
use crate::utils::truncated_debug::TruncatedDebug;
use crate::{body::response_flags::ResponseFlags, listeners::synthetic_http_response::SyntheticHttpResponse};
use bytes::{Bytes, BytesMut};
use http_body::Frame;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    BodyProcessingMode, HeaderProcessingMode, ProcessingMode, RouteCacheAction, TrailerProcessingMode,
};
use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::common_response::ResponseStatus;
use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::{HeaderMutation, HttpTrailers};
use orion_data_plane_api::envoy_data_plane_api::envoy::{
    extensions::filters::http::ext_proc::v3::ProcessingMode as EnvoyProcessingMode,
    service::ext_proc::v3::{
        body_mutation::Mutation, processing_request::Request as ProcessingRequestType, BodyResponse, HeadersResponse,
        HttpBody, HttpHeaders, ProcessingRequest, TrailersResponse,
    },
};

use orion_format::types::ResponseFlags as FmtResponseFlags;
use smallvec::SmallVec;
use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{debug, warn};

pub struct RequestProcessing<M: kind::Mode>(Processing<M, kind::RequestMsg>);

impl<M: kind::Mode> Deref for RequestProcessing<M> {
    type Target = Processing<M, kind::RequestMsg>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<M: kind::Mode> DerefMut for RequestProcessing<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct ResponseProcessing<M: kind::Mode>(Processing<M, kind::ResponseMsg>);

impl<M: kind::Mode> Deref for ResponseProcessing<M> {
    type Target = Processing<M, kind::ResponseMsg>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<M: kind::Mode> DerefMut for ResponseProcessing<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct FramesBuffer {
    data_buffer: Option<BytesMut>,
    trailers_buffer: Option<Frame<Bytes>>,
    last_merge: Option<Instant>,
    count: u32,
    frame_merge_limit: u32,
    frame_merge_window: Duration,
}

pub enum Phase {
    Headers,
    Body,
    Trailers,
}

impl FramesBuffer {
    fn new(frame_merge_limit: u32, frame_merge_window: Duration) -> Self {
        Self {
            data_buffer: None,
            trailers_buffer: None,
            count: 0,
            last_merge: None,
            frame_merge_limit,
            frame_merge_window,
        }
    }

    // merge can either return a DATA frame or None
    pub fn push(&mut self, frame: Frame<Bytes>, now: tokio::time::Instant) -> Option<Frame<Bytes>> {
        if let Some(new_data) = frame.data_ref() {
            // DATA
            if self.trailers_buffer.is_some() {
                // This case should never occur, as no frames are expected after the final TRAILERS.
                warn!(target: "ext_proc", "FramesBuffer::merge_frame: unexpected frame after TRAILERS!");
            } else if let Some(buf) = self.data_buffer.as_mut() {
                self.count += 1;
                buf.extend_from_slice(new_data.as_ref());
            } else {
                self.count = 1;
                self.data_buffer = Some(BytesMut::from(new_data.as_ref()));
            }

            let emit = self.count >= self.frame_merge_limit
                || now.duration_since(self.last_merge.unwrap_or(now)) >= self.frame_merge_window;

            self.last_merge = Some(now);

            if emit {
                self.count = 0;
                self.data_buffer.take().map(|buf| Frame::data(buf.freeze()))
            } else {
                None
            }
        } else {
            // TRAILERS
            let data = self.data_buffer.take().map(|buf| Frame::data(buf.freeze()));
            self.count = 0;
            self.last_merge = Some(now);
            self.trailers_buffer = Some(frame);
            data
        }
    }

    // take can either return a DATA or TRAILERS frame
    pub fn take(&mut self) -> Option<Frame<Bytes>> {
        self.count = 0;
        self.last_merge = None;
        if let Some(buf) = self.data_buffer.take() {
            Some(Frame::data(buf.freeze()))
        } else {
            self.trailers_buffer.take()
        }
    }

    #[inline]
    #[allow(unused)]
    pub fn has_data(&self) -> bool {
        self.data_buffer.is_some()
    }

    #[inline]
    #[allow(unused)]
    pub fn has_trailers(&self) -> bool {
        self.trailers_buffer.is_some()
    }
}

#[allow(clippy::struct_excessive_bools)]
pub struct Processing<M: kind::Mode, Msg: kind::MsgKind> {
    http_headers: Option<CombinedHeaderMap>,
    pub trailers: Option<http::HeaderMap>,
    pub frame_bridge: FrameBridge,
    pub reply_channel: Option<oneshot::Sender<ProcessingStatus>>,
    http_version: Option<http::Version>,
    send_body_without_waiting_for_header_response: bool,
    pub failure_mode_allow: bool,
    streaming_body_enabled: bool,
    pub end_of_stream: bool,
    pub frames_buffer: FramesBuffer,
    pub inflight_frames: SmallVec<[Frame<Bytes>; 2]>,
    pub parked_trailers: Option<Frame<Bytes>>,
    pub headers_ready_status: Option<ReadyStatus>, // ready_status saved headers response and fused with body response before in Action::Return
    _mode: std::marker::PhantomData<M>,
    _msg: std::marker::PhantomData<Msg>,
}

impl<M: kind::Mode + Default, Msg: kind::MsgKind> From<&ExternalProcessingWorkerConfig> for Processing<M, Msg> {
    fn from(config: &ExternalProcessingWorkerConfig) -> Self {
        debug!(target: "ext_proc", "From<&ExternalProcessingWorkerConfig for Processing<>");

        Self {
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
            failure_mode_allow: config.failure_mode_allow,

            http_headers: None,
            frame_bridge: FrameBridge::default(),
            trailers: None,
            reply_channel: None,
            http_version: None,
            streaming_body_enabled: false,
            end_of_stream: false,
            frames_buffer: FramesBuffer::new(config.frame_merge_limit, config.frame_merge_window),
            inflight_frames: SmallVec::new(),
            parked_trailers: None,
            headers_ready_status: None,
            _mode: std::marker::PhantomData,
            _msg: std::marker::PhantomData,
        }
    }
}

impl<M: kind::Mode + Default> From<&ExternalProcessingWorkerConfig> for RequestProcessing<M> {
    fn from(value: &ExternalProcessingWorkerConfig) -> Self {
        debug!(target: "ext_proc", "From<&ExternalProcessingWorkerConfig for RequestProcessing<{}>", std::any::type_name::<M>());
        Self(Processing::<M, kind::RequestMsg>::from(value))
    }
}

impl<M: kind::Mode + Default> From<&ExternalProcessingWorkerConfig> for ResponseProcessing<M> {
    fn from(value: &ExternalProcessingWorkerConfig) -> Self {
        debug!(target: "ext_proc", "From<&ExternalProcessingWorkerConfig for ResponseProcessing<{}>", std::any::type_name::<M>());
        Self(Processing::<M, kind::ResponseMsg>::from(value))
    }
}

impl Processing<kind::Processing, kind::RequestMsg> {
    #[allow(clippy::unused_self)]
    pub fn apply_mode_overrides(
        &self,
        envoy_mode: &EnvoyProcessingMode,
        allowed_override_modes: &[ProcessingMode],
        override_mode: &OverridableGlobalModes,
    ) {
        // --- Body Mode Override ---
        if let Ok(mode) = BodyProcessingMode::try_from(envoy_mode.request_body_mode) {
            if allowed_override_modes.is_empty()
                || allowed_override_modes.iter().any(|allowed| allowed.request_body_mode == mode)
            {
                override_mode.set_body_mode::<kind::RequestMsg>(mode);
            }
        }

        // --- Trailer Mode Override ---
        if let Ok(mode) = TrailerProcessingMode::try_from(envoy_mode.request_trailer_mode) {
            if mode != TrailerProcessingMode::Default
                && (allowed_override_modes.is_empty()
                    || allowed_override_modes.iter().any(|allowed| allowed.request_trailer_mode == mode))
            {
                override_mode.set_trailer_mode::<kind::RequestMsg>(mode);
            }
        }
    }
}

impl Processing<kind::Processing, kind::ResponseMsg> {
    #[allow(clippy::unused_self)]
    pub fn apply_mode_overrides(
        &self,
        envoy_mode: &EnvoyProcessingMode,
        allowed_override_modes: &[ProcessingMode],
        override_mode: &OverridableGlobalModes,
    ) {
        // --- Header Mode Override ---
        if let Ok(mode) = HeaderProcessingMode::try_from(envoy_mode.response_header_mode) {
            if mode != HeaderProcessingMode::Default
                && (allowed_override_modes.is_empty()
                    || allowed_override_modes.iter().any(|allowed| allowed.response_header_mode == mode))
            {
                override_mode.set_header_mode::<kind::ResponseMsg>(mode);
            }
        }

        // --- Body Mode Override ---
        if let Ok(mode) = BodyProcessingMode::try_from(envoy_mode.response_body_mode) {
            if allowed_override_modes.is_empty()
                || allowed_override_modes.iter().any(|allowed| allowed.response_body_mode == mode)
            {
                override_mode.set_body_mode::<kind::ResponseMsg>(mode);
            }
        }

        // --- Trailer Mode Override ---
        if let Ok(mode) = TrailerProcessingMode::try_from(envoy_mode.response_trailer_mode) {
            if mode != TrailerProcessingMode::Default
                && (allowed_override_modes.is_empty()
                    || allowed_override_modes.iter().any(|allowed| allowed.response_trailer_mode == mode))
            {
                override_mode.set_trailer_mode::<kind::ResponseMsg>(mode);
            }
        }
    }
}

impl<Msg: kind::MsgKind + OverridableModeSelector> Processing<kind::Processing, Msg> {
    #[must_use = "must handle the returned Action"]
    pub async fn handle_headers_response(
        &mut self,
        response: HeadersResponse,
        route_cache_action: &RouteCacheAction,
        override_mode: &OverridableGlobalModes,
        timeout_active: &mut bool,
    ) -> Action<ProcessingRequest> {
        let mut status = None;

        if let Some(response_data) = response.response {
            let should_clear_route_cache = match route_cache_action {
                RouteCacheAction::Clear => true,
                RouteCacheAction::Retain => false,
                RouteCacheAction::Default => response_data.clear_route_cache,
            };

            debug!(target: "ext_proc", "handle_headers_response: non_empty_body:{:?}, has_pending_trailers:{:?}",
                    self.frame_bridge.source_has_non_empty_body(),
                    self.frame_bridge.source_has_pending_trailers());

            let embedded_status = ResponseStatus::try_from(response_data.status).unwrap_or(ResponseStatus::Continue);
            if matches!(
                override_mode.body_mode::<Msg>(),
                OverridableBodyMode::Buffered | OverridableBodyMode::BufferedPartial
            ) && override_mode.should_process_body::<Msg>()
                && self.frame_bridge.source_has_non_empty_body() == Some(true)
                && !matches!(embedded_status, ResponseStatus::ContinueAndReplace)
            {
                debug!(target: "ext_proc", "handle_headers_response: parking ReadyStatus to return when the response on body is received");
                // let's park the ReadyStatus to delay the Action::Ready and to fuse later
                //  with the ReadyStatus in the handle_body_response.
                self.headers_ready_status = Some(ReadyStatus {
                    headers_modifications: response_data.header_mutation,
                    clear_route_cache: should_clear_route_cache,
                });
            } else {
                status = if Msg::IS_REQUEST {
                    Some(ProcessingStatus::RequestReady(ReadyStatus {
                        headers_modifications: response_data.header_mutation,
                        clear_route_cache: should_clear_route_cache,
                    }))
                } else {
                    Some(ProcessingStatus::ResponseReady(ReadyStatus {
                        headers_modifications: response_data.header_mutation,
                        clear_route_cache: should_clear_route_cache,
                    }))
                };
                debug!(target: "ext_proc", "handle_headers_response: return status {status:?}");
            }

            if matches!(embedded_status, ResponseStatus::ContinueAndReplace) {
                debug!(target: "ext_proc", "handle_headers_response: ResponseStatus:ContinueAndReplace");
                let body_replacement =
                    match response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation) {
                        Some(Mutation::Body(bytes)) => Some(Frame::data(bytes.into())),
                        Some(Mutation::ClearBody(true)) => Some(Frame::data(Bytes::new())),
                        Some(Mutation::ClearBody(false)) | None => None,
                        Some(Mutation::StreamedResponse(chunk)) => Some(Frame::data(chunk.body.into())),
                    };

                // replace body if specified
                if let Some(new_body) = body_replacement {
                    debug!(target: "ext_proc", "handle_headers_response: body replacement requested: {new_body:?}");
                    _ = self.frame_bridge.inject_frame(Ok(new_body)).await;
                }

                // replace trailers if specified
                if let Some(trailers) = response_data.trailers {
                    debug!(target: "ext_proc", "handle_headers_response: trailers replacement requested: {trailers:?}");
                    let new_trailers: http::HeaderMap = EnvoyHeaderMap(trailers).into();
                    _ = self.frame_bridge.inject_frame(Ok(Frame::trailers(new_trailers))).await;
                }

                debug!(target: "ext_proc", "frame bridge closed (handle header response)!");
                self.frame_bridge_close(timeout_active).await;
            } else {
                debug!(target: "ext_proc", "handle_headers_response: ResponseStatus:Continue: headers processed");
                if !override_mode.should_process_body::<Msg>() && !override_mode.should_process_trailers::<Msg>() {
                    debug!(target: "ext_proc", "handle_headers_response: complete to stream original body and close!");
                    self.frame_bridge.complete().await;
                    debug!(target: "ext_proc", "frame bridge closed (handle header response)!");
                    self.frame_bridge_close(timeout_active).await;
                } else {
                    let stream_body_enabled = self.try_enable_streaming_body();
                    debug!(target: "ext_proc", "handle_headers_response: trying to enable streaming body: {stream_body_enabled}");
                }
            }
        } else {
            // todo(Nicola): this is UB; we are not sure what to do if response.response is None
            status = if Msg::IS_REQUEST {
                Some(ProcessingStatus::RequestReady(ReadyStatus {
                    headers_modifications: None,
                    clear_route_cache: false,
                }))
            } else {
                Some(ProcessingStatus::ResponseReady(ReadyStatus {
                    headers_modifications: None,
                    clear_route_cache: false,
                }))
            };

            let stream_body_enabled = self.try_enable_streaming_body();
            debug!(target: "ext_proc", "handle_headers_response: (UB) trying to enable streaming body: {stream_body_enabled}");
        }

        *timeout_active = false;
        match status {
            Some(status) => Action::Return(status),
            None => Action::None,
        }
    }

    #[allow(clippy::too_many_lines)]
    #[must_use = "must handle the returned Action"]
    pub async fn handle_body_response(
        &mut self,
        body_response: BodyResponse,
        route_cache_action: Option<&RouteCacheAction>,
        timeout_active: &mut bool,
    ) -> Action<ProcessingRequest> {
        debug!(target: "ext_proc", "handle_body_response: inflight frames ({})", self.inflight_frames.len());

        if let Some(response_data) = body_response.response {
            let chunk_replacement = match response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation) {
                Some(Mutation::Body(bytes)) => Some(Frame::data(bytes.into())),
                Some(Mutation::ClearBody(true)) => Some(Frame::data(Bytes::new())),
                Some(Mutation::ClearBody(false)) | None => None,
                Some(Mutation::StreamedResponse(chunk)) => Some(Frame::data(chunk.body.into())),
            };

            if let Some(new_chunk) = chunk_replacement {
                debug!(target: "ext_proc", "handle_body_response: chunk replacement -> {new_chunk:?}");
                _ = self.frame_bridge.inject_frame(Ok(new_chunk)).await;
                if !self.inflight_frames.is_empty() {
                    self.inflight_frames.drain(0..1);
                }
            } else {
                debug!(target: "ext_proc", "handle_body_response: no chunk replacement requested");
                if !self.inflight_frames.is_empty() {
                    if let Some(frame) = self.inflight_frames.drain(0..1).next() {
                        _ = self.frame_bridge.inject_frame(Ok(frame)).await;
                    }
                }
            }

            let should_clear_route_cache = route_cache_action
                .map(|action| match action {
                    RouteCacheAction::Clear => true,
                    RouteCacheAction::Retain => false,
                    RouteCacheAction::Default => response_data.clear_route_cache,
                })
                .unwrap_or(false);

            let mut status = if Msg::IS_REQUEST {
                ProcessingStatus::RequestReady(ReadyStatus::default())
            } else {
                ProcessingStatus::ResponseReady(ReadyStatus::default())
            };

            if Msg::IS_REQUEST {
                debug!(target: "ext_proc", "handle_body_response: request status {status:?}");
                status.with_request_ready(|req_ready| {
                    let ready_status = self.headers_ready_status.take();
                    req_ready.clear_route_cache = should_clear_route_cache
                        || ready_status.as_ref().map(|status| status.clear_route_cache).unwrap_or(false);
                    req_ready.headers_modifications = Self::concat_header_mutations(
                        ready_status.map(|status| status.headers_modifications).flatten(),
                        response_data.header_mutation,
                    );
                });
            } else {
                debug!(target: "ext_proc", "handle_body_response: response status {status:?}");
                status.with_response_ready(|resp_ready| {
                    let ready_status = self.headers_ready_status.take();
                    resp_ready.clear_route_cache = should_clear_route_cache
                        || ready_status.as_ref().map(|status| status.clear_route_cache).unwrap_or(false);
                    resp_ready.headers_modifications = Self::concat_header_mutations(
                        ready_status.map(|status| status.headers_modifications).flatten(),
                        response_data.header_mutation,
                    );
                });
            }

            let embedded_status = ResponseStatus::try_from(response_data.status).unwrap_or(ResponseStatus::Continue);

            if matches!(embedded_status, ResponseStatus::ContinueAndReplace) {
                debug!(target: "ext_proc", "handle_body_response: CONTINUE_AND_REPLACE: sending message status {status:?}");
                self.frame_bridge_close(timeout_active).await;
                return Action::Return(status);
            }

            if self.end_of_stream && self.inflight_frames.is_empty() {
                debug!(target: "ext_proc", "handle_body_response: end_of_stream (closing frame bridge)!");
                self.frame_bridge_close(timeout_active).await;
            }

            Action::Return(status)
        } else {
            Action::Return(
                self.status_error("handle_body_response: No response data in body response", self.failure_mode_allow),
            )
        }
    }

    #[inline]
    fn concat_header_mutations(left: Option<HeaderMutation>, right: Option<HeaderMutation>) -> Option<HeaderMutation> {
        match (left, right) {
            (None, None) => None,
            (None, r @ Some(_)) => r,
            (l @ Some(_), None) => l,
            (Some(l), Some(r)) => Some(HeaderMutation {
                set_headers: [l.set_headers, r.set_headers].concat(),
                remove_headers: [l.remove_headers, r.remove_headers].concat(),
            }),
        }
    }

    #[must_use = "must handle the returned Action"]
    pub async fn handle_trailers_response(
        &mut self,
        trailers_response: TrailersResponse,
        timeout_active: &mut bool,
    ) -> Action<ProcessingRequest> {
        if let Some(mut trailers) = self.trailers.take() {
            // update the local version of trailers, if required if let Some(trailers) = self.body_context.trailers.as_mut() {
            debug!(target: "ext_proc", "handle_trailers_response: mutating trailers...");
            if let Some(ref trailers_updates) = trailers_response.header_mutation {
                let _ = apply_trailer_mutations(&mut trailers, trailers_updates, None);
            }

            _ = self.frame_bridge.inject_frame(Ok(Frame::trailers(trailers))).await;

            self.end_of_stream = true;
            self.frame_bridge_close(timeout_active).await;
            Action::Return(ProcessingStatus::ready::<Msg>())
        } else {
            debug!(target: "ext_proc", "frame bridge closed (handle trailers response)!");
            self.frame_bridge_close(timeout_active).await;
            Action::Return(
                self.status_error("handle_trailers_response: No trailers to process", self.failure_mode_allow),
            )
        }
    }

    pub async fn inject_inflight_frames_and_complete(&mut self) {
        debug!(target: "ext_proc", "interrupt_and_complete!");
        for frame in self.inflight_frames.drain(..) {
            _ = self.frame_bridge.inject_frame(Ok(frame)).await;
        }
        self.frame_bridge.complete().await;
    }
}

impl<M: kind::Mode + Default, Msg: kind::MsgKind + OverridableModeSelector> Processing<M, Msg> {
    #[must_use = "must handle the returned Action"]
    #[allow(clippy::too_many_arguments)]
    pub async fn process(
        &mut self,
        headers: Option<CombinedHeaderMap>,
        frame_bridge: FrameBridge,
        reply_channel: oneshot::Sender<ProcessingStatus>,
        http_version: http::Version,
        override_mode: &OverridableGlobalModes,
    ) -> Action<ProcessingRequest> {
        debug!(target: "ext_proc", "process: processing headers/body/trailers started");

        self.reply_channel = Some(reply_channel);
        self.http_version = Some(http_version);
        self.http_headers = headers;
        self.frame_bridge = frame_bridge;

        //
        // 1) process headers if available and configured...
        //
        if self.http_headers.is_some() && override_mode.should_process_headers::<Msg>() {
            debug!(target: "ext_proc", "process: processing headers...");
            return self.process_headers(override_mode);
        }

        //
        // 2) headers are skipped, let's process the body...
        //

        if self.frame_bridge.source_has_non_empty_body_or_trailers()
            && (override_mode.should_process_body::<Msg>() || override_mode.should_process_trailers::<Msg>())
        {
            debug!(target: "ext_proc", "process: processing body and trailers...");
            return self.process_body_and_trailers(override_mode).await;
        }

        debug!(target: "ext_proc", "process: nothing to do!");

        //
        // 3) Nothing to do with this request or response.
        //

        self.frame_bridge.complete().await;
        self.frame_bridge.close();
        return Action::Return(ProcessingStatus::ready::<Msg>());
    }

    #[must_use = "must handle the returned Action"]
    fn process_headers(&mut self, override_mode: &OverridableGlobalModes) -> Action<ProcessingRequest> {
        debug!(target: "ext_proc", "process_request headers {:?}", self.http_headers);
        let Some(headers) = &self.http_headers else {
            return Action::Return(self.status_error(
                format!("process_request: Unexpected headers provided: {:?}", self.http_headers).as_str(),
                self.failure_mode_allow,
            ));
        };

        self.end_of_stream = self.is_end_stream(Phase::Headers, override_mode);

        let envmap: EnvoyHeaderMap = headers.into();

        let processing_request = if Msg::IS_REQUEST {
            ProcessingRequest {
                request: Some(ProcessingRequestType::RequestHeaders(HttpHeaders {
                    headers: Some(envmap.0),
                    attributes: HashMap::default(),
                    end_of_stream: self.end_of_stream,
                })),
                metadata_context: None,
                attributes: HashMap::default(),
                observability_mode: M::OBSERVABILITY,
                protocol_config: None,
            }
        } else {
            ProcessingRequest {
                request: Some(ProcessingRequestType::ResponseHeaders(HttpHeaders {
                    headers: Some(envmap.0),
                    attributes: HashMap::default(),
                    end_of_stream: self.end_of_stream,
                })),
                metadata_context: None,
                attributes: HashMap::default(),
                observability_mode: M::OBSERVABILITY,
                protocol_config: None,
            }
        };

        let send_body_or_trailers = !self.end_of_stream;

        if M::OBSERVABILITY || (self.send_body_without_waiting_for_header_response && send_body_or_trailers) {
            // force enable streaming body. Note: in observability mode we want to enable streaming body regardless of the presence of body/trailers
            // to properly handle the termination condition.
            self.enable_streaming_body();
        }

        Action::Send(processing_request)
    }

    #[must_use = "must handle the returned Action"]
    async fn process_body_and_trailers(&mut self, override_mode: &OverridableGlobalModes) -> Action<ProcessingRequest> {
        debug!(target: "ext_proc", "process: body and trailers (headers are skipped)...");

        let streaming_enabled = self.try_enable_streaming_body();

        if M::OBSERVABILITY {
            return Action::Return(ProcessingStatus::ready::<Msg>());
        }

        if matches!(
            override_mode.body_mode::<Msg>(),
            OverridableBodyMode::Buffered | OverridableBodyMode::BufferedPartial
        ) {
            if self.frame_bridge.source_has_non_empty_body() == Some(true) && override_mode.should_process_body::<Msg>()
            {
                if streaming_enabled {
                    return Action::None;
                } else {
                    debug!(target: "ext_proc", "process_body_and_trailers: no body/trailers to stream (internal error)");
                }
            }
        }

        return Action::Return(ProcessingStatus::ready::<Msg>());
    }

    #[must_use = "must handle the returned Action"]
    pub fn handle_outgoing_body_chunk(
        &mut self,
        mut chunk: Frame<Bytes>,
        end_of_stream: bool,
    ) -> Action<ProcessingRequest> {
        debug!(target: "ext_proc", "handle_body_chunk: sending data frame: {}, end_of_stream: {end_of_stream}",
            if chunk.is_data() { "DATA" } else if chunk.is_trailers() { "TRAILERS" } else { "OTHER" });

        let processing_request = if let Some(bytes) = chunk.data_mut() {
            // DATA
            let data = std::mem::take(bytes);

            let http_body = HttpBody {
                body: data.into(),
                end_of_stream,
                // todo(francesco): additional fields appeared after first
                // ext_proc development, currently not handled, verify if they
                // are useful
                end_of_stream_without_message: false,
                grpc_message_compressed: false,
            };
            if Msg::IS_REQUEST {
                ProcessingRequest {
                    request: Some(ProcessingRequestType::RequestBody(http_body)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: M::OBSERVABILITY,
                    protocol_config: None,
                }
            } else {
                ProcessingRequest {
                    request: Some(ProcessingRequestType::ResponseBody(http_body)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: M::OBSERVABILITY,
                    protocol_config: None,
                }
            }
        } else if let Some(traiers) = chunk.trailers_mut() {
            // TRAILERS
            let data = std::mem::take(traiers);

            // store trailers for potential update later
            self.trailers = Some(data.clone());

            let envoy_trailers: EnvoyHeaderMap = (&data).into();

            if Msg::IS_REQUEST {
                ProcessingRequest {
                    request: Some(ProcessingRequestType::RequestTrailers(HttpTrailers {
                        trailers: Some(envoy_trailers.0),
                    })),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: M::OBSERVABILITY,
                    protocol_config: None,
                }
            } else {
                ProcessingRequest {
                    request: Some(ProcessingRequestType::ResponseTrailers(HttpTrailers {
                        trailers: Some(envoy_trailers.0),
                    })),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: M::OBSERVABILITY,
                    protocol_config: None,
                }
            }
        } else {
            let msg = "handle_body_chunk: unexpected non-data frame to send";
            debug!(target: "ext_proc", msg);
            return Action::Return(self.status_error(msg, self.failure_mode_allow));
        };

        debug!(target: "ext_proc", "handle_body_chunk: prepared processing_request: {:?}", TruncatedDebug::<_,1024>(&processing_request));
        Action::Send(processing_request)
    }

    pub fn status_timeout(&mut self, failure_mode_allow: bool) -> ProcessingStatus {
        if failure_mode_allow {
            ProcessingStatus::HaltedOnError
        } else {
            let http_version = self.http_version.unwrap_or(http::Version::HTTP_11);
            ProcessingStatus::EndWithDirectResponse(
                SyntheticHttpResponse::gateway_timeout(
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::UPSTREAM_REQUEST_TIMEOUT),
                )
                .into_response(http_version),
            )
        }
    }

    #[inline]
    pub fn status_error(&mut self, msg: &str, failure_mode_allow: bool) -> ProcessingStatus {
        if failure_mode_allow {
            ProcessingStatus::HaltedOnError
        } else {
            let http_version = self.http_version.unwrap_or(http::Version::HTTP_11);
            ProcessingStatus::EndWithDirectResponse(
                SyntheticHttpResponse::internal_server_error(
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::NO_FILTER_CONFIG_FOUND),
                    msg,
                )
                .into_response(http_version),
            )
        }
    }

    #[inline]
    pub fn status_internal_error(&mut self, msg: &str) -> ProcessingStatus {
        let http_version = self.http_version.unwrap_or(http::Version::HTTP_11);
        ProcessingStatus::EndWithDirectResponse(
            SyntheticHttpResponse::internal_server_error(
                EventFailure::ExtProcError.into(),
                ResponseFlags(FmtResponseFlags::UNAUTHORIZED_EXTERNAL_SERVICE),
                msg,
            )
            .into_response(http_version),
        )
    }

    #[inline]
    pub async fn frame_bridge_close(&mut self, timeout_active: &mut bool) {
        *timeout_active = false;
        self.streaming_body_enabled = false;
        if let Some(trailers) = self.parked_trailers.take() {
            _ = self.frame_bridge.inject_frame(Ok(trailers)).await;
        }
        self.frame_bridge.close();
    }

    #[inline]
    fn is_end_stream(&self, phase: Phase, override_mode: &OverridableGlobalModes) -> bool {
        // Determine if trailers will actually be sent.
        // It requires both the configuration to allow it and the physical presence of trailers.
        // If the presence is unknown (None), we assume true to keep the stream open safely.
        let will_send_trailers = override_mode.should_process_trailers::<Msg>()
            && self.frame_bridge.source_has_pending_trailers().unwrap_or(true);

        // Determine if the body will actually be sent.
        // Note: If you are in Buffered mode, Envoy sends an empty body anyway,
        // so you might only rely on `should_process_body` depending on its internal implementation.
        let will_send_body =
            override_mode.should_process_body::<Msg>() && self.frame_bridge.source_has_non_empty_body().unwrap_or(true);

        match phase {
            // In the Headers phase, it is the end of the stream ONLY IF no body and no trailers will follow.
            Phase::Headers => !will_send_body && !will_send_trailers,

            // In the Body phase, it is the end of the stream ONLY IF no trailers will follow.
            Phase::Body => !will_send_trailers,

            // The Trailers phase is strictly the last element, so it always closes the stream.
            Phase::Trailers => true,
        }
    }

    #[inline]
    #[must_use]
    pub fn try_enable_streaming_body(&mut self) -> bool {
        // enable streaming only if we have a body to stream
        if self.frame_bridge.source_has_non_empty_body_or_trailers() {
            debug!(target: "ext_proc", "enabling body streaming...");
            self.streaming_body_enabled = true;
            true
        } else {
            debug!(target: "ext_proc", "body streaming not enabled (body and trailers are empty)");
            self.frame_bridge.close();
            false
        }
    }

    #[inline]
    pub fn enable_streaming_body(&mut self) {
        debug!(target: "ext_proc", "enabling body streaming...");
        self.streaming_body_enabled = true;
    }

    #[inline]
    pub fn is_streaming_body_enabled(&self) -> bool {
        self.streaming_body_enabled
    }
}
