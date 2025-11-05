use crate::body::channel_body::FrameBridge;
use crate::event_error::EventFailure;
use crate::listeners::http_connection_manager::ext_proc::common_state::{
    Action, ProcessingStatus, ProcessingState, ReadyStatus, State,
};
use crate::listeners::http_connection_manager::ext_proc::mutation::apply_header_mutations;
use crate::listeners::http_connection_manager::ext_proc::worker_config::ExternalProcessingWorkerConfig;
use crate::listeners::http_connection_manager::ext_proc::EnvoyHeaderMap;
use crate::{body::response_flags::ResponseFlags, listeners::synthetic_http_response::SyntheticHttpResponse};
use bytes::Bytes;
use http_body::{Frame};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    BodyProcessingMode, HeaderProcessingMode, ProcessingMode, RouteCacheAction, TrailerProcessingMode,
};
use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HttpTrailers;
use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::common_response::ResponseStatus;
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
use tokio::sync::oneshot;
use tracing::debug;

pub struct RequestProcessing<S: State>(Processing<S>);

impl<S: State> Deref for RequestProcessing<S> {
    type Target = Processing<S>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S: State> DerefMut for RequestProcessing<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub struct ResponseProcessing<S: State>(Processing<S>);

impl<S: State> Deref for ResponseProcessing<S> {
    type Target = Processing<S>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S: State> DerefMut for ResponseProcessing<S> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


pub struct Processing<S: State> {
    pub state: S,

    headers_mode: HeaderProcessingMode,
    body_mode: BodyProcessingMode,
    trailers_mode: TrailerProcessingMode,

    http_headers: Option<http::HeaderMap>,
    pub trailers: Option<http::HeaderMap>,

    pub frame_bridge: FrameBridge,
    pub buffered_chunk: Option<Bytes>,

    partial_status: Option<ProcessingStatus>,
    pub reply_channel: Option<oneshot::Sender<ProcessingStatus>>,
    http_version: Option<http::Version>,
    send_body_without_waiting_for_header_response: bool,
    pub failure_mode_allow: bool,
    pub streaming_body_enabled: bool,
    pub end_of_stream: bool,
}

enum ProcessingKind {
    Request,
    Response,
}

impl<S: State + Default> From<(&ExternalProcessingWorkerConfig, ProcessingKind)> for Processing<S> {
    fn from((config, kind): (&ExternalProcessingWorkerConfig, ProcessingKind)) -> Self {
        debug!(target: "ext_proc", "From<&ExternalProcessingWorkerConfig for Processing<>");
        assert!(
            !config.observability_mode,
            "Attempted to create RequestProcessing<ProcessingState> in observability mode"
        );

        let processing_mode = &config.processing_mode;

        Self{
            state: S::default(),
            headers_mode: match kind {
                ProcessingKind::Request => processing_mode.request_header_mode,
                ProcessingKind::Response => processing_mode.response_header_mode,
            },
            body_mode: match kind {
                ProcessingKind::Request => processing_mode.request_body_mode,
                ProcessingKind::Response => processing_mode.response_body_mode,
            },
            trailers_mode: match kind {
                ProcessingKind::Request => processing_mode.request_trailer_mode,
                ProcessingKind::Response => processing_mode.response_trailer_mode,
            },

            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
            failure_mode_allow: config.failure_mode_allow,

            http_headers: None,
            frame_bridge: FrameBridge::default(),
            trailers: None,
            buffered_chunk: None,
            partial_status: None,
            reply_channel: None,
            http_version: None,
            streaming_body_enabled: false,
            end_of_stream: false,
        }
    }
}

impl<S: State + Default> From<&ExternalProcessingWorkerConfig> for RequestProcessing<S> {
    fn from(value: &ExternalProcessingWorkerConfig) -> Self {
        debug!(target: "ext_proc", "From<&ExternalProcessingWorkerConfig for RequestProcessing<>");
        Self(Processing::<S>::from((value, ProcessingKind::Request)))
    }
}

impl<S: State + Default> From<&ExternalProcessingWorkerConfig> for ResponseProcessing<S> {
    fn from(value: &ExternalProcessingWorkerConfig) -> Self {
        debug!(target: "ext_proc", "From<&ExternalProcessingWorkerConfig for ResponseProcessing<>");
        Self(Processing::<S>::from((value, ProcessingKind::Response)))
    }
}


impl Processing<ProcessingState> {
    pub fn apply_mode_overrides(
        &mut self,
        envoy_mode: &EnvoyProcessingMode,
        allowed_override_modes: &[ProcessingMode],
    ) {
        if matches!(self.state, ProcessingState::WaitingForHeadersReply) {
            if let Ok(mode) = BodyProcessingMode::try_from(envoy_mode.request_body_mode) {
                if allowed_override_modes.iter().any(|allowed| allowed.request_body_mode == mode) {
                    self.body_mode = mode;
                }
            }
            if let Ok(mode) = TrailerProcessingMode::try_from(envoy_mode.request_trailer_mode) {
                if allowed_override_modes.iter().any(|allowed| allowed.request_trailer_mode == mode) {
                    self.trailers_mode = mode;
                }
            }
        }
    }

    #[must_use = "must handle the returned Action"]
    pub async fn process(
        &mut self,
        headers: Option<http::HeaderMap>,
        frame_bridge: FrameBridge,
        reply_channel: oneshot::Sender<ProcessingStatus>,
        http_version: http::Version,
    ) -> Action<ProcessingRequest>
    {
        self.reply_channel = Some(reply_channel);
        self.http_version = Some(http_version);
        self.http_headers = headers;
        self.frame_bridge = frame_bridge;

        if self.http_headers.is_some() {
            return self.process_headers();
        }

        self.streaming_body_enabled = true;

        let status = ProcessingStatus::RequestReady(ReadyStatus::default());
        Action::Return(status)
    }

    #[must_use = "must handle the returned Action"]
    fn process_headers(&mut self) -> Action<ProcessingRequest> {
        debug!(target: "ext_proc", "process_request headers {:?}", self.http_headers);
        let Some(headers) = &self.http_headers else {
            return Action::Return(
                self.status_error(
                    format!("process_request: Unexpected headers provided: {:?}", self.http_headers).as_str(),
                    self.failure_mode_allow,
                ),
            );
        };

        let end_of_stream = !self.should_process_body() && !self.should_process_trailers();

        let envmap: EnvoyHeaderMap = headers.into();
        let processing_request = ProcessingRequest {
            request: Some(ProcessingRequestType::RequestHeaders(HttpHeaders {
                headers: Some(envmap.0),
                attributes: HashMap::default(),
                end_of_stream,
            })),
            metadata_context: None,
            attributes: HashMap::default(),
            observability_mode: false,
            protocol_config: None,
        };

        let next_state = if self.send_body_without_waiting_for_header_response
            && !matches!(self.body_mode, BodyProcessingMode::None)
        {
            self.streaming_body_enabled = true;
            ProcessingState::StreamingBody
        } else {
            ProcessingState::WaitingForHeadersReply
        };

        Action::Send(processing_request, next_state)
    }

    // #[must_use = "must handle the returned Action"]
    // pub async fn prepare_body(&mut self, body: FrameBridge) -> Action<ProcessingRequest> {
    //     match self.body_context.body_mode {
    //         BodyProcessingMode::Buffered | BodyProcessingMode::BufferedPartial => {
    //             debug!(target: "ext_proc", "prepare_body: Buffered/BufferedPartial body configured!");
    //             let body_bytes: Bytes = match body {
    //                 PolyBody::Full(body) => body.collect().await.unwrap().to_bytes(),
    //                 PolyBody::Collected(collected) => collected.to_bytes(),
    //                 _ => {
    //                     debug!(target: "ext_proc", "prepare_body: Unexpected {:?}, wanted PolyBody Full or Collected!", body);
    //                     return Action::Return(self.status_error(
    //                         format!("prepare_body: Unexpected {:?}, wanted PolyBody Full or Collected!", body).as_str(),
    //                         self.failure_mode_allow,
    //                     ));
    //                 },
    //             };

    //             let http_body =
    //                 HttpBody { body: body_bytes.into(), end_of_stream: !self.is_trailers_processing_planned() };
    //             let processing_request = ProcessingRequest {
    //                 request: Some(ProcessingRequestType::RequestBody(http_body)),
    //                 metadata_context: None,
    //                 attributes: HashMap::default(),
    //                 observability_mode: false,
    //                 protocol_config: None,
    //             };
    //             Action::Send(processing_request, ProcessingState::WaitingForBodyReply, false)
    //         },
    //         BodyProcessingMode::Streamed | BodyProcessingMode::FullDuplexStreamed => {
    //             debug!(target: "ext_proc", "prepare_body: Streamed body configured!");
    //             self.body_context.body = Some(body);
    //             Action::Streaming(ProcessingState::StreamingBody)
    //         },
    //         mode => {
    //             return Action::Return(
    //                 self.status_error(format!("prepare_body: Unexpected {mode:?}!").as_str(), self.failure_mode_allow),
    //             )
    //         },
    //     }
    // }

    // #[must_use = "must handle the returned Action"]
    // pub fn prepare_trailers(&self, trailers: &http::HeaderMap) -> Action<ProcessingRequest> {
    //     let trailers_to_send: EnvoyHeaderMap = trailers.into();

    //     let processing_request = ProcessingRequest {
    //         request: Some(ProcessingRequestType::RequestTrailers(HttpTrailers { trailers: Some(trailers_to_send.0) })),
    //         metadata_context: None,
    //         attributes: HashMap::default(),
    //         observability_mode: false,
    //         protocol_config: None,
    //     };

    //     Action::Send(processing_request, self.state, false)
    // }

    #[must_use = "must handle the returned Action"]
    pub async fn handle_headers_response(
        &mut self,
        response: HeadersResponse,
        route_cache_action: &RouteCacheAction,
        wants_response_headers: bool,
        wants_response_body: bool,
        wants_response_trailers: bool,
    ) -> Action<ProcessingRequest> {
        match self.state {
            ProcessingState::WaitingForHeadersReply | ProcessingState::StreamingBody => {
                let status;

                if let Some(response_data) = response.response {
                    let should_clear_route_cache = match route_cache_action {
                        RouteCacheAction::Clear => true,
                        RouteCacheAction::Retain => false,
                        RouteCacheAction::Default => response_data.clear_route_cache,
                    };

                    status = ProcessingStatus::RequestReady(ReadyStatus {
                        headers_modifications: response_data.header_mutation,
                        override_sending_response_headers: Some(wants_response_headers),
                        override_sending_response_body: Some(wants_response_body),
                        override_sending_response_trailers: Some(wants_response_trailers),
                        clear_route_cache: should_clear_route_cache,
                    });

                    let embedded_status =
                        ResponseStatus::try_from(response_data.status).unwrap_or(ResponseStatus::Continue);

                    if matches!(embedded_status, ResponseStatus::ContinueAndReplace) {
                        debug!(target: "ext_proc", "handle_headers_response: ResponseStatus:ContinueAndReplace");
                        let body_replacement =
                            match response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation) {
                                Some(Mutation::Body(bytes)) => {
                                    Some(Frame::data(bytes.into()))
                                },
                                Some(Mutation::ClearBody(true)) => Some(Frame::data(Bytes::new())),
                                Some(Mutation::ClearBody(false)) | None => None,
                                Some(Mutation::StreamedResponse(_)) => {
                                    return Action::Return(self.status_error(
                                    "StreamedResponse mutation not supported in response to header processing request",
                                    self.failure_mode_allow,
                                ));
                                },
                            };

                        // replace body if specified
                        if let Some(new_body) = body_replacement {
                            debug!(target: "ext_proc", "handle_headers_response: Body replacement requested: {new_body:?}");
                            _ = self.frame_bridge.inject_frame(Ok(new_body)).await;
                        }

                        // replace trailers if specified
                        if let Some(trailers) = response_data.trailers {
                            debug!(target: "ext_proc", "handle_headers_response: Trailers replacement requested: {trailers:?}");
                            let new_trailers : http::HeaderMap = EnvoyHeaderMap(trailers).into();
                            _ = self.frame_bridge.inject_frame(Ok(Frame::trailers(new_trailers))).await;
                        }

                    } else {
                        debug!(target: "ext_proc", "handle_headers_response: ResponseStatus:Continue: headers processed");
                        if !self.should_process_body() && !self.should_process_trailers() {
                            debug!(target: "ext_proc", "handle_headers_response: complete to stream original body and close!");
                            self.frame_bridge.complete().await;
                            debug!(target: "ext_proc", "frame brige closed!");
                            self.frame_bridge.close().await;
                        } else {
                            debug!(target: "ext_proc", "handle_headers_response: streaming body enabled...");
                            self.streaming_body_enabled = true;
                        }
                    }
                } else {
                    status = ProcessingStatus::RequestReady(ReadyStatus {
                        headers_modifications: None,
                        override_sending_response_headers: Some(wants_response_headers),
                        override_sending_response_body: Some(wants_response_body),
                        override_sending_response_trailers: Some(wants_response_trailers),
                        clear_route_cache: false,
                    });

                    self.streaming_body_enabled = true;
                }

                Action::Return(status)
            },
            s => {
                debug!(target: "ext_proc", "handles_header_response: Unexpected state {s:?}");
                Action::Return(self.status_error(
                    format!("handles_header_response: Unexpected state {s:?}").as_str(),
                    self.failure_mode_allow,
                ))
            },
        }
    }

    #[allow(clippy::too_many_lines)]
    #[must_use = "must handle the returned Action"]
    pub async fn handle_body_response(
        &mut self,
        sent_frames: &mut SmallVec<[Frame<Bytes>; 2]>,
        body_response: BodyResponse,
        route_cache_action: Option<&RouteCacheAction>,
    ) -> Action<ProcessingRequest> {
        debug!(target: "ext_proc", "handle_body_response: pending frames => {sent_frames:?}");

        if let Some(response_data) = body_response.response {
            let chunk_replacement = match response_data
                .body_mutation
                .and_then(|body_mutation| body_mutation.mutation)
            {
                Some(Mutation::Body(bytes)) => Some(Frame::data(bytes.into())),
                Some(Mutation::ClearBody(true)) => Some(Frame::data(Bytes::new())),
                Some(Mutation::ClearBody(false)) | None => None,
                Some(Mutation::StreamedResponse(chunk)) => {
                    Some(Frame::data(chunk.body.into()))
                },
            };
            debug!(target: "ext_proc", "chunk_replacement => {chunk_replacement:?}");

            let mut status =
                self.partial_status.take().unwrap_or_else(|| ProcessingStatus::RequestReady(ReadyStatus::default()));

            match chunk_replacement {
                Some(new_chunk) => {
                    debug!(target: "ext_proc", "handle_body_response: chunk replacement requested: {new_chunk:?}");
                    _ = self.frame_bridge.inject_frame(Ok(new_chunk)).await;
                    sent_frames.drain(0..1);
                },
                None => {
                    debug!(target: "ext_proc", "handle_body_response: no chunk replacement requested");
                    if let Some(frame) = sent_frames.drain(0..1).next() {
                        _ = self.frame_bridge.inject_frame(Ok(frame)).await;
                    }
                },
            }

            if let Some(route_cache_action) = route_cache_action {
                if let Some(header_modifications) = response_data.header_mutation {
                    let should_clear_route_cache = match route_cache_action {
                        RouteCacheAction::Clear => true,
                        RouteCacheAction::Retain => false,
                        RouteCacheAction::Default => response_data.clear_route_cache,
                    };

                    status.with_request_ready(|req_ready| {
                        req_ready.headers_modifications = Some(header_modifications);
                        req_ready.clear_route_cache = should_clear_route_cache;
                    });
                }
            }

            let embedded_status =
                ResponseStatus::try_from(response_data.status).unwrap_or(ResponseStatus::Continue);

            if matches!(embedded_status, ResponseStatus::ContinueAndReplace) {
                debug!(target: "ext_proc", "handle_body_response: CONTINUE_AND_REPLACE: Sending message status {status:?}");
                self.streaming_body_enabled = false;
                self.frame_bridge.close().await;
                return Action::Return(status);
            }

            if self.end_of_stream && sent_frames.is_empty() {
                debug!(target: "ext_proc", "handle_body_response: end_of_stream reached, closing frame bridge");
                self.streaming_body_enabled = false;
                self.frame_bridge.close().await;
            }

            return Action::Return(status);
        } else {
            return Action::Return(self.status_error(
                "handle_body_response: No response data in body response",
                self.failure_mode_allow,
            ));
        }
    }

    #[must_use = "must handle the returned Action"]
    pub async fn handle_body_chunk(&mut self, mut chunk: Frame<Bytes>, end_of_stream: bool) -> Action<ProcessingRequest> {
        debug!(target: "ext_proc", "handle_body_chunk: sending data frame: {}, end_of_stream: {end_of_stream}",
            if chunk.is_data() { "DATA" } else if chunk.is_trailers() { "TRAILERS" } else { "OTHER" });

        let processing_request = if let Some(bytes) = chunk.data_mut() { // DATA
            let data = std::mem::take(bytes);

            let http_body = HttpBody { body: data.into(), end_of_stream };
            ProcessingRequest {
                request: Some(ProcessingRequestType::RequestBody(http_body)),
                metadata_context: None,
                attributes: HashMap::default(),
                observability_mode: self.state.is_observability_mode(),
                protocol_config: None,
            }

        } else if let Some(traiers) = chunk.trailers_mut() { // TRAILERS
            let data = std::mem::take(traiers);

            // store trailers for potential update later
            self.trailers = Some(data.clone());

            let envoy_trailers: EnvoyHeaderMap = (&data).into();

            ProcessingRequest {
                request: Some(ProcessingRequestType::RequestTrailers(HttpTrailers {
                    trailers: Some(envoy_trailers.0),
                })),
                metadata_context: None,
                attributes: HashMap::default(),
                observability_mode: self.state.is_observability_mode(),
                protocol_config: None,
            }

        } else {
            let msg = "handle_body_chunk: unexpected non-data frame to send";
            debug!(target: "ext_proc", msg);
            return Action::Return(self.status_error(
                msg,
                self.failure_mode_allow,
            ));
        };

        debug!(target: "ext_proc", "handle_body_chunk: prepared processing_request: {:?}", processing_request);
        if matches!(self.state, ProcessingState::StreamingBody) {
            Action::Send(processing_request, ProcessingState::StreamingBody)
        } else {
            Action::Send(processing_request, self.state)
        }
    }

    #[must_use = "must handle the returned Action"]
    pub async fn handle_trailers_response(&mut self, trailers_response: TrailersResponse) -> Action<ProcessingRequest> {
        if let Some(mut trailers) = self.trailers.take() {
            // update the local version of trailers, if required if let Some(trailers) = self.body_context.trailers.as_mut() {
            debug!(target: "ext_proc", "handle_trailers_response: mutating trailers...");
            if let Some(ref trailers_updates) = trailers_response.header_mutation {
                let _ = apply_header_mutations(&mut trailers, trailers_updates, None);
            }

            _ = self.frame_bridge.inject_frame(Ok(Frame::trailers(trailers))).await;
            self.frame_bridge.complete().await;
            self.end_of_stream = true;

            let status = ProcessingStatus::RequestReady(ReadyStatus::default());
            return Action::Return(status);
        } else {
            debug!(target: "ext_proc", "frame brige closed!");
            self.frame_bridge.close().await;
            return Action::Return(
                self.status_error("handle_trailers_response: No trailers to process", self.failure_mode_allow),
            );
        }
    }

    #[must_use = "must handle the returned Action"]
    pub fn handle_noop_response(&mut self, ctor: fn(ReadyStatus) -> ProcessingStatus, wants_response_body: Option<bool>, wants_response_headers: Option<bool>) -> Action<ProcessingRequest> {
        todo!("Implement handle_noop_response");
        //self.state = ProcessingState::default();
        //let status = self.partial_status.take().unwrap_or_else(|| ctor(ReadyStatus {
        //    body_replacement: self.body_context.body.take(),
        //    override_sending_response_headers: wants_response_headers,
        //    override_sending_response_body: wants_response_body,
        //    clear_route_cache: false,
        //    headers_modifications: None,
        //    trailers_modifications: None,
        //}));

        //Action::Return(status)
    }
}

impl<S: State + Default> Processing<S> {

    #[inline]
    pub fn should_process_headers(&self) -> bool {
        matches!(self.headers_mode, HeaderProcessingMode::Send)
    }

    #[inline]
    pub fn should_process_body(&self) -> bool {
        !matches!(self.body_mode, BodyProcessingMode::None)
    }

    #[inline]
    pub fn should_process_trailers(&self) -> bool {
        matches!(self.trailers_mode, TrailerProcessingMode::Send)
    }


    #[inline]
    pub fn is_awaiting_reply(&self) -> bool {
        self.reply_channel.is_some()
    }

    pub fn status_timeout(&mut self, failure_mode_allow: bool) -> ProcessingStatus {
        let status = if failure_mode_allow {
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
        };
        self.state = S::default();
        status
    }

    pub fn status_error(&mut self, msg: &str, failure_mode_allow: bool) -> ProcessingStatus {
        let status = if failure_mode_allow {
            ProcessingStatus::HaltedOnError
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
        self.state = S::default();
        status
    }
}
