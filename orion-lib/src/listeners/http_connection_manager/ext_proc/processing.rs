use crate::body::channel_body::FrameBridge;
use crate::event_error::EventFailure;
use crate::listeners::http_connection_manager::ext_proc::common_state::{
    Action, BodyContext, ProcessingStatus, ProcessingState, ReadyStatus, State,
};
use crate::listeners::http_connection_manager::ext_proc::mutation::apply_header_mutations;
use crate::listeners::http_connection_manager::ext_proc::worker_config::ExternalProcessingWorkerConfig;
use crate::listeners::http_connection_manager::ext_proc::EnvoyHeaderMap;
use crate::{body::response_flags::ResponseFlags, listeners::synthetic_http_response::SyntheticHttpResponse, PolyBody};
use bytes::Bytes;
use http_body::{Body, Frame};
use http_body_util::{BodyExt, Collected, Empty, Full};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    BodyProcessingMode, HeaderProcessingMode, ProcessingMode, RouteCacheAction, TrailerProcessingMode,
};
use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::common_response::ResponseStatus;
use orion_data_plane_api::envoy_data_plane_api::envoy::{
    extensions::filters::http::ext_proc::v3::ProcessingMode as EnvoyProcessingMode,
    service::ext_proc::v3::{
        body_mutation::Mutation, processing_request::Request as ProcessingRequestType, BodyResponse, HeadersResponse,
        HttpBody, HttpHeaders, HttpTrailers, ProcessingRequest, TrailersResponse,
    },
};
use orion_format::types::ResponseFlags as FmtResponseFlags;
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
    http_headers: Option<http::HeaderMap>,
    pub body_context: BodyContext,
    partial_status: Option<ProcessingStatus>,
    pub reply_channel: Option<oneshot::Sender<ProcessingStatus>>,
    http_version: Option<http::Version>,
    send_body_without_waiting_for_header_response: bool,
    failure_mode_allow: bool,
}

impl<S: State + Default> From<&ExternalProcessingWorkerConfig> for Processing<S> {
    fn from(config: &ExternalProcessingWorkerConfig) -> Self {
        assert!(
            !config.observability_mode,
            "Attempted to create RequestProcessing<ProcessingState> in observability mode"
        );

        let processing_mode = &config.processing_mode;

        Self{
            state: S::default(),
            http_headers: None,
            headers_mode: processing_mode.request_header_mode,
            body_context: BodyContext::new(processing_mode.request_body_mode, processing_mode.request_trailer_mode),
            partial_status: None,
            reply_channel: None,
            http_version: None,
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
            failure_mode_allow: config.failure_mode_allow,
        }
    }
}

impl<S: State + Default> From<&ExternalProcessingWorkerConfig> for RequestProcessing<S> {
    fn from(value: &ExternalProcessingWorkerConfig) -> Self {
        Self(Processing::<S>::from(value))
    }
}

impl<S: State + Default> From<&ExternalProcessingWorkerConfig> for ResponseProcessing<S> {
    fn from(value: &ExternalProcessingWorkerConfig) -> Self {
        Self(Processing::<S>::from(value))
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
                    self.body_context.body_mode = mode;
                }
            }
            if let Ok(mode) = TrailerProcessingMode::try_from(envoy_mode.request_trailer_mode) {
                if allowed_override_modes.iter().any(|allowed| allowed.request_trailer_mode == mode) {
                    self.body_context.trailers_mode = mode;
                }
            }
        }
    }

    #[must_use = "must handle the returned Action"]
    pub async fn process(
        &mut self,
        headers: Option<http::HeaderMap>,
        frame_bridge: Option<FrameBridge>,
        reply_channel: oneshot::Sender<ProcessingStatus>,
        http_version: http::Version,
    ) -> Action<ProcessingRequest>
    {
        self.reply_channel = Some(reply_channel);
        self.http_version = Some(http_version);
        self.http_headers = headers;
        self.body_context.frame_bridge = frame_bridge;

        if self.http_headers.is_some() {
            return self.process_headers();
        }

        //if let Some(body) = self.body_context.body_buffered.take() {
        //    self.state = ProcessingState::WaitingForBodyInput;
        //    return self.prepare_body(body).await;
        //}

        //if let Some(trailers) = self.body_context.trailers.as_ref() {
        //    self.state = ProcessingState::WaitingForBodyInput;
        //    return self.prepare_trailers(trailers);
        //}

        Action::Return(
            self.status_error("No headers, body, or trailers provided to process_request", self.failure_mode_allow),
        )
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

        let end_of_stream = !self.is_body_processing_planned() && !self.is_trailers_processing_planned();

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
            // && matches!(self.body_context.body_mode, BodyProcessingMode::Streamed)
            && self.body_context.frame_bridge.is_some()
        {
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
    ) -> Action<ProcessingRequest> {
        match self.state {
            ProcessingState::WaitingForHeadersReply | ProcessingState::StreamingBody => {
                let mut status;

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
                        clear_route_cache: should_clear_route_cache,
                    });

                    debug!(target: "ext_proc", "handle_headers_response: prepred Status: {status:?}");

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

                        if let Some(new_body) = body_replacement {
                            debug!(target: "ext_proc", "handle_headers_response: Body replacement requested: {new_body:?}");
                            if let Some(bridge) = self.body_context.frame_bridge.as_mut() {
                               bridge.inject_frame(Ok(new_body));
                               bridge.close();
                            }
                        }

                        debug!(target: "ext_proc", "handle_headers_response: Sending message status {status:?}");
                        return Action::Return(status);
                    }
                } else {
                    status = ProcessingStatus::RequestReady(ReadyStatus {
                        headers_modifications: None,
                        override_sending_response_headers: Some(wants_response_headers),
                        override_sending_response_body: Some(wants_response_body),
                        clear_route_cache: false,
                    });
                    debug!(target: "ext_proc", "handle_headers_response: prepared Status: {status:?}");
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
        body_response: BodyResponse,
        route_cache_action: Option<&RouteCacheAction>,
    ) -> Action<ProcessingRequest> {
        todo!("Implement handle_body_response");
        //if body_response.response.is_none() && !self.is_trailers_processing_planned() {
        //    let status =
        //        self.partial_status.take().unwrap_or_else(|| ProcessingStatus::RequestReady(ReadyStatus::default()));
        //    self.body_context.finish_stream();
        //    debug!(target: "ext_proc", "handle_body_response: Sending message status {status:?}");
        //    return Action::Return(status);
        //}

        //// we got a body response to process...
        //match self.state {
        //    ProcessingState::WaitingForBodyReply => {
        //        if let Some(response_data) = body_response.response {
        //            let body_replacement = match response_data
        //                .body_mutation
        //                .and_then(|body_mutation| body_mutation.mutation)
        //            {
        //                Some(Mutation::Body(bytes)) => Some(PolyBody::from(Full::new(Bytes::copy_from_slice(&bytes)))),
        //                Some(Mutation::ClearBody(true)) => Some(PolyBody::from(Empty::<Bytes>::default())),
        //                Some(Mutation::ClearBody(false)) | None => None,
        //                Some(Mutation::StreamedResponse(_)) => {
        //                    return Action::Return(self.status_error(
        //                        "StreamedResponse mutation not supported in response to buffered processing request",
        //                        self.failure_mode_allow,
        //                    ));
        //                },
        //            };
        //            debug!(target: "ext_proc", "body_replacement => {body_replacement:?}");
        //            let mut status =
        //                self.partial_status.take().unwrap_or_else(|| ProcessingStatus::RequestReady(ReadyStatus::default()));

        //            status.with_request_ready(|req_ready| {
        //                req_ready.body_replacement = body_replacement;
        //            });

        //            if let Some(route_cache_action) = route_cache_action {
        //                if let Some(header_modifications) = response_data.header_mutation {
        //                    let should_clear_route_cache = match route_cache_action {
        //                        RouteCacheAction::Clear => true,
        //                        RouteCacheAction::Retain => false,
        //                        RouteCacheAction::Default => response_data.clear_route_cache,
        //                    };

        //                    status.with_request_ready(|req_ready| {
        //                        req_ready.headers_modifications = Some(header_modifications);
        //                        req_ready.clear_route_cache = should_clear_route_cache;
        //                    });
        //                }
        //            }

        //            let embedded_status =
        //                ResponseStatus::try_from(response_data.status).unwrap_or(ResponseStatus::Continue);

        //            if matches!(embedded_status, ResponseStatus::ContinueAndReplace) {
        //                debug!(target: "ext_proc", "handle_body_response: CONTINUE_AND_REPLACE: Sending message status {status:?}");
        //                self.body_context.finish_stream();
        //                return Action::Return(status);
        //            }

        //            debug!(target: "ext_proc", "handle_body_response: CONTINUE: body processed");

        //            if self.is_trailers_processing_planned() {
        //                if let Some(trailers) = self.body_context.trailers.as_ref() {
        //                    self.partial_status = Some(status);
        //                    return self.prepare_trailers(trailers);
        //                } else {
        //                    debug!(target: "ext_proc", "handle_body_response: trailers processing planned but no trailers available");
        //                }
        //            }

        //            self.body_context.finish_stream();
        //            return Action::Return(status);
        //        } else {
        //            return Action::Return(self.status_error(
        //                "handle_body_response: No response data in body response",
        //                self.failure_mode_allow,
        //            ));
        //        }
        //    },

        //    ProcessingState::StreamingBodyWaitingForReply | ProcessingState::FullDuplexStreamingBody => {
        //        if let Some(response_data) = body_response.response {
        //            let body_mutation = response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation);
        //            let mut end_of_stream: bool = false;
        //            let body_mutated =
        //                body_mutation.is_some() && !matches!(body_mutation, Some(Mutation::ClearBody(false)));

        //            match body_mutation {
        //                Some(Mutation::StreamedResponse(streamed_response)) => {
        //                    if let Some(sender) = &self.body_context.inbound_body_sender {
        //                        let _ = sender.send_data(streamed_response.body.into()).await;
        //                    }
        //                    if streamed_response.end_of_stream {
        //                        if !matches!(self.body_context.trailers_mode, TrailerProcessingMode::Send) {
        //                            self.body_context.finish_stream();
        //                            end_of_stream = true;
        //                        }
        //                    } else if matches!(self.state, ProcessingState::StreamingBodyWaitingForReply) {
        //                        self.state = ProcessingState::StreamingBody;
        //                    }
        //                },
        //                Some(Mutation::Body(bytes)) => {
        //                    if let Some(sender) = &self.body_context.inbound_body_sender {
        //                        let _ = sender.send_data(bytes.into()).await;
        //                    }
        //                    if !matches!(self.body_context.trailers_mode, TrailerProcessingMode::Send) {
        //                        self.body_context.finish_stream();
        //                        end_of_stream = true;
        //                    }
        //                },
        //                Some(Mutation::ClearBody(true)) => {
        //                    self.body_context.finish_stream();
        //                    end_of_stream = true;
        //                },
        //                Some(Mutation::ClearBody(false)) | None => {
        //                    end_of_stream = true;
        //                },
        //            }
        //            let mut status =
        //                self.partial_status.take().unwrap_or_else(|| ProcessingStatus::RequestReady(ReadyStatus::default()));

        //            if body_mutated {
        //                status.with_request_ready(|req_ready| {
        //                    req_ready.body_replacement = self.body_context.body.take();
        //                });
        //            }

        //            if end_of_stream {
        //                debug!(target: "ext_proc", "handle_body_response: end_of_stream reached!");
        //                return Action::Return(status);
        //            } else {
        //                self.partial_status = Some(status);
        //                if let Some(trailers) = self.body_context.trailers.as_ref() {
        //                    return self.prepare_trailers(trailers);
        //                } else {
        //                    debug!(target: "ext_proc", "handle_body_response: trailers processing planned but no trailers available");
        //                    return Action::Return(self.partial_status.take().unwrap());
        //                }
        //            }
        //        } else {
        //            return Action::Return(self.status_error(
        //                "handle_body_response: No response data in body response",
        //                self.failure_mode_allow,
        //            ));
        //        }
        //    },
        //    s => {
        //        return Action::Return(self.status_error(
        //            format!("handle_body_response: Unexpected state {s:?}").as_str(),
        //            self.failure_mode_allow,
        //        ));
        //    },
        //}
    }

    #[must_use = "must handle the returned Action"]
    pub async fn handle_body_chunk(&mut self, data: Bytes, end_of_stream: bool) -> Action<ProcessingRequest> {
        todo!()
        //let http_body = HttpBody { body: data.to_vec(), end_of_stream };
        //let processing_request = ProcessingRequest {
        //    request: Some(ProcessingRequestType::RequestBody(http_body)),
        //    metadata_context: None,
        //    attributes: HashMap::default(),
        //    observability_mode: self.state.is_observability_mode(),
        //    protocol_config: None,
        //};

        //if matches!(self.state, ProcessingState::StreamingBody) {
        //    Action::Send(processing_request, ProcessingState::StreamingBodyWaitingForReply, false)
        //} else {
        //    Action::Send(processing_request, self.state, false)
        //}
    }

    #[must_use = "must handle the returned Action"]
    pub async fn handle_trailers_response(&mut self, trailers_response: TrailersResponse) -> Action<ProcessingRequest> {
        todo!("Implement handle_trailers_response");
        //debug!(target: "ext_proc", "handle_trailers_response: started");

        //if self.is_body_streaming() {
        //    // STREAMED body
        //    if let Some(mut trailers) = self.body_context.trailers.take() {
        //        // update the local version of trailers, if required if let Some(trailers) = self.body_context.trailers.as_mut() {
        //        debug!(target: "ext_proc", "handle_trailers_response: mutating trailers...");
        //        if let Some(ref trailers_updates) = trailers_response.header_mutation {
        //            let _ = apply_header_mutations(&mut trailers, trailers_updates, None);
        //        }

        //        if let Some(ref sender) = self.body_context.inbound_body_sender {
        //            debug!(target: "ext_proc", "handle_trailers_response: sending trailers to the inbound body sender...");
        //            let _ = sender.send_trailers(trailers).await;
        //        }

        //        let mut status =
        //            self.partial_status.take().unwrap_or_else(|| ProcessingStatus::RequestReady(ReadyStatus::default()));

        //        status.with_request_ready(|req_ready| {
        //            // no need to send trailers modifications if body is still streaming (they will be applied later)
        //            req_ready.trailers_modifications = None
        //        });

        //        self.body_context.finish_stream();
        //        return Action::Return(status);
        //    } else {
        //        return Action::Return(
        //            self.status_error("handle_trailers_response: No trailers to process", self.failure_mode_allow),
        //        );
        //    }
        //} else {
        //    // BUFFERED body
        //    let mut status =
        //        self.partial_status.take().unwrap_or_else(|| ProcessingStatus::RequestReady(ReadyStatus::default()));
        //    status.with_request_ready(|req_ready| {
        //        req_ready.trailers_modifications = trailers_response.header_mutation;
        //    });

        //    return Action::Return(status);
        //}
    }

    #[inline]
    pub fn is_body_streaming(&self) -> bool {
        matches!(
            self.state,
            ProcessingState::StreamingBody
                | ProcessingState::WaitingForBodyReply
        )
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
    pub fn is_headers_processing_planned(&self) -> bool {
        matches!(self.headers_mode, HeaderProcessingMode::Send)
    }

    #[inline]
    pub fn is_body_processing_planned(&self) -> bool {
        !matches!(self.body_context.body_mode, BodyProcessingMode::None)
    }

    #[inline]
    pub fn is_trailers_processing_planned(&self) -> bool {
        matches!(self.body_context.trailers_mode, TrailerProcessingMode::Send)
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
