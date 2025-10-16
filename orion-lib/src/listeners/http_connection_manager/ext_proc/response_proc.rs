use crate::event_error::EventFailure;
use crate::listeners::http_connection_manager::ext_proc::common_state::{
    BodyContext, ObservabilityMode, ProcessingState, ProcessingStatus
};
use crate::listeners::http_connection_manager::ext_proc::mutation::apply_header_mutations;
use crate::listeners::http_connection_manager::ext_proc::worker_config::ExternalProcessingWorkerConfig;
use crate::{body::response_flags::ResponseFlags, listeners::synthetic_http_response::SyntheticHttpResponse, PolyBody};
use bytes::Bytes;
use http_body::Body;
use http_body_util::{BodyExt, Empty, Full};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    BodyProcessingMode, HeaderProcessingMode, ProcessingMode, TrailerProcessingMode,
};
use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::common_response::ResponseStatus;
use orion_data_plane_api::envoy_data_plane_api::envoy::{
    config::core::v3::{HeaderMap, HeaderValue},
    extensions::filters::http::ext_proc::v3::ProcessingMode as EnvoyProcessingMode,
    service::ext_proc::v3::{
        body_mutation::Mutation, processing_request::Request as ProcessingRequestType, BodyResponse, HeadersResponse,
        HttpBody, HttpHeaders, HttpTrailers, ProcessingRequest, TrailersResponse,
    },
};
use orion_format::types::ResponseFlags as FmtResponseFlags;
use std::collections::HashMap;
use tokio::sync::oneshot;
use tracing::warn;

pub struct ResponseProcessing {
    pub state: ProcessingState,
    pub body_context: BodyContext,
    partial_reply: Option<ProcessingStatus>,
    reply_channel: Option<oneshot::Sender<ProcessingStatus>>,
    header_mode: HeaderProcessingMode,
    http_version: Option<http::Version>,
    send_body_without_waiting_for_header_response: bool,
    failure_mode_allow: bool,
}

impl From<&ExternalProcessingWorkerConfig> for ResponseProcessing {
    fn from(config: &ExternalProcessingWorkerConfig) -> Self {
        let processing_mode = &config.processing_mode;

        let observability_mode = if config.observability_mode { ObservabilityMode::On } else { ObservabilityMode::Off };

        let initial_state = match processing_mode {
            ProcessingMode {
                    response_header_mode: HeaderProcessingMode::Default | HeaderProcessingMode::Send, ..
                } => ProcessingState::WaitingForHeadersInput(observability_mode),
            ProcessingMode {
                response_body_mode:
                    BodyProcessingMode::Buffered
                    | BodyProcessingMode::BufferedPartial
                    | BodyProcessingMode::Streamed
                    | BodyProcessingMode::FullDuplexStreamed,
                ..
            }
            | ProcessingMode { response_trailer_mode: TrailerProcessingMode::Send, .. } => ProcessingState::WaitingForBodyInput(observability_mode),
           _ => ProcessingState::Idle,
        };

        Self {
            state: initial_state,
            body_context: BodyContext::new(processing_mode.response_body_mode, processing_mode.response_trailer_mode),
            partial_reply: None,
            reply_channel: None,
            header_mode: processing_mode.response_header_mode,
            http_version: None,
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
            failure_mode_allow: config.failure_mode_allow,
        }
    }
}

impl ResponseProcessing {
    pub fn apply_mode_overrides(
        &mut self,
        envoy_mode: &EnvoyProcessingMode,
        allowed_override_modes: &[ProcessingMode],
    ) {
        let inactive_state = matches!(
            self.state,
            ProcessingState::WaitingForHeadersInput(_) | ProcessingState::WaitingForBodyInput(_) | ProcessingState::Idle
        );
        if inactive_state {
            if let Ok(mode) = HeaderProcessingMode::try_from(envoy_mode.response_header_mode) {
                if mode != HeaderProcessingMode::Default
                    && allowed_override_modes.iter().any(|allowed| allowed.response_header_mode == mode)
                {
                    self.header_mode = mode;
                    if matches!(self.header_mode, HeaderProcessingMode::Default | HeaderProcessingMode::Send) {
                        self.state = ProcessingState::WaitingForHeadersInput(self.state.observability_mode());
                    }
                }
            }
        }
        if let Ok(mode) = BodyProcessingMode::try_from(envoy_mode.response_body_mode) {
            if allowed_override_modes.iter().any(|allowed| allowed.response_body_mode == mode) {
                self.body_context.body_mode = mode;
                if !matches!(self.state, ProcessingState::WaitingForHeadersInput(_))
                    && matches!(
                        self.body_context.body_mode,
                        BodyProcessingMode::Buffered
                            | BodyProcessingMode::BufferedPartial
                            | BodyProcessingMode::Streamed
                            | BodyProcessingMode::FullDuplexStreamed
                    )
                {
                    self.state = ProcessingState::WaitingForBodyInput(self.state.observability_mode());
                }
            }
        }
        if let Ok(mode) = TrailerProcessingMode::try_from(envoy_mode.response_trailer_mode) {
            if allowed_override_modes.iter().any(|allowed| allowed.response_trailer_mode == mode) {
                self.body_context.trailer_mode = mode;
            }
        }
    }

    pub fn process_response(
        &mut self,
        headers: HttpHeaders,
        body: PolyBody,
        reply_channel: oneshot::Sender<ProcessingStatus>,
        http_version: http::Version,
    ) -> Option<ProcessingRequest> {
        self.reply_channel = Some(reply_channel);
        self.http_version = Some(http_version);
        if !body.is_end_stream() {
            self.body_context.body = Some(body);
        }
        match &self.state {
            ProcessingState::WaitingForHeadersInput(ObservabilityMode::On)=> {
                let processing_request = ProcessingRequest {
                    request: Some(ProcessingRequestType::ResponseHeaders(headers)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: true,
                    protocol_config: None,
                };
                if matches!(self.body_context.body_mode, BodyProcessingMode::Streamed)
                    && self.body_context.body.is_some()
                {
                    self.state = ProcessingState::StreamingBody(ObservabilityMode::On);
                    self.body_context.start_streaming();
                } else {
                    self.state = ProcessingState::Idle;
                }
                let status = ProcessingStatus::ResponseIsReady {
                    header_modifications: None,
                    body_replacement: self.body_context.body.take(),
                };
                self.exit_with_status(status);
                Some(processing_request)
            },
            ProcessingState::WaitingForHeadersInput(ObservabilityMode::Off) => {
                let processing_request = ProcessingRequest {
                    request: Some(ProcessingRequestType::ResponseHeaders(headers)),
                    metadata_context: None,
                    attributes: HashMap::default(),
                    observability_mode: false,
                    protocol_config: None,
                };
                if self.send_body_without_waiting_for_header_response
                    && matches!(self.body_context.body_mode, BodyProcessingMode::Streamed)
                    && self.body_context.body.is_some()
                {
                    self.body_context.start_streaming();
                    self.state = ProcessingState::StreamingBody(ObservabilityMode::Off);
                } else {
                    self.state = ProcessingState::WaitingForHeadersReply;
                }
                Some(processing_request)
            },
            _ => None,
        }
    }

    pub async fn handle_headers_response(&mut self, response: HeadersResponse) -> Option<ProcessingRequest> {
        match &self.state {
            ProcessingState::WaitingForHeadersReply | ProcessingState::StreamingBody(_) => {
                let mut status = if let Some(response_data) = response.response {
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
                                    return None;
                                },
                            };
                        let status = ProcessingStatus::ResponseIsReady {
                            header_modifications: response_data.header_mutation,
                            body_replacement,
                        };
                        if let Some(reply_channel) = self.reply_channel.take() {
                            let _ = reply_channel.send(status);
                        }
                        return None;
                    }
                    ProcessingStatus::ResponseIsReady {
                        header_modifications: response_data.header_mutation,
                        body_replacement: None,
                    }
                } else {
                    ProcessingStatus::ResponseIsReady { header_modifications: None, body_replacement: None }
                };
                if let Some(reply_channel) = self.reply_channel.take() {
                    if self.is_body_processing_planned() && self.body_context.body.is_some() {
                        self.state = ProcessingState::WaitingForBodyInput(ObservabilityMode::Off);
                        self.partial_reply = Some(status);
                        if let Some(body) = self.body_context.body.take() {
                            return self.process_body(body, reply_channel, None).await;
                        }
                    } else {
                        if let ProcessingStatus::ResponseIsReady { ref mut body_replacement, .. } = status {
                            *body_replacement = self.body_context.body.take();
                        }
                        let _ = reply_channel.send(status);
                    }
                }
                None
            },
            _ => None,
        }
    }

    pub async fn process_body(
        &mut self,
        body: PolyBody,
        reply_channel: oneshot::Sender<ProcessingStatus>,
        http_version: Option<http::Version>,
    ) -> Option<ProcessingRequest> {
        self.reply_channel = Some(reply_channel);
        if let Some(http_version) = http_version {
            self.http_version = Some(http_version);
        }
        match &self.state {
            ProcessingState::WaitingForBodyInput(observability_mode) => match self.body_context.body_mode {
                BodyProcessingMode::Buffered | BodyProcessingMode::BufferedPartial => {
                    let Ok(collected_body) = body.collect().await else {
                        self.exit_on_error(
                            "Failed to collect response body bytes for external processing",
                            self.failure_mode_allow,
                        );
                        return None;
                    };
                    if matches!(self.body_context.trailer_mode, TrailerProcessingMode::Send) {
                        if let Some(trailers) = collected_body.trailers() {
                            self.body_context.trailers = Some(trailers.clone());
                        }
                    }
                    let body_bytes = collected_body.to_bytes();
                    let http_body = HttpBody { body: body_bytes.into(), end_of_stream: true };
                    let processing_request = ProcessingRequest {
                        request: Some(ProcessingRequestType::ResponseBody(http_body)),
                        metadata_context: None,
                        attributes: HashMap::default(),
                        observability_mode: false,
                        protocol_config: None,
                    };
                    self.state = ProcessingState::WaitingForBodyReply;
                    Some(processing_request)
                },
                BodyProcessingMode::Streamed | BodyProcessingMode::FullDuplexStreamed => {
                    self.body_context.body = Some(body);
                    self.state = ProcessingState::StreamingBody(*observability_mode);
                    self.body_context.start_streaming();
                    None
                },
                BodyProcessingMode::None if matches!(self.body_context.trailer_mode, TrailerProcessingMode::Send) => {
                    let Ok(collected_body) = body.collect().await else {
                        self.exit_on_error(
                            "Failed to collect response body trailers for external processing",
                            self.failure_mode_allow,
                        );
                        return None;
                    };
                    let trailers = collected_body.trailers().cloned();
                    let body_bytes = collected_body.to_bytes();
                    if self.body_context.make_new_body_channel(body_bytes).await.is_err() {
                        self.exit_on_error(
                            "Failed to prepare response body trailers for external processing",
                            self.failure_mode_allow,
                        );
                        return None;
                    }
                    if let Some(reply_channel) = self.reply_channel.take() {
                        return self.process_trailers(trailers, Some(reply_channel), self.http_version);
                    }
                    None
                },
                BodyProcessingMode::None => None,
            },
            _ => None,
        }
    }

    pub async fn handle_body_chunk(&mut self, data: Bytes, end_of_stream: bool) -> Option<ProcessingRequest> {
        let http_body = HttpBody { body: data.to_vec(), end_of_stream };
        let processing_request = ProcessingRequest {
            request: Some(ProcessingRequestType::ResponseBody(http_body)),
            metadata_context: None,
            attributes: HashMap::default(),
            observability_mode: self.state.is_observability_mode(),
            protocol_config: None,
        };
        match &self.state {
            ProcessingState::StreamingBody(ObservabilityMode::On) => {
                if let Some(sender) = &self.body_context.body_sender {
                    let _ = sender.send_data(data).await;
                    if end_of_stream {
                        if let Some(trailers) = self.body_context.trailers.take() {
                            let _ = sender.send_trailers(trailers).await;
                        }
                        self.state = ProcessingState::Idle;
                    }
                }
                Some(processing_request)
            },
            ProcessingState::StreamingBody(ObservabilityMode::Off) => {
                self.state = ProcessingState::StreamingBodyWaitingForReply;
                Some(processing_request)
            },
            _ => Some(processing_request),
        }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn handle_body_response(&mut self, body_response: BodyResponse) -> Option<ProcessingRequest> {
        if body_response.response.is_none() {
            let status = self.partial_reply.take().unwrap_or(ProcessingStatus::ResponseIsReady {
                header_modifications: None,
                body_replacement: self.body_context.body.take(),
            });
            if let Some(reply_channel) = self.reply_channel.take() {
                let _ = reply_channel.send(status);
            }
            self.state = ProcessingState::Idle;
            self.body_context.finish_stream();
            return None;
        }
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
                            return None;
                        },
                    };
                    let mut status = self.partial_reply.take().unwrap_or(ProcessingStatus::ResponseIsReady {
                        header_modifications: None,
                        body_replacement: self.body_context.body.take(),
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
                            self.state = ProcessingState::Idle;
                            self.body_context.finish_stream();
                            return None;
                        }
                        self.partial_reply = Some(status);
                        let trailers = self.body_context.trailers.take();
                        return self.process_trailers(trailers, Some(reply_channel), self.http_version);
                    }
                }
                None
            },
            ProcessingState::StreamingBodyWaitingForReply | ProcessingState::FullDuplexStreamingBody => {
                if let Some(response_data) = body_response.response {
                    let body_mutation = response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation);
                    let mut end_of_stream: bool = false;
                    match body_mutation {
                        Some(Mutation::StreamedResponse(streamed_response)) => {
                            if let Some(sender) = &self.body_context.body_sender {
                                let _ = sender.send_data(streamed_response.body.into()).await;
                            }
                            if streamed_response.end_of_stream {
                                if matches!(self.body_context.trailer_mode, TrailerProcessingMode::Send) {
                                    self.state = ProcessingState::ProcessingTrailers(ObservabilityMode::Off);
                                } else {
                                    self.state = ProcessingState::Idle;
                                    self.body_context.finish_stream();
                                    end_of_stream = true;
                                }
                            } else if matches!(self.state, ProcessingState::StreamingBodyWaitingForReply) {
                                self.state = ProcessingState::StreamingBody(ObservabilityMode::Off);
                            }
                        },
                        Some(Mutation::Body(bytes)) => {
                            if let Some(sender) = &self.body_context.body_sender {
                                let _ = sender.send_data(bytes.into()).await;
                            }
                            if matches!(self.body_context.trailer_mode, TrailerProcessingMode::Send) {
                                self.state = ProcessingState::ProcessingTrailers(ObservabilityMode::Off);
                            } else {
                                self.state = ProcessingState::Idle;
                                self.body_context.finish_stream();
                                end_of_stream = true;
                            }
                        },
                        Some(Mutation::ClearBody(_)) | None => {
                            self.state = ProcessingState::Idle;
                            self.body_context.finish_stream();
                            end_of_stream = true;
                        },
                    }
                    let mut status = self.partial_reply.take().unwrap_or(ProcessingStatus::ResponseIsReady {
                        header_modifications: None,
                        body_replacement: None,
                    });
                    if let ProcessingStatus::ResponseIsReady { body_replacement: ref mut body, .. } = status {
                        if body.is_none() {
                            *body = self.body_context.body.take();
                        }
                    }
                    if end_of_stream {
                        if let Some(reply_channel) = self.reply_channel.take() {
                            let _ = reply_channel.send(status);
                        }
                    } else {
                        self.partial_reply = Some(status);
                        let trailers = self.body_context.trailers.take();
                        let reply_channel = self.reply_channel.take();
                        return self.process_trailers(trailers, reply_channel, self.http_version);
                    }
                }
                None
            },
            _ => None,
        }
    }

    pub fn process_trailers(
        &mut self,
        trailers: Option<http::HeaderMap>,
        reply_channel: Option<oneshot::Sender<ProcessingStatus>>,
        http_version: Option<http::Version>,
    ) -> Option<ProcessingRequest> {
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
            let trailers_to_send = HeaderMap { headers: header_values };
            let processing_request = ProcessingRequest {
                request: Some(ProcessingRequestType::ResponseTrailers(HttpTrailers {
                    trailers: Some(trailers_to_send),
                })),
                metadata_context: None,
                attributes: HashMap::default(),
                observability_mode: self.state.is_observability_mode(),
                protocol_config: None,
            };
            if self.state.is_observability_mode() {
                self.state = ProcessingState::Idle;
            } else {
                self.state = ProcessingState::ProcessingTrailers(ObservabilityMode::Off);
            }
            Some(processing_request)
        } else {
            if let Some(reply_channel) = self.reply_channel.take() {
                let status = self.partial_reply.take().unwrap_or(ProcessingStatus::ResponseIsReady {
                    header_modifications: None,
                    body_replacement: self.body_context.body.take(),
                });
                let _ = reply_channel.send(status);
                self.state = ProcessingState::Idle;
                self.body_context.finish_stream();
            }
            None
        }
    }

    pub async fn handle_trailers_response(&mut self, trailers_response: TrailersResponse) -> Option<ProcessingRequest> {
        if let Some(mut trailers) = self.body_context.trailers.take() {
            if !self.state.is_observability_mode() {
                if let Some(trailers_updates) = trailers_response.header_mutation {
                    let _ = apply_header_mutations(&mut trailers, &trailers_updates, None);
                }
            }
            match &self.state {
                ProcessingState::ProcessingTrailers(ObservabilityMode::Off) => {
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
                        self.body_context.finish_stream();
                    }
                },
                &ProcessingState::ProcessingTrailers(ObservabilityMode::On)=> {
                    if let Some(sender) = &self.body_context.body_sender {
                        let _ = sender.send_trailers(trailers).await;
                    }
                    self.state = ProcessingState::Idle;
                    self.body_context.finish_stream();
                },
                _ => {},
            }
        }
        None
    }

    pub fn handle_noop_response(&mut self) {
        if let Some(reply_channel) = self.reply_channel.take() {
            let status = self.partial_reply.take().unwrap_or(ProcessingStatus::ResponseIsReady {
                header_modifications: None,
                body_replacement: self.body_context.body.take(),
            });
            let _ = reply_channel.send(status);
            self.state = ProcessingState::Idle;
        }
    }

    pub fn is_awaiting_reply(&self) -> bool {
        self.reply_channel.is_some()
    }

    pub fn is_header_processing_planned(&self) -> bool {
        !matches!(self.header_mode, HeaderProcessingMode::Skip)
    }

    pub fn is_body_processing_planned(&self) -> bool {
        !matches!(self.body_context.body_mode, BodyProcessingMode::None)
    }

    pub fn is_accepting_body_data(&self) -> bool {
        matches!(
            self.state,
                ProcessingState::StreamingBody(_)
                | ProcessingState::FullDuplexStreamingBody
        )
    }

    pub fn exit_on_timeout(&mut self, failure_mode_allow: bool) {
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

    pub fn exit_on_error(&mut self, msg: &str, failure_mode_allow: bool) {
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

    pub fn exit_with_status(&mut self, status: ProcessingStatus) {
        if let Some(channel) = self.reply_channel.take() {
            let _ = channel.send(status);
        }
    }
}
