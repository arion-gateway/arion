// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::body::channel_body::FrameBridge;
use crate::event_error::EventFailure;
use crate::listeners::http_connection_manager::ext_proc::kind;
use crate::listeners::http_connection_manager::ext_proc::mutation::apply_trailer_mutations;
use crate::listeners::http_connection_manager::ext_proc::r#override::{
    OverridableBodyMode, OverridableGlobalModes, OverridableModeSelector,
};
use crate::listeners::http_connection_manager::ext_proc::status::{ProcessingStatus, ReadyStatus};
use crate::listeners::http_connection_manager::ext_proc::worker_config::ExternalProcessingWorkerConfig;
use crate::listeners::http_connection_manager::ext_proc::EnvoyHeaderMap;
use crate::utils::truncated_debug::TruncatedDebug;
use crate::{body::response_flags::ResponseFlags, listeners::synthetic_http_response::SyntheticHttpResponse};
use bytes::{Bytes, BytesMut};
use http_body::Frame;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    BodyProcessingMode, HeaderProcessingMode, ProcessingMode, RouteCacheAction, TrailerProcessingMode,
};
use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::HeaderMap as ProstHeaderMap;
use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::HeaderValue as ProstHeaderValue;
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

#[allow(dead_code)]
#[derive(Copy, Clone, Debug)]
pub enum Phase {
    Headers,
    Body,
    Trailers,
}

pub enum BufferedData {
    Single(Bytes),
    Merged(BytesMut),
}

pub struct FramesBuffer {
    data_buffer: Option<BufferedData>,
    trailers_buffer: Option<Frame<Bytes>>,
    last_merge: Option<Instant>,
    count: u32,
    frame_merge_limit: u32,
    frame_merge_window: Duration,
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
        let is_data = frame.is_data();

        if is_data {
            // DATA
            let new_data = frame.into_data().unwrap_or_else(|_| unreachable!());

            if self.trailers_buffer.is_some() {
                // This case should never occur, as no frames are expected after the final TRAILERS.
                warn!(target: "ext_proc", "FramesBuffer::merge_frame: unexpected frame after TRAILERS!");
            } else if let Some(buf) = self.data_buffer.take() {
                self.count += 1;
                match buf {
                    BufferedData::Single(old_data) => {
                        let mut new_buf = BytesMut::with_capacity(old_data.len() + new_data.len());
                        new_buf.extend_from_slice(&old_data);
                        new_buf.extend_from_slice(new_data.as_ref());
                        self.data_buffer = Some(BufferedData::Merged(new_buf));
                    },
                    BufferedData::Merged(mut m) => {
                        m.extend_from_slice(new_data.as_ref());
                        self.data_buffer = Some(BufferedData::Merged(m));
                    },
                }
            } else {
                self.count = 1;
                self.data_buffer = Some(BufferedData::Single(new_data));
            }

            let emit = self.count >= self.frame_merge_limit
                || now.duration_since(self.last_merge.unwrap_or(now)) >= self.frame_merge_window;

            self.last_merge = Some(now);

            if emit {
                self.count = 0;
                self.data_buffer.take().map(|buf| match buf {
                    BufferedData::Single(b) => Frame::data(b),
                    BufferedData::Merged(b) => Frame::data(b.freeze()),
                })
            } else {
                None
            }
        } else {
            // TRAILERS
            let data = self.data_buffer.take().map(|buf| match buf {
                BufferedData::Single(b) => Frame::data(b),
                BufferedData::Merged(b) => Frame::data(b.freeze()),
            });
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
            Some(match buf {
                BufferedData::Single(b) => Frame::data(b),
                BufferedData::Merged(b) => Frame::data(b.freeze()),
            })
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
pub struct Processing<M: kind::Mode, Msg: kind::MessageKind> {
    http_headers: Option<EnvoyHeaderMap>,
    pub trailers: Option<http::HeaderMap>,
    pub frame_bridge: protected::FrameBridge,
    pub reply_channel: Option<oneshot::Sender<ProcessingStatus>>,
    http_version: Option<http::Version>,
    send_body_without_waiting_for_header_response: bool,
    pub failure_mode_allow: bool,
    streaming_body_enabled: bool,
    end_of_stream: bool,
    pub frames_buffer: FramesBuffer,
    pub inflight_frames: SmallVec<[Frame<Bytes>; 2]>,
    pub parked_trailers: Option<Frame<Bytes>>,
    pub headers_ready_status: Option<ReadyStatus>, // ready_status saved headers response and fused with body response before in Action::Return
    _mode: std::marker::PhantomData<M>,
    _msg: std::marker::PhantomData<Msg>,
}

pub(crate) mod protected {
    use bytes::Bytes;
    use futures::Stream;
    use http_body::Frame;
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::sync::mpsc;
    use tracing::{debug, warn};

    use crate::{
        body::channel_body::FrameBridge as PubFrameBridge,
        listeners::http_connection_manager::ext_proc::{kind, processing::Processing, status::ProcessingStatus},
    };

    #[derive(Default)]
    pub struct FrameBridge {
        inner: PubFrameBridge,
    }

    impl Stream for FrameBridge {
        type Item = Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Pin::new(&mut self.inner).poll_next(cx)
        }
    }

    // Zero-Sized Type acting as a token/proof.
    // The private field `()` prevents external instantiation.
    #[derive(Clone, Copy)]
    pub struct ReturnStatusProof(());

    impl<M: kind::Mode, Msg: kind::MessageKind> Processing<M, Msg> {
        #[inline]
        #[must_use = "ProofReturn must be used to ensure correct usage of the API"]
        pub fn return_status(&mut self, status: ProcessingStatus, msg: &str) -> ReturnStatusProof {
            if let Some(reply_channel) = self.reply_channel.take() {
                debug!(target: "ext_proc", "{msg} @{kind}: -> return {status:?}", kind = Msg::NAME);
                if reply_channel.send(status).is_err() {
                    warn!(target: "ext_proc", "{msg} @{kind}: failed to send response status through reply channel", kind = Msg::NAME);
                }
            }

            ReturnStatusProof(())
        }

        #[inline]
        #[must_use = "ProofReturn must be used to ensure correct usage of the API"]
        pub fn make_proof(&self) -> Option<ReturnStatusProof> {
            self.reply_channel.is_none().then_some(ReturnStatusProof(()))
        }
    }

    impl FrameBridge {
        pub(crate) fn new(frame_bridge: PubFrameBridge) -> Self {
            Self { inner: frame_bridge }
        }

        #[inline]
        pub async fn inject_frame(
            &mut self,
            frame: Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>,
            _proof: ReturnStatusProof,
        ) -> Result<(), mpsc::error::SendError<Result<Frame<Bytes>, Box<dyn std::error::Error + Send + Sync>>>>
        {
            // to inject a frame a single frame the proof return is not be required...
            self.inner.inject_frame(frame).await
        }

        #[inline]
        pub async fn drain_and_inject(&mut self, _proof: ReturnStatusProof) {
            let () = self.inner.drain_and_inject().await;
        }

        /// Returns the number of frames injected into the `ChannelBody` so far.
        #[allow(dead_code)]
        pub fn injected_frames(&self) -> usize {
            self.inner.injected_frames()
        }

        #[inline]
        pub fn close(&mut self, timeout_active: Option<&mut bool>) {
            if let Some(timeout_active) = timeout_active {
                *timeout_active = false;
            }
            self.inner.close();
        }

        #[inline]
        pub async fn drain_and_close(
            &mut self,
            proof: ReturnStatusProof,
            frames: impl IntoIterator<Item = Frame<Bytes>>,
            trailers: Option<Frame<Bytes>>,
            timeout_active: Option<&mut bool>,
        ) {
            // 1. frames to inject first...
            for frame in frames {
                _ = self.inject_frame(Ok(frame), proof).await;
            }

            // 2. remaining frames if not yet processed...
            () = self.drain_and_inject(proof).await;

            // 3. frame bridge could be already drained, but we might have parked trailers to inject...
            if let Some(trailers) = trailers {
                _ = self.inject_frame(Ok(trailers), proof).await;
            }

            self.close(timeout_active);
        }

        #[inline]
        #[allow(dead_code)]
        #[must_use]
        pub fn is_closed(&self) -> bool {
            self.inner.is_closed()
        }

        #[inline]
        #[allow(dead_code)]
        #[must_use]
        pub fn is_open(&self) -> bool {
            !self.inner.is_closed()
        }

        /// Check if the `FrameBridge` has been constructed with an empty body.
        pub fn source_has_non_empty_body(&self) -> Option<bool> {
            self.inner.source_has_non_empty_body()
        }

        /// Check if the `FrameBridge` has been constructed with trailers.
        pub fn source_has_pending_trailers(&self) -> Option<bool> {
            self.inner.source_has_pending_trailers()
        }

        /// Check if the `FrameBridge` has been constructed with a non-empty body or trailers.
        pub fn source_has_non_empty_body_or_trailers(&self) -> bool {
            self.inner.source_has_non_empty_body_or_trailers()
        }
    }
}

impl<M: kind::Mode + Default, Msg: kind::MessageKind> From<&ExternalProcessingWorkerConfig> for Processing<M, Msg> {
    fn from(config: &ExternalProcessingWorkerConfig) -> Self {
        debug!(target: "ext_proc", "From<&ExternalProcessingWorkerConfig for Processing<>");

        Self {
            send_body_without_waiting_for_header_response: config.send_body_without_waiting_for_header_response,
            failure_mode_allow: config.failure_mode_allow,

            http_headers: None,
            frame_bridge: protected::FrameBridge::default(),
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

impl<Msg: kind::MessageKind + OverridableModeSelector> Processing<kind::Processing, Msg> {
    #[must_use = "must handle the returned Processing Request"]
    #[allow(clippy::too_many_lines)]
    pub async fn handle_headers_response(
        &mut self,
        mut headers_response: HeadersResponse,
        route_cache_action: &RouteCacheAction,
        override_mode: &OverridableGlobalModes,
        timeout_active: &mut bool,
    ) -> Option<ProcessingRequest> {
        // NB: headers_response.response cannot be None here (prost artifact). We can't simply unwrap
        // the option because it's forbidden by our clippy rules

        if let Some(mut response_data) = headers_response.response.take() {
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
                // let's park the ReadyStatus to delay the return to orion and to fuse later
                //  with the ReadyStatus in the handle_body_response.
                self.headers_ready_status = Some(ReadyStatus {
                    headers_modifications: response_data.header_mutation.take(),
                    clear_route_cache: should_clear_route_cache,
                });

                let stream_body_enabled = self.try_enable_streaming_body();
                debug!(target: "ext_proc", "handle_headers_response: trying to enable streaming body: {stream_body_enabled}");
            } else {
                let headers_modifications = response_data.header_mutation.take();
                let proof = self.make_proof().unwrap_or_else(|| {
                    let status = if Msg::IS_REQUEST {
                        ProcessingStatus::RequestReady(ReadyStatus {
                            clear_route_cache: should_clear_route_cache,
                            headers_modifications,
                        })
                    } else {
                        ProcessingStatus::ResponseReady(ReadyStatus {
                            clear_route_cache: should_clear_route_cache,
                            headers_modifications,
                        })
                    };

                    self.return_status(
                        status,
                        "handle_headers_response: returning status after processing headers response",
                    )
                });

                if matches!(embedded_status, ResponseStatus::ContinueAndReplace) {
                    debug!(target: "ext_proc", "handle_headers_response: ResponseStatus:ContinueAndReplace");
                    let body_replacement =
                        match response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation) {
                            Some(Mutation::Body(bytes)) => Some(Frame::data(bytes.into())),
                            Some(Mutation::ClearBody(true)) => Some(Frame::data(Bytes::new())),
                            Some(Mutation::ClearBody(false)) | None => None,
                            Some(Mutation::StreamedResponse(chunk)) => Some(Frame::data(chunk.body.into())),
                        };

                    let status = if Msg::IS_REQUEST {
                        ProcessingStatus::RequestReady(ReadyStatus {
                            headers_modifications: response_data.header_mutation,
                            clear_route_cache: should_clear_route_cache,
                        })
                    } else {
                        ProcessingStatus::ResponseReady(ReadyStatus {
                            headers_modifications: response_data.header_mutation,
                            clear_route_cache: should_clear_route_cache,
                        })
                    };

                    // Witness Token, this is the proof that we already returned a status to the Orion
                    // to avoid possible deadlock when injecting frames in the bridge.

                    let proof = self.return_status(
                        status,
                        "handle_headers_response: returning status after processing headers response",
                    );

                    // replace body if specified
                    if let Some(new_body) = body_replacement {
                        debug!(target: "ext_proc", "handle_headers_response: body replacement requested: {new_body:?}");
                        _ = self.frame_bridge.inject_frame(Ok(new_body), proof).await;
                    }

                    // replace trailers if specified
                    if let Some(trailers) = response_data.trailers {
                        debug!(target: "ext_proc", "handle_headers_response: trailers replacement requested: {trailers:?}");
                        let new_trailers: http::HeaderMap = EnvoyHeaderMap(trailers).into();
                        _ = self.frame_bridge.inject_frame(Ok(Frame::trailers(new_trailers)), proof).await;
                    }

                    debug!(target: "ext_proc", "frame bridge closed (handle header response)!");
                    self.frame_bridge.close(Some(timeout_active));
                    self.set_streaming_body(false);
                } else {
                    debug!(target: "ext_proc", "handle_headers_response: ResponseStatus:Continue: headers processed: should_process_body:{}, should_process_trailers:{}",
                        override_mode.should_process_body::<Msg>(), override_mode.should_process_trailers::<Msg>());

                    if !self.frame_bridge.source_has_non_empty_body_or_trailers()
                        || (!override_mode.should_process_body::<Msg>()
                            && !override_mode.should_process_trailers::<Msg>())
                    {
                        debug!(target: "ext_proc", "handle_headers_response: complete to stream original body and close!");
                        self.frame_bridge.drain_and_inject(proof).await;
                        self.frame_bridge.close(Some(timeout_active));
                        self.set_streaming_body(false);
                    } else {
                        let stream_body_enabled = self.try_enable_streaming_body();
                        debug!(target: "ext_proc", "handle_headers_response: enable streaming body: {stream_body_enabled}");
                    }
                }
            }
        } else {
            unreachable!("headers_response.response should never be None (prost artifact), but clippy doesn't allow unwrapping the option");
        }

        *timeout_active = false;
        None
    }

    #[allow(clippy::too_many_lines)]
    #[must_use = "must handle the returned Processing Request"]
    pub async fn handle_body_response(
        &mut self,
        mut body_response: BodyResponse,
        route_cache_action: Option<&RouteCacheAction>,
        timeout_active: &mut bool,
    ) -> Option<ProcessingRequest> {
        debug!(target: "ext_proc", "handle_body_response: inflight frames ({})", self.inflight_frames.len());

        // NB: body_response.response cannot be None here (prost artifact). We can't simply unwrap
        // the option because it's forbidden by our clippy rules
        if let Some(mut response_data) = body_response.response.take() {
            let chunk_replacement = match response_data.body_mutation.and_then(|body_mutation| body_mutation.mutation) {
                Some(Mutation::Body(bytes)) => Some(Frame::data(bytes.into())),
                Some(Mutation::ClearBody(true)) => Some(Frame::data(Bytes::new())),
                Some(Mutation::ClearBody(false)) | None => None,
                Some(Mutation::StreamedResponse(chunk)) => Some(Frame::data(chunk.body.into())),
            };

            let proof = self.make_proof().unwrap_or_else(|| {
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
                            ready_status.and_then(|status| status.headers_modifications),
                            response_data.header_mutation.take(),
                        );
                    });
                } else {
                    debug!(target: "ext_proc", "handle_body_response: response status {status:?}");
                    status.with_response_ready(|resp_ready| {
                        let ready_status = self.headers_ready_status.take();
                        resp_ready.clear_route_cache = should_clear_route_cache
                            || ready_status.as_ref().map(|status| status.clear_route_cache).unwrap_or(false);
                        resp_ready.headers_modifications = Self::concat_header_mutations(
                            ready_status.and_then(|status| status.headers_modifications),
                            response_data.header_mutation.take(),
                        );
                    });
                }

                self.return_status(status, "handle_body_response: returning status after processing body response")
            });

            if let Some(new_chunk) = chunk_replacement {
                debug!(target: "ext_proc", "handle_body_response: chunk replacement -> {new_chunk:?}");
                _ = self.frame_bridge.inject_frame(Ok(new_chunk), proof).await;
                if !self.inflight_frames.is_empty() {
                    self.inflight_frames.drain(0..1);
                }
            } else {
                debug!(target: "ext_proc", "handle_body_response: no chunk replacement requested");
                if !self.inflight_frames.is_empty() {
                    if let Some(frame) = self.inflight_frames.drain(0..1).next() {
                        _ = self.frame_bridge.inject_frame(Ok(frame), proof).await;
                    }
                }
            }

            let embedded_status = ResponseStatus::try_from(response_data.status).unwrap_or(ResponseStatus::Continue);

            if matches!(embedded_status, ResponseStatus::ContinueAndReplace) {
                debug!(target: "ext_proc", "handle_body_response: CONTINUE_AND_REPLACE: closing the frame bridge.");
                self.frame_bridge.close(Some(timeout_active));
                self.set_streaming_body(false);
                return None;
            }

            if self.end_of_stream() && self.inflight_frames.is_empty() {
                debug!(target: "ext_proc", "handle_body_response: end_of_stream (closing frame bridge) -> trailers: {:?}", self.parked_trailers);
                let trailers = std::mem::take(&mut self.parked_trailers);
                self.frame_bridge.drain_and_close(proof, None, trailers, Some(timeout_active)).await;
                self.set_streaming_body(false);
            }

            return None;
        }

        unreachable!("body_response.response should never be None (prost artifact), but clippy doesn't allow unwrapping the option");
    }

    #[inline]
    fn concat_header_mutations(left: Option<HeaderMutation>, right: Option<HeaderMutation>) -> Option<HeaderMutation> {
        match (left, right) {
            (None, None) => None,
            (None, r @ Some(_)) => r,
            (l @ Some(_), None) => l,
            (Some(mut l), Some(mut r)) => {
                l.set_headers.append(&mut r.set_headers);
                l.remove_headers.append(&mut r.remove_headers);
                Some(l)
            },
        }
    }

    #[must_use = "must handle the returned Action"]
    pub async fn handle_trailers_response(
        &mut self,
        mut trailers_response: TrailersResponse,
        timeout_active: &mut bool,
    ) -> Option<ProcessingRequest> {
        if let Some(mut trailers) = self.trailers.take() {
            let proof = self.make_proof().unwrap_or_else(|| {
                let status = ProcessingStatus::ready::<Msg>();
                self.return_status(status, "handle_trailers_response: returning status in trailers response")
            });

            // update the local version of trailers, if required if let Some(trailers) = self.body_context.trailers.as_mut() {
            debug!(target: "ext_proc", "handle_trailers_response: mutating trailers...");
            if let Some(trailers_updates) = trailers_response.header_mutation.take() {
                let _ = apply_trailer_mutations(&mut trailers, trailers_updates, None).ok();
            }

            _ = self.frame_bridge.inject_frame(Ok(Frame::trailers(trailers)), proof).await;

            self.frame_bridge.close(Some(timeout_active));
            self.set_streaming_body(false);
            None
        } else {
            debug!(target: "ext_proc", "frame bridge closed (handle trailers response)!");

            let _proof = self.make_proof().unwrap_or_else(|| {
                let status =
                    self.status_error("handle_trailers_response: No trailers to process", self.failure_mode_allow);
                self.return_status(status, "handle_trailers_response: returning status in trailers response")
            });

            self.frame_bridge.close(Some(timeout_active));
            self.set_streaming_body(false);

            None
        }
    }
}

impl<M: kind::Mode + Default, Msg: kind::MessageKind + OverridableModeSelector> Processing<M, Msg> {
    #[must_use = "must handle the returned Action"]
    #[allow(clippy::too_many_arguments)]
    pub async fn process(
        &mut self,
        headers: Option<EnvoyHeaderMap>,
        frame_bridge: FrameBridge,
        reply_channel: oneshot::Sender<ProcessingStatus>,
        http_version: http::Version,
        override_mode: &OverridableGlobalModes,
    ) -> Option<ProcessingRequest> {
        debug!(target: "ext_proc", "process: processing headers/body/trailers started");

        self.reply_channel = Some(reply_channel);
        self.http_version = Some(http_version);
        self.http_headers = headers;
        self.frame_bridge = protected::FrameBridge::new(frame_bridge);

        //
        // 1) process headers if available and configured...
        //
        if self.http_headers.is_some() && override_mode.should_process_headers::<Msg>() {
            debug!(target: "ext_proc", "process: processing headers...");
            return self.process_headers(override_mode).await;
        }

        //
        // 2) headers are skipped, let's process the body...
        //

        if self.frame_bridge.source_has_non_empty_body_or_trailers()
            && (override_mode.should_process_body::<Msg>() || override_mode.should_process_trailers::<Msg>())
        {
            debug!(target: "ext_proc", "process: processing body and trailers...");
            return self.process_body_and_trailers(override_mode);
        }

        debug!(target: "ext_proc", "process: nothing to do!");

        //
        // 3) Nothing to do with this request or response.
        //

        let proof = self.return_status(ProcessingStatus::ready::<Msg>(), "No action required");
        self.frame_bridge.drain_and_inject(proof).await;
        self.frame_bridge.close(None);
        None
    }

    #[must_use = "must handle the returned Action"]
    async fn process_headers(&mut self, override_mode: &OverridableGlobalModes) -> Option<ProcessingRequest> {
        debug!(target: "ext_proc", "process_request headers {:?}", self.http_headers);
        let Some(envmap) = self.http_headers.take() else {
            let status_error =
                self.status_error("process_request: Unexpected missing headers!", self.failure_mode_allow);

            let proof = self.return_status(status_error, "Unexpected missing headers!");
            self.frame_bridge.drain_and_inject(proof).await;
            self.frame_bridge.close(None);
            self.set_streaming_body(false);
            return None;
        };

        self.update_end_stream(Phase::Headers, override_mode);

        let processing_request = if Msg::IS_REQUEST {
            ProcessingRequest {
                request: Some(ProcessingRequestType::RequestHeaders(HttpHeaders {
                    headers: Some(envmap.0),
                    attributes: HashMap::default(),
                    end_of_stream: self.end_of_stream(),
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
                    end_of_stream: self.end_of_stream(),
                })),
                metadata_context: None,
                attributes: HashMap::default(),
                observability_mode: M::OBSERVABILITY,
                protocol_config: None,
            }
        };

        let send_body_or_trailers = !self.end_of_stream();

        if M::OBSERVABILITY || (self.send_body_without_waiting_for_header_response && send_body_or_trailers) {
            // force enable streaming body. Note: in observability mode we want to enable streaming body regardless of the presence of body/trailers
            // to properly handle the termination condition.
            self.set_streaming_body(true);
        }

        Some(processing_request)
    }

    #[must_use = "must handle the returned Action"]
    fn process_body_and_trailers(&mut self, override_mode: &OverridableGlobalModes) -> Option<ProcessingRequest> {
        debug!(target: "ext_proc", "process: body and trailers (headers are skipped)...");

        let streaming_enabled = self.try_enable_streaming_body();

        if M::OBSERVABILITY {
            _ = self.return_status(ProcessingStatus::ready::<Msg>(), "observability!");
            return None;
        }

        if matches!(
            override_mode.body_mode::<Msg>(),
            OverridableBodyMode::Buffered | OverridableBodyMode::BufferedPartial
        ) && self.frame_bridge.source_has_non_empty_body() == Some(true)
            && override_mode.should_process_body::<Msg>()
        {
            if streaming_enabled {
                return None;
            }
            debug!(target: "ext_proc", "process_body_and_trailers: no body/trailers to stream (internal error)");
        }

        _ = self.return_status(ProcessingStatus::ready::<Msg>(), "process_body_and_trailers (ready status)!");
        None
    }

    #[must_use = "must handle the returned Action"]
    pub fn handle_outgoing_body_chunk(
        &mut self,
        mut chunk: Frame<Bytes>,
        end_of_stream: bool,
    ) -> Option<ProcessingRequest> {
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
        } else if let Some(trailers) = chunk.trailers_mut() {
            // TRAILERS
            let data = std::mem::take(trailers);

            let mut headers_vec = Vec::with_capacity(data.len());
            for (name, value) in &data {
                let header_name = name.as_str();
                let header_value = if let Ok(value_str) = value.to_str() {
                    ProstHeaderValue { key: header_name.to_owned(), value: value_str.to_owned(), raw_value: Vec::new() }
                } else {
                    ProstHeaderValue {
                        key: header_name.to_owned(),
                        value: String::default(),
                        raw_value: value.as_bytes().into(),
                    }
                };
                headers_vec.push(header_value);
            }
            let envoy_trailers = EnvoyHeaderMap(ProstHeaderMap { headers: headers_vec });

            // store trailers for potential update later WITHOUT CLONING
            self.trailers = Some(data);

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
            let status_error = self.status_error(msg, self.failure_mode_allow);
            _ = self.return_status(status_error, "handle_body_chunk: unexpected non-data frame to send");
            return None;
        };

        debug!(target: "ext_proc", "handle_body_chunk: prepared processing_request: {:?}", TruncatedDebug::<_,1024>(&processing_request));
        Some(processing_request)
    }

    pub fn status_timeout(&mut self, failure_mode_allow: bool) -> ProcessingStatus {
        if failure_mode_allow {
            ProcessingStatus::HaltedOnError
        } else {
            let http_version = self.http_version.unwrap_or(http::Version::HTTP_11);
            ProcessingStatus::EndWithDirectResponse(Box::new(
                SyntheticHttpResponse::gateway_timeout(
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::UPSTREAM_REQUEST_TIMEOUT),
                )
                .into_response(http_version),
            ))
        }
    }

    #[inline]
    pub fn status_error(&mut self, msg: &str, failure_mode_allow: bool) -> ProcessingStatus {
        if failure_mode_allow {
            ProcessingStatus::HaltedOnError
        } else {
            let http_version = self.http_version.unwrap_or(http::Version::HTTP_11);
            ProcessingStatus::EndWithDirectResponse(Box::new(
                SyntheticHttpResponse::internal_server_error(
                    EventFailure::ExtProcError.into(),
                    ResponseFlags(FmtResponseFlags::NO_FILTER_CONFIG_FOUND),
                )
                .with_body(msg.to_owned())
                .into_response(http_version),
            ))
        }
    }

    #[inline]
    pub fn status_internal_error(&mut self, msg: &str) -> ProcessingStatus {
        let http_version = self.http_version.unwrap_or(http::Version::HTTP_11);
        ProcessingStatus::EndWithDirectResponse(Box::new(
            SyntheticHttpResponse::internal_server_error(
                EventFailure::ExtProcError.into(),
                ResponseFlags(FmtResponseFlags::UNAUTHORIZED_EXTERNAL_SERVICE),
            )
            .with_body(msg.to_owned())
            .into_response(http_version),
        ))
    }

    #[inline]
    fn update_end_stream(&mut self, phase: Phase, override_mode: &OverridableGlobalModes) {
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

        self.end_of_stream = match phase {
            // In the Headers phase, it is the end of the stream ONLY IF no body and no trailers will follow.
            Phase::Headers => !will_send_body && !will_send_trailers,

            // In the Body phase, it is the end of the stream ONLY IF no trailers will follow.
            Phase::Body => !will_send_trailers,

            // The Trailers phase is strictly the last element, so it always closes the stream.
            Phase::Trailers => true,
        };
    }

    #[inline]
    #[must_use]
    pub fn end_of_stream(&self) -> bool {
        self.end_of_stream
    }

    #[inline]
    #[allow(dead_code)]
    pub fn set_end_of_stream(&mut self, value: bool) {
        self.end_of_stream = value;
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
            self.frame_bridge.close(None);
            false
        }
    }

    #[inline]
    pub fn set_streaming_body(&mut self, value: bool) {
        debug!(target: "ext_proc", "set streaming to {value}.");
        self.streaming_body_enabled = value;
        if !value {
            self.end_of_stream = true;
        }
    }

    #[inline]
    pub fn streaming_body_enabled(&self) -> bool {
        self.streaming_body_enabled
    }
}
