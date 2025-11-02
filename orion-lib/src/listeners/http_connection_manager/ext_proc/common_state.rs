use bytes::Bytes;

use http::Response;
use http_body_util::BodyStream;
use http_body_util::Empty;
use tracing::debug;

use crate::body::channel_body::FrameBridge;
use crate::{body::poly_body::BodySender, PolyBody};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    BodyProcessingMode, TrailerProcessingMode,
};

use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HeaderMutation;

pub trait State {
    fn is_observability_mode(&self) -> bool;
}

#[derive(Debug, Clone, Default)]
pub enum ObservabilityState {
    #[default]
    WaitingForHeadersInput,
    WaitingForBodyInput,
    StreamingBody,
}

#[derive(Debug, Copy, Clone, Default)]
pub enum ProcessingState {
    #[default]
    WaitingForHeadersInput,
    WaitingForHeadersReply,
    WaitingForBodyInput,
    StreamingBody,
    WaitingForBodyReply,
    //StreamingBodyWaitingForReply,
    //FullDuplexStreamingBody,
}

impl State for ObservabilityState {
    fn is_observability_mode(&self) -> bool {
        true
    }
}

impl State for ProcessingState {
    fn is_observability_mode(&self) -> bool {
        false
    }
}

pub enum Action<P> {
    Send(P, ProcessingState),
    Return(ProcessingStatus),
}

#[derive(Debug)]
pub enum ProcessingStatus {
    RequestReady(ReadyStatus),
    ResponseReady(ReadyStatus),
    HaltedOnError,
    EndWithDirectResponse(Response<PolyBody>),
}

impl ProcessingStatus {
    pub fn with_request_ready<F>(&mut self, f: F)
    where
        F: FnOnce(&mut ReadyStatus) -> (),
    {
        if let ProcessingStatus::RequestReady(ready) = self {
            f(ready);
        }
    }

    pub fn with_response_ready<F>(&mut self, f: F)
    where
        F: FnOnce(&mut ReadyStatus) -> (),
    {
        if let ProcessingStatus::ResponseReady(ready) = self {
            f(ready);
        }
    }
}

#[derive(Debug, Default)]
pub struct ReadyStatus {
    pub headers_modifications: Option<HeaderMutation>,
    // pub body_replacement: Option<PolyBody>,
    // pub trailers_modifications: Option<HeaderMutation>,
    pub override_sending_response_headers: Option<bool>,
    pub override_sending_response_body: Option<bool>,
    pub clear_route_cache: bool,
}

// pub struct BodyContext {
//     pub frame_bridge: Option<FrameBridge>,
//     pub body_mode: BodyProcessingMode,
//     pub trailers: Option<http::HeaderMap>,
//     pub trailers_mode: TrailerProcessingMode,
//     pub buffered_chunk: Option<Bytes>,
// }

//impl BodyContext {
//    pub fn new(body_mode: BodyProcessingMode, trailer_mode: TrailerProcessingMode) -> Self {
//        Self {
//            frame_bridge: None,
//            body_mode,
//            trailers_mode: trailer_mode,
//            trailers: None,
//            buffered_chunk: None,
//        }
//    }

    //pub fn start_streaming(&mut self) {
    //    debug!(target: "ext_proc", "Starting body streaming...");
    //    if let Some(body) = self.body.take() {
    //        self.outbound_body_stream = BodyStream::new(body);
    //        let (new_body, sender) = PolyBody::new_stream_body(16);
    //        self.body = Some(new_body);
    //        self.inbound_body_sender = Some(BodySender::new(sender));
    //    } else {
    //        debug!(target: "ext_proc", "Could not start body streaming: no body present!");
    //    }
    //}

    //pub async fn make_new_body_channel(&mut self, data: Bytes) -> Result<(), orion_error::Error> {
    //    let (new_body, sender) = PolyBody::new_stream_body(16);
    //    self.body = Some(new_body);
    //    let sender = BodySender::new(sender);
    //    sender.send_data(data).await?;
    //    self.inbound_body_sender = Some(sender);
    //    Ok(())
    //}

    //pub fn finish_stream(&mut self) {
    //    debug!(target: "ext_proc", "Terminating body streaming (sender channel dropped)");
    //    if let Some(sender) = self.inbound_body_sender.take() {
    //        drop(sender);
    //    }
    //}
// }
