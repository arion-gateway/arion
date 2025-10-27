use bytes::Bytes;

use http::Response;
use http_body_util::BodyStream;
use http_body_util::Empty;
use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HttpHeaders;

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
    WaitingForHeadersInput,
    WaitingForBodyInput,
    StreamingBody,
    ProcessingTrailers,
    #[default]
    Idle,
}

#[derive(Debug, Clone, Default)]
pub enum ProcessingState {
    WaitingForHeadersInput,
    WaitingForHeadersReply,
    WaitingForBodyInput,
    WaitingForBodyReply,
    StreamingBody,
    StreamingBodyWaitingForReply,
    #[allow(dead_code)]
    FullDuplexStreamingBody,
    ProcessingTrailers,
    #[default]
    Idle,
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


#[derive(Debug)]
pub enum ExtProcStatus {
    RequestReady(RequestReady),
    ResponseReady(ResponseReady),
    HaltedOnError,
    EndWithDirectResponse(Response<PolyBody>),
}

impl ExtProcStatus {
    pub fn with_request_ready<F>(&mut self, f: F)
        where F: FnOnce(&mut RequestReady) -> ()
    {
        if let ExtProcStatus::RequestReady(ref mut ready) = self {
            f(ready);
        }
    }

    pub fn with_response_ready<F>(&mut self, f: F)
        where F: FnOnce(&mut ResponseReady) -> ()
    {
        if let ExtProcStatus::ResponseReady(ref mut ready) = self {
            f(ready);
        }
    }
}

#[derive(Debug, Default)]
pub struct RequestReady {
    pub headers_modifications: Option<HeaderMutation>,
    pub body_replacement: Option<PolyBody>,
    pub trailers_modifications: Option<HeaderMutation>,
    pub override_sending_response_headers: Option<bool>,
    pub override_sending_response_body: Option<bool>,
    pub clear_route_cache: bool,
}

#[derive(Debug, Default)]
pub struct ResponseReady {
    pub headers_modifications: Option<HeaderMutation>,
    pub body_replacement: Option<PolyBody>,
    pub trailers_modifications: Option<HeaderMutation>,
}

pub struct BodyContext {
    pub body: Option<PolyBody>,
    pub body_mode: BodyProcessingMode,
    pub trailers: Option<http::HeaderMap>,
    pub trailers_mode: TrailerProcessingMode,
    pub outbound_body_stream: BodyStream<PolyBody>,
    pub inbound_body_sender: Option<BodySender>,
    pub buffered_chunk: Option<Bytes>,
}

impl BodyContext {
    pub fn new(body_mode: BodyProcessingMode, trailer_mode: TrailerProcessingMode) -> Self {
        Self {
            body: None,
            outbound_body_stream: BodyStream::new(PolyBody::from(Empty::<Bytes>::default())),
            inbound_body_sender: None,
            body_mode,
            trailers_mode: trailer_mode,
            trailers: None,
            buffered_chunk: None,
        }
    }

    pub fn start_streaming(&mut self) {
        // TODO: build the body with trailers (if any)
        if let Some(body) = self.body.take() {
            self.outbound_body_stream = BodyStream::new(body);
            let (new_body, sender) = PolyBody::new_stream_body(16);
            self.body = Some(new_body);
            self.inbound_body_sender = Some(BodySender::new(sender));
        }
    }

    pub async fn make_new_body_channel(&mut self, data: Bytes) -> Result<(), ()> {
        let (new_body, sender) = PolyBody::new_stream_body(16);
        self.body = Some(new_body);
        let sender = BodySender::new(sender);
        if (sender.send_data(data).await).is_err() {
            return Err(());
        }
        self.inbound_body_sender = Some(sender);
        Ok(())
    }

    pub fn finish_stream(&mut self) {
        if let Some(sender) = self.inbound_body_sender.take() {
            drop(sender);
        }
    }
}
