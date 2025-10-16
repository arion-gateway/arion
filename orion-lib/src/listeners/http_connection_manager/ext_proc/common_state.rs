use bytes::Bytes;

use http::Response;
use http_body_util::BodyStream;
use http_body_util::Empty;

use crate::{body::poly_body::BodySender, PolyBody};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::ext_proc::{
    BodyProcessingMode, TrailerProcessingMode,
};

use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HeaderMutation;

#[derive(Debug, Clone, Copy, Default)]
pub enum ObservabilityMode {
    #[default]
    Off,
    On
}

#[derive(Debug, Clone, Default)]
pub enum ProcessingState {
    WaitingForHeadersInput(ObservabilityMode),
    WaitingForHeadersReply,
    WaitingForBodyInput(ObservabilityMode),
    WaitingForBodyReply,
    StreamingBody(ObservabilityMode),
    StreamingBodyWaitingForReply,
    #[allow(dead_code)]
    FullDuplexStreamingBody,
    ProcessingTrailers(ObservabilityMode),
    #[default]
    Idle,
}

impl ProcessingState {
    pub fn observability_mode(&self) -> ObservabilityMode {
        match self {
            ProcessingState::WaitingForHeadersInput(mode)
            | ProcessingState::WaitingForBodyInput(mode)
            | ProcessingState::StreamingBody(mode)
            | ProcessingState::ProcessingTrailers(mode) => *mode,
            _ => ObservabilityMode::Off,
        }
    }

    pub fn is_observability_mode(&self) -> bool {
        matches!(
            self,
            ProcessingState::WaitingForHeadersInput(ObservabilityMode::On)
                | ProcessingState::WaitingForBodyInput(ObservabilityMode::On)
                | ProcessingState::StreamingBody(ObservabilityMode::On)
        )
    }
}

#[derive(Debug)]
pub enum ProcessingStatus {
    RequestIsReady {
        header_modifications: Option<HeaderMutation>,
        body_replacement: Option<PolyBody>,
        override_sending_response_headers: Option<bool>,
        override_sending_response_body: Option<bool>,
        clear_route_cache: bool,
    },
    ResponseIsReady {
        header_modifications: Option<HeaderMutation>,
        body_replacement: Option<PolyBody>,
    },
    HaltedOnError {
        restore_body: Option<PolyBody>,
    },
    EndWithDirectResponse(Response<PolyBody>),
}

pub struct BodyContext {
    pub body: Option<PolyBody>,
    pub body_stream: BodyStream<PolyBody>,
    pub body_sender: Option<BodySender>,
    pub body_mode: BodyProcessingMode,
    pub trailer_mode: TrailerProcessingMode,
    pub trailers: Option<http::HeaderMap>,
    pub buffered_chunk: Option<Bytes>,
}

impl BodyContext {
    pub fn new(body_mode: BodyProcessingMode, trailer_mode: TrailerProcessingMode) -> Self {
        Self {
            body: None,
            body_stream: BodyStream::new(PolyBody::from(Empty::<Bytes>::default())),
            body_sender: None,
            body_mode,
            trailer_mode,
            trailers: None,
            buffered_chunk: None,
        }
    }

    pub fn start_streaming(&mut self) {
        if let Some(body) = self.body.take() {
            self.body_stream = BodyStream::new(body);
            let (new_body, sender) = PolyBody::channel(16);
            self.body = Some(new_body);
            self.body_sender = Some(BodySender::new(sender));
        }
    }

    pub async fn make_new_body_channel(&mut self, data: Bytes) -> Result<(), ()> {
        let (new_body, sender) = PolyBody::channel(16);
        self.body = Some(new_body);
        let sender = BodySender::new(sender);
        if (sender.send_data(data).await).is_err() {
            return Err(());
        }
        self.body_sender = Some(sender);
        Ok(())
    }

    pub fn finish_stream(&mut self) {
        if let Some(sender) = self.body_sender.take() {
            drop(sender);
        }
    }
}
