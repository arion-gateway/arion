use crate::PolyBody;

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
    // WaitingForBodyReply,
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
    EndWithDirectResponse(http::Response<PolyBody>),
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
    pub clear_route_cache: bool,
}
