use crate::{body::timeout_body::TimeoutBody, PolyBody};

use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HeaderMutation;

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum Action<P> {
    Send(P),
    Return(ProcessingStatus),
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum ProcessingStatus {
    RequestReady(ReadyStatus),
    ResponseReady(ReadyStatus),
    HaltedOnError,
    EndWithDirectResponse(http::Response<TimeoutBody<PolyBody>>),
}

impl ProcessingStatus {
    pub fn with_request_ready<F>(&mut self, f: F)
    where
        F: FnOnce(&mut ReadyStatus),
    {
        if let ProcessingStatus::RequestReady(ready) = self {
            f(ready);
        }
    }

    pub fn with_response_ready<F>(&mut self, f: F)
    where
        F: FnOnce(&mut ReadyStatus),
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
