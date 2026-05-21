use crate::{listeners::http_connection_manager::ext_proc::kind::MessageKind, OrionResponseBody};

use orion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HeaderMutation;

#[derive(Debug)]
pub enum ProcessingStatus {
    RequestReady(ReadyStatus),
    ResponseReady(ReadyStatus),
    HaltedOnError,
    EndWithDirectResponse(Box<http::Response<OrionResponseBody>>),
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

    #[inline]
    pub fn ready<M: MessageKind>() -> ProcessingStatus {
        if M::IS_REQUEST {
            Self::RequestReady(ReadyStatus::default())
        } else {
            Self::ResponseReady(ReadyStatus::default())
        }
    }
}

#[derive(Debug, Default)]
pub struct ReadyStatus {
    pub headers_modifications: Option<HeaderMutation>,
    pub clear_route_cache: bool,
}
