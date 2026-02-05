use http::Response;
use orion_error::Error;
use orion_format::types::ResponseFlags as FmtResponseFlags;
use orion_interner::StringInterner;
use smol_str::SmolStr;
use std::error::Error as ErrorTrait;
use std::io;
use tokio::time::error::Elapsed;

use crate::Error as BoxError;
use crate::{body::response_flags::ResponseFlags, clusters::retry_policy::RetryCondition};

#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("I/O Error: {0:?}")]
    IoError(#[from] io::Error),
    #[error("ConnectTimeout")]
    ConnectTimeout(#[from] Elapsed),
    #[error("PerTryTimeout)")]
    PerTryTimeout,
    #[error("RouteTimeout")]
    RouteTimeout,
    #[error("Reset")]
    Reset,
    #[error("RefusedStream")]
    RefusedStream,
    #[allow(unused)]
    #[error("Http3PostConnectFailure")]
    Http3PostConnectFailure,
    #[error("Error: {0}")]
    Error(#[from] Error),
}

#[derive(Debug, Clone)]
pub enum EventFailure {
    AdminFilterResponse,
    ClusterNotFound,
    DirectResponse,
    FilterChainNotFound,
    InternalRedirect,
    NoHealthyUpstream,
    RouteNotFound,
    UpgradeFailed,
    RbacAccessDenied(SmolStr),
    RateLimited,
    ExtProcError,
    ViaUpstream,
}

#[derive(Debug, Clone)]
pub enum EventKind {
    Error(EventError),
    Failure(EventFailure),
}

// Implement From<EventError> for EventKind
impl From<EventError> for EventKind {
    fn from(error: EventError) -> Self {
        EventKind::Error(error)
    }
}

// Implement From<EventFailure> for EventKind
impl From<EventFailure> for EventKind {
    fn from(failure: EventFailure) -> Self {
        EventKind::Failure(failure)
    }
}

impl EventKind {
    pub fn code_details(&self) -> Option<ResponseCodeDetails> {
        match self {
            EventKind::Error(err) => match err {
                EventError::IoError(err) => Some(ResponseCodeDetails::from(err)),
                EventError::ConnectTimeout(_) => Some(ResponseCodeDetails("connect_timeout")),
                EventError::PerTryTimeout => Some(ResponseCodeDetails("upstream_per_try_timeout")),
                EventError::RouteTimeout => Some(ResponseCodeDetails("upstream_response_timeout")),
                EventError::Reset => Some(ResponseCodeDetails("upstream_reset_after_response_started{TCP_RESET}")),
                EventError::RefusedStream => Some(ResponseCodeDetails("http2.remote_refuse")),
                EventError::Http3PostConnectFailure => Some(ResponseCodeDetails("http3.remote_reset")),
                EventError::Error(_) => Some(ResponseCodeDetails("internal_error")),
            },
            EventKind::Failure(fail) => match fail {
                EventFailure::AdminFilterResponse => Some(ResponseCodeDetails("admin_filter_response")),
                EventFailure::ClusterNotFound => Some(ResponseCodeDetails("cluster_not_found")),
                EventFailure::DirectResponse => Some(ResponseCodeDetails("direct_response")),
                EventFailure::FilterChainNotFound => Some(ResponseCodeDetails("filter_chain_not_found")),
                EventFailure::InternalRedirect => Some(ResponseCodeDetails("internal_redirect")),
                EventFailure::NoHealthyUpstream => Some(ResponseCodeDetails("no_healthy_upstream")),
                EventFailure::RouteNotFound => Some(ResponseCodeDetails("route_not_found")),
                EventFailure::UpgradeFailed => Some(ResponseCodeDetails("upgrade_failed")),
                EventFailure::RbacAccessDenied(id) => {
                    Some(ResponseCodeDetails(format!("rbac_access_denied[{id}]").to_static_str()))
                },
                EventFailure::RateLimited => Some(ResponseCodeDetails("rate_limited")),
                EventFailure::ExtProcError => Some(ResponseCodeDetails("ext_proc_error")),
                EventFailure::ViaUpstream => Some(ResponseCodeDetails("via_upstream")),
            },
        }
    }

    pub fn termination_details(&self) -> Option<ConnectionTerminationDetails> {
        match self {
            #[allow(clippy::collapsible_match)]
            EventKind::Error(err) => match err {
                EventError::IoError(err) => Some(ConnectionTerminationDetails::from(err)),
                EventError::RouteTimeout => Some(ConnectionTerminationDetails("route timeout was reached")),
                _ => None,
            },
            EventKind::Failure(fail) => match fail {
                EventFailure::RbacAccessDenied(id) => Some(ConnectionTerminationDetails(
                    format!("rbac_access_denied_matched_policy[{id}]").to_static_str(),
                )),
                _ => None,
            },
        }
    }
}

pub struct UpstreamTransportEventError(pub &'static str);
pub struct ResponseCodeDetails(pub &'static str);
pub struct ConnectionTerminationDetails(pub &'static str);

pub fn find_error_in_chain<'a, E: ErrorTrait + 'static>(mut err: &'a (dyn ErrorTrait + 'static)) -> Option<&'a E> {
    loop {
        if let Some(found) = err.downcast_ref::<E>() {
            return Some(found);
        }
        match err.source() {
            Some(next) => err = next,
            None => return None,
        }
    }
}

impl From<&io::Error> for UpstreamTransportEventError {
    fn from(err: &io::Error) -> Self {
        UpstreamTransportEventError(match err.kind() {
            io::ErrorKind::ConnectionRefused => "connection_refused",
            io::ErrorKind::NotConnected => "not_connected",
            io::ErrorKind::AddrInUse => "addr_in_use",
            io::ErrorKind::AddrNotAvailable => "addr_not_available",
            io::ErrorKind::NetworkUnreachable => "network_unreachable",
            io::ErrorKind::PermissionDenied => "permission_denied",
            io::ErrorKind::ConnectionAborted => "connection_aborted",
            io::ErrorKind::ConnectionReset => "connection_reset",
            io::ErrorKind::TimedOut => "connection_timed_out",
            _ => "connect_failure",
        })
    }
}

impl From<&io::Error> for ResponseCodeDetails {
    fn from(err: &io::Error) -> Self {
        ResponseCodeDetails(match err.kind() {
            io::ErrorKind::ConnectionRefused => "upstream_reset_before_response_started{CONNECTION_REFUSED}",
            io::ErrorKind::NotConnected => "upstream_reset_after_response_started{NOT_CONNECTED}",
            io::ErrorKind::AddrInUse => "upstream_reset_before_response_started{ADDR_IN_USE}",
            io::ErrorKind::AddrNotAvailable => "upstream_reset_before_response_started{ADDR_NOT_AVAILABLE}",
            io::ErrorKind::NetworkUnreachable => "upstream_reset_before_response_started{NETWORK_UNREACHABLE}",
            io::ErrorKind::PermissionDenied => "upstream_reset_before_response_started{PERMISSION_DENIED}",
            io::ErrorKind::ConnectionAborted => "upstream_reset_after_response_started{CONNECTION_ABORTED}",
            io::ErrorKind::ConnectionReset => "upstream_reset_after_response_started{TCP_RESET}",
            io::ErrorKind::TimedOut => "streaming_timeout",
            _ => "connection_reset",
        })
    }
}

impl From<&io::Error> for ConnectionTerminationDetails {
    fn from(err: &io::Error) -> Self {
        ConnectionTerminationDetails(match err.kind() {
            io::ErrorKind::TimedOut => "transport socket timeout was reached",
            _ => "I/O error",
        })
    }
}

impl TryFrom<&EventError> for UpstreamTransportEventError {
    type Error = ();
    fn try_from(value: &EventError) -> Result<Self, Self::Error> {
        match value {
            EventError::IoError(io_err) => Ok(UpstreamTransportEventError::from(io_err)),
            EventError::ConnectTimeout(_) => Ok(UpstreamTransportEventError("connect_timeout")),
            EventError::Reset => Ok(UpstreamTransportEventError("upstream_reset")),
            _ => Err(()), // other errors are not transport errors
        }
    }
}

// DISCLAIMER: This is a workaround for the fact that `EventError` cannot implement `Clone`.
// Cloning is not possible because `Elapsed` and `io::Error` do not implement `Clone`.
// Their presence in `EventError` is required by the `hyper_util` crate, which needs to
// traverse `EventError` to extract the underlying `io::Error` or `Elapsed` in order to
// produce a more specific error message.
// In this case, we create a new `EventError` by reconstructing the `io::Error` with the
// same kind and message as the original. This effectively acts as a "shallow clone" of
// the error: not perfect, but sufficient for our use case.

impl Clone for EventError {
    fn clone(&self) -> Self {
        match self {
            EventError::IoError(io_err) => {
                let new_io_err = io::Error::new(io_err.kind(), io_err.to_string());
                EventError::IoError(new_io_err)
            },
            EventError::ConnectTimeout(_) => EventError::ConnectTimeout(elapsed()),
            EventError::PerTryTimeout => EventError::PerTryTimeout,
            EventError::RouteTimeout => EventError::RouteTimeout,
            EventError::Reset => EventError::Reset,
            EventError::RefusedStream => EventError::RefusedStream,
            EventError::Http3PostConnectFailure => EventError::Http3PostConnectFailure,
            EventError::Error(err) => EventError::Error(Error::new(err.to_string())),
        }
    }
}

impl From<EventError> for ResponseFlags {
    fn from(err: EventError) -> Self {
        match err {
            EventError::IoError(_) | EventError::ConnectTimeout(_) => {
                ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE)
            },
            EventError::PerTryTimeout => ResponseFlags(FmtResponseFlags::UPSTREAM_REQUEST_TIMEOUT),
            EventError::RouteTimeout => ResponseFlags(FmtResponseFlags::empty()),
            EventError::Reset | EventError::RefusedStream | EventError::Http3PostConnectFailure => {
                ResponseFlags(FmtResponseFlags::UPSTREAM_REMOTE_RESET)
            },
            EventError::Error(_) => ResponseFlags(FmtResponseFlags::LOCAL_RESET),
        }
    }
}

pub fn elapsed() -> Elapsed {
    unsafe { std::mem::transmute(()) }
}

pub trait TryInferFrom<F>: Sized {
    fn try_infer_from(source: F) -> Option<Self>;
}

impl<'a, B> TryInferFrom<&'a Result<Response<B>, BoxError>> for RetryCondition<'a, B> {
    fn try_infer_from(source: &'a Result<Response<B>, BoxError>) -> Option<Self> {
        match source {
            Ok(ref resp) => {
                // NOTE: exclude a priory the evaluation of the retry policy for 1xx, and 2xx.
                if resp.status().is_informational() || resp.status().is_success() {
                    return None;
                }
                Some(RetryCondition::Response(resp))
            },
            Err(err) => {
                let ev = EventError::try_infer_from(err.as_ref())?;
                Some(RetryCondition::Error(ev))
            },
        }
    }
}

impl<'a> TryInferFrom<&'a (dyn std::error::Error + 'static)> for EventError {
    fn try_infer_from(err: &'a (dyn std::error::Error + 'static)) -> Option<Self> {
        if err.downcast_ref::<Elapsed>().is_some() {
            // Note: This should never happen, as the user should remap the Tokio timeout
            // to a suitable EventError (e.g., timeout(dur, fut).await.map_err(|_| EventError::ConnectTimeout)).
            // Just in case, the PerTryTimeout error is the closest one we can choose.
            return Some(EventError::PerTryTimeout);
        }

        if let Some(failure) = err.downcast_ref::<EventError>() {
            return Some(failure.clone());
        }

        if let Some(h2_reason) = err.downcast_ref::<h2::Error>().and_then(h2::Error::reason) {
            match h2_reason {
                h2::Reason::REFUSED_STREAM => return Some(EventError::RefusedStream),
                h2::Reason::CONNECT_ERROR => {
                    return Some(EventError::IoError(io::Error::new(
                        io::ErrorKind::ConnectionRefused,
                        "H2 connection refused",
                    )));
                },
                _ => return Some(EventError::Reset),
            }
        }

        if let Some(io_err) = err.downcast_ref::<io::Error>() {
            return Some(EventError::IoError(io::Error::new(io_err.kind(), io_err.to_string())));
        }

        if let Some(source_err) = err.source() {
            return Self::try_infer_from(source_err);
        }

        // the rest of the errors are remapped to Reset
        Some(EventError::Reset)
    }
}
