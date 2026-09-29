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

use arion_format::types::ResponseFlags as FmtResponseFlags;
use arion_interner::StringInterner;
use smol_str::SmolStr;
use std::error::Error as ErrorTrait;
use std::io;
use tokio::time::error::Elapsed;

use crate::body::response_flags::ResponseFlags;

#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("I/O Error: {0:?}")]
    Io(
        #[source]
        #[from]
        io::Error,
    ),
    #[error("ConnectTimeout")]
    ConnectTimeout(#[from] Elapsed),
    #[error("PerTryTimeout")]
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
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Other upstream error: {0}")]
    Other(String),
}

#[derive(Debug, thiserror::Error)]
pub enum DownstreamError {
    #[error("I/O Error: {0:?}")]
    Io(
        #[source]
        #[from]
        io::Error,
    ),
    #[error("Reset")]
    Reset,
    #[error("Protocol error: {0}")]
    Protocol(String),
    #[error("Timeout")]
    Timeout,
    #[error("Other downstream error: {0}")]
    Other(String),
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
    CedarAccessDenied(SmolStr),
    RateLimited,
    ExtProcError,
    ViaUpstream,
    UpstreamOverflow,
}

#[derive(Debug, Clone)]
pub enum EventKind {
    Upstream(UpstreamError),
    Downstream(DownstreamError),
    Failure(EventFailure),
}

/// Per-response extension carrying access-log / metrics event info.
#[derive(Debug, Clone)]
pub struct EventErrorContext {
    pub response_flags: ResponseFlags,
    pub event_kind: Option<EventKind>,
}

// Implement From<EventError> for EventKind
impl From<UpstreamError> for EventKind {
    fn from(error: UpstreamError) -> Self {
        EventKind::Upstream(error)
    }
}

// Implement From<EventError> for EventKind
impl From<DownstreamError> for EventKind {
    fn from(error: DownstreamError) -> Self {
        EventKind::Downstream(error)
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
            EventKind::Upstream(err) => match err {
                UpstreamError::Io(err) => Some(ResponseCodeDetails::from(err)),
                UpstreamError::ConnectTimeout(_) => Some(ResponseCodeDetails("connect_timeout")),
                UpstreamError::PerTryTimeout => Some(ResponseCodeDetails("upstream_per_try_timeout")),
                UpstreamError::RouteTimeout => Some(ResponseCodeDetails("upstream_response_timeout")),
                UpstreamError::Reset => Some(ResponseCodeDetails("upstream_reset_after_response_started{TCP_RESET}")),
                UpstreamError::RefusedStream => Some(ResponseCodeDetails("http2.remote_refuse")),
                UpstreamError::Http3PostConnectFailure => Some(ResponseCodeDetails("http3.remote_reset")),
                UpstreamError::Protocol(_) => Some(ResponseCodeDetails("upstream_protocol_error")),
                UpstreamError::Other(_) => Some(ResponseCodeDetails("internal_error")),
            },
            EventKind::Downstream(err) => match err {
                DownstreamError::Io(err) => Some(ResponseCodeDetails::from(err)),
                DownstreamError::Reset => Some(ResponseCodeDetails("downstream_connection_reset")),
                DownstreamError::Protocol(_) => Some(ResponseCodeDetails("downstream_protocol_error")),
                DownstreamError::Timeout => Some(ResponseCodeDetails("downstream_request_timeout")),
                DownstreamError::Other(_) => Some(ResponseCodeDetails("downstream_internal_error")),
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
                EventFailure::CedarAccessDenied(id) => {
                    Some(ResponseCodeDetails(format!("cedar_access_denied[{id}]").to_static_str()))
                },
                EventFailure::RateLimited => Some(ResponseCodeDetails("rate_limited")),
                EventFailure::ExtProcError => Some(ResponseCodeDetails("ext_proc_error")),
                EventFailure::ViaUpstream => Some(ResponseCodeDetails("via_upstream")),
                EventFailure::UpstreamOverflow => Some(ResponseCodeDetails("upstream_overflow")),
            },
        }
    }

    pub fn termination_details(&self) -> Option<ConnectionTerminationDetails> {
        match self {
            EventKind::Downstream(err) => match err {
                DownstreamError::Io(err) => Some(ConnectionTerminationDetails::from(err)),
                DownstreamError::Reset => Some(ConnectionTerminationDetails("downstream_connection_reset")),
                DownstreamError::Protocol(_) => Some(ConnectionTerminationDetails("downstream_protocol_error")),
                DownstreamError::Timeout => Some(ConnectionTerminationDetails("downstream_timeout")),
                DownstreamError::Other(_) => Some(ConnectionTerminationDetails("downstream_error")),
            },
            EventKind::Upstream(_) | EventKind::Failure(_) => None,
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

// valid for both l4 and l7..
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
            io::ErrorKind::TimedOut => "upstream_streaming_timeout{TIMEOUT}",
            io::ErrorKind::BrokenPipe => "upstream_reset_after_response_started{BROKEN_PIPE}",
            io::ErrorKind::UnexpectedEof => "upstream_reset_after_response_started{UNEXPECTED_EOF}",
            _ => "connection_reset",
        })
    }
}

impl From<&io::Error> for ConnectionTerminationDetails {
    fn from(err: &io::Error) -> Self {
        ConnectionTerminationDetails(match err.kind() {
            io::ErrorKind::TimedOut => "transport_socket_timeout_was_reached{TIMEOUT}",
            io::ErrorKind::ConnectionReset => "connection_reset_by_peer{TCP_RESET}", // TCP RST received, common when the client forcefully closes the connection
            io::ErrorKind::ConnectionAborted => "connection_aborted{CONNECTION_ABORTED}", // Software routing issue or network drop
            io::ErrorKind::BrokenPipe => "remote_close{BROKEN_PIPE}", // Attempted to write to a socket that was already closed by the downstream
            io::ErrorKind::UnexpectedEof => "remote_close{UNEXPECTED_EOF}", // Downstream closed the connection cleanly but prematurely
            io::ErrorKind::NotConnected => "local_close{NOT_CONNECTED}", // Tried to read/write on a disconnected socket
            _ => "Generic I/O error",
        })
    }
}

impl TryFrom<&UpstreamError> for UpstreamTransportEventError {
    type Error = ();

    fn try_from(value: &UpstreamError) -> Result<Self, Self::Error> {
        match value {
            // Map standard I/O errors using the previously defined From trait
            UpstreamError::Io(io_err) => Ok(UpstreamTransportEventError::from(io_err)),

            // Connection phase timeout
            UpstreamError::ConnectTimeout(_) => Ok(UpstreamTransportEventError("upstream_connect_timeout")),

            // Timeout for a single retry attempt
            UpstreamError::PerTryTimeout => Ok(UpstreamTransportEventError("upstream_per_try_timeout")),

            // Overall route/request timeout
            UpstreamError::RouteTimeout => Ok(UpstreamTransportEventError("upstream_response_timeout")),

            // Generic connection reset
            UpstreamError::Reset => Ok(UpstreamTransportEventError("upstream_reset")),

            // HTTP/2 or HTTP/3 refused stream
            UpstreamError::RefusedStream => Ok(UpstreamTransportEventError("upstream_refused_stream")),

            // HTTP/3 specific post-connect failure
            UpstreamError::Http3PostConnectFailure => Ok(UpstreamTransportEventError("http3_post_connect_failure")),

            UpstreamError::Protocol(_) => Ok(UpstreamTransportEventError("upstream_protocol_error")),

            UpstreamError::Other(_) => Err(()),
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

impl Clone for UpstreamError {
    fn clone(&self) -> Self {
        match self {
            UpstreamError::Io(io_err) => {
                let new_io_err = io::Error::new(io_err.kind(), io_err.to_string());
                UpstreamError::Io(new_io_err)
            },
            UpstreamError::ConnectTimeout(_) => UpstreamError::ConnectTimeout(elapsed()),
            UpstreamError::PerTryTimeout => UpstreamError::PerTryTimeout,
            UpstreamError::RouteTimeout => UpstreamError::RouteTimeout,
            UpstreamError::Reset => UpstreamError::Reset,
            UpstreamError::RefusedStream => UpstreamError::RefusedStream,
            UpstreamError::Http3PostConnectFailure => UpstreamError::Http3PostConnectFailure,
            UpstreamError::Protocol(msg) => UpstreamError::Protocol(msg.clone()),
            UpstreamError::Other(msg) => UpstreamError::Other(msg.clone()),
        }
    }
}

impl Clone for DownstreamError {
    fn clone(&self) -> Self {
        match self {
            DownstreamError::Io(io_err) => {
                let new_io_err = io::Error::new(io_err.kind(), io_err.to_string());
                DownstreamError::Io(new_io_err)
            },
            DownstreamError::Reset => DownstreamError::Reset,
            DownstreamError::Protocol(msg) => DownstreamError::Protocol(msg.clone()),
            DownstreamError::Timeout => DownstreamError::Timeout,
            DownstreamError::Other(msg) => DownstreamError::Other(msg.clone()),
        }
    }
}

impl From<UpstreamError> for ResponseFlags {
    fn from(err: UpstreamError) -> Self {
        match err {
            UpstreamError::Io(_) | UpstreamError::ConnectTimeout(_) => {
                ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE)
            },
            UpstreamError::PerTryTimeout => ResponseFlags(FmtResponseFlags::UPSTREAM_REQUEST_TIMEOUT),
            UpstreamError::RouteTimeout => ResponseFlags(FmtResponseFlags::empty()),
            UpstreamError::Reset | UpstreamError::RefusedStream | UpstreamError::Http3PostConnectFailure => {
                ResponseFlags(FmtResponseFlags::UPSTREAM_REMOTE_RESET)
            },
            UpstreamError::Protocol(_) => ResponseFlags(FmtResponseFlags::UPSTREAM_PROTOCOL_ERROR),
            UpstreamError::Other(_) => ResponseFlags(FmtResponseFlags::LOCAL_RESET),
        }
    }
}

impl From<DownstreamError> for ResponseFlags {
    fn from(err: DownstreamError) -> Self {
        match err {
            DownstreamError::Io(_) => ResponseFlags(FmtResponseFlags::DOWNSTREAM_CONNECTION_TERMINATION),
            DownstreamError::Reset => ResponseFlags(FmtResponseFlags::DOWNSTREAM_REMOTE_RESET),
            DownstreamError::Protocol(_) => ResponseFlags(FmtResponseFlags::DOWNSTREAM_PROTOCOL_ERROR),
            DownstreamError::Timeout => ResponseFlags(FmtResponseFlags::STREAM_IDLE_TIMEOUT),
            DownstreamError::Other(_) => ResponseFlags(FmtResponseFlags::LOCAL_RESET),
        }
    }
}

pub fn elapsed() -> Elapsed {
    // SAFETY: a way to construct the Elapsed tokio time error
    unsafe { std::mem::transmute(()) }
}

impl From<&h2::Error> for DownstreamError {
    fn from(err: &h2::Error) -> Self {
        if let Some(reason) = err.reason() {
            match reason {
                h2::Reason::NO_ERROR | h2::Reason::CANCEL | h2::Reason::REFUSED_STREAM => DownstreamError::Reset,
                h2::Reason::PROTOCOL_ERROR
                | h2::Reason::FRAME_SIZE_ERROR
                | h2::Reason::FLOW_CONTROL_ERROR
                | h2::Reason::SETTINGS_TIMEOUT
                | h2::Reason::COMPRESSION_ERROR => DownstreamError::Protocol(format!("h2 protocol error: {reason:?}")),
                _ => DownstreamError::Reset,
            }
        } else {
            DownstreamError::Reset
        }
    }
}

impl From<&hyper::Error> for DownstreamError {
    fn from(err: &hyper::Error) -> Self {
        if let Some(h2_err) = find_error_in_chain::<h2::Error>(err) {
            return DownstreamError::from(h2_err);
        }
        if let Some(io_err) = find_error_in_chain::<io::Error>(err) {
            return DownstreamError::Io(io::Error::new(io_err.kind(), io_err.to_string()));
        }
        if err.is_canceled() || err.is_closed() {
            DownstreamError::Reset
        } else if err.is_timeout() {
            DownstreamError::Timeout
        } else if err.is_parse() || err.is_parse_status() || err.is_parse_too_large() {
            DownstreamError::Protocol(err.to_string())
        } else {
            DownstreamError::Other(err.to_string())
        }
    }
}

impl From<hyper::Error> for DownstreamError {
    #[inline]
    fn from(err: hyper::Error) -> Self {
        DownstreamError::from(&err)
    }
}

impl DownstreamError {
    pub fn from_dyn_error(err: &(dyn ErrorTrait + 'static)) -> Self {
        if let Some(downstream) = find_error_in_chain::<DownstreamError>(err) {
            return downstream.clone();
        }
        if let Some(crate_err) = find_error_in_chain::<crate::Error>(err) {
            if let Some(downstream) = crate_err.as_downstream_error() {
                return downstream.clone();
            }
        }
        if let Some(h2_err) = find_error_in_chain::<h2::Error>(err) {
            return DownstreamError::from(h2_err);
        }
        if let Some(hyper_err) = find_error_in_chain::<hyper::Error>(err) {
            return DownstreamError::from(hyper_err);
        }
        if let Some(io_err) = find_error_in_chain::<io::Error>(err) {
            return DownstreamError::Io(io::Error::new(io_err.kind(), io_err.to_string()));
        }
        if find_error_in_chain::<Elapsed>(err).is_some() {
            return DownstreamError::Timeout;
        }
        DownstreamError::Other(err.to_string())
    }
}

impl From<&h2::Error> for UpstreamError {
    fn from(err: &h2::Error) -> Self {
        if let Some(reason) = err.reason() {
            match reason {
                h2::Reason::REFUSED_STREAM => UpstreamError::RefusedStream,
                h2::Reason::CONNECT_ERROR => {
                    UpstreamError::Io(io::Error::new(io::ErrorKind::ConnectionRefused, "H2 connection refused"))
                },
                h2::Reason::PROTOCOL_ERROR
                | h2::Reason::FRAME_SIZE_ERROR
                | h2::Reason::FLOW_CONTROL_ERROR
                | h2::Reason::SETTINGS_TIMEOUT
                | h2::Reason::COMPRESSION_ERROR => UpstreamError::Protocol(format!("h2 protocol error: {reason:?}")),
                _ => UpstreamError::Reset,
            }
        } else {
            UpstreamError::Reset
        }
    }
}

impl From<&hyper::Error> for UpstreamError {
    fn from(err: &hyper::Error) -> Self {
        if let Some(h2_err) = find_error_in_chain::<h2::Error>(err) {
            return UpstreamError::from(h2_err);
        }
        if let Some(io_err) = find_error_in_chain::<io::Error>(err) {
            return UpstreamError::Io(io::Error::new(io_err.kind(), io_err.to_string()));
        }
        if err.is_canceled() || err.is_closed() {
            UpstreamError::Reset
        } else if err.is_timeout() {
            UpstreamError::PerTryTimeout
        } else if err.is_parse() || err.is_parse_status() || err.is_parse_too_large() {
            UpstreamError::Protocol(err.to_string())
        } else {
            UpstreamError::Other(err.to_string())
        }
    }
}

impl From<hyper::Error> for UpstreamError {
    #[inline]
    fn from(err: hyper::Error) -> Self {
        UpstreamError::from(&err)
    }
}

impl UpstreamError {
    pub fn from_dyn_error(err: &(dyn ErrorTrait + 'static)) -> Self {
        if let Some(upstream) = find_error_in_chain::<UpstreamError>(err) {
            return upstream.clone();
        }
        if let Some(crate_err) = find_error_in_chain::<crate::Error>(err) {
            if let Some(upstream) = crate_err.as_upstream_error() {
                return upstream.clone();
            }
        }
        if find_error_in_chain::<Elapsed>(err).is_some() {
            return UpstreamError::PerTryTimeout;
        }
        if let Some(h2_err) = find_error_in_chain::<h2::Error>(err) {
            return UpstreamError::from(h2_err);
        }
        if let Some(hyper_err) = find_error_in_chain::<hyper::Error>(err) {
            return UpstreamError::from(hyper_err);
        }
        if let Some(io_err) = find_error_in_chain::<io::Error>(err) {
            return UpstreamError::Io(io::Error::new(io_err.kind(), io_err.to_string()));
        }
        UpstreamError::Other(err.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::connector::TcpErrorContext;
    use crate::Error;

    #[test]
    fn test_error_chain_debug() {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "Connection refused");
        let upstream_err = UpstreamError::Io(io_err);
        let err = Error::upstream_with_context(
            TcpErrorContext {
                upstream_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 8080)),
                response_flags: arion_format::types::ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
                cluster_name: "test_cluster",
            },
            upstream_err,
        );

        println!("DEBUG TEST: err = {err:?}");
        println!("DEBUG TEST: err display = {err}");

        let mut temp_err: Option<&(dyn std::error::Error + 'static)> = Some(&err);
        while let Some(e) = temp_err {
            println!("DEBUG TEST: cause = {}, type = {:?}", e, e.source().map(|_| "has_source"));
            temp_err = e.source();
        }

        let found = err.find_source::<io::Error>();
        assert!(found.is_some(), "Should find std::io::Error in chain!");

        let ctx = err.upstream_context().expect("Should find TcpErrorContext in chain!");
        assert_eq!(ctx.cluster_name, "test_cluster");
    }

    #[test]
    fn test_error_classification_through_wrappers() {
        let err = Error::upstream(UpstreamError::RouteTimeout);
        let upstream = err.as_upstream_error();
        assert!(matches!(upstream, Some(UpstreamError::RouteTimeout)), "boxed: got {upstream:?}");

        let err = Error::upstream_with_context(
            TcpErrorContext {
                cluster_name: "test_cluster",
                upstream_addr: std::net::SocketAddr::from(([127, 0, 0, 1], 8080)),
                response_flags: arion_format::types::ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
            },
            UpstreamError::ConnectTimeout(elapsed()),
        );
        let upstream = err.as_upstream_error();
        assert!(matches!(upstream, Some(UpstreamError::ConnectTimeout(_))), "wrapped: got {upstream:?}");

        let err = Error::from(io::Error::new(io::ErrorKind::ConnectionRefused, "nope"));
        assert!(err.find_source::<io::Error>().is_some(), "io::Error not found through Io variant");
    }

    #[test]
    fn test_downstream_h2_error_mapping() {
        // Test that h2 error reason CANCEL maps to Reset
        let h2_err = h2::Error::from(h2::Reason::CANCEL);
        let downstream = DownstreamError::from(&h2_err);
        assert!(matches!(downstream, DownstreamError::Reset));

        // Test that h2 error reason PROTOCOL_ERROR maps to Protocol
        let h2_err = h2::Error::from(h2::Reason::PROTOCOL_ERROR);
        let downstream = DownstreamError::from(&h2_err);
        assert!(matches!(downstream, DownstreamError::Protocol(_)));
    }
}
