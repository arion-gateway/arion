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
use smol_str::{SmolStr, format_smolstr};
use std::io;
use tokio::time::error::Elapsed;

use crate::body::poly_body::PolyBodyError;
use crate::body::response_flags::{BodyKind, ResponseFlags};
use crate::body::timeout_body::TimeoutBodyError;
use crate::transport::connector::{ConnectError, ConnectErrorKind};

#[derive(Debug, thiserror::Error)]
pub enum UpstreamError {
    #[error("{0}")]
    Connect(#[from] Box<ConnectError>),
    #[error("I/O Error: {0:?}")]
    Io(
        #[source]
        #[from]
        io::Error,
    ),
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
                UpstreamError::Connect(conn_err) => match &conn_err.kind {
                    ConnectErrorKind::Timeout(_) => Some(ResponseCodeDetails(SmolStr::new_static("connect_timeout"))),
                    ConnectErrorKind::Dns(_) => Some(ResponseCodeDetails(SmolStr::new_static("dns_resolution_failed"))),
                    ConnectErrorKind::CircuitBreaker => {
                        Some(ResponseCodeDetails(SmolStr::new_static("circuit_breaker_overflow")))
                    },
                    ConnectErrorKind::Io(err) => Some(ResponseCodeDetails::from(err)),
                    _ => Some(ResponseCodeDetails(SmolStr::new_static("upstream_connect_failure"))),
                },
                UpstreamError::Io(err) => Some(ResponseCodeDetails::from(err)),
                UpstreamError::PerTryTimeout => {
                    Some(ResponseCodeDetails(SmolStr::new_static("upstream_per_try_timeout")))
                },
                UpstreamError::RouteTimeout => {
                    Some(ResponseCodeDetails(SmolStr::new_static("upstream_response_timeout")))
                },
                UpstreamError::Reset => {
                    Some(ResponseCodeDetails(SmolStr::new_static("upstream_reset_after_response_started{TCP_RESET}")))
                },
                UpstreamError::RefusedStream => Some(ResponseCodeDetails(SmolStr::new_static("http2.remote_refuse"))),
                UpstreamError::Http3PostConnectFailure => {
                    Some(ResponseCodeDetails(SmolStr::new_static("http3.remote_reset")))
                },
                UpstreamError::Protocol(_) => Some(ResponseCodeDetails(SmolStr::new_static("upstream_protocol_error"))),
                UpstreamError::Other(_) => Some(ResponseCodeDetails(SmolStr::new_static("internal_error"))),
            },
            EventKind::Downstream(err) => match err {
                DownstreamError::Io(err) => Some(ResponseCodeDetails::from(err)),
                DownstreamError::Reset => Some(ResponseCodeDetails(SmolStr::new_static("downstream_connection_reset"))),
                DownstreamError::Protocol(_) => {
                    Some(ResponseCodeDetails(SmolStr::new_static("downstream_protocol_error")))
                },
                DownstreamError::Timeout => {
                    Some(ResponseCodeDetails(SmolStr::new_static("downstream_request_timeout")))
                },
                DownstreamError::Other(_) => {
                    Some(ResponseCodeDetails(SmolStr::new_static("downstream_internal_error")))
                },
            },
            EventKind::Failure(fail) => match fail {
                EventFailure::AdminFilterResponse => {
                    Some(ResponseCodeDetails(SmolStr::new_static("admin_filter_response")))
                },
                EventFailure::ClusterNotFound => Some(ResponseCodeDetails(SmolStr::new_static("cluster_not_found"))),
                EventFailure::DirectResponse => Some(ResponseCodeDetails(SmolStr::new_static("direct_response"))),
                EventFailure::FilterChainNotFound => {
                    Some(ResponseCodeDetails(SmolStr::new_static("filter_chain_not_found")))
                },
                EventFailure::InternalRedirect => Some(ResponseCodeDetails(SmolStr::new_static("internal_redirect"))),
                EventFailure::NoHealthyUpstream => {
                    Some(ResponseCodeDetails(SmolStr::new_static("no_healthy_upstream")))
                },
                EventFailure::RouteNotFound => Some(ResponseCodeDetails(SmolStr::new_static("route_not_found"))),
                EventFailure::UpgradeFailed => Some(ResponseCodeDetails(SmolStr::new_static("upgrade_failed"))),
                EventFailure::RbacAccessDenied(id) => {
                    Some(ResponseCodeDetails(format_smolstr!("rbac_access_denied[{id}]")))
                },
                EventFailure::CedarAccessDenied(id) => {
                    Some(ResponseCodeDetails(format_smolstr!("cedar_access_denied[{id}]")))
                },
                EventFailure::RateLimited => Some(ResponseCodeDetails(SmolStr::new_static("rate_limited"))),
                EventFailure::ExtProcError => Some(ResponseCodeDetails(SmolStr::new_static("ext_proc_error"))),
                EventFailure::ViaUpstream => Some(ResponseCodeDetails(SmolStr::new_static("via_upstream"))),
                EventFailure::UpstreamOverflow => Some(ResponseCodeDetails(SmolStr::new_static("upstream_overflow"))),
            },
        }
    }

    pub fn termination_details(&self) -> Option<ConnectionTerminationDetails> {
        match self {
            EventKind::Downstream(err) => match err {
                DownstreamError::Io(err) => Some(ConnectionTerminationDetails::from(err)),
                DownstreamError::Reset => {
                    Some(ConnectionTerminationDetails(SmolStr::new_static("downstream_connection_reset")))
                },
                DownstreamError::Protocol(_) => {
                    Some(ConnectionTerminationDetails(SmolStr::new_static("downstream_protocol_error")))
                },
                DownstreamError::Timeout => {
                    Some(ConnectionTerminationDetails(SmolStr::new_static("downstream_timeout")))
                },
                DownstreamError::Other(_) => {
                    Some(ConnectionTerminationDetails(SmolStr::new_static("downstream_error")))
                },
            },
            EventKind::Upstream(_) | EventKind::Failure(_) => None,
        }
    }
}

pub struct UpstreamTransportEventError(pub SmolStr);
pub struct ResponseCodeDetails(pub SmolStr);
pub struct ConnectionTerminationDetails(pub SmolStr);

impl From<&io::Error> for UpstreamTransportEventError {
    fn from(err: &io::Error) -> Self {
        UpstreamTransportEventError(match err.kind() {
            io::ErrorKind::ConnectionRefused => SmolStr::new_static("connection_refused"),
            io::ErrorKind::NotConnected => SmolStr::new_static("not_connected"),
            io::ErrorKind::AddrInUse => SmolStr::new_static("addr_in_use"),
            io::ErrorKind::AddrNotAvailable => SmolStr::new_static("addr_not_available"),
            io::ErrorKind::NetworkUnreachable => SmolStr::new_static("network_unreachable"),
            io::ErrorKind::PermissionDenied => SmolStr::new_static("permission_denied"),
            io::ErrorKind::ConnectionAborted => SmolStr::new_static("connection_aborted"),
            io::ErrorKind::ConnectionReset => SmolStr::new_static("connection_reset"),
            io::ErrorKind::TimedOut => SmolStr::new_static("connection_timed_out"),
            _ => SmolStr::new_static("connect_failure"),
        })
    }
}

// valid for both l4 and l7..
impl From<&io::Error> for ResponseCodeDetails {
    fn from(err: &io::Error) -> Self {
        ResponseCodeDetails(match err.kind() {
            io::ErrorKind::ConnectionRefused => {
                SmolStr::new_static("upstream_reset_before_response_started{CONNECTION_REFUSED}")
            },
            io::ErrorKind::NotConnected => SmolStr::new_static("upstream_reset_after_response_started{NOT_CONNECTED}"),
            io::ErrorKind::AddrInUse => SmolStr::new_static("upstream_reset_before_response_started{ADDR_IN_USE}"),
            io::ErrorKind::AddrNotAvailable => {
                SmolStr::new_static("upstream_reset_before_response_started{ADDR_NOT_AVAILABLE}")
            },
            io::ErrorKind::NetworkUnreachable => {
                SmolStr::new_static("upstream_reset_before_response_started{NETWORK_UNREACHABLE}")
            },
            io::ErrorKind::PermissionDenied => {
                SmolStr::new_static("upstream_reset_before_response_started{PERMISSION_DENIED}")
            },
            io::ErrorKind::ConnectionAborted => {
                SmolStr::new_static("upstream_reset_after_response_started{CONNECTION_ABORTED}")
            },
            io::ErrorKind::ConnectionReset => SmolStr::new_static("upstream_reset_after_response_started{TCP_RESET}"),
            io::ErrorKind::TimedOut => SmolStr::new_static("upstream_streaming_timeout{TIMEOUT}"),
            io::ErrorKind::BrokenPipe => SmolStr::new_static("upstream_reset_after_response_started{BROKEN_PIPE}"),
            io::ErrorKind::UnexpectedEof => {
                SmolStr::new_static("upstream_reset_after_response_started{UNEXPECTED_EOF}")
            },
            _ => SmolStr::new_static("connection_reset"),
        })
    }
}

impl From<&io::Error> for ConnectionTerminationDetails {
    fn from(err: &io::Error) -> Self {
        ConnectionTerminationDetails(match err.kind() {
            io::ErrorKind::TimedOut => SmolStr::new_static("transport_socket_timeout_was_reached{TIMEOUT}"),
            io::ErrorKind::ConnectionReset => SmolStr::new_static("connection_reset_by_peer{TCP_RESET})"), // TCP RST received, common when the client forcefully closes the connection
            io::ErrorKind::ConnectionAborted => SmolStr::new_static("connection_aborted{CONNECTION_ABORTED}"), // Software routing issue or network drop
            io::ErrorKind::BrokenPipe => SmolStr::new_static("remote_close{BROKEN_PIPE}"), // Attempted to write to a socket that was already closed by the downstream
            io::ErrorKind::UnexpectedEof => SmolStr::new_static("remote_close{UNEXPECTED_EOF}"), // Downstream closed the connection cleanly but prematurely
            io::ErrorKind::NotConnected => SmolStr::new_static("local_close{NOT_CONNECTED}"), // Tried to read/write on a disconnected socket
            _ => SmolStr::new_static("Generic I/O error"),
        })
    }
}

impl TryFrom<&UpstreamError> for UpstreamTransportEventError {
    type Error = ();

    fn try_from(value: &UpstreamError) -> Result<Self, Self::Error> {
        match value {
            UpstreamError::Connect(conn_err) => match &conn_err.kind {
                ConnectErrorKind::Timeout(_) => {
                    Ok(UpstreamTransportEventError(SmolStr::new_static("upstream_connect_timeout")))
                },
                ConnectErrorKind::Dns(_) => {
                    Ok(UpstreamTransportEventError(SmolStr::new_static("dns_resolution_failed")))
                },
                ConnectErrorKind::Io(io_err) => Ok(UpstreamTransportEventError::from(io_err)),
                _ => Ok(UpstreamTransportEventError(SmolStr::new_static("upstream_connect_failure"))),
            },
            // Map standard I/O errors using the previously defined From trait
            UpstreamError::Io(io_err) => Ok(UpstreamTransportEventError::from(io_err)),

            // Timeout for a single retry attempt
            UpstreamError::PerTryTimeout => {
                Ok(UpstreamTransportEventError(SmolStr::new_static("upstream_per_try_timeout")))
            },

            // Overall route/request timeout
            UpstreamError::RouteTimeout => {
                Ok(UpstreamTransportEventError(SmolStr::new_static("upstream_response_timeout")))
            },

            // Generic connection reset
            UpstreamError::Reset => Ok(UpstreamTransportEventError(SmolStr::new_static("upstream_reset"))),

            // HTTP/2 or HTTP/3 refused stream
            UpstreamError::RefusedStream => {
                Ok(UpstreamTransportEventError(SmolStr::new_static("upstream_refused_stream")))
            },

            // HTTP/3 specific post-connect failure
            UpstreamError::Http3PostConnectFailure => {
                Ok(UpstreamTransportEventError(SmolStr::new_static("http3_post_connect_failure")))
            },

            UpstreamError::Protocol(_) => {
                Ok(UpstreamTransportEventError(SmolStr::new_static("upstream_protocol_error")))
            },

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
            UpstreamError::Connect(conn_err) => UpstreamError::Connect(conn_err.clone()),
            UpstreamError::Io(io_err) => {
                let new_io_err = io::Error::new(io_err.kind(), io_err.to_string());
                UpstreamError::Io(new_io_err)
            },
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
            UpstreamError::Connect(conn_err) => ResponseFlags(conn_err.context.response_flags),
            UpstreamError::Io(_) => ResponseFlags(FmtResponseFlags::UPSTREAM_CONNECTION_FAILURE),
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

// ==================== Typed DownstreamError Conversions ====================

impl From<&h2::Error> for DownstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &h2::Error) -> Self {
        if let Some(reason) = err.reason() {
            match reason {
                h2::Reason::REFUSED_STREAM | h2::Reason::CANCEL | h2::Reason::NO_ERROR => DownstreamError::Reset,
                h2::Reason::PROTOCOL_ERROR
                | h2::Reason::FRAME_SIZE_ERROR
                | h2::Reason::FLOW_CONTROL_ERROR
                | h2::Reason::SETTINGS_TIMEOUT
                | h2::Reason::COMPRESSION_ERROR => DownstreamError::Protocol(format!("h2 protocol error: {reason:?}")),
                h2::Reason::CONNECT_ERROR => {
                    DownstreamError::Io(io::Error::new(io::ErrorKind::ConnectionRefused, "H2 connection refused"))
                },
                _ => DownstreamError::Other(err.to_string()),
            }
        } else {
            DownstreamError::Reset
        }
    }
}

impl From<h2::Error> for DownstreamError {
    #[inline]
    fn from(err: h2::Error) -> Self {
        DownstreamError::from(&err)
    }
}

impl From<&hyper::Error> for DownstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &hyper::Error) -> Self {
        if err.is_timeout() {
            DownstreamError::Timeout
        } else if err.is_incomplete_message()
            || err.is_canceled()
            || err.is_closed()
            || err.is_body_write_aborted()
            || err.is_user()
        {
            DownstreamError::Reset
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

impl From<&io::Error> for DownstreamError {
    #[cold]
    #[inline(never)]
    fn from(io: &io::Error) -> Self {
        if io.kind() == io::ErrorKind::TimedOut {
            DownstreamError::Timeout
        } else if matches!(
            io.kind(),
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted | io::ErrorKind::BrokenPipe
        ) {
            DownstreamError::Reset
        } else {
            DownstreamError::Io(io::Error::new(io.kind(), io.to_string()))
        }
    }
}

impl From<&PolyBodyError> for DownstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &PolyBodyError) -> Self {
        match err {
            PolyBodyError::Hyper(h) => DownstreamError::from(h.as_ref()),
            PolyBodyError::TimedOut => DownstreamError::Timeout,
            PolyBodyError::Io(io) => DownstreamError::from(io.as_ref()),
            PolyBodyError::Grpc(g) => DownstreamError::Other(g.to_string()),
            PolyBodyError::ExtProc(e) => DownstreamError::Other(e.to_string()),
            PolyBodyError::TrailersNotSupported(s) => DownstreamError::Other((*s).to_owned()),
        }
    }
}

impl From<PolyBodyError> for DownstreamError {
    #[inline]
    fn from(err: PolyBodyError) -> Self {
        DownstreamError::from(&err)
    }
}

impl From<&TimeoutBodyError<PolyBodyError>> for DownstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &TimeoutBodyError<PolyBodyError>) -> Self {
        match err {
            TimeoutBodyError::TimedOut => DownstreamError::Timeout,
            TimeoutBodyError::BodyError(inner) => DownstreamError::from(inner),
        }
    }
}

impl From<TimeoutBodyError<PolyBodyError>> for DownstreamError {
    #[inline]
    fn from(err: TimeoutBodyError<PolyBodyError>) -> Self {
        DownstreamError::from(&err)
    }
}

impl From<&TimeoutBodyError<hyper::Error>> for DownstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &TimeoutBodyError<hyper::Error>) -> Self {
        match err {
            TimeoutBodyError::TimedOut => DownstreamError::Timeout,
            TimeoutBodyError::BodyError(inner) => DownstreamError::from(inner),
        }
    }
}

impl From<TimeoutBodyError<hyper::Error>> for DownstreamError {
    #[inline]
    fn from(err: TimeoutBodyError<hyper::Error>) -> Self {
        DownstreamError::from(&err)
    }
}

impl From<&(dyn std::error::Error + 'static)> for DownstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &(dyn std::error::Error + 'static)) -> Self {
        enum ProtocolErr<'a> {
            H2(h2::Reason),
            Hyper(&'a hyper::Error),
        }

        let mut curr: Option<&(dyn std::error::Error + 'static)> = Some(err);
        let mut reset = false;
        let mut protocol: Option<ProtocolErr<'_>> = None;
        let mut fallback_io: Option<&io::Error> = None;

        while let Some(e) = curr {
            if let Some(downstream) = e.downcast_ref::<DownstreamError>() {
                return downstream.clone();
            }
            if let Some(crate_err) = e.downcast_ref::<crate::Error>() {
                if let Some(downstream) = crate_err.as_downstream_error() {
                    return downstream.clone();
                }
            }

            if let Some(io) = e.downcast_ref::<io::Error>() {
                if io.kind() == io::ErrorKind::TimedOut {
                    return DownstreamError::Timeout;
                }
                if matches!(
                    io.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted | io::ErrorKind::BrokenPipe
                ) {
                    reset = true;
                }
                if fallback_io.is_none() {
                    fallback_io = Some(io);
                }
            } else if let Some(h) = e.downcast_ref::<hyper::Error>() {
                if h.is_timeout() {
                    return DownstreamError::Timeout;
                }
                if h.is_canceled() || h.is_closed() {
                    reset = true;
                }
                if protocol.is_none() && (h.is_parse() || h.is_parse_status() || h.is_parse_too_large()) {
                    protocol = Some(ProtocolErr::Hyper(h));
                }
            } else if let Some(h2_err) = e.downcast_ref::<h2::Error>() {
                let reason = h2_err.reason();
                if matches!(reason, Some(h2::Reason::REFUSED_STREAM | h2::Reason::CANCEL | h2::Reason::NO_ERROR) | None)
                {
                    reset = true;
                }
                if protocol.is_none() {
                    if let Some(reason) = reason {
                        if matches!(
                            reason,
                            h2::Reason::PROTOCOL_ERROR
                                | h2::Reason::FRAME_SIZE_ERROR
                                | h2::Reason::FLOW_CONTROL_ERROR
                                | h2::Reason::SETTINGS_TIMEOUT
                                | h2::Reason::COMPRESSION_ERROR
                        ) {
                            protocol = Some(ProtocolErr::H2(reason));
                        }
                    }
                }
            } else if e.is::<Elapsed>() {
                return DownstreamError::Timeout;
            } else if let Some(t) = e.downcast_ref::<TimeoutBodyError<PolyBodyError>>() {
                if matches!(t, TimeoutBodyError::TimedOut | TimeoutBodyError::BodyError(PolyBodyError::TimedOut)) {
                    return DownstreamError::Timeout;
                }
            } else if let Some(t) = e.downcast_ref::<TimeoutBodyError<hyper::Error>>() {
                if matches!(t, TimeoutBodyError::TimedOut) {
                    return DownstreamError::Timeout;
                }
            } else if let Some(p) = e.downcast_ref::<PolyBodyError>() {
                if matches!(p, PolyBodyError::TimedOut) {
                    return DownstreamError::Timeout;
                }
            }

            curr = e.source();
        }

        if reset {
            DownstreamError::Reset
        } else if let Some(proto) = protocol {
            match proto {
                ProtocolErr::H2(reason) => DownstreamError::Protocol(format!("h2 protocol error: {reason:?}")),
                ProtocolErr::Hyper(h) => DownstreamError::Protocol(h.to_string()),
            }
        } else if let Some(io) = fallback_io {
            DownstreamError::Io(io::Error::new(io.kind(), io.to_string()))
        } else {
            DownstreamError::Other(err.to_string())
        }
    }
}

impl From<&(dyn std::error::Error + Send + Sync + 'static)> for DownstreamError {
    #[inline]
    fn from(err: &(dyn std::error::Error + Send + Sync + 'static)) -> Self {
        DownstreamError::from(err as &(dyn std::error::Error + 'static))
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for DownstreamError {
    #[inline]
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        DownstreamError::from(err.as_ref())
    }
}

impl From<Box<dyn std::error::Error + 'static>> for DownstreamError {
    #[inline]
    fn from(err: Box<dyn std::error::Error + 'static>) -> Self {
        DownstreamError::from(err.as_ref())
    }
}

impl From<&crate::Error> for DownstreamError {
    #[inline]
    fn from(err: &crate::Error) -> Self {
        DownstreamError::from(err as &(dyn std::error::Error + 'static))
    }
}

impl From<crate::Error> for DownstreamError {
    #[inline]
    fn from(err: crate::Error) -> Self {
        DownstreamError::from(&err)
    }
}

// ==================== Typed UpstreamError Conversions ====================

impl From<&h2::Error> for UpstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &h2::Error) -> Self {
        if let Some(reason) = err.reason() {
            match reason {
                h2::Reason::REFUSED_STREAM => UpstreamError::RefusedStream,
                h2::Reason::CANCEL | h2::Reason::NO_ERROR => UpstreamError::Reset,
                h2::Reason::PROTOCOL_ERROR
                | h2::Reason::FRAME_SIZE_ERROR
                | h2::Reason::FLOW_CONTROL_ERROR
                | h2::Reason::SETTINGS_TIMEOUT
                | h2::Reason::COMPRESSION_ERROR => UpstreamError::Protocol(format!("h2 protocol error: {reason:?}")),
                h2::Reason::CONNECT_ERROR => {
                    UpstreamError::Io(io::Error::new(io::ErrorKind::ConnectionRefused, "H2 connection refused"))
                },
                _ => UpstreamError::Other(err.to_string()),
            }
        } else {
            UpstreamError::Reset
        }
    }
}

impl From<h2::Error> for UpstreamError {
    #[inline]
    fn from(err: h2::Error) -> Self {
        UpstreamError::from(&err)
    }
}

impl From<&hyper::Error> for UpstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &hyper::Error) -> Self {
        if err.is_timeout() || err.is_incomplete_message() {
            UpstreamError::PerTryTimeout
        } else if err.is_canceled() || err.is_closed() || err.is_body_write_aborted() {
            UpstreamError::Reset
        } else if err.is_user() {
            UpstreamError::Io(std::io::Error::new(std::io::ErrorKind::ConnectionAborted, err.to_string()))
        } else if err.is_parse() || err.is_parse_status() || err.is_parse_too_large() {
            UpstreamError::Protocol(err.to_string())
        } else {
            UpstreamError::from(err as &(dyn std::error::Error + 'static))
        }
    }
}

impl From<hyper::Error> for UpstreamError {
    #[inline]
    fn from(err: hyper::Error) -> Self {
        UpstreamError::from(&err)
    }
}

impl From<&io::Error> for UpstreamError {
    #[cold]
    #[inline(never)]
    fn from(io: &io::Error) -> Self {
        if io.kind() == io::ErrorKind::TimedOut {
            UpstreamError::PerTryTimeout
        } else if matches!(
            io.kind(),
            io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted | io::ErrorKind::BrokenPipe
        ) {
            UpstreamError::Reset
        } else {
            UpstreamError::Io(io::Error::new(io.kind(), io.to_string()))
        }
    }
}

impl From<&PolyBodyError> for UpstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &PolyBodyError) -> Self {
        match err {
            PolyBodyError::Hyper(h) => UpstreamError::from(h.as_ref()),
            PolyBodyError::TimedOut => UpstreamError::PerTryTimeout,
            PolyBodyError::Io(io) => UpstreamError::from(io.as_ref()),
            PolyBodyError::Grpc(g) => UpstreamError::Other(g.to_string()),
            PolyBodyError::ExtProc(e) => UpstreamError::Other(e.to_string()),
            PolyBodyError::TrailersNotSupported(s) => UpstreamError::Other((*s).to_owned()),
        }
    }
}

impl From<PolyBodyError> for UpstreamError {
    #[inline]
    fn from(err: PolyBodyError) -> Self {
        UpstreamError::from(&err)
    }
}

impl From<&TimeoutBodyError<PolyBodyError>> for UpstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &TimeoutBodyError<PolyBodyError>) -> Self {
        match err {
            TimeoutBodyError::TimedOut => UpstreamError::PerTryTimeout,
            TimeoutBodyError::BodyError(inner) => UpstreamError::from(inner),
        }
    }
}

impl From<TimeoutBodyError<PolyBodyError>> for UpstreamError {
    #[inline]
    fn from(err: TimeoutBodyError<PolyBodyError>) -> Self {
        UpstreamError::from(&err)
    }
}

impl From<&TimeoutBodyError<hyper::Error>> for UpstreamError {
    #[cold]
    #[inline(never)]
    fn from(err: &TimeoutBodyError<hyper::Error>) -> Self {
        match err {
            TimeoutBodyError::TimedOut => UpstreamError::PerTryTimeout,
            TimeoutBodyError::BodyError(inner) => UpstreamError::from(inner),
        }
    }
}

impl From<TimeoutBodyError<hyper::Error>> for UpstreamError {
    #[inline]
    fn from(err: TimeoutBodyError<hyper::Error>) -> Self {
        UpstreamError::from(&err)
    }
}

impl From<ConnectError> for UpstreamError {
    #[inline]
    fn from(err: ConnectError) -> Self {
        UpstreamError::Connect(Box::new(err))
    }
}

impl From<&ConnectError> for UpstreamError {
    #[inline]
    fn from(err: &ConnectError) -> Self {
        UpstreamError::Connect(Box::new(err.clone()))
    }
}

impl From<&(dyn std::error::Error + 'static)> for UpstreamError {
    #[cold]
    #[inline(never)]
    #[allow(clippy::too_many_lines)]
    fn from(err: &(dyn std::error::Error + 'static)) -> Self {
        enum ProtocolErr<'a> {
            H2(h2::Reason),
            Hyper(&'a hyper::Error),
        }

        enum FallbackIo<'a> {
            Io(&'a io::Error),
            H2Connect,
        }

        let mut curr: Option<&(dyn std::error::Error + 'static)> = Some(err);
        let mut reset = false;
        let mut refused = false;
        let mut protocol: Option<ProtocolErr<'_>> = None;
        let mut fallback_io: Option<FallbackIo<'_>> = None;

        while let Some(e) = curr {
            if let Some(upstream) = e.downcast_ref::<UpstreamError>() {
                return upstream.clone();
            }
            if let Some(crate_err) = e.downcast_ref::<crate::Error>() {
                if let Some(upstream) = crate_err.as_upstream_error() {
                    return upstream.clone();
                }
            }
            if let Some(conn) = e.downcast_ref::<ConnectError>() {
                return UpstreamError::Connect(Box::new(conn.clone()));
            } else if let Some(conn) = e.downcast_ref::<Box<ConnectError>>() {
                return UpstreamError::Connect(conn.clone());
            }

            if let Some(io) = e.downcast_ref::<io::Error>() {
                if io.kind() == io::ErrorKind::TimedOut {
                    return UpstreamError::PerTryTimeout;
                }
                if matches!(
                    io.kind(),
                    io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted | io::ErrorKind::BrokenPipe
                ) {
                    reset = true;
                }
                if fallback_io.is_none() {
                    fallback_io = Some(FallbackIo::Io(io));
                }
            } else if let Some(h) = e.downcast_ref::<hyper::Error>() {
                if h.is_timeout() {
                    return UpstreamError::PerTryTimeout;
                }
                if h.is_canceled() || h.is_closed() {
                    reset = true;
                }
                if protocol.is_none() && (h.is_parse() || h.is_parse_status() || h.is_parse_too_large()) {
                    protocol = Some(ProtocolErr::Hyper(h));
                }
            } else if let Some(h2_err) = e.downcast_ref::<h2::Error>() {
                let reason = h2_err.reason();
                if reason == Some(h2::Reason::REFUSED_STREAM) {
                    refused = true;
                }
                if matches!(reason, Some(h2::Reason::CANCEL | h2::Reason::NO_ERROR) | None) {
                    reset = true;
                }
                if protocol.is_none() {
                    if let Some(reason) = reason {
                        if matches!(
                            reason,
                            h2::Reason::PROTOCOL_ERROR
                                | h2::Reason::FRAME_SIZE_ERROR
                                | h2::Reason::FLOW_CONTROL_ERROR
                                | h2::Reason::SETTINGS_TIMEOUT
                                | h2::Reason::COMPRESSION_ERROR
                        ) {
                            protocol = Some(ProtocolErr::H2(reason));
                        }
                    }
                }
                if fallback_io.is_none() && reason == Some(h2::Reason::CONNECT_ERROR) {
                    fallback_io = Some(FallbackIo::H2Connect);
                }
            } else if e.is::<Elapsed>() {
                return UpstreamError::PerTryTimeout;
            } else if let Some(t) = e.downcast_ref::<TimeoutBodyError<PolyBodyError>>() {
                if matches!(t, TimeoutBodyError::TimedOut | TimeoutBodyError::BodyError(PolyBodyError::TimedOut)) {
                    return UpstreamError::PerTryTimeout;
                }
            } else if let Some(t) = e.downcast_ref::<TimeoutBodyError<hyper::Error>>() {
                if matches!(t, TimeoutBodyError::TimedOut) {
                    return UpstreamError::PerTryTimeout;
                }
            } else if let Some(p) = e.downcast_ref::<PolyBodyError>() {
                if matches!(p, PolyBodyError::TimedOut) {
                    return UpstreamError::PerTryTimeout;
                }
            }

            curr = e.source();
        }

        if refused {
            UpstreamError::RefusedStream
        } else if reset {
            UpstreamError::Reset
        } else if let Some(proto) = protocol {
            match proto {
                ProtocolErr::H2(reason) => UpstreamError::Protocol(format!("h2 protocol error: {reason:?}")),
                ProtocolErr::Hyper(h) => UpstreamError::Protocol(h.to_string()),
            }
        } else if let Some(io) = fallback_io {
            match io {
                FallbackIo::Io(io) => UpstreamError::Io(io::Error::new(io.kind(), io.to_string())),
                FallbackIo::H2Connect => {
                    UpstreamError::Io(io::Error::new(io::ErrorKind::ConnectionRefused, "H2 connection refused"))
                },
            }
        } else {
            UpstreamError::Other(err.to_string())
        }
    }
}

impl From<&(dyn std::error::Error + Send + Sync + 'static)> for UpstreamError {
    #[inline]
    fn from(err: &(dyn std::error::Error + Send + Sync + 'static)) -> Self {
        UpstreamError::from(err as &(dyn std::error::Error + 'static))
    }
}

impl From<Box<dyn std::error::Error + Send + Sync>> for UpstreamError {
    #[inline]
    fn from(err: Box<dyn std::error::Error + Send + Sync>) -> Self {
        UpstreamError::from(err.as_ref())
    }
}

impl From<Box<dyn std::error::Error + 'static>> for UpstreamError {
    #[inline]
    fn from(err: Box<dyn std::error::Error + 'static>) -> Self {
        UpstreamError::from(err.as_ref())
    }
}

impl From<&crate::Error> for UpstreamError {
    #[inline]
    fn from(err: &crate::Error) -> Self {
        UpstreamError::from(err as &(dyn std::error::Error + 'static))
    }
}

impl From<crate::Error> for UpstreamError {
    #[inline]
    fn from(err: crate::Error) -> Self {
        UpstreamError::from(&err)
    }
}

// ==================== Typed EventKind Conversions ====================

impl From<(&'_ PolyBodyError, BodyKind)> for EventKind {
    fn from((err, kind): (&PolyBodyError, BodyKind)) -> Self {
        match kind {
            BodyKind::Request => EventKind::Downstream(DownstreamError::from(err)),
            BodyKind::Response => EventKind::Upstream(UpstreamError::from(err)),
        }
    }
}

impl From<(&'_ TimeoutBodyError<PolyBodyError>, BodyKind)> for EventKind {
    fn from((err, kind): (&TimeoutBodyError<PolyBodyError>, BodyKind)) -> Self {
        match kind {
            BodyKind::Request => EventKind::Downstream(DownstreamError::from(err)),
            BodyKind::Response => EventKind::Upstream(UpstreamError::from(err)),
        }
    }
}

impl From<(&'_ TimeoutBodyError<hyper::Error>, BodyKind)> for EventKind {
    fn from((err, kind): (&TimeoutBodyError<hyper::Error>, BodyKind)) -> Self {
        match kind {
            BodyKind::Request => EventKind::Downstream(DownstreamError::from(err)),
            BodyKind::Response => EventKind::Upstream(UpstreamError::from(err)),
        }
    }
}

impl From<(&'_ hyper::Error, BodyKind)> for EventKind {
    fn from((err, kind): (&hyper::Error, BodyKind)) -> Self {
        match kind {
            BodyKind::Request => EventKind::Downstream(DownstreamError::from(err)),
            BodyKind::Response => EventKind::Upstream(UpstreamError::from(err)),
        }
    }
}

impl From<(&'_ h2::Error, BodyKind)> for EventKind {
    fn from((err, kind): (&h2::Error, BodyKind)) -> Self {
        match kind {
            BodyKind::Request => EventKind::Downstream(DownstreamError::from(err)),
            BodyKind::Response => EventKind::Upstream(UpstreamError::from(err)),
        }
    }
}

impl From<(&'_ std::convert::Infallible, BodyKind)> for EventKind {
    fn from((inf, _): (&std::convert::Infallible, BodyKind)) -> Self {
        match *inf {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Error;
    use crate::transport::connector::TcpErrorContext;

    #[test]
    fn test_upstream_connect_error() {
        use crate::transport::connector::{ConnectError, ConnectErrorKind, TcpErrorContext};
        use arion_format::types::ResponseFlags as FmtFlags;

        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, "connection refused");
        let conn_err = ConnectError {
            context: TcpErrorContext {
                upstream_addr: Some(std::net::SocketAddr::from(([10, 0, 0, 1], 9000))),
                response_flags: FmtFlags::UPSTREAM_CONNECTION_FAILURE,
                cluster_name: "backend_cluster".into(),
            },
            kind: ConnectErrorKind::Io(io_err),
        };

        assert!(conn_err.is_retriable());
        assert!(!conn_err.is_timeout());

        let upstream_err = UpstreamError::from(conn_err);
        let flags: ResponseFlags = upstream_err.clone().into();
        assert_eq!(flags.0, FmtFlags::UPSTREAM_CONNECTION_FAILURE);

        let err = Error::upstream(upstream_err);
        let ctx = err.upstream_context().expect("Should extract context from UpstreamError::Connect");
        assert_eq!(ctx.cluster_name.as_str(), "backend_cluster");
        assert_eq!(ctx.upstream_addr, Some(std::net::SocketAddr::from(([10, 0, 0, 1], 9000))));

        assert!(err.find_source::<io::Error>().is_some(), "Should find inner io::Error");

        let timeout_conn_err = ConnectError {
            context: TcpErrorContext {
                upstream_addr: Some(std::net::SocketAddr::from(([10, 0, 0, 1], 9000))),
                response_flags: FmtFlags::UPSTREAM_CONNECTION_FAILURE,
                cluster_name: "backend_cluster".into(),
            },
            kind: ConnectErrorKind::Timeout(elapsed()),
        };
        assert!(timeout_conn_err.is_timeout());
        assert!(timeout_conn_err.is_retriable());
        assert!(timeout_conn_err.elapsed().is_some());

        let timeout_err = Error::upstream(timeout_conn_err);
        assert!(
            timeout_err.find_source::<tokio::time::error::Elapsed>().is_some(),
            "Elapsed must be preserved in source chain"
        );
    }

    #[test]
    fn test_error_chain_debug() {
        let conn_err = ConnectError {
            context: TcpErrorContext {
                upstream_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 8080))),
                response_flags: arion_format::types::ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
                cluster_name: "test_cluster".into(),
            },
            kind: ConnectErrorKind::Io(io::Error::new(io::ErrorKind::ConnectionRefused, "Connection refused")),
        };
        let err = Error::upstream(conn_err);

        let found = err.find_source::<io::Error>();
        assert!(found.is_some(), "Should find std::io::Error in chain!");

        let ctx = err.upstream_context().expect("Should find TcpErrorContext in chain!");
        assert_eq!(ctx.cluster_name.as_str(), "test_cluster");
    }

    #[test]
    fn test_error_classification_through_wrappers() {
        let err = Error::upstream(UpstreamError::RouteTimeout);
        let upstream = err.as_upstream_error();
        assert!(matches!(upstream, Some(UpstreamError::RouteTimeout)), "boxed: got {upstream:?}");

        let conn_err = ConnectError {
            context: TcpErrorContext {
                cluster_name: "test_cluster".into(),
                upstream_addr: Some(std::net::SocketAddr::from(([127, 0, 0, 1], 8080))),
                response_flags: arion_format::types::ResponseFlags::UPSTREAM_CONNECTION_FAILURE,
            },
            kind: ConnectErrorKind::Timeout(elapsed()),
        };
        let err = Error::upstream(conn_err);
        let upstream = err.as_upstream_error();
        assert!(matches!(upstream, Some(UpstreamError::Connect(c)) if c.is_timeout()), "wrapped: got {upstream:?}");
        assert!(err.upstream_context().is_some());

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

    #[test]
    fn test_timeout_body_error_mapping() {
        let err: TimeoutBodyError<PolyBodyError> = TimeoutBodyError::TimedOut;
        let downstream = DownstreamError::from(&err);
        assert!(matches!(downstream, DownstreamError::Timeout));

        let upstream = UpstreamError::from(&err);
        assert!(matches!(upstream, UpstreamError::PerTryTimeout));

        // PolyBodyError::TimedOut wrapped in TimeoutBodyError::BodyError
        let body_timeout_err: TimeoutBodyError<PolyBodyError> = TimeoutBodyError::BodyError(PolyBodyError::TimedOut);
        let downstream = DownstreamError::from(&body_timeout_err);
        assert!(matches!(downstream, DownstreamError::Timeout));
        let upstream = UpstreamError::from(&body_timeout_err);
        assert!(matches!(upstream, UpstreamError::PerTryTimeout));

        // Plain PolyBodyError::TimedOut
        let poly_timeout = PolyBodyError::TimedOut;
        let downstream = DownstreamError::from(&poly_timeout);
        assert!(matches!(downstream, DownstreamError::Timeout));
        let upstream = UpstreamError::from(&poly_timeout);
        assert!(matches!(upstream, UpstreamError::PerTryTimeout));

        // Non-timeout TimeoutBodyError should not map to Timeout
        let body_io_err: TimeoutBodyError<PolyBodyError> = TimeoutBodyError::BodyError(PolyBodyError::Io(
            std::sync::Arc::new(io::Error::new(io::ErrorKind::ConnectionReset, "connection reset")),
        ));
        let downstream = DownstreamError::from(&body_io_err);
        assert!(!matches!(downstream, DownstreamError::Timeout));
        let upstream = UpstreamError::from(&body_io_err);
        assert!(!matches!(upstream, UpstreamError::PerTryTimeout));

        // Test From trait objects with 'static bound
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(TimeoutBodyError::<PolyBodyError>::TimedOut);
        let downstream = DownstreamError::from(boxed.as_ref());
        assert!(matches!(downstream, DownstreamError::Timeout));
        let upstream = UpstreamError::from(boxed.as_ref());
        assert!(matches!(upstream, UpstreamError::PerTryTimeout));

        let dyn_ref: &(dyn std::error::Error + 'static) = &TimeoutBodyError::<PolyBodyError>::TimedOut;
        let downstream = DownstreamError::from(dyn_ref);
        assert!(matches!(downstream, DownstreamError::Timeout));
        let upstream = UpstreamError::from(dyn_ref);
        assert!(matches!(upstream, UpstreamError::PerTryTimeout));
    }
}
