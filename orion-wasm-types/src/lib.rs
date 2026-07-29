//! Shared ABI types between the Orion proxy (host) and the Orion Wasm SDK (guest).
//!
//! The numeric values of every enum variant are part of the **stable ABI**:
//! changing them is a breaking change that requires bumping the crate version
//! and updating both the host and the guest.

use core::convert::TryFrom;

/// Error returned when converting a raw `i32` into [`OrionWasmError`] and the
/// value does not match any known variant.
/// Idiomatic Rust error type for Orion SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrionWasmError {
    NotFound,
    BufferTooSmall,
    InvalidMemoryAccess,
    InternalError,
    Timeout,
}

impl core::fmt::Display for OrionWasmError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::NotFound => write!(f, "NotFound"),
            Self::BufferTooSmall => write!(f, "BufferTooSmall"),
            Self::InvalidMemoryAccess => write!(f, "InvalidMemoryAccess"),
            Self::InternalError => write!(f, "InternalError"),
            Self::Timeout => write!(f, "Timeout"),
        }
    }
}

impl std::error::Error for OrionWasmError {}

/// Action codes that a plugin returns to the host to control filter flow.
///
/// Mirrored on the host side by
/// `orion-lib/src/listeners/http_connection_manager/wasm/types.rs` (which
/// simply re-exports this crate).
#[repr(i32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FilterAction {
    /// Continue processing — pass the request to the next filter / upstream.
    Continue = 0,
    /// Pause and ask the host to buffer the full request body, then invoke
    /// `on_request_body`.
    PauseAndBufferBody = 1,
    /// A direct response has been produced via `schedule_direct_response`; the
    /// host should short-circuit and return it to the client.
    DirectResponse = 2,
}

/// Error returned when converting a raw `i32` into [`FilterAction`] and the
/// value does not match any known variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownFilterAction(pub i32);

impl core::fmt::Display for UnknownFilterAction {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "unknown FilterAction value: {}", self.0)
    }
}

impl TryFrom<i32> for FilterAction {
    type Error = UnknownFilterAction;

    #[inline]
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Continue),
            1 => Ok(Self::PauseAndBufferBody),
            2 => Ok(Self::DirectResponse),
            _ => Err(UnknownFilterAction(v)),
        }
    }
}

impl From<FilterAction> for i32 {
    #[inline]
    fn from(v: FilterAction) -> Self {
        v as i32
    }
}

#[repr(u32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum LogLevel {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownLogLevel(pub u32);

impl core::fmt::Display for UnknownLogLevel {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "unknown LogLevel value: {}", self.0)
    }
}

impl TryFrom<u32> for LogLevel {
    type Error = UnknownLogLevel;

    #[inline]
    fn try_from(v: u32) -> Result<Self, UnknownLogLevel> {
        match v {
            1 => Ok(LogLevel::Error),
            2 => Ok(Self::Warn),
            3 => Ok(Self::Info),
            4 => Ok(Self::Debug),
            5 => Ok(Self::Trace),
            _ => Err(UnknownLogLevel(v)),
        }
    }
}

impl From<LogLevel> for u32 {
    #[inline]
    fn from(v: LogLevel) -> Self {
        v as u32
    }
}

use http::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Serialize, Deserialize)]
pub enum HeaderMutation {
    Set(
        #[serde(with = "http_serde_ext::header_name")] HeaderName,
        #[serde(with = "http_serde_ext::header_value")] HeaderValue,
    ),
    Add(
        #[serde(with = "http_serde_ext::header_name")] HeaderName,
        #[serde(with = "http_serde_ext::header_value")] HeaderValue,
    ),
    Replace(
        #[serde(with = "http_serde_ext::header_name")] HeaderName,
        #[serde(with = "http_serde_ext::header_value")] HeaderValue,
    ),
    Remove(#[serde(with = "http_serde_ext::header_name")] HeaderName),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CalloutRequest {
    pub cluster_name: SmolStr,
    #[serde(with = "http_serde_ext::request")]
    pub request: http::Request<bytes::Bytes>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CalloutResponse {
    #[serde(with = "http_serde_ext::response")]
    pub response: http::Response<bytes::Bytes>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GrpcCalloutRequest {
    pub cluster_name: SmolStr,
    pub service_name: SmolStr,
    pub method_name: SmolStr,
    pub initial_metadata: Vec<(SmolStr, SmolStr)>,
    pub message: bytes::Bytes,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct GrpcCalloutResponse {
    pub initial_metadata: Vec<(SmolStr, SmolStr)>,
    pub message: bytes::Bytes,
    pub trailing_metadata: Vec<(SmolStr, SmolStr)>,
    pub status: u32,
    pub status_message: SmolStr,
}

// ============================================================================
// Downstream Metadata
// ============================================================================

/// Represents the transport protocol from the proxy protocol header.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ProxyProtocol {
    Unspec,
    Stream,
    Datagram,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum DownstreamConnectionMetadata {
    FromSocket {
        peer_address: std::net::SocketAddr,
        local_address: std::net::SocketAddr,
    },
    FromProxyProtocol {
        original_peer_address: std::net::SocketAddr,
        original_destination_address: std::net::SocketAddr,
        protocol: ProxyProtocol,
        tlv_data: std::collections::HashMap<u8, Vec<u8>>,
        proxy_peer_address: std::net::SocketAddr,
        proxy_local_address: std::net::SocketAddr,
    },
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DownstreamMetadata {
    pub connection: DownstreamConnectionMetadata,
    pub sni: Option<String>,
    pub listener_name: String,
}

// ============================================================================
// Shared Ordering for Atomics
// ============================================================================

#[repr(u32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SharedOrdering {
    Relaxed = 0,
    Release = 1,
    Acquire = 2,
    AcqRel = 3,
    SeqCst = 4,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownSharedOrdering(pub u32);

impl core::fmt::Display for UnknownSharedOrdering {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "unknown SharedOrdering value: {}", self.0)
    }
}

impl TryFrom<u32> for SharedOrdering {
    type Error = UnknownSharedOrdering;

    #[inline]
    fn try_from(v: u32) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Relaxed),
            1 => Ok(Self::Release),
            2 => Ok(Self::Acquire),
            3 => Ok(Self::AcqRel),
            4 => Ok(Self::SeqCst),
            _ => Err(UnknownSharedOrdering(v)),
        }
    }
}

impl From<SharedOrdering> for u32 {
    #[inline]
    fn from(v: SharedOrdering) -> Self {
        v as u32
    }
}

impl From<core::sync::atomic::Ordering> for SharedOrdering {
    #[inline]
    fn from(order: core::sync::atomic::Ordering) -> Self {
        match order {
            core::sync::atomic::Ordering::Relaxed => Self::Relaxed,
            core::sync::atomic::Ordering::Release => Self::Release,
            core::sync::atomic::Ordering::Acquire => Self::Acquire,
            core::sync::atomic::Ordering::AcqRel => Self::AcqRel,
            core::sync::atomic::Ordering::SeqCst => Self::SeqCst,
            _ => Self::SeqCst, // Fallback for any other exotic orderings
        }
    }
}

impl From<SharedOrdering> for core::sync::atomic::Ordering {
    #[inline]
    fn from(order: SharedOrdering) -> Self {
        match order {
            SharedOrdering::Relaxed => Self::Relaxed,
            SharedOrdering::Release => Self::Release,
            SharedOrdering::Acquire => Self::Acquire,
            SharedOrdering::AcqRel => Self::AcqRel,
            SharedOrdering::SeqCst => Self::SeqCst,
        }
    }
}

impl From<OrionWasmError> for i32 {
    #[inline]
    fn from(e: OrionWasmError) -> Self {
        match e {
            OrionWasmError::NotFound => 1,
            OrionWasmError::BufferTooSmall => 2,
            OrionWasmError::InvalidMemoryAccess => 3,
            OrionWasmError::InternalError => 4,
            OrionWasmError::Timeout => 5,
        }
    }
}

impl OrionWasmError {
    #[inline]
    pub fn from_ffi(v: i32) -> Result<(), Self> {
        match v {
            0 => Ok(()),
            1 => Err(Self::NotFound),
            2 => Err(Self::BufferTooSmall),
            3 => Err(Self::InvalidMemoryAccess),
            4 => Err(Self::InternalError),
            5 => Err(Self::Timeout),
            _ => Err(Self::InternalError),
        }
    }
}
