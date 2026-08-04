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

use serde::{Deserialize, Serialize};

use bytes::Bytes;

/// A lightweight header name that can be borrowed or owned.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum WasmHeaderName<'a> {
    Borrowed(&'a str),
    Owned(SmolStr),
}

impl<'a> WasmHeaderName<'a> {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Borrowed(s) => s.as_bytes(),
            Self::Owned(s) => s.as_bytes(),
        }
    }
}

impl<'a> From<&'a str> for WasmHeaderName<'a> {
    fn from(s: &'a str) -> Self {
        Self::Borrowed(s)
    }
}

impl<'a> From<String> for WasmHeaderName<'a> {
    fn from(s: String) -> Self {
        Self::Owned(SmolStr::from(s))
    }
}

impl<'a> From<SmolStr> for WasmHeaderName<'a> {
    fn from(s: SmolStr) -> Self {
        Self::Owned(s)
    }
}

impl<'a> From<http::header::HeaderName> for WasmHeaderName<'a> {
    fn from(name: http::header::HeaderName) -> Self {
        Self::Owned(SmolStr::new(name.as_str()))
    }
}

/// A lightweight header value that can be borrowed or owned.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum WasmHeaderValue<'a> {
    Borrowed(&'a [u8]),
    Owned(Bytes),
}

impl<'a> WasmHeaderValue<'a> {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Borrowed(b) => b,
            Self::Owned(b) => b.as_ref(),
        }
    }

    pub fn to_str(&self) -> Result<&str, core::str::Utf8Error> {
        core::str::from_utf8(self.as_bytes())
    }
}

impl<'a> From<&'a [u8]> for WasmHeaderValue<'a> {
    fn from(b: &'a [u8]) -> Self {
        Self::Borrowed(b)
    }
}

impl<'a> From<&'a str> for WasmHeaderValue<'a> {
    fn from(s: &'a str) -> Self {
        Self::Borrowed(s.as_bytes())
    }
}

impl<'a> From<Vec<u8>> for WasmHeaderValue<'a> {
    fn from(v: Vec<u8>) -> Self {
        Self::Owned(Bytes::from(v))
    }
}

impl<'a> From<String> for WasmHeaderValue<'a> {
    fn from(s: String) -> Self {
        Self::Owned(Bytes::from(s))
    }
}

impl<'a> From<Bytes> for WasmHeaderValue<'a> {
    fn from(b: Bytes) -> Self {
        Self::Owned(b)
    }
}

impl<'a> From<http::header::HeaderValue> for WasmHeaderValue<'a> {
    fn from(val: http::header::HeaderValue) -> Self {
        Self::Owned(Bytes::copy_from_slice(val.as_bytes()))
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum HeaderMutation<'a> {
    Set(#[serde(borrow)] WasmHeaderName<'a>, #[serde(borrow)] WasmHeaderValue<'a>),
    Add(#[serde(borrow)] WasmHeaderName<'a>, #[serde(borrow)] WasmHeaderValue<'a>),
    Replace(#[serde(borrow)] WasmHeaderName<'a>, #[serde(borrow)] WasmHeaderValue<'a>),
    Remove(#[serde(borrow)] WasmHeaderName<'a>),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WasmRequest {
    #[serde(with = "http_serde_ext::request")]
    pub request: http::Request<bytes::Bytes>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct WasmResponse {
    #[serde(with = "http_serde_ext::response")]
    pub response: http::Response<bytes::Bytes>,
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

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DirectResponse {
    #[serde(with = "http_serde_ext::response")]
    pub response: http::Response<bytes::Bytes>,
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
use smol_str::SmolStr;

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum WasmUri<'a> {
    Borrowed(&'a str),
    Owned(SmolStr),
}

impl<'a> WasmUri<'a> {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Borrowed(s) => s,
            Self::Owned(s) => s.as_str(),
        }
    }
}

impl<'a> From<&'a str> for WasmUri<'a> {
    fn from(s: &'a str) -> Self {
        Self::Borrowed(s)
    }
}

impl<'a> From<String> for WasmUri<'a> {
    fn from(s: String) -> Self {
        Self::Owned(SmolStr::from(s))
    }
}

impl<'a> From<&'a http::Uri> for WasmUri<'a> {
    fn from(uri: &'a http::Uri) -> Self {
        Self::Owned(smol_str::SmolStr::new(uri.to_string()))
    }
}

impl<'a> From<http::Uri> for WasmUri<'a> {
    fn from(uri: http::Uri) -> Self {
        Self::Owned(smol_str::SmolStr::new(uri.to_string()))
    }
}

impl<'a> core::borrow::Borrow<str> for WasmUri<'a> {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl<'a> WasmUri<'a> {
    pub fn as_bytes(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}
