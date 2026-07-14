//! Shared ABI types between the Orion proxy (host) and the Orion Wasm SDK (guest).
//!
//! The numeric values of every enum variant are part of the **stable ABI**:
//! changing them is a breaking change that requires bumping the crate version
//! and updating both the host and the guest.

use core::convert::TryFrom;

/// Result codes returned by Orion hostcalls.
///
/// Mirrored on the host side by
/// `orion-lib/src/listeners/http_connection_manager/wasm/types.rs` (which
/// simply re-exports this crate).
#[repr(i32)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OrionWasmResult {
    /// Operation completed successfully.
    Ok = 0,
    /// The requested resource (e.g. a header) was not found.
    NotFound = 1,
    /// The caller-provided buffer was too small to hold the result.
    BufferTooSmall = 2,
    /// An invalid memory access was attempted (out-of-bounds Wasm memory).
    InvalidMemoryAccess = 3,
    /// An internal host error occurred.
    InternalError = 4,
}

/// Error returned when converting a raw `i32` into [`OrionWasmResult`] and the
/// value does not match any known variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnknownOrionWasmResult(pub i32);

impl core::fmt::Display for UnknownOrionWasmResult {
    #[inline]
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "unknown OrionWasmResult value: {}", self.0)
    }
}

impl TryFrom<i32> for OrionWasmResult {
    type Error = UnknownOrionWasmResult;

    #[inline]
    fn try_from(v: i32) -> Result<Self, Self::Error> {
        match v {
            0 => Ok(Self::Ok),
            1 => Ok(Self::NotFound),
            2 => Ok(Self::BufferTooSmall),
            3 => Ok(Self::InvalidMemoryAccess),
            4 => Ok(Self::InternalError),
            _ => Err(UnknownOrionWasmResult(v)),
        }
    }
}

impl From<OrionWasmResult> for i32 {
    #[inline]
    fn from(v: OrionWasmResult) -> Self {
        v as i32
    }
}

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
    /// A direct response has been produced via `send_direct_response`; the
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

use http::{HeaderMap, Method, StatusCode, header::{HeaderName, HeaderValue}};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

pub enum HeaderMutation {
    Set(HeaderName, HeaderValue),
    Add(HeaderName, HeaderValue),
    Replace(HeaderName, HeaderValue),
    Remove(HeaderName),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CalloutRequest {
    pub cluster_name: SmolStr,
    pub path: SmolStr,
    #[serde(with = "http_serde_ext::method")]
    pub method: Method,
    #[serde(with = "http_serde_ext::header_map")]
    pub headers: HeaderMap,
    pub body: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct CalloutResponse {
    #[serde(with = "http_serde_ext::status_code")]
    pub status: StatusCode,
    #[serde(with = "http_serde_ext::header_map")]
    pub headers: HeaderMap,
    pub body: Option<Vec<u8>>,
}
