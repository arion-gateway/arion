//! Orion WebAssembly SDK (Guest side)
//!
//! This crate provides a high-level Rust API for writing WebAssembly plugins
//! that run inside the Orion proxy via the Wasmtime runtime.
//!
//! Plugin authors implement the idiomatic [`Plugin`] trait and invoke the
//! [`orion_plugin!`](crate::orion_plugin!) macro to generate the `extern "C"`
//! entry points the host imports. The hostcalls (`get_request_header`,
//! `get_request_body`, `send_http_direct_response`, ...) are wrapped by the
//! SDK so user code never touches raw FFI.
//!
//! The ABI types (e.g. `FilterAction`) are shared with the host
//! via the standalone [`orion_wasm_types`] crate.

pub use orion_wasm_types::{
    CalloutRequest, CalloutResponse, FilterAction, HeaderMutation, OrionWasmError, LogLevel
};
pub use http::{self, HeaderMap};
pub use bytes;

// ============================================================================
// FFI declarations
// ============================================================================
pub mod ffi;

// ============================================================================
// Internal wrappers
// ============================================================================
mod internal;

// ============================================================================
// Modules
// ============================================================================
pub mod host;
pub mod plugin;
pub mod request;
pub mod response;
pub mod tracing;
pub mod typestate;
pub mod shared;

// ============================================================================
// Prelude
// ============================================================================
pub mod prelude {
    pub use crate::plugin::Plugin;
    pub use crate::request::RequestHandle;
    pub use crate::response::ResponseHandle;
    pub use crate::typestate::{HttpBody, HttpHeaders};
    pub use orion_wasm_sdk_macros::orion_plugin;
}

// Re-export core types for backward compatibility
pub use orion_wasm_types::{WasmHeaderName, WasmHeaderValue, WasmUri};
pub use host::*;
pub use plugin::*;
pub use request::*;
pub use response::*;
pub use tracing::{init_tracing, OrionWasmSubscriber};
pub use typestate::*;
pub use shared::*;

// ============================================================================
// Allocator
// ============================================================================
#[no_mangle]
pub extern "C" fn orion_malloc(size: u32) -> *mut u8 {
    let mut buf = Vec::with_capacity(size as usize);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}
