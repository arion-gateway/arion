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

//! Arion WebAssembly SDK (Guest side)
//!
//! This crate provides a high-level Rust API for writing WebAssembly plugins
//! that run inside the Arion proxy via the Wasmtime runtime.
//!
//! Plugin authors implement the idiomatic [`Plugin`] trait and annotate the
//! impl with [`arion_plugin`](crate::arion_plugin) to generate the `extern "C"`
//! entry points the host imports. Hostcalls such as `arion_get_header`,
//! `arion_get_body`, `arion_set_body`, and `arion_send_direct_response` are
//! wrapped by the SDK so user code never touches raw FFI.
//!
//! This crate also exports [`arion_malloc`] for the host (callouts, metadata).
//!
//! The ABI types (e.g. [`FilterAction`]) are shared with the host via the
//! standalone [`arion_wasm_types`] crate. See the crate README for the full guide.

pub use arion_wasm_types::{ArionWasmError, CalloutRequest, CalloutResponse, FilterAction, HeaderMutation, LogLevel};
pub use bytes;
pub use http::{self, HeaderMap};

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
pub mod shared;
pub mod tracing;
pub mod typestate;

// ============================================================================
// Prelude
// ============================================================================
pub mod prelude {
    pub use crate::plugin::Plugin;
    pub use crate::request::RequestHandle;
    pub use crate::response::ResponseHandle;
    pub use crate::typestate::{HttpBody, HttpHeaders};
    pub use arion_wasm_sdk_macros::arion_plugin;
}

// Re-export core types for backward compatibility
pub use arion_wasm_types::{WasmHeaderName, WasmHeaderValue, WasmUri};
pub use host::*;
pub use plugin::*;
pub use request::*;
pub use response::*;
pub use shared::*;
pub use tracing::{init_tracing, ArionWasmSubscriber};
pub use typestate::*;

// ============================================================================
// Allocator
// ============================================================================
#[no_mangle]
pub extern "C" fn arion_malloc(size: u32) -> *mut u8 {
    let mut buf = Vec::with_capacity(size as usize);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}
