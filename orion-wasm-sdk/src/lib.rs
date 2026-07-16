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
//! The ABI types (`OrionWasmResult`, `FilterAction`) are shared with the host
//! via the standalone [`orion_wasm_types`] crate.

pub use orion_wasm_types::{
    CalloutRequest, CalloutResponse, FilterAction, HeaderMutation, OrionWasmError, OrionWasmResult,
};

// ============================================================================
// FFI declarations — match the hostcalls registered in
// orion-lib/src/listeners/http_connection_manager/wasm/hostcalls.rs
// ============================================================================
mod ffi;

// ============================================================================
// Header Map serialization
// ============================================================================

pub use http::{header::HeaderName, header::HeaderValue, HeaderMap};

mod internal;
use crate::internal::*;

// ============================================================================
// Typestate API

// ============================================================================

/// Marker type representing the headers phase of an HTTP request or response.
pub struct HttpHeaders;
/// Marker type representing the body phase of an HTTP request or response.
pub struct HttpBody;

pub trait State {}
impl State for HttpHeaders {}
impl State for HttpBody {}

/// Typestate wrapper around a request handle.
pub struct RequestHandle<S: State> {
    handle: u64,
    _marker: core::marker::PhantomData<S>,
}

impl<S: State> RequestHandle<S> {
    /// Create a new request handle.
    #[doc(hidden)]
    pub unsafe fn new(handle: u64) -> Self {
        Self { handle, _marker: core::marker::PhantomData }
    }
}

/// Typestate wrapper around a response handle.
pub struct ResponseHandle<S: State> {
    handle: u64,
    _marker: core::marker::PhantomData<S>,
}

impl<S: State> ResponseHandle<S> {
    /// Create a new response handle.
    #[doc(hidden)]
    pub unsafe fn new(handle: u64) -> Self {
        Self { handle, _marker: core::marker::PhantomData }
    }
}

impl RequestHandle<HttpHeaders> {
    /// Read an HTTP request header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmError> {
        get_http_header(self.handle, ffi::HeaderTarget::Request, name)
    }

    pub fn set_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        set_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    pub fn add_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        add_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    pub fn remove_header(&self, name: &HeaderName) -> Result<(), OrionWasmError> {
        remove_http_header(self.handle, ffi::HeaderTarget::Request, name)
    }

    pub fn replace_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        replace_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmError> {
        get_http_headers_map(self.handle, ffi::HeaderTarget::Request)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmError> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::Request, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmError> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::Request, mutations)
    }

    pub fn send_direct_response(&self, status_code: u16, body: &[u8]) -> Result<(), OrionWasmError> {
        send_http_direct_response(self.handle, status_code, body)
    }

    /// Convenience wrapper around `send_direct_response`.
    pub fn direct_response(&self, status_code: u16, body: &[u8]) -> FilterAction {
        match self.send_direct_response(status_code, body) {
            Ok(()) => FilterAction::DirectResponse,
            Err(_) => FilterAction::Continue,
        }
    }
}

impl RequestHandle<HttpBody> {
    /// Read an HTTP request header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmError> {
        get_http_header(self.handle, ffi::HeaderTarget::Request, name)
    }

    pub fn set_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        set_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    pub fn add_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        add_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    pub fn remove_header(&self, name: &HeaderName) -> Result<(), OrionWasmError> {
        remove_http_header(self.handle, ffi::HeaderTarget::Request, name)
    }

    pub fn replace_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        replace_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    /// Read the buffered request body.
    pub fn get_body(&self) -> Result<Vec<u8>, OrionWasmError> {
        get_http_body(self.handle, ffi::HeaderTarget::Request)
    }

    /// Replace the buffered request body.
    pub fn set_body(&self, body: &[u8]) -> Result<(), OrionWasmError> {
        set_http_body(self.handle, ffi::HeaderTarget::Request, body)
    }

    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmError> {
        get_http_headers_map(self.handle, ffi::HeaderTarget::Request)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmError> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::Request, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmError> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::Request, mutations)
    }

    pub fn send_direct_response(&self, status_code: u16, body: &[u8]) -> Result<(), OrionWasmError> {
        send_http_direct_response(self.handle, status_code, body)
    }

    /// Convenience wrapper around `send_direct_response`.
    pub fn direct_response(&self, status_code: u16, body: &[u8]) -> FilterAction {
        match self.send_direct_response(status_code, body) {
            Ok(()) => FilterAction::DirectResponse,
            Err(_) => FilterAction::Continue,
        }
    }
}

impl ResponseHandle<HttpHeaders> {
    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmError> {
        get_http_headers_map(self.handle, ffi::HeaderTarget::Response)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmError> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::Response, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmError> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::Response, mutations)
    }

    /// Read an HTTP response header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmError> {
        get_http_header(self.handle, ffi::HeaderTarget::Response, name)
    }

    pub fn set_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        set_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    pub fn add_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        add_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    pub fn remove_header(&self, name: &HeaderName) -> Result<(), OrionWasmError> {
        remove_http_header(self.handle, ffi::HeaderTarget::Response, name)
    }

    pub fn replace_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        replace_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }
}

impl ResponseHandle<HttpBody> {
    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmError> {
        get_http_headers_map(self.handle, ffi::HeaderTarget::Response)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmError> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::Response, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmError> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::Response, mutations)
    }

    /// Read an HTTP response header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmError> {
        get_http_header(self.handle, ffi::HeaderTarget::Response, name)
    }

    pub fn set_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        set_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    pub fn add_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        add_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    pub fn remove_header(&self, name: &HeaderName) -> Result<(), OrionWasmError> {
        remove_http_header(self.handle, ffi::HeaderTarget::Response, name)
    }

    pub fn replace_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        replace_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    /// Read the buffered response body.
    pub fn get_body(&self) -> Result<Vec<u8>, OrionWasmError> {
        get_http_body(self.handle, ffi::HeaderTarget::Response)
    }

    /// Replace the buffered response body.
    pub fn set_body(&self, body: &[u8]) -> Result<(), OrionWasmError> {
        set_http_body(self.handle, ffi::HeaderTarget::Response, body)
    }
}

// ============================================================================
// High-level `Plugin` trait + `orion_plugin!` macro
// ============================================================================

/// Read the plugin configuration.
pub fn get_plugin_config() -> Result<Option<String>, OrionWasmError> {
    let mut buf: Vec<u8> = Vec::with_capacity(DEFAULT_HEAP_BUF_SIZE);
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::orion_get_plugin_config(buf.as_mut_ptr(), buf.capacity() as u32, &mut written_len as *mut u32)
        };

        match OrionWasmResult::from_ffi(res) {
            Ok(()) => {
                unsafe {
                    buf.set_len(written_len as usize);
                }
                return String::from_utf8(buf).map(Some).map_err(|_| OrionWasmError::InternalError);
            },
            Err(OrionWasmError::NotFound) => return Ok(None),
            Err(OrionWasmError::BufferTooSmall) => {
                let new_cap = buf.capacity().saturating_mul(2);
                if new_cap == buf.capacity() {
                    return Err(OrionWasmError::BufferTooSmall);
                }
                buf.reserve_exact(new_cap);
            },
            Err(other) => return Err(other),
        }
    }
}

/// Set multiple custom metric key-value pairs at once.
pub fn set_custom_metrics<'a, I>(metrics: I) -> Result<(), OrionWasmError>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let pairs: Vec<(&str, &str)> = metrics.into_iter().collect();
    let buf = bincode_next::serde::encode_to_vec(pairs.as_slice(), bincode_next::config::standard())
        .map_err(|_| OrionWasmError::InternalError)?;
    let res = unsafe { ffi::orion_set_custom_metrics(buf.as_ptr(), buf.len() as u32) };
    OrionWasmResult::from_ffi(res)
}

/// Set multiple access log operators at once.
pub fn set_access_log_operators<'a, I>(operators: I) -> Result<(), OrionWasmError>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let pairs: Vec<(&str, &str)> = operators.into_iter().collect();
    let buf = bincode_next::serde::encode_to_vec(pairs.as_slice(), bincode_next::config::standard())
        .map_err(|_| OrionWasmError::InternalError)?;
    let res = unsafe { ffi::orion_set_access_log_operators(buf.as_ptr(), buf.len() as u32) };
    OrionWasmResult::from_ffi(res)
}

/// Idiomatic interface implemented by Orion Wasm plugins.
pub trait Plugin {
    /// Invoked once when the Wasm module is instantiated.
    #[inline]
    fn on_plugin_start(&mut self) {}

    /// Invoked when the Wasm module instance is destroyed by the host.
    #[inline]
    fn on_plugin_destroy(&mut self) {}

    /// Invoked at the beginning of a new HTTP request/transaction.
    #[inline]
    fn on_transaction_start(&mut self) {}

    /// Invoked on the request path before the body has been buffered.
    #[inline]
    fn on_request_headers(&mut self, _ctx: &RequestHandle<HttpHeaders>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked after the plugin returned [`FilterAction::PauseAndBufferBody`]
    /// from [`Plugin::on_request_headers`] and the host has buffered the full
    /// request body.
    #[inline]
    fn on_request_body(&mut self, _ctx: &RequestHandle<HttpBody>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked on the response path before the body has been buffered.
    #[inline]
    fn on_response_headers(&mut self, _ctx: &ResponseHandle<HttpHeaders>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked after the plugin returned [`FilterAction::PauseAndBufferBody`]
    /// from [`Plugin::on_response_headers`] and the host has buffered the full
    /// response body.
    #[inline]
    fn on_response_body(&mut self, _ctx: &ResponseHandle<HttpBody>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked when the request/response has been fully processed and the transaction is complete.
    /// This is a good place to clean up any transaction-specific resources before the plugin is reused.
    #[inline]
    fn on_transaction_complete(&mut self) {}
}

/// # Example
///
/// ```no_run
/// use orion_wasm_sdk::{Plugin, FilterAction, RequestHandle, RequestHeaders};
///
/// #[derive(Default)]
/// struct AuthFilter;
///
/// #[orion_plugin]
/// impl Plugin for AuthFilter {
///     fn on_request_headers(&mut self, ctx: &RequestHandle<RequestHeaders>) -> FilterAction {
///         match ctx.get_header("Authorization") {
///             Ok(Some(v)) if v == "Bearer secret-token" => FilterAction::Continue,
///             _ => ctx.direct_response(401, b"unauthorized"),
///         }
///     }
/// }
///
/// ```
pub use orion_wasm_sdk_macros::orion_plugin;

// ============================================================================
// Tracing integration
// ============================================================================

struct WasmVisitor {
    message: String,
}

impl tracing::field::Visit for WasmVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write;
        if field.name() == "message" {
            let _ = write!(self.message, "{:?} ", value);
        } else {
            let _ = write!(self.message, "{}={:?} ", field.name(), value);
        }
    }
}

pub struct OrionWasmSubscriber;

impl tracing::Subscriber for OrionWasmSubscriber {
    fn enabled(&self, _metadata: &tracing::Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut visitor = WasmVisitor { message: String::new() };
        event.record(&mut visitor);

        let level = match *event.metadata().level() {
            tracing::Level::ERROR => 1,
            tracing::Level::WARN => 2,
            tracing::Level::INFO => 3,
            tracing::Level::DEBUG => 4,
            tracing::Level::TRACE => 5,
        };

        unsafe {
            ffi::orion_log(level, visitor.message.as_ptr(), visitor.message.len() as u32);
        }
    }

    fn enter(&self, _span: &tracing::span::Id) {}

    fn exit(&self, _span: &tracing::span::Id) {}
}

pub fn init_tracing() -> Result<(), tracing::subscriber::SetGlobalDefaultError> {
    tracing::subscriber::set_global_default(OrionWasmSubscriber)
}

/// Dispatches an asynchronous HTTP call using the host's cluster manager.
pub fn dispatch_http_call(request: &CalloutRequest) -> Result<CalloutResponse, OrionWasmError> {
    let req_bytes = match bincode_next::serde::encode_to_vec(request, bincode_next::config::standard()) {
        Ok(b) => b,
        Err(_) => return Err(OrionWasmError::InternalError),
    };

    let mut resp_ptr: *mut u8 = std::ptr::null_mut();
    let mut resp_len = 0u32;

    let res = unsafe {
        ffi::orion_dispatch_http_call(
            req_bytes.as_ptr(),
            req_bytes.len() as u32,
            &mut resp_ptr as *mut *mut u8,
            &mut resp_len as *mut u32,
        )
    };

    if res == 0 {
        if resp_ptr.is_null() {
            return Err(OrionWasmError::InternalError);
        }
        let resp_buf = unsafe { Vec::from_raw_parts(resp_ptr, resp_len as usize, resp_len as usize) };
        match bincode_next::serde::decode_from_slice(&resp_buf, bincode_next::config::standard()) {
            Ok((resp, _)) => Ok(resp),
            Err(_) => Err(OrionWasmError::InternalError),
        }
    } else {
        Err(OrionWasmResult::from_ffi(res).unwrap_err())
    }
}

#[no_mangle]
pub extern "C" fn orion_malloc(size: u32) -> *mut u8 {
    let mut buf = Vec::with_capacity(size as usize);
    let ptr = buf.as_mut_ptr();
    std::mem::forget(buf);
    ptr
}
