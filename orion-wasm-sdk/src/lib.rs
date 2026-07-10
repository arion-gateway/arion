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

pub use orion_wasm_types::{FilterAction, OrionWasmResult};


// ============================================================================
// FFI declarations — match the hostcalls registered in
// orion-lib/src/listeners/http_connection_manager/wasm/hostcalls.rs
// ============================================================================
#[cfg(target_arch = "wasm32")]
mod ffi;


// ============================================================================
// Header Map serialization
// ============================================================================

pub use http::HeaderMap;

#[cfg(target_arch = "wasm32")]
fn serialize_header_map(headers: &HeaderMap) -> Vec<u8> {
    let mut buf = Vec::new();
    let num_headers = headers.iter().count() as u32;
    buf.extend_from_slice(&num_headers.to_le_bytes());

    for (key, val) in headers.iter() {
        let key_bytes = key.as_str().as_bytes();
        let val_bytes = val.as_bytes();

        buf.extend_from_slice(&(key_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(key_bytes);

        buf.extend_from_slice(&(val_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(val_bytes);
    }
    buf
}

#[cfg(target_arch = "wasm32")]
fn deserialize_header_map(data: &[u8]) -> Option<HeaderMap> {
    let mut headers = HeaderMap::new();
    if data.len() < 4 {
        return Some(headers);
    }

    let num_headers = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let mut offset = 4;

    for _ in 0..num_headers {
        if offset + 4 > data.len() { return None; }
        let key_len = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + key_len > data.len() { return None; }
        let key_bytes = &data[offset..offset+key_len];
        offset += key_len;

        if offset + 4 > data.len() { return None; }
        let val_len = u32::from_le_bytes(data[offset..offset+4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + val_len > data.len() { return None; }
        let val_bytes = &data[offset..offset+val_len];
        offset += val_len;

        if let (Ok(name), Ok(value)) = (http::header::HeaderName::from_bytes(key_bytes), http::header::HeaderValue::from_bytes(val_bytes)) {
            headers.append(name, value);
        } else {
            return None;
        }
    }
    Some(headers)
}

// ============================================================================
// High-level API
// ============================================================================


/// Read an HTTP request header by name.
#[cfg(target_arch = "wasm32")]
fn get_http_request_header(request_handle: u64, name: &str) -> Result<Option<String>, OrionWasmResult> {
    const INITIAL_BUF: usize = 1024;

    // Try with a stack buffer first to avoid allocation in the common case.
    let mut stack_buf = [0u8; INITIAL_BUF];
    let mut written_len: u32 = 0;

    let res = unsafe {
        ffi::orion_get_request_header(
            request_handle,
            name.as_ptr(),
            name.len() as u32,
            stack_buf.as_mut_ptr(),
            stack_buf.len() as u32,
            &mut written_len as *mut u32,
        )
    };

    match OrionWasmResult::try_from(res) {
        Ok(OrionWasmResult::Ok) => {
            let len = written_len as usize;
            Ok(String::from_utf8(stack_buf[..len].to_vec()).ok())
        }
        Ok(OrionWasmResult::NotFound) => Ok(None),
        Ok(OrionWasmResult::BufferTooSmall) => {
            // grow exponentially
            let mut heap_buf: Vec<u8> = vec![0u8; INITIAL_BUF * 4];
            let mut written_len: u32 = 0;

            loop {
                let res = unsafe {
                    ffi::orion_get_request_header(
                        request_handle,
                        name.as_ptr(),
                        name.len() as u32,
                        heap_buf.as_mut_ptr(),
                        heap_buf.len() as u32,
                        &mut written_len as *mut u32,
                    )
                };

                match OrionWasmResult::try_from(res) {
                    Ok(OrionWasmResult::Ok) => {
                        let len = written_len as usize;
                        return Ok(String::from_utf8(heap_buf[..len].to_vec()).ok());
                    }
                    Ok(OrionWasmResult::NotFound) => return Ok(None),
                    Ok(OrionWasmResult::BufferTooSmall) => {
                        let new_len = heap_buf.len().saturating_mul(2);
                        if new_len == heap_buf.len() {
                            return Err(OrionWasmResult::BufferTooSmall);
                        }
                        heap_buf.resize(new_len, 0);
                    }
                    Ok(other) => return Err(other),
                    Err(_) => return Err(OrionWasmResult::InternalError),
                }
            }
        }
        Ok(other) => Err(other),
        Err(_) => Err(OrionWasmResult::InternalError),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn get_http_request_header(_request_handle: u64, _name: &str) -> Result<Option<String>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

/// Read the buffered request body.
#[cfg(target_arch = "wasm32")]
fn get_http_request_body(request_handle: u64) -> Result<Vec<u8>, OrionWasmResult> {
    const INITIAL_BUF: usize = 4096;

    let mut buf: Vec<u8> = vec![0u8; INITIAL_BUF];
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::orion_get_request_body(
                request_handle,
                buf.as_mut_ptr(),
                buf.len() as u32,
                &mut written_len as *mut u32,
            )
        };

        match OrionWasmResult::try_from(res) {
            Ok(OrionWasmResult::Ok) => {
                buf.truncate(written_len as usize);
                return Ok(buf);
            }
            Ok(OrionWasmResult::BufferTooSmall) => {
                let new_len = buf.len().saturating_mul(2);
                if new_len == buf.len() {
                    return Err(OrionWasmResult::BufferTooSmall);
                }
                buf.resize(new_len, 0);
            }
            Ok(other) => return Err(other),
            Err(_) => return Err(OrionWasmResult::InternalError),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn get_http_request_body(_request_handle: u64) -> Result<Vec<u8>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

/// Send a direct (local) HTTP response and short-circuit the filter chain.
#[cfg(target_arch = "wasm32")]
fn send_http_direct_response(request_handle: u64, status_code: u16, body: &[u8]) -> Result<(), OrionWasmResult> {
    let res = unsafe {
        ffi::orion_send_direct_response(
            request_handle,
            status_code as u32,
            body.as_ptr(),
            body.len() as u32,
        )
    };

    match OrionWasmResult::try_from(res) {
        Ok(OrionWasmResult::Ok) => Ok(()),
        Ok(other) => Err(other),
        Err(_) => Err(OrionWasmResult::InternalError),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn send_http_direct_response(_request_handle: u64, _status_code: u16, _body: &[u8]) -> Result<(), OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

/// Read an HTTP response header by name.
#[cfg(target_arch = "wasm32")]
fn get_http_response_header(response_handle: u64, name: &str) -> Result<Option<String>, OrionWasmResult> {
    const INITIAL_BUF: usize = 1024;
    let mut stack_buf = [0u8; INITIAL_BUF];
    let mut written_len: u32 = 0;

    let res = unsafe {
        ffi::orion_get_response_header(
            response_handle,
            name.as_ptr(),
            name.len() as u32,
            stack_buf.as_mut_ptr(),
            stack_buf.len() as u32,
            &mut written_len as *mut u32,
        )
    };

    match OrionWasmResult::try_from(res) {
        Ok(OrionWasmResult::Ok) => {
            let len = written_len as usize;
            Ok(String::from_utf8(stack_buf[..len].to_vec()).ok())
        }
        Ok(OrionWasmResult::NotFound) => Ok(None),
        Ok(OrionWasmResult::BufferTooSmall) => {
            let mut heap_buf: Vec<u8> = vec![0u8; INITIAL_BUF * 4];
            let mut written_len: u32 = 0;

            loop {
                let res = unsafe {
                    ffi::orion_get_response_header(
                        response_handle,
                        name.as_ptr(),
                        name.len() as u32,
                        heap_buf.as_mut_ptr(),
                        heap_buf.len() as u32,
                        &mut written_len as *mut u32,
                    )
                };

                match OrionWasmResult::try_from(res) {
                    Ok(OrionWasmResult::Ok) => {
                        let len = written_len as usize;
                        return Ok(String::from_utf8(heap_buf[..len].to_vec()).ok());
                    }
                    Ok(OrionWasmResult::NotFound) => return Ok(None),
                    Ok(OrionWasmResult::BufferTooSmall) => {
                        let new_len = heap_buf.len().saturating_mul(2);
                        if new_len == heap_buf.len() {
                            return Err(OrionWasmResult::BufferTooSmall);
                        }
                        heap_buf.resize(new_len, 0);
                    }
                    Ok(other) => return Err(other),
                    Err(_) => return Err(OrionWasmResult::InternalError),
                }
            }
        }
        Ok(other) => Err(other),
        Err(_) => Err(OrionWasmResult::InternalError),
    }
}

/// Read the buffered response body.
#[cfg(target_arch = "wasm32")]
fn get_http_response_body(response_handle: u64) -> Result<Vec<u8>, OrionWasmResult> {
    const INITIAL_BUF: usize = 4096;

    let mut buf: Vec<u8> = vec![0u8; INITIAL_BUF];
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::orion_get_response_body(
                response_handle,
                buf.as_mut_ptr(),
                buf.len() as u32,
                &mut written_len as *mut u32,
            )
        };

        match OrionWasmResult::try_from(res) {
            Ok(OrionWasmResult::Ok) => {
                buf.truncate(written_len as usize);
                return Ok(buf);
            }
            Ok(OrionWasmResult::BufferTooSmall) => {
                let new_len = buf.len().saturating_mul(2);
                if new_len == buf.len() {
                    return Err(OrionWasmResult::BufferTooSmall);
                }
                buf.resize(new_len, 0);
            }
            Ok(other) => return Err(other),
            Err(_) => return Err(OrionWasmResult::InternalError),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn get_http_response_body(_response_handle: u64) -> Result<Vec<u8>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

#[cfg(not(target_arch = "wasm32"))]
fn get_http_response_header(_response_handle: u64, _name: &str) -> Result<Option<String>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}


#[cfg(target_arch = "wasm32")]
fn get_http_request_headers_map(request_handle: u64) -> Result<HeaderMap, OrionWasmResult> {
    const INITIAL_BUF: usize = 4096;
    let mut buf: Vec<u8> = vec![0u8; INITIAL_BUF];
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::orion_get_request_headers_map(
                request_handle,
                buf.as_mut_ptr(),
                buf.len() as u32,
                &mut written_len as *mut u32,
            )
        };

        match OrionWasmResult::try_from(res) {
            Ok(OrionWasmResult::Ok) => {
                buf.truncate(written_len as usize);
                return deserialize_header_map(&buf).ok_or(OrionWasmResult::InternalError);
            }
            Ok(OrionWasmResult::BufferTooSmall) => {
                let new_len = buf.len().saturating_mul(2);
                if new_len == buf.len() { return Err(OrionWasmResult::BufferTooSmall); }
                buf.resize(new_len, 0);
            }
            Ok(other) => return Err(other),
            Err(_) => return Err(OrionWasmResult::InternalError),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn get_http_request_headers_map(_request_handle: u64) -> Result<HeaderMap, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

#[cfg(target_arch = "wasm32")]
fn set_http_request_headers_map(request_handle: u64, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
    let serialized = serialize_header_map(headers);
    let res = unsafe { ffi::orion_set_request_headers_map(request_handle, serialized.as_ptr(), serialized.len() as u32) };
    match OrionWasmResult::try_from(res) {
        Ok(OrionWasmResult::Ok) => Ok(()),
        Ok(other) => Err(other),
        Err(_) => Err(OrionWasmResult::InternalError),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn set_http_request_headers_map(_request_handle: u64, _headers: &HeaderMap) -> Result<(), OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

#[cfg(target_arch = "wasm32")]
fn get_http_response_headers_map(response_handle: u64) -> Result<HeaderMap, OrionWasmResult> {
    const INITIAL_BUF: usize = 4096;
    let mut buf: Vec<u8> = vec![0u8; INITIAL_BUF];
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::orion_get_response_headers_map(
                response_handle,
                buf.as_mut_ptr(),
                buf.len() as u32,
                &mut written_len as *mut u32,
            )
        };

        match OrionWasmResult::try_from(res) {
            Ok(OrionWasmResult::Ok) => {
                buf.truncate(written_len as usize);
                return deserialize_header_map(&buf).ok_or(OrionWasmResult::InternalError);
            }
            Ok(OrionWasmResult::BufferTooSmall) => {
                let new_len = buf.len().saturating_mul(2);
                if new_len == buf.len() { return Err(OrionWasmResult::BufferTooSmall); }
                buf.resize(new_len, 0);
            }
            Ok(other) => return Err(other),
            Err(_) => return Err(OrionWasmResult::InternalError),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn get_http_response_headers_map(_response_handle: u64) -> Result<HeaderMap, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

#[cfg(target_arch = "wasm32")]
fn set_http_response_headers_map(response_handle: u64, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
    let serialized = serialize_header_map(headers);
    let res = unsafe { ffi::orion_set_response_headers_map(response_handle, serialized.as_ptr(), serialized.len() as u32) };
    match OrionWasmResult::try_from(res) {
        Ok(OrionWasmResult::Ok) => Ok(()),
        Ok(other) => Err(other),
        Err(_) => Err(OrionWasmResult::InternalError),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn set_http_response_headers_map(_response_handle: u64, _headers: &HeaderMap) -> Result<(), OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

// ============================================================================
// Typestate API

// ============================================================================

/// Marker type representing the headers phase of an HTTP request.
pub struct RequestHeaders;
/// Marker type representing the body phase of an HTTP request.
pub struct RequestBody;
/// Marker type representing the headers phase of an HTTP response.
pub struct ResponseHeaders;
/// Marker type representing the body phase of an HTTP response.
pub struct ResponseBody;

pub trait State {}
impl State for RequestHeaders {}
impl State for RequestBody {}
impl State for ResponseHeaders {}
impl State for ResponseBody {}

/// Typestate wrapper around a request handle.
pub struct RequestHandle<S: State> {
    handle: u64,
    _marker: core::marker::PhantomData<S>,
}

impl<S: State> RequestHandle<S> {
    /// Create a new request handle.
    #[doc(hidden)]
    pub unsafe fn new(handle: u64) -> Self {
        Self {
            handle,
            _marker: core::marker::PhantomData,
        }
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
        Self {
            handle,
            _marker: core::marker::PhantomData,
        }
    }
}

impl RequestHandle<RequestHeaders> {
    /// Read an HTTP request header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<String>, OrionWasmResult> {
        get_http_request_header(self.handle, name)
    }

    /// Send a direct (local) HTTP response, short-circuiting the filter chain.
    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmResult> {
        get_http_request_headers_map(self.handle)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
        set_http_request_headers_map(self.handle, headers)
    }

    pub fn send_direct_response(&self, status_code: u16, body: &[u8]) -> Result<(), OrionWasmResult> {
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

impl RequestHandle<RequestBody> {
    /// Read an HTTP request header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<String>, OrionWasmResult> {
        get_http_request_header(self.handle, name)
    }

    /// Read the buffered request body.
    pub fn get_body(&self) -> Result<Vec<u8>, OrionWasmResult> {
        get_http_request_body(self.handle)
    }

    /// Send a direct (local) HTTP response, short-circuiting the filter chain.
    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmResult> {
        get_http_request_headers_map(self.handle)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
        set_http_request_headers_map(self.handle, headers)
    }

    pub fn send_direct_response(&self, status_code: u16, body: &[u8]) -> Result<(), OrionWasmResult> {
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

impl ResponseHandle<ResponseHeaders> {
    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmResult> {
        get_http_response_headers_map(self.handle)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
        set_http_response_headers_map(self.handle, headers)
    }

    /// Read an HTTP response header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<String>, OrionWasmResult> {
        get_http_response_header(self.handle, name)
    }
}

impl ResponseHandle<ResponseBody> {
    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmResult> {
        get_http_response_headers_map(self.handle)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
        set_http_response_headers_map(self.handle, headers)
    }

    /// Read an HTTP response header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<String>, OrionWasmResult> {
        get_http_response_header(self.handle, name)
    }

    /// Read the buffered response body.
    pub fn get_body(&self) -> Result<Vec<u8>, OrionWasmResult> {
        get_http_response_body(self.handle)
    }
}

// ============================================================================
// High-level `Plugin` trait + `orion_plugin!` macro
// ============================================================================

/// Idiomatic interface implemented by Orion Wasm plugins.
pub trait Plugin {
    /// Invoked once when the Wasm module is instantiated.
    #[inline]
    fn on_plugin_start(&mut self) { }

    /// Invoked when the Wasm module instance is destroyed by the host.
    #[inline]
    fn on_plugin_destroy(&mut self) { }

    /// Invoked at the beginning of a new HTTP request/transaction.
    #[inline]
    fn on_transaction_start(&mut self) { }

    /// Invoked on the request path before the body has been buffered.
    #[inline]
    fn on_request_headers(&mut self, _ctx: &RequestHandle<RequestHeaders>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked after the plugin returned [`FilterAction::PauseAndBufferBody`]
    /// from [`Plugin::on_request_headers`] and the host has buffered the full
    /// request body.
    #[inline]
    fn on_request_body(&mut self, _ctx: &RequestHandle<RequestBody>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked on the response path before the body has been buffered.
    #[inline]
    fn on_response_headers(&mut self, _ctx: &ResponseHandle<ResponseHeaders>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked after the plugin returned [`FilterAction::PauseAndBufferBody`]
    /// from [`Plugin::on_response_headers`] and the host has buffered the full
    /// response body.
    #[inline]
    fn on_response_body(&mut self, _ctx: &ResponseHandle<ResponseBody>) -> FilterAction {
        FilterAction::Continue
    }

    /// Invoked when the request/response has been fully processed and the transaction is complete.
    /// This is a good place to clean up any transaction-specific resources before the plugin is reused.
    #[inline]
    fn on_transaction_complete(&mut self) { }
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

        #[cfg(target_arch = "wasm32")]
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
