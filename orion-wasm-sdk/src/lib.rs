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
mod ffi {
    #[link(wasm_import_module = "env")]
    extern "C" {
        /// Read an HTTP request header by name.
        pub fn orion_get_request_header(
            request_handle: u64,
            name_ptr: *const u8,
            name_len: u32,
            value_ptr: *mut u8,
            value_max_len: u32,
            written_len_ptr: *mut u32,
        ) -> i32;

        /// Read the buffered request body.
        pub fn orion_get_request_body(
            request_handle: u64,
            body_ptr: *mut u8,
            max_len: u32,
            written_len_ptr: *mut u32,
        ) -> i32;

        /// Send a direct (local) HTTP response, short-circuiting the filter chain.
        pub fn orion_send_direct_response(
            request_handle: u64,
            status_code: u32,
            body_ptr: *const u8,
            body_len: u32,
        ) -> i32;

        /// Read an HTTP response header by name.
        pub fn orion_get_response_header(
            response_handle: u64,
            name_ptr: *const u8,
            name_len: u32,
            value_ptr: *mut u8,
            value_max_len: u32,
            written_len_ptr: *mut u32,
        ) -> i32;

        /// Read the buffered response body.
        pub fn orion_get_response_body(
            response_handle: u64,
            body_ptr: *mut u8,
            max_len: u32,
            written_len_ptr: *mut u32,
        ) -> i32;
    }
}

// ============================================================================
// High-level API
// ============================================================================

/// Read an HTTP request header by name.
#[cfg(target_arch = "wasm32")]
pub fn get_http_request_header(request_handle: u64, name: &str) -> Result<Option<String>, OrionWasmResult> {
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
pub fn get_http_request_header(_request_handle: u64, _name: &str) -> Result<Option<String>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

/// Read the buffered request body.
#[cfg(target_arch = "wasm32")]
pub fn get_http_request_body(request_handle: u64) -> Result<Vec<u8>, OrionWasmResult> {
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
pub fn get_http_request_body(_request_handle: u64) -> Result<Vec<u8>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

/// Send a direct (local) HTTP response and short-circuit the filter chain.
#[cfg(target_arch = "wasm32")]
pub fn send_http_direct_response(request_handle: u64, status_code: u16, body: &[u8]) -> Result<(), OrionWasmResult> {
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
pub fn send_http_direct_response(_request_handle: u64, _status_code: u16, _body: &[u8]) -> Result<(), OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

/// Read an HTTP response header by name.
#[cfg(target_arch = "wasm32")]
pub fn get_http_response_header(response_handle: u64, name: &str) -> Result<Option<String>, OrionWasmResult> {
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
pub fn get_http_response_body(response_handle: u64) -> Result<Vec<u8>, OrionWasmResult> {
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
pub fn get_http_response_body(_response_handle: u64) -> Result<Vec<u8>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

#[cfg(not(target_arch = "wasm32"))]
pub fn get_http_response_header(_response_handle: u64, _name: &str) -> Result<Option<String>, OrionWasmResult> {
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
    /// Read an HTTP response header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<String>, OrionWasmResult> {
        get_http_response_header(self.handle, name)
    }
}

impl ResponseHandle<ResponseBody> {
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
}

/// # Example
///
/// ```no_run
/// use orion_wasm_sdk::{Plugin, FilterAction, RequestHandle, RequestHeaders, orion_plugin};
///
/// #[derive(Default)]
/// struct AuthFilter;
///
/// impl Plugin for AuthFilter {
///     fn on_request_headers(&mut self, ctx: &RequestHandle<RequestHeaders>) -> FilterAction {
///         match ctx.get_header("Authorization") {
///             Ok(Some(v)) if v == "Bearer secret-token" => FilterAction::Continue,
///             _ => ctx.direct_response(401, b"unauthorized"),
///         }
///     }
/// }
///
/// orion_plugin!(AuthFilter);
/// ```
#[macro_export]
macro_rules! orion_plugin {
    ($t:ty) => {
        static mut PLUGIN: ::std::option::Option<$t> = ::std::option::Option::None;

        #[no_mangle]
        pub extern "C" fn on_request_headers(request_handle: u64) -> i32 {
            use $crate::{Plugin, RequestHandle, RequestHeaders};
            // SAFETY: Wasm execution within a request context is sequential.
            let plugin = unsafe {
                if PLUGIN.is_none() {
                    PLUGIN = ::std::option::Option::Some(<$t as ::std::default::Default>::default());
                }
                match PLUGIN.as_mut() {
                    ::std::option::Option::Some(p) => p,
                    ::std::option::Option::None => return $crate::FilterAction::Continue.into(),
                }
            };
            let ctx = unsafe { RequestHandle::<RequestHeaders>::new(request_handle) };
            Plugin::on_request_headers(plugin, &ctx).into()
        }

        #[no_mangle]
        pub extern "C" fn on_request_body(request_handle: u64, _body_len: u32) -> i32 {
            use $crate::{Plugin, RequestHandle, RequestBody};
            // SAFETY: Wasm execution within a request context is sequential.
            let plugin = unsafe {
                if PLUGIN.is_none() {
                    PLUGIN = ::std::option::Option::Some(<$t as ::std::default::Default>::default());
                }
                match PLUGIN.as_mut() {
                    ::std::option::Option::Some(p) => p,
                    ::std::option::Option::None => return $crate::FilterAction::Continue.into(),
                }
            };
            let ctx = unsafe { RequestHandle::<RequestBody>::new(request_handle) };
            Plugin::on_request_body(plugin, &ctx).into()
        }

        #[no_mangle]
        pub extern "C" fn on_response_headers(response_handle: u64) -> i32 {
            use $crate::{Plugin, ResponseHandle, ResponseHeaders};
            // SAFETY: Wasm execution within a request context is sequential.
            let plugin = unsafe {
                if PLUGIN.is_none() {
                    PLUGIN = ::std::option::Option::Some(<$t as ::std::default::Default>::default());
                }
                match PLUGIN.as_mut() {
                    ::std::option::Option::Some(p) => p,
                    ::std::option::Option::None => return $crate::FilterAction::Continue.into(),
                }
            };
            let ctx = unsafe { ResponseHandle::<ResponseHeaders>::new(response_handle) };
            Plugin::on_response_headers(plugin, &ctx).into()
        }

        #[no_mangle]
        pub extern "C" fn on_response_body(response_handle: u64, _body_len: u32) -> i32 {
            use $crate::{Plugin, ResponseHandle, ResponseBody};
            // SAFETY: Wasm execution within a request context is sequential.
            let plugin = unsafe {
                if PLUGIN.is_none() {
                    PLUGIN = ::std::option::Option::Some(<$t as ::std::default::Default>::default());
                }
                match PLUGIN.as_mut() {
                    ::std::option::Option::Some(p) => p,
                    ::std::option::Option::None => return $crate::FilterAction::Continue.into(),
                }
            };
            let ctx = unsafe { ResponseHandle::<ResponseBody>::new(response_handle) };
            Plugin::on_response_body(plugin, &ctx).into()
        }
    };
}
