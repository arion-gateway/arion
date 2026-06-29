//! Orion WebAssembly SDK (Guest side)
//!
//! This crate provides a high-level Rust API for writing WebAssembly plugins
//! that run inside the Orion proxy via the Wasmtime runtime.
//!
//! Plugins export functions like `on_request_headers` and `on_request_body`,
//! and use the hostcalls defined here to interact with the host (Orion).
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
        ///
        /// - `name_ptr` / `name_len`: pointer to the header name in Wasm memory.
        /// - `value_ptr` / `value_max_len`: caller-allocated buffer for the value.
        /// - `written_len_ptr`: pointer to a `u32` where the actual written length
        ///   will be stored.
        ///
        /// Returns an [`OrionWasmResult`] as `i32`.
        pub fn orion_get_request_header(
            name_ptr: *const u8,
            name_len: u32,
            value_ptr: *mut u8,
            value_max_len: u32,
            written_len_ptr: *mut u32,
        ) -> i32;

        /// Read the buffered request body.
        ///
        /// Only available after the plugin returned [`FilterAction::PauseAndBufferBody`]
        /// from `on_request_headers`, causing the host to collect the body and call
        /// `on_request_body`.
        ///
        /// - `body_ptr` / `max_len`: caller-allocated buffer for the body bytes.
        /// - `written_len_ptr`: pointer to a `u32` where the actual written length
        ///   will be stored.
        ///
        /// Returns an [`OrionWasmResult`] as `i32`.
        pub fn orion_get_request_body(
            body_ptr: *mut u8,
            max_len: u32,
            written_len_ptr: *mut u32,
        ) -> i32;

        /// Send a direct (local) HTTP response, short-circuiting the filter chain.
        ///
        /// - `status_code`: HTTP status code (e.g. 403).
        /// - `body_ptr` / `body_len`: pointer to the response body in Wasm memory.
        ///
        /// Returns an [`OrionWasmResult`] as `i32`.
        pub fn orion_send_direct_response(
            status_code: u32,
            body_ptr: *const u8,
            body_len: u32,
        ) -> i32;
    }
}

// ============================================================================
// High-level API
// ============================================================================

/// Read an HTTP request header by name.
///
/// Returns `Ok(Some(value))` if the header exists, `Ok(None)` if it was not
/// found, or `Err(OrionWasmResult)` if a host-level error occurred.
#[cfg(target_arch = "wasm32")]
pub fn get_request_header(name: &str) -> Result<Option<String>, OrionWasmResult> {
    const INITIAL_BUF: usize = 1024;

    // Try with a stack buffer first to avoid allocation in the common case.
    let mut stack_buf = [0u8; INITIAL_BUF];
    let mut written_len: u32 = 0;

    let res = unsafe {
        ffi::orion_get_request_header(
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
            Ok(Some(String::from_utf8(stack_buf[..len].to_vec()).ok()))
        }
        Ok(OrionWasmResult::NotFound) => Ok(None),
        Ok(OrionWasmResult::BufferTooSmall) => {
            // The value didn't fit in the stack buffer — retry with a heap
            // allocation sized to a reasonable upper bound.  We don't know the
            // exact required size from the host, so we grow exponentially.
            let mut heap_buf: Vec<u8> = vec![0u8; INITIAL_BUF * 4];
            let mut written_len: u32 = 0;

            loop {
                let res = unsafe {
                    ffi::orion_get_request_header(
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
                        return Ok(String::from_utf8(heap_buf[..len].to_vec()).ok().map(Some).unwrap_or(None));
                    }
                    Ok(OrionWasmResult::NotFound) => return Ok(None),
                    Ok(OrionWasmResult::BufferTooSmall) => {
                        // Double the buffer and retry.
                        let new_len = heap_buf.len().saturating_mul(2);
                        if new_len == heap_buf.len() {
                            // Can't grow further.
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
pub fn get_request_header(_name: &str) -> Result<Option<String>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

/// Read the buffered request body.
///
/// This is only valid inside `on_request_body` (i.e. after the plugin
/// returned [`FilterAction::PauseAndBufferBody`] from `on_request_headers`).
///
/// Returns the body as `Vec<u8>`, or an [`OrionWasmResult`] error.
#[cfg(target_arch = "wasm32")]
pub fn get_request_body() -> Result<Vec<u8>, OrionWasmResult> {
    const INITIAL_BUF: usize = 4096;

    let mut buf: Vec<u8> = vec![0u8; INITIAL_BUF];
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::orion_get_request_body(
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
                // Grow and retry.  We don't have an exact size hint from the
                // host, so we double until it fits.
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
pub fn get_request_body() -> Result<Vec<u8>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

/// Send a direct (local) HTTP response and short-circuit the filter chain.
///
/// After calling this, the plugin should return [`FilterAction::DirectResponse`]
/// from its entry point (`on_request_headers` or `on_request_body`).
#[cfg(target_arch = "wasm32")]
pub fn send_direct_response(status_code: u16, body: &[u8]) -> Result<(), OrionWasmResult> {
    let res = unsafe {
        ffi::orion_send_direct_response(
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
pub fn send_direct_response(_status_code: u16, _body: &[u8]) -> Result<(), OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}
