use crate::ffi;
use crate::internal::DEFAULT_HEAP_BUF_SIZE;
use orion_wasm_types::{CalloutRequest, CalloutResponse, GrpcCalloutRequest, GrpcCalloutResponse, OrionWasmError, OrionWasmResult};

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

/// Dispatches an asynchronous gRPC call using the host's cluster manager.
pub fn dispatch_grpc_call(request: &GrpcCalloutRequest) -> Result<GrpcCalloutResponse, OrionWasmError> {
    let req_bytes = match bincode_next::serde::encode_to_vec(request, bincode_next::config::standard()) {
        Ok(b) => b,
        Err(_) => return Err(OrionWasmError::InternalError),
    };

    let mut resp_ptr: *mut u8 = std::ptr::null_mut();
    let mut resp_len = 0u32;

    let res = unsafe {
        ffi::orion_dispatch_grpc_call(
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

/// Sets an absolute IO timeout for all subsequent IO operations in the current context.
///
/// If any subsequent blocking IO operation (such as `dispatch_http_call`) does not complete
/// before the timeout expires, it will return `OrionWasmError::Timeout`.
pub fn set_io_timeout(duration: std::time::Duration) -> Result<(), OrionWasmError> {
    let microseconds = duration.as_micros().try_into().unwrap_or(u64::MAX);
    let res = unsafe { ffi::orion_set_io_timeout(microseconds) };
    OrionWasmResult::from_ffi(res)
}

/// Disarms the current IO timeout and returns the remaining time.
///
/// Returns `Ok(Duration::ZERO)` if no timeout was set, or if the timeout had already expired.
pub fn clear_io_timeout() -> Result<std::time::Duration, OrionWasmError> {
    let mut remaining_us = 0u64;
    let res = unsafe { ffi::orion_clear_io_timeout(&mut remaining_us as *mut u64) };
    OrionWasmResult::from_ffi(res)?;
    Ok(std::time::Duration::from_micros(remaining_us))
}

/// Suspends the execution of the WebAssembly module for the specified duration.
///
/// Thanks to Orion's asynchronous Wasm engine, this does not block the proxy server.
/// It only suspends the current Wasm execution.
pub fn sleep(duration: std::time::Duration) -> Result<(), OrionWasmError> {
    let microseconds = duration.as_micros().try_into().unwrap_or(u64::MAX);
    let res = unsafe { ffi::orion_sleep(microseconds) };
    OrionWasmResult::from_ffi(res)
}
