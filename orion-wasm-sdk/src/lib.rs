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

pub use http::{HeaderMap, header::HeaderName, header::HeaderValue};

#[cfg(target_arch = "wasm32")]
fn serialize_header_map(headers: &HeaderMap) -> Vec<u8> {
    let mut capacity = 4;
    for (key, val) in headers.iter() {
        capacity += 8 + key.as_str().len() + val.len();
    }

    let mut buf = Vec::with_capacity(capacity);
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

const DEFAULT_HEADER_STACK_BUF_SIZE: usize = 1024;
const DEFAULT_HEAP_BUF_SIZE: usize = 4096;


/// Read an HTTP header by name.
#[cfg(target_arch = "wasm32")]
fn get_http_header(handle: u64, target: ffi::HeaderTarget, name: &str) -> Result<Option<HeaderValue>, OrionWasmResult> {
    let mut stack_buf = [0u8; DEFAULT_HEADER_STACK_BUF_SIZE];
    let mut written_len: u32 = 0;

    let res = unsafe {
        ffi::orion_get_header(
            handle,
            target as u32,
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
            Ok(HeaderValue::from_bytes(&stack_buf[..len]).ok())
        }
        Ok(OrionWasmResult::NotFound) => Ok(None),
        Ok(OrionWasmResult::BufferTooSmall) => {
            let mut heap_buf: Vec<u8> = Vec::with_capacity(DEFAULT_HEAP_BUF_SIZE);
            let mut written_len: u32 = 0;

            loop {
                let res = unsafe {
                    ffi::orion_get_header(
                        handle,
                        target as u32,
                        name.as_ptr(),
                        name.len() as u32,
                        heap_buf.as_mut_ptr(),
                        heap_buf.capacity() as u32,
                        &mut written_len as *mut u32,
                    )
                };

                match OrionWasmResult::try_from(res) {
                    Ok(OrionWasmResult::Ok) => {
                        unsafe { heap_buf.set_len(written_len as usize); }
                        return Ok(HeaderValue::from_bytes(&heap_buf).ok());
                    }
                    Ok(OrionWasmResult::NotFound) => return Ok(None),
                    Ok(OrionWasmResult::BufferTooSmall) => {
                        let new_cap = heap_buf.capacity().saturating_mul(2);
                        if new_cap == heap_buf.capacity() {
                            return Err(OrionWasmResult::BufferTooSmall);
                        }
                        heap_buf.reserve_exact(new_cap);
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
fn get_http_header(_handle: u64, _target: ffi::HeaderTarget, _name: &str) -> Result<Option<HeaderValue>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

/// Read the buffered body.
#[cfg(target_arch = "wasm32")]
fn get_http_body(handle: u64, target: ffi::HeaderTarget) -> Result<Vec<u8>, OrionWasmResult> {
    let mut buf: Vec<u8> = Vec::with_capacity(DEFAULT_HEAP_BUF_SIZE);
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::orion_get_body(
                handle,
                target as u32,
                buf.as_mut_ptr(),
                buf.capacity() as u32,
                &mut written_len as *mut u32,
            )
        };

        match OrionWasmResult::try_from(res) {
            Ok(OrionWasmResult::Ok) => {
                unsafe { buf.set_len(written_len as usize); }
                return Ok(buf);
            }
            Ok(OrionWasmResult::BufferTooSmall) => {
                let new_cap = buf.capacity().saturating_mul(2);
                if new_cap == buf.capacity() {
                    return Err(OrionWasmResult::BufferTooSmall);
                }
                buf.reserve_exact(new_cap);
            }
            Ok(other) => return Err(other),
            Err(_) => return Err(OrionWasmResult::InternalError),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn get_http_body(_handle: u64, _target: ffi::HeaderTarget) -> Result<Vec<u8>, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

/// Replace the buffered body.
#[cfg(target_arch = "wasm32")]
fn set_http_body(handle: u64, target: ffi::HeaderTarget, body: &[u8]) -> Result<(), OrionWasmResult> {
    let res = unsafe {
        ffi::orion_set_body(
            handle,
            target as u32,
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
fn set_http_body(_handle: u64, _target: ffi::HeaderTarget, _body: &[u8]) -> Result<(), OrionWasmResult> {
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

#[cfg(target_arch = "wasm32")]
fn get_http_headers_map(handle: u64, target: ffi::HeaderTarget) -> Result<HeaderMap, OrionWasmResult> {
    let mut buf: Vec<u8> = Vec::with_capacity(DEFAULT_HEAP_BUF_SIZE);
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::orion_get_headers_map(
                handle,
                target as u32,
                buf.as_mut_ptr(),
                buf.capacity() as u32,
                &mut written_len as *mut u32,
            )
        };

        match OrionWasmResult::try_from(res) {
            Ok(OrionWasmResult::Ok) => {
                unsafe { buf.set_len(written_len as usize); }
                return deserialize_header_map(&buf).ok_or(OrionWasmResult::InternalError);
            }
            Ok(OrionWasmResult::BufferTooSmall) => {
                let new_cap = buf.capacity().saturating_mul(2);
                if new_cap == buf.capacity() { return Err(OrionWasmResult::BufferTooSmall); }
                buf.reserve_exact(new_cap);
            }
            Ok(other) => return Err(other),
            Err(_) => return Err(OrionWasmResult::InternalError),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn get_http_headers_map(_handle: u64, _target: ffi::HeaderTarget) -> Result<HeaderMap, OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

#[cfg(target_arch = "wasm32")]
fn set_http_headers_map(handle: u64, target: ffi::HeaderTarget, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
    let serialized = serialize_header_map(headers);
    let res = unsafe { ffi::orion_set_headers_map(handle, target as u32, serialized.as_ptr(), serialized.len() as u32) };
    match OrionWasmResult::try_from(res) {
        Ok(OrionWasmResult::Ok) => Ok(()),
        Ok(other) => Err(other),
        Err(_) => Err(OrionWasmResult::InternalError),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn set_http_headers_map(_handle: u64, _target: ffi::HeaderTarget, _headers: &HeaderMap) -> Result<(), OrionWasmResult> {
    Err(OrionWasmResult::InternalError)
}

#[cfg(target_arch = "wasm32")]
fn set_http_header(handle: u64, target: ffi::HeaderTarget, name: &HeaderName, value: &HeaderValue) -> Result<(), OrionWasmResult> {
    let name_bytes = name.as_str().as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe { ffi::orion_set_header(handle, target as u32, name_bytes.as_ptr(), name_bytes.len() as u32, value_bytes.as_ptr(), value_bytes.len() as u32) };
    match OrionWasmResult::try_from(res) { Ok(OrionWasmResult::Ok) => Ok(()), Ok(other) => Err(other), Err(_) => Err(OrionWasmResult::InternalError) }
}
#[cfg(not(target_arch = "wasm32"))]
fn set_http_header(_handle: u64, _target: ffi::HeaderTarget, _name: &HeaderName, _value: &HeaderValue) -> Result<(), OrionWasmResult> { Err(OrionWasmResult::InternalError) }

#[cfg(target_arch = "wasm32")]
fn add_http_header(handle: u64, target: ffi::HeaderTarget, name: &HeaderName, value: &HeaderValue) -> Result<(), OrionWasmResult> {
    let name_bytes = name.as_str().as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe { ffi::orion_add_header(handle, target as u32, name_bytes.as_ptr(), name_bytes.len() as u32, value_bytes.as_ptr(), value_bytes.len() as u32) };
    match OrionWasmResult::try_from(res) { Ok(OrionWasmResult::Ok) => Ok(()), Ok(other) => Err(other), Err(_) => Err(OrionWasmResult::InternalError) }
}
#[cfg(not(target_arch = "wasm32"))]
fn add_http_header(_handle: u64, _target: ffi::HeaderTarget, _name: &HeaderName, _value: &HeaderValue) -> Result<(), OrionWasmResult> { Err(OrionWasmResult::InternalError) }

#[cfg(target_arch = "wasm32")]
fn remove_http_header(handle: u64, target: ffi::HeaderTarget, name: &HeaderName) -> Result<(), OrionWasmResult> {
    let name_bytes = name.as_str().as_bytes();
    let res = unsafe { ffi::orion_remove_header(handle, target as u32, name_bytes.as_ptr(), name_bytes.len() as u32) };
    match OrionWasmResult::try_from(res) { Ok(OrionWasmResult::Ok) => Ok(()), Ok(other) => Err(other), Err(_) => Err(OrionWasmResult::InternalError) }
}
#[cfg(not(target_arch = "wasm32"))]
fn remove_http_header(_handle: u64, _target: ffi::HeaderTarget, _name: &HeaderName) -> Result<(), OrionWasmResult> { Err(OrionWasmResult::InternalError) }

#[cfg(target_arch = "wasm32")]
fn replace_http_header(handle: u64, target: ffi::HeaderTarget, name: &HeaderName, value: &HeaderValue) -> Result<(), OrionWasmResult> {
    let name_bytes = name.as_str().as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe { ffi::orion_replace_header(handle, target as u32, name_bytes.as_ptr(), name_bytes.len() as u32, value_bytes.as_ptr(), value_bytes.len() as u32) };
    match OrionWasmResult::try_from(res) { Ok(OrionWasmResult::Ok) => Ok(()), Ok(other) => Err(other), Err(_) => Err(OrionWasmResult::InternalError) }
}
#[cfg(not(target_arch = "wasm32"))]
fn replace_http_header(_handle: u64, _target: ffi::HeaderTarget, _name: &HeaderName, _value: &HeaderValue) -> Result<(), OrionWasmResult> { Err(OrionWasmResult::InternalError) }


pub enum HeaderMutation {
    Set(HeaderName, HeaderValue),
    Add(HeaderName, HeaderValue),
    Replace(HeaderName, HeaderValue),
    Remove(HeaderName),
}

#[cfg(target_arch = "wasm32")]
fn serialize_header_mutations(mutations: &[HeaderMutation]) -> Vec<u8> {
    let mut capacity = 4;
    for mutation in mutations {
        capacity += 1;
        match mutation {
            HeaderMutation::Set(name, value)
            | HeaderMutation::Add(name, value)
            | HeaderMutation::Replace(name, value) => {
                capacity += 8 + name.as_str().len() + value.len();
            }
            HeaderMutation::Remove(name) => {
                capacity += 4 + name.as_str().len();
            }
        }
    }

    let mut buf = Vec::with_capacity(capacity);
    let num_mutations = mutations.len() as u32;
    buf.extend_from_slice(&num_mutations.to_le_bytes());

    for mutation in mutations {
        match mutation {
            HeaderMutation::Set(name, value) => {
                buf.push(0);
                write_key_val(&mut buf, name, value);
            }
            HeaderMutation::Add(name, value) => {
                buf.push(1);
                write_key_val(&mut buf, name, value);
            }
            HeaderMutation::Replace(name, value) => {
                buf.push(2);
                write_key_val(&mut buf, name, value);
            }
            HeaderMutation::Remove(name) => {
                buf.push(3);
                let name_bytes = name.as_str().as_bytes();
                buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(name_bytes);
            }
        }
    }
    buf
}

#[inline]
#[cfg(target_arch = "wasm32")]
fn write_key_val(buf: &mut Vec<u8>, name: &HeaderName, value: &HeaderValue) {
    let name_bytes = name.as_str().as_bytes();
    let value_bytes = value.as_bytes();
    buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(value_bytes);
}

#[cfg(target_arch = "wasm32")]
fn apply_header_mutations(handle: u64, target: ffi::HeaderTarget, mutations: &[HeaderMutation]) -> Result<(), OrionWasmResult> {
    let serialized = serialize_header_mutations(mutations);
    let res = unsafe { ffi::orion_apply_header_mutations(handle, target as u32, serialized.as_ptr(), serialized.len() as u32) };
    match OrionWasmResult::try_from(res) {
        Ok(OrionWasmResult::Ok) => Ok(()),
        Ok(other) => Err(other),
        Err(_) => Err(OrionWasmResult::InternalError),
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn apply_header_mutations(_handle: u64, _target: ffi::HeaderTarget, _mutations: &[HeaderMutation]) -> Result<(), OrionWasmResult> {
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
    pub fn get_header(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmResult> {
        get_http_header(self.handle, ffi::HeaderTarget::Request, name)
    }

    pub fn set_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        set_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    pub fn add_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        add_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    pub fn remove_header(&self, name: &HeaderName) -> Result<(), OrionWasmResult> {
        remove_http_header(self.handle, ffi::HeaderTarget::Request, name)
    }

    pub fn replace_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        replace_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmResult> {
        get_http_headers_map(self.handle, ffi::HeaderTarget::Request)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::Request, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmResult> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::Request, mutations)
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
    pub fn get_header(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmResult> {
        get_http_header(self.handle, ffi::HeaderTarget::Request, name)
    }

    pub fn set_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        set_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    pub fn add_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        add_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    pub fn remove_header(&self, name: &HeaderName) -> Result<(), OrionWasmResult> {
        remove_http_header(self.handle, ffi::HeaderTarget::Request, name)
    }

    pub fn replace_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        replace_http_header(self.handle, ffi::HeaderTarget::Request, &name, &value)
    }

    /// Read the buffered request body.
    pub fn get_body(&self) -> Result<Vec<u8>, OrionWasmResult> {
        get_http_body(self.handle, ffi::HeaderTarget::Request)
    }

    /// Replace the buffered request body.
    pub fn set_body(&self, body: &[u8]) -> Result<(), OrionWasmResult> {
        set_http_body(self.handle, ffi::HeaderTarget::Request, body)
    }

    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmResult> {
        get_http_headers_map(self.handle, ffi::HeaderTarget::Request)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::Request, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmResult> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::Request, mutations)
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
        get_http_headers_map(self.handle, ffi::HeaderTarget::Response)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::Response, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmResult> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::Response, mutations)
    }

    /// Read an HTTP response header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmResult> {
        get_http_header(self.handle, ffi::HeaderTarget::Response, name)
    }

    pub fn set_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        set_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    pub fn add_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        add_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    pub fn remove_header(&self, name: &HeaderName) -> Result<(), OrionWasmResult> {
        remove_http_header(self.handle, ffi::HeaderTarget::Response, name)
    }

    pub fn replace_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        replace_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }
}

impl ResponseHandle<ResponseBody> {
    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmResult> {
        get_http_headers_map(self.handle, ffi::HeaderTarget::Response)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmResult> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::Response, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmResult> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::Response, mutations)
    }

    /// Read an HTTP response header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmResult> {
        get_http_header(self.handle, ffi::HeaderTarget::Response, name)
    }

    pub fn set_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        set_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    pub fn add_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        add_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    pub fn remove_header(&self, name: &HeaderName) -> Result<(), OrionWasmResult> {
        remove_http_header(self.handle, ffi::HeaderTarget::Response, name)
    }

    pub fn replace_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmResult> {
        replace_http_header(self.handle, ffi::HeaderTarget::Response, &name, &value)
    }

    /// Read the buffered response body.
    pub fn get_body(&self) -> Result<Vec<u8>, OrionWasmResult> {
        get_http_body(self.handle, ffi::HeaderTarget::Response)
    }

    /// Replace the buffered response body.
    pub fn set_body(&self, body: &[u8]) -> Result<(), OrionWasmResult> {
        set_http_body(self.handle, ffi::HeaderTarget::Response, body)
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
