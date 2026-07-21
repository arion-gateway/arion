use crate::ffi;
use crate::internal::*;
use crate::typestate::{HttpBody, State};
use http::{header::{HeaderName, HeaderValue}, HeaderMap};
use orion_wasm_types::{FilterAction, HeaderMutation, OrionWasmError, OrionWasmResult};

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

    /// Retrieve the downstream metadata for this connection.
    pub fn get_downstream_metadata(&self) -> Result<Option<orion_wasm_types::DownstreamMetadata>, OrionWasmError> {
        let mut resp_ptr: *mut u8 = std::ptr::null_mut();
        let mut resp_len = 0u32;

        let res = unsafe {
            ffi::orion_get_downstream_metadata(
                self.handle,
                &mut resp_ptr as *mut *mut u8,
                &mut resp_len as *mut u32,
            )
        };

        if res == 0 {
            if resp_ptr.is_null() {
                return Ok(None);
            }
            let resp_buf = unsafe { Vec::from_raw_parts(resp_ptr, resp_len as usize, resp_len as usize) };
            match bincode_next::serde::decode_from_slice(&resp_buf, bincode_next::config::standard()) {
                Ok((meta, _)) => Ok(Some(meta)),
                Err(_) => Err(OrionWasmError::InternalError),
            }
        } else if res == 1 { // NotFound
            Ok(None)
        } else {
            Err(OrionWasmResult::from_ffi(res).unwrap_err())
        }
    }

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
    /// Read the buffered request body.
    pub fn get_body(&self) -> Result<Vec<u8>, OrionWasmError> {
        get_http_body(self.handle, ffi::HeaderTarget::Request)
    }

    /// Replace the buffered request body.
    pub fn set_body(&self, body: &[u8]) -> Result<(), OrionWasmError> {
        set_http_body(self.handle, ffi::HeaderTarget::Request, body)
    }

    pub fn get_trailers_map(&self) -> Result<HeaderMap, OrionWasmError> {
        get_http_headers_map(self.handle, ffi::HeaderTarget::RequestTrailers)
    }

    pub fn set_trailers_map(&self, trailers: &HeaderMap) -> Result<(), OrionWasmError> {
        set_http_headers_map(self.handle, ffi::HeaderTarget::RequestTrailers, trailers)
    }

    pub fn get_trailer(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmError> {
        get_http_header(self.handle, ffi::HeaderTarget::RequestTrailers, name)
    }

    pub fn set_trailer(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        set_http_header(self.handle, ffi::HeaderTarget::RequestTrailers, &name, &value)
    }

    pub fn add_trailer(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        add_http_header(self.handle, ffi::HeaderTarget::RequestTrailers, &name, &value)
    }

    pub fn remove_trailer(&self, name: &HeaderName) -> Result<(), OrionWasmError> {
        remove_http_header(self.handle, ffi::HeaderTarget::RequestTrailers, name)
    }

    pub fn replace_trailer(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        replace_http_header(self.handle, ffi::HeaderTarget::RequestTrailers, &name, &value)
    }

    pub fn apply_trailer_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmError> {
        apply_header_mutations(self.handle, ffi::HeaderTarget::RequestTrailers, mutations)
    }
}
