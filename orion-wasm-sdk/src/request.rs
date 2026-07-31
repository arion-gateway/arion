use crate::ffi;
use crate::internal::*;
use crate::typestate::{HttpBody, State};
use http::{header::{HeaderName, HeaderValue}, HeaderMap};
use orion_wasm_types::{FilterAction, HeaderMutation, OrionWasmError};

/// Typestate wrapper around a request handle.
pub struct RequestHandle<S: State> {
    
    _marker: core::marker::PhantomData<S>,
}

impl<S: State> RequestHandle<S> {
    /// Create a new request handle.
    #[doc(hidden)]
    pub fn new() -> Self {
        Self { _marker: core::marker::PhantomData }
    }

    /// Retrieve the downstream metadata for this connection.
    pub fn get_downstream_metadata(&self) -> Result<Option<orion_wasm_types::DownstreamMetadata>, OrionWasmError> {
        let mut resp_ptr: *mut u8 = std::ptr::null_mut();
        let mut resp_len = 0u32;

        let res = unsafe {
            ffi::orion_get_downstream_metadata(
                
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
            Err(OrionWasmError::from_ffi(res).unwrap_err())
        }
    }

    /// Retrieve the HTTP URI for the current request.
    pub fn get_uri(&self) -> Result<http::Uri, OrionWasmError> {
        get_http_uri()
    }

    /// Set the HTTP URI for the current request.
    pub fn set_uri(&self, uri: &http::Uri) -> Result<(), OrionWasmError> {
        set_http_uri(uri)
    }

    /// Read an HTTP request header by name.
    pub fn get_header(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmError> {
        get_http_header( 0, name)
    }

    pub fn set_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        set_http_header( 0, &name, &value)
    }

    pub fn add_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        add_http_header( 0, &name, &value)
    }

    pub fn remove_header(&self, name: &HeaderName) -> Result<(), OrionWasmError> {
        remove_http_header( 0, name)
    }

    pub fn replace_header(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        replace_http_header( 0, &name, &value)
    }

    pub fn get_headers_map(&self) -> Result<HeaderMap, OrionWasmError> {
        get_http_headers_map( 0)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), OrionWasmError> {
        set_http_headers_map( 0, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmError> {
        apply_header_mutations( 0, mutations)
    }

    /// Schedule a direct HTTP response payload in host memory for the current transaction.
    ///
    /// Calling this method stages the direct response on the host. The response will be sent to the client
    /// when the plugin hook returns [`FilterAction::DirectResponse`].
    pub fn schedule_direct_response(&self, response: http::Response<bytes::Bytes>) -> Result<(), OrionWasmError> {
        send_http_direct_response(response)
    }

    /// Convenience wrapper around `schedule_direct_response`.
    pub fn direct_response(&self, response: http::Response<bytes::Bytes>) -> FilterAction {
        match self.schedule_direct_response(response) {
            Ok(()) => FilterAction::DirectResponse,
            Err(_) => FilterAction::Continue,
        }
    }
}

impl RequestHandle<HttpBody> {
    /// Read the buffered request body.
    pub fn get_body(&self) -> Result<bytes::Bytes, OrionWasmError> {
        get_http_body( 0)
    }

    /// Replace the buffered request body.
    pub fn set_body(&self, body: &[u8]) -> Result<(), OrionWasmError> {
        set_http_body( 0, body)
    }

    /// Materialize the request (headers and body) into a standard http::Request<Bytes>.
    pub fn take_request(&self) -> Result<http::Request<bytes::Bytes>, OrionWasmError> {
        let mut buf = vec![0u8; 1024 * 64];
        let mut written_len: u32 = 0;
        
        loop {
            let res = unsafe {
                ffi::orion_get_request(buf.as_mut_ptr(), buf.len() as u32, &mut written_len as *mut u32)
            };
            
            if res == 0 {
                buf.truncate(written_len as usize);
                let wasm_req = match bincode_next::serde::decode_from_slice::<orion_wasm_types::WasmRequest, _>(&buf, bincode_next::config::standard()) {
                    Ok((w, _)) => w,
                    Err(_) => return Err(OrionWasmError::InternalError),
                };
                return Ok(wasm_req.request);
            } else if res == OrionWasmError::BufferTooSmall as i32 {
                if buf.len() > 10 * 1024 * 1024 {
                    return Err(OrionWasmError::BufferTooSmall);
                }
                buf.resize(buf.len() * 2, 0);
            } else {
                return Err(OrionWasmError::from_ffi(res).err().unwrap_or(OrionWasmError::InternalError));
            }
        }
    }

    /// Replace the entire request (headers, URI, method, and body) from a given http::Request<Bytes>.
    pub fn replace_request(&self, req: &http::Request<bytes::Bytes>) -> Result<(), OrionWasmError> {
        let wasm_req = orion_wasm_types::WasmRequest { request: req.clone() };
        let serialized = match bincode_next::serde::encode_to_vec(&wasm_req, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(_) => return Err(OrionWasmError::InternalError),
        };
        
        let res = unsafe { ffi::orion_set_request(serialized.as_ptr(), serialized.len() as u32) };
        if res == 0 {
            Ok(())
        } else {
            Err(OrionWasmError::from_ffi(res).err().unwrap_or(OrionWasmError::InternalError))
        }
    }

    pub fn get_trailers_map(&self) -> Result<HeaderMap, OrionWasmError> {
        get_http_headers_map( 1)
    }

    pub fn set_trailers_map(&self, trailers: &HeaderMap) -> Result<(), OrionWasmError> {
        set_http_headers_map( 1, trailers)
    }

    pub fn get_trailer(&self, name: &str) -> Result<Option<HeaderValue>, OrionWasmError> {
        get_http_header( 1, name)
    }

    pub fn set_trailer(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        set_http_header( 1, &name, &value)
    }

    pub fn add_trailer(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        add_http_header( 1, &name, &value)
    }

    pub fn remove_trailer(&self, name: &HeaderName) -> Result<(), OrionWasmError> {
        remove_http_header( 1, name)
    }

    pub fn replace_trailer(&self, name: HeaderName, value: HeaderValue) -> Result<(), OrionWasmError> {
        replace_http_header( 1, &name, &value)
    }

    pub fn apply_trailer_mutations(&self, mutations: &[HeaderMutation]) -> Result<(), OrionWasmError> {
        apply_header_mutations( 1, mutations)
    }
}
