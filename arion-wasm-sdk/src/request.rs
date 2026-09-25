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

use crate::ffi;
use crate::internal::*;
use crate::typestate::{HttpBody, State};
use arion_wasm_types::WasmUri;
use arion_wasm_types::{ArionWasmError, FilterAction, HeaderMutation};
use arion_wasm_types::{WasmHeaderName, WasmHeaderValue};
use http::HeaderMap;

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
    pub fn get_downstream_metadata(&self) -> Result<Option<arion_wasm_types::DownstreamMetadata>, ArionWasmError> {
        let mut resp_ptr: *mut u8 = std::ptr::null_mut();
        let mut resp_len = 0u32;

        let res =
            unsafe { ffi::arion_get_downstream_metadata(&mut resp_ptr as *mut *mut u8, &mut resp_len as *mut u32) };

        if res == 0 {
            if resp_ptr.is_null() {
                return Ok(None);
            }
            let resp_buf = unsafe { Vec::from_raw_parts(resp_ptr, resp_len as usize, resp_len as usize) };
            match bincode_next::serde::decode_from_slice(&resp_buf, bincode_next::config::standard()) {
                Ok((meta, _)) => Ok(Some(meta)),
                Err(_) => Err(ArionWasmError::InternalError),
            }
        } else if res == 1 {
            // NotFound
            Ok(None)
        } else {
            Err(ArionWasmError::from_ffi(res).unwrap_err())
        }
    }

    /// Retrieve the HTTP URI for the current request.
    pub fn get_uri(&self) -> Result<WasmUri<'static>, ArionWasmError> {
        get_http_uri()
    }

    /// Set the HTTP URI for the current request.
    pub fn set_uri<'a, U>(&self, uri: U) -> Result<(), ArionWasmError>
    where
        U: Into<WasmUri<'a>>,
    {
        set_http_uri(&uri.into())
    }

    /// Read an HTTP request header by name.
    pub fn get_header<'a, N>(&self, name: N) -> Result<Option<WasmHeaderValue<'static>>, ArionWasmError>
    where
        N: Into<WasmHeaderName<'a>>,
    {
        get_http_header(0, &name.into())
    }

    pub fn set_header<'a, N, V>(&self, name: N, value: V) -> Result<(), ArionWasmError>
    where
        N: Into<WasmHeaderName<'a>>,
        V: Into<WasmHeaderValue<'a>>,
    {
        set_http_header(0, &name.into(), &value.into())
    }

    pub fn add_header<'a, N, V>(&self, name: N, value: V) -> Result<(), ArionWasmError>
    where
        N: Into<WasmHeaderName<'a>>,
        V: Into<WasmHeaderValue<'a>>,
    {
        add_http_header(0, &name.into(), &value.into())
    }

    pub fn remove_header<'a, N>(&self, name: N) -> Result<(), ArionWasmError>
    where
        N: Into<WasmHeaderName<'a>>,
    {
        remove_http_header(0, &name.into())
    }

    pub fn replace_header<'a, N, V>(&self, name: N, value: V) -> Result<(), ArionWasmError>
    where
        N: Into<WasmHeaderName<'a>>,
        V: Into<WasmHeaderValue<'a>>,
    {
        replace_http_header(0, &name.into(), &value.into())
    }

    pub fn get_headers_map(&self) -> Result<HeaderMap, ArionWasmError> {
        get_http_headers_map(0)
    }

    pub fn set_headers_map(&self, headers: &HeaderMap) -> Result<(), ArionWasmError> {
        set_http_headers_map(0, headers)
    }

    pub fn apply_header_mutations(&self, mutations: &[HeaderMutation<'_>]) -> Result<(), ArionWasmError> {
        apply_header_mutations(0, mutations)
    }

    /// Schedule a direct HTTP response payload in host memory for the current transaction.
    ///
    /// Calling this method stages the direct response on the host. The response will be sent to the client
    /// when the plugin hook returns [`FilterAction::DirectResponse`].
    pub fn schedule_direct_response(&self, response: http::Response<bytes::Bytes>) -> Result<(), ArionWasmError> {
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
    pub fn get_body(&self) -> Result<bytes::Bytes, ArionWasmError> {
        get_http_body()
    }

    /// Replace the buffered request body.
    ///
    /// If the request already has a `Content-Length` header and the length changes,
    /// the host updates that header. It does not insert `Content-Length` when absent.
    pub fn set_body(&self, body: &[u8]) -> Result<(), ArionWasmError> {
        set_http_body(body)
    }

    /// Materialize the request (headers and body) into a standard http::Request<Bytes>.
    pub fn take_request(&self) -> Result<http::Request<bytes::Bytes>, ArionWasmError> {
        let mut buf = Vec::with_capacity(1024 * 64);
        let mut written_len: u32 = 0;

        loop {
            let res = unsafe {
                ffi::arion_get_request(buf.as_mut_ptr(), buf.capacity() as u32, &mut written_len as *mut u32)
            };

            if res == 0 {
                unsafe { buf.set_len(written_len as usize) };
                let wasm_req = match bincode_next::serde::decode_from_slice::<arion_wasm_types::WasmRequest, _>(
                    &buf,
                    bincode_next::config::standard(),
                ) {
                    Ok((w, _)) => w,
                    Err(_) => return Err(ArionWasmError::InternalError),
                };
                return Ok(wasm_req.request);
            } else if res == ArionWasmError::BufferTooSmall as i32 {
                let exact_len = written_len as usize;
                if exact_len <= buf.capacity() {
                    return Err(ArionWasmError::BufferTooSmall);
                }
                buf.reserve_exact(exact_len - buf.len());
            } else {
                return Err(ArionWasmError::from_ffi(res).err().unwrap_or(ArionWasmError::InternalError));
            }
        }
    }

    /// Replace the entire request (headers, URI, method, and body) from a given http::Request<Bytes>.
    pub fn replace_request(&self, req: &http::Request<bytes::Bytes>) -> Result<(), ArionWasmError> {
        let wasm_req = arion_wasm_types::SerWasmRequest { request: req };
        let serialized = match bincode_next::serde::encode_to_vec(&wasm_req, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(_) => return Err(ArionWasmError::InternalError),
        };

        let res = unsafe { ffi::arion_set_request(serialized.as_ptr(), serialized.len() as u32) };
        if res == 0 {
            Ok(())
        } else {
            Err(ArionWasmError::from_ffi(res).err().unwrap_or(ArionWasmError::InternalError))
        }
    }

    pub fn get_trailers_map(&self) -> Result<HeaderMap, ArionWasmError> {
        get_http_headers_map(1)
    }

    pub fn set_trailers_map(&self, trailers: &HeaderMap) -> Result<(), ArionWasmError> {
        set_http_headers_map(1, trailers)
    }

    pub fn get_trailer<'a, N>(&self, name: N) -> Result<Option<WasmHeaderValue<'static>>, ArionWasmError>
    where
        N: Into<WasmHeaderName<'a>>,
    {
        get_http_header(1, &name.into())
    }

    pub fn set_trailer<'a, N, V>(&self, name: N, value: V) -> Result<(), ArionWasmError>
    where
        N: Into<WasmHeaderName<'a>>,
        V: Into<WasmHeaderValue<'a>>,
    {
        set_http_header(1, &name.into(), &value.into())
    }

    pub fn add_trailer<'a, N, V>(&self, name: N, value: V) -> Result<(), ArionWasmError>
    where
        N: Into<WasmHeaderName<'a>>,
        V: Into<WasmHeaderValue<'a>>,
    {
        add_http_header(1, &name.into(), &value.into())
    }

    pub fn remove_trailer<'a, N>(&self, name: N) -> Result<(), ArionWasmError>
    where
        N: Into<WasmHeaderName<'a>>,
    {
        remove_http_header(1, &name.into())
    }

    pub fn replace_trailer<'a, N, V>(&self, name: N, value: V) -> Result<(), ArionWasmError>
    where
        N: Into<WasmHeaderName<'a>>,
        V: Into<WasmHeaderValue<'a>>,
    {
        replace_http_header(1, &name.into(), &value.into())
    }

    pub fn apply_trailer_mutations(&self, mutations: &[HeaderMutation<'_>]) -> Result<(), ArionWasmError> {
        apply_header_mutations(1, mutations)
    }
}
