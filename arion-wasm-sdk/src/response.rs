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
use arion_wasm_types::{ArionWasmError, HeaderMutation};
use arion_wasm_types::{WasmHeaderName, WasmHeaderValue};
use http::HeaderMap;

/// Typestate wrapper around a response handle.
pub struct ResponseHandle<S: State> {
    _marker: core::marker::PhantomData<S>,
}

impl<S: State> ResponseHandle<S> {
    /// Create a new response handle.
    #[doc(hidden)]
    pub fn new() -> Self {
        Self { _marker: core::marker::PhantomData }
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

    /// Read the HTTP response status code.
    pub fn get_status_code(&self) -> Result<Option<u16>, ArionWasmError> {
        match get_http_status_code() {
            Ok(status) => Ok(Some(status)),
            Err(ArionWasmError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Set the HTTP response status code.
    pub fn set_status_code(&self, status: u16) -> Result<(), ArionWasmError> {
        set_http_status_code(status)
    }

    /// Read an HTTP response header by name.
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
}

impl ResponseHandle<HttpBody> {
    /// Read the buffered response body.
    pub fn get_body(&self) -> Result<bytes::Bytes, ArionWasmError> {
        get_http_body()
    }

    /// Replace the buffered response body.
    ///
    /// If the response already has a `Content-Length` header and the length changes,
    /// the host updates that header. It does not insert `Content-Length` when absent.
    pub fn set_body(&self, body: &[u8]) -> Result<(), ArionWasmError> {
        set_http_body(body)
    }

    /// Materialize the response (headers and body) into a standard http::Response<Bytes>.
    pub fn take_response(&self) -> Result<http::Response<bytes::Bytes>, ArionWasmError> {
        let mut buf = Vec::with_capacity(1024 * 64);
        let mut written_len: u32 = 0;

        loop {
            let res = unsafe {
                ffi::arion_get_response(buf.as_mut_ptr(), buf.capacity() as u32, &mut written_len as *mut u32)
            };

            if res == 0 {
                unsafe { buf.set_len(written_len as usize) };
                let wasm_res = match bincode_next::serde::decode_from_slice::<arion_wasm_types::WasmResponse, _>(
                    &buf,
                    bincode_next::config::standard(),
                ) {
                    Ok((w, _)) => w,
                    Err(_) => return Err(ArionWasmError::InternalError),
                };
                return Ok(wasm_res.response);
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

    /// Replace the entire response (headers, status code, and body) from a given http::Response<Bytes>.
    pub fn replace_response(&self, res: &http::Response<bytes::Bytes>) -> Result<(), ArionWasmError> {
        let wasm_res = arion_wasm_types::SerWasmResponse { response: res };
        let serialized = match bincode_next::serde::encode_to_vec(&wasm_res, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(_) => return Err(ArionWasmError::InternalError),
        };

        let res = unsafe { ffi::arion_set_response(serialized.as_ptr(), serialized.len() as u32) };
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
