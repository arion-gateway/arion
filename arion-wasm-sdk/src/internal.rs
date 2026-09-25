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
use crate::{ArionWasmError, HeaderMutation};
use arion_wasm_types::WasmUri;
use arion_wasm_types::{WasmHeaderName, WasmHeaderValue};
use http::HeaderMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// HeaderMap serde wrappers
// `bincode_next::serde::encode_to_vec` / `decode_from_slice` need a type that
// implements `Serialize` / `Deserialize`; these zero-overhead newtypes delegate
// to `http_serde_ext` without introducing extra allocations.

struct SerHeaderMap<'a>(&'a HeaderMap);

impl<'a> Serialize for SerHeaderMap<'a> {
    #[inline]
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        http_serde_ext::header_map::serialize(self.0, s)
    }
}

struct DeHeaderMap(HeaderMap);

impl<'de> Deserialize<'de> for DeHeaderMap {
    #[inline]
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        http_serde_ext::header_map::deserialize(d).map(DeHeaderMap)
    }
}

// Header mutations
// High-level API

pub(crate) const DEFAULT_HEADER_STACK_BUF_SIZE: usize = 1024;
pub(crate) const DEFAULT_HEAP_BUF_SIZE: usize = 4096;

/// Read an HTTP header by name.
pub(crate) fn get_http_header(
    is_trailer: u32,
    name: &WasmHeaderName<'_>,
) -> Result<Option<WasmHeaderValue<'static>>, ArionWasmError> {
    let name_bytes = name.as_bytes();
    let mut stack_buf = [0u8; DEFAULT_HEADER_STACK_BUF_SIZE];
    let mut written_len: u32 = 0;

    let res = unsafe {
        ffi::arion_get_header(
            is_trailer,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            stack_buf.as_mut_ptr(),
            stack_buf.len() as u32,
            &mut written_len as *mut u32,
        )
    };

    match ArionWasmError::from_ffi(res) {
        Ok(()) => {
            let len = written_len as usize;
            Ok(Some(WasmHeaderValue::Owned(bytes::Bytes::copy_from_slice(&stack_buf[..len]))))
        },
        Err(ArionWasmError::NotFound) => Ok(None),
        Err(ArionWasmError::BufferTooSmall) => {
            let exact_len = written_len as usize;
            let mut heap_buf: Vec<u8> = Vec::with_capacity(exact_len);

            let res = unsafe {
                ffi::arion_get_header(
                    is_trailer,
                    name_bytes.as_ptr(),
                    name_bytes.len() as u32,
                    heap_buf.as_mut_ptr(),
                    exact_len as u32,
                    &mut written_len as *mut u32,
                )
            };

            match ArionWasmError::from_ffi(res) {
                Ok(()) => {
                    unsafe {
                        heap_buf.set_len(written_len as usize);
                    }
                    Ok(Some(WasmHeaderValue::Owned(bytes::Bytes::from(heap_buf))))
                },
                Err(e) => Err(e),
            }
        },
        Err(other) => Err(other),
    }
}

/// Read the buffered body.
pub(crate) fn get_http_body() -> Result<bytes::Bytes, ArionWasmError> {
    let mut buf: Vec<u8> = Vec::with_capacity(DEFAULT_HEAP_BUF_SIZE);
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe { ffi::arion_get_body(buf.as_mut_ptr(), buf.capacity() as u32, &mut written_len as *mut u32) };

        match ArionWasmError::from_ffi(res) {
            Ok(()) => {
                unsafe {
                    buf.set_len(written_len as usize);
                }
                return Ok(buf.into());
            },
            Err(ArionWasmError::BufferTooSmall) => {
                let exact_len = written_len as usize;
                if exact_len <= buf.capacity() {
                    return Err(ArionWasmError::BufferTooSmall);
                }
                buf.reserve_exact(exact_len - buf.len());
            },
            Err(other) => return Err(other),
        }
    }
}

/// Replace the buffered body.
pub(crate) fn set_http_body(body: &[u8]) -> Result<(), ArionWasmError> {
    let res = unsafe { ffi::arion_set_body(body.as_ptr(), body.len() as u32) };

    ArionWasmError::from_ffi(res)
}

/// Send a direct (local) HTTP response and short-circuit the filter chain.
pub(crate) fn send_http_direct_response(response: http::Response<bytes::Bytes>) -> Result<(), ArionWasmError> {
    let direct_resp = arion_wasm_types::DirectResponse { response };
    let serialized = bincode_next::serde::encode_to_vec(&direct_resp, bincode_next::config::standard())
        .map_err(|_| ArionWasmError::InternalError)?;
    let res = unsafe { ffi::arion_send_direct_response(serialized.as_ptr(), serialized.len() as u32) };

    ArionWasmError::from_ffi(res)
}

pub(crate) fn get_http_headers_map(is_trailer: u32) -> Result<HeaderMap, ArionWasmError> {
    let mut buf: Vec<u8> = Vec::with_capacity(DEFAULT_HEAP_BUF_SIZE);
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::arion_get_headers_map(
                is_trailer,
                buf.as_mut_ptr(),
                buf.capacity() as u32,
                &mut written_len as *mut u32,
            )
        };

        match ArionWasmError::from_ffi(res) {
            Ok(()) => {
                unsafe {
                    buf.set_len(written_len as usize);
                }
                return bincode_next::serde::decode_from_slice::<DeHeaderMap, _>(
                    &buf,
                    bincode_next::config::standard(),
                )
                .map(|(DeHeaderMap(map), _)| map)
                .map_err(|_| ArionWasmError::InternalError);
            },
            Err(ArionWasmError::BufferTooSmall) => {
                let exact_len = written_len as usize;
                if exact_len <= buf.capacity() {
                    return Err(ArionWasmError::BufferTooSmall);
                }
                buf.reserve_exact(exact_len - buf.len());
            },
            Err(other) => return Err(other),
        }
    }
}

pub(crate) fn set_http_headers_map(is_trailer: u32, headers: &HeaderMap) -> Result<(), ArionWasmError> {
    let serialized = bincode_next::serde::encode_to_vec(SerHeaderMap(headers), bincode_next::config::standard())
        .map_err(|_| ArionWasmError::InternalError)?;
    let res = unsafe { ffi::arion_set_headers_map(is_trailer, serialized.as_ptr(), serialized.len() as u32) };
    ArionWasmError::from_ffi(res)
}

pub(crate) fn set_http_header(
    is_trailer: u32,
    name: &WasmHeaderName<'_>,
    value: &WasmHeaderValue<'_>,
) -> Result<(), ArionWasmError> {
    let name_bytes = name.as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe {
        ffi::arion_set_header(
            is_trailer,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            value_bytes.as_ptr(),
            value_bytes.len() as u32,
        )
    };
    ArionWasmError::from_ffi(res)
}

pub(crate) fn get_http_uri() -> Result<WasmUri<'static>, ArionWasmError> {
    let mut stack_buf = [0u8; DEFAULT_HEADER_STACK_BUF_SIZE];
    let mut written_len: u32 = 0;

    let res =
        unsafe { ffi::arion_get_uri(stack_buf.as_mut_ptr(), stack_buf.len() as u32, &mut written_len as *mut u32) };

    match ArionWasmError::from_ffi(res) {
        Ok(()) => {
            let len = written_len as usize;
            let s = std::str::from_utf8(&stack_buf[..len]).map_err(|_| ArionWasmError::InternalError)?;
            Ok(WasmUri::Owned(smol_str::SmolStr::new(s)))
        },
        Err(ArionWasmError::BufferTooSmall) => {
            let exact_len = written_len as usize;
            let mut heap_buf: Vec<u8> = Vec::with_capacity(exact_len);

            let res =
                unsafe { ffi::arion_get_uri(heap_buf.as_mut_ptr(), exact_len as u32, &mut written_len as *mut u32) };

            match ArionWasmError::from_ffi(res) {
                Ok(()) => {
                    unsafe {
                        heap_buf.set_len(written_len as usize);
                    }
                    let s = String::from_utf8(heap_buf).map_err(|_| ArionWasmError::InternalError)?;
                    Ok(WasmUri::Owned(smol_str::SmolStr::new(s)))
                },
                Err(e) => Err(e),
            }
        },
        Err(other) => Err(other),
    }
}

pub(crate) fn set_http_uri(uri: &WasmUri<'_>) -> Result<(), ArionWasmError> {
    let bytes = uri.as_bytes();
    let res = unsafe { ffi::arion_set_uri(bytes.as_ptr(), bytes.len() as u32) };
    ArionWasmError::from_ffi(res)
}

pub(crate) fn get_http_status_code() -> Result<u16, ArionWasmError> {
    let mut status_code: u32 = 0;
    let res = unsafe { ffi::arion_get_status_code(&mut status_code as *mut u32) };

    match ArionWasmError::from_ffi(res) {
        Ok(()) => {
            let code_u16 = u16::try_from(status_code).map_err(|_| ArionWasmError::InternalError)?;
            Ok(code_u16)
        },
        Err(e) => Err(e),
    }
}

pub(crate) fn set_http_status_code(status_code: u16) -> Result<(), ArionWasmError> {
    let res = unsafe { ffi::arion_set_status_code(status_code as u32) };
    ArionWasmError::from_ffi(res)
}

pub(crate) fn add_http_header(
    is_trailer: u32,
    name: &WasmHeaderName<'_>,
    value: &WasmHeaderValue<'_>,
) -> Result<(), ArionWasmError> {
    let name_bytes = name.as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe {
        ffi::arion_add_header(
            is_trailer,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            value_bytes.as_ptr(),
            value_bytes.len() as u32,
        )
    };
    ArionWasmError::from_ffi(res)
}

pub(crate) fn remove_http_header(is_trailer: u32, name: &WasmHeaderName<'_>) -> Result<(), ArionWasmError> {
    let name_bytes = name.as_bytes();
    let res = unsafe { ffi::arion_remove_header(is_trailer, name_bytes.as_ptr(), name_bytes.len() as u32) };
    ArionWasmError::from_ffi(res)
}

pub(crate) fn replace_http_header(
    is_trailer: u32,
    name: &WasmHeaderName<'_>,
    value: &WasmHeaderValue<'_>,
) -> Result<(), ArionWasmError> {
    let name_bytes = name.as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe {
        ffi::arion_replace_header(
            is_trailer,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            value_bytes.as_ptr(),
            value_bytes.len() as u32,
        )
    };
    ArionWasmError::from_ffi(res)
}

pub(crate) fn apply_header_mutations(is_trailer: u32, mutations: &[HeaderMutation<'_>]) -> Result<(), ArionWasmError> {
    let serialized = bincode_next::serde::encode_to_vec(mutations, bincode_next::config::standard())
        .map_err(|_| ArionWasmError::InternalError)?;
    let res = unsafe { ffi::arion_apply_header_mutations(is_trailer, serialized.as_ptr(), serialized.len() as u32) };
    ArionWasmError::from_ffi(res)
}
