use crate::ffi;
use crate::{HeaderMutation, OrionWasmError};
use http::HeaderMap;
use orion_wasm_types::{WasmHeaderName, WasmHeaderValue};
use orion_wasm_types::WasmUri;
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
) -> Result<Option<WasmHeaderValue<'static>>, OrionWasmError> {
    let name_bytes = name.as_bytes();
    let mut stack_buf = [0u8; DEFAULT_HEADER_STACK_BUF_SIZE];
    let mut written_len: u32 = 0;

    let res = unsafe {
        ffi::orion_get_header(
            is_trailer,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            stack_buf.as_mut_ptr(),
            stack_buf.len() as u32,
            &mut written_len as *mut u32,
        )
    };

    match OrionWasmError::from_ffi(res) {
        Ok(()) => {
            let len = written_len as usize;
            Ok(Some(WasmHeaderValue::Owned(bytes::Bytes::copy_from_slice(&stack_buf[..len]))))
        },
        Err(OrionWasmError::NotFound) => Ok(None),
        Err(OrionWasmError::BufferTooSmall) => {
            let mut heap_buf: Vec<u8> = Vec::with_capacity(DEFAULT_HEAP_BUF_SIZE);
            let mut written_len: u32 = 0;

            loop {
                let res = unsafe {
                    ffi::orion_get_header(
                        is_trailer,
                        name_bytes.as_ptr(),
                        name_bytes.len() as u32,
                        heap_buf.as_mut_ptr(),
                        heap_buf.capacity() as u32,
                        &mut written_len as *mut u32,
                    )
                };

                match OrionWasmError::from_ffi(res) {
                    Ok(()) => {
                        unsafe {
                            heap_buf.set_len(written_len as usize);
                        }
                        return Ok(Some(WasmHeaderValue::Owned(bytes::Bytes::from(heap_buf))));
                    },
                    Err(OrionWasmError::NotFound) => return Ok(None),
                    Err(OrionWasmError::BufferTooSmall) => {
                        let exact_len = written_len as usize;
                        if exact_len <= heap_buf.capacity() {
                            return Err(OrionWasmError::BufferTooSmall);
                        }
                        heap_buf.reserve_exact(exact_len - heap_buf.len());
                    },
                    Err(other) => return Err(other),
                }
            }
        },
        Err(other) => Err(other),
    }
}

/// Read the buffered body.
pub(crate) fn get_http_body(is_trailer: u32) -> Result<bytes::Bytes, OrionWasmError> {
    let mut buf: Vec<u8> = Vec::with_capacity(DEFAULT_HEAP_BUF_SIZE);
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::orion_get_body(
                is_trailer,
                buf.as_mut_ptr(),
                buf.capacity() as u32,
                &mut written_len as *mut u32,
            )
        };

        match OrionWasmError::from_ffi(res) {
            Ok(()) => {
                unsafe {
                    buf.set_len(written_len as usize);
                }
                return Ok(buf.into());
            },
            Err(OrionWasmError::BufferTooSmall) => {
                let exact_len = written_len as usize;
                if exact_len <= buf.capacity() {
                    return Err(OrionWasmError::BufferTooSmall);
                }
                buf.reserve_exact(exact_len - buf.len());
            },
            Err(other) => return Err(other),
        }
    }
}

/// Replace the buffered body.
pub(crate) fn set_http_body(is_trailer: u32, body: &[u8]) -> Result<(), OrionWasmError> {
    let res = unsafe { ffi::orion_set_body(is_trailer, body.as_ptr(), body.len() as u32) };

    OrionWasmError::from_ffi(res)
}

/// Send a direct (local) HTTP response and short-circuit the filter chain.
pub(crate) fn send_http_direct_response(
    response: http::Response<bytes::Bytes>,
) -> Result<(), OrionWasmError> {
    let direct_resp = orion_wasm_types::DirectResponse { response };
    let serialized = bincode_next::serde::encode_to_vec(&direct_resp, bincode_next::config::standard())
        .map_err(|_| OrionWasmError::InternalError)?;
    let res = unsafe {
        ffi::orion_send_direct_response(serialized.as_ptr(), serialized.len() as u32)
    };

    OrionWasmError::from_ffi(res)
}

pub(crate) fn get_http_headers_map(is_trailer: u32) -> Result<HeaderMap, OrionWasmError> {
    let mut buf: Vec<u8> = Vec::with_capacity(DEFAULT_HEAP_BUF_SIZE);
    let mut written_len: u32 = 0;

    loop {
        let res = unsafe {
            ffi::orion_get_headers_map(
                is_trailer,
                buf.as_mut_ptr(),
                buf.capacity() as u32,
                &mut written_len as *mut u32,
            )
        };

        match OrionWasmError::from_ffi(res) {
            Ok(()) => {
                unsafe {
                    buf.set_len(written_len as usize);
                }
                return bincode_next::serde::decode_from_slice::<DeHeaderMap, _>(
                    &buf,
                    bincode_next::config::standard(),
                )
                .map(|(DeHeaderMap(map), _)| map)
                .map_err(|_| OrionWasmError::InternalError);
            },
            Err(OrionWasmError::BufferTooSmall) => {
                let exact_len = written_len as usize;
                if exact_len <= buf.capacity() {
                    return Err(OrionWasmError::BufferTooSmall);
                }
                buf.reserve_exact(exact_len - buf.len());
            },
            Err(other) => return Err(other),
        }
    }
}

pub(crate) fn set_http_headers_map(
    is_trailer: u32,
    headers: &HeaderMap,
) -> Result<(), OrionWasmError> {
    let serialized = bincode_next::serde::encode_to_vec(SerHeaderMap(headers), bincode_next::config::standard())
        .map_err(|_| OrionWasmError::InternalError)?;
    let res =
        unsafe { ffi::orion_set_headers_map(is_trailer, serialized.as_ptr(), serialized.len() as u32) };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn set_http_header(
    is_trailer: u32,
    name: &WasmHeaderName<'_>,
    value: &WasmHeaderValue<'_>,
) -> Result<(), OrionWasmError> {
    let name_bytes = name.as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe {
        ffi::orion_set_header(
            is_trailer,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            value_bytes.as_ptr(),
            value_bytes.len() as u32,
        )
    };
    OrionWasmError::from_ffi(res)
}


pub(crate) fn get_http_uri() -> Result<WasmUri<'static>, OrionWasmError> {
    let mut stack_buf = [0u8; DEFAULT_HEADER_STACK_BUF_SIZE];
    let mut written_len: u32 = 0;

    let res = unsafe {
        ffi::orion_get_uri(
            stack_buf.as_mut_ptr(),
            stack_buf.len() as u32,
            &mut written_len as *mut u32,
        )
    };

    match OrionWasmError::from_ffi(res) {
        Ok(()) => {
            let len = written_len as usize;
            let s = std::str::from_utf8(&stack_buf[..len])
                .map_err(|_| OrionWasmError::InternalError)?;
            Ok(WasmUri::Owned(smol_str::SmolStr::new(s)))
        },
        Err(OrionWasmError::BufferTooSmall) => {
            let mut heap_buf: Vec<u8> = Vec::with_capacity(DEFAULT_HEAP_BUF_SIZE);
            let mut written_len: u32 = 0;

            loop {
                let res = unsafe {
                    ffi::orion_get_uri(
                        heap_buf.as_mut_ptr(),
                        heap_buf.capacity() as u32,
                        &mut written_len as *mut u32,
                    )
                };

                match OrionWasmError::from_ffi(res) {
                    Ok(()) => {
                        unsafe {
                            heap_buf.set_len(written_len as usize);
                        }
                        let s = std::str::from_utf8(&heap_buf)
                            .map_err(|_| OrionWasmError::InternalError)?;
                        return Ok(WasmUri::Owned(smol_str::SmolStr::new(s)));
                    },
                    Err(OrionWasmError::BufferTooSmall) => {
                        let exact_len = written_len as usize;
                        if exact_len <= heap_buf.capacity() {
                            return Err(OrionWasmError::BufferTooSmall);
                        }
                        heap_buf.reserve_exact(exact_len - heap_buf.len());
                    },
                    Err(other) => return Err(other),
                }
            }
        },
        Err(other) => Err(other),
    }
}

pub(crate) fn set_http_uri(uri: &WasmUri<'_>) -> Result<(), OrionWasmError> {
    let bytes = uri.as_bytes();
    let res = unsafe { ffi::orion_set_uri(bytes.as_ptr(), bytes.len() as u32) };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn get_http_status_code() -> Result<u16, OrionWasmError> {
    let mut status_code: u32 = 0;
    let res = unsafe {
        ffi::orion_get_status_code(&mut status_code as *mut u32)
    };

    match OrionWasmError::from_ffi(res) {
        Ok(()) => {
            let code_u16 = u16::try_from(status_code).map_err(|_| OrionWasmError::InternalError)?;
            Ok(code_u16)
        },
        Err(e) => Err(e),
    }
}

pub(crate) fn set_http_status_code(status_code: u16) -> Result<(), OrionWasmError> {
    let res = unsafe { ffi::orion_set_status_code(status_code as u32) };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn add_http_header(
    is_trailer: u32,
    name: &WasmHeaderName<'_>,
    value: &WasmHeaderValue<'_>,
) -> Result<(), OrionWasmError> {
    let name_bytes = name.as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe {
        ffi::orion_add_header(
            is_trailer,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            value_bytes.as_ptr(),
            value_bytes.len() as u32,
        )
    };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn remove_http_header(
    is_trailer: u32,
    name: &WasmHeaderName<'_>,
) -> Result<(), OrionWasmError> {
    let name_bytes = name.as_bytes();
    let res = unsafe { ffi::orion_remove_header(is_trailer, name_bytes.as_ptr(), name_bytes.len() as u32) };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn replace_http_header(
    is_trailer: u32,
    name: &WasmHeaderName<'_>,
    value: &WasmHeaderValue<'_>,
) -> Result<(), OrionWasmError> {
    let name_bytes = name.as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe {
        ffi::orion_replace_header(
            is_trailer,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            value_bytes.as_ptr(),
            value_bytes.len() as u32,
        )
    };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn apply_header_mutations(
    is_trailer: u32,
    mutations: &[HeaderMutation<'_>],
) -> Result<(), OrionWasmError> {
    let serialized = bincode_next::serde::encode_to_vec(mutations, bincode_next::config::standard())
        .map_err(|_| OrionWasmError::InternalError)?;
    let res = unsafe {
        ffi::orion_apply_header_mutations(is_trailer, serialized.as_ptr(), serialized.len() as u32)
    };
    OrionWasmError::from_ffi(res)
}
