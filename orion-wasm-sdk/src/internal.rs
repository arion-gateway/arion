use crate::ffi;
use crate::{HeaderMutation, OrionWasmError};
use http::{header::HeaderName, header::HeaderValue, HeaderMap};
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
    handle: u64,
    target: ffi::HeaderTarget,
    name: &str,
) -> Result<Option<HeaderValue>, OrionWasmError> {
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

    match OrionWasmError::from_ffi(res) {
        Ok(()) => {
            let len = written_len as usize;
            Ok(HeaderValue::from_bytes(&stack_buf[..len]).ok())
        },
        Err(OrionWasmError::NotFound) => Ok(None),
        Err(OrionWasmError::BufferTooSmall) => {
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

                match OrionWasmError::from_ffi(res) {
                    Ok(()) => {
                        unsafe {
                            heap_buf.set_len(written_len as usize);
                        }
                        return Ok(HeaderValue::from_bytes(&heap_buf).ok());
                    },
                    Err(OrionWasmError::NotFound) => return Ok(None),
                    Err(OrionWasmError::BufferTooSmall) => {
                        let new_cap = heap_buf.capacity().saturating_mul(2);
                        if new_cap == heap_buf.capacity() {
                            return Err(OrionWasmError::BufferTooSmall);
                        }
                        heap_buf.reserve_exact(new_cap);
                    },
                    Err(other) => return Err(other),
                }
            }
        },
        Err(other) => Err(other),
    }
}

/// Read the buffered body.
pub(crate) fn get_http_body(handle: u64, target: ffi::HeaderTarget) -> Result<Vec<u8>, OrionWasmError> {
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

        match OrionWasmError::from_ffi(res) {
            Ok(()) => {
                unsafe {
                    buf.set_len(written_len as usize);
                }
                return Ok(buf);
            },
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

/// Replace the buffered body.
pub(crate) fn set_http_body(handle: u64, target: ffi::HeaderTarget, body: &[u8]) -> Result<(), OrionWasmError> {
    let res = unsafe { ffi::orion_set_body(handle, target as u32, body.as_ptr(), body.len() as u32) };

    OrionWasmError::from_ffi(res)
}

/// Send a direct (local) HTTP response and short-circuit the filter chain.
pub(crate) fn send_http_direct_response(
    request_handle: u64,
    status_code: u16,
    body: &[u8],
) -> Result<(), OrionWasmError> {
    let res = unsafe {
        ffi::orion_send_direct_response(request_handle, status_code as u32, body.as_ptr(), body.len() as u32)
    };

    OrionWasmError::from_ffi(res)
}

pub(crate) fn get_http_headers_map(handle: u64, target: ffi::HeaderTarget) -> Result<HeaderMap, OrionWasmError> {
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

pub(crate) fn set_http_headers_map(
    handle: u64,
    target: ffi::HeaderTarget,
    headers: &HeaderMap,
) -> Result<(), OrionWasmError> {
    let serialized = bincode_next::serde::encode_to_vec(SerHeaderMap(headers), bincode_next::config::standard())
        .map_err(|_| OrionWasmError::InternalError)?;
    let res =
        unsafe { ffi::orion_set_headers_map(handle, target as u32, serialized.as_ptr(), serialized.len() as u32) };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn set_http_header(
    handle: u64,
    target: ffi::HeaderTarget,
    name: &HeaderName,
    value: &HeaderValue,
) -> Result<(), OrionWasmError> {
    let name_bytes = name.as_str().as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe {
        ffi::orion_set_header(
            handle,
            target as u32,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            value_bytes.as_ptr(),
            value_bytes.len() as u32,
        )
    };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn add_http_header(
    handle: u64,
    target: ffi::HeaderTarget,
    name: &HeaderName,
    value: &HeaderValue,
) -> Result<(), OrionWasmError> {
    let name_bytes = name.as_str().as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe {
        ffi::orion_add_header(
            handle,
            target as u32,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            value_bytes.as_ptr(),
            value_bytes.len() as u32,
        )
    };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn remove_http_header(
    handle: u64,
    target: ffi::HeaderTarget,
    name: &HeaderName,
) -> Result<(), OrionWasmError> {
    let name_bytes = name.as_str().as_bytes();
    let res = unsafe { ffi::orion_remove_header(handle, target as u32, name_bytes.as_ptr(), name_bytes.len() as u32) };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn replace_http_header(
    handle: u64,
    target: ffi::HeaderTarget,
    name: &HeaderName,
    value: &HeaderValue,
) -> Result<(), OrionWasmError> {
    let name_bytes = name.as_str().as_bytes();
    let value_bytes = value.as_bytes();
    let res = unsafe {
        ffi::orion_replace_header(
            handle,
            target as u32,
            name_bytes.as_ptr(),
            name_bytes.len() as u32,
            value_bytes.as_ptr(),
            value_bytes.len() as u32,
        )
    };
    OrionWasmError::from_ffi(res)
}

pub(crate) fn apply_header_mutations(
    handle: u64,
    target: ffi::HeaderTarget,
    mutations: &[HeaderMutation],
) -> Result<(), OrionWasmError> {
    let serialized = bincode_next::serde::encode_to_vec(mutations, bincode_next::config::standard())
        .map_err(|_| OrionWasmError::InternalError)?;
    let res = unsafe {
        ffi::orion_apply_header_mutations(handle, target as u32, serialized.as_ptr(), serialized.len() as u32)
    };
    OrionWasmError::from_ffi(res)
}
