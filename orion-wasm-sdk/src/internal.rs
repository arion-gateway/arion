use crate::ffi;
use crate::{HeaderMutation, OrionWasmError, OrionWasmResult};
use http::{header::HeaderName, header::HeaderValue, HeaderMap};

pub(crate) fn serialize_header_map(headers: &HeaderMap) -> Vec<u8> {
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

pub(crate) fn deserialize_header_map(data: &[u8]) -> Option<HeaderMap> {
    if data.len() < 4 {
        return Some(HeaderMap::new());
    }

    let num_headers = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let mut headers = HeaderMap::with_capacity(num_headers as usize);
    let mut offset = 4;

    for _ in 0..num_headers {
        if offset + 4 > data.len() {
            return None;
        }
        let key_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + key_len > data.len() {
            return None;
        }
        let key_bytes = &data[offset..offset + key_len];
        offset += key_len;

        if offset + 4 > data.len() {
            return None;
        }
        let val_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + val_len > data.len() {
            return None;
        }
        let val_bytes = &data[offset..offset + val_len];
        offset += val_len;

        if let (Ok(name), Ok(value)) =
            (http::header::HeaderName::from_bytes(key_bytes), http::header::HeaderValue::from_bytes(val_bytes))
        {
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

    match OrionWasmResult::from_ffi(res) {
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

                match OrionWasmResult::from_ffi(res) {
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

        match OrionWasmResult::from_ffi(res) {
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

    OrionWasmResult::from_ffi(res)
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

    OrionWasmResult::from_ffi(res)
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

        match OrionWasmResult::from_ffi(res) {
            Ok(()) => {
                unsafe {
                    buf.set_len(written_len as usize);
                }
                return deserialize_header_map(&buf).ok_or(OrionWasmError::InternalError);
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
    let serialized = serialize_header_map(headers);
    let res =
        unsafe { ffi::orion_set_headers_map(handle, target as u32, serialized.as_ptr(), serialized.len() as u32) };
    OrionWasmResult::from_ffi(res)
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
    OrionWasmResult::from_ffi(res)
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
    OrionWasmResult::from_ffi(res)
}

pub(crate) fn remove_http_header(
    handle: u64,
    target: ffi::HeaderTarget,
    name: &HeaderName,
) -> Result<(), OrionWasmError> {
    let name_bytes = name.as_str().as_bytes();
    let res = unsafe { ffi::orion_remove_header(handle, target as u32, name_bytes.as_ptr(), name_bytes.len() as u32) };
    OrionWasmResult::from_ffi(res)
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
    OrionWasmResult::from_ffi(res)
}

pub(crate) fn serialize_header_mutations(mutations: &[HeaderMutation]) -> Vec<u8> {
    let mut capacity = 4;
    for mutation in mutations {
        capacity += 1;
        match mutation {
            HeaderMutation::Set(name, value)
            | HeaderMutation::Add(name, value)
            | HeaderMutation::Replace(name, value) => {
                capacity += 8 + name.as_str().len() + value.len();
            },
            HeaderMutation::Remove(name) => {
                capacity += 4 + name.as_str().len();
            },
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
            },
            HeaderMutation::Add(name, value) => {
                buf.push(1);
                write_key_val(&mut buf, name, value);
            },
            HeaderMutation::Replace(name, value) => {
                buf.push(2);
                write_key_val(&mut buf, name, value);
            },
            HeaderMutation::Remove(name) => {
                buf.push(3);
                let name_bytes = name.as_str().as_bytes();
                buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(name_bytes);
            },
        }
    }
    buf
}

#[inline]
pub(crate) fn write_key_val(buf: &mut Vec<u8>, name: &HeaderName, value: &HeaderValue) {
    let name_bytes = name.as_str().as_bytes();
    let value_bytes = value.as_bytes();
    buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(name_bytes);
    buf.extend_from_slice(&(value_bytes.len() as u32).to_le_bytes());
    buf.extend_from_slice(value_bytes);
}

pub(crate) fn apply_header_mutations(
    handle: u64,
    target: ffi::HeaderTarget,
    mutations: &[HeaderMutation],
) -> Result<(), OrionWasmError> {
    let serialized = serialize_header_mutations(mutations);
    let res = unsafe {
        ffi::orion_apply_header_mutations(handle, target as u32, serialized.as_ptr(), serialized.len() as u32)
    };
    OrionWasmResult::from_ffi(res)
}

/// Serialize an iterator of `(key, value)` string pairs into the wire format shared by
/// the `orion_set_custom_metrics` and `orion_set_access_log_operators` hostcalls:
/// a little-endian `u32` count, followed by each pair encoded as
/// `u32 key_len | key_bytes | u32 val_len | val_bytes`.
pub(crate) fn serialize_kv_pairs<'a, I>(pairs: I) -> Vec<u8>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let iter = pairs.into_iter();
    let estimated = {
        let (lower, upper) = iter.size_hint();
        upper.unwrap_or(lower)
    };

    // 4 bytes count + (4 bytes k_len + k_bytes + 4 bytes v_len + v_bytes) per item.
    // Assumes an average of 16 bytes per string as a safe baseline.
    let mut buf = Vec::with_capacity(4 + estimated * 40);
    buf.extend_from_slice(&[0, 0, 0, 0]); // placeholder for count
    let mut count: u32 = 0;
    for (k, v) in iter {
        let k_bytes = k.as_bytes();
        let v_bytes = v.as_bytes();
        buf.extend_from_slice(&(k_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(k_bytes);
        buf.extend_from_slice(&(v_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(v_bytes);
        count += 1;
    }
    buf[0..4].copy_from_slice(&count.to_le_bytes());
    buf
}
