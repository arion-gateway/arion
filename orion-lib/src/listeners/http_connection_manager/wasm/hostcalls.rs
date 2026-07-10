use super::types::OrionWasmResult;
use crate::body::timeout_body::TimeoutBody;
use crate::OrionRequestBody;
use crate::OrionResponseBody;
use bytes::Bytes;
use http::StatusCode;
use http::{Request, Response};
use http_body_util::Full;
use smol_str::ToSmolStr;
use wasmtime::{Caller, Linker};

pub struct WasmState {
    pub name: &'static str,
    pub direct_response: Option<Response<OrionResponseBody>>,
    pub buffered_request_body: Option<bytes::Bytes>,
    pub buffered_response_body: Option<bytes::Bytes>,
}

fn orion_get_header(
    mut caller: Caller<'_, WasmState>,
    handle: u64,
    handle_type: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let name = {
        let data = memory.data(&caller);
        let start = name_ptr as usize;
        let end = start + name_len as usize;
        if end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        match std::str::from_utf8(&data[start..end]) {
            Ok(s) => s.to_smolstr(),
            Err(_) => return OrionWasmResult::InvalidMemoryAccess.into(),
        }
    };

    let val_opt = match handle_type {
        0 => {
            let request = unsafe { &*(handle as *const Request<OrionRequestBody>) };
            request.headers().get(name.as_str())
        }
        1 => {
            let response = unsafe { &*(handle as *const Response<OrionResponseBody>) };
            response.headers().get(name.as_str())
        }
        _ => return OrionWasmResult::InternalError.into(),
    };

    if let Some(val) = val_opt {
        let val_bytes = val.as_bytes();
        if val_bytes.len() > value_max_len as usize {
            return OrionWasmResult::BufferTooSmall.into();
        }

        let data = memory.data_mut(&mut caller);

        let val_start = value_ptr as usize;
        let val_end = val_start + val_bytes.len();
        if val_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[val_start..val_end].copy_from_slice(val_bytes);

        let len_start = written_len_ptr as usize;
        let len_end = len_start + 4;
        if len_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[len_start..len_end].copy_from_slice(&(val_bytes.len() as u32).to_le_bytes());

        OrionWasmResult::Ok.into()
    } else {
        OrionWasmResult::NotFound.into()
    }
}

fn orion_get_body(
    mut caller: Caller<'_, WasmState>,
    handle: u64,
    handle_type: u32,
    body_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let body_bytes = match handle_type {
        0 => match caller.data().buffered_request_body.as_ref() {
            Some(b) => b.clone(),
            None => return OrionWasmResult::NotFound.into(),
        }
        1 => match caller.data().buffered_response_body.as_ref() {
            Some(b) => b.clone(),
            None => return OrionWasmResult::NotFound.into(),
        }
        _ => return OrionWasmResult::InternalError.into(),
    };

    if body_bytes.len() > max_len as usize {
        return OrionWasmResult::BufferTooSmall.into();
    }

    let data = memory.data_mut(&mut caller);

    let start = body_ptr as usize;
    let end = start + body_bytes.len();
    if end > data.len() {
        return OrionWasmResult::InvalidMemoryAccess.into();
    }
    data[start..end].copy_from_slice(&body_bytes);

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return OrionWasmResult::InvalidMemoryAccess.into();
    }
    data[len_start..len_end].copy_from_slice(&(body_bytes.len() as u32).to_le_bytes());

    OrionWasmResult::Ok.into()
}

fn orion_send_direct_response(
    mut caller: Caller<'_, WasmState>,
    request_handle: u64,
    status_code: u32,
    body_ptr: u32,
    body_len: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let request = unsafe { &*(request_handle as *const Request<OrionRequestBody>) };

    let body_bytes = if body_len > 0 {
        let data = memory.data(&caller);
        let start = body_ptr as usize;
        let end = start + body_len as usize;
        if end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        Bytes::copy_from_slice(&data[start..end])
    } else {
        Bytes::new()
    };

    let status = match StatusCode::from_u16(status_code as u16) {
        Ok(s) => s,
        Err(_) => return OrionWasmResult::InternalError.into(),
    };

    use crate::body::poly_body::PolyBody;
    let mut response = Response::new(TimeoutBody::new(None, PolyBody::from(Full::from(body_bytes))));
    *response.status_mut() = status;
    *response.version_mut() = request.version();

    caller.data_mut().direct_response = Some(response);

    OrionWasmResult::Ok.into()
}



fn orion_log(mut caller: Caller<'_, WasmState>, level: u32, msg_ptr: u32, msg_len: u32) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let data = memory.data(&caller);
    let start = msg_ptr as usize;
    let end = start + msg_len as usize;

    if end > data.len() {
        return OrionWasmResult::InvalidMemoryAccess.into();
    }

    let msg = match std::str::from_utf8(&data[start..end]) {
        Ok(s) => s,
        Err(_) => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let name = caller.data().name;
    match level {
        1 => tracing::error!(target: "wasm", "{name}: {msg}"),
        2 => tracing::warn!(target:  "wasm", "{name}: {msg}"),
        3 => tracing::info!(target:  "wasm", "{name}: {msg}"),
        4 => tracing::debug!(target: "wasm", "{name}: {msg}"),
        _ => tracing::trace!(target: "wasm", "{name}: {msg}"),
    }

    OrionWasmResult::Ok.into()
}

use http::HeaderMap;

fn serialize_header_map(headers: &HeaderMap) -> Vec<u8> {
    let mut buf = Vec::new();
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

fn deserialize_header_map(data: &[u8]) -> Option<HeaderMap> {
    let mut headers = HeaderMap::new();
    if data.len() < 4 {
        return Some(headers);
    }

    let num_headers = u32::from_le_bytes(data[0..4].try_into().unwrap());
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

fn orion_get_headers_map(
    mut caller: Caller<'_, WasmState>,
    handle: u64,
    handle_type: u32,
    buf_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };
    
    let serialized = match handle_type {
        0 => {
            let request = unsafe { &*(handle as *const Request<OrionRequestBody>) };
            serialize_header_map(request.headers())
        }
        1 => {
            let response = unsafe { &*(handle as *const Response<OrionResponseBody>) };
            serialize_header_map(response.headers())
        }
        _ => return OrionWasmResult::InternalError.into(),
    };

    if serialized.len() > max_len as usize {
        return OrionWasmResult::BufferTooSmall.into();
    }
    let data = memory.data_mut(&mut caller);
    let start = buf_ptr as usize;
    let end = start + serialized.len();
    if end > data.len() {
        return OrionWasmResult::InvalidMemoryAccess.into();
    }
    data[start..end].copy_from_slice(&serialized);
    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return OrionWasmResult::InvalidMemoryAccess.into();
    }
    data[len_start..len_end].copy_from_slice(&(serialized.len() as u32).to_le_bytes());
    OrionWasmResult::Ok.into()
}

fn orion_set_headers_map(
    mut caller: Caller<'_, WasmState>,
    handle: u64,
    handle_type: u32,
    buf_ptr: u32,
    buf_len: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };
    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    if end > data.len() {
        return OrionWasmResult::InvalidMemoryAccess.into();
    }
    let headers = match deserialize_header_map(&data[start..end]) {
        Some(h) => h,
        None => return OrionWasmResult::InternalError.into(),
    };
    
    match handle_type {
        0 => {
            let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
            *request.headers_mut() = headers;
        }
        1 => {
            let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
            *response.headers_mut() = headers;
        }
        _ => return OrionWasmResult::InternalError.into(),
    }
    
    OrionWasmResult::Ok.into()
}

fn get_name_value_from_memory(
    caller: &mut Caller<'_, WasmState>,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> Result<(http::header::HeaderName, http::header::HeaderValue), OrionWasmResult> {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return Err(OrionWasmResult::InvalidMemoryAccess),
    };
    let data = memory.data(caller);

    let n_start = name_ptr as usize;
    let n_end = n_start + name_len as usize;
    if n_end > data.len() {
        return Err(OrionWasmResult::InvalidMemoryAccess);
    }
    let name_str = std::str::from_utf8(&data[n_start..n_end]).map_err(|_| OrionWasmResult::InvalidMemoryAccess)?;
    let name = http::header::HeaderName::try_from(name_str).map_err(|_| OrionWasmResult::InternalError)?;

    let v_start = value_ptr as usize;
    let v_end = v_start + value_len as usize;
    if v_end > data.len() {
        return Err(OrionWasmResult::InvalidMemoryAccess);
    }
    let value =
        http::header::HeaderValue::from_bytes(&data[v_start..v_end]).map_err(|_| OrionWasmResult::InternalError)?;

    Ok((name, value))
}

fn get_name_from_memory(
    caller: &mut Caller<'_, WasmState>,
    name_ptr: u32,
    name_len: u32,
) -> Result<http::header::HeaderName, OrionWasmResult> {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return Err(OrionWasmResult::InvalidMemoryAccess),
    };
    let data = memory.data(caller);

    let n_start = name_ptr as usize;
    let n_end = n_start + name_len as usize;
    if n_end > data.len() {
        return Err(OrionWasmResult::InvalidMemoryAccess);
    }
    let name_str = std::str::from_utf8(&data[n_start..n_end]).map_err(|_| OrionWasmResult::InvalidMemoryAccess)?;
    http::header::HeaderName::try_from(name_str).map_err(|_| OrionWasmResult::InternalError)
}

fn orion_set_header(
    mut caller: Caller<'_, WasmState>,
    handle: u64,
    handle_type: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    match get_name_value_from_memory(&mut caller, name_ptr, name_len, value_ptr, value_len) {
        Ok((name, value)) => match handle_type {
            0 => {
                let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
                request.headers_mut().insert(name, value);
                OrionWasmResult::Ok.into()
            },
            1 => {
                let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
                response.headers_mut().insert(name, value);
                OrionWasmResult::Ok.into()
            },
            _ => OrionWasmResult::InternalError.into(),
        },
        Err(e) => e.into(),
    }
}

fn orion_add_header(
    mut caller: Caller<'_, WasmState>,
    handle: u64,
    handle_type: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    match get_name_value_from_memory(&mut caller, name_ptr, name_len, value_ptr, value_len) {
        Ok((name, value)) => match handle_type {
            0 => {
                let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
                request.headers_mut().append(name, value);
                OrionWasmResult::Ok.into()
            },
            1 => {
                let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
                response.headers_mut().append(name, value);
                OrionWasmResult::Ok.into()
            },
            _ => OrionWasmResult::InternalError.into(),
        },
        Err(e) => e.into(),
    }
}

fn orion_remove_header(
    mut caller: Caller<'_, WasmState>,
    handle: u64,
    handle_type: u32,
    name_ptr: u32,
    name_len: u32,
) -> i32 {
    match get_name_from_memory(&mut caller, name_ptr, name_len) {
        Ok(name) => match handle_type {
            0 => {
                let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
                request.headers_mut().remove(&name);
                OrionWasmResult::Ok.into()
            },
            1 => {
                let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
                response.headers_mut().remove(&name);
                OrionWasmResult::Ok.into()
            },
            _ => OrionWasmResult::InternalError.into(),
        },
        Err(e) => e.into(),
    }
}

fn orion_replace_header(
    mut caller: Caller<'_, WasmState>,
    handle: u64,
    handle_type: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    match get_name_value_from_memory(&mut caller, name_ptr, name_len, value_ptr, value_len) {
        Ok((name, value)) => match handle_type {
            0 => {
                let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
                if request.headers().contains_key(&name) {
                    request.headers_mut().insert(name, value);
                }
                OrionWasmResult::Ok.into()
            },
            1 => {
                let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
                if response.headers().contains_key(&name) {
                    response.headers_mut().insert(name, value);
                }
                OrionWasmResult::Ok.into()
            },
            _ => OrionWasmResult::InternalError.into(),
        },
        Err(e) => e.into(),
    }
}

pub fn register_hostcalls(linker: &mut Linker<WasmState>) -> Result<(), wasmtime::Error> {

    linker.func_wrap("env", "orion_get_header", orion_get_header)?;
    linker.func_wrap("env", "orion_get_body", orion_get_body)?;
    linker.func_wrap("env", "orion_get_headers_map", orion_get_headers_map)?;
    linker.func_wrap("env", "orion_set_headers_map", orion_set_headers_map)?;

    linker.func_wrap("env", "orion_set_header", orion_set_header)?;
    linker.func_wrap("env", "orion_add_header", orion_add_header)?;
    linker.func_wrap("env", "orion_remove_header", orion_remove_header)?;
    linker.func_wrap("env", "orion_replace_header", orion_replace_header)?;
    
    linker.func_wrap("env", "orion_send_direct_response", orion_send_direct_response)?;
    linker.func_wrap("env", "orion_log", orion_log)?;
    Ok(())
}
