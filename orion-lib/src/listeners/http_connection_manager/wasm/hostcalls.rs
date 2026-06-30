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
    pub request: Option<*mut Request<OrionRequestBody>>,
    pub direct_response: Option<Response<OrionResponseBody>>,
    pub buffered_body: Option<bytes::Bytes>,
    pub buffered_response_body: Option<bytes::Bytes>,
}

fn orion_get_request_header(
    mut caller: Caller<'_, WasmState>,
    request_handle: u64,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(), // Memory not found
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

    let request = unsafe { &*(request_handle as *const Request<OrionRequestBody>) };

    if let Some(val) = request.headers().get(name.as_str()) {
        let val_bytes = val.as_bytes();
        if val_bytes.len() > value_max_len as usize {
            return OrionWasmResult::BufferTooSmall.into(); // Buffer too small
        }

        let data = memory.data_mut(&mut caller);

        // Write the value
        let val_start = value_ptr as usize;
        let val_end = val_start + val_bytes.len();
        if val_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[val_start..val_end].copy_from_slice(val_bytes);

        // Write the written length
        let len_start = written_len_ptr as usize;
        let len_end = len_start + 4;
        if len_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[len_start..len_end].copy_from_slice(&(val_bytes.len() as u32).to_le_bytes());

        OrionWasmResult::Ok.into() // OK
    } else {
        OrionWasmResult::NotFound.into() // Not found
    }
}

fn orion_get_request_body(
    mut caller: Caller<'_, WasmState>,
    _request_handle: u64,
    body_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let body_bytes = match caller.data().buffered_body.as_ref() {
        Some(b) => b.clone(),
        None => return OrionWasmResult::NotFound.into(),
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
    _request_handle: u64,
    status_code: u32,
    body_ptr: u32,
    body_len: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

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

    caller.data_mut().direct_response = Some(response);

    OrionWasmResult::Ok.into()
}

fn orion_get_response_header(
    mut caller: Caller<'_, WasmState>,
    response_handle: u64,
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

    let response = unsafe { &*(response_handle as *const Response<OrionResponseBody>) };

    if let Some(val) = response.headers().get(name.as_str()) {
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

fn orion_get_response_body(
    mut caller: Caller<'_, WasmState>,
    _response_handle: u64,
    body_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let body_bytes = match caller.data().buffered_response_body.as_ref() {
        Some(b) => b.clone(),
        None => return OrionWasmResult::NotFound.into(),
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

pub fn register_hostcalls(linker: &mut Linker<WasmState>) -> Result<(), wasmtime::Error> {
    linker.func_wrap("env", "orion_get_request_header", orion_get_request_header)?;
    linker.func_wrap("env", "orion_send_direct_response", orion_send_direct_response)?;
    linker.func_wrap("env", "orion_get_request_body", orion_get_request_body)?;
    linker.func_wrap("env", "orion_get_response_header", orion_get_response_header)?;
    linker.func_wrap("env", "orion_get_response_body", orion_get_response_body)?;
    Ok(())
}
