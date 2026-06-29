use super::types::OrionWasmResult;
use crate::OrionRequestBody;
use http::{Request, Response};
use crate::OrionResponseBody;
use bytes::Bytes;
use http_body_util::Full;
use crate::body::timeout_body::TimeoutBody;
use http::StatusCode;
use smol_str::ToSmolStr;
use wasmtime::{Caller, Linker};

pub struct WasmState {
    pub request: Option<*mut Request<OrionRequestBody>>,
    pub direct_response: Option<Response<OrionResponseBody>>,
}

pub fn register_hostcalls(linker: &mut Linker<WasmState>) -> Result<(), wasmtime::Error> {
    linker.func_wrap(
        "env",
        "orion_send_direct_response",
        |mut caller: Caller<'_, WasmState>, status_code: u32, body_ptr: u32, body_len: u32| -> i32 {
            let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
                Some(mem) => mem,
                None => return OrionWasmResult::InvalidMemoryAccess as i32,
            };

            let body_bytes = if body_len > 0 {
                let data = memory.data(&caller);
                let start = body_ptr as usize;
                let end = start + body_len as usize;
                if end > data.len() {
                    return OrionWasmResult::InvalidMemoryAccess as i32;
                }
                Bytes::copy_from_slice(&data[start..end])
            } else {
                Bytes::new()
            };

            let status = match StatusCode::from_u16(status_code as u16) {
                Ok(s) => s,
                Err(_) => return OrionWasmResult::InternalError as i32,
            };

            let mut response = Response::new(TimeoutBody::new(None, Full::from(body_bytes).into()));
            *response.status_mut() = status;
            
            // Assume HTTP/1.1 for now, but we'll overwrite it in the apply_request
            
            caller.data_mut().direct_response = Some(response);
            
            OrionWasmResult::Ok as i32
        },
    )?;

    linker.func_wrap(
        "env",
        "orion_get_request_header",
        |mut caller: Caller<'_, WasmState>,
         name_ptr: u32,
         name_len: u32,
         value_ptr: u32,
         value_max_len: u32,
         written_len_ptr: u32|
         -> i32 {
            let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
                Some(mem) => mem,
                None => return OrionWasmResult::InvalidMemoryAccess as i32, // Memory not found
            };

            let name = {
                let data = memory.data(&caller);
                let start = name_ptr as usize;
                let end = start + name_len as usize;
                if end > data.len() {
                    return OrionWasmResult::InvalidMemoryAccess as i32;
                }
                match std::str::from_utf8(&data[start..end]) {
                    Ok(s) => s.to_smolstr(),
                    Err(_) => return OrionWasmResult::InvalidMemoryAccess as i32,
                }
            };

            let request_ptr = match caller.data().request {
                Some(ptr) => ptr,
                None => return OrionWasmResult::InvalidMemoryAccess as i32, // No request in context
            };

            let request = unsafe { &*request_ptr };

            if let Some(val) = request.headers().get(name.as_str()) {
                let val_bytes = val.as_bytes();
                if val_bytes.len() > value_max_len as usize {
                    return OrionWasmResult::BufferTooSmall as i32; // Buffer too small
                }

                let data = memory.data_mut(&mut caller);

                // Write the value
                let val_start = value_ptr as usize;
                let val_end = val_start + val_bytes.len();
                if val_end > data.len() {
                    return OrionWasmResult::InvalidMemoryAccess as i32;
                }
                data[val_start..val_end].copy_from_slice(val_bytes);

                // Write the written length
                let len_start = written_len_ptr as usize;
                let len_end = len_start + 4;
                if len_end > data.len() {
                    return OrionWasmResult::InvalidMemoryAccess as i32;
                }
                data[len_start..len_end].copy_from_slice(&(val_bytes.len() as u32).to_le_bytes());

                OrionWasmResult::Ok as i32 // OK
            } else {
                OrionWasmResult::NotFound as i32 // Not found
            }
        },
    )?;

    Ok(())
}
