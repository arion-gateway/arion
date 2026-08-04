#![allow(clippy::similar_names)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::undocumented_unsafe_blocks)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::too_many_lines)]

use std::sync::LazyLock;

use crate::body::timeout_body::TimeoutBody;
use crate::listeners::metadata::DownstreamMetadata;
use crate::OrionRequestBody;
use crate::OrionResponseBody;
use bytes::Bytes;

use http::{Request, Response};
use http_body_util::Full;
use smol_str::{SmolStr, ToSmolStr};
use wasmtime::{Caller, Linker};

use orion_wasm_types::{
    CalloutRequest, CalloutResponse, GrpcCalloutRequest, GrpcCalloutResponse, HeaderMutation, LogLevel, OrionWasmError,
};
pub struct WasmState {
    pub name: &'static str,
    pub plugin_config: Option<String>,
    pub direct_response: Option<Response<OrionResponseBody>>,
    pub buffered_request_body: Option<bytes::Bytes>,
    pub buffered_response_body: Option<bytes::Bytes>,
    pub request_trailers: Option<http::HeaderMap>,
    pub response_trailers: Option<http::HeaderMap>,
    pub access_log_operators: Vec<(SmolStr, SmolStr)>,
    pub io_deadline: Option<std::time::Instant>,
    pub shared_memory: std::sync::Arc<super::shared::SharedMemory>,
    pub active_request_handle: Option<u64>,
    pub active_response_handle: Option<u64>,
}

trait IntoWasmAbi {
    fn into_wasm_abi(self) -> i32;
}

impl<T> IntoWasmAbi for Result<T, OrionWasmError> {
    #[inline]
    fn into_wasm_abi(self) -> i32 {
        match self {
            Ok(_) => 0,
            Err(e) => e.into(),
        }
    }
}

fn orion_get_header(
    mut caller: Caller<'_, WasmState>,
    is_trailer: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let name = {
        let data = memory.data(&caller);
        let start = name_ptr as usize;
        let end = start + name_len as usize;
        match data.get(start..end).and_then(|s| std::str::from_utf8(s).ok()) {
            Some(s) => s.to_smolstr(),
            None => return OrionWasmError::InvalidMemoryAccess.into(),
        }
    };

    let (data, state) = memory.data_and_store_mut(&mut caller);

    let val_bytes = if is_trailer == 0 {
        if let Some(req_ptr) = state.active_request_handle {
            let request = unsafe { &*(req_ptr as *const Request<OrionRequestBody>) };
            request.headers().get(name.as_str())
        } else if let Some(res_ptr) = state.active_response_handle {
            let response = unsafe { &*(res_ptr as *const Response<OrionResponseBody>) };
            response.headers().get(name.as_str())
        } else {
            return OrionWasmError::InternalError.into();
        }
    } else {
        if state.active_request_handle.is_some() {
            state.request_trailers.as_ref().and_then(|t| t.get(name.as_str()))
        } else if state.active_response_handle.is_some() {
            state.response_trailers.as_ref().and_then(|t| t.get(name.as_str()))
        } else {
            return OrionWasmError::InternalError.into();
        }
    }
    .map(|v| v.as_bytes());

    if let Some(val_bytes) = val_bytes {
        if val_bytes.len() > value_max_len as usize {
            return OrionWasmError::BufferTooSmall.into();
        }

        let val_start = value_ptr as usize;
        let val_end = val_start + val_bytes.len();
        if val_end > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(val_start..val_end) {
            slice.copy_from_slice(val_bytes);
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        let len_start = written_len_ptr as usize;
        let len_end = len_start + 4;
        if len_end > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(len_start..len_end) {
            slice.copy_from_slice(&(u32::try_from(val_bytes.len()).unwrap_or(0)).to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        0
    } else {
        OrionWasmError::NotFound.into()
    }
}

fn orion_get_plugin_config(
    mut caller: Caller<'_, WasmState>,
    config_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let (data, state) = memory.data_and_store_mut(&mut caller);

    let config_bytes = match state.plugin_config.as_ref() {
        Some(s) => s.as_bytes(),
        None => return OrionWasmError::NotFound.into(),
    };

    if config_bytes.len() > max_len as usize {
        return OrionWasmError::BufferTooSmall.into();
    }
    let start = config_ptr as usize;
    let end = start + config_bytes.len();
    if end > data.len() {
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(config_bytes);
    } else {
        tracing::error!("Invalid memory index");
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(config_bytes.len()).unwrap_or(0)).to_le_bytes());
    } else {
        tracing::error!("Invalid memory index");
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn orion_get_body(
    mut caller: Caller<'_, WasmState>,
    _is_trailer: u32,
    body_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let (data, state) = memory.data_and_store_mut(&mut caller);

    let body_bytes = if state.active_request_handle.is_some() {
        match state.buffered_request_body.as_ref() {
            Some(b) => b.as_ref(),
            None => return OrionWasmError::NotFound.into(),
        }
    } else if state.active_response_handle.is_some() {
        match state.buffered_response_body.as_ref() {
            Some(b) => b.as_ref(),
            None => return OrionWasmError::NotFound.into(),
        }
    } else {
        return OrionWasmError::InternalError.into();
    };

    if body_bytes.len() > max_len as usize {
        return OrionWasmError::BufferTooSmall.into();
    }

    let start = body_ptr as usize;
    let end = start + body_bytes.len();
    if end > data.len() {
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(&body_bytes);
    } else {
        tracing::error!("Invalid memory index");
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(body_bytes.len()).unwrap_or(0)).to_le_bytes());
    } else {
        tracing::error!("Invalid memory index");
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn orion_set_body(mut caller: Caller<'_, WasmState>, _is_trailer: u32, body_ptr: u32, body_len: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let body_bytes = if body_len > 0 {
        let data = memory.data(&caller);
        let start = body_ptr as usize;
        let end = start + body_len as usize;
        match data.get(start..end) {
            Some(s) => Bytes::copy_from_slice(s),
            None => return OrionWasmError::InvalidMemoryAccess.into(),
        }
    } else {
        Bytes::new()
    };

    if caller.data().active_request_handle.is_some() {
        caller.data_mut().buffered_request_body = Some(body_bytes);
    } else if caller.data().active_response_handle.is_some() {
        caller.data_mut().buffered_response_body = Some(body_bytes);
    } else {
        return OrionWasmError::InternalError.into();
    }

    0
}

fn orion_send_direct_response(mut caller: Caller<'_, WasmState>, resp_ptr: u32, resp_len: u32) -> i32 {
    use crate::body::poly_body::PolyBody;

    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = resp_ptr as usize;
    let end = start + resp_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return OrionWasmError::InvalidMemoryAccess.into(),
    };

    let direct_resp = match bincode_next::serde::decode_from_slice::<orion_wasm_types::DirectResponse, _>(
        slice,
        bincode_next::config::standard(),
    ) {
        Ok((r, _)) => r,
        Err(_) => return OrionWasmError::InternalError.into(),
    };

    let (parts, body) = direct_resp.response.into_parts();
    let response = Response::from_parts(parts, TimeoutBody::new(None, PolyBody::from(Full::from(body))));

    caller.data_mut().direct_response = Some(response);

    0
}

#[allow(unused_variables)]
fn orion_set_custom_metrics(mut caller: Caller<'_, WasmState>, buffer_ptr: u32, buffer_len: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    #[cfg(feature = "metrics")]
    {
        let data = memory.data(&caller);
        let start = buffer_ptr as usize;
        let end = start + buffer_len as usize;
        let buf = match data.get(start..end) {
            Some(s) => s,
            None => return OrionWasmError::InvalidMemoryAccess.into(),
        };

        let pairs = match bincode_next::serde::decode_borrowed_from_slice::<Vec<(&str, &str)>, _>(
            buf,
            bincode_next::config::standard(),
        ) {
            Ok((p, _)) => p,
            Err(_) => return OrionWasmError::InvalidMemoryAccess.into(),
        };

        if let Some(custom_metrics) = orion_metrics::metrics::custom::CUSTOM_METRICS.get() {
            let mut kv = orion_metrics::key_value::KeyValueMap::default();
            for (k, v) in &pairs {
                kv.insert(k, v);
            }
            custom_metrics.with_key_value(orion_metrics::metrics::custom::MetricsHook::Wasm, &kv, &[]);
        }
        0
    }
    #[cfg(not(feature = "metrics"))]
    {
        OrionWasmError::InternalError.into()
    }
}

fn orion_set_access_log_operators(mut caller: Caller<'_, WasmState>, buffer_ptr: u32, buffer_len: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = buffer_ptr as usize;
    let end = start + buffer_len as usize;
    let buf = match data.get(start..end) {
        Some(s) => s,
        None => return OrionWasmError::InvalidMemoryAccess.into(),
    };

    let operators = match bincode_next::serde::decode_from_slice::<Vec<(SmolStr, SmolStr)>, _>(
        buf,
        bincode_next::config::standard(),
    ) {
        Ok((ops, _)) => ops,
        Err(_) => return OrionWasmError::InvalidMemoryAccess.into(),
    };

    caller.data_mut().access_log_operators.extend(operators);
    0
}

fn orion_log(mut caller: Caller<'_, WasmState>, level: u32, msg_ptr: u32, msg_len: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = msg_ptr as usize;
    let end = start + msg_len as usize;

    let msg = match data.get(start..end).and_then(|s| std::str::from_utf8(s).ok()) {
        Some(s) => s,
        None => return OrionWasmError::InvalidMemoryAccess.into(),
    };

    let name = caller.data().name;
    match LogLevel::try_from(level) {
        Ok(LogLevel::Error) => tracing::error!(target: "wasm", "{name}: {msg}"),
        Ok(LogLevel::Warn) => tracing::warn!(target:  "wasm", "{name}: {msg}"),
        Ok(LogLevel::Info) => tracing::info!(target:  "wasm", "{name}: {msg}"),
        Ok(LogLevel::Debug) => tracing::debug!(target: "wasm", "{name}: {msg}"),
        Ok(LogLevel::Trace) | Err(_) => tracing::trace!(target: "wasm", "{name}: {msg}"),
    }

    0
}

use http::HeaderMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// HeaderMap serde wrappers

struct SerHeaderMap<'a>(&'a HeaderMap);

impl Serialize for SerHeaderMap<'_> {
    #[inline]
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        http_serde_ext::header_map::serialize(self.0, serializer)
    }
}

struct DeHeaderMap(HeaderMap);

impl<'de> Deserialize<'de> for DeHeaderMap {
    #[inline]
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        http_serde_ext::header_map::deserialize(deserializer).map(DeHeaderMap)
    }
}

fn orion_get_headers_map(
    mut caller: Caller<'_, WasmState>,
    is_trailer: u32,
    buf_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    static EMPTY_MAP: LazyLock<http::HeaderMap> = LazyLock::new(http::HeaderMap::new);

    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let headers = if is_trailer == 0 {
        if let Some(req_ptr) = caller.data().active_request_handle {
            let request = unsafe { &*(req_ptr as *const Request<OrionRequestBody>) };
            request.headers()
        } else if let Some(res_ptr) = caller.data().active_response_handle {
            let response = unsafe { &*(res_ptr as *const Response<OrionResponseBody>) };
            response.headers()
        } else {
            return OrionWasmError::InternalError.into();
        }
    } else {
        if caller.data().active_request_handle.is_some() {
            caller.data().request_trailers.as_ref().unwrap_or(&EMPTY_MAP)
        } else if caller.data().active_response_handle.is_some() {
            caller.data().response_trailers.as_ref().unwrap_or(&EMPTY_MAP)
        } else {
            return OrionWasmError::InternalError.into();
        }
    };

    let serialized = match bincode_next::serde::encode_to_vec(SerHeaderMap(headers), bincode_next::config::standard()) {
        Ok(b) => b,
        Err(_) => return OrionWasmError::InternalError.into(),
    };

    if serialized.len() > max_len as usize {
        return OrionWasmError::BufferTooSmall.into();
    }
    let data = memory.data_mut(&mut caller);
    let start = buf_ptr as usize;
    let end = start + serialized.len();
    if end > data.len() {
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(&serialized);
    } else {
        tracing::error!("Invalid memory index");
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(serialized.len()).unwrap_or(0)).to_le_bytes());
    } else {
        tracing::error!("Invalid memory index");
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    0
}

fn orion_set_headers_map(mut caller: Caller<'_, WasmState>, is_trailer: u32, buf_ptr: u32, buf_len: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };
    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return OrionWasmError::InvalidMemoryAccess.into(),
    };
    let headers =
        match bincode_next::serde::decode_from_slice::<DeHeaderMap, _>(slice, bincode_next::config::standard()) {
            Ok((DeHeaderMap(h), _)) => h,
            Err(_) => return OrionWasmError::InternalError.into(),
        };

    if is_trailer == 0 {
        if let Some(req_ptr) = caller.data().active_request_handle {
            let request = unsafe { &mut *(req_ptr as *mut Request<OrionRequestBody>) };
            *request.headers_mut() = headers;
        } else if let Some(res_ptr) = caller.data().active_response_handle {
            let response = unsafe { &mut *(res_ptr as *mut Response<OrionResponseBody>) };
            *response.headers_mut() = headers;
        } else {
            return OrionWasmError::InternalError.into();
        }
    } else {
        if caller.data().active_request_handle.is_some() {
            caller.data_mut().request_trailers = Some(headers);
        } else if caller.data().active_response_handle.is_some() {
            caller.data_mut().response_trailers = Some(headers);
        } else {
            return OrionWasmError::InternalError.into();
        }
    }

    0
}

fn orion_get_uri(mut caller: Caller<'_, WasmState>, buf_ptr: u32, max_len: u32, written_len_ptr: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let uri = if let Some(req_ptr) = caller.data().active_request_handle {
        let request = unsafe { &*(req_ptr as *const Request<OrionRequestBody>) };
        request.uri().clone()
    } else {
        return OrionWasmError::InternalError.into();
    };

    let wasm_uri = orion_wasm_types::WasmUri { uri };
    let serialized = match bincode_next::serde::encode_to_vec(&wasm_uri, bincode_next::config::standard()) {
        Ok(b) => b,
        Err(_) => return OrionWasmError::InternalError.into(),
    };

    if serialized.len() > max_len as usize {
        return OrionWasmError::BufferTooSmall.into();
    }
    let data = memory.data_mut(&mut caller);
    let start = buf_ptr as usize;
    let end = start + serialized.len();
    if end > data.len() {
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(&serialized);
    } else {
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(serialized.len()).unwrap_or(0)).to_le_bytes());
    } else {
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn orion_set_uri(mut caller: Caller<'_, WasmState>, buf_ptr: u32, buf_len: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return OrionWasmError::InvalidMemoryAccess.into(),
    };

    let wasm_uri = match bincode_next::serde::decode_from_slice::<orion_wasm_types::WasmUri, _>(
        slice,
        bincode_next::config::standard(),
    ) {
        Ok((u, _)) => u,
        Err(_) => return OrionWasmError::InternalError.into(),
    };

    if let Some(req_ptr) = caller.data().active_request_handle {
        let request = unsafe { &mut *(req_ptr as *mut Request<OrionRequestBody>) };
        *request.uri_mut() = wasm_uri.uri;
        0
    } else {
        OrionWasmError::InternalError.into()
    }
}

fn orion_get_status_code(mut caller: Caller<'_, WasmState>, out_status_ptr: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let status_code = if let Some(res_ptr) = caller.data().active_response_handle {
        let response = unsafe { &*(res_ptr as *const Response<OrionResponseBody>) };
        response.status().as_u16() as u32
    } else {
        return OrionWasmError::InternalError.into();
    };

    let data = memory.data_mut(&mut caller);
    let start = out_status_ptr as usize;
    let end = start + 4;
    if end > data.len() {
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(&status_code.to_le_bytes());
    } else {
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn orion_set_status_code(mut caller: Caller<'_, WasmState>, status_code: u32) -> i32 {
    if let Some(res_ptr) = caller.data().active_response_handle {
        if let Ok(code) = u16::try_from(status_code) {
            if let Ok(status) = http::StatusCode::from_u16(code) {
                let response = unsafe { &mut *(res_ptr as *mut Response<OrionResponseBody>) };
                *response.status_mut() = status;
                return 0;
            }
        }
        OrionWasmError::InternalError.into()
    } else {
        OrionWasmError::InternalError.into()
    }
}

fn get_name_value_from_memory(
    caller: &mut Caller<'_, WasmState>,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> Result<(http::header::HeaderName, http::header::HeaderValue), OrionWasmError> {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return Err(OrionWasmError::InvalidMemoryAccess);
    };
    let data = memory.data(caller);

    let n_start = name_ptr as usize;
    let n_end = n_start + name_len as usize;
    let name_str = std::str::from_utf8(data.get(n_start..n_end).ok_or(OrionWasmError::InvalidMemoryAccess)?)
        .map_err(|_e| OrionWasmError::InvalidMemoryAccess)?;
    let name = http::header::HeaderName::try_from(name_str).map_err(|_e| OrionWasmError::InternalError)?;

    let v_start = value_ptr as usize;
    let v_end = v_start + value_len as usize;
    let value =
        http::header::HeaderValue::from_bytes(data.get(v_start..v_end).ok_or(OrionWasmError::InvalidMemoryAccess)?)
            .map_err(|_e| OrionWasmError::InternalError)?;

    Ok((name, value))
}

fn get_name_from_memory(
    caller: &mut Caller<'_, WasmState>,
    name_ptr: u32,
    name_len: u32,
) -> Result<http::header::HeaderName, OrionWasmError> {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return Err(OrionWasmError::InvalidMemoryAccess);
    };
    let data = memory.data(caller);

    let n_start = name_ptr as usize;
    let n_end = n_start + name_len as usize;
    let name_str = std::str::from_utf8(data.get(n_start..n_end).ok_or(OrionWasmError::InvalidMemoryAccess)?)
        .map_err(|_e| OrionWasmError::InvalidMemoryAccess)?;
    http::header::HeaderName::try_from(name_str).map_err(|_e| OrionWasmError::InternalError)
}

fn orion_set_header(
    mut caller: Caller<'_, WasmState>,
    is_trailer: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    let mut inner = || -> Result<(), OrionWasmError> {
        let (name, value) = get_name_value_from_memory(&mut caller, name_ptr, name_len, value_ptr, value_len)?;

        if is_trailer == 0 {
            if let Some(req_ptr) = caller.data().active_request_handle {
                let request = unsafe { &mut *(req_ptr as *mut Request<OrionRequestBody>) };
                request.headers_mut().insert(name, value);
            } else if let Some(res_ptr) = caller.data().active_response_handle {
                let response = unsafe { &mut *(res_ptr as *mut Response<OrionResponseBody>) };
                response.headers_mut().insert(name, value);
            } else {
                return Err(OrionWasmError::InternalError);
            }
        } else {
            if caller.data().active_request_handle.is_some() {
                let trailers = caller.data_mut().request_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.insert(name, value);
            } else if caller.data().active_response_handle.is_some() {
                let trailers = caller.data_mut().response_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.insert(name, value);
            } else {
                return Err(OrionWasmError::InternalError);
            }
        }

        Ok(())
    };

    inner().into_wasm_abi()
}

fn orion_add_header(
    mut caller: Caller<'_, WasmState>,
    is_trailer: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    let mut inner = || -> Result<(), OrionWasmError> {
        let (name, value) = get_name_value_from_memory(&mut caller, name_ptr, name_len, value_ptr, value_len)?;

        if is_trailer == 0 {
            if let Some(req_ptr) = caller.data().active_request_handle {
                let request = unsafe { &mut *(req_ptr as *mut Request<OrionRequestBody>) };
                request.headers_mut().append(name, value);
            } else if let Some(res_ptr) = caller.data().active_response_handle {
                let response = unsafe { &mut *(res_ptr as *mut Response<OrionResponseBody>) };
                response.headers_mut().append(name, value);
            } else {
                return Err(OrionWasmError::InternalError);
            }
        } else {
            if caller.data().active_request_handle.is_some() {
                let trailers = caller.data_mut().request_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.append(name, value);
            } else if caller.data().active_response_handle.is_some() {
                let trailers = caller.data_mut().response_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.append(name, value);
            } else {
                return Err(OrionWasmError::InternalError);
            }
        }

        Ok(())
    };

    inner().into_wasm_abi()
}

fn orion_remove_header(mut caller: Caller<'_, WasmState>, is_trailer: u32, name_ptr: u32, name_len: u32) -> i32 {
    let mut inner = || -> Result<(), OrionWasmError> {
        let name = get_name_from_memory(&mut caller, name_ptr, name_len)?;

        if is_trailer == 0 {
            if let Some(req_ptr) = caller.data().active_request_handle {
                let request = unsafe { &mut *(req_ptr as *mut Request<OrionRequestBody>) };
                request.headers_mut().remove(&name);
            } else if let Some(res_ptr) = caller.data().active_response_handle {
                let response = unsafe { &mut *(res_ptr as *mut Response<OrionResponseBody>) };
                response.headers_mut().remove(&name);
            } else {
                return Err(OrionWasmError::InternalError);
            }
        } else {
            if caller.data().active_request_handle.is_some() {
                if let Some(trailers) = caller.data_mut().request_trailers.as_mut() {
                    trailers.remove(&name);
                }
            } else if caller.data().active_response_handle.is_some() {
                if let Some(trailers) = caller.data_mut().response_trailers.as_mut() {
                    trailers.remove(&name);
                }
            } else {
                return Err(OrionWasmError::InternalError);
            }
        }

        Ok(())
    };

    inner().into_wasm_abi()
}

fn orion_replace_header(
    mut caller: Caller<'_, WasmState>,
    is_trailer: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    let mut inner = || -> Result<(), OrionWasmError> {
        let (name, value) = get_name_value_from_memory(&mut caller, name_ptr, name_len, value_ptr, value_len)?;

        if is_trailer == 0 {
            if let Some(req_ptr) = caller.data().active_request_handle {
                let request = unsafe { &mut *(req_ptr as *mut Request<OrionRequestBody>) };
                if request.headers().contains_key(&name) {
                    request.headers_mut().insert(name, value);
                }
            } else if let Some(res_ptr) = caller.data().active_response_handle {
                let response = unsafe { &mut *(res_ptr as *mut Response<OrionResponseBody>) };
                if response.headers().contains_key(&name) {
                    response.headers_mut().insert(name, value);
                }
            } else {
                return Err(OrionWasmError::InternalError);
            }
        } else {
            if caller.data().active_request_handle.is_some() {
                let trailers = caller.data_mut().request_trailers.get_or_insert_with(http::HeaderMap::new);
                if trailers.contains_key(&name) {
                    trailers.insert(name, value);
                }
            } else if caller.data().active_response_handle.is_some() {
                let trailers = caller.data_mut().response_trailers.get_or_insert_with(http::HeaderMap::new);
                if trailers.contains_key(&name) {
                    trailers.insert(name, value);
                }
            } else {
                return Err(OrionWasmError::InternalError);
            }
        }

        Ok(())
    };

    inner().into_wasm_abi()
}

fn deserialize_header_mutations(data: &[u8]) -> Option<Vec<HeaderMutation>> {
    bincode_next::serde::decode_from_slice::<Vec<HeaderMutation>, _>(data, bincode_next::config::standard())
        .ok()
        .map(|(mutations, _)| mutations)
}

fn apply_mutations_to_map(map: &mut http::HeaderMap, mutations: Vec<HeaderMutation>) {
    for mutation in mutations {
        match mutation {
            HeaderMutation::Set(name, value) => {
                map.insert(name, value);
            },
            HeaderMutation::Add(name, value) => {
                map.append(name, value);
            },
            HeaderMutation::Replace(name, value) => {
                if map.contains_key(&name) {
                    map.insert(name, value);
                }
            },
            HeaderMutation::Remove(name) => while map.remove(&name).is_some() {},
        }
    }
}

fn orion_apply_header_mutations(mut caller: Caller<'_, WasmState>, is_trailer: u32, buf_ptr: u32, buf_len: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };
    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return OrionWasmError::InvalidMemoryAccess.into(),
    };
    let mutations = match deserialize_header_mutations(slice) {
        Some(m) => m,
        None => return OrionWasmError::InternalError.into(),
    };

    if is_trailer == 0 {
        if let Some(req_ptr) = caller.data().active_request_handle {
            let request = unsafe { &mut *(req_ptr as *mut Request<OrionRequestBody>) };
            apply_mutations_to_map(request.headers_mut(), mutations);
        } else if let Some(res_ptr) = caller.data().active_response_handle {
            let response = unsafe { &mut *(res_ptr as *mut Response<OrionResponseBody>) };
            apply_mutations_to_map(response.headers_mut(), mutations);
        } else {
            return OrionWasmError::InternalError.into();
        }
    } else {
        if caller.data().active_request_handle.is_some() {
            let trailers = caller.data_mut().request_trailers.get_or_insert_with(http::HeaderMap::new);
            apply_mutations_to_map(trailers, mutations);
        } else if caller.data().active_response_handle.is_some() {
            let trailers = caller.data_mut().response_trailers.get_or_insert_with(http::HeaderMap::new);
            apply_mutations_to_map(trailers, mutations);
        } else {
            return OrionWasmError::InternalError.into();
        }
    }

    0
}

use crate::clusters::{clusters_manager, RoutingContext, RoutingPriority};
use http_body_util::BodyExt;
use orion_configuration::config::cluster::ClusterSpecifier;

fn orion_dispatch_http_call(
    mut caller: Caller<'_, WasmState>,
    (req_ptr, req_len, resp_ptr_ptr, resp_len_ptr): (u32, u32, u32, u32),
) -> Box<dyn std::future::Future<Output = i32> + Send + '_> {
    Box::new(async move {
        // 1. Read the serialized request from Wasm memory
        let req_bytes = {
            let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
                return OrionWasmError::InvalidMemoryAccess.into();
            };
            let data = memory.data(&caller);
            let start = req_ptr as usize;
            let end = start + req_len as usize;
            match data.get(start..end) {
                Some(s) => s.to_vec(),
                None => return OrionWasmError::InvalidMemoryAccess.into(),
            }
        };

        let callout_req: CalloutRequest =
            match bincode_next::serde::decode_from_slice(&req_bytes, bincode_next::config::standard()) {
                Ok((req, _)) => req,
                Err(e) => {
                    tracing::error!("Callout deserialization failed: {:?}", e);
                    return OrionWasmError::InternalError.into();
                },
            };

        // 2. Resolve cluster and acquire connection
        let cluster_spec = ClusterSpecifier::Cluster(callout_req.cluster_name.clone());
        let cluster_id = match clusters_manager::resolve_cluster(&cluster_spec, None) {
            Some(id) => id,
            None => return OrionWasmError::NotFound.into(),
        };

        let http_service = match clusters_manager::get_http_connection(cluster_id, RoutingContext::None) {
            Ok(svc) => svc,
            Err(e) => {
                tracing::error!("Callout failed to get HTTP connection: {:?}", e);
                return OrionWasmError::InternalError.into();
            },
        };

        let (mut parts, body_bytes) = callout_req.request.into_parts();

        if !parts.headers.contains_key(http::header::HOST) {
            if let Some(host) = parts.uri.host() {
                if let Ok(host_val) = http::header::HeaderValue::from_str(host) {
                    parts.headers.insert(http::header::HOST, host_val);
                }
            }
        }

        let instrumented = crate::OrionRequestBody::default().map_inner(|_| {
            crate::body::timeout_body::TimeoutBody::new(
                None,
                crate::body::poly_body::PolyBody::from(http_body_util::Full::from(body_bytes)),
            )
        });

        let request = http::Request::from_parts(parts, instrumented);
        let channel = http_service.channel();

        let timeout_duration = if let Some(deadline) = caller.data().io_deadline {
            let now = std::time::Instant::now();
            if now >= deadline {
                return OrionWasmError::Timeout.into();
            }
            Some(deadline.duration_since(now))
        } else {
            None
        };

        #[allow(unused_variables)]
        let clock = quanta::Clock::new();

        // 3. Send async request - this is where the Wasm fiber suspends!
        let request_fut = channel.send_request(
            request,
            None,
            None,
            RoutingPriority::Default,
            None,
            #[cfg(feature = "instrumentation")]
            &clock,
        );

        let response = match timeout_duration {
            Some(duration) => match pingora_timeout::fast_timeout::fast_timeout(duration, request_fut).await {
                Ok(Ok(r)) => r,
                Ok(Err(e)) => {
                    tracing::error!("Callout HTTP request failed: {:?}", e);
                    return OrionWasmError::InternalError.into();
                },
                Err(_) => {
                    return OrionWasmError::Timeout.into();
                },
            },
            None => match request_fut.await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Callout HTTP request failed: {:?}", e);
                    return OrionWasmError::InternalError.into();
                },
            },
        };

        let status = response.status();
        let resp_headers = response.headers().clone();
        let version = response.version();

        let body_bytes = match response.into_body().collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                tracing::error!("Callout failed to collect response body: {:?}", e);
                return OrionWasmError::InternalError.into();
            },
        };

        let mut builder = http::Response::builder().status(status).version(version);
        for (k, v) in resp_headers {
            if let Some(name) = k {
                builder = builder.header(name, v);
            }
        }
        let http_resp = builder.body(body_bytes).unwrap_or_else(|_| http::Response::new(bytes::Bytes::new()));

        let callout_resp = CalloutResponse { response: http_resp };

        let resp_bytes = match bincode_next::serde::encode_to_vec(&callout_resp, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Callout response serialization failed: {:?}", e);
                return OrionWasmError::InternalError.into();
            },
        };

        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let alloc_func = if let Some(func) = caller.get_export("orion_malloc").and_then(wasmtime::Extern::into_func) {
            func
        } else {
            tracing::error!("Callout failed: guest does not export orion_malloc");
            return OrionWasmError::InternalError.into();
        };

        // Call orion_malloc on the guest
        let mut results = [wasmtime::Val::I32(0)];
        if let Err(e) = alloc_func
            .call_async(&mut caller, &[wasmtime::Val::I32(i32::try_from(resp_bytes.len()).unwrap_or(0))], &mut results)
            .await
        {
            tracing::error!("Callout failed to call orion_malloc: {:?}", e);
            return OrionWasmError::InternalError.into();
        }

        let resp_ptr = match results[0] {
            wasmtime::Val::I32(ptr) => u32::try_from(ptr).unwrap_or(0),
            _ => return OrionWasmError::InternalError.into(),
        };

        let data = memory.data_mut(&mut caller);
        let rb_start = resp_ptr as usize;
        let rb_end = rb_start + resp_bytes.len();
        if rb_end > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(rb_start..rb_end) {
            slice.copy_from_slice(&resp_bytes);
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        // Write the pointer and length back to the guest
        let ptr_start = resp_ptr_ptr as usize;
        let ptr_end = ptr_start + 4;
        if ptr_end > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(ptr_start..ptr_end) {
            slice.copy_from_slice(&resp_ptr.to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        let rl_start = resp_len_ptr as usize;
        let rl_end = rl_start + 4;
        if rl_end > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(rl_start..rl_end) {
            slice.copy_from_slice(&(u32::try_from(resp_bytes.len()).unwrap_or(0)).to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        0
    })
}

struct RawBytesCodec;

impl tonic::codec::Codec for RawBytesCodec {
    type Encode = bytes::Bytes;
    type Decode = bytes::Bytes;
    type Encoder = RawBytesEncoder;
    type Decoder = RawBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder {
        RawBytesEncoder
    }
    fn decoder(&mut self) -> Self::Decoder {
        RawBytesDecoder
    }
}

struct RawBytesEncoder;
impl tonic::codec::Encoder for RawBytesEncoder {
    type Item = bytes::Bytes;
    type Error = tonic::Status;

    fn encode(&mut self, item: Self::Item, dst: &mut tonic::codec::EncodeBuf<'_>) -> Result<(), Self::Error> {
        bytes::BufMut::put_slice(dst, &item);
        Ok(())
    }
}

struct RawBytesDecoder;
impl tonic::codec::Decoder for RawBytesDecoder {
    type Item = bytes::Bytes;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut tonic::codec::DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        use bytes::Buf;
        if !src.has_remaining() {
            return Ok(None);
        }
        let bytes = src.copy_to_bytes(src.remaining());
        Ok(Some(bytes))
    }
}

fn orion_dispatch_grpc_call(
    mut caller: Caller<'_, WasmState>,
    (req_ptr, req_len, resp_ptr_ptr, resp_len_ptr): (u32, u32, u32, u32),
) -> Box<dyn std::future::Future<Output = i32> + Send + '_> {
    Box::new(async move {
        let req_bytes = {
            let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
                return OrionWasmError::InvalidMemoryAccess.into();
            };
            let data = memory.data(&caller);
            let start = req_ptr as usize;
            let end = start + req_len as usize;
            match data.get(start..end) {
                Some(s) => s.to_vec(),
                None => return OrionWasmError::InvalidMemoryAccess.into(),
            }
        };

        let callout_req: GrpcCalloutRequest =
            match bincode_next::serde::decode_from_slice(&req_bytes, bincode_next::config::standard()) {
                Ok((req, _)) => req,
                Err(e) => {
                    tracing::error!("gRPC Callout deserialization failed: {:?}", e);
                    return OrionWasmError::InternalError.into();
                },
            };

        let cluster_spec = ClusterSpecifier::Cluster(callout_req.cluster_name.clone());
        let cluster_id = match clusters_manager::resolve_cluster(&cluster_spec, None) {
            Some(id) => id,
            None => return OrionWasmError::NotFound.into(),
        };

        let grpc_service = match clusters_manager::get_grpc_connection(cluster_id, RoutingContext::None) {
            Ok(svc) => svc,
            Err(e) => {
                tracing::error!("gRPC Callout failed to get connection: {:?}", e);
                return OrionWasmError::InternalError.into();
            },
        };

        let mut client = tonic::client::Grpc::new(grpc_service);
        let mut path_str = String::with_capacity(2 + callout_req.service_name.len() + callout_req.method_name.len());
        path_str.push('/');
        path_str.push_str(&callout_req.service_name);
        path_str.push('/');
        path_str.push_str(&callout_req.method_name);

        let path = match http::uri::PathAndQuery::try_from(path_str) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("gRPC Callout invalid path: {:?}", e);
                return OrionWasmError::InternalError.into();
            },
        };

        let mut grpc_req = tonic::Request::new(callout_req.message);
        for (k, v) in callout_req.initial_metadata {
            if let Ok(metadata_name) = tonic::metadata::MetadataKey::from_bytes(k.as_bytes()) {
                if let Ok(metadata_value) = tonic::metadata::MetadataValue::try_from(v.as_bytes()) {
                    grpc_req.metadata_mut().insert(metadata_name, metadata_value);
                }
            }
        }

        let timeout_duration = if let Some(deadline) = caller.data().io_deadline {
            let now = std::time::Instant::now();
            if now >= deadline {
                return OrionWasmError::Timeout.into();
            }
            Some(deadline.duration_since(now))
        } else {
            None
        };

        if let Some(duration) = timeout_duration {
            grpc_req.set_timeout(duration);
        }

        let request_fut = client.unary(grpc_req, path, RawBytesCodec);

        let response_res = match timeout_duration {
            Some(duration) => match pingora_timeout::fast_timeout::fast_timeout(duration, request_fut).await {
                Ok(Ok(r)) => Ok(r),
                Ok(Err(e)) => Err(e),
                Err(_) => {
                    return OrionWasmError::Timeout.into();
                },
            },
            None => request_fut.await,
        };

        let callout_resp = match response_res {
            Ok(response) => {
                let mut initial_metadata = Vec::with_capacity(response.metadata().len());
                for kv in response.metadata().iter() {
                    match kv {
                        tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                            initial_metadata.push((k.as_str().into(), v.to_str().unwrap_or("").into()));
                        },
                        tonic::metadata::KeyAndValueRef::Binary(k, v) => {
                            use base64::prelude::*;
                            initial_metadata.push((
                                k.as_str().into(),
                                BASE64_STANDARD.encode(v.to_bytes().unwrap_or_default()).into(),
                            ));
                        },
                    }
                }

                GrpcCalloutResponse {
                    initial_metadata,
                    message: response.into_inner(),
                    trailing_metadata: Vec::new(),
                    status: 0,
                    status_message: "".into(),
                }
            },
            Err(status) => {
                let mut trailing_metadata = Vec::with_capacity(status.metadata().len());
                for kv in status.metadata().iter() {
                    match kv {
                        tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                            trailing_metadata.push((k.as_str().into(), v.to_str().unwrap_or("").into()));
                        },
                        tonic::metadata::KeyAndValueRef::Binary(k, v) => {
                            use base64::prelude::*;
                            trailing_metadata.push((
                                k.as_str().into(),
                                BASE64_STANDARD.encode(v.to_bytes().unwrap_or_default()).into(),
                            ));
                        },
                    }
                }
                GrpcCalloutResponse {
                    initial_metadata: Vec::new(),
                    message: bytes::Bytes::new(),
                    trailing_metadata,
                    status: status.code() as u32,
                    status_message: status.message().into(),
                }
            },
        };

        let resp_bytes = match bincode_next::serde::encode_to_vec(&callout_resp, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("gRPC Callout response serialization failed: {:?}", e);
                return OrionWasmError::InternalError.into();
            },
        };

        let memory = if let Some(mem) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) {
            mem
        } else {
            tracing::error!("gRPC Callout failed: guest does not export memory");
            return OrionWasmError::InvalidMemoryAccess.into();
        };
        let alloc_func = if let Some(func) = caller.get_export("orion_malloc").and_then(wasmtime::Extern::into_func) {
            func
        } else {
            tracing::error!("gRPC Callout failed: guest does not export orion_malloc");
            return OrionWasmError::InternalError.into();
        };

        let mut results = [wasmtime::Val::I32(0)];
        if let Err(e) = alloc_func
            .call_async(&mut caller, &[wasmtime::Val::I32(i32::try_from(resp_bytes.len()).unwrap_or(0))], &mut results)
            .await
        {
            tracing::error!("gRPC Callout failed to call orion_malloc: {:?}", e);
            return OrionWasmError::InternalError.into();
        }

        let resp_ptr = match results[0] {
            wasmtime::Val::I32(ptr) => u32::try_from(ptr).unwrap_or(0),
            _ => return OrionWasmError::InternalError.into(),
        };

        let data = memory.data_mut(&mut caller);
        let rb_start = resp_ptr as usize;
        let rb_end = rb_start + resp_bytes.len();
        if rb_end > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(rb_start..rb_end) {
            slice.copy_from_slice(&resp_bytes);
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        let ptr_start = resp_ptr_ptr as usize;
        let ptr_end = ptr_start + 4;
        if ptr_end > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(ptr_start..ptr_end) {
            slice.copy_from_slice(&resp_ptr.to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        let rl_start = resp_len_ptr as usize;
        let rl_end = rl_start + 4;
        if rl_end > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(rl_start..rl_end) {
            slice.copy_from_slice(&(u32::try_from(resp_bytes.len()).unwrap_or(0)).to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        0
    })
}

fn orion_set_io_timeout(mut caller: Caller<'_, WasmState>, microseconds: u64) -> i32 {
    let deadline = std::time::Instant::now() + std::time::Duration::from_micros(microseconds);
    caller.data_mut().io_deadline = Some(deadline);
    0
}

fn orion_clear_io_timeout(mut caller: Caller<'_, WasmState>, remaining_us_ptr: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let remaining_us: u64 = if let Some(deadline) = caller.data_mut().io_deadline.take() {
        let now = std::time::Instant::now();
        if deadline > now {
            u64::try_from((deadline - now).as_micros()).unwrap_or(0)
        } else {
            0
        }
    } else {
        0
    };

    let data = memory.data_mut(&mut caller);
    let start = remaining_us_ptr as usize;
    let end = start + 8;
    if end > data.len() {
        return OrionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(&remaining_us.to_le_bytes());
    } else {
        tracing::error!("Invalid memory index");
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn orion_sleep(
    caller: Caller<'_, WasmState>,
    (microseconds,): (u64,),
) -> Box<dyn std::future::Future<Output = i32> + Send + '_> {
    let mut duration = std::time::Duration::from_micros(microseconds);
    let mut is_timeout = false;

    if let Some(deadline) = caller.data().io_deadline {
        let now = std::time::Instant::now();
        if now >= deadline {
            duration = std::time::Duration::ZERO;
            is_timeout = true;
        } else {
            let max_duration = deadline.duration_since(now);
            if duration > max_duration {
                duration = max_duration;
                is_timeout = true;
            }
        }
    }

    Box::new(async move {
        if duration > std::time::Duration::ZERO {
            pingora_timeout::fast_timeout::fast_sleep(duration).await;
        }

        if is_timeout {
            OrionWasmError::Timeout.into()
        } else {
            0
        }
    })
}

fn orion_get_downstream_metadata(
    mut caller: Caller<'_, WasmState>,
    (out_ptr_ptr, out_len_ptr): (u32, u32),
) -> Box<dyn std::future::Future<Output = i32> + Send + '_> {
    Box::new(async move {
        let req_ptr = match caller.data().active_request_handle {
            Some(p) => p,
            None => return OrionWasmError::InternalError.into(),
        };
        let request = unsafe { &*(req_ptr as *const Request<OrionRequestBody>) };

        let host_meta = match request.extensions().get::<Box<DownstreamMetadata>>() {
            Some(m) => m,
            None => return OrionWasmError::NotFound.into(),
        };

        let mapped_connection = match &host_meta.connection {
            crate::listeners::metadata::DownstreamConnectionMetadata::FromSocket { peer_address, local_address } => {
                orion_wasm_types::DownstreamConnectionMetadata::FromSocket {
                    peer_address: *peer_address,
                    local_address: *local_address,
                }
            },
            crate::listeners::metadata::DownstreamConnectionMetadata::FromProxyProtocol {
                original_peer_address,
                original_destination_address,
                protocol,
                tlv_data,
                proxy_peer_address,
                proxy_local_address,
            } => {
                let mapped_protocol = match protocol {
                    ppp::v2::Protocol::Stream => orion_wasm_types::ProxyProtocol::Stream,
                    ppp::v2::Protocol::Datagram => orion_wasm_types::ProxyProtocol::Datagram,
                    ppp::v2::Protocol::Unspecified => orion_wasm_types::ProxyProtocol::Unspec,
                };
                let mut mapped_tlv = std::collections::HashMap::new();
                for (k, v) in tlv_data {
                    let k_u8: u8 = (*k).clone().into();
                    mapped_tlv.insert(k_u8, v.clone());
                }
                orion_wasm_types::DownstreamConnectionMetadata::FromProxyProtocol {
                    original_peer_address: *original_peer_address,
                    original_destination_address: *original_destination_address,
                    protocol: mapped_protocol,
                    tlv_data: mapped_tlv,
                    proxy_peer_address: *proxy_peer_address,
                    proxy_local_address: *proxy_local_address,
                }
            },
        };

        let guest_meta = orion_wasm_types::DownstreamMetadata {
            connection: mapped_connection,
            sni: host_meta.sni.as_ref().map(std::string::ToString::to_string),
            listener_name: host_meta.listener_name.to_owned(),
        };

        let encoded = match bincode_next::serde::encode_to_vec(&guest_meta, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Failed to encode DownstreamMetadata: {:?}", e);
                return OrionWasmError::InternalError.into();
            },
        };

        let Some(alloc_func) = caller.get_export("orion_malloc").and_then(wasmtime::Extern::into_func) else {
            return OrionWasmError::InternalError.into();
        };

        let mut results = [wasmtime::Val::I32(0)];
        if let Err(e) = alloc_func
            .call_async(&mut caller, &[wasmtime::Val::I32(i32::try_from(encoded.len()).unwrap_or(0))], &mut results)
            .await
        {
            tracing::error!("Failed to call orion_malloc async: {:?}", e);
            return OrionWasmError::InternalError.into();
        }

        let allocated_ptr = match results[0] {
            wasmtime::Val::I32(ptr) => u32::try_from(ptr).unwrap_or(0),
            _ => return OrionWasmError::InternalError.into(),
        };

        let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
            return OrionWasmError::InvalidMemoryAccess.into();
        };

        let data = memory.data_mut(&mut caller);
        let start = allocated_ptr as usize;
        let end = start + encoded.len();
        if end > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(start..end) {
            slice.copy_from_slice(&encoded);
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        let ptr_start = out_ptr_ptr as usize;
        if ptr_start + 4 > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(ptr_start..ptr_start + 4) {
            slice.copy_from_slice(&allocated_ptr.to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        let len_start = out_len_ptr as usize;
        if len_start + 4 > data.len() {
            return OrionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(len_start..len_start + 4) {
            slice.copy_from_slice(&(u32::try_from(encoded.len()).unwrap_or(0)).to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return OrionWasmError::InvalidMemoryAccess.into();
        }

        0
    })
}

fn orion_shared_resolve(mut caller: Caller<'_, WasmState>, name_ptr: u32, name_len: u32, var_type: u32) -> u32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return u32::MAX;
    };
    let data = memory.data(&caller);
    let start = name_ptr as usize;
    let end = start + name_len as usize;
    let name = match data.get(start..end).and_then(|s| std::str::from_utf8(s).ok()) {
        Some(s) => s.to_owned(),
        None => return u32::MAX,
    };

    let shared = std::sync::Arc::clone(&caller.data().shared_memory);
    let map = &shared.name_to_id;
    let pin = map.pin();

    if let Some(&var_id) = pin.get(&name) {
        return match (var_id, var_type) {
            (super::shared::VarId::U64(id), 0)
            | (super::shared::VarId::I64(id), 1)
            | (super::shared::VarId::Blob(id), 2) => id,
            _ => u32::MAX,
        };
    }

    let (id, new_var_id) = match var_type {
        0 => {
            let id = shared.next_u64.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if id as usize >= shared.u64_vars.len() {
                return u32::MAX;
            }
            (id, super::shared::VarId::U64(id))
        },
        1 => {
            let id = shared.next_i64.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if id as usize >= shared.i64_vars.len() {
                return u32::MAX;
            }
            (id, super::shared::VarId::I64(id))
        },
        2 => {
            let id = shared.next_blob.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if id as usize >= shared.blob_vars.len() {
                return u32::MAX;
            }
            (id, super::shared::VarId::Blob(id))
        },
        _ => return u32::MAX,
    };

    pin.insert(name, new_var_id);
    id
}

#[inline]
fn convert_ordering(order: u32) -> std::sync::atomic::Ordering {
    match order {
        0 => std::sync::atomic::Ordering::Relaxed,
        1 => std::sync::atomic::Ordering::Release,
        2 => std::sync::atomic::Ordering::Acquire,
        3 => std::sync::atomic::Ordering::AcqRel,
        _ => std::sync::atomic::Ordering::SeqCst,
    }
}

macro_rules! get_shared_atomic {
    ($caller:expr, $field:ident, $id:expr) => {{
        let res = $caller.data().shared_memory.$field.get($id as usize);
        if res.is_none() {
            tracing::error!("Wasm shared variable index out of bounds: id {} on {}", $id, stringify!($field));
        }
        res
    }};
}

macro_rules! get_shared_blob {
    ($shared:expr, $id:expr) => {{
        let res = $shared.blob_vars.get($id as usize);
        if res.is_none() {
            tracing::error!("Wasm shared variable index out of bounds: id {} on blob_vars", $id);
        }
        res
    }};
}

// U64 Hostcalls
fn ext_shared_u64_load(caller: Caller<'_, WasmState>, id: u32, order: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| v.load(convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_u64_store(caller: Caller<'_, WasmState>, id: u32, val: u64, order: u32) {
    if let Some(v) = get_shared_atomic!(caller, u64_vars, id) {
        v.store(val, convert_ordering(order));
    }
}
fn ext_shared_u64_swap(caller: Caller<'_, WasmState>, id: u32, val: u64, order: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| v.swap(val, convert_ordering(order))).unwrap_or(0)
}

#[allow(clippy::too_many_arguments)]
fn ext_shared_u64_compare_exchange(
    caller: Caller<'_, WasmState>,
    id: u32,
    current: u64,
    new: u64,
    succ: u32,
    fail: u32,
) -> u64 {
    get_shared_atomic!(caller, u64_vars, id)
        .map(|v| match v.compare_exchange(current, new, convert_ordering(succ), convert_ordering(fail)) {
            Ok(prev) | Err(prev) => prev,
        })
        .unwrap_or(0)
}
fn ext_shared_u64_fetch_add(caller: Caller<'_, WasmState>, id: u32, val: u64, order: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| v.fetch_add(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_u64_fetch_sub(caller: Caller<'_, WasmState>, id: u32, val: u64, order: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| v.fetch_sub(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_u64_fetch_and(caller: Caller<'_, WasmState>, id: u32, val: u64, order: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| v.fetch_and(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_u64_fetch_nand(caller: Caller<'_, WasmState>, id: u32, val: u64, order: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| v.fetch_nand(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_u64_fetch_or(caller: Caller<'_, WasmState>, id: u32, val: u64, order: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| v.fetch_or(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_u64_fetch_xor(caller: Caller<'_, WasmState>, id: u32, val: u64, order: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| v.fetch_xor(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_u64_fetch_max(caller: Caller<'_, WasmState>, id: u32, val: u64, order: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| v.fetch_max(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_u64_fetch_min(caller: Caller<'_, WasmState>, id: u32, val: u64, order: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| v.fetch_min(val, convert_ordering(order))).unwrap_or(0)
}

// I64 Hostcalls
fn ext_shared_i64_load(caller: Caller<'_, WasmState>, id: u32, order: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| v.load(convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_i64_store(caller: Caller<'_, WasmState>, id: u32, val: i64, order: u32) {
    if let Some(v) = get_shared_atomic!(caller, i64_vars, id) {
        v.store(val, convert_ordering(order));
    }
}
fn ext_shared_i64_swap(caller: Caller<'_, WasmState>, id: u32, val: i64, order: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| v.swap(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_i64_compare_exchange(
    caller: Caller<'_, WasmState>,
    id: u32,
    current: i64,
    new: i64,
    succ: u32,
    fail: u32,
) -> i64 {
    get_shared_atomic!(caller, i64_vars, id)
        .map(|v| match v.compare_exchange(current, new, convert_ordering(succ), convert_ordering(fail)) {
            Ok(prev) | Err(prev) => prev,
        })
        .unwrap_or(0)
}
fn ext_shared_i64_fetch_add(caller: Caller<'_, WasmState>, id: u32, val: i64, order: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| v.fetch_add(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_i64_fetch_sub(caller: Caller<'_, WasmState>, id: u32, val: i64, order: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| v.fetch_sub(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_i64_fetch_and(caller: Caller<'_, WasmState>, id: u32, val: i64, order: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| v.fetch_and(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_i64_fetch_nand(caller: Caller<'_, WasmState>, id: u32, val: i64, order: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| v.fetch_nand(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_i64_fetch_or(caller: Caller<'_, WasmState>, id: u32, val: i64, order: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| v.fetch_or(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_i64_fetch_xor(caller: Caller<'_, WasmState>, id: u32, val: i64, order: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| v.fetch_xor(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_i64_fetch_max(caller: Caller<'_, WasmState>, id: u32, val: i64, order: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| v.fetch_max(val, convert_ordering(order))).unwrap_or(0)
}
fn ext_shared_i64_fetch_min(caller: Caller<'_, WasmState>, id: u32, val: i64, order: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| v.fetch_min(val, convert_ordering(order))).unwrap_or(0)
}

// Blob Hostcalls
fn ext_shared_blob_read(
    mut caller: Caller<'_, WasmState>,
    id: u32,
    buf_ptr: u32,
    buf_len: u32,
    out_version_ptr: u32,
) -> u32 {
    let shared = std::sync::Arc::clone(&caller.data().shared_memory);
    let Some(blob_lock) = get_shared_blob!(shared, id) else {
        return u32::MAX;
    };
    let blob = blob_lock.read();

    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return u32::MAX;
    };
    let data = memory.data_mut(&mut caller);

    let ver_start = out_version_ptr as usize;
    if ver_start + 8 <= data.len() {
        if let Some(slice) = data.get_mut(ver_start..ver_start + 8) {
            slice.copy_from_slice(&blob.version.to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return u32::MAX;
        }
    }

    let actual_len = u32::try_from(blob.data.len()).unwrap_or(0);
    if actual_len <= buf_len {
        let start = buf_ptr as usize;
        if start + actual_len as usize <= data.len() {
            if let Some(slice) = data.get_mut(start..start + actual_len as usize) {
                slice.copy_from_slice(&blob.data);
            } else {
                tracing::error!("Invalid memory index");
                return u32::MAX;
            }
        }
    }
    actual_len
}

fn ext_shared_blob_write(mut caller: Caller<'_, WasmState>, id: u32, buf_ptr: u32, buf_len: u32) -> u64 {
    let shared = std::sync::Arc::clone(&caller.data().shared_memory);
    let Some(blob_lock) = get_shared_blob!(shared, id) else {
        return 0;
    };
    let mut blob = blob_lock.write();

    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return 0;
    };
    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let slice = match data.get(start..start + buf_len as usize) {
        Some(s) => s,
        None => return 0,
    };

    blob.data.clear();
    blob.data.extend_from_slice(slice);
    blob.version += 1;
    blob.version
}

fn ext_shared_blob_cas(
    mut caller: Caller<'_, WasmState>,
    id: u32,
    buf_ptr: u32,
    buf_len: u32,
    expected_version: u64,
    out_success_ptr: u32,
) -> u64 {
    let shared = std::sync::Arc::clone(&caller.data().shared_memory);
    let Some(blob_lock) = get_shared_blob!(shared, id) else {
        return 0;
    };
    let mut blob = blob_lock.write();

    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return 0;
    };

    let success = blob.version == expected_version;
    if success {
        let data = memory.data(&caller);
        let start = buf_ptr as usize;
        if let Some(slice) = data.get(start..start + buf_len as usize) {
            blob.data.clear();
            blob.data.extend_from_slice(slice);
            blob.version += 1;
        }
    }

    let data_mut = memory.data_mut(&mut caller);
    let succ_start = out_success_ptr as usize;
    if let Some(slice) = data_mut.get_mut(succ_start..succ_start + 4) {
        slice.copy_from_slice(&u32::from(success).to_le_bytes());
    }

    blob.version
}

fn orion_get_request(mut caller: Caller<'_, WasmState>, buf_ptr: u32, max_len: u32, written_len_ptr: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let req_ptr = match caller.data().active_request_handle {
        Some(ptr) => ptr,
        None => return OrionWasmError::InternalError.into(),
    };
    let request = unsafe { &*(req_ptr as *const Request<OrionRequestBody>) };

    let body_bytes = caller.data().buffered_request_body.clone().unwrap_or_default();

    let mut builder =
        http::Request::builder().method(request.method().clone()).uri(request.uri().clone()).version(request.version());

    for (k, v) in request.headers() {
        builder = builder.header(k, v);
    }
    let req = match builder.body(body_bytes) {
        Ok(r) => r,
        Err(_) => return OrionWasmError::InternalError.into(),
    };

    let wasm_req = orion_wasm_types::WasmRequest { request: req };
    let serialized = match bincode_next::serde::encode_to_vec(&wasm_req, bincode_next::config::standard()) {
        Ok(b) => b,
        Err(_) => return OrionWasmError::InternalError.into(),
    };

    if serialized.len() > max_len as usize {
        return OrionWasmError::BufferTooSmall.into();
    }

    let data = memory.data_mut(&mut caller);
    let start = buf_ptr as usize;
    let end = start + serialized.len();
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(&serialized);
    } else {
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(serialized.len()).unwrap_or(0)).to_le_bytes());
    } else {
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn orion_set_request(mut caller: Caller<'_, WasmState>, buf_ptr: u32, buf_len: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return OrionWasmError::InvalidMemoryAccess.into(),
    };

    let wasm_req = match bincode_next::serde::decode_from_slice::<orion_wasm_types::WasmRequest, _>(
        slice,
        bincode_next::config::standard(),
    ) {
        Ok((r, _)) => r,
        Err(_) => return OrionWasmError::InternalError.into(),
    };

    let (parts, body) = wasm_req.request.into_parts();

    if let Some(req_ptr) = caller.data().active_request_handle {
        let request = unsafe { &mut *(req_ptr as *mut Request<OrionRequestBody>) };
        *request.method_mut() = parts.method;
        *request.uri_mut() = parts.uri;
        *request.version_mut() = parts.version;
        *request.headers_mut() = parts.headers;

        caller.data_mut().buffered_request_body = Some(body);
        0
    } else {
        OrionWasmError::InternalError.into()
    }
}

fn orion_get_response(mut caller: Caller<'_, WasmState>, buf_ptr: u32, max_len: u32, written_len_ptr: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let res_ptr = match caller.data().active_response_handle {
        Some(ptr) => ptr,
        None => return OrionWasmError::InternalError.into(),
    };
    let response = unsafe { &*(res_ptr as *const Response<OrionResponseBody>) };

    let body_bytes = caller.data().buffered_response_body.clone().unwrap_or_default();

    let mut builder = http::Response::builder().status(response.status()).version(response.version());

    for (k, v) in response.headers() {
        builder = builder.header(k, v);
    }
    let res = match builder.body(body_bytes) {
        Ok(r) => r,
        Err(_) => return OrionWasmError::InternalError.into(),
    };

    let wasm_res = orion_wasm_types::WasmResponse { response: res };
    let serialized = match bincode_next::serde::encode_to_vec(&wasm_res, bincode_next::config::standard()) {
        Ok(b) => b,
        Err(_) => return OrionWasmError::InternalError.into(),
    };

    if serialized.len() > max_len as usize {
        return OrionWasmError::BufferTooSmall.into();
    }

    let data = memory.data_mut(&mut caller);
    let start = buf_ptr as usize;
    let end = start + serialized.len();
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(&serialized);
    } else {
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(serialized.len()).unwrap_or(0)).to_le_bytes());
    } else {
        return OrionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn orion_set_response(mut caller: Caller<'_, WasmState>, buf_ptr: u32, buf_len: u32) -> i32 {
    let Some(memory) = caller.get_export("memory").and_then(wasmtime::Extern::into_memory) else {
        return OrionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return OrionWasmError::InvalidMemoryAccess.into(),
    };

    let wasm_res = match bincode_next::serde::decode_from_slice::<orion_wasm_types::WasmResponse, _>(
        slice,
        bincode_next::config::standard(),
    ) {
        Ok((r, _)) => r,
        Err(_) => return OrionWasmError::InternalError.into(),
    };

    let (parts, body) = wasm_res.response.into_parts();

    if let Some(res_ptr) = caller.data().active_response_handle {
        let response = unsafe { &mut *(res_ptr as *mut Response<OrionResponseBody>) };
        *response.status_mut() = parts.status;
        *response.version_mut() = parts.version;
        *response.headers_mut() = parts.headers;

        caller.data_mut().buffered_response_body = Some(body);
        0
    } else {
        OrionWasmError::InternalError.into()
    }
}

pub fn register_hostcalls(linker: &mut Linker<WasmState>) -> Result<(), wasmtime::Error> {
    linker.func_wrap("env", "orion_get_uri", orion_get_uri)?;
    linker.func_wrap("env", "orion_set_uri", orion_set_uri)?;
    linker.func_wrap("env", "orion_get_status_code", orion_get_status_code)?;
    linker.func_wrap("env", "orion_set_status_code", orion_set_status_code)?;
    linker.func_wrap("env", "orion_get_request", orion_get_request)?;
    linker.func_wrap("env", "orion_set_request", orion_set_request)?;
    linker.func_wrap("env", "orion_get_response", orion_get_response)?;
    linker.func_wrap("env", "orion_set_response", orion_set_response)?;

    linker.func_wrap("env", "orion_get_plugin_config", orion_get_plugin_config)?;
    linker.func_wrap("env", "orion_get_header", orion_get_header)?;
    linker.func_wrap("env", "orion_get_body", orion_get_body)?;
    linker.func_wrap("env", "orion_set_body", orion_set_body)?;
    linker.func_wrap("env", "orion_get_headers_map", orion_get_headers_map)?;
    linker.func_wrap("env", "orion_set_headers_map", orion_set_headers_map)?;

    linker.func_wrap("env", "orion_set_header", orion_set_header)?;
    linker.func_wrap("env", "orion_add_header", orion_add_header)?;
    linker.func_wrap("env", "orion_remove_header", orion_remove_header)?;
    linker.func_wrap("env", "orion_replace_header", orion_replace_header)?;
    linker.func_wrap("env", "orion_apply_header_mutations", orion_apply_header_mutations)?;

    linker.func_wrap("env", "orion_send_direct_response", orion_send_direct_response)?;
    linker.func_wrap_async("env", "orion_get_downstream_metadata", orion_get_downstream_metadata)?;

    linker.func_wrap("env", "orion_set_custom_metrics", orion_set_custom_metrics)?;
    linker.func_wrap("env", "orion_set_access_log_operators", orion_set_access_log_operators)?;
    linker.func_wrap("env", "orion_log", orion_log)?;
    linker.func_wrap("env", "orion_set_io_timeout", orion_set_io_timeout)?;
    linker.func_wrap("env", "orion_clear_io_timeout", orion_clear_io_timeout)?;
    linker.func_wrap_async("env", "orion_dispatch_http_call", orion_dispatch_http_call)?;
    linker.func_wrap_async("env", "orion_dispatch_grpc_call", orion_dispatch_grpc_call)?;
    linker.func_wrap_async("env", "orion_sleep", orion_sleep)?;

    // Shared Memory Hostcalls
    linker.func_wrap("env", "ext_shared_resolve", orion_shared_resolve)?;

    linker.func_wrap("env", "ext_shared_u64_load", ext_shared_u64_load)?;
    linker.func_wrap("env", "ext_shared_u64_store", ext_shared_u64_store)?;
    linker.func_wrap("env", "ext_shared_u64_swap", ext_shared_u64_swap)?;
    linker.func_wrap("env", "ext_shared_u64_compare_exchange", ext_shared_u64_compare_exchange)?;
    linker.func_wrap("env", "ext_shared_u64_fetch_add", ext_shared_u64_fetch_add)?;
    linker.func_wrap("env", "ext_shared_u64_fetch_sub", ext_shared_u64_fetch_sub)?;
    linker.func_wrap("env", "ext_shared_u64_fetch_and", ext_shared_u64_fetch_and)?;
    linker.func_wrap("env", "ext_shared_u64_fetch_nand", ext_shared_u64_fetch_nand)?;
    linker.func_wrap("env", "ext_shared_u64_fetch_or", ext_shared_u64_fetch_or)?;
    linker.func_wrap("env", "ext_shared_u64_fetch_xor", ext_shared_u64_fetch_xor)?;
    linker.func_wrap("env", "ext_shared_u64_fetch_max", ext_shared_u64_fetch_max)?;
    linker.func_wrap("env", "ext_shared_u64_fetch_min", ext_shared_u64_fetch_min)?;

    linker.func_wrap("env", "ext_shared_i64_load", ext_shared_i64_load)?;
    linker.func_wrap("env", "ext_shared_i64_store", ext_shared_i64_store)?;
    linker.func_wrap("env", "ext_shared_i64_swap", ext_shared_i64_swap)?;
    linker.func_wrap("env", "ext_shared_i64_compare_exchange", ext_shared_i64_compare_exchange)?;
    linker.func_wrap("env", "ext_shared_i64_fetch_add", ext_shared_i64_fetch_add)?;
    linker.func_wrap("env", "ext_shared_i64_fetch_sub", ext_shared_i64_fetch_sub)?;
    linker.func_wrap("env", "ext_shared_i64_fetch_and", ext_shared_i64_fetch_and)?;
    linker.func_wrap("env", "ext_shared_i64_fetch_nand", ext_shared_i64_fetch_nand)?;
    linker.func_wrap("env", "ext_shared_i64_fetch_or", ext_shared_i64_fetch_or)?;
    linker.func_wrap("env", "ext_shared_i64_fetch_xor", ext_shared_i64_fetch_xor)?;
    linker.func_wrap("env", "ext_shared_i64_fetch_max", ext_shared_i64_fetch_max)?;
    linker.func_wrap("env", "ext_shared_i64_fetch_min", ext_shared_i64_fetch_min)?;

    linker.func_wrap("env", "ext_shared_blob_read", ext_shared_blob_read)?;
    linker.func_wrap("env", "ext_shared_blob_write", ext_shared_blob_write)?;
    linker.func_wrap("env", "ext_shared_blob_cas", ext_shared_blob_cas)?;

    Ok(())
}
