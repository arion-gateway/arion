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

#![allow(clippy::similar_names)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::undocumented_unsafe_blocks)]
#![allow(clippy::needless_pass_by_value)]
#![allow(clippy::manual_let_else)]
#![allow(clippy::too_many_lines)]

use std::sync::LazyLock;
use triomphe::Arc;

use crate::body::timeout_body::TimeoutBody;
use crate::listeners::http_connection_manager::RequestCtx;
use crate::ArionRequestBody;
use crate::ArionResponseBody;
use bytes::Bytes;

use http::HeaderValue;
use http::{Request, Response};
use http_body_util::Full;
use smol_str::{SmolStr, ToSmolStr};
use wasmtime::{Caller, Func, Linker, Memory};

use arion_wasm_types::{
    ArionWasmError, CalloutRequest, CalloutResponse, GrpcCalloutRequest, GrpcCalloutResponse, HeaderMutation, LogLevel,
};
pub struct WasmState {
    pub name: &'static str,
    pub plugin_config: Option<String>,
    pub direct_response: Option<Response<ArionResponseBody>>,
    pub buffered_request_body: Option<bytes::Bytes>,
    pub buffered_response_body: Option<bytes::Bytes>,
    pub request_trailers: Option<http::HeaderMap>,
    pub response_trailers: Option<http::HeaderMap>,
    pub access_log_operators: Vec<(SmolStr, SmolStr)>,
    pub io_deadline: Option<std::time::Instant>,
    pub shared_memory: Arc<super::shared::SharedMemory>,
    pub active_request_handle: Option<u64>,
    pub active_response_handle: Option<u64>,
    /// Pointer to `RequestCtx` for the active request (valid while the filter runs).
    pub active_request_ctx: Option<u64>,
    /// Guest linear memory, resolved once at instantiate time.
    pub memory: Option<Memory>,
    /// Guest allocator export, resolved once at instantiate time.
    pub arion_malloc: Option<Func>,
}

impl WasmState {
    /// Clear all per-request / per-transaction fields before recycling into the pool.
    ///
    /// Keeps long-lived fields: `name`, `plugin_config`, `shared_memory`, `memory`, `arion_malloc`.
    #[inline]
    pub fn reset_ephemeral(&mut self) {
        self.direct_response = None;
        self.buffered_request_body = None;
        self.buffered_response_body = None;
        self.request_trailers = None;
        self.response_trailers = None;
        self.access_log_operators.clear();
        self.io_deadline = None;
        self.active_request_handle = None;
        self.active_response_handle = None;
        self.active_request_ctx = None;
    }
}

/// Return the guest linear memory handle cached on [`WasmState`].
///
/// Falls back to a one-shot export lookup if the cache was not primed yet
/// (should not happen on the normal instantiate path).
#[inline]
fn guest_memory(caller: &mut Caller<'_, WasmState>) -> Option<Memory> {
    if let Some(memory) = caller.data().memory {
        return Some(memory);
    }
    let memory = caller.get_export("memory").and_then(wasmtime::Extern::into_memory)?;
    caller.data_mut().memory = Some(memory);
    Some(memory)
}

/// Return the guest `arion_malloc` export cached on [`WasmState`].
#[inline]
fn guest_malloc(caller: &mut Caller<'_, WasmState>) -> Option<Func> {
    if let Some(func) = caller.data().arion_malloc {
        return Some(func);
    }
    let func = caller.get_export("arion_malloc").and_then(wasmtime::Extern::into_func)?;
    caller.data_mut().arion_malloc = Some(func);
    Some(func)
}

trait IntoWasmAbi {
    fn into_wasm_abi(self) -> i32;
}

impl<T> IntoWasmAbi for Result<T, ArionWasmError> {
    #[inline]
    fn into_wasm_abi(self) -> i32 {
        match self {
            Ok(_) => 0,
            Err(e) => e.into(),
        }
    }
}

fn arion_get_header(
    mut caller: Caller<'_, WasmState>,
    is_trailer: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let name = {
        let data = memory.data(&caller);
        let start = name_ptr as usize;
        let end = start + name_len as usize;
        match data.get(start..end).and_then(|s| std::str::from_utf8(s).ok()) {
            Some(s) => s.to_smolstr(),
            None => return ArionWasmError::InvalidMemoryAccess.into(),
        }
    };

    let (data, state) = memory.data_and_store_mut(&mut caller);

    let val_bytes = if is_trailer == 0 {
        if let Some(req_ptr) = state.active_request_handle {
            let request = unsafe { &*(req_ptr as *const Request<ArionRequestBody>) };
            request.headers().get(name.as_str())
        } else if let Some(res_ptr) = state.active_response_handle {
            let response = unsafe { &*(res_ptr as *const Response<ArionResponseBody>) };
            response.headers().get(name.as_str())
        } else {
            return ArionWasmError::InternalError.into();
        }
    } else {
        if state.active_request_handle.is_some() {
            state.request_trailers.as_ref().and_then(|t| t.get(name.as_str()))
        } else if state.active_response_handle.is_some() {
            state.response_trailers.as_ref().and_then(|t| t.get(name.as_str()))
        } else {
            return ArionWasmError::InternalError.into();
        }
    }
    .map(HeaderValue::as_bytes);

    if let Some(val_bytes) = val_bytes {
        let len_start = written_len_ptr as usize;
        let len_end = len_start + 4;
        if len_end > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(len_start..len_end) {
            slice.copy_from_slice(&(u32::try_from(val_bytes.len()).unwrap_or(0)).to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
        }

        if val_bytes.len() > value_max_len as usize {
            return ArionWasmError::BufferTooSmall.into();
        }

        let val_start = value_ptr as usize;
        let val_end = val_start + val_bytes.len();
        if val_end > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(val_start..val_end) {
            slice.copy_from_slice(val_bytes);
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
        }

        0
    } else {
        ArionWasmError::NotFound.into()
    }
}

fn arion_get_plugin_config(
    mut caller: Caller<'_, WasmState>,
    config_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let (data, state) = memory.data_and_store_mut(&mut caller);

    let config_bytes = match state.plugin_config.as_ref() {
        Some(s) => s.as_bytes(),
        None => return ArionWasmError::NotFound.into(),
    };

    if config_bytes.len() > max_len as usize {
        return ArionWasmError::BufferTooSmall.into();
    }
    let start = config_ptr as usize;
    let end = start + config_bytes.len();
    if end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(config_bytes);
    } else {
        tracing::error!("Invalid memory index");
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(config_bytes.len()).unwrap_or(0)).to_le_bytes());
    } else {
        tracing::error!("Invalid memory index");
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn arion_get_body(mut caller: Caller<'_, WasmState>, body_ptr: u32, max_len: u32, written_len_ptr: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let (data, state) = memory.data_and_store_mut(&mut caller);

    let body_bytes = if state.active_request_handle.is_some() {
        match state.buffered_request_body.as_ref() {
            Some(b) => b.as_ref(),
            None => return ArionWasmError::NotFound.into(),
        }
    } else if state.active_response_handle.is_some() {
        match state.buffered_response_body.as_ref() {
            Some(b) => b.as_ref(),
            None => return ArionWasmError::NotFound.into(),
        }
    } else {
        return ArionWasmError::InternalError.into();
    };

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(body_bytes.len()).unwrap_or(0)).to_le_bytes());
    } else {
        tracing::error!("Invalid memory index");
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    if body_bytes.len() > max_len as usize {
        return ArionWasmError::BufferTooSmall.into();
    }

    let start = body_ptr as usize;
    let end = start + body_bytes.len();
    if end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(body_bytes);
    } else {
        tracing::error!("Invalid memory index");
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn arion_set_body(mut caller: Caller<'_, WasmState>, body_ptr: u32, body_len: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let body_bytes = if body_len > 0 {
        let data = memory.data(&caller);
        let start = body_ptr as usize;
        let end = start + body_len as usize;
        match data.get(start..end) {
            Some(s) => Bytes::copy_from_slice(s),
            None => return ArionWasmError::InvalidMemoryAccess.into(),
        }
    } else {
        Bytes::new()
    };

    if caller.data().active_request_handle.is_some() {
        caller.data_mut().buffered_request_body = Some(body_bytes);
    } else if caller.data().active_response_handle.is_some() {
        caller.data_mut().buffered_response_body = Some(body_bytes);
    } else {
        return ArionWasmError::InternalError.into();
    }

    0
}

fn arion_send_direct_response(mut caller: Caller<'_, WasmState>, resp_ptr: u32, resp_len: u32) -> i32 {
    use crate::body::poly_body::PolyBody;

    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = resp_ptr as usize;
    let end = start + resp_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
    };

    let direct_resp = match bincode_next::serde::decode_from_slice::<arion_wasm_types::DirectResponse, _>(
        slice,
        bincode_next::config::standard(),
    ) {
        Ok((r, _)) => r,
        Err(_) => return ArionWasmError::InternalError.into(),
    };

    let (parts, body) = direct_resp.response.into_parts();
    let response = Response::from_parts(parts, TimeoutBody::new(None, PolyBody::from(Full::from(body))).into());

    caller.data_mut().direct_response = Some(response);

    0
}

#[allow(unused_variables)]
fn arion_set_custom_metrics(mut caller: Caller<'_, WasmState>, buffer_ptr: u32, buffer_len: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    #[cfg(feature = "metrics")]
    {
        let data = memory.data(&caller);
        let start = buffer_ptr as usize;
        let end = start + buffer_len as usize;
        let buf = match data.get(start..end) {
            Some(s) => s,
            None => return ArionWasmError::InvalidMemoryAccess.into(),
        };

        let (pairs, _): (Vec<(&str, &str)>, _) =
            match bincode_next::borrow_decode_from_slice(buf, bincode_next::config::standard()) {
                Ok(res) => res,
                Err(_) => return ArionWasmError::InvalidMemoryAccess.into(),
            };

        if let Some(custom_metrics) = arion_metrics::metrics::custom::CUSTOM_METRICS.get() {
            let mut kv = arion_metrics::str_pair::StrMap::default();
            for (k, v) in &pairs {
                kv.insert(k, v);
            }
            custom_metrics.with_key_value(arion_metrics::metrics::custom::MetricsHook::Wasm, &kv, &[]);
        }
        0
    }
    #[cfg(not(feature = "metrics"))]
    {
        ArionWasmError::InternalError.into()
    }
}

fn arion_set_access_log_operators(mut caller: Caller<'_, WasmState>, buffer_ptr: u32, buffer_len: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = buffer_ptr as usize;
    let end = start + buffer_len as usize;
    let buf = match data.get(start..end) {
        Some(s) => s,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
    };

    let operators = match bincode_next::serde::decode_from_slice::<Vec<(SmolStr, SmolStr)>, _>(
        buf,
        bincode_next::config::standard(),
    ) {
        Ok((ops, _)) => ops,
        Err(_) => return ArionWasmError::InvalidMemoryAccess.into(),
    };

    caller.data_mut().access_log_operators.extend(operators);
    0
}

fn arion_log(mut caller: Caller<'_, WasmState>, level: u32, msg_ptr: u32, msg_len: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = msg_ptr as usize;
    let end = start + msg_len as usize;

    let msg = match data.get(start..end).and_then(|s| std::str::from_utf8(s).ok()) {
        Some(s) => s,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
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

fn arion_get_headers_map(
    mut caller: Caller<'_, WasmState>,
    is_trailer: u32,
    buf_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    static EMPTY_MAP: LazyLock<http::HeaderMap> = LazyLock::new(http::HeaderMap::new);

    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let (data, state) = memory.data_and_store_mut(&mut caller);

    let headers = if is_trailer == 0 {
        if let Some(req_ptr) = state.active_request_handle {
            let request = unsafe { &*(req_ptr as *const Request<ArionRequestBody>) };
            request.headers()
        } else if let Some(res_ptr) = state.active_response_handle {
            let response = unsafe { &*(res_ptr as *const Response<ArionResponseBody>) };
            response.headers()
        } else {
            return ArionWasmError::InternalError.into();
        }
    } else {
        if state.active_request_handle.is_some() {
            state.request_trailers.as_ref().unwrap_or(&EMPTY_MAP)
        } else if state.active_response_handle.is_some() {
            state.response_trailers.as_ref().unwrap_or(&EMPTY_MAP)
        } else {
            return ArionWasmError::InternalError.into();
        }
    };

    let start = buf_ptr as usize;
    let end = start + max_len as usize;
    if end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    let out_slice = if let Some(slice) = data.get_mut(start..end) {
        slice
    } else {
        tracing::error!("Invalid memory index");
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let written = match bincode_next::serde::encode_into_slice(
        SerHeaderMap(headers),
        out_slice,
        bincode_next::config::standard(),
    ) {
        Ok(w) => w,
        Err(_) => return ArionWasmError::BufferTooSmall.into(),
    };

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(written).unwrap_or(0)).to_le_bytes());
    } else {
        tracing::error!("Invalid memory index");
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    0
}

fn arion_set_headers_map(mut caller: Caller<'_, WasmState>, is_trailer: u32, buf_ptr: u32, buf_len: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };
    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
    };
    let headers =
        match bincode_next::serde::decode_from_slice::<DeHeaderMap, _>(slice, bincode_next::config::standard()) {
            Ok((DeHeaderMap(h), _)) => h,
            Err(_) => return ArionWasmError::InternalError.into(),
        };

    if is_trailer == 0 {
        if let Some(req_ptr) = caller.data().active_request_handle {
            let request = unsafe { &mut *(req_ptr as *mut Request<ArionRequestBody>) };
            *request.headers_mut() = headers;
        } else if let Some(res_ptr) = caller.data().active_response_handle {
            let response = unsafe { &mut *(res_ptr as *mut Response<ArionResponseBody>) };
            *response.headers_mut() = headers;
        } else {
            return ArionWasmError::InternalError.into();
        }
    } else {
        if caller.data().active_request_handle.is_some() {
            caller.data_mut().request_trailers = Some(headers);
        } else if caller.data().active_response_handle.is_some() {
            caller.data_mut().response_trailers = Some(headers);
        } else {
            return ArionWasmError::InternalError.into();
        }
    }

    0
}

fn arion_get_uri(mut caller: Caller<'_, WasmState>, buf_ptr: u32, max_len: u32, written_len_ptr: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let (data, state) = memory.data_and_store_mut(&mut caller);

    let uri = if let Some(req_ptr) = state.active_request_handle {
        let request = unsafe { &*(req_ptr as *const Request<ArionRequestBody>) };
        request.uri()
    } else {
        return ArionWasmError::InternalError.into();
    };

    let uri_str = uri.to_string();
    let uri_bytes = uri_str.as_bytes();

    let start = buf_ptr as usize;
    let end = start + max_len as usize;
    if end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    let written = uri_bytes.len();
    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(written).unwrap_or(0)).to_le_bytes());
    } else {
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    if uri_bytes.len() > max_len as usize {
        return ArionWasmError::BufferTooSmall.into();
    }

    let out_slice = match data.get_mut(start..start + uri_bytes.len()) {
        Some(slice) => slice,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
    };
    out_slice.copy_from_slice(uri_bytes);

    0
}

fn arion_set_uri(mut caller: Caller<'_, WasmState>, buf_ptr: u32, buf_len: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
    };

    let uri_str = match std::str::from_utf8(slice) {
        Ok(s) => s,
        Err(_) => return ArionWasmError::InternalError.into(),
    };

    let uri = match http::Uri::try_from(uri_str) {
        Ok(u) => u,
        Err(_) => return ArionWasmError::InternalError.into(),
    };

    if let Some(req_ptr) = caller.data().active_request_handle {
        let request = unsafe { &mut *(req_ptr as *mut Request<ArionRequestBody>) };
        *request.uri_mut() = uri;
        0
    } else {
        ArionWasmError::InternalError.into()
    }
}

fn arion_get_status_code(mut caller: Caller<'_, WasmState>, out_status_ptr: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let status_code = if let Some(res_ptr) = caller.data().active_response_handle {
        let response = unsafe { &*(res_ptr as *const Response<ArionResponseBody>) };
        u32::from(response.status().as_u16())
    } else {
        return ArionWasmError::InternalError.into();
    };

    let data = memory.data_mut(&mut caller);
    let start = out_status_ptr as usize;
    let end = start + 4;
    if end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(&status_code.to_le_bytes());
    } else {
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn arion_set_status_code(caller: Caller<'_, WasmState>, status_code: u32) -> i32 {
    if let Some(res_ptr) = caller.data().active_response_handle {
        if let Ok(code) = u16::try_from(status_code) {
            if let Ok(status) = http::StatusCode::from_u16(code) {
                let response = unsafe { &mut *(res_ptr as *mut Response<ArionResponseBody>) };
                *response.status_mut() = status;
                return 0;
            }
        }
        ArionWasmError::InternalError.into()
    } else {
        ArionWasmError::InternalError.into()
    }
}

fn get_name_value_from_memory(
    caller: &mut Caller<'_, WasmState>,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> Result<(http::header::HeaderName, http::header::HeaderValue), ArionWasmError> {
    let Some(memory) = guest_memory(caller) else {
        return Err(ArionWasmError::InvalidMemoryAccess);
    };
    let data = memory.data(caller);

    let n_start = name_ptr as usize;
    let n_end = n_start + name_len as usize;
    let name =
        http::header::HeaderName::from_bytes(data.get(n_start..n_end).ok_or(ArionWasmError::InvalidMemoryAccess)?)
            .map_err(|_e| ArionWasmError::InternalError)?;

    let v_start = value_ptr as usize;
    let v_end = v_start + value_len as usize;
    let value =
        http::header::HeaderValue::from_bytes(data.get(v_start..v_end).ok_or(ArionWasmError::InvalidMemoryAccess)?)
            .map_err(|_e| ArionWasmError::InternalError)?;

    Ok((name, value))
}

fn get_name_from_memory(
    caller: &mut Caller<'_, WasmState>,
    name_ptr: u32,
    name_len: u32,
) -> Result<http::header::HeaderName, ArionWasmError> {
    let Some(memory) = guest_memory(caller) else {
        return Err(ArionWasmError::InvalidMemoryAccess);
    };
    let data = memory.data(caller);

    let n_start = name_ptr as usize;
    let n_end = n_start + name_len as usize;
    http::header::HeaderName::from_bytes(data.get(n_start..n_end).ok_or(ArionWasmError::InvalidMemoryAccess)?)
        .map_err(|_e| ArionWasmError::InternalError)
}

fn arion_set_header(
    mut caller: Caller<'_, WasmState>,
    is_trailer: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    let mut inner = || -> Result<(), ArionWasmError> {
        let (name, value) = get_name_value_from_memory(&mut caller, name_ptr, name_len, value_ptr, value_len)?;

        if is_trailer == 0 {
            if let Some(req_ptr) = caller.data().active_request_handle {
                let request = unsafe { &mut *(req_ptr as *mut Request<ArionRequestBody>) };
                request.headers_mut().insert(name, value);
            } else if let Some(res_ptr) = caller.data().active_response_handle {
                let response = unsafe { &mut *(res_ptr as *mut Response<ArionResponseBody>) };
                response.headers_mut().insert(name, value);
            } else {
                return Err(ArionWasmError::InternalError);
            }
        } else {
            if caller.data().active_request_handle.is_some() {
                let trailers = caller.data_mut().request_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.insert(name, value);
            } else if caller.data().active_response_handle.is_some() {
                let trailers = caller.data_mut().response_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.insert(name, value);
            } else {
                return Err(ArionWasmError::InternalError);
            }
        }

        Ok(())
    };

    inner().into_wasm_abi()
}

fn arion_add_header(
    mut caller: Caller<'_, WasmState>,
    is_trailer: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    let mut inner = || -> Result<(), ArionWasmError> {
        let (name, value) = get_name_value_from_memory(&mut caller, name_ptr, name_len, value_ptr, value_len)?;

        if is_trailer == 0 {
            if let Some(req_ptr) = caller.data().active_request_handle {
                let request = unsafe { &mut *(req_ptr as *mut Request<ArionRequestBody>) };
                request.headers_mut().append(name, value);
            } else if let Some(res_ptr) = caller.data().active_response_handle {
                let response = unsafe { &mut *(res_ptr as *mut Response<ArionResponseBody>) };
                response.headers_mut().append(name, value);
            } else {
                return Err(ArionWasmError::InternalError);
            }
        } else {
            if caller.data().active_request_handle.is_some() {
                let trailers = caller.data_mut().request_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.append(name, value);
            } else if caller.data().active_response_handle.is_some() {
                let trailers = caller.data_mut().response_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.append(name, value);
            } else {
                return Err(ArionWasmError::InternalError);
            }
        }

        Ok(())
    };

    inner().into_wasm_abi()
}

fn arion_remove_header(mut caller: Caller<'_, WasmState>, is_trailer: u32, name_ptr: u32, name_len: u32) -> i32 {
    let mut inner = || -> Result<(), ArionWasmError> {
        let name = get_name_from_memory(&mut caller, name_ptr, name_len)?;

        if is_trailer == 0 {
            if let Some(req_ptr) = caller.data().active_request_handle {
                let request = unsafe { &mut *(req_ptr as *mut Request<ArionRequestBody>) };
                request.headers_mut().remove(&name);
            } else if let Some(res_ptr) = caller.data().active_response_handle {
                let response = unsafe { &mut *(res_ptr as *mut Response<ArionResponseBody>) };
                response.headers_mut().remove(&name);
            } else {
                return Err(ArionWasmError::InternalError);
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
                return Err(ArionWasmError::InternalError);
            }
        }

        Ok(())
    };

    inner().into_wasm_abi()
}

fn arion_replace_header(
    mut caller: Caller<'_, WasmState>,
    is_trailer: u32,
    name_ptr: u32,
    name_len: u32,
    value_ptr: u32,
    value_len: u32,
) -> i32 {
    let mut inner = || -> Result<(), ArionWasmError> {
        let (name, value) = get_name_value_from_memory(&mut caller, name_ptr, name_len, value_ptr, value_len)?;

        if is_trailer == 0 {
            if let Some(req_ptr) = caller.data().active_request_handle {
                let request = unsafe { &mut *(req_ptr as *mut Request<ArionRequestBody>) };
                if request.headers().contains_key(&name) {
                    request.headers_mut().insert(name, value);
                }
            } else if let Some(res_ptr) = caller.data().active_response_handle {
                let response = unsafe { &mut *(res_ptr as *mut Response<ArionResponseBody>) };
                if response.headers().contains_key(&name) {
                    response.headers_mut().insert(name, value);
                }
            } else {
                return Err(ArionWasmError::InternalError);
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
                return Err(ArionWasmError::InternalError);
            }
        }

        Ok(())
    };

    inner().into_wasm_abi()
}

fn deserialize_header_mutations(data: &[u8]) -> Option<Vec<HeaderMutation<'_>>> {
    bincode_next::serde::borrow_decode_from_slice::<Vec<HeaderMutation<'_>>, _>(data, bincode_next::config::standard())
        .ok()
        .map(|(mutations, _)| mutations)
}

fn apply_mutations_to_map(map: &mut http::HeaderMap, mutations: Vec<HeaderMutation<'_>>) {
    for mutation in mutations {
        match mutation {
            HeaderMutation::Set(name, value) => {
                if let (Ok(n), Ok(v)) = (
                    http::header::HeaderName::from_bytes(name.as_bytes()),
                    http::header::HeaderValue::from_bytes(value.as_bytes()),
                ) {
                    map.insert(n, v);
                }
            },
            HeaderMutation::Add(name, value) => {
                if let (Ok(n), Ok(v)) = (
                    http::header::HeaderName::from_bytes(name.as_bytes()),
                    http::header::HeaderValue::from_bytes(value.as_bytes()),
                ) {
                    map.append(n, v);
                }
            },
            HeaderMutation::Replace(name, value) => {
                if let (Ok(n), Ok(v)) = (
                    http::header::HeaderName::from_bytes(name.as_bytes()),
                    http::header::HeaderValue::from_bytes(value.as_bytes()),
                ) {
                    if map.contains_key(&n) {
                        map.insert(n, v);
                    }
                }
            },
            HeaderMutation::Remove(name) => {
                if let Ok(n) = http::header::HeaderName::from_bytes(name.as_bytes()) {
                    map.remove(&n);
                }
            },
        }
    }
}

fn arion_apply_header_mutations(mut caller: Caller<'_, WasmState>, is_trailer: u32, buf_ptr: u32, buf_len: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    // `data_and_store_mut` splits guest linear memory from WasmState so we can
    // zero-copy-decode `HeaderMutation<'_>` from the slice and still mutably
    // reach trailers on the store.

    let (data, state) = memory.data_and_store_mut(&mut caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
    };
    let Some(mutations) = deserialize_header_mutations(slice) else {
        return ArionWasmError::InternalError.into();
    };

    if is_trailer == 0 {
        if let Some(req_ptr) = state.active_request_handle {
            let request = unsafe { &mut *(req_ptr as *mut Request<ArionRequestBody>) };
            apply_mutations_to_map(request.headers_mut(), mutations);
        } else if let Some(res_ptr) = state.active_response_handle {
            let response = unsafe { &mut *(res_ptr as *mut Response<ArionResponseBody>) };
            apply_mutations_to_map(response.headers_mut(), mutations);
        } else {
            return ArionWasmError::InternalError.into();
        }
    } else if state.active_request_handle.is_some() {
        let trailers = state.request_trailers.get_or_insert_with(http::HeaderMap::new);
        apply_mutations_to_map(trailers, mutations);
    } else if state.active_response_handle.is_some() {
        let trailers = state.response_trailers.get_or_insert_with(http::HeaderMap::new);
        apply_mutations_to_map(trailers, mutations);
    } else {
        return ArionWasmError::InternalError.into();
    }

    0
}

use crate::clusters::{clusters_manager, RoutingContext, RoutingPriority};
use arion_configuration::config::cluster::ClusterSpecifier;
use http_body_util::BodyExt;

fn arion_dispatch_http_call(
    mut caller: Caller<'_, WasmState>,
    (req_ptr, req_len, resp_ptr_ptr, resp_len_ptr): (u32, u32, u32, u32),
) -> Box<dyn std::future::Future<Output = i32> + Send + '_> {
    Box::new(async move {
        let mut callout_req: CalloutRequest = {
            let Some(memory) = guest_memory(&mut caller) else {
                return ArionWasmError::InvalidMemoryAccess.into();
            };
            let data = memory.data(&caller);
            let start = req_ptr as usize;
            let end = start + req_len as usize;
            let slice = match data.get(start..end) {
                Some(s) => s,
                None => return ArionWasmError::InvalidMemoryAccess.into(),
            };
            match bincode_next::serde::decode_from_slice(slice, bincode_next::config::standard()) {
                Ok((req, _)) => req,
                Err(e) => {
                    tracing::error!("Callout deserialization failed: {:?}", e);
                    return ArionWasmError::InternalError.into();
                },
            }
        };

        // 2. Resolve cluster and acquire connection
        let cluster_spec = ClusterSpecifier::Cluster(std::mem::take(&mut callout_req.cluster_name));
        let cluster_id = match clusters_manager::resolve_cluster(&cluster_spec, None) {
            Some(id) => id,
            None => return ArionWasmError::NotFound.into(),
        };

        let http_service = match clusters_manager::get_http_connection(cluster_id, RoutingContext::None) {
            Ok(svc) => svc,
            Err(e) => {
                tracing::error!("Callout failed to get HTTP connection: {:?}", e);
                return ArionWasmError::InternalError.into();
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

        let instrumented = crate::ArionRequestBody::default().map_inner(|_| {
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
                return ArionWasmError::Timeout.into();
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
                    return ArionWasmError::InternalError.into();
                },
                Err(_) => {
                    return ArionWasmError::Timeout.into();
                },
            },
            None => match request_fut.await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Callout HTTP request failed: {:?}", e);
                    return ArionWasmError::InternalError.into();
                },
            },
        };

        let (resp_parts, resp_body) = response.into_parts();
        let status = resp_parts.status;
        let resp_headers = resp_parts.headers;
        let version = resp_parts.version;

        let body_bytes = match resp_body.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                tracing::error!("Callout failed to collect response body: {:?}", e);
                return ArionWasmError::InternalError.into();
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
                return ArionWasmError::InternalError.into();
            },
        };

        let Some(memory) = guest_memory(&mut caller) else {
            return ArionWasmError::InvalidMemoryAccess.into();
        };
        let Some(alloc_func) = guest_malloc(&mut caller) else {
            tracing::error!("Callout failed: guest does not export arion_malloc");
            return ArionWasmError::InternalError.into();
        };

        // Call arion_malloc on the guest
        let mut results = [wasmtime::Val::I32(0)];
        if let Err(e) = alloc_func
            .call_async(&mut caller, &[wasmtime::Val::I32(i32::try_from(resp_bytes.len()).unwrap_or(0))], &mut results)
            .await
        {
            tracing::error!("Callout failed to call arion_malloc: {:?}", e);
            return ArionWasmError::InternalError.into();
        }

        let resp_ptr = match results[0] {
            wasmtime::Val::I32(ptr) => u32::try_from(ptr).unwrap_or(0),
            _ => return ArionWasmError::InternalError.into(),
        };

        let data = memory.data_mut(&mut caller);
        let rb_start = resp_ptr as usize;
        let rb_end = rb_start + resp_bytes.len();
        if rb_end > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(rb_start..rb_end) {
            slice.copy_from_slice(&resp_bytes);
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
        }

        // Write the pointer and length back to the guest
        let ptr_start = resp_ptr_ptr as usize;
        let ptr_end = ptr_start + 4;
        if ptr_end > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(ptr_start..ptr_end) {
            slice.copy_from_slice(&resp_ptr.to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
        }

        let rl_start = resp_len_ptr as usize;
        let rl_end = rl_start + 4;
        if rl_end > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(rl_start..rl_end) {
            slice.copy_from_slice(&(u32::try_from(resp_bytes.len()).unwrap_or(0)).to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
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

fn arion_dispatch_grpc_call(
    mut caller: Caller<'_, WasmState>,
    (req_ptr, req_len, resp_ptr_ptr, resp_len_ptr): (u32, u32, u32, u32),
) -> Box<dyn std::future::Future<Output = i32> + Send + '_> {
    Box::new(async move {
        let mut callout_req: GrpcCalloutRequest = {
            let Some(memory) = guest_memory(&mut caller) else {
                return ArionWasmError::InvalidMemoryAccess.into();
            };
            let data = memory.data(&caller);
            let start = req_ptr as usize;
            let end = start + req_len as usize;
            let slice = match data.get(start..end) {
                Some(s) => s,
                None => return ArionWasmError::InvalidMemoryAccess.into(),
            };
            match bincode_next::serde::decode_from_slice(slice, bincode_next::config::standard()) {
                Ok((req, _)) => req,
                Err(e) => {
                    tracing::error!("gRPC Callout deserialization failed: {:?}", e);
                    return ArionWasmError::InternalError.into();
                },
            }
        };

        let cluster_spec = ClusterSpecifier::Cluster(std::mem::take(&mut callout_req.cluster_name));
        let cluster_id = match clusters_manager::resolve_cluster(&cluster_spec, None) {
            Some(id) => id,
            None => return ArionWasmError::NotFound.into(),
        };

        let grpc_service = match clusters_manager::get_grpc_connection(cluster_id, RoutingContext::None) {
            Ok(svc) => svc,
            Err(e) => {
                tracing::error!("gRPC Callout failed to get connection: {:?}", e);
                return ArionWasmError::InternalError.into();
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
                return ArionWasmError::InternalError.into();
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
                return ArionWasmError::Timeout.into();
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
                    return ArionWasmError::Timeout.into();
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
                return ArionWasmError::InternalError.into();
            },
        };

        let memory = if let Some(mem) = guest_memory(&mut caller) {
            mem
        } else {
            tracing::error!("gRPC Callout failed: guest does not export memory");
            return ArionWasmError::InvalidMemoryAccess.into();
        };
        let Some(alloc_func) = guest_malloc(&mut caller) else {
            tracing::error!("gRPC Callout failed: guest does not export arion_malloc");
            return ArionWasmError::InternalError.into();
        };

        let mut results = [wasmtime::Val::I32(0)];
        if let Err(e) = alloc_func
            .call_async(&mut caller, &[wasmtime::Val::I32(i32::try_from(resp_bytes.len()).unwrap_or(0))], &mut results)
            .await
        {
            tracing::error!("gRPC Callout failed to call arion_malloc: {:?}", e);
            return ArionWasmError::InternalError.into();
        }

        let resp_ptr = match results[0] {
            wasmtime::Val::I32(ptr) => u32::try_from(ptr).unwrap_or(0),
            _ => return ArionWasmError::InternalError.into(),
        };

        let data = memory.data_mut(&mut caller);
        let rb_start = resp_ptr as usize;
        let rb_end = rb_start + resp_bytes.len();
        if rb_end > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(rb_start..rb_end) {
            slice.copy_from_slice(&resp_bytes);
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
        }

        let ptr_start = resp_ptr_ptr as usize;
        let ptr_end = ptr_start + 4;
        if ptr_end > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(ptr_start..ptr_end) {
            slice.copy_from_slice(&resp_ptr.to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
        }

        let rl_start = resp_len_ptr as usize;
        let rl_end = rl_start + 4;
        if rl_end > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(rl_start..rl_end) {
            slice.copy_from_slice(&(u32::try_from(resp_bytes.len()).unwrap_or(0)).to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
        }

        0
    })
}

fn arion_set_io_timeout(mut caller: Caller<'_, WasmState>, microseconds: u64) -> i32 {
    let deadline = std::time::Instant::now() + std::time::Duration::from_micros(microseconds);
    caller.data_mut().io_deadline = Some(deadline);
    0
}

fn arion_clear_io_timeout(mut caller: Caller<'_, WasmState>, remaining_us_ptr: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
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
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(start..end) {
        slice.copy_from_slice(&remaining_us.to_le_bytes());
    } else {
        tracing::error!("Invalid memory index");
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn arion_sleep(
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
            ArionWasmError::Timeout.into()
        } else {
            0
        }
    })
}

#[derive(serde::Serialize)]
struct SerDownstreamMetadata<'a, K: Clone + Into<u8>> {
    connection: SerDownstreamConnectionMetadata<'a, K>,
    sni: Option<&'a str>,
    listener_name: &'a str,
}

#[derive(serde::Serialize)]
enum SerDownstreamConnectionMetadata<'a, K: Clone + Into<u8>> {
    FromSocket {
        peer_address: std::net::SocketAddr,
        local_address: std::net::SocketAddr,
    },
    FromProxyProtocol {
        original_peer_address: Option<std::net::SocketAddr>,
        original_destination_address: Option<std::net::SocketAddr>,
        protocol: arion_wasm_types::ProxyProtocol,
        #[serde(serialize_with = "serialize_tlv")]
        tlv_data: &'a std::collections::HashMap<K, Vec<u8>>,
        proxy_peer_address: std::net::SocketAddr,
        proxy_local_address: std::net::SocketAddr,
    },
}

fn serialize_tlv<K: Clone + Into<u8>, S: serde::Serializer>(
    tlv: &std::collections::HashMap<K, Vec<u8>>,
    s: S,
) -> Result<S::Ok, S::Error> {
    use serde::ser::SerializeMap;
    let mut map = s.serialize_map(Some(tlv.len()))?;
    for (k, v) in tlv {
        let k_u8: u8 = (*k).clone().into();
        map.serialize_entry(&k_u8, v)?;
    }
    map.end()
}

fn arion_get_downstream_metadata(
    mut caller: Caller<'_, WasmState>,
    (out_ptr_ptr, out_len_ptr): (u32, u32),
) -> Box<dyn std::future::Future<Output = i32> + Send + '_> {
    Box::new(async move {
        let host_meta = match caller.data().active_request_ctx {
            Some(ctx_ptr) => {
                let req_ctx = unsafe { &*(ctx_ptr as *const RequestCtx) };
                req_ctx.conn.downstream.as_ref()
            },
            None => return ArionWasmError::NotFound.into(),
        };

        let mapped_connection = match &host_meta.connection {
            crate::listeners::metadata::DownstreamConnectionMetadata::FromSocket { peer_address, local_address } => {
                SerDownstreamConnectionMetadata::FromSocket {
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
                    ppp::v2::Protocol::Stream => arion_wasm_types::ProxyProtocol::Stream,
                    ppp::v2::Protocol::Datagram => arion_wasm_types::ProxyProtocol::Datagram,
                    ppp::v2::Protocol::Unspecified => arion_wasm_types::ProxyProtocol::Unspec,
                };
                SerDownstreamConnectionMetadata::FromProxyProtocol {
                    original_peer_address: Some(*original_peer_address),
                    original_destination_address: Some(*original_destination_address),
                    protocol: mapped_protocol,
                    tlv_data,
                    proxy_peer_address: *proxy_peer_address,
                    proxy_local_address: *proxy_local_address,
                }
            },
        };

        let guest_meta = SerDownstreamMetadata {
            connection: mapped_connection,
            sni: host_meta.sni.as_deref(),
            listener_name: host_meta.listener_name,
        };

        let encoded = match bincode_next::serde::encode_to_vec(&guest_meta, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Failed to encode DownstreamMetadata: {:?}", e);
                return ArionWasmError::InternalError.into();
            },
        };

        let Some(alloc_func) = guest_malloc(&mut caller) else {
            return ArionWasmError::InternalError.into();
        };

        let mut results = [wasmtime::Val::I32(0)];
        if let Err(e) = alloc_func
            .call_async(&mut caller, &[wasmtime::Val::I32(i32::try_from(encoded.len()).unwrap_or(0))], &mut results)
            .await
        {
            tracing::error!("Failed to call arion_malloc async: {:?}", e);
            return ArionWasmError::InternalError.into();
        }

        let allocated_ptr = match results[0] {
            wasmtime::Val::I32(ptr) => u32::try_from(ptr).unwrap_or(0),
            _ => return ArionWasmError::InternalError.into(),
        };

        let Some(memory) = guest_memory(&mut caller) else {
            return ArionWasmError::InvalidMemoryAccess.into();
        };

        let data = memory.data_mut(&mut caller);
        let start = allocated_ptr as usize;
        let end = start + encoded.len();
        if end > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(start..end) {
            slice.copy_from_slice(&encoded);
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
        }

        let ptr_start = out_ptr_ptr as usize;
        if ptr_start + 4 > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(ptr_start..ptr_start + 4) {
            slice.copy_from_slice(&allocated_ptr.to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
        }

        let len_start = out_len_ptr as usize;
        if len_start + 4 > data.len() {
            return ArionWasmError::InvalidMemoryAccess.into();
        }
        if let Some(slice) = data.get_mut(len_start..len_start + 4) {
            slice.copy_from_slice(&(u32::try_from(encoded.len()).unwrap_or(0)).to_le_bytes());
        } else {
            tracing::error!("Invalid memory index");
            return ArionWasmError::InvalidMemoryAccess.into();
        }

        0
    })
}

fn arion_shared_resolve(mut caller: Caller<'_, WasmState>, name_ptr: u32, name_len: u32, var_type: u32) -> u32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return u32::MAX;
    };
    let data = memory.data(&caller);
    let start = name_ptr as usize;
    let end = start + name_len as usize;
    let s = match data.get(start..end).and_then(|slice| std::str::from_utf8(slice).ok()) {
        Some(s) => s,
        None => return u32::MAX,
    };

    let shared = Arc::clone(&caller.data().shared_memory);
    let map = &shared.name_to_id;
    let pin = map.pin();

    if let Some(&var_id) = pin.get(s) {
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

    let name_owned = s.to_owned();
    pin.insert(name_owned, new_var_id);
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
    let shared = Arc::clone(&caller.data().shared_memory);
    let Some(blob_lock) = get_shared_blob!(shared, id) else {
        return u32::MAX;
    };
    let blob = blob_lock.read();

    let Some(memory) = guest_memory(&mut caller) else {
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
    let shared = Arc::clone(&caller.data().shared_memory);
    let Some(blob_lock) = get_shared_blob!(shared, id) else {
        return 0;
    };
    let mut blob = blob_lock.write();

    let Some(memory) = guest_memory(&mut caller) else {
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
    let shared = Arc::clone(&caller.data().shared_memory);
    let Some(blob_lock) = get_shared_blob!(shared, id) else {
        return 0;
    };
    let mut blob = blob_lock.write();

    let Some(memory) = guest_memory(&mut caller) else {
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

fn arion_get_request(mut caller: Caller<'_, WasmState>, buf_ptr: u32, max_len: u32, written_len_ptr: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let (data, state) = memory.data_and_store_mut(&mut caller);

    let req_ptr = match state.active_request_handle {
        Some(ptr) => ptr,
        None => return ArionWasmError::InternalError.into(),
    };
    let request = unsafe { &*(req_ptr as *const Request<ArionRequestBody>) };

    let empty_body = bytes::Bytes::new();
    let body_bytes = state.buffered_request_body.as_ref().unwrap_or(&empty_body);

    let wasm_req = arion_wasm_types::ProxyWasmRequest {
        method: request.method(),
        uri: request.uri(),
        headers: request.headers(),
        version: request.version(),
        body: body_bytes,
    };

    let start = buf_ptr as usize;
    let end = start + max_len as usize;
    if end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    let out_slice = match data.get_mut(start..end) {
        Some(slice) => slice,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
    };

    let written = match bincode_next::serde::encode_into_slice(&wasm_req, out_slice, bincode_next::config::standard()) {
        Ok(w) => w,
        Err(_) => return ArionWasmError::BufferTooSmall.into(),
    };

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(written).unwrap_or(0)).to_le_bytes());
    } else {
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn arion_set_request(mut caller: Caller<'_, WasmState>, buf_ptr: u32, buf_len: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
    };

    let wasm_req = match bincode_next::serde::decode_from_slice::<arion_wasm_types::WasmRequest, _>(
        slice,
        bincode_next::config::standard(),
    ) {
        Ok((r, _)) => r,
        Err(_) => return ArionWasmError::InternalError.into(),
    };

    let (parts, body) = wasm_req.request.into_parts();

    if let Some(req_ptr) = caller.data().active_request_handle {
        let request = unsafe { &mut *(req_ptr as *mut Request<ArionRequestBody>) };
        *request.method_mut() = parts.method;
        *request.uri_mut() = parts.uri;
        *request.version_mut() = parts.version;
        *request.headers_mut() = parts.headers;

        caller.data_mut().buffered_request_body = Some(body);
        0
    } else {
        ArionWasmError::InternalError.into()
    }
}

fn arion_get_response(mut caller: Caller<'_, WasmState>, buf_ptr: u32, max_len: u32, written_len_ptr: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let (data, state) = memory.data_and_store_mut(&mut caller);

    let res_ptr = match state.active_response_handle {
        Some(ptr) => ptr,
        None => return ArionWasmError::InternalError.into(),
    };
    let response = unsafe { &*(res_ptr as *const Response<ArionResponseBody>) };

    let empty_body = bytes::Bytes::new();
    let body_bytes = state.buffered_response_body.as_ref().unwrap_or(&empty_body);

    let wasm_res = arion_wasm_types::ProxyWasmResponse {
        status: response.status(),
        version: response.version(),
        headers: response.headers(),
        body: body_bytes,
    };

    let start = buf_ptr as usize;
    let end = start + max_len as usize;
    if end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    let out_slice = match data.get_mut(start..end) {
        Some(slice) => slice,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
    };

    let written = match bincode_next::serde::encode_into_slice(&wasm_res, out_slice, bincode_next::config::standard()) {
        Ok(w) => w,
        Err(_) => return ArionWasmError::BufferTooSmall.into(),
    };

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return ArionWasmError::InvalidMemoryAccess.into();
    }
    if let Some(slice) = data.get_mut(len_start..len_end) {
        slice.copy_from_slice(&(u32::try_from(written).unwrap_or(0)).to_le_bytes());
    } else {
        return ArionWasmError::InvalidMemoryAccess.into();
    }

    0
}

fn arion_set_response(mut caller: Caller<'_, WasmState>, buf_ptr: u32, buf_len: u32) -> i32 {
    let Some(memory) = guest_memory(&mut caller) else {
        return ArionWasmError::InvalidMemoryAccess.into();
    };

    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    let end = start + buf_len as usize;
    let slice = match data.get(start..end) {
        Some(s) => s,
        None => return ArionWasmError::InvalidMemoryAccess.into(),
    };

    let wasm_res = match bincode_next::serde::decode_from_slice::<arion_wasm_types::WasmResponse, _>(
        slice,
        bincode_next::config::standard(),
    ) {
        Ok((r, _)) => r,
        Err(_) => return ArionWasmError::InternalError.into(),
    };

    let (parts, body) = wasm_res.response.into_parts();

    if let Some(res_ptr) = caller.data().active_response_handle {
        let response = unsafe { &mut *(res_ptr as *mut Response<ArionResponseBody>) };
        *response.status_mut() = parts.status;
        *response.version_mut() = parts.version;
        *response.headers_mut() = parts.headers;

        caller.data_mut().buffered_response_body = Some(body);
        0
    } else {
        ArionWasmError::InternalError.into()
    }
}

pub fn register_hostcalls(linker: &mut Linker<WasmState>) -> Result<(), wasmtime::Error> {
    linker.func_wrap("env", "arion_get_uri", arion_get_uri)?;
    linker.func_wrap("env", "arion_set_uri", arion_set_uri)?;
    linker.func_wrap("env", "arion_get_status_code", arion_get_status_code)?;
    linker.func_wrap("env", "arion_set_status_code", arion_set_status_code)?;
    linker.func_wrap("env", "arion_get_request", arion_get_request)?;
    linker.func_wrap("env", "arion_set_request", arion_set_request)?;
    linker.func_wrap("env", "arion_get_response", arion_get_response)?;
    linker.func_wrap("env", "arion_set_response", arion_set_response)?;

    linker.func_wrap("env", "arion_get_plugin_config", arion_get_plugin_config)?;
    linker.func_wrap("env", "arion_get_header", arion_get_header)?;
    linker.func_wrap("env", "arion_get_body", arion_get_body)?;
    linker.func_wrap("env", "arion_set_body", arion_set_body)?;
    linker.func_wrap("env", "arion_get_headers_map", arion_get_headers_map)?;
    linker.func_wrap("env", "arion_set_headers_map", arion_set_headers_map)?;

    linker.func_wrap("env", "arion_set_header", arion_set_header)?;
    linker.func_wrap("env", "arion_add_header", arion_add_header)?;
    linker.func_wrap("env", "arion_remove_header", arion_remove_header)?;
    linker.func_wrap("env", "arion_replace_header", arion_replace_header)?;
    linker.func_wrap("env", "arion_apply_header_mutations", arion_apply_header_mutations)?;

    linker.func_wrap("env", "arion_send_direct_response", arion_send_direct_response)?;
    linker.func_wrap_async("env", "arion_get_downstream_metadata", arion_get_downstream_metadata)?;

    linker.func_wrap("env", "arion_set_custom_metrics", arion_set_custom_metrics)?;
    linker.func_wrap("env", "arion_set_access_log_operators", arion_set_access_log_operators)?;
    linker.func_wrap("env", "arion_log", arion_log)?;
    linker.func_wrap("env", "arion_set_io_timeout", arion_set_io_timeout)?;
    linker.func_wrap("env", "arion_clear_io_timeout", arion_clear_io_timeout)?;
    linker.func_wrap_async("env", "arion_dispatch_http_call", arion_dispatch_http_call)?;
    linker.func_wrap_async("env", "arion_dispatch_grpc_call", arion_dispatch_grpc_call)?;
    linker.func_wrap_async("env", "arion_sleep", arion_sleep)?;

    // Shared Memory Hostcalls
    linker.func_wrap("env", "ext_shared_resolve", arion_shared_resolve)?;

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
