use std::sync::LazyLock;

use super::types::OrionWasmResult;
use crate::body::timeout_body::TimeoutBody;
use crate::OrionRequestBody;
use crate::OrionResponseBody;
use bytes::Bytes;
use http::StatusCode;
use http::{Request, Response};
use http_body_util::Full;
use smol_str::{SmolStr, ToSmolStr};
use wasmtime::{Caller, Linker};

use orion_wasm_types::{CalloutRequest, CalloutResponse, HeaderMutation, HeaderTarget, LogLevel, GrpcCalloutRequest, GrpcCalloutResponse};
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

    let val_bytes = match HeaderTarget::try_from(handle_type) {
        Ok(HeaderTarget::Request) => {
            let request = unsafe { &*(handle as *const Request<OrionRequestBody>) };
            request.headers().get(name.as_str())
        },
        Ok(HeaderTarget::Response) => {
            let response = unsafe { &*(handle as *const Response<OrionResponseBody>) };
            response.headers().get(name.as_str())
        },
        Ok(HeaderTarget::RequestTrailers) => caller.data().request_trailers.as_ref().and_then(|t| t.get(name.as_str())),
        Ok(HeaderTarget::ResponseTrailers) => {
            caller.data().response_trailers.as_ref().and_then(|t| t.get(name.as_str()))
        },
        Err(_) => return OrionWasmResult::InternalError.into(),
    }
    .map(|v| v.as_bytes().to_vec());

    if let Some(val_bytes) = val_bytes {
        if val_bytes.len() > value_max_len as usize {
            return OrionWasmResult::BufferTooSmall.into();
        }

        let data = memory.data_mut(&mut caller);

        let val_start = value_ptr as usize;
        let val_end = val_start + val_bytes.len();
        if val_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[val_start..val_end].copy_from_slice(&val_bytes);

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

fn orion_get_plugin_config(
    mut caller: Caller<'_, WasmState>,
    config_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let config_string = match caller.data().plugin_config.as_ref() {
        Some(s) => s.clone(),
        None => return OrionWasmResult::NotFound.into(),
    };
    let config_bytes = config_string.as_bytes();

    if config_bytes.len() > max_len as usize {
        return OrionWasmResult::BufferTooSmall.into();
    }

    let data = memory.data_mut(&mut caller);
    let start = config_ptr as usize;
    let end = start + config_bytes.len();
    if end > data.len() {
        return OrionWasmResult::InvalidMemoryAccess.into();
    }
    data[start..end].copy_from_slice(&config_bytes);

    let len_start = written_len_ptr as usize;
    let len_end = len_start + 4;
    if len_end > data.len() {
        return OrionWasmResult::InvalidMemoryAccess.into();
    }
    data[len_start..len_end].copy_from_slice(&(config_bytes.len() as u32).to_le_bytes());

    OrionWasmResult::Ok.into()
}

fn orion_get_body(
    mut caller: Caller<'_, WasmState>,
    _handle: u64,
    handle_type: u32,
    body_ptr: u32,
    max_len: u32,
    written_len_ptr: u32,
) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let body_bytes = match HeaderTarget::try_from(handle_type) {
        Ok(HeaderTarget::Request) => match caller.data().buffered_request_body.as_ref() {
            Some(b) => b.clone(),
            None => return OrionWasmResult::NotFound.into(),
        },
        Ok(HeaderTarget::Response) => match caller.data().buffered_response_body.as_ref() {
            Some(b) => b.clone(),
            None => return OrionWasmResult::NotFound.into(),
        },
        Ok(HeaderTarget::RequestTrailers) | Ok(HeaderTarget::ResponseTrailers) | Err(_) => {
            return OrionWasmResult::InternalError.into()
        },
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

fn orion_set_body(
    mut caller: Caller<'_, WasmState>,
    _handle: u64,
    handle_type: u32,
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

    match HeaderTarget::try_from(handle_type) {
        Ok(HeaderTarget::Request) => caller.data_mut().buffered_request_body = Some(body_bytes),
        Ok(HeaderTarget::Response) => caller.data_mut().buffered_response_body = Some(body_bytes),
        Ok(HeaderTarget::RequestTrailers) | Ok(HeaderTarget::ResponseTrailers) | Err(_) => {
            return OrionWasmResult::InternalError.into()
        },
    }

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

fn orion_set_custom_metrics(mut caller: Caller<'_, WasmState>, buffer_ptr: u32, buffer_len: u32) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    #[cfg(feature = "metrics")]
    {
        let data = memory.data(&caller);
        let start = buffer_ptr as usize;
        let end = start + buffer_len as usize;
        if end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        let buf = &data[start..end];

        let pairs = match bincode_next::serde::decode_from_slice::<Vec<(SmolStr, SmolStr)>, _>(
            buf,
            bincode_next::config::standard(),
        ) {
            Ok((p, _)) => p,
            Err(_) => return OrionWasmResult::InvalidMemoryAccess.into(),
        };

        if let Some(custom_metrics) = orion_metrics::metrics::custom::CUSTOM_METRICS.get() {
            let mut kv = orion_metrics::key_value::KeyValueMap::default();
            for (k, v) in &pairs {
                kv.insert(k.as_str(), v.as_str());
            }
            custom_metrics.with_key_value(orion_metrics::metrics::custom::MetricsHook::Wasm, &kv, &[]);
        }
        OrionWasmResult::Ok.into()
    }
    #[cfg(not(feature = "metrics"))]
    {
        OrionWasmResult::InternalError.into()
    }
}

fn orion_set_access_log_operators(mut caller: Caller<'_, WasmState>, buffer_ptr: u32, buffer_len: u32) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let data = memory.data(&caller);
    let start = buffer_ptr as usize;
    let end = start + buffer_len as usize;
    if end > data.len() {
        return OrionWasmResult::InvalidMemoryAccess.into();
    }
    let buf = &data[start..end];

    let operators = match bincode_next::serde::decode_from_slice::<Vec<(SmolStr, SmolStr)>, _>(
        buf,
        bincode_next::config::standard(),
    ) {
        Ok((ops, _)) => ops,
        Err(_) => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    caller.data_mut().access_log_operators.extend(operators);
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
    match LogLevel::try_from(level) {
        Ok(LogLevel::Error) => tracing::error!(target: "wasm", "{name}: {msg}"),
        Ok(LogLevel::Warn) => tracing::warn!(target:  "wasm", "{name}: {msg}"),
        Ok(LogLevel::Info) => tracing::info!(target:  "wasm", "{name}: {msg}"),
        Ok(LogLevel::Debug) => tracing::debug!(target: "wasm", "{name}: {msg}"),
        Ok(LogLevel::Trace) | Err(_) => tracing::trace!(target: "wasm", "{name}: {msg}"),
    }

    OrionWasmResult::Ok.into()
}

use http::HeaderMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

// HeaderMap serde wrappers

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

    static EMPTY_MAP: LazyLock<http::HeaderMap> = LazyLock::new(|| http::HeaderMap::new());

    let headers = match HeaderTarget::try_from(handle_type) {
        Ok(HeaderTarget::Request) => {
            let request = unsafe { &*(handle as *const Request<OrionRequestBody>) };
            request.headers()
        },
        Ok(HeaderTarget::Response) => {
            let response = unsafe { &*(handle as *const Response<OrionResponseBody>) };
            response.headers()
        },
        Ok(HeaderTarget::RequestTrailers) => caller.data().request_trailers.as_ref().unwrap_or(&EMPTY_MAP),
        Ok(HeaderTarget::ResponseTrailers) => caller.data().response_trailers.as_ref().unwrap_or(&EMPTY_MAP),
        Err(_) => return OrionWasmResult::InternalError.into(),
    };
    let serialized = match bincode_next::serde::encode_to_vec(&SerHeaderMap(headers), bincode_next::config::standard())
    {
        Ok(b) => b,
        Err(_) => return OrionWasmResult::InternalError.into(),
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
    let headers = match bincode_next::serde::decode_from_slice::<DeHeaderMap, _>(
        &data[start..end],
        bincode_next::config::standard(),
    ) {
        Ok((DeHeaderMap(h), _)) => h,
        Err(_) => return OrionWasmResult::InternalError.into(),
    };

    match HeaderTarget::try_from(handle_type) {
        Ok(HeaderTarget::Request) => {
            let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
            *request.headers_mut() = headers;
        },
        Ok(HeaderTarget::Response) => {
            let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
            *response.headers_mut() = headers;
        },
        Ok(HeaderTarget::RequestTrailers) => {
            caller.data_mut().request_trailers = Some(headers);
        },
        Ok(HeaderTarget::ResponseTrailers) => {
            caller.data_mut().response_trailers = Some(headers);
        },
        Err(_) => return OrionWasmResult::InternalError.into(),
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
        Ok((name, value)) => match HeaderTarget::try_from(handle_type) {
            Ok(HeaderTarget::Request) => {
                let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
                request.headers_mut().insert(name, value);
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::Response) => {
                let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
                response.headers_mut().insert(name, value);
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::RequestTrailers) => {
                let trailers = caller.data_mut().request_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.insert(name, value);
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::ResponseTrailers) => {
                let trailers = caller.data_mut().response_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.insert(name, value);
                OrionWasmResult::Ok.into()
            },
            Err(_) => OrionWasmResult::InternalError.into(),
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
        Ok((name, value)) => match HeaderTarget::try_from(handle_type) {
            Ok(HeaderTarget::Request) => {
                let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
                request.headers_mut().append(name, value);
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::Response) => {
                let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
                response.headers_mut().append(name, value);
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::RequestTrailers) => {
                let trailers = caller.data_mut().request_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.append(name, value);
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::ResponseTrailers) => {
                let trailers = caller.data_mut().response_trailers.get_or_insert_with(http::HeaderMap::new);
                trailers.append(name, value);
                OrionWasmResult::Ok.into()
            },
            Err(_) => OrionWasmResult::InternalError.into(),
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
        Ok(name) => match HeaderTarget::try_from(handle_type) {
            Ok(HeaderTarget::Request) => {
                let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
                request.headers_mut().remove(&name);
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::Response) => {
                let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
                response.headers_mut().remove(&name);
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::RequestTrailers) => {
                if let Some(trailers) = caller.data_mut().request_trailers.as_mut() {
                    trailers.remove(&name);
                }
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::ResponseTrailers) => {
                if let Some(trailers) = caller.data_mut().response_trailers.as_mut() {
                    trailers.remove(&name);
                }
                OrionWasmResult::Ok.into()
            },
            Err(_) => OrionWasmResult::InternalError.into(),
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
        Ok((name, value)) => match HeaderTarget::try_from(handle_type) {
            Ok(HeaderTarget::Request) => {
                let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
                if request.headers().contains_key(&name) {
                    request.headers_mut().insert(name, value);
                }
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::Response) => {
                let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
                if response.headers().contains_key(&name) {
                    response.headers_mut().insert(name, value);
                }
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::RequestTrailers) => {
                let trailers = caller.data_mut().request_trailers.get_or_insert_with(http::HeaderMap::new);
                if trailers.contains_key(&name) {
                    trailers.insert(name, value);
                }
                OrionWasmResult::Ok.into()
            },
            Ok(HeaderTarget::ResponseTrailers) => {
                let trailers = caller.data_mut().response_trailers.get_or_insert_with(http::HeaderMap::new);
                if trailers.contains_key(&name) {
                    trailers.insert(name, value);
                }
                OrionWasmResult::Ok.into()
            },
            Err(_) => OrionWasmResult::InternalError.into(),
        },
        Err(e) => e.into(),
    }
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

fn orion_apply_header_mutations(
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
    let mutations = match deserialize_header_mutations(&data[start..end]) {
        Some(m) => m,
        None => return OrionWasmResult::InternalError.into(),
    };

    match HeaderTarget::try_from(handle_type) {
        Ok(HeaderTarget::Request) => {
            let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
            apply_mutations_to_map(request.headers_mut(), mutations);
        },
        Ok(HeaderTarget::Response) => {
            let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
            apply_mutations_to_map(response.headers_mut(), mutations);
        },
        Ok(HeaderTarget::RequestTrailers) => {
            let trailers = caller.data_mut().request_trailers.get_or_insert_with(http::HeaderMap::new);
            apply_mutations_to_map(trailers, mutations);
        },
        Ok(HeaderTarget::ResponseTrailers) => {
            let trailers = caller.data_mut().response_trailers.get_or_insert_with(http::HeaderMap::new);
            apply_mutations_to_map(trailers, mutations);
        },
        Err(_) => return OrionWasmResult::InternalError.into(),
    }

    OrionWasmResult::Ok.into()
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
            let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
                Some(mem) => mem,
                None => return OrionWasmResult::InvalidMemoryAccess.into(),
            };
            let data = memory.data(&caller);
            let start = req_ptr as usize;
            let end = start + req_len as usize;
            if end > data.len() {
                return OrionWasmResult::InvalidMemoryAccess.into();
            }
            data[start..end].to_vec()
        };

        let callout_req: CalloutRequest =
            match bincode_next::serde::decode_from_slice(&req_bytes, bincode_next::config::standard()) {
                Ok((req, _)) => req,
                Err(e) => {
                    tracing::error!("Callout deserialization failed: {:?}", e);
                    return OrionWasmResult::InternalError.into();
                },
            };

        // 2. Resolve cluster and acquire connection
        let cluster_spec = ClusterSpecifier::Cluster(callout_req.cluster_name.clone());
        let cluster_id = match clusters_manager::resolve_cluster(&cluster_spec, None) {
            Some(id) => id,
            None => return OrionWasmResult::NotFound.into(),
        };

        let http_service = match clusters_manager::get_http_connection(cluster_id, RoutingContext::None) {
            Ok(svc) => svc,
            Err(e) => {
                tracing::error!("Callout failed to get HTTP connection: {:?}", e);
                return OrionWasmResult::InternalError.into();
            },
        };

        let uri_str = if callout_req.path.starts_with("http://") || callout_req.path.starts_with("https://") {
            callout_req.path.to_string()
        } else {
            format!("http://{}{}", callout_req.cluster_name, callout_req.path)
        };

        let mut builder = http::Request::builder().method(callout_req.method).uri(uri_str);

        let mut has_host = false;
        for (k, v) in callout_req.headers.into_iter() {
            if let Some(name) = k {
                if name == http::header::HOST {
                    has_host = true;
                }
                builder = builder.header(name, v);
            }
        }

        if !has_host {
            builder = builder.header(http::header::HOST, callout_req.cluster_name.as_str());
        }

        let body_bytes = callout_req.body.unwrap_or_default();
        let instrumented = crate::OrionRequestBody::default().map_inner(|_| {
            crate::body::timeout_body::TimeoutBody::new(
                None,
                crate::body::poly_body::PolyBody::from(http_body_util::Full::from(body_bytes)),
            )
        });

        let request = match builder.body(instrumented) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Callout failed to build request body: {:?}", e);
                return OrionWasmResult::InternalError.into();
            },
        };

        let channel = http_service.channel();

        let timeout_duration = if let Some(deadline) = caller.data().io_deadline {
            let now = std::time::Instant::now();
            if now >= deadline {
                return OrionWasmResult::Timeout.into();
            }
            Some(deadline.duration_since(now))
        } else {
            None
        };

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
                    return OrionWasmResult::InternalError.into();
                },
                Err(_) => {
                    return OrionWasmResult::Timeout.into();
                },
            },
            None => match request_fut.await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!("Callout HTTP request failed: {:?}", e);
                    return OrionWasmResult::InternalError.into();
                },
            },
        };

        let status = response.status();
        let resp_headers = response.headers().clone();

        let body_bytes = match response.into_body().collect().await {
            Ok(collected) => collected.to_bytes().to_vec(),
            Err(e) => {
                tracing::error!("Callout failed to collect response body: {:?}", e);
                return OrionWasmResult::InternalError.into();
            },
        };

        let callout_resp = CalloutResponse { status, headers: resp_headers, body: Some(body_bytes) };

        let resp_bytes = match bincode_next::serde::encode_to_vec(&callout_resp, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Callout response serialization failed: {:?}", e);
                return OrionWasmResult::InternalError.into();
            },
        };

        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let alloc_func = match caller.get_export("orion_malloc").and_then(|e| e.into_func()) {
            Some(func) => func,
            None => {
                tracing::error!("Callout failed: guest does not export orion_malloc");
                return OrionWasmResult::InternalError.into();
            },
        };

        // Call orion_malloc on the guest
        let mut results = [wasmtime::Val::I32(0)];
        if let Err(e) =
            alloc_func.call_async(&mut caller, &[wasmtime::Val::I32(resp_bytes.len() as i32)], &mut results).await
        {
            tracing::error!("Callout failed to call orion_malloc: {:?}", e);
            return OrionWasmResult::InternalError.into();
        }

        let resp_ptr = match results[0] {
            wasmtime::Val::I32(ptr) => ptr as u32,
            _ => return OrionWasmResult::InternalError.into(),
        };

        let data = memory.data_mut(&mut caller);
        let rb_start = resp_ptr as usize;
        let rb_end = rb_start + resp_bytes.len();
        if rb_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[rb_start..rb_end].copy_from_slice(&resp_bytes);

        // Write the pointer and length back to the guest
        let ptr_start = resp_ptr_ptr as usize;
        let ptr_end = ptr_start + 4;
        if ptr_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[ptr_start..ptr_end].copy_from_slice(&resp_ptr.to_le_bytes());

        let rl_start = resp_len_ptr as usize;
        let rl_end = rl_start + 4;
        if rl_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[rl_start..rl_end].copy_from_slice(&(resp_bytes.len() as u32).to_le_bytes());

        OrionWasmResult::Ok.into()
    })
}

struct RawBytesCodec;

impl tonic::codec::Codec for RawBytesCodec {
    type Encode = Vec<u8>;
    type Decode = Vec<u8>;
    type Encoder = RawBytesEncoder;
    type Decoder = RawBytesDecoder;

    fn encoder(&mut self) -> Self::Encoder { RawBytesEncoder }
    fn decoder(&mut self) -> Self::Decoder { RawBytesDecoder }
}

struct RawBytesEncoder;
impl tonic::codec::Encoder for RawBytesEncoder {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn encode(&mut self, item: Self::Item, dst: &mut tonic::codec::EncodeBuf<'_>) -> Result<(), Self::Error> {
        bytes::BufMut::put_slice(dst, &item);
        Ok(())
    }
}

struct RawBytesDecoder;
impl tonic::codec::Decoder for RawBytesDecoder {
    type Item = Vec<u8>;
    type Error = tonic::Status;

    fn decode(&mut self, src: &mut tonic::codec::DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        use bytes::Buf;
        if !src.has_remaining() { return Ok(None); }
        let bytes = src.copy_to_bytes(src.remaining()).to_vec();
        Ok(Some(bytes))
    }
}

fn orion_dispatch_grpc_call(
    mut caller: Caller<'_, WasmState>,
    (req_ptr, req_len, resp_ptr_ptr, resp_len_ptr): (u32, u32, u32, u32),
) -> Box<dyn std::future::Future<Output = i32> + Send + '_> {
    Box::new(async move {
        let req_bytes = {
            let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
                Some(mem) => mem,
                None => return OrionWasmResult::InvalidMemoryAccess.into(),
            };
            let data = memory.data(&caller);
            let start = req_ptr as usize;
            let end = start + req_len as usize;
            if end > data.len() {
                return OrionWasmResult::InvalidMemoryAccess.into();
            }
            data[start..end].to_vec()
        };

        let callout_req: GrpcCalloutRequest =
            match bincode_next::serde::decode_from_slice(&req_bytes, bincode_next::config::standard()) {
                Ok((req, _)) => req,
                Err(e) => {
                    tracing::error!("gRPC Callout deserialization failed: {:?}", e);
                    return OrionWasmResult::InternalError.into();
                },
            };

        let cluster_spec = ClusterSpecifier::Cluster(callout_req.cluster_name.clone());
        let cluster_id = match clusters_manager::resolve_cluster(&cluster_spec, None) {
            Some(id) => id,
            None => return OrionWasmResult::NotFound.into(),
        };

        let grpc_service = match clusters_manager::get_grpc_connection(cluster_id, RoutingContext::None) {
            Ok(svc) => svc,
            Err(e) => {
                tracing::error!("gRPC Callout failed to get connection: {:?}", e);
                return OrionWasmResult::InternalError.into();
            },
        };

        let mut client = tonic::client::Grpc::new(grpc_service);
        let path = match http::uri::PathAndQuery::try_from(format!("/{}/{}", callout_req.service_name, callout_req.method_name)) {
            Ok(p) => p,
            Err(e) => {
                tracing::error!("gRPC Callout invalid path: {:?}", e);
                return OrionWasmResult::InternalError.into();
            }
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
                return OrionWasmResult::Timeout.into();
            }
            Some(deadline.duration_since(now))
        } else {
            None
        };

        let request_fut = client.unary(grpc_req, path, RawBytesCodec);

        let response_res = match timeout_duration {
            Some(duration) => match pingora_timeout::fast_timeout::fast_timeout(duration, request_fut).await {
                Ok(Ok(r)) => Ok(r),
                Ok(Err(e)) => Err(e),
                Err(_) => {
                    return OrionWasmResult::Timeout.into();
                },
            },
            None => request_fut.await,
        };

        let callout_resp = match response_res {
            Ok(response) => {
                let mut initial_metadata = Vec::new();
                for kv in response.metadata().iter() {
                    if let tonic::metadata::KeyAndValueRef::Ascii(k, v) = kv {
                        initial_metadata.push((k.as_str().into(), v.to_str().unwrap_or("").into()));
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
                GrpcCalloutResponse {
                    initial_metadata: Vec::new(),
                    message: Vec::new(),
                    trailing_metadata: Vec::new(),
                    status: status.code() as u32,
                    status_message: status.message().into(),
                }
            }
        };

        let resp_bytes = match bincode_next::serde::encode_to_vec(&callout_resp, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("gRPC Callout response serialization failed: {:?}", e);
                return OrionWasmResult::InternalError.into();
            },
        };

        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let alloc_func = match caller.get_export("orion_malloc").and_then(|e| e.into_func()) {
            Some(func) => func,
            None => {
                tracing::error!("gRPC Callout failed: guest does not export orion_malloc");
                return OrionWasmResult::InternalError.into();
            },
        };

        let mut results = [wasmtime::Val::I32(0)];
        if let Err(e) =
            alloc_func.call_async(&mut caller, &[wasmtime::Val::I32(resp_bytes.len() as i32)], &mut results).await
        {
            tracing::error!("gRPC Callout failed to call orion_malloc: {:?}", e);
            return OrionWasmResult::InternalError.into();
        }

        let resp_ptr = match results[0] {
            wasmtime::Val::I32(ptr) => ptr as u32,
            _ => return OrionWasmResult::InternalError.into(),
        };

        let data = memory.data_mut(&mut caller);
        let rb_start = resp_ptr as usize;
        let rb_end = rb_start + resp_bytes.len();
        if rb_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[rb_start..rb_end].copy_from_slice(&resp_bytes);

        let ptr_start = resp_ptr_ptr as usize;
        let ptr_end = ptr_start + 4;
        if ptr_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[ptr_start..ptr_end].copy_from_slice(&resp_ptr.to_le_bytes());

        let rl_start = resp_len_ptr as usize;
        let rl_end = rl_start + 4;
        if rl_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[rl_start..rl_end].copy_from_slice(&(resp_bytes.len() as u32).to_le_bytes());

        OrionWasmResult::Ok.into()
    })
}

fn orion_set_io_timeout(mut caller: Caller<'_, WasmState>, microseconds: u64) -> i32 {
    let deadline = std::time::Instant::now() + std::time::Duration::from_micros(microseconds);
    caller.data_mut().io_deadline = Some(deadline);
    OrionWasmResult::Ok.into()
}

fn orion_clear_io_timeout(mut caller: Caller<'_, WasmState>, remaining_us_ptr: u32) -> i32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return OrionWasmResult::InvalidMemoryAccess.into(),
    };

    let remaining_us: u64 = if let Some(deadline) = caller.data_mut().io_deadline.take() {
        let now = std::time::Instant::now();
        if deadline > now {
            (deadline - now).as_micros() as u64
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
        return OrionWasmResult::InvalidMemoryAccess.into();
    }
    data[start..end].copy_from_slice(&remaining_us.to_le_bytes());

    OrionWasmResult::Ok.into()
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
            OrionWasmResult::Timeout.into()
        } else {
            OrionWasmResult::Ok.into()
        }
    })
}

fn orion_get_downstream_metadata(
    mut caller: Caller<'_, WasmState>,
    (request_handle, out_ptr_ptr, out_len_ptr): (u64, u32, u32),
) -> Box<dyn std::future::Future<Output = i32> + Send + '_> {
    Box::new(async move {
        let request = unsafe { &*(request_handle as *const Request<OrionRequestBody>) };

        let host_meta = match request.extensions().get::<Box<crate::listeners::metadata::DownstreamMetadata>>() {
            Some(m) => m,
            None => return OrionWasmResult::NotFound.into(),
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
            sni: host_meta.sni.as_ref().map(|s| s.to_string()),
            listener_name: host_meta.listener_name.to_string(),
        };

        let encoded = match bincode_next::serde::encode_to_vec(&guest_meta, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Failed to encode DownstreamMetadata: {:?}", e);
                return OrionWasmResult::InternalError.into();
            },
        };

        let alloc_func = match caller.get_export("orion_malloc").and_then(|e| e.into_func()) {
            Some(func) => func,
            None => return OrionWasmResult::InternalError.into(),
        };

        let mut results = [wasmtime::Val::I32(0)];
        if let Err(e) = alloc_func.call_async(&mut caller, &[wasmtime::Val::I32(encoded.len() as i32)], &mut results).await {
            tracing::error!("Failed to call orion_malloc async: {:?}", e);
            return OrionWasmResult::InternalError.into();
        }

        let allocated_ptr = match results[0] {
            wasmtime::Val::I32(ptr) => ptr as u32,
            _ => return OrionWasmResult::InternalError.into(),
        };

        let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
            Some(mem) => mem,
            None => return OrionWasmResult::InvalidMemoryAccess.into(),
        };

        let data = memory.data_mut(&mut caller);
        let start = allocated_ptr as usize;
        let end = start + encoded.len();
        if end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[start..end].copy_from_slice(&encoded);

        let ptr_start = out_ptr_ptr as usize;
        if ptr_start + 4 > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[ptr_start..ptr_start + 4].copy_from_slice(&allocated_ptr.to_le_bytes());

        let len_start = out_len_ptr as usize;
        if len_start + 4 > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[len_start..len_start + 4].copy_from_slice(&(encoded.len() as u32).to_le_bytes());

        OrionWasmResult::Ok.into()
    })
}

fn orion_shared_resolve(mut caller: Caller<'_, WasmState>, name_ptr: u32, name_len: u32, var_type: u32) -> u32 {
    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return u32::MAX,
    };
    let data = memory.data(&caller);
    let start = name_ptr as usize;
    let end = start + name_len as usize;
    if end > data.len() { return u32::MAX; }
    let name = match std::str::from_utf8(&data[start..end]) {
        Ok(s) => s.to_string(),
        Err(_) => return u32::MAX,
    };

    let shared = caller.data().shared_memory.clone();
    let map = &shared.name_to_id;
    let pin = map.pin();

    if let Some(&var_id) = pin.get(&name) {
        return match (var_id, var_type) {
            (super::shared::VarId::U64(id), 0) => id,
            (super::shared::VarId::I64(id), 1) => id,
            (super::shared::VarId::Blob(id), 2) => id,
            _ => u32::MAX,
        };
    }

    let (id, new_var_id) = match var_type {
        0 => {
            let id = shared.next_u64.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if id as usize >= shared.u64_vars.len() { return u32::MAX; }
            (id, super::shared::VarId::U64(id))
        },
        1 => {
            let id = shared.next_i64.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if id as usize >= shared.i64_vars.len() { return u32::MAX; }
            (id, super::shared::VarId::I64(id))
        },
        2 => {
            let id = shared.next_blob.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if id as usize >= shared.blob_vars.len() { return u32::MAX; }
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
    }}
}

macro_rules! get_shared_blob {
    ($shared:expr, $id:expr) => {{
        let res = $shared.blob_vars.get($id as usize);
        if res.is_none() {
            tracing::error!("Wasm shared variable index out of bounds: id {} on blob_vars", $id);
        }
        res
    }}
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
fn ext_shared_u64_compare_exchange(caller: Caller<'_, WasmState>, id: u32, current: u64, new: u64, succ: u32, fail: u32) -> u64 {
    get_shared_atomic!(caller, u64_vars, id).map(|v| {
        match v.compare_exchange(current, new, convert_ordering(succ), convert_ordering(fail)) {
            Ok(prev) => prev,
            Err(prev) => prev,
        }
    }).unwrap_or(0)
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
fn ext_shared_i64_compare_exchange(caller: Caller<'_, WasmState>, id: u32, current: i64, new: i64, succ: u32, fail: u32) -> i64 {
    get_shared_atomic!(caller, i64_vars, id).map(|v| {
        match v.compare_exchange(current, new, convert_ordering(succ), convert_ordering(fail)) {
            Ok(prev) => prev,
            Err(prev) => prev,
        }
    }).unwrap_or(0)
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
fn ext_shared_blob_read(mut caller: Caller<'_, WasmState>, id: u32, buf_ptr: u32, buf_len: u32, out_version_ptr: u32) -> u32 {
    let shared = caller.data().shared_memory.clone();
    let blob_lock = match get_shared_blob!(shared, id) {
        Some(b) => b,
        None => return u32::MAX,
    };
    let blob = blob_lock.read().unwrap();

    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return u32::MAX,
    };
    let data = memory.data_mut(&mut caller);

    let ver_start = out_version_ptr as usize;
    if ver_start + 8 <= data.len() {
        data[ver_start..ver_start+8].copy_from_slice(&blob.version.to_le_bytes());
    }

    let actual_len = blob.data.len() as u32;
    if actual_len <= buf_len {
        let start = buf_ptr as usize;
        if start + actual_len as usize <= data.len() {
            data[start..start + actual_len as usize].copy_from_slice(&blob.data);
        }
    }
    actual_len
}

fn ext_shared_blob_write(mut caller: Caller<'_, WasmState>, id: u32, buf_ptr: u32, buf_len: u32) -> u64 {
    let shared = caller.data().shared_memory.clone();
    let blob_lock = match get_shared_blob!(shared, id) {
        Some(b) => b,
        None => return 0,
    };
    let mut blob = blob_lock.write().unwrap();

    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return 0,
    };
    let data = memory.data(&caller);
    let start = buf_ptr as usize;
    if start + buf_len as usize > data.len() { return 0; }

    blob.data.clear();
    blob.data.extend_from_slice(&data[start..start + buf_len as usize]);
    blob.version += 1;
    blob.version
}

fn ext_shared_blob_cas(mut caller: Caller<'_, WasmState>, id: u32, buf_ptr: u32, buf_len: u32, expected_version: u64, out_success_ptr: u32) -> u64 {
    let shared = caller.data().shared_memory.clone();
    let blob_lock = match get_shared_blob!(shared, id) {
        Some(b) => b,
        None => return 0,
    };
    let mut blob = blob_lock.write().unwrap();

    let memory = match caller.get_export("memory").and_then(|m| m.into_memory()) {
        Some(mem) => mem,
        None => return 0,
    };

    let success = blob.version == expected_version;
    if success {
        let data = memory.data(&caller);
        let start = buf_ptr as usize;
        if start + buf_len as usize <= data.len() {
            blob.data.clear();
            blob.data.extend_from_slice(&data[start..start + buf_len as usize]);
            blob.version += 1;
        }
    }

    let data_mut = memory.data_mut(&mut caller);
    let succ_start = out_success_ptr as usize;
    if succ_start + 4 <= data_mut.len() {
        data_mut[succ_start..succ_start+4].copy_from_slice(&(success as u32).to_le_bytes());
    }

    blob.version
}

pub fn register_hostcalls(linker: &mut Linker<WasmState>) -> Result<(), wasmtime::Error> {
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
