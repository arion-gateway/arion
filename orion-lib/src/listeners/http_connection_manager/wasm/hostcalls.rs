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

use orion_wasm_types::{CalloutRequest, CalloutResponse, HeaderMutation};
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
        },
        1 => {
            let response = unsafe { &*(handle as *const Response<OrionResponseBody>) };
            response.headers().get(name.as_str())
        },
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

    let body_bytes = match handle_type {
        0 => match caller.data().buffered_request_body.as_ref() {
            Some(b) => b.clone(),
            None => return OrionWasmResult::NotFound.into(),
        },
        1 => match caller.data().buffered_response_body.as_ref() {
            Some(b) => b.clone(),
            None => return OrionWasmResult::NotFound.into(),
        },
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

    match handle_type {
        0 => caller.data_mut().buffered_request_body = Some(body_bytes),
        1 => caller.data_mut().buffered_response_body = Some(body_bytes),
        _ => return OrionWasmResult::InternalError.into(),
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
        },
        1 => {
            let response = unsafe { &*(handle as *const Response<OrionResponseBody>) };
            serialize_header_map(response.headers())
        },
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
        },
        1 => {
            let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
            *response.headers_mut() = headers;
        },
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

fn deserialize_header_mutations(data: &[u8]) -> Option<Vec<HeaderMutation>> {
    if data.len() < 4 {
        return None;
    }
    let num_mutations = u32::from_le_bytes(data[0..4].try_into().unwrap());
    let mut offset = 4;
    let mut mutations = Vec::new();

    for _ in 0..num_mutations {
        if offset >= data.len() {
            return None;
        }
        let mut_type = data[offset];
        offset += 1;

        if offset + 4 > data.len() {
            return None;
        }
        let name_len = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;

        if offset + name_len > data.len() {
            return None;
        }
        let name_bytes = &data[offset..offset + name_len];
        offset += name_len;

        let name_str = std::str::from_utf8(name_bytes).ok()?;
        let name = http::header::HeaderName::try_from(name_str).ok()?;

        if mut_type == 3 {
            mutations.push(HeaderMutation::Remove(name));
        } else {
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
            let value = http::header::HeaderValue::from_bytes(val_bytes).ok()?;

            match mut_type {
                0 => mutations.push(HeaderMutation::Set(name, value)),
                1 => mutations.push(HeaderMutation::Add(name, value)),
                2 => mutations.push(HeaderMutation::Replace(name, value)),
                _ => return None,
            }
        }
    }
    Some(mutations)
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

    match handle_type {
        0 => {
            let request = unsafe { &mut *(handle as *mut Request<OrionRequestBody>) };
            apply_mutations_to_map(request.headers_mut(), mutations);
        },
        1 => {
            let response = unsafe { &mut *(handle as *mut Response<OrionResponseBody>) };
            apply_mutations_to_map(response.headers_mut(), mutations);
        },
        _ => return OrionWasmResult::InternalError.into(),
    }

    OrionWasmResult::Ok.into()
}

use orion_configuration::config::cluster::ClusterSpecifier;
use crate::clusters::{clusters_manager, RoutingContext, RoutingPriority};
use http_body_util::BodyExt;

fn orion_dispatch_http_call(
    mut caller: Caller<'_, WasmState>,
    (req_ptr, req_len, resp_buf_ptr, resp_buf_max, resp_len_ptr): (u32, u32, u32, u32, u32),
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

        let callout_req: CalloutRequest = match bincode_next::serde::decode_from_slice(&req_bytes, bincode_next::config::standard()) {
            Ok((req, _)) => req,
            Err(e) => {
                tracing::error!("Callout deserialization failed: {:?}", e);
                return OrionWasmResult::InternalError.into();
            }
        };

        // 2. Resolve cluster and acquire connection
        let cluster_spec = ClusterSpecifier::Cluster(callout_req.cluster_name.clone().into());
        let cluster_id = match clusters_manager::resolve_cluster(&cluster_spec, None) {
            Some(id) => id,
            None => return OrionWasmResult::NotFound.into(),
        };

        let http_service = match clusters_manager::get_http_connection(cluster_id, RoutingContext::None) {
            Ok(svc) => svc,
            Err(e) => {
                tracing::error!("Callout failed to get HTTP connection: {:?}", e);
                return OrionWasmResult::InternalError.into();
            }
        };

        let uri_str = if callout_req.path.starts_with("http://") || callout_req.path.starts_with("https://") {
            callout_req.path.to_string()
        } else {
            format!("http://{}{}", callout_req.cluster_name, callout_req.path)
        };

        let mut builder = http::Request::builder()
            .method(callout_req.method)
            .uri(uri_str);
            
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
                crate::body::poly_body::PolyBody::from(http_body_util::Full::from(body_bytes))
            )
        });
        
        let request = match builder.body(instrumented) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Callout failed to build request body: {:?}", e);
                return OrionWasmResult::InternalError.into();
            }
        };

        let channel = http_service.channel();
        
        // 3. Send async request - this is where the Wasm fiber suspends!
        let response_result = channel.send_request(
            request,
            Some(std::time::Duration::from_secs(5)),
            None,
            RoutingPriority::Default,
            None,
            #[cfg(feature = "instrumentation")]
            &quanta::Clock::new(),
        ).await;

        let response = match response_result {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Callout HTTP request failed: {:?}", e);
                return OrionWasmResult::InternalError.into();
            }
        };

        let status = response.status();
        let resp_headers = response.headers().clone();

        let body_bytes = match response.into_body().collect().await {
            Ok(collected) => collected.to_bytes().to_vec(),
            Err(e) => {
                tracing::error!("Callout failed to collect response body: {:?}", e);
                return OrionWasmResult::InternalError.into();
            }
        };

        let callout_resp = CalloutResponse {
            status,
            headers: resp_headers,
            body: Some(body_bytes),
        };

        let resp_bytes = match bincode_next::serde::encode_to_vec(&callout_resp, bincode_next::config::standard()) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!("Callout response serialization failed: {:?}", e);
                return OrionWasmResult::InternalError.into();
            }
        };

        if resp_bytes.len() > resp_buf_max as usize {
            return OrionWasmResult::BufferTooSmall.into();
        }

        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let data = memory.data_mut(&mut caller);
        
        let rb_start = resp_buf_ptr as usize;
        let rb_end = rb_start + resp_bytes.len();
        if rb_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[rb_start..rb_end].copy_from_slice(&resp_bytes);

        let rl_start = resp_len_ptr as usize;
        let rl_end = rl_start + 4;
        if rl_end > data.len() {
            return OrionWasmResult::InvalidMemoryAccess.into();
        }
        data[rl_start..rl_end].copy_from_slice(&(resp_bytes.len() as u32).to_le_bytes());

        OrionWasmResult::Ok.into()
    })
}

pub fn register_hostcalls(linker: &mut Linker<WasmState>) -> Result<(), wasmtime::Error> {
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
    linker.func_wrap("env", "orion_log", orion_log)?;
    linker.func_wrap_async("env", "orion_dispatch_http_call", orion_dispatch_http_call)?;
    Ok(())
}
