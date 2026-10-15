// Copyright 2026 The arion-gateway Authors
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

//! Tool invocation: request-scoped context, backend dispatch, and shared telemetry.

use std::{io, sync::Arc as StdArc, time::Instant};

use arion_tracing::http_tracer::ScopedClientSpan;
use bytes::Bytes;
use opentelemetry::{KeyValue, StringValue};
use pingora_timeout::fast_timeout::fast_timeout;
use rmcp::{ServiceError, model::CallToolResult};
use serde::Serialize;
use serde_json::Value;
use tracing::{debug, info};

#[cfg(feature = "metrics")]
use {
    crate::{get_shard_id, with_histogram, with_metric},
    arion_metrics::metrics::mcp as mcp_metrics,
};

use super::{
    ToolEntry, ToolId, ToolsRegistry,
    entry::{McpServerBackend, ToolBackend},
    errors::{CallToolError, ToolInvocationErrorCode, ToolInvocationFailure, ToolInvocationOutcome},
};
use crate::listeners::http_connection_manager::{RequestCtx, mcp_gateway::mcp::Session};

/// Request-scoped values borrowed from the live request during a tool call.
#[derive(Clone, Copy)]
pub struct ToolCallContext<'a> {
    pub extensions: &'a http::Extensions,
    pub headers: &'a http::HeaderMap,
    pub request: &'a RequestCtx,
    pub session: &'a StdArc<Session>,
}

/// Owned all-default call context for tests; `context()` borrows it.
#[cfg(test)]
#[derive(Default)]
pub(crate) struct TestCallContext {
    pub extensions: http::Extensions,
    pub headers: http::HeaderMap,
    pub request: RequestCtx,
    pub session: StdArc<Session>,
}

#[cfg(test)]
impl TestCallContext {
    pub fn context(&self) -> ToolCallContext<'_> {
        ToolCallContext {
            extensions: &self.extensions,
            headers: &self.headers,
            request: &self.request,
            session: &self.session,
        }
    }
}

impl ToolsRegistry {
    /// Invokes an exact registry tool. Deliberately does not refresh the
    /// registry; the tool must already be present.
    pub async fn invoke_tool(
        &self,
        cx: ToolCallContext<'_>,
        id: &ToolId,
        arguments: Value,
    ) -> Result<CallToolResult, CallToolError> {
        debug!(target: "mcp_gateway", "call: tool '{}.{}'", id.namespace, id.name);

        let entry = self.get_tool(id).ok_or_else(|| CallToolError::ToolNotFound(id.qualified_name()))?;
        if !entry.is_permitted(cx.extensions) {
            return Err(CallToolError::RbacDenied(id.qualified_name()));
        }
        entry.validate_against_input_schema(&arguments)?;

        match &entry.backend {
            ToolBackend::Rest(backend) => backend.invoke(&entry, arguments, cx).await,
            ToolBackend::McpServer(backend) => invoke_mcp_server(&entry, backend, &arguments, cx).await,
            ToolBackend::FunctionGraph => Err(CallToolError::FunctionGraphNotImplemented),
        }
    }
}

async fn invoke_mcp_server(
    entry: &ToolEntry,
    backend: &McpServerBackend,
    arguments: &Value,
    cx: ToolCallContext<'_>,
) -> Result<CallToolResult, CallToolError> {
    let url = &backend.url;
    let cluster = &backend.cluster;
    let session = cx.session;
    let arguments = arguments.as_object();
    let upstream_tool_name = backend.upstream_tool_name.as_ref();
    let upstream_call = async {
        let cached = session.mcp_upstreams.pin().get(url.as_str()).map(StdArc::clone);
        let client = if let Some(client) = cached {
            client
        } else {
            let new_client = StdArc::new(ToolsRegistry::get_mcp_client(cluster.as_str(), url.as_str()).await?);
            StdArc::clone(session.mcp_upstreams.pin().get_or_insert_with(url.clone(), || new_client))
        };

        match client.call_tool_borrowed(upstream_tool_name, arguments).await {
            Ok(result) => Ok::<_, CallToolError>(result),
            Err(error) => {
                if matches!(&error, ServiceError::TransportSend(_) | ServiceError::TransportClosed) {
                    info!(target: "mcp_gateway", "removing MCP client for URL {url} due to transport error!");
                    session.mcp_upstreams.pin().remove(url.as_str());
                }
                Err(CallToolError::ServiceError(Box::new(error)))
            },
        }
    };

    let invocation = Invocation::start(
        cx.request,
        entry.tool_name_attr.clone(),
        "mcp_server",
        backend.upstream_attr.clone(),
    );
    let mut outcome: ToolInvocationOutcome = match fast_timeout(backend.timeout, upstream_call).await {
        Err(_) => Err(ToolInvocationFailure::upstream_timeout()),
        Ok(Ok(result)) => Ok(result),
        Ok(Err(error)) => {
            debug!(target: "mcp_gateway", "upstream MCP tool call failed: {error}");
            Err(ToolInvocationFailure::upstream_error())
        },
    };

    if let Ok(result) = &outcome {
        let result_bytes = serialized_json_size(result)?;
        invocation.record_response_bytes(result_bytes);
        if result_bytes > backend.max_response_bytes {
            outcome = Err(ToolInvocationFailure::response_too_large(backend.max_response_bytes));
        }
    }

    Ok(invocation.complete(outcome))
}

/// Owns tracing and metrics state for one tool invocation.
pub(super) struct Invocation {
    tool_name: StringValue,
    started: Instant,
    span: ScopedClientSpan,
}

impl Invocation {
    pub(super) fn start(
        req_ctx: &RequestCtx,
        tool_name: StringValue,
        backend: &'static str,
        upstream: StringValue,
    ) -> Self {
        let started = Instant::now();
        let mut span = req_ctx.begin_upstream_span(upstream.as_str());
        span.set_attributes([
            KeyValue::new("mcp.tool.name", tool_name.clone()),
            KeyValue::new("mcp.backend", backend),
            KeyValue::new("mcp.upstream", upstream),
        ]);
        Self { tool_name, started, span }
    }

    pub(super) fn span(&mut self) -> &mut ScopedClientSpan {
        &mut self.span
    }

    pub(super) fn record_response_bytes(&self, bytes: usize) {
        record_response_bytes(&self.tool_name, bytes);
    }

    pub(super) fn complete(mut self, outcome: ToolInvocationOutcome) -> CallToolResult {
        let failure_code = match &outcome {
            Ok(result) if result.is_error == Some(true) => Some(ToolInvocationErrorCode::UpstreamError.as_str()),
            Ok(_) => None,
            Err(failure) => Some(failure.code.as_str()),
        };
        if let Some(code) = failure_code {
            self.span.set_attribute(KeyValue::new("mcp.error.code", code));
            self.span.set_error(code);
        }
        self.span.complete();
        record_invocation(&self.tool_name, self.started, failure_code);
        outcome.unwrap_or_else(ToolInvocationFailure::into_call_tool_result)
    }
}

#[cfg(feature = "metrics")]
fn record_invocation(tool_name: &StringValue, started: Instant, failure_code: Option<&'static str>) {
    let shard_id = get_shard_id!();
    let elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let tool_attrs = &[KeyValue::new("tool", tool_name.clone())];
    with_metric!(mcp_metrics::TOOL_RQ_TOTAL, add, 1, shard_id, tool_attrs);
    with_histogram!(mcp_metrics::TOOL_RQ_TIME, record, elapsed_ms, shard_id, tool_attrs);
    if let Some(error_code) = failure_code {
        with_metric!(
            mcp_metrics::TOOL_RQ_FAILURES_TOTAL,
            add,
            1,
            shard_id,
            &[KeyValue::new("tool", tool_name.clone()), KeyValue::new("error", error_code)]
        );
    }
}

#[cfg(not(feature = "metrics"))]
fn record_invocation(_tool_name: &StringValue, _started: Instant, _failure_code: Option<&'static str>) {}

#[cfg(feature = "metrics")]
fn record_response_bytes(tool_name: &StringValue, bytes: usize) {
    let bytes = u64::try_from(bytes).unwrap_or(u64::MAX);
    with_histogram!(
        mcp_metrics::TOOL_RESPONSE_BYTES,
        record,
        bytes,
        get_shard_id!(),
        &[KeyValue::new("tool", tool_name.clone())]
    );
}

#[cfg(not(feature = "metrics"))]
fn record_response_bytes(_tool_name: &StringValue, _bytes: usize) {}

/// Counts encoded bytes without allocating a second copy of the JSON response.
#[derive(Default)]
struct CountingWriter(usize);

impl io::Write for CountingWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 = self.0.checked_add(buf.len()).ok_or_else(|| io::Error::other("serialized size overflow"))?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn serialized_json_size(value: &impl Serialize) -> Result<usize, serde_json::Error> {
    let mut writer = CountingWriter::default();
    serde_json::to_writer(&mut writer, value)?;
    Ok(writer.0)
}

/// Serialize a JSON-RPC 2.0 response envelope `{"jsonrpc":"2.0","id":<id>,"result":<result>}`
/// directly into a byte buffer in a single pass, avoiding intermediate `serde_json::Value` allocations.
pub(crate) fn serialize_json_rpc_response(
    id: &rmcp::model::RequestId,
    result: &impl Serialize,
) -> Result<Bytes, serde_json::Error> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(br#"{"jsonrpc":"2.0","id":"#);
    serde_json::to_writer(&mut buf, id)?;
    buf.extend_from_slice(br#","result":"#);
    serde_json::to_writer(&mut buf, result)?;
    buf.push(b'}');
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Converts a tool result into its wire value while moving an existing
    /// structured payload directly into `structuredContent`.
    fn call_tool_result_into_value(mut result: CallToolResult) -> Result<Value, serde_json::Error> {
        let content = serde_json::to_value(std::mem::take(&mut result.content))?;
        let mut value = serde_json::Map::with_capacity(5);
        if let Some(result_type) = result.result_type.take() {
            value.insert("resultType".to_owned(), serde_json::to_value(result_type)?);
        }
        value.insert("content".to_owned(), content);
        if let Some(structured_content) = result.structured_content.take() {
            value.insert("structuredContent".to_owned(), structured_content);
        }
        if let Some(is_error) = result.is_error.take() {
            value.insert("isError".to_owned(), Value::Bool(is_error));
        }
        if let Some(meta) = result.meta.take() {
            value.insert("_meta".to_owned(), serde_json::to_value(meta)?);
        }
        Ok(Value::Object(value))
    }

    #[test]
    fn counting_writer_matches_compact_json() {
        let value = json!({"nested": ["escaped\ntext", "é", {"emoji": "🦀"}], "number": 42});
        assert_eq!(serialized_json_size(&value).unwrap(), serde_json::to_vec(&value).unwrap().len());
    }

    #[test]
    fn moved_result_value_matches_serialized_call_tool_result() {
        let payload = json!({
            "temperature": 22.5,
            "tags": ["a", "b"],
            "nested": {"x": [1, 2, 3], "emoji": "🦀"},
        });
        let serialized = serde_json::to_value(CallToolResult::structured(payload.clone())).unwrap();
        let moved = call_tool_result_into_value(CallToolResult::structured(payload)).unwrap();
        assert_eq!(moved, serialized);
    }

    #[test]
    fn serialize_json_rpc_response_matches_json_rpc_response() {
        let id = rmcp::model::RequestId::Number(42);
        let result = CallToolResult::structured(json!({
            "temperature": 22.5,
            "tags": ["a", "b"],
            "nested": {"x": [1, 2, 3], "emoji": "🦀"},
        }));
        let bytes = serialize_json_rpc_response(&id, &result).unwrap();

        let expected = serde_json::to_vec(&rmcp::model::JsonRpcResponse {
            jsonrpc: rmcp::model::JsonRpcVersion2_0,
            id,
            result: serde_json::to_value(&result).unwrap(),
        })
        .unwrap();
        assert_eq!(bytes.as_ref(), expected.as_slice());
    }
}
