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

use std::sync::{Arc as StdArc, Mutex, atomic::Ordering};
use std::time::Instant;

use futures::FutureExt;
use futures::future::BoxFuture;
use rmcp::model::{CallToolResult, ContentBlock};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use smol_str::SmolStr;
use thiserror::Error;
use tokio_util::sync::CancellationToken;

use super::toolkits::ToolkitRegistry;
use crate::listeners::http_connection_manager::{
    RequestCtx,
    mcp_gateway::{
        mcp::Session,
        tools::{CallToolError, ToolCallContext, ToolId, ToolSearchRequest, ToolsRegistry},
    },
};

/// Dispatches sandboxed tool and catalog calls into the gateway.
pub trait ToolDispatcher: Send + Sync + 'static {
    /// `seq` is the execution-wide submission sequence number; records sort by
    /// it so `toolCalls` stays deterministic under concurrent completion.
    fn call_tool(
        self: StdArc<Self>,
        seq: u64,
        id: ToolId,
        args: Value,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Value, DispatchDefect>>;

    fn catalog(
        self: StdArc<Self>,
        request: CatalogRequest,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Value, DispatchDefect>>;
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum CatalogRequest {
    NamespacesList { limit: Option<usize>, offset: Option<usize> },
    ToolsSearch { query: String, namespace: Option<SmolStr>, limit: Option<usize> },
    ToolsList { namespace: SmolStr, limit: Option<usize>, offset: Option<usize> },
    ToolsDescribe { namespace: SmolStr, name: SmolStr },
    ToolkitsList { limit: Option<usize>, offset: Option<usize> },
    ToolkitsGet { name: SmolStr },
}

/// An infrastructure failure while dispatching a sandbox request.
#[derive(Debug, Error)]
pub enum DispatchDefect {
    #[error("tool dispatch failed")]
    Cancelled,
    #[allow(dead_code)]
    #[error("tool dispatch failed")]
    InvalidResponse,
}

/// One completed actual-tool dispatch, surfaced as `toolCalls` in the execute
/// result.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolCallRecord {
    #[serde(skip)]
    pub seq: u64,
    pub tool: String,
    pub ok: bool,
    pub duration_ms: u64,
}

pub struct CodeModeToolDispatcher {
    tools: StdArc<ToolsRegistry>,
    toolkits: StdArc<ToolkitRegistry>,
    request_extensions: http::Extensions,
    request_headers: http::HeaderMap,
    request_context: RequestCtx,
    session: StdArc<Session>,
    tool_calls: Mutex<Vec<ToolCallRecord>>,
}

impl CodeModeToolDispatcher {
    pub fn new(tools: StdArc<ToolsRegistry>, toolkits: StdArc<ToolkitRegistry>, cx: ToolCallContext<'_>) -> Self {
        Self {
            tools,
            toolkits,
            request_extensions: cx.extensions.clone(),
            request_headers: cx.headers.clone(),
            request_context: cx.request.clone(),
            session: StdArc::clone(cx.session),
            tool_calls: Mutex::new(Vec::new()),
        }
    }

    fn call_context(&self) -> ToolCallContext<'_> {
        ToolCallContext {
            extensions: &self.request_extensions,
            headers: &self.request_headers,
            request: &self.request_context,
            session: &self.session,
        }
    }

    pub fn take_tool_calls(&self) -> Vec<ToolCallRecord> {
        let mut records =
            std::mem::take(&mut *self.tool_calls.lock().unwrap_or_else(std::sync::PoisonError::into_inner));
        records.sort_unstable_by_key(|record| record.seq);
        records
    }

    fn record_tool_call(&self, seq: u64, id: &ToolId, envelope: &Value, started: Instant) {
        let record = ToolCallRecord {
            seq,
            tool: id.qualified_name(),
            ok: envelope.get("ok").and_then(Value::as_bool).unwrap_or(false),
            duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        };
        self.tool_calls.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(record);
    }
}

impl ToolDispatcher for CodeModeToolDispatcher {
    fn call_tool(
        self: StdArc<Self>,
        seq: u64,
        id: ToolId,
        args: Value,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Value, DispatchDefect>> {
        async move {
            self.session.last_activity.store(Instant::now(), Ordering::Relaxed);
            let started = Instant::now();
            let call = self.tools.invoke_tool(self.call_context(), &id, args);
            let result = tokio::select! {
                () = cancel.cancelled() => return Err(DispatchDefect::Cancelled),
                result = call => result,
            };

            let envelope = match result {
                Ok(result) => tool_result_envelope(result),
                Err(error) => tool_error_envelope(&error),
            };
            self.record_tool_call(seq, &id, &envelope, started);
            Ok(envelope)
        }
        .boxed()
    }

    fn catalog(
        self: StdArc<Self>,
        request: CatalogRequest,
        cancel: CancellationToken,
    ) -> BoxFuture<'static, Result<Value, DispatchDefect>> {
        async move {
            let operation = async {
                self.tools.ensure_tools_current(&self.request_context).await?;
                match request {
                    CatalogRequest::NamespacesList { limit, offset } => {
                        self.tools.catalog_namespaces(&self.request_extensions, limit, offset).and_then(to_value)
                    },
                    CatalogRequest::ToolsSearch { query, namespace, limit } => self
                        .tools
                        .catalog_search_tools(
                            &self.request_extensions,
                            &self.request_context,
                            ToolSearchRequest { query, namespace, limit },
                        )
                        .await
                        .map(|items| json!({ "items": items })),
                    CatalogRequest::ToolsList { namespace, limit, offset } => self
                        .tools
                        .catalog_list_tools(&self.request_extensions, &namespace, limit, offset)
                        .and_then(to_value),
                    CatalogRequest::ToolsDescribe { namespace, name } => {
                        let id = ToolId::new(namespace, name)
                            .map_err(|error| CallToolError::ValidationError(error.to_string()))?;
                        self.tools
                            .catalog_describe_tool(&self.request_extensions, &id)
                            .ok_or_else(|| CallToolError::ToolNotFound(id.qualified_name()))
                            .and_then(to_value)
                    },
                    CatalogRequest::ToolkitsList { limit, offset } => {
                        self.toolkits.list(&self.tools, &self.request_extensions, limit, offset).and_then(to_value)
                    },
                    CatalogRequest::ToolkitsGet { name } => self
                        .toolkits
                        .get(&self.tools, &self.request_extensions, &name)
                        .ok_or_else(|| CallToolError::ToolNotFound(name.to_string()))
                        .and_then(to_value),
                }
            };
            let result = tokio::select! {
                () = cancel.cancelled() => return Err(DispatchDefect::Cancelled),
                result = operation => result,
            };
            match result {
                Ok(data) => Ok(json!({ "ok": true, "data": data })),
                Err(error) => Ok(catalog_error_envelope(&error)),
            }
        }
        .boxed()
    }
}

fn to_value<T: serde::Serialize>(value: T) -> Result<Value, CallToolError> {
    serde_json::to_value(value).map_err(CallToolError::from)
}

/// The branchable `{ok, ...}` value a guest tool call resolves with.
fn tool_result_envelope(result: CallToolResult) -> Value {
    if result.is_error == Some(true) {
        let error = result.structured_content.and_then(|value| value.get("error").cloned()).unwrap_or_else(|| {
            json!({
                "code": "upstream_error",
                "message": joined_text(&result.content).unwrap_or_else(|| "The upstream tool call failed".to_owned()),
                "retryable": false
            })
        });
        return json!({ "ok": false, "error": error });
    }

    let omitted = result.content.iter().filter(|content| !matches!(content, ContentBlock::Text(_))).count();
    let data =
        result.structured_content.or_else(|| joined_text(&result.content).map(Value::String)).unwrap_or(Value::Null);
    json!({ "ok": true, "data": data, "meta": { "omitted": omitted } })
}

fn tool_error_envelope(error: &CallToolError) -> Value {
    let (code, message) = match error {
        CallToolError::ToolNotFound(_) => ("tool_not_found", "The requested tool was not found"),
        CallToolError::RbacDenied(_) => ("access_denied", "Access to the requested tool was denied"),
        CallToolError::NameNotString | CallToolError::ValidationError(_) => {
            ("invalid_input", "The tool input did not match its schema")
        },
        CallToolError::FunctionGraphNotImplemented | CallToolError::RequestEncoding { .. } => {
            ("upstream_error", "The upstream tool call failed")
        },
        CallToolError::ClientInitializeError(_) | CallToolError::ServiceError(_) => {
            ("upstream_error", "The upstream tool is unavailable")
        },
        CallToolError::SerdeError(_) => ("upstream_error", "The upstream tool returned an invalid response"),
        CallToolError::CodeModeConcurrencyLimit => {
            ("execution_unavailable", "Code Mode execution capacity is currently exhausted")
        },
    };
    error_envelope(code, message, false)
}

fn catalog_error_envelope(error: &CallToolError) -> Value {
    let (code, message) = match error {
        CallToolError::ValidationError(_) | CallToolError::NameNotString => ("invalid_input", error.to_string()),
        CallToolError::ToolNotFound(_) | CallToolError::RbacDenied(_) => {
            ("not_found", "The requested catalog entry was not found.".to_owned())
        },
        _ => ("catalog_unavailable", "The catalog is temporarily unavailable.".to_owned()),
    };
    error_envelope(code, &message, code == "catalog_unavailable")
}

pub(super) fn error_envelope(code: &str, message: &str, retryable: bool) -> Value {
    json!({
        "ok": false,
        "error": { "code": code, "message": message, "retryable": retryable }
    })
}

fn joined_text(content: &[ContentBlock]) -> Option<String> {
    let text = content
        .iter()
        .filter_map(|content| match content {
            ContentBlock::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    (!text.is_empty()).then_some(text)
}

#[cfg(test)]
mod tests {
    use arion_configuration::config::network_filters::{
        http_connection_manager::http_filters::mcp_gateway::{
            DisclosureMode, EmbeddingVector, HttpUpstreamPolicy, McpRbacPermission, McpTool, McpToolRbac,
            McpToolSearch, McpToolkit, McpToolkitToolRef, UpstreamBackend, UpstreamLimits,
        },
        network_rbac::Action,
    };
    use serde_json::json;

    use super::*;

    fn actual_tool(name: &str, description: &str, rbac: Option<McpToolRbac>) -> McpTool {
        McpTool {
            namespace: "test".into(),
            name: name.into(),
            description: description.into(),
            input_schema: serde_json::Map::new(),
            output_schema: serde_json::Map::new(),
            backend: UpstreamBackend::Rest {
                method: http::Method::GET,
                path: format!("/{name}").into(),
                query_params: Vec::new(),
                cluster: "test".into(),
                upstream_policy: Box::new(HttpUpstreamPolicy::default()),
                body_template: None,
                body_json: None,
            },
            rbac,
            embedding: EmbeddingVector::default(),
            disclosure_mode: DisclosureMode::Eager,
        }
    }

    fn empty_toolkits() -> StdArc<ToolkitRegistry> {
        StdArc::new(ToolkitRegistry::with_config(Vec::new()).unwrap())
    }

    #[test]
    fn catalog_request_identifiers_remain_owned() {
        for request in [
            json!({"operation": "tools_describe", "namespace": "fresh_catalog_namespace", "name": "fresh_catalog_tool"}),
            json!({"operation": "toolkits_get", "name": "fresh_catalog_toolkit"}),
            json!({"operation": "tools_search", "query": "fresh_catalog_query", "namespace": "fresh_catalog_namespace"}),
        ] {
            serde_json::from_value::<CatalogRequest>(request).unwrap();
        }
        for value in ["fresh_catalog_namespace", "fresh_catalog_tool", "fresh_catalog_toolkit", "fresh_catalog_query"] {
            assert!(arion_interner::InternedStr::lookup(value).is_none(), "{value} must not be interned");
        }
    }

    fn dispatcher_with(
        tools: StdArc<ToolsRegistry>,
        toolkits: StdArc<ToolkitRegistry>,
    ) -> StdArc<CodeModeToolDispatcher> {
        let cx = crate::listeners::http_connection_manager::mcp_gateway::tools::TestCallContext::default();
        StdArc::new(CodeModeToolDispatcher::new(tools, toolkits, cx.context()))
    }

    #[tokio::test]
    async fn missing_tool_calls_are_projected_and_recorded() {
        let tools = StdArc::new(
            ToolsRegistry::with_config(
                Vec::new(),
                Vec::new(),
                McpToolSearch::default(),
                UpstreamLimits::default(),
                None,
            )
            .unwrap(),
        );
        let dispatcher = dispatcher_with(tools, empty_toolkits());

        let result = StdArc::clone(&dispatcher)
            .call_tool(1, ToolId::new("test", "missing").unwrap(), json!({}), CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(result["ok"], false);
        assert_eq!(result["error"]["code"], "tool_not_found");

        let records = dispatcher.take_tool_calls();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].tool, "test.missing");
        assert!(!records[0].ok);
    }

    #[tokio::test]
    async fn tool_call_records_sort_by_submission_sequence() {
        let tools = StdArc::new(
            ToolsRegistry::with_config(
                Vec::new(),
                Vec::new(),
                McpToolSearch::default(),
                UpstreamLimits::default(),
                None,
            )
            .unwrap(),
        );
        let dispatcher = dispatcher_with(tools, empty_toolkits());

        let first = StdArc::clone(&dispatcher).call_tool(
            1,
            ToolId::new("test", "first").unwrap(),
            json!({}),
            CancellationToken::new(),
        );
        let second = StdArc::clone(&dispatcher).call_tool(
            2,
            ToolId::new("test", "second").unwrap(),
            json!({}),
            CancellationToken::new(),
        );

        // Completion order is the reverse of submission order.
        second.await.unwrap();
        first.await.unwrap();

        let records = dispatcher.take_tool_calls();
        assert_eq!(
            records.iter().map(|record| record.tool.as_str()).collect::<Vec<_>>(),
            ["test.first", "test.second"]
        );
    }

    #[tokio::test]
    async fn codemode_catalog_search_includes_eager_tools() {
        let tools = StdArc::new(
            ToolsRegistry::with_config(
                vec![actual_tool("weather", "Weather forecast", None)],
                Vec::new(),
                McpToolSearch::default(),
                UpstreamLimits::default(),
                None,
            )
            .unwrap(),
        );
        let dispatcher = dispatcher_with(tools, empty_toolkits());

        let search = StdArc::clone(&dispatcher)
            .catalog(
                CatalogRequest::ToolsSearch { query: "weather".to_owned(), namespace: None, limit: None },
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(search["data"]["items"].as_array().map(Vec::len), Some(1));
        assert_eq!(search["data"]["items"][0]["name"], "weather");
    }

    #[tokio::test]
    async fn missing_and_unauthorized_toolkits_have_the_same_catalog_error() {
        let denied_rbac = McpToolRbac {
            action: Action::Allow,
            permissions: vec![McpRbacPermission::JwtHeader { field: "kid".into(), value: "required".into() }],
        };
        let tools = StdArc::new(
            ToolsRegistry::with_config(
                vec![actual_tool("allowed", "Allowed", None), actual_tool("denied", "Denied", Some(denied_rbac))],
                Vec::new(),
                McpToolSearch::default(),
                UpstreamLimits::default(),
                None,
            )
            .unwrap(),
        );
        let toolkits = StdArc::new(
            ToolkitRegistry::with_config(vec![McpToolkit {
                name: "protected".into(),
                description: "Protected workflow".into(),
                instructions: "Use both protected tools.".into(),
                tools: vec![
                    McpToolkitToolRef { namespace: "test".into(), name: "allowed".into() },
                    McpToolkitToolRef { namespace: "test".into(), name: "denied".into() },
                ],
            }])
            .unwrap(),
        );
        let dispatcher = dispatcher_with(tools, toolkits);

        let protected = StdArc::clone(&dispatcher)
            .catalog(CatalogRequest::ToolkitsGet { name: "protected".into() }, CancellationToken::new())
            .await
            .unwrap();
        let missing = StdArc::clone(&dispatcher)
            .catalog(CatalogRequest::ToolkitsGet { name: "missing".into() }, CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(protected, missing);
        assert_eq!(protected["error"]["code"], "not_found");
    }

    #[test]
    fn structured_content_wins_over_text() {
        let mut result = CallToolResult::structured(json!({ "answer": 42 }));
        result.content = vec![ContentBlock::text("fallback")];

        assert_eq!(
            tool_result_envelope(result),
            json!({ "ok": true, "data": { "answer": 42 }, "meta": { "omitted": 0 } })
        );
    }

    #[test]
    fn tool_errors_remain_branchable_values() {
        let result = CallToolResult::structured_error(json!({
            "error": { "code": "upstream_timeout", "message": "timed out", "retryable": true }
        }));

        assert_eq!(tool_result_envelope(result)["ok"], false);
        assert_eq!(
            tool_result_envelope(CallToolResult::structured_error(json!({"error": {"code": "x"}})))["error"]["code"],
            "x"
        );
    }
}
