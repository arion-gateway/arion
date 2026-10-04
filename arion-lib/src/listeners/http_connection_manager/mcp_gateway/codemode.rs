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

//! Sandboxed TypeScript execution for the MCP gateway.

mod dispatcher;
mod engine;
mod error;
mod hostcalls;
mod toolkits;
mod ts_strip;

pub use error::CodeModeError;
pub use toolkits::ToolkitRegistry;

use std::{
    sync::{Arc as StdArc, LazyLock},
    time::Duration,
};

use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpCodeMode;
use rmcp::model::{self, Tool};
use serde::Deserialize;
use serde_json::{json, Value};

use dispatcher::{CodeModeToolDispatcher, ToolCallRecord, ToolDispatcher};
use engine::{CodeModeEngine, CodeModeLimits, ExecOutcome, ExecuteRequest};

use super::tools::{
    serialized_json_size, CallToolError, NamespaceSummary, ToolBuilderError, ToolCallContext, ToolsRegistry,
    CODEMODE_EXECUTE_TOOL_NAME,
};
use crate::listeners::http_connection_manager::RequestCtx;

const INSTRUCTIONS: &str = "Use `execute` to run TypeScript that composes MCP tools. The execute tool description documents available toolkits and catalog discovery APIs. Invoke only exact qualified tool names.";

const EXECUTE_OPENING: &str = "Run TypeScript against a minimal SDK that can programmatically explore and discover tools available from this MCP server, invoke multiple tools, and compose their results.\n";
const TOOLKIT_INTRO: &str = "\nToolkits are available. A toolkit is a curated collection of tool definitions for a task, optionally with guidance for composing them. Prefer an available toolkit when it matches the work to be done.\n\nAvailable toolkits:\n";
const TOOLKIT_MORE: &str = "More toolkits are available through catalog.toolkits.list().\n";
const TOOLKIT_RETRIEVAL: &str = "\nRetrieve a toolkit with catalog.toolkits.get({name}).\n";
const NAMESPACE_HEADER: &str = "\nAvailable tool namespaces:\n";
const NAMESPACE_MORE: &str = "More namespaces are available through catalog.namespaces.list().\n";
const EXECUTE_GUIDANCE: &str = "\nTo programmatically discover tools and toolkits, use:\n- catalog.namespaces.list({limit?, offset?})\n- catalog.tools.search({query, namespace?, limit?})\n- catalog.tools.list({namespace, limit?, offset?})\n- catalog.tools.describe({namespace, name})\n- catalog.toolkits.list({limit?, offset?})\n- catalog.toolkits.get({name})\n\nInvoke only exact qualified names, preferably with tools[\"namespace.name\"](arguments).\n\nCalls return {ok:true,data,...} or {ok:false,error,...}. Branch on ok, use Promise.all for independent calls, and explicitly return the final value.";
const MAX_EXECUTE_DESCRIPTION_BYTES: usize = 4 * 1024;

static EXECUTE_INPUT_SCHEMA: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "type": "object",
        "properties": {
            "code": { "type": "string" },
            "timeout_ms": { "type": "integer", "minimum": 1 }
        },
        "required": ["code"],
        "additionalProperties": false
    })
});

static EXECUTE_INPUT_VALIDATOR: LazyLock<Result<jsonschema::Validator, String>> =
    LazyLock::new(|| jsonschema::Validator::new(&EXECUTE_INPUT_SCHEMA).map_err(|error| error.to_string()));

fn execute_description(
    toolkits: &[toolkits::ToolkitSummary],
    namespaces: &[NamespaceSummary],
    more_namespaces: bool,
) -> String {
    let mut description = String::from(EXECUTE_OPENING);
    if !toolkits.is_empty() {
        description.push_str(TOOLKIT_INTRO);
        let namespace_reserve = if namespaces.is_empty() { 0 } else { NAMESPACE_HEADER.len() + NAMESPACE_MORE.len() };
        let reserved = TOOLKIT_MORE.len() + TOOLKIT_RETRIEVAL.len() + namespace_reserve + EXECUTE_GUIDANCE.len();
        let mut omitted = false;
        for toolkit in toolkits {
            let entry = format!("- {} — {}\n", toolkit.name, toolkit.description);
            if description.len() + entry.len() + reserved > MAX_EXECUTE_DESCRIPTION_BYTES {
                omitted = true;
                break;
            }
            description.push_str(&entry);
        }
        if omitted {
            description.push_str(TOOLKIT_MORE);
        }
        description.push_str(TOOLKIT_RETRIEVAL);
    }

    if !namespaces.is_empty() {
        description.push_str(NAMESPACE_HEADER);
        let reserved = NAMESPACE_MORE.len() + EXECUTE_GUIDANCE.len();
        let mut omitted = more_namespaces;
        for namespace in namespaces {
            let entry = format!("- {}\n", namespace.namespace);
            if description.len() + entry.len() + reserved > MAX_EXECUTE_DESCRIPTION_BYTES {
                omitted = true;
                break;
            }
            description.push_str(&entry);
        }
        if omitted {
            description.push_str(NAMESPACE_MORE);
        }
    }
    description.push_str(EXECUTE_GUIDANCE);
    debug_assert!(description.len() <= MAX_EXECUTE_DESCRIPTION_BYTES);
    description
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecuteToolArguments {
    code: String,
    timeout_ms: Option<u64>,
}

/// The exclusive MCP surface selected by a code-mode gateway.
pub(crate) struct CodeMode {
    engine: CodeModeEngine,
    max_output_bytes: usize,
    toolkits: StdArc<ToolkitRegistry>,
    execute_input_validator: &'static jsonschema::Validator,
}

impl std::fmt::Debug for CodeMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodeMode")
            .field("max_output_bytes", &self.max_output_bytes)
            .field("toolkits", &self.toolkits)
            .finish_non_exhaustive()
    }
}

impl CodeMode {
    pub fn try_new(config: &McpCodeMode) -> Result<Self, ToolBuilderError> {
        let toolkits = ToolkitRegistry::with_config(config.toolkits.clone())
            .map_err(|error| ToolBuilderError::InvalidToolkit(error.to_string()))?;
        let limits = CodeModeLimits {
            execution_timeout: config.execution_timeout,
            memory_bytes: config.memory_limit_bytes,
            max_output_bytes: config.max_output_bytes,
            max_host_calls: config.max_host_calls,
            max_concurrent_executions: config.max_concurrent_executions,
            max_concurrent_tool_calls: config.max_concurrent_tool_calls,
            ..CodeModeLimits::default()
        };
        let engine = CodeModeEngine::try_new_with_artifact(limits, config.engine_artifact_path.as_deref())?;
        let execute_input_validator =
            EXECUTE_INPUT_VALIDATOR.as_ref().map_err(|error| CodeModeError::ExecuteSchema(error.clone()))?;
        Ok(Self {
            engine,
            max_output_bytes: config.max_output_bytes,
            toolkits: StdArc::new(toolkits),
            execute_input_validator,
        })
    }

    pub const fn instructions() -> &'static str {
        INSTRUCTIONS
    }

    pub fn toolkits(&self) -> &StdArc<ToolkitRegistry> {
        &self.toolkits
    }

    pub async fn list_tools(
        &self,
        tools: &ToolsRegistry,
        req_ext: &http::Extensions,
        req_ctx: &RequestCtx,
    ) -> Result<model::ListToolsResult, CallToolError> {
        tools.ensure_tools_current(req_ctx).await?;
        let toolkits = self.toolkits.visible_summaries(tools, req_ext);
        let namespaces = tools.catalog_namespaces(req_ext, Some(100), Some(0))?;
        let description = execute_description(&toolkits, &namespaces.items, namespaces.has_more);
        let Value::Object(input_schema) = &*EXECUTE_INPUT_SCHEMA else { unreachable!() };
        Ok(model::ListToolsResult {
            tools: vec![Tool::new(CODEMODE_EXECUTE_TOOL_NAME, description, StdArc::new(input_schema.clone()))],
            next_cursor: None,
            meta: None,
            ttl_ms: None,
            result_type: None,
            cache_scope: None,
        })
    }

    pub fn prepare_execute(&self, name: &str, arguments: Value) -> Result<ExecuteRequest, CallToolError> {
        if name != CODEMODE_EXECUTE_TOOL_NAME {
            return Err(CallToolError::ToolNotFound(name.to_owned()));
        }
        if let Some(error) = self.execute_input_validator.iter_errors(&arguments).next() {
            return Err(CallToolError::ValidationError(error.to_string()));
        }
        let arguments: ExecuteToolArguments = serde_json::from_value(arguments)
            .map_err(|error| CallToolError::ValidationError(format!("invalid execute arguments: {error}")))?;
        Ok(ExecuteRequest { code: arguments.code, timeout: arguments.timeout_ms.map(Duration::from_millis) })
    }

    pub async fn call_tool(
        &self,
        registry: &StdArc<ToolsRegistry>,
        cx: ToolCallContext<'_>,
        name: &str,
        arguments: Value,
    ) -> Result<model::CallToolResult, CallToolError> {
        let request = self.prepare_execute(name, arguments)?;
        let dispatcher =
            StdArc::new(CodeModeToolDispatcher::new(StdArc::clone(registry), StdArc::clone(&self.toolkits), cx));

        let outcome = self.engine.execute(request, StdArc::clone(&dispatcher) as StdArc<dyn ToolDispatcher>).await;
        if matches!(&outcome.result, Err(CodeModeError::ConcurrencyLimit)) {
            return Err(CallToolError::CodeModeConcurrencyLimit);
        }

        Ok(call_tool_result(outcome, dispatcher.take_tool_calls(), self.max_output_bytes))
    }
}

fn call_tool_result(
    outcome: ExecOutcome,
    tool_calls: Vec<ToolCallRecord>,
    max_output_bytes: usize,
) -> model::CallToolResult {
    let ExecOutcome { result, logs } = outcome;
    let (status, result, error, diagnostics, is_error) = match result {
        Ok(result) => ("completed", result, None, Vec::new(), Some(false)),
        Err(error) => {
            let diagnostic = error.diagnostic();
            ("error", Value::Null, Some(diagnostic.clone()), vec![diagnostic], Some(true))
        },
    };

    let parts = CodeModeResultParts { status, result, error, diagnostics, logs, tool_calls, is_error };
    let full = parts.build(parts.logs.len(), parts.tool_calls.len(), false);
    if serialized_len(&full) <= max_output_bytes {
        return full;
    }

    let empty = parts.build(0, 0, true);
    if serialized_len(&empty) > max_output_bytes {
        return CodeModeResultParts::output_too_large().build(0, 0, false);
    }

    let kept_tool_calls = largest_fitting_prefix(parts.tool_calls.len(), |count| {
        serialized_len(&parts.build(0, count, true)) <= max_output_bytes
    });
    let kept_logs = largest_fitting_prefix(parts.logs.len(), |count| {
        serialized_len(&parts.build(count, kept_tool_calls, true)) <= max_output_bytes
    });
    parts.build(kept_logs, kept_tool_calls, true)
}

struct CodeModeResultParts {
    status: &'static str,
    result: Value,
    error: Option<Value>,
    diagnostics: Vec<Value>,
    logs: Vec<String>,
    tool_calls: Vec<ToolCallRecord>,
    is_error: Option<bool>,
}

impl CodeModeResultParts {
    fn output_too_large() -> Self {
        let diagnostic = CodeModeError::ResultTooLarge.diagnostic();
        Self {
            status: "error",
            result: Value::Null,
            error: Some(diagnostic.clone()),
            diagnostics: vec![diagnostic],
            logs: Vec::new(),
            tool_calls: Vec::new(),
            is_error: Some(true),
        }
    }

    fn build(&self, kept_logs: usize, kept_tool_calls: usize, truncated: bool) -> model::CallToolResult {
        let logs = self.logs.get(..kept_logs).unwrap_or(self.logs.as_slice());
        let tool_calls = self.tool_calls.get(..kept_tool_calls).unwrap_or(self.tool_calls.as_slice());
        let value = json!({
            "status": self.status,
            "result": self.result,
            "error": self.error,
            "diagnostics": self.diagnostics,
            "logs": logs,
            "toolCalls": tool_calls
        });
        let mut result = if self.is_error == Some(true) {
            model::CallToolResult::structured_error(value)
        } else {
            model::CallToolResult::structured(value)
        };
        if truncated {
            result.meta = Some(model::MetaObject(serde_json::Map::from_iter([(
                "arion/codemodeTruncation".to_owned(),
                json!({
                    "logsOmitted": self.logs.len() - kept_logs,
                    "toolCallsOmitted": self.tool_calls.len() - kept_tool_calls
                }),
            )])));
        }
        result
    }
}

fn serialized_len(result: &model::CallToolResult) -> usize {
    serialized_json_size(result).unwrap_or(usize::MAX)
}

fn largest_fitting_prefix(total: usize, mut fits: impl FnMut(usize) -> bool) -> usize {
    let mut low = 0;
    let mut high = total;
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        if fits(middle) {
            low = middle;
        } else {
            high = middle - 1;
        }
    }
    low
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncates_metadata_within_the_final_output_limit() {
        let expected_answer = "x".repeat(100);
        let outcome =
            ExecOutcome { result: Ok(json!({ "answer": expected_answer })), logs: vec!["log".repeat(200); 4] };
        let tool_calls = vec![
            ToolCallRecord { seq: 1, tool: "crm.find_customer".to_owned(), ok: true, duration_ms: 1 },
            ToolCallRecord { seq: 2, tool: "support.create_case".to_owned(), ok: true, duration_ms: 2 },
        ];

        let result = call_tool_result(outcome, tool_calls, 1024);

        assert!(serialized_len(&result) <= 1024);
        assert!(serialized_len(&CodeModeResultParts::output_too_large().build(0, 0, false)) <= 1024);
        let structured = result.structured_content.as_ref().expect("structured result");
        assert_eq!(structured.pointer("/result/answer").and_then(Value::as_str), Some(expected_answer.as_str()));
        assert_eq!(structured.pointer("/toolCalls").and_then(Value::as_array).map(Vec::len), Some(2));
        assert!(structured.pointer("/logs").and_then(Value::as_array).is_some_and(|logs| logs.len() < 4));
        let truncation =
            result.meta.as_ref().and_then(|meta| meta.0.get("arion/codemodeTruncation")).expect("truncation metadata");
        assert!(truncation.get("logsOmitted").and_then(Value::as_u64).is_some_and(|count| count > 0));
        assert_eq!(truncation.get("toolCallsOmitted").and_then(Value::as_u64), Some(0));
    }
}
