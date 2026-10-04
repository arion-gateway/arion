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

use std::sync::Arc as StdArc;

use serde::Deserialize;
use serde_json::Value;
use wasmtime::component::{HasSelf, Linker};

use super::dispatcher::{error_envelope, CatalogRequest, DispatchDefect};
use super::engine::ExecState;
use crate::listeners::http_connection_manager::mcp_gateway::tools::ToolId;

wasmtime::component::bindgen!({
    path: "../arion-sandbox/wit",
    world: "sandbox",
    imports: { "arion:codemode/host.next-completion": async },
    exports: { default: async },
});

pub(super) use exports::arion::codemode::runner::ExecutionError;

/// Imports allowed for compatible artifacts; capabilities still come from the
/// per-execution `WasiCtx`.
pub(super) const REQUIRED_COMPONENT_IMPORTS: &[&str] = &[
    "wasi:cli/environment@0.2.6",
    "wasi:cli/exit@0.2.6",
    "wasi:cli/stderr@0.2.6",
    "wasi:cli/stdin@0.2.6",
    "wasi:cli/stdout@0.2.6",
    "wasi:cli/terminal-input@0.2.6",
    "wasi:cli/terminal-output@0.2.6",
    "wasi:cli/terminal-stderr@0.2.6",
    "wasi:cli/terminal-stdin@0.2.6",
    "wasi:cli/terminal-stdout@0.2.6",
    "wasi:clocks/monotonic-clock@0.2.6",
    "wasi:clocks/wall-clock@0.2.6",
    "wasi:filesystem/preopens@0.2.6",
    "wasi:filesystem/types@0.2.6",
    "wasi:io/error@0.2.6",
    "wasi:io/streams@0.2.6",
    "wasi:random/random@0.2.6",
    "arion:codemode/host@0.1.0",
];

pub(super) fn register(linker: &mut Linker<ExecState>) -> Result<(), wasmtime::Error> {
    wasmtime_wasi::p2::add_to_linker_async(&mut *linker)?;
    arion::codemode::host::add_to_linker::<_, HasSelf<_>>(linker, |state| state)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
struct ToolCallRequest {
    qualified_name: String,
    arguments: Value,
}

impl arion::codemode::host::Host for ExecState {
    fn request(&mut self, kind: String, payload: String) -> Result<String, String> {
        if !self.consume_host_call() {
            return Err("maximum host-call count exceeded".to_owned());
        }
        if kind == "log" {
            self.push_log(payload);
            return Ok("null".to_owned());
        }
        Err("synchronous host requests support only \"log\"".to_owned())
    }

    fn submit(&mut self, kind: String, payload: String) -> Result<u64, String> {
        if !self.consume_host_call() {
            return Err("maximum host-call count exceeded".to_owned());
        }
        let id = self.next_call_seq();
        let dispatcher = StdArc::clone(&self.dispatcher);
        let cancel = self.cancel.child_token();
        let permits = StdArc::clone(&self.dispatch_permits);
        let max_output_bytes = self.limits.max_output_bytes;
        self.spawn_dispatch(id, async move {
            let Ok(_permit) = permits.acquire_owned().await else {
                return Err("execution was cancelled".to_owned());
            };
            let result = match kind.as_str() {
                "tool_call" => match parse_tool_call(&payload) {
                    Ok((tool, args)) => dispatcher.call_tool(id, tool, args, cancel).await,
                    Err(value) => Ok(value),
                },
                "catalog" => match serde_json::from_str::<CatalogRequest>(&payload) {
                    Ok(request) => dispatcher.catalog(request, cancel).await,
                    Err(error) => Ok(invalid_input(&format!("invalid catalog request: {error}"))),
                },
                _ => Ok(invalid_input("unknown host request kind")),
            };
            finalize_dispatch_result(result, max_output_bytes)
        });
        Ok(id)
    }

    async fn next_completion(&mut self) -> Result<Option<(u64, Result<String, String>)>, String> {
        self.wait_for_completion().await
    }
}

/// Splits a `tool_call` payload into its dispatchable parts; the error side is
/// the branchable value to complete the call with instead of dispatching.
fn parse_tool_call(payload: &str) -> Result<(ToolId, Value), Value> {
    let request: ToolCallRequest = match serde_json::from_str(payload) {
        Ok(request) => request,
        Err(error) => return Err(invalid_input(&format!("invalid tool call: {error}"))),
    };
    match ToolId::parse_qualified(&request.qualified_name) {
        Some(id) => Ok((id, request.arguments)),
        None => {
            Err(error_envelope("tool_not_found", &format!("Tool '{}' was not found", request.qualified_name), false))
        },
    }
}

/// Serializes a dispatch outcome and applies the per-call output limit; the
/// error side is an infrastructure failure that rejects the pending promise.
fn finalize_dispatch_result(result: Result<Value, DispatchDefect>, max_output_bytes: usize) -> Result<String, String> {
    match result {
        Ok(value) => {
            let json = serde_json::to_string(&value).map_err(|error| error.to_string())?;
            if json.len() > max_output_bytes {
                serde_json::to_string(&error_envelope(
                    "result_too_large",
                    "The result exceeded the configured output limit.",
                    false,
                ))
                .map_err(|error| error.to_string())
            } else {
                Ok(json)
            }
        },
        Err(DispatchDefect::Cancelled) => Err("execution was cancelled".to_owned()),
        Err(DispatchDefect::InvalidResponse) => Err("host returned an invalid response".to_owned()),
    }
}

fn invalid_input(message: &str) -> Value {
    error_envelope("invalid_input", message, false)
}
