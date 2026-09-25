// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::let_underscore_must_use, clippy::str_to_string)]

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use http::StatusCode;
use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::Tool as OrionMcpTool;
use orion_e2e_tests::config_builder::{
    inline_string_data_source, ClusterBuilder, EndpointBuilder, McpGatewayBuilder, McpGatewayHttpConfigBuilder,
    McpRestBackendBuilder, McpSemanticSearchBuilder, McpToolBuilder,
};
use orion_e2e_tests::{CallToolResult, McpTestClient, OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend};
use serde_json::{json, Value};

const LISTENER_NAME: &str = "mcp_semantic_search_demo";
const SAMPLE_QUERY: &str = "I need the weather forecast and temperature";

const WEATHER_CLUSTER: &str = "weather_backend";
const USERS_CLUSTER: &str = "users_backend";
const PAYMENTS_CLUSTER: &str = "payments_backend";
const ADMIN_CLUSTER: &str = "admin_backend";

#[derive(Debug, Parser)]
#[command(name = "mcp_semantic_search")]
#[command(about = "Run an interactive Orion MCP Gateway semantic-search demo backed by built-in BM25 ranking.")]
struct Args {
    #[arg(long, default_value_t = 2, value_name = "N")]
    top_k: u32,

    #[arg(long)]
    assisted_discovery: bool,

    #[arg(long)]
    verbose_orion: bool,

    #[arg(long)]
    keep_config: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    validate_args(&args)?;

    let backends = DemoBackends::start().await?;
    let config_path = build_config(&args, &backends)?;
    let spawn_options = spawn_options(&args);
    let orion = OrionInstance::spawn_auto_port(&config_path, LISTENER_NAME, spawn_options).await?;
    let listener_addr = orion
        .listener_addr()
        .ok_or_else(|| io::Error::new(io::ErrorKind::AddrNotAvailable, "Orion did not report a listener address"))?;

    print_startup(&args, &backends, &config_path, listener_addr);

    let mut client = McpTestClient::new(listener_addr);
    client.initialize().await?;
    let initial_tools = client.list_tools().await?;
    println!("Initial tools/list: {}", format_names(initial_tools.tools.iter().map(|tool| tool.name.as_str())));

    let search_result = client.call_tool("semantic_search", json!({ "user_query": SAMPLE_QUERY })).await?;
    if args.assisted_discovery {
        let updated_tools = client.list_tools().await?;
        println!(
            "Sample semantic_search matched tools/list entries: {}",
            format_names(updated_tools.tools.iter().map(|tool| tool.name.as_str()))
        );
    } else {
        let names = semantic_result_tool_names(&search_result);
        println!("Sample semantic_search result: {}", format_names(names.iter().map(String::as_str)));
    }

    println!();
    println!("Press Ctrl-C to stop Orion and the demo backends.");
    let ctrl_c = tokio::signal::ctrl_c().await;

    orion.shutdown();
    if !args.keep_config {
        let _ = std::fs::remove_file(&config_path);
    }

    ctrl_c?;
    Ok(())
}

fn validate_args(args: &Args) -> Result<(), io::Error> {
    if args.top_k == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "--top-k must be greater than zero"));
    }
    Ok(())
}

fn spawn_options(args: &Args) -> SpawnOptions {
    let options = SpawnOptions::default().with_ready_timeout(Duration::from_secs(300));
    if args.verbose_orion {
        options.with_verbose()
    } else {
        options
    }
}

fn build_config(args: &Args, backends: &DemoBackends) -> orion_e2e_tests::Result<PathBuf> {
    let gateway = McpGatewayBuilder::new("orion-mcp-local-demo", "1.0.0")
        .tools(demo_tools())
        .semantic_search(McpSemanticSearchBuilder::new().assisted_discovery(args.assisted_discovery).top_k(args.top_k));

    McpGatewayHttpConfigBuilder::new(gateway)
        .listener(LISTENER_NAME, 0)
        .build_bootstrap(backends.clusters())
        .build_to_temp()
}

struct DemoBackends {
    weather: TestBackend,
    users: TestBackend,
    payments: TestBackend,
    admin: TestBackend,
}

impl DemoBackends {
    async fn start() -> orion_e2e_tests::Result<Self> {
        let weather = TestBackend::start().await?;
        weather
            .set_default_response(json_response(
                r#"{"temperature_c":21,"condition":"sunny","forecast":"clear skies through the afternoon"}"#,
            ))
            .await;

        let users = TestBackend::start().await?;
        users
            .set_default_response(json_response(r#"{"id":"42","name":"Demo User","plan":"pro","status":"active"}"#))
            .await;

        let payments = TestBackend::start().await?;
        payments
            .set_default_response(json_response(
                r#"{"status":"accepted","transaction_id":"txn_demo_123","currency":"EUR"}"#,
            ))
            .await;

        let admin = TestBackend::start().await?;
        admin.set_default_response(json_response(r#"{"deleted":"42","audit_id":"audit_demo_456"}"#)).await;

        Ok(Self { weather, users, payments, admin })
    }

    fn clusters(&self) -> Vec<ClusterBuilder> {
        vec![
            ClusterBuilder::new(WEATHER_CLUSTER).endpoint(EndpointBuilder::from_socket_addr(self.weather.addr())),
            ClusterBuilder::new(USERS_CLUSTER).endpoint(EndpointBuilder::from_socket_addr(self.users.addr())),
            ClusterBuilder::new(PAYMENTS_CLUSTER).endpoint(EndpointBuilder::from_socket_addr(self.payments.addr())),
            ClusterBuilder::new(ADMIN_CLUSTER).endpoint(EndpointBuilder::from_socket_addr(self.admin.addr())),
        ]
    }
}

fn json_response(body: &'static str) -> PreConfiguredResponse {
    PreConfiguredResponse {
        status: StatusCode::OK,
        headers: vec![("content-type".to_owned(), "application/json".to_owned())],
        body: Bytes::from_static(body.as_bytes()),
        delay: None,
    }
}

fn demo_tools() -> Vec<OrionMcpTool> {
    vec![
        McpToolBuilder::new("get_weather", "Get weather forecast, temperature, and sky condition information")
            .input_schema(inline_string_data_source(r#"{"type":"object","properties":{}}"#))
            .rest_backend(McpRestBackendBuilder::new(WEATHER_CLUSTER, "GET", "/api/weather"))
            .build(),
        McpToolBuilder::new("get_user", "Retrieve user profile, account plan, and status information")
            .input_schema(inline_string_data_source(
                r#"{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}"#,
            ))
            .rest_backend(McpRestBackendBuilder::new(USERS_CLUSTER, "GET", "/api/users/{{id}}"))
            .build(),
        McpToolBuilder::new("process_payment", "Process payment transactions, billing requests, and invoices")
            .input_schema(inline_string_data_source(
                r#"{"type":"object","properties":{"amount":{"type":"number"},"currency":{"type":"string"}},"required":["amount","currency"]}"#,
            ))
            .rest_backend(McpRestBackendBuilder::new(PAYMENTS_CLUSTER, "POST", "/api/payments"))
            .build(),
        McpToolBuilder::new("admin_delete_user", "Delete a user account as an administrator and write an audit record")
            .input_schema(inline_string_data_source(
                r#"{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}"#,
            ))
            .rest_backend(McpRestBackendBuilder::new(ADMIN_CLUSTER, "DELETE", "/api/admin/users/{{id}}"))
            .build(),
    ]
}

fn print_startup(args: &Args, backends: &DemoBackends, config_path: &std::path::Path, listener_addr: SocketAddr) {
    println!("Orion MCP semantic-search demo is running");
    println!("Config: {}", config_path.display());
    println!("MCP endpoint: http://{listener_addr}/mcp");
    println!("Ranking: BM25");
    println!("Top K: {}", args.top_k);
    println!("Assisted discovery: {}", args.assisted_discovery);
    println!();
    println!("Backends:");
    println!("  weather:  http://{}", backends.weather.addr());
    println!("  users:    http://{}", backends.users.addr());
    println!("  payments: http://{}", backends.payments.addr());
    println!("  admin:    http://{}", backends.admin.addr());
    println!();
    print_curl_examples(listener_addr);
}

fn print_curl_examples(listener_addr: SocketAddr) {
    let initialize = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": { "name": "demo-client", "version": "1.0.0" }
        }
    });
    let list_tools = json!({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}});
    let semantic_search = json!({
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {
            "name": "semantic_search",
            "arguments": { "user_query": SAMPLE_QUERY }
        }
    });

    println!("Manual curl smoke test");
    println!("----------------------");
    println!("Copy and paste this block into another terminal.");
    println!("It captures the real mcp-session-id from initialize; <session-id> is only a placeholder.");
    println!();
    println!("# 1. Initialize the MCP session and capture the session id.");
    println!("MCP_ENDPOINT=http://{listener_addr}/mcp");
    println!("INIT_RESPONSE=$(mktemp)");
    println!();
    println!("curl -i -s \"$MCP_ENDPOINT\" \\");
    println!("  -H 'content-type: application/json' \\");
    println!("  -H 'accept: text/event-stream, application/json' \\");
    println!("  -d '{initialize}' > \"$INIT_RESPONSE\"");
    println!();
    println!("SESSION_ID=$(awk 'tolower($1) == \"mcp-session-id:\" {{ print $2 }}' \"$INIT_RESPONSE\" | tr -d '\\r')");
    println!("echo");
    println!("echo '== initialize response =='");
    println!("cat \"$INIT_RESPONSE\"");
    println!("test -n \"$SESSION_ID\" || {{ echo 'missing mcp-session-id from initialize' >&2; exit 1; }}");
    println!();
    println!("# 2. List tools using the captured session id.");
    println!("echo");
    println!("echo '== tools/list =='");
    println!("curl -s \"$MCP_ENDPOINT\" \\");
    println!("  -H 'content-type: application/json' \\");
    println!("  -H 'accept: text/event-stream, application/json' \\");
    println!("  -H \"mcp-session-id: $SESSION_ID\" \\");
    println!("  -d '{list_tools}'");
    println!();
    println!("# 3. Run semantic search using the same session id.");
    println!("echo");
    println!("echo '== semantic_search =='");
    println!("curl -s \"$MCP_ENDPOINT\" \\");
    println!("  -H 'content-type: application/json' \\");
    println!("  -H 'accept: text/event-stream, application/json' \\");
    println!("  -H \"mcp-session-id: $SESSION_ID\" \\");
    println!("  -d '{semantic_search}'");
    println!("echo");
    println!();
    println!("# 4. Clean up the temporary response file.");
    println!("rm -f \"$INIT_RESPONSE\"");
    println!();
}

fn semantic_result_tool_names(result: &CallToolResult) -> Vec<String> {
    let Some(content) = result.content.first() else {
        return Vec::new();
    };

    let Ok(returned_tools) = serde_json::from_str::<Value>(&content.text) else {
        return Vec::new();
    };

    returned_tools
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(ToOwned::to_owned))
        .collect()
}

fn format_names<'a>(names: impl IntoIterator<Item = &'a str>) -> String {
    let names: Vec<&str> = names.into_iter().collect();
    if names.is_empty() {
        "(none)".to_owned()
    } else {
        names.join(", ")
    }
}
