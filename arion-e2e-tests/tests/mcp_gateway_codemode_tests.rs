// Copyright 2026 The arion-gateway Authors
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

//! MCP Gateway Code Mode end-to-end tests.
//!
//! Requires the arion binary to be built with the codemode feature:
//! `cargo build --features codemode` (or point `ARION_BIN` at such a build).

use arion_e2e_tests::config_builder::{
    inline_string_data_source, ClusterBuilder, EndpointBuilder, McpCodeModeBuilder, McpGatewayBuilder,
    McpGatewayHttpConfigBuilder, McpRestBackendBuilder, McpToolBuilder, McpToolkitBuilder,
};
use arion_e2e_tests::{ArionInstance, McpTestClient, PreConfiguredResponse, SpawnOptions, TestBackend};
use serde_json::json;

const SERVER_NAME: &str = "codemode-gateway";
const SERVER_VERSION: &str = "1.0.0";

#[tokio::test]
#[ignore]
async fn test_codemode_surface_and_toolkit_retrieval() {
    let gateway = McpGatewayBuilder::new(SERVER_NAME, SERVER_VERSION)
        .tool(
            McpToolBuilder::new("crm", "find_customer", "Find a customer by email or account ID")
                .input_schema(inline_string_data_source(r#"{"type":"object","properties":{}}"#))
                .rest_backend(McpRestBackendBuilder::new("crm_cluster", "GET", "/customer")),
        )
        .tool(
            McpToolBuilder::new("support", "create_case", "Create a support case")
                .input_schema(inline_string_data_source(r#"{"type":"object","properties":{}}"#))
                .rest_backend(McpRestBackendBuilder::new("support_cluster", "GET", "/case")),
        )
        .code_mode(
            McpCodeModeBuilder::new().toolkit(
                McpToolkitBuilder::new(
                    "open_customer_case",
                    "Find a customer and open a linked support case.",
                    "Find the customer first, then open a case using the returned customer ID.",
                )
                .tool("crm", "find_customer")
                .tool("support", "create_case"),
            ),
        );
    let bootstrap = McpGatewayHttpConfigBuilder::new(gateway).build_bootstrap([
        ClusterBuilder::new("crm_cluster").endpoint(EndpointBuilder::new("127.0.0.1", 1)),
        ClusterBuilder::new("support_cluster").endpoint(EndpointBuilder::new("127.0.0.1", 1)),
    ]);
    let config_path = bootstrap.build_to_temp().unwrap_or_else(|error| panic!("Failed to build config: {error}"));
    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .unwrap_or_else(|error| panic!("Failed to spawn Arion: {error}"));
    let listener_addr = arion.listener_addr().unwrap_or_else(|| panic!("Arion did not report its listener address"));
    let mut client = McpTestClient::new(listener_addr);
    client.initialize().await.unwrap_or_else(|error| panic!("Failed to initialize MCP client: {error}"));

    let tools = client.list_tools().await.unwrap_or_else(|error| panic!("Failed to list tools: {error}"));
    assert_eq!(tools.tools.len(), 1);
    let execute_tool = tools.tools.first().unwrap_or_else(|| panic!("CodeMode did not expose the execute tool"));
    assert_eq!(execute_tool.name, "execute");
    let description = execute_tool.description.as_str();
    assert!(description.contains("open_customer_case"), "toolkit missing from description: {description}");
    assert!(description.contains("- crm"), "crm namespace missing from description: {description}");
    assert!(description.contains("- support"), "support namespace missing from description: {description}");

    let result = client
        .call_tool("execute", json!({ "code": "return await catalog.toolkits.get({name: 'open_customer_case'})" }))
        .await
        .unwrap_or_else(|error| panic!("Failed to execute toolkit retrieval: {error}"));
    let structured = result.structured_content.unwrap_or_else(|| panic!("execute did not return structured content"));
    assert_eq!(structured.get("status").and_then(serde_json::Value::as_str), Some("completed"));
    let toolkit = structured.pointer("/result/data").unwrap_or_else(|| panic!("execute did not return toolkit data"));
    assert_eq!(toolkit.get("name").and_then(serde_json::Value::as_str), Some("open_customer_case"));
    let content = toolkit
        .get("content")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("toolkit content was not a string"));
    assert!(content.starts_with("Find the customer first, then open a case using the returned customer ID."));
    assert!(content.contains("tools[\"crm.find_customer\"]"));
    assert!(content.contains("tools[\"support.create_case\"]"));
    assert!(content.contains("Input schema:"));

    arion.shutdown();
    std::fs::remove_file(config_path).unwrap_or_else(|error| panic!("Failed to remove temporary config: {error}"));
}

#[tokio::test]
#[ignore]
async fn test_codemode_composes_tools_and_records_calls() {
    let mut crm_backend =
        TestBackend::start().await.unwrap_or_else(|error| panic!("Failed to start CRM backend: {error}"));
    let support_backend =
        TestBackend::start().await.unwrap_or_else(|error| panic!("Failed to start support backend: {error}"));
    crm_backend.set_default_response(PreConfiguredResponse::with_body(r#"{"id": "c-1"}"#)).await;
    support_backend.set_default_response(PreConfiguredResponse::with_body(r#"{"case": "opened"}"#)).await;

    let gateway = McpGatewayBuilder::new(SERVER_NAME, SERVER_VERSION)
        .tool(
            McpToolBuilder::new("crm", "find_customer", "Find a customer by email or account ID")
                .input_schema(inline_string_data_source(r#"{"type":"object","properties":{}}"#))
                .rest_backend(McpRestBackendBuilder::new("crm_cluster", "GET", "/customer")),
        )
        .tool(
            McpToolBuilder::new("support", "create_case", "Create a support case")
                .input_schema(inline_string_data_source(r#"{"type":"object","properties":{}}"#))
                .rest_backend(McpRestBackendBuilder::new("support_cluster", "GET", "/case")),
        )
        .code_mode(McpCodeModeBuilder::new());
    let bootstrap = McpGatewayHttpConfigBuilder::new(gateway).build_bootstrap([
        ClusterBuilder::new("crm_cluster").endpoint(EndpointBuilder::from_socket_addr(crm_backend.addr())),
        ClusterBuilder::new("support_cluster").endpoint(EndpointBuilder::from_socket_addr(support_backend.addr())),
    ]);
    let config_path = bootstrap.build_to_temp().unwrap_or_else(|error| panic!("Failed to build config: {error}"));
    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .unwrap_or_else(|error| panic!("Failed to spawn Arion: {error}"));
    let listener_addr = arion.listener_addr().unwrap_or_else(|| panic!("Arion did not report its listener address"));
    let mut client = McpTestClient::new(listener_addr);
    client.initialize().await.unwrap_or_else(|error| panic!("Failed to initialize MCP client: {error}"));

    let code = r#"
        const customer = await tools["crm.find_customer"]({});
        if (!customer.ok) return customer;
        const created = await tools.support.create_case({});
        if (!created.ok) return created;
        console.log("case opened for", customer.data);
        return { customer: customer.data, created: created.data };
    "#;
    let result = client
        .call_tool("execute", json!({ "code": code }))
        .await
        .unwrap_or_else(|error| panic!("Failed to execute composed tool calls: {error}"));
    let structured = result.structured_content.unwrap_or_else(|| panic!("execute did not return structured content"));

    assert_eq!(structured.get("status").and_then(serde_json::Value::as_str), Some("completed"), "{structured}");
    assert!(structured
        .pointer("/result/customer")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|v| v.contains("c-1")));
    assert!(structured
        .pointer("/result/created")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|v| v.contains("opened")));

    let tool_calls = structured
        .get("toolCalls")
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("execute did not return a toolCalls array"));
    assert_eq!(
        tool_calls.iter().filter_map(|call| call.get("tool").and_then(serde_json::Value::as_str)).collect::<Vec<_>>(),
        ["crm.find_customer", "support.create_case"]
    );
    assert!(
        tool_calls.iter().all(|call| call.get("ok").and_then(serde_json::Value::as_bool).unwrap_or_default()),
        "{tool_calls:?}"
    );

    let logs = structured
        .get("logs")
        .and_then(serde_json::Value::as_array)
        .unwrap_or_else(|| panic!("execute did not return a logs array"));
    assert_eq!(logs.len(), 1);
    let log =
        logs.first().and_then(serde_json::Value::as_str).unwrap_or_else(|| panic!("execute log was not a string"));
    assert!(log.starts_with("case opened for"), "{logs:?}");

    let request = crm_backend
        .await_request()
        .await
        .unwrap_or_else(|error| panic!("CRM backend did not receive a tool call: {error}"));
    assert_eq!(request.path(), "/customer");

    arion.shutdown();
    std::fs::remove_file(config_path).unwrap_or_else(|error| panic!("Failed to remove temporary config: {error}"));
}

#[tokio::test]
#[ignore]
async fn test_codemode_search_describe_call_flow() {
    let crm_backend = TestBackend::start().await.unwrap_or_else(|error| panic!("Failed to start CRM backend: {error}"));
    crm_backend.set_default_response(PreConfiguredResponse::with_body(r#"{"id": "c-1"}"#)).await;

    let gateway = McpGatewayBuilder::new(SERVER_NAME, SERVER_VERSION)
        .tool(
            McpToolBuilder::new("crm", "find_customer", "Find a customer by email or account ID")
                .input_schema(inline_string_data_source(r#"{"type":"object","properties":{}}"#))
                .rest_backend(McpRestBackendBuilder::new("crm_cluster", "GET", "/customer")),
        )
        .code_mode(McpCodeModeBuilder::new());
    let bootstrap = McpGatewayHttpConfigBuilder::new(gateway).build_bootstrap([
        ClusterBuilder::new("crm_cluster").endpoint(EndpointBuilder::from_socket_addr(crm_backend.addr()))
    ]);
    let config_path = bootstrap.build_to_temp().unwrap_or_else(|error| panic!("Failed to build config: {error}"));
    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .unwrap_or_else(|error| panic!("Failed to spawn Arion: {error}"));
    let listener_addr = arion.listener_addr().unwrap_or_else(|| panic!("Arion did not report its listener address"));
    let mut client = McpTestClient::new(listener_addr);
    client.initialize().await.unwrap_or_else(|error| panic!("Failed to initialize MCP client: {error}"));

    let code = r#"
        const found = await catalog.tools.search({query: "find a customer", namespace: "crm", limit: 5});
        if (!found.ok) return found;
        const first = found.data.items[0];
        const described = await catalog.tools.describe({namespace: first.namespace, name: first.name});
        if (!described.ok) return described;
        const called = await tools[described.data.qualifiedName]({});
        return { qualifiedName: described.data.qualifiedName, hasSchema: !!described.data.inputSchema, ok: called.ok };
    "#;
    let result = client
        .call_tool("execute", json!({ "code": code }))
        .await
        .unwrap_or_else(|error| panic!("Failed to execute search/describe flow: {error}"));
    let structured = result.structured_content.unwrap_or_else(|| panic!("execute did not return structured content"));

    assert_eq!(structured.get("status").and_then(serde_json::Value::as_str), Some("completed"), "{structured}");
    assert_eq!(
        structured.pointer("/result/qualifiedName").and_then(serde_json::Value::as_str),
        Some("crm.find_customer")
    );
    assert!(structured.pointer("/result/hasSchema").and_then(serde_json::Value::as_bool).unwrap_or_default());
    assert!(structured.pointer("/result/ok").and_then(serde_json::Value::as_bool).unwrap_or_default());

    arion.shutdown();
    std::fs::remove_file(config_path).unwrap_or_else(|error| panic!("Failed to remove temporary config: {error}"));
}
