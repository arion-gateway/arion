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

//! MCP Gateway semantic-search E2E tests.
//!
//! These tests intentionally use remote embeddings with a service
//! supplied by the test harness instead of fastembed/local models so they remain offline-safe.

#![allow(
    clippy::let_underscore_must_use,
    clippy::indexing_slicing,
    clippy::assertions_on_result_states,
    clippy::expect_used,
    clippy::cast_precision_loss,
    clippy::single_match,
    clippy::manual_assert,
    clippy::cast_possible_truncation
)]

use arion_data_plane_api::envoy_data_plane_api::arion::extensions::filters::http::mcp::mcp_gateway::v3::{
    DisclosureMode, Tool as ArionMcpTool,
};
use arion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, McpGatewayBuilder, McpGatewayHttpConfigBuilder,
    McpRestBackendBuilder, McpToolBuilder, McpToolSearchBuilder, inline_string_data_source,
};
use arion_e2e_tests::{
    ArionInstance, CallToolResult, CapturedEmbeddingsTestRequest, EmbeddingsTestService, McpTestClient,
    PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
};
use http::Method;
use serde_json::{Value, json};
use std::time::Duration;

const BACKEND_CLUSTER: &str = "backend_cluster";
const EMBEDDINGS_CLUSTER: &str = "embedding_cluster";
const TEST_EMBEDDINGS_MODEL: &str = "test-embedding-model";
const TEST_EMBEDDINGS_PATH: &str = "/v1/embeddings";
const TEST_EMBEDDINGS_DIMENSIONS: u32 = 3;
const TEST_TOOL_NAMESPACE: &str = "test";

#[derive(Clone, Copy)]
struct TestTool {
    name: &'static str,
    description: &'static str,
    method: &'static str,
    path: &'static str,
    embedding: Option<[f64; 3]>,
}

impl TestTool {
    fn with_embedding(mut self, embedding: [f64; 3]) -> Self {
        self.embedding = Some(embedding);
        self
    }

    fn into_config(self) -> ArionMcpTool {
        let mut builder = McpToolBuilder::new(TEST_TOOL_NAMESPACE, self.name, self.description)
            .input_schema(inline_string_data_source(r#"{"type":"object","properties":{}}"#))
            .rest_backend(McpRestBackendBuilder::new(BACKEND_CLUSTER, self.method, self.path));

        if let Some(embedding) = self.embedding {
            builder = builder.embedding(embedding.into_iter().map(|n| n as f32));
        }

        builder.build()
    }
}

const GET_WEATHER_TOOL: TestTool = TestTool {
    name: "get_weather",
    description: "Get current weather forecast and temperature information",
    method: "GET",
    path: "/api/weather",
    embedding: None,
};

const GET_USER_TOOL: TestTool = TestTool {
    name: "get_user",
    description: "Retrieve user profile and account information",
    method: "GET",
    path: "/api/user",
    embedding: None,
};

const PROCESS_PAYMENT_TOOL: TestTool = TestTool {
    name: "process_payment",
    description: "Process payment transaction and billing",
    method: "POST",
    path: "/api/payment",
    embedding: None,
};

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_semantic_search_with_explicit_embeddings() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)).await;
    let mut embeddings = EmbeddingsTestService::start().await.expect("Failed to start embeddings test service");

    let tools = tool_configs([
        GET_WEATHER_TOOL.with_embedding([1.0, 0.0, 0.0]),
        GET_USER_TOOL.with_embedding([0.0, 1.0, 0.0]),
        PROCESS_PAYMENT_TOOL.with_embedding([0.0, 0.0, 1.0]),
    ]);
    let bootstrap = mcp_gateway_with_remote_semantic_search_bootstrap(tools, &backend, &embeddings, 1);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", semantic_spawn_options())
        .await
        .expect("Failed to spawn Arion");

    let mut client = McpTestClient::new(arion.listener_addr().unwrap());
    client.initialize().await.expect("Failed to initialize");

    let listed = client.list_tools().await.expect("Failed to list tools");
    let listed_names: Vec<_> = listed.tools.iter().map(|tool| tool.name.as_str()).collect();
    assert_eq!(listed_names, vec!["tool_search"]);
    assert!(embeddings.try_recv_request().is_none(), "explicit tool embeddings should avoid bootstrap embedding");

    let result = client
        .call_tool("tool_search", json!({ "query": "I need to check the weather forecast" }))
        .await
        .expect("Failed to call tool_search");
    assert!(!result.is_error.unwrap_or(false), "tool_search call should succeed");

    let returned_tools: Value = serde_json::from_str(&result.content[0].text).unwrap();
    assert!(returned_tools[0].get("description").is_some());
    assert!(returned_tools[0].get("inputSchema").is_some());
    let names = semantic_result_tool_names(&result);
    assert_eq!(names, vec!["test.get_weather"]);

    let query_request = embeddings.await_request().await.expect("No query embedding request received");
    assert_embedding_request(&query_request);
    assert_eq!(query_request.input, vec!["I need to check the weather forecast"]);
    assert!(embeddings.try_recv_request().is_none(), "semantic search should only embed the query");

    let call_result =
        client.call_tool("test.process_payment", json!({})).await.expect("Failed to call process_payment");
    assert!(!call_result.is_error.unwrap_or(false), "unlisted tools should remain callable by name");
    let request = backend.await_request().await.expect("No request to REST backend");
    assert_eq!(request.path(), "/api/payment");

    arion.shutdown();
    embeddings.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_semantic_search_with_remote_embeddings() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)).await;
    let mut embeddings = EmbeddingsTestService::start().await.expect("Failed to start embeddings test service");

    let tools = tool_configs([GET_WEATHER_TOOL, GET_USER_TOOL, PROCESS_PAYMENT_TOOL]);
    let bootstrap = mcp_gateway_with_remote_semantic_search_bootstrap(tools, &backend, &embeddings, 1);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", semantic_spawn_options())
        .await
        .expect("Failed to spawn Arion");

    let mut client = McpTestClient::new(arion.listener_addr().unwrap());
    client.initialize().await.expect("Failed to initialize");

    let listed = client.list_tools().await.expect("Failed to list tools");
    let listed_names: Vec<_> = listed.tools.iter().map(|tool| tool.name.as_str()).collect();
    assert_eq!(listed_names, vec!["tool_search"]);

    let batch_request = embeddings.await_request().await.expect("No tool batch embedding request received");
    assert_embedding_request(&batch_request);
    assert_eq!(batch_request.input.len(), 3);
    assert!(batch_request.input.iter().any(|input| input.contains("get_weather")));
    assert!(batch_request.input.iter().any(|input| input.contains("get_user")));
    assert!(batch_request.input.iter().any(|input| input.contains("process_payment")));

    let result = client
        .call_tool("tool_search", json!({ "query": "I need to check the weather forecast" }))
        .await
        .expect("Failed to call tool_search");
    assert!(!result.is_error.unwrap_or(false), "tool_search call should succeed");

    let query_request = embeddings.await_request().await.expect("No query embedding request received");
    assert_embedding_request(&query_request);
    assert_eq!(query_request.input, vec!["I need to check the weather forecast"]);

    let names = semantic_result_tool_names(&result);
    assert_eq!(names, vec!["test.get_weather"]);

    arion.shutdown();
    embeddings.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_rest_search_is_sessionless_includes_both_modes_and_validates_http() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let mut progressive = GET_WEATHER_TOOL.into_config();
    progressive.disclosure_mode = DisclosureMode::Progressive.into();
    let mut eager = GET_USER_TOOL.into_config();
    eager.disclosure_mode = DisclosureMode::Eager.into();
    let gateway = McpGatewayBuilder::new("test-gateway", "1.0.0")
        .tools([progressive, eager])
        .search(McpToolSearchBuilder::new().max_allowable_results(3));
    let bootstrap = McpGatewayHttpConfigBuilder::new(gateway).build_bootstrap(vec![
        ClusterBuilder::new(BACKEND_CLUSTER).endpoint(EndpointBuilder::from_socket_addr(backend.addr())),
    ]);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");
    let arion = ArionInstance::spawn_auto_port(&config_path, "http", semantic_spawn_options())
        .await
        .expect("Failed to spawn Arion");
    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client
        .send(
            RequestBuilder::post("/mcp/search")
                .header("content-type", "application/json")
                .body(r#"{"query":"weather or user","limit":20}"#),
        )
        .await
        .unwrap();
    response.assert_status(http::StatusCode::OK);
    response.assert_header("content-type", "application/json");
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    let tools = body["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 2);
    assert!(tools.iter().all(|tool| tool.get("inputSchema").is_some()));

    for body in [r#"{"query":""}"#, r#"{"query":"x","limit":0}"#, "not-json"] {
        let response = client
            .send(RequestBuilder::post("/mcp/search").header("content-type", "application/json").body(body))
            .await
            .unwrap();
        response.assert_status(http::StatusCode::BAD_REQUEST);
    }
    client.get("/mcp/search").await.unwrap().assert_status(http::StatusCode::METHOD_NOT_ALLOWED);

    arion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_rest_search_works_when_every_tool_is_eager_without_semantic_config() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let mut tool = GET_WEATHER_TOOL.into_config();
    tool.disclosure_mode = DisclosureMode::Eager.into();
    let gateway = McpGatewayBuilder::new("test-gateway", "1.0.0").tool(tool);
    let bootstrap = McpGatewayHttpConfigBuilder::new(gateway).build_bootstrap(vec![
        ClusterBuilder::new(BACKEND_CLUSTER).endpoint(EndpointBuilder::from_socket_addr(backend.addr())),
    ]);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");
    let arion = ArionInstance::spawn_auto_port(&config_path, "http", semantic_spawn_options())
        .await
        .expect("Failed to spawn Arion");
    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::post("/mcp/search").body(r#"{"query":"weather"}"#)).await.unwrap();
    response.assert_status(http::StatusCode::OK);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["tools"].as_array().unwrap().len(), 1);

    arion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_rest_search_returns_ok_for_an_empty_catalog() {
    let gateway = McpGatewayBuilder::new("test-gateway", "1.0.0");
    let bootstrap = McpGatewayHttpConfigBuilder::new(gateway).build_bootstrap(std::iter::empty::<ClusterBuilder>());
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");
    let arion = ArionInstance::spawn_auto_port(&config_path, "http", semantic_spawn_options())
        .await
        .expect("Failed to spawn Arion");
    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::post("/mcp/search").body(r#"{"query":"weather"}"#)).await.unwrap();
    response.assert_status(http::StatusCode::OK);
    let body: Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(body["tools"], json!([]));

    arion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

fn mcp_gateway_with_remote_semantic_search_bootstrap(
    tools: Vec<ArionMcpTool>,
    backend: &TestBackend,
    embeddings: &EmbeddingsTestService,
    max_allowable_results: u32,
) -> BootstrapBuilder {
    let gateway = McpGatewayBuilder::new("test-gateway", "1.0.0").tools(tools).search(
        McpToolSearchBuilder::new()
            .remote_embeddings(EMBEDDINGS_CLUSTER, TEST_EMBEDDINGS_MODEL)
            .embeddings_path(TEST_EMBEDDINGS_PATH)
            .embeddings_timeout(Duration::from_secs(5))
            .embeddings_dimensions(TEST_EMBEDDINGS_DIMENSIONS)
            .max_allowable_results(max_allowable_results),
    );

    McpGatewayHttpConfigBuilder::new(gateway).build_bootstrap(vec![
        ClusterBuilder::new(BACKEND_CLUSTER).endpoint(EndpointBuilder::from_socket_addr(backend.addr())),
        ClusterBuilder::new(EMBEDDINGS_CLUSTER).endpoint(EndpointBuilder::from_socket_addr(embeddings.addr())),
    ])
}

fn semantic_spawn_options() -> SpawnOptions {
    SpawnOptions::default()
}

fn tool_configs(tools: impl IntoIterator<Item = TestTool>) -> Vec<ArionMcpTool> {
    tools.into_iter().map(TestTool::into_config).collect()
}

fn semantic_result_tool_names(result: &CallToolResult) -> Vec<String> {
    let content_text = result.content.first().map(|content| content.text.clone()).unwrap_or_default();
    let returned_tools: Value = serde_json::from_str(&content_text).expect("tool_search should return tool JSON");
    returned_tools
        .as_array()
        .expect("tool_search should return an array")
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(ToOwned::to_owned))
        .collect()
}

fn assert_embedding_request(request: &CapturedEmbeddingsTestRequest) {
    assert_eq!(request.method, Method::POST);
    assert_eq!(request.path(), TEST_EMBEDDINGS_PATH);
    assert_eq!(request.model.as_deref(), Some(TEST_EMBEDDINGS_MODEL));
    assert_eq!(request.header("user-agent"), Some("arion/embeddings"));
    let content_type = request.header("content-type").unwrap_or_default();
    assert!(content_type.contains("application/json"), "expected JSON content-type, got {content_type:?}");
}
