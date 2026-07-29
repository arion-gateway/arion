// Copyright 2025 The kmesh Authors
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

use http::Method;
use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::Tool as OrionMcpTool;
use orion_e2e_tests::config_builder::{
    inline_string_data_source, BootstrapBuilder, ClusterBuilder, EndpointBuilder, McpGatewayBuilder,
    McpGatewayHttpConfigBuilder, McpRestBackendBuilder, McpSemanticSearchBuilder, McpToolBuilder,
};
use orion_e2e_tests::{
    CallToolResult, CapturedEmbeddingsTestRequest, EmbeddingsTestService, McpTestClient, OrionInstance,
    PreConfiguredResponse, SpawnOptions, TestBackend,
};
use serde_json::{json, Value};
use std::time::Duration;

const BACKEND_CLUSTER: &str = "backend_cluster";
const EMBEDDINGS_CLUSTER: &str = "embedding_cluster";
const TEST_EMBEDDINGS_MODEL: &str = "test-embedding-model";
const TEST_EMBEDDINGS_PATH: &str = "/v1/embeddings";
const TEST_EMBEDDINGS_DIMENSIONS: u32 = 3;

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

    fn into_config(self) -> OrionMcpTool {
        let mut builder = McpToolBuilder::new(self.name, self.description)
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

const QUERY_DATABASE_TOOL: TestTool = TestTool {
    name: "query_database",
    description: "Execute SQL queries and retrieve database records",
    method: "GET",
    path: "/api/database",
    embedding: None,
};

const GET_ANALYTICS_TOOL: TestTool = TestTool {
    name: "get_analytics",
    description: "Fetch analytics data and statistics reports",
    method: "GET",
    path: "/api/analytics",
    embedding: None,
};

const SEND_EMAIL_TOOL: TestTool = TestTool {
    name: "send_email",
    description: "Send email notifications and messages",
    method: "POST",
    path: "/api/email",
    embedding: None,
};

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_semantic_search_direct_mode_with_explicit_embeddings() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)).await;
    let mut embeddings = EmbeddingsTestService::start().await.expect("Failed to start embeddings test service");

    let tools = tool_configs([
        GET_WEATHER_TOOL.with_embedding([1.0, 0.0, 0.0]),
        GET_USER_TOOL.with_embedding([0.0, 1.0, 0.0]),
        PROCESS_PAYMENT_TOOL.with_embedding([0.0, 0.0, 1.0]),
    ]);
    let bootstrap = mcp_gateway_with_remote_semantic_search_bootstrap(tools, &backend, &embeddings, false, 1);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", semantic_spawn_options())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(orion.listener_addr().unwrap());
    client.initialize().await.expect("Failed to initialize");

    let listed = client.list_tools().await.expect("Failed to list tools");
    assert!(listed.tools.iter().any(|tool| tool.name == "semantic_search"));
    assert!(embeddings.try_recv_request().is_none(), "explicit tool embeddings should avoid bootstrap embedding");

    let result = client
        .call_tool("semantic_search", json!({ "user_query": "I need to check the weather forecast" }))
        .await
        .expect("Failed to call semantic_search");
    assert!(!result.is_error.unwrap_or(false), "semantic_search call should succeed");

    let names = semantic_result_tool_names(&result);
    assert_eq!(names, vec!["get_weather"]);

    let query_request = embeddings.await_request().await.expect("No query embedding request received");
    assert_embedding_request(&query_request);
    assert_eq!(query_request.input, vec!["I need to check the weather forecast"]);
    assert!(embeddings.try_recv_request().is_none(), "semantic search should only embed the query");

    orion.shutdown();
    embeddings.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_semantic_search_direct_mode_with_remote_embeddings() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)).await;
    let mut embeddings = EmbeddingsTestService::start().await.expect("Failed to start embeddings test service");

    let tools = tool_configs([GET_WEATHER_TOOL, GET_USER_TOOL, PROCESS_PAYMENT_TOOL]);
    let bootstrap = mcp_gateway_with_remote_semantic_search_bootstrap(tools, &backend, &embeddings, false, 1);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", semantic_spawn_options())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(orion.listener_addr().unwrap());
    client.initialize().await.expect("Failed to initialize");

    let listed = client.list_tools().await.expect("Failed to list tools");
    assert!(listed.tools.iter().any(|tool| tool.name == "semantic_search"));

    let batch_request = embeddings.await_request().await.expect("No tool batch embedding request received");
    assert_embedding_request(&batch_request);
    assert_eq!(batch_request.input.len(), 3);
    assert!(batch_request.input.iter().any(|input| input.contains("get_weather")));
    assert!(batch_request.input.iter().any(|input| input.contains("get_user")));
    assert!(batch_request.input.iter().any(|input| input.contains("process_payment")));

    let result = client
        .call_tool("semantic_search", json!({ "user_query": "I need to check the weather forecast" }))
        .await
        .expect("Failed to call semantic_search");
    assert!(!result.is_error.unwrap_or(false), "semantic_search call should succeed");

    let query_request = embeddings.await_request().await.expect("No query embedding request received");
    assert_embedding_request(&query_request);
    assert_eq!(query_request.input, vec!["I need to check the weather forecast"]);

    let names = semantic_result_tool_names(&result);
    assert_eq!(names, vec!["get_weather"]);

    orion.shutdown();
    embeddings.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_semantic_search_assisted_discovery_with_remote_embeddings() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)).await;
    let mut embeddings = EmbeddingsTestService::start().await.expect("Failed to start embeddings test service");

    let tools = tool_configs([QUERY_DATABASE_TOOL, GET_ANALYTICS_TOOL, SEND_EMAIL_TOOL]);
    let bootstrap = mcp_gateway_with_remote_semantic_search_bootstrap(tools, &backend, &embeddings, true, 1);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", semantic_spawn_options())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(orion.listener_addr().unwrap());
    client.initialize().await.expect("Failed to initialize");

    let initial_tools = client.list_tools().await.expect("Failed to list initial tools");
    let initial_names: Vec<_> = initial_tools.tools.iter().map(|tool| tool.name.as_str()).collect();
    assert_eq!(initial_names, vec!["semantic_search"]);

    let batch_request = embeddings.await_request().await.expect("No tool batch embedding request received");
    assert_embedding_request(&batch_request);
    assert_eq!(batch_request.input.len(), 3);
    assert!(batch_request.input.iter().any(|input| input.contains("query_database")));
    assert!(batch_request.input.iter().any(|input| input.contains("get_analytics")));
    assert!(batch_request.input.iter().any(|input| input.contains("send_email")));

    let result = client
        .call_tool("semantic_search", json!({ "user_query": "I need to query the database for records" }))
        .await
        .expect("Failed to call semantic_search");
    assert!(!result.is_error.unwrap_or(false), "semantic_search call should succeed");

    let query_request = embeddings.await_request().await.expect("No query embedding request received");
    assert_embedding_request(&query_request);
    assert_eq!(query_request.input, vec!["I need to query the database for records"]);

    let filtered_tools = client.list_tools().await.expect("Failed to list filtered tools");
    let filtered_names: Vec<_> = filtered_tools.tools.iter().map(|tool| tool.name.as_str()).collect();
    assert!(filtered_names.contains(&"semantic_search"));
    assert!(filtered_names.contains(&"query_database"));
    assert!(!filtered_names.contains(&"get_analytics"));
    assert!(!filtered_names.contains(&"send_email"));

    let call_result = client.call_tool("query_database", json!({})).await.expect("Failed to call query_database");
    assert!(!call_result.is_error.unwrap_or(false), "query_database tool should be callable");
    let request = backend.await_request().await.expect("No request to REST backend");
    assert_eq!(request.path(), "/api/database");

    orion.shutdown();
    embeddings.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

fn mcp_gateway_with_remote_semantic_search_bootstrap(
    tools: Vec<OrionMcpTool>,
    backend: &TestBackend,
    embeddings: &EmbeddingsTestService,
    enable_assisted_discovery: bool,
    top_k: u32,
) -> BootstrapBuilder {
    let gateway = McpGatewayBuilder::new("test-gateway", "1.0.0").tools(tools).semantic_search(
        McpSemanticSearchBuilder::new()
            .remote_embeddings(EMBEDDINGS_CLUSTER, TEST_EMBEDDINGS_MODEL)
            .embeddings_path(TEST_EMBEDDINGS_PATH)
            .embeddings_timeout(Duration::from_secs(5))
            .embeddings_dimensions(TEST_EMBEDDINGS_DIMENSIONS)
            .assisted_discovery(enable_assisted_discovery)
            .top_k(top_k),
    );

    McpGatewayHttpConfigBuilder::new(gateway).build_bootstrap(vec![
        ClusterBuilder::new(BACKEND_CLUSTER).endpoint(EndpointBuilder::from_socket_addr(backend.addr())),
        ClusterBuilder::new(EMBEDDINGS_CLUSTER).endpoint(EndpointBuilder::from_socket_addr(embeddings.addr())),
    ])
}

fn semantic_spawn_options() -> SpawnOptions {
    SpawnOptions::default()
}

fn tool_configs(tools: impl IntoIterator<Item = TestTool>) -> Vec<OrionMcpTool> {
    tools.into_iter().map(TestTool::into_config).collect()
}

fn semantic_result_tool_names(result: &CallToolResult) -> Vec<String> {
    let content_text = result.content.first().map(|content| content.text.clone()).unwrap_or_default();
    let returned_tools: Value = serde_json::from_str(&content_text).expect("semantic_search should return tool JSON");
    returned_tools
        .as_array()
        .expect("semantic_search should return an array")
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(ToOwned::to_owned))
        .collect()
}

fn assert_embedding_request(request: &CapturedEmbeddingsTestRequest) {
    assert_eq!(request.method, Method::POST);
    assert_eq!(request.path(), TEST_EMBEDDINGS_PATH);
    assert_eq!(request.model.as_deref(), Some(TEST_EMBEDDINGS_MODEL));
    assert_eq!(request.header("user-agent"), Some("orion/embeddings"));
    let content_type = request.header("content-type").unwrap_or_default();
    assert!(content_type.contains("application/json"), "expected JSON content-type, got {content_type:?}");
}
