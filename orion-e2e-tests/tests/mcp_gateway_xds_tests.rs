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

//! MCP Gateway xDS E2E Tests
//!
//! Covers fine-grained MCP filter updates delivered via the custom `Tool`
//! and `DynamicMcpServer` extension resources on the ADS Delta stream.

use std::net::SocketAddr;
use std::time::Duration;

use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::Tool as OrionMcpTool;
use orion_e2e_tests::config_builder::{
    inline_string_data_source, mcp_resource_id, ClusterBuilder, DynamicMcpServerBuilder, EndpointBuilder,
    McpGatewayBuilder, McpGatewayHttpConfigBuilder, McpRestBackendBuilder, McpToolBuilder, McpToolRbacBuilder,
};
use orion_e2e_tests::{
    generate_jwt_token, HarnessError, JwtKeyPair, McpTestClient, McpTool, MockMcpServer, PreConfiguredResponse,
    TestBackend, TestJwtClaims, XdsEnabledHarness,
};
use serde_json::json;

const SERVER_NAME: &str = "mcp-xds-gw";
const SERVER_VERSION: &str = "1.0.0";
const TDS_CONFIG: &str = "main";
const LISTENER_WAIT: Duration = Duration::from_secs(10);
const JWT_AUDIENCE: &str = "mcp-gateway";

async fn start_harness_with_mcp_listener() -> (XdsEnabledHarness, SocketAddr) {
    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");

    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let dummy_cluster = ClusterBuilder::new("dummy").endpoint(EndpointBuilder::new("127.0.0.1", 1)).build();
    harness.push_cluster(&dummy_cluster).await.expect("Failed to push dummy cluster");

    let listener = mcp_gateway_http_config_using_xds(SERVER_NAME, TDS_CONFIG, Vec::new())
        .listener("http", listener_port)
        .build_listener()
        .build();
    harness.push_listener(&listener).await.expect("Failed to push MCP listener");
    harness.orion_mut().wait_for_listener_at(listener_addr, LISTENER_WAIT).await.expect("Listener not ready");

    (harness, listener_addr)
}

fn rest_tool_proto(name: &str, description: &str, cluster: &str, path: &str) -> OrionMcpTool {
    McpToolBuilder::new(name, description)
        .input_schema(inline_string_data_source(r#"{"type":"object","properties":{}}"#))
        .rest_backend(McpRestBackendBuilder::new(cluster, "GET", path))
        .build()
}

fn mcp_gateway_using_xds(server_name: &str, tds_config: &str, static_tools: Vec<OrionMcpTool>) -> McpGatewayBuilder {
    McpGatewayBuilder::new(server_name, SERVER_VERSION).tds(tds_config).tools(static_tools)
}

fn mcp_gateway_http_config_using_xds(
    server_name: &str,
    tds_config: &str,
    static_tools: Vec<OrionMcpTool>,
) -> McpGatewayHttpConfigBuilder {
    McpGatewayHttpConfigBuilder::new(mcp_gateway_using_xds(server_name, tds_config, static_tools))
}

async fn new_initialized_client(addr: SocketAddr) -> McpTestClient {
    let mut client = McpTestClient::new(addr);
    client.initialize().await.expect("Failed to initialize MCP session");
    client
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_tool_add_appears_in_list_tools() {
    let (harness, listener_addr) = start_harness_with_mcp_listener().await;
    let client = new_initialized_client(listener_addr).await;

    let tool = rest_tool_proto("weather", "Weather lookup", "weather_cluster", "/weather");
    let resource_id = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "weather");
    harness.push_mcp_tool(&resource_id, &tool).await.expect("Failed to push MCP tool");

    let tools = client.list_tools().await.expect("Failed to list tools");
    let names: Vec<&str> = tools.tools.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"weather"), "expected 'weather' in {names:?}");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_tool_remove_disappears_from_list() {
    let (harness, listener_addr) = start_harness_with_mcp_listener().await;
    let client = new_initialized_client(listener_addr).await;

    let tool = rest_tool_proto("weather", "Weather lookup", "weather_cluster", "/weather");
    let resource_id = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "weather");
    harness.push_mcp_tool(&resource_id, &tool).await.expect("Failed to push MCP tool");

    let before = client.list_tools().await.expect("Failed to list tools");
    assert!(before.tools.iter().any(|t| t.name == "weather"));

    harness.remove_mcp_tool(&resource_id).await.expect("Failed to remove MCP tool");

    let after = client.list_tools().await.expect("Failed to list tools");
    assert!(
        !after.tools.iter().any(|t| t.name == "weather"),
        "tool should be gone from list after remove, got {:?}",
        after.tools.iter().map(|t| &t.name).collect::<Vec<_>>()
    );

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_tool_update_replaces_definition() {
    let (harness, listener_addr) = start_harness_with_mcp_listener().await;
    let client = new_initialized_client(listener_addr).await;

    let resource_id = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "weather");

    let v1 = rest_tool_proto("weather", "Original description", "weather_cluster", "/weather");
    harness.push_mcp_tool(&resource_id, &v1).await.expect("Failed to push v1");

    let v1_tools = client.list_tools().await.expect("Failed to list tools v1");
    let v1_tool = v1_tools.tools.iter().find(|t| t.name == "weather").expect("weather tool missing");
    assert_eq!(v1_tool.description, "Original description");

    let v2 = rest_tool_proto("weather", "Updated description", "weather_cluster", "/weather");
    harness.push_mcp_tool(&resource_id, &v2).await.expect("Failed to push v2");

    let v2_tools = client.list_tools().await.expect("Failed to list tools v2");
    let v2_tool = v2_tools.tools.iter().find(|t| t.name == "weather").expect("weather tool missing after update");
    assert_eq!(v2_tool.description, "Updated description");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_tool_call_end_to_end_after_xds_push() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"temp": 20}"#)).await;

    let (harness, listener_addr) = start_harness_with_mcp_listener().await;
    let client = new_initialized_client(listener_addr).await;

    let weather_cluster =
        ClusterBuilder::new("weather_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).build();
    harness.push_cluster(&weather_cluster).await.expect("Failed to push weather cluster");

    let tool = rest_tool_proto("weather", "Weather lookup", "weather_cluster", "/weather");
    let resource_id = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "weather");
    harness.push_mcp_tool(&resource_id, &tool).await.expect("Failed to push MCP tool");

    let result = client.call_tool("weather", json!({})).await.expect("Failed to call tool");
    assert!(!result.is_error.unwrap_or(false), "tool call returned error: {:?}", result.content);

    let captured = backend.await_request().await.expect("Backend did not receive request");
    assert_eq!(captured.path(), "/weather");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_rbac_on_xds_pushed_tool() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"ok": true}"#)).await;

    let jwt_keys = JwtKeyPair::generate();

    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");
    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let dummy = ClusterBuilder::new("dummy").endpoint(EndpointBuilder::new("127.0.0.1", 1)).build();
    harness.push_cluster(&dummy).await.expect("Failed to push dummy cluster");

    let backend_cluster =
        ClusterBuilder::new("secure_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).build();
    harness.push_cluster(&backend_cluster).await.expect("Failed to push backend cluster");

    let listener = mcp_gateway_http_config_using_xds(SERVER_NAME, TDS_CONFIG, Vec::new())
        .listener("http", listener_port)
        .jwt_auth(jwt_keys.get_jwks_inline(), [JWT_AUDIENCE])
        .build_listener()
        .build();
    harness.push_listener(&listener).await.expect("Failed to push MCP listener");
    harness.orion_mut().wait_for_listener_at(listener_addr, LISTENER_WAIT).await.expect("Listener not ready");

    let tool = McpToolBuilder::new("secure_tool", "Admin-only tool")
        .input_schema(inline_string_data_source(r#"{"type":"object","properties":{}}"#))
        .rest_backend(McpRestBackendBuilder::new("secure_cluster", "GET", "/secure"))
        .rbac(McpToolRbacBuilder::allow().jwt_claim("role", "admin"))
        .build();
    let resource_id = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "secure_tool");
    harness.push_mcp_tool(&resource_id, &tool).await.expect("Failed to push RBAC tool");

    let admin_token = generate_jwt_token(&TestJwtClaims::new("alice", "admin"), &jwt_keys.private_key)
        .expect("Failed to generate admin token");
    let mut admin_client = McpTestClient::new(listener_addr).with_jwt(admin_token);
    admin_client.initialize().await.expect("Failed to initialize admin session");
    let admin_result = admin_client.call_tool("secure_tool", json!({})).await.expect("admin call failed");
    assert!(
        !admin_result.is_error.unwrap_or(false),
        "admin call should succeed, got: {:?}",
        admin_result.content.first().map(|c| &c.text)
    );

    let user_token = generate_jwt_token(&TestJwtClaims::new("bob", "user"), &jwt_keys.private_key)
        .expect("Failed to generate user token");
    let mut user_client = McpTestClient::new(listener_addr).with_jwt(user_token);
    user_client.initialize().await.expect("Failed to initialize user session");
    let user_result = user_client.call_tool("secure_tool", json!({})).await;
    match user_result {
        Ok(result) => assert!(result.is_error.unwrap_or(false), "non-admin call should be denied, got {result:?}"),
        Err(_) => {},
    }

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_malformed_resource_id_is_rejected() {
    let (harness, listener_addr) = start_harness_with_mcp_listener().await;
    let client = new_initialized_client(listener_addr).await;

    let tool = rest_tool_proto("probe", "probe", "probe_cluster", "/probe");

    let err = harness.push_mcp_tool("bad-id", &tool).await.expect_err("push of malformed id must be NACKed");
    assert!(matches!(err, HarnessError::Nack { .. }), "expected Nack, got {err:?}");

    let err = harness.push_mcp_tool("srv/cfg/", &tool).await.expect_err("push with empty resource name must be NACKed");
    assert!(matches!(err, HarnessError::Nack { .. }), "expected Nack, got {err:?}");

    let ok_id = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "probe");
    harness.push_mcp_tool(&ok_id, &tool).await.expect("valid push must still succeed after prior NACKs");

    let tools = client.list_tools().await.expect("Failed to list tools");
    assert!(tools.tools.iter().any(|t| t.name == "probe"));

    harness.shutdown();
}

async fn push_mcp_listener(
    harness: &mut XdsEnabledHarness,
    name: &str,
    server_name: &str,
    tds_config: &str,
) -> SocketAddr {
    let port = harness.allocate_listener_port().expect("Failed to allocate port");
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = mcp_gateway_http_config_using_xds(server_name, tds_config, Vec::new())
        .listener(name, port)
        .build_listener()
        .build();
    harness.push_listener(&listener).await.expect("Failed to push listener");
    harness.orion_mut().wait_for_listener_at(addr, LISTENER_WAIT).await.expect("Listener not ready");
    addr
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_two_listeners_scope_isolation() {
    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");
    let dummy = ClusterBuilder::new("dummy").endpoint(EndpointBuilder::new("127.0.0.1", 1)).build();
    harness.push_cluster(&dummy).await.expect("Failed to push dummy cluster");

    let addr_a = push_mcp_listener(&mut harness, "listener_a", "server_a", "main").await;
    let addr_b = push_mcp_listener(&mut harness, "listener_b", "server_b", "main").await;

    let client_a = new_initialized_client(addr_a).await;
    let client_b = new_initialized_client(addr_b).await;

    let tool = rest_tool_proto("only_in_a", "tool", "some_cluster", "/only_in_a");
    let id_a = mcp_resource_id("server_a", "main", "only_in_a");
    harness.push_mcp_tool(&id_a, &tool).await.expect("Failed to push tool to scope A");

    let tools_a = client_a.list_tools().await.expect("Failed to list tools A");
    assert!(tools_a.tools.iter().any(|t| t.name == "only_in_a"), "scope A should have tool");

    let tools_b = client_b.list_tools().await.expect("Failed to list tools B");
    assert!(
        !tools_b.tools.iter().any(|t| t.name == "only_in_a"),
        "scope B must not see scope A's tool, got {:?}",
        tools_b.tools.iter().map(|t| &t.name).collect::<Vec<_>>()
    );

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_two_listeners_independent_lifecycles() {
    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");
    let dummy = ClusterBuilder::new("dummy").endpoint(EndpointBuilder::new("127.0.0.1", 1)).build();
    harness.push_cluster(&dummy).await.expect("Failed to push dummy cluster");

    let addr_a = push_mcp_listener(&mut harness, "listener_a", "server_a", "main").await;
    let addr_b = push_mcp_listener(&mut harness, "listener_b", "server_b", "main").await;

    let client_a = new_initialized_client(addr_a).await;
    let client_b = new_initialized_client(addr_b).await;

    let tool_a = rest_tool_proto("tool_a", "tool", "some_cluster", "/tool_a");
    let tool_b = rest_tool_proto("tool_b", "tool", "some_cluster", "/tool_b");
    let id_a = mcp_resource_id("server_a", "main", "tool_a");
    let id_b = mcp_resource_id("server_b", "main", "tool_b");

    harness.push_mcp_tool(&id_a, &tool_a).await.expect("push A failed");
    harness.push_mcp_tool(&id_b, &tool_b).await.expect("push B failed");

    let before_a = client_a.list_tools().await.expect("list A failed");
    let before_b = client_b.list_tools().await.expect("list B failed");
    assert!(before_a.tools.iter().any(|t| t.name == "tool_a"));
    assert!(before_b.tools.iter().any(|t| t.name == "tool_b"));

    harness.remove_mcp_tool(&id_a).await.expect("remove A failed");

    let after_a = client_a.list_tools().await.expect("list A after remove failed");
    let after_b = client_b.list_tools().await.expect("list B after remove failed");
    assert!(!after_a.tools.iter().any(|t| t.name == "tool_a"), "scope A tool should be removed");
    assert!(after_b.tools.iter().any(|t| t.name == "tool_b"), "scope B tool must remain");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_duplicate_server_name_rejected() {
    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");
    let dummy = ClusterBuilder::new("dummy").endpoint(EndpointBuilder::new("127.0.0.1", 1)).build();
    harness.push_cluster(&dummy).await.expect("Failed to push dummy cluster");

    let port_a = harness.allocate_listener_port().expect("port a");
    let addr_a = SocketAddr::from(([127, 0, 0, 1], port_a));
    let listener_a = mcp_gateway_http_config_using_xds("same_server", "cfg_a", Vec::new())
        .listener("listener_a", port_a)
        .build_listener()
        .build();
    harness.push_listener(&listener_a).await.expect("push listener A");
    harness.orion_mut().wait_for_listener_at(addr_a, LISTENER_WAIT).await.expect("listener A not ready");

    let port_b = harness.allocate_listener_port().expect("port b");
    let addr_b = SocketAddr::from(([127, 0, 0, 1], port_b));
    let listener_b = mcp_gateway_http_config_using_xds("same_server", "cfg_b", Vec::new())
        .listener("listener_b", port_b)
        .build_listener()
        .build();
    let push_result = harness.push_listener(&listener_b).await;

    match push_result {
        Err(HarnessError::Nack { .. }) => {},
        Ok(()) => {
            if std::net::TcpStream::connect_timeout(&addr_b, Duration::from_millis(500)).is_ok() {
                panic!("listener B bound successfully despite duplicate server name");
            }
        },
        Err(other) => panic!("unexpected error pushing duplicate-server listener: {other:?}"),
    }

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_static_and_xds_coexist() {
    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");

    let dummy = ClusterBuilder::new("dummy").endpoint(EndpointBuilder::new("127.0.0.1", 1)).build();
    harness.push_cluster(&dummy).await.expect("push dummy cluster");

    let port = harness.allocate_listener_port().expect("port");
    let addr = SocketAddr::from(([127, 0, 0, 1], port));

    let static_tool = rest_tool_proto("static_a", "statically-configured tool", "static_cluster", "/a");
    let listener = mcp_gateway_http_config_using_xds(SERVER_NAME, TDS_CONFIG, vec![static_tool])
        .listener("http", port)
        .build_listener()
        .build();
    harness.push_listener(&listener).await.expect("push listener");
    harness.orion_mut().wait_for_listener_at(addr, LISTENER_WAIT).await.expect("listener not ready");

    let client = new_initialized_client(addr).await;

    let before = client.list_tools().await.expect("list before");
    assert!(before.tools.iter().any(|t| t.name == "static_a"), "static_a should be present from bootstrap");

    let tool_b = rest_tool_proto("xds_b", "xDS-pushed tool", "xds_cluster", "/b");
    let id_b = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "xds_b");
    harness.push_mcp_tool(&id_b, &tool_b).await.expect("push xds_b");

    let after = client.list_tools().await.expect("list after xds_b");
    let names: Vec<&str> = after.tools.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"static_a"), "static_a must still be present, got {names:?}");
    assert!(names.contains(&"xds_b"), "xds_b must be present, got {names:?}");

    let overwrite = rest_tool_proto("static_a", "overwritten via xDS", "other_cluster", "/other");
    let id_a = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "static_a");
    harness.push_mcp_tool(&id_a, &overwrite).await.expect("xDS may overwrite Provided tool");

    let final_tools = client.list_tools().await.expect("list after overwrite");
    let static_a = final_tools.tools.iter().find(|t| t.name == "static_a").expect("static_a still present");
    assert_eq!(static_a.description, "overwritten via xDS");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_dynamic_server_add_materializes_tools() {
    let mock = MockMcpServer::start().await.expect("Failed to start mock MCP server");
    let mock_url = format!("http://{}/mcp", mock.addr());

    let (harness, listener_addr) = start_harness_with_mcp_listener().await;
    let client = new_initialized_client(listener_addr).await;

    let server_proto = DynamicMcpServerBuilder::new("dyn_srv", "dynamic mock", &mock_url).build();
    let resource_id = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "dyn_srv");
    harness.push_dynamic_mcp_server(&resource_id, &server_proto).await.expect("push dyn server");

    let tools = client.list_tools().await.expect("list tools");
    let names: Vec<&str> = tools.tools.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"dyn_srv__mock_echo"), "expected dyn_srv__mock_echo, got {names:?}");
    assert!(names.contains(&"dyn_srv__mock_add"), "expected dyn_srv__mock_add, got {names:?}");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_dynamic_server_remove_evicts_tools() {
    let mock = MockMcpServer::start().await.expect("Failed to start mock MCP server");
    let mock_url = format!("http://{}/mcp", mock.addr());

    let (harness, listener_addr) = start_harness_with_mcp_listener().await;
    let client = new_initialized_client(listener_addr).await;

    let server_proto = DynamicMcpServerBuilder::new("dyn_srv", "dynamic mock", &mock_url).build();
    let resource_id = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "dyn_srv");
    harness.push_dynamic_mcp_server(&resource_id, &server_proto).await.expect("push dyn server");

    assert!(
        client.list_tools().await.expect("list tools").tools.iter().any(|t| t.name.starts_with("dyn_srv__")),
        "tools should be materialized before removal"
    );

    harness.remove_dynamic_mcp_server(&resource_id).await.expect("remove dyn server");

    let tools = client.list_tools().await.expect("list tools after remove");
    assert!(
        !tools.tools.iter().any(|t| t.name.starts_with("dyn_srv__")),
        "dynamic tools should be evicted after server removal, got {:?}",
        tools.tools.iter().map(|t| &t.name).collect::<Vec<_>>()
    );

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_dynamic_server_replaces_tools_on_update() {
    let mock = MockMcpServer::start().await.expect("Failed to start mock MCP server");
    let mock_url = format!("http://{}/mcp", mock.addr());

    let (harness, listener_addr) = start_harness_with_mcp_listener().await;
    let client = new_initialized_client(listener_addr).await;

    let server_proto = DynamicMcpServerBuilder::new("dyn_srv", "dynamic mock", &mock_url).build();
    let resource_id = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "dyn_srv");
    harness.push_dynamic_mcp_server(&resource_id, &server_proto).await.expect("initial push");

    let names_before: Vec<String> =
        client.list_tools().await.expect("list").tools.iter().map(|t| t.name.clone()).collect();
    assert!(names_before.iter().any(|n| n == "dyn_srv__mock_echo"));
    assert!(names_before.iter().any(|n| n == "dyn_srv__mock_add"));
    assert!(!names_before.iter().any(|n| n == "dyn_srv__mock_new"));

    mock.clear_tools().await;
    mock.add_tool(McpTool {
        name: "mock_new".to_string(),
        description: "a new tool".to_string(),
        input_schema: json!({"type": "object", "properties": {}}),
        output_schema: None,
    })
    .await;

    harness.push_dynamic_mcp_server(&resource_id, &server_proto).await.expect("re-push");

    let names_after: Vec<String> =
        client.list_tools().await.expect("list").tools.iter().map(|t| t.name.clone()).collect();
    assert!(names_after.iter().any(|n| n == "dyn_srv__mock_new"), "new tool should appear, got {names_after:?}");
    assert!(!names_after.iter().any(|n| n == "dyn_srv__mock_echo"), "old tools should be evicted, got {names_after:?}");
    assert!(!names_after.iter().any(|n| n == "dyn_srv__mock_add"), "old tools should be evicted, got {names_after:?}");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_xds_unknown_scope_is_noop() {
    let (harness, listener_addr) = start_harness_with_mcp_listener().await;
    let client = new_initialized_client(listener_addr).await;

    let tool = rest_tool_proto("ghost", "ghost", "ghost_cluster", "/ghost");

    let unknown_id = mcp_resource_id("other_server", "other_cfg", "ghost");
    harness.push_mcp_tool(&unknown_id, &tool).await.expect("push to unknown scope should be ACKed as a no-op");

    let tools = client.list_tools().await.expect("Failed to list tools");
    assert!(tools.tools.is_empty(), "no tools should be registered from unknown-scope push, got {:?}", tools.tools);

    let ok_id = mcp_resource_id(SERVER_NAME, TDS_CONFIG, "ghost");
    harness.push_mcp_tool(&ok_id, &tool).await.expect("subsequent valid push must succeed");

    let tools = client.list_tools().await.expect("Failed to list tools");
    assert!(tools.tools.iter().any(|t| t.name == "ghost"));

    harness.shutdown();
}
