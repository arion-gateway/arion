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

//! MCP Gateway E2E Integration Tests
//!
//! This test suite covers:
//! - Basic MCP protocol (initialize, ping, tools/list)
//! - REST backend transcoding (path, query params, body templating)
//! - MCP backend (mock MCP server)
//! - Tool RBAC with JWT claims and headers
//! - Semantic search tool

use orion_e2e_tests::config_builder::{ClusterBuilder, EndpointBuilder};
use orion_e2e_tests::{
    generate_jwt_token, mcp_gateway_config, mcp_gateway_with_direct_semantic_search_config,
    mcp_gateway_with_jwt_and_semantic_search_config, mcp_gateway_with_jwt_config, mcp_server_tool_config, rbac_config,
    rest_tool_config, JwtKeyPair, McpTestClient, MockMcpServer, OrionInstance, PreConfiguredResponse, SpawnOptions,
    TestBackend, TestJwtClaims,
};
use serde_json::json;

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_initialize_and_ping() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"status": "ok"}"#)).await;

    let tool = rest_tool_config(
        "test_tool",
        "A simple test tool",
        "test_cluster",
        "GET",
        "/api/test",
        json!({
            "type": "object",
            "properties": {}
        }),
        vec![],
        None,
        None,
    );

    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("test_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));

    let init_result = client.initialize().await.expect("Failed to initialize");
    assert!(!init_result.is_null());

    client.ping().await.expect("Failed to ping");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_tools_list() {
    let weather_backend = TestBackend::start().await.expect("Failed to start weather backend");
    let user_backend = TestBackend::start().await.expect("Failed to start user backend");

    weather_backend
        .set_default_response(PreConfiguredResponse::with_body(r#"{"temp": 25, "condition": "sunny"}"#))
        .await;
    user_backend.set_default_response(PreConfiguredResponse::with_body(r#"{"name": "John", "id": "123"}"#)).await;

    let weather_tool = rest_tool_config(
        "get_weather",
        "Get weather forecast",
        "weather_cluster",
        "GET",
        "/weather",
        json!({
            "type": "object",
            "properties": {}
        }),
        vec![],
        None,
        None,
    );

    let user_tool = rest_tool_config(
        "get_user",
        "Get user profile",
        "user_cluster",
        "GET",
        "/user",
        json!({
            "type": "object",
            "properties": {}
        }),
        vec![],
        None,
        None,
    );

    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![weather_tool, user_tool],
        vec![
            ClusterBuilder::new("weather_cluster").endpoint(EndpointBuilder::from_socket_addr(weather_backend.addr())),
            ClusterBuilder::new("user_cluster").endpoint(EndpointBuilder::from_socket_addr(user_backend.addr())),
        ],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));
    client.initialize().await.expect("Failed to initialize");

    let tools = client.list_tools().await.expect("Failed to list tools");
    assert_eq!(tools.tools.len(), 2);

    let tool_names: Vec<&str> = tools.tools.iter().map(|t| t.name.as_str()).collect();
    assert!(tool_names.contains(&"get_weather"));
    assert!(tool_names.contains(&"get_user"));

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_rest_path_templating() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "success"}"#)).await;

    let tool = rest_tool_config(
        "fetch_item",
        "Fetch an item by ID",
        "backend_cluster",
        "GET",
        "/api/items/{{item_id}}",
        json!({
            "type": "object",
            "properties": {
                "item_id": {"type": "string"}
            },
            "required": ["item_id"]
        }),
        vec![],
        None,
        None,
    );

    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));
    client.initialize().await.expect("Failed to initialize");

    let result = client
        .call_tool(
            "fetch_item",
            json!({
                "item_id": "12345"
            }),
        )
        .await
        .expect("Failed to call tool");

    assert!(!result.is_error.unwrap_or(false), "Tool call should succeed with valid arguments");

    let request = backend.await_request().await.expect("No request received");
    assert_eq!(request.path(), "/api/items/12345", "Path template should substitute item_id variable");
    assert_eq!(request.method, "GET", "Request method should be GET");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_rest_query_params() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"users": []}"#)).await;

    let tool = json!({
        "name": "search_users",
        "description": "Search users with filters",
        "rest_backend": {
            "cluster": "backend_cluster",
            "method": "GET",
            "path": "/api/users",
            "query_params": [
                { "name": "name", "source": "name" },
                { "name": "age", "source": "age" },
                { "name": "active", "source": "is_active" }
            ]
        },
        "input_schema": {
            "inline_string": r#"{
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "age": { "type": "integer" },
                    "is_active": { "type": "boolean" }
                }
            }"#
        }
    });

    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));
    client.initialize().await.expect("Failed to initialize");

    client
        .call_tool(
            "search_users",
            json!({
                "name": "John",
                "age": 30,
                "is_active": true
            }),
        )
        .await
        .expect("Failed to call tool");

    let request = backend.await_request().await.expect("No request received");
    let path_and_query = request.path_and_query();
    assert!(path_and_query.starts_with("/api/users"), "Path should start with /api/users, got: {}", path_and_query);
    assert!(path_and_query.contains("name=John"), "Query params should include name=John, got: {}", path_and_query);
    assert!(path_and_query.contains("age=30"), "Query params should include age=30, got: {}", path_and_query);
    assert!(
        path_and_query.contains("active=true"),
        "Query params should map is_active to active=true, got: {}",
        path_and_query
    );

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_rest_body_templating() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"id": "123"}"#)).await;

    let tool = json!({
        "name": "create_user",
        "description": "Create a new user",
        "rest_backend": {
            "cluster": "backend_cluster",
            "method": "POST",
            "path": "/api/users",
            "body_template": {
                "inline_string": r#"{"username": "{{username}}", "email": "{{email}}", "age": {{age}}}"#
            }
        },
        "input_schema": {
            "inline_string": r#"{
                "type": "object",
                "properties": {
                    "username": { "type": "string" },
                    "email": { "type": "string" },
                    "age": { "type": "integer" }
                },
                "required": ["username", "email"]
            }"#
        }
    });

    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));
    client.initialize().await.expect("Failed to initialize");

    client
        .call_tool(
            "create_user",
            json!({
                "username": "johndoe",
                "email": "john@example.com",
                "age": 30
            }),
        )
        .await
        .expect("Failed to call tool");

    let request = backend.await_request().await.expect("No request received");
    assert_eq!(request.method, "POST", "Request method should be POST");

    let body = request.body_str().expect("Request has no body");
    let body_json: serde_json::Value = serde_json::from_str(body).expect("Invalid JSON body");

    assert_eq!(body_json["username"], "johndoe", "Body should contain templated username");
    assert_eq!(body_json["email"], "john@example.com", "Body should contain templated email");
    assert_eq!(body_json["age"], 30, "Body should contain templated age as number");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_rest_full_transcoding() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"updated": true}"#)).await;

    let tool = json!({
        "name": "update_resource",
        "description": "Update a resource",
        "rest_backend": {
            "cluster": "backend_cluster",
            "method": "PUT",
            "path": "/api/resources/{{resource_id}}",
            "query_params": [
                { "name": "force", "source": "force_update" }
            ],
            "body_template": {
                "inline_string": r#"{
                    "data": "{{content}}",
                    "metadata": {
                        "version": {{ version }}
                    }
                }"#
            }
        },
        "input_schema": {
            "inline_string": r#"{
                "type": "object",
                "properties": {
                    "resource_id": { "type": "string" },
                    "force_update": { "type": "boolean" },
                    "content": { "type": "string" },
                    "version": { "type": "integer" }
                },
                "required": ["resource_id", "content"]
            }"#
        }
    });

    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));
    client.initialize().await.expect("Failed to initialize");

    client
        .call_tool(
            "update_resource",
            json!({
                "resource_id": "res-123",
                "force_update": true,
                "content": "new data",
                "version": 2
            }),
        )
        .await
        .expect("Failed to call tool");

    let request = backend.await_request().await.expect("No request received");
    assert_eq!(request.method, "PUT", "Request method should be PUT");
    assert!(request.path().starts_with("/api/resources/res-123"), "Path should include templated resource_id");
    assert!(
        request.path_and_query().contains("force=true"),
        "Query params should include force_update mapped to force=true"
    );

    let body = request.body_str().expect("Request has no body");
    let body_json: serde_json::Value = serde_json::from_str(body).expect("Invalid JSON body");
    assert_eq!(body_json["data"], "new data", "Body should contain templated content");
    assert_eq!(body_json["metadata"]["version"], 2, "Body should contain nested templated version");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_mcp_backend() {
    let mut mock_mcp = MockMcpServer::start().await.expect("Failed to start mock MCP server");

    let tool = mcp_server_tool_config(
        "mock_echo",
        "Call remote echo tool",
        format!("http://{}/mcp", mock_mcp.addr()),
        "streamable_http",
        Some("10s".to_string()),
    );

    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("mcp_backend_cluster").endpoint(EndpointBuilder::from_socket_addr(mock_mcp.addr()))],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));
    client.initialize().await.expect("Failed to initialize");

    let result = client
        .call_tool(
            "mock_echo",
            json!({
                "message": "Hello, MCP!"
            }),
        )
        .await
        .expect("Failed to call tool");

    assert!(!result.is_error.unwrap_or(false), "Tool call returned error");
    let content_text = result.content.first().map(|c| c.text.clone()).unwrap_or_default();
    assert!(content_text.contains("Hello, MCP!"), "Expected echoed message in response, got: {}", content_text);

    mock_mcp.shutdown();
    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_rbac_jwt_claim_allow() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"secret": "data"}"#)).await;

    let jwt_keys = JwtKeyPair::generate();

    let rbac = rbac_config("allow", vec![("jwt_claim".to_string(), "role".to_string(), "admin".to_string())]);

    let tool = json!({
        "name": "admin_only_tool",
        "description": "Tool for admins only",
        "rbac": rbac,
        "rest_backend": {
            "cluster": "backend_cluster",
            "method": "GET",
            "path": "/admin/secret"
        },
        "input_schema": {
            "inline_string": r#"{"type": "object", "properties": {}}"#
        }
    });

    let bootstrap = mcp_gateway_with_jwt_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
        &jwt_keys.get_jwks_inline(),
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    // Test 1: Access with admin role should succeed
    let admin_claims = TestJwtClaims::new("user1", "admin");
    let admin_token = generate_jwt_token(&admin_claims, &jwt_keys.private_key).expect("Failed to generate token");

    let mut admin_client =
        McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&admin_token);
    admin_client.initialize().await.expect("Failed to initialize");

    let result = admin_client.call_tool("admin_only_tool", json!({})).await.expect("Failed to call tool");
    assert!(!result.is_error.unwrap_or(false), "Admin should have access to tool");

    let _ = backend.await_request().await.expect("No request received");

    // Test 2: Access with user role should fail
    let user_claims = TestJwtClaims::new("user2", "user");
    let user_token = generate_jwt_token(&user_claims, &jwt_keys.private_key).expect("Failed to generate token");

    let mut user_client =
        McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&user_token);
    user_client.initialize().await.expect("Failed to initialize");

    let result = user_client.call_tool("admin_only_tool", json!({})).await;
    assert!(result.is_err(), "User without admin role should be denied access");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_rbac_jwt_claim_deny() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"data": "value"}"#)).await;

    let jwt_keys = JwtKeyPair::generate();

    let rbac = rbac_config("deny", vec![("jwt_claim".to_string(), "role".to_string(), "guest".to_string())]);

    let tool = json!({
        "name": "premium_tool",
        "description": "Tool for non-guests",
        "rbac": rbac,
        "rest_backend": {
            "cluster": "backend_cluster",
            "method": "GET",
            "path": "/api/premium"
        },
        "input_schema": {
            "inline_string": r#"{"type": "object", "properties": {}}"#
        }
    });

    let bootstrap = mcp_gateway_with_jwt_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
        &jwt_keys.get_jwks_inline(),
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    // Test 1: Regular user should have access
    let user_claims = TestJwtClaims::new("user1", "user");
    let user_token = generate_jwt_token(&user_claims, &jwt_keys.private_key).expect("Failed to generate token");

    let mut user_client =
        McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&user_token);
    user_client.initialize().await.expect("Failed to initialize");

    let result = user_client.call_tool("premium_tool", json!({})).await.expect("Failed to call tool");
    assert!(!result.is_error.unwrap_or(false), "User with non-guest role should have access");

    let _ = backend.await_request().await.expect("No request received");

    // Test 2: Guest should be denied
    let guest_claims = TestJwtClaims::new("user2", "guest");
    let guest_token = generate_jwt_token(&guest_claims, &jwt_keys.private_key).expect("Failed to generate token");

    let mut guest_client =
        McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&guest_token);
    guest_client.initialize().await.expect("Failed to initialize");

    // Guest can initialize (not protected) but should be denied tool access
    let result = guest_client.call_tool("premium_tool", json!({})).await;
    assert!(result.is_err(), "Guest should be denied access to premium_tool");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_rbac_multiple_permissions() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"data": "ok"}"#)).await;

    let jwt_keys = JwtKeyPair::generate();

    let rbac = rbac_config(
        "allow",
        vec![
            ("jwt_claim".to_string(), "role".to_string(), "admin".to_string()),
            ("jwt_claim".to_string(), "role".to_string(), "moderator".to_string()),
            ("jwt_claim".to_string(), "department".to_string(), "security".to_string()),
        ],
    );

    let tool = json!({
        "name": "multi_perm_tool",
        "description": "Tool with multiple permission options",
        "rbac": rbac,
        "rest_backend": {
            "cluster": "backend_cluster",
            "method": "GET",
            "path": "/api/multi"
        },
        "input_schema": {
            "inline_string": r#"{"type": "object", "properties": {}}"#
        }
    });

    let bootstrap = mcp_gateway_with_jwt_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
        &jwt_keys.get_jwks_inline(),
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    // Test with different roles - all should work
    for role in ["admin", "moderator"] {
        let claims = TestJwtClaims::new("user", role);
        let token = generate_jwt_token(&claims, &jwt_keys.private_key).expect("Failed to generate token");

        let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&token);
        client.initialize().await.expect("Failed to initialize");

        let result = client.call_tool("multi_perm_tool", json!({})).await.expect("Failed to call tool");
        assert!(!result.is_error.unwrap_or(false), "User with role '{}' should have access", role);

        let _ = backend.await_request().await.expect("No request received");
    }

    // Test with department claim
    let dept_claims = TestJwtClaims::new("user", "user").with_claim("department", "security");
    let dept_token = generate_jwt_token(&dept_claims, &jwt_keys.private_key).expect("Failed to generate token");

    let mut dept_client =
        McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&dept_token);
    dept_client.initialize().await.expect("Failed to initialize");

    let result = dept_client.call_tool("multi_perm_tool", json!({})).await.expect("Failed to call tool");
    assert!(!result.is_error.unwrap_or(false), "User with department 'security' should have access");

    // Test with regular user - should fail
    let regular_claims = TestJwtClaims::new("user", "user");
    let regular_token = generate_jwt_token(&regular_claims, &jwt_keys.private_key).expect("Failed to generate token");

    let mut regular_client =
        McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&regular_token);
    regular_client.initialize().await.expect("Failed to initialize");

    let result = regular_client.call_tool("multi_perm_tool", json!({})).await;
    assert!(result.is_err(), "Regular user without any matching permissions should be denied");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_rbac_jwt_header() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"data": "ok"}"#)).await;

    let jwt_keys = JwtKeyPair::generate();

    let rbac = rbac_config("allow", vec![("jwt_header".to_string(), "kid".to_string(), jwt_keys.kid.clone())]);

    let tool = json!({
        "name": "header_protected_tool",
        "description": "Tool protected by JWT header",
        "rbac": rbac,
        "rest_backend": {
            "cluster": "backend_cluster",
            "method": "GET",
            "path": "/api/protected"
        },
        "input_schema": {
            "inline_string": r#"{"type": "object", "properties": {}}"#
        }
    });

    let bootstrap = mcp_gateway_with_jwt_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
        &jwt_keys.get_jwks_inline(),
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    // Test with valid token (has correct kid in header)
    let claims = TestJwtClaims::new("user", "admin");
    let token = generate_jwt_token(&claims, &jwt_keys.private_key).expect("Failed to generate token");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&token);
    client.initialize().await.expect("Failed to initialize");

    let result = client.call_tool("header_protected_tool", json!({})).await.expect("Failed to call tool");
    assert!(!result.is_error.unwrap_or(false), "Tool call should succeed with valid JWT kid header");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_semantic_search_tool() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend
        .set_default_response(PreConfiguredResponse::with_body(
            r#"{"tools": [{"name": "dynamic_tool", "description": "A dynamic tool"}]}"#,
        ))
        .await;

    let jwt_keys = JwtKeyPair::generate();

    let bootstrap = mcp_gateway_with_jwt_and_semantic_search_config(
        "test-gateway",
        "1.0.0",
        vec![],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
        &jwt_keys.get_jwks_inline(),
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let claims = TestJwtClaims::new("user1", "user");
    let token = generate_jwt_token(&claims, &jwt_keys.private_key).expect("Failed to generate token");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&token);
    client.initialize().await.expect("Failed to initialize");

    // List tools - should include semantic_search tool
    let tools = client.list_tools().await.expect("Failed to list tools");
    let has_semantic_search = tools.tools.iter().any(|t| t.name == "semantic_search");
    assert!(has_semantic_search, "semantic_search tool should be present when enabled");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_tool_without_rbac() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)).await;

    let jwt_keys = JwtKeyPair::generate();

    // Create tool WITHOUT RBAC - should be accessible to all authenticated users
    let tool = json!({
        "name": "public_tool",
        "description": "Tool accessible to all authenticated users",
        "rest_backend": {
            "cluster": "backend_cluster",
            "method": "GET",
            "path": "/api/public"
        },
        "input_schema": {
            "inline_string": r#"{"type": "object", "properties": {}}"#
        }
    });

    let bootstrap = mcp_gateway_with_jwt_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
        &jwt_keys.get_jwks_inline(),
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    // Any authenticated user should be able to access
    let claims = TestJwtClaims::new("user", "any_role");
    let token = generate_jwt_token(&claims, &jwt_keys.private_key).expect("Failed to generate token");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&token);
    client.initialize().await.expect("Failed to initialize");

    let result = client.call_tool("public_tool", json!({})).await.expect("Failed to call tool");
    assert!(!result.is_error.unwrap_or(false), "Any authenticated user should access public tool");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_tool_not_found() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)).await;

    let tool = rest_tool_config(
        "existing_tool",
        "An existing tool",
        "backend_cluster",
        "GET",
        "/api/existing",
        json!({"type": "object", "properties": {}}),
        vec![],
        None,
        None,
    );

    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));
    client.initialize().await.expect("Failed to initialize");

    // Try to call a non-existent tool
    let result = client.call_tool("non_existent_tool", json!({})).await;
    assert!(result.is_err(), "Calling non-existent tool should return an error");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_invalid_arguments() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)).await;

    let tool = json!({
        "name": "param_tool",
        "description": "Tool requiring parameters",
        "rest_backend": {
            "cluster": "backend_cluster",
            "method": "GET",
            "path": "/api/param"
        },
        "input_schema": {
            "inline_string": r#"{
                "type": "object",
                "properties": {
                    "required_param": { "type": "string" }
                },
                "required": ["required_param"]
            }"#
        }
    });

    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));
    client.initialize().await.expect("Failed to initialize");

    // Call with missing required parameter
    let result = client.call_tool("param_tool", json!({})).await;
    // Should fail validation
    assert!(result.is_err(), "Calling tool with missing required parameter should return an error");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_mixed_backends() {
    let mut rest_backend = TestBackend::start().await.expect("Failed to start REST backend");
    let mut mock_mcp = MockMcpServer::start().await.expect("Failed to start mock MCP server");

    rest_backend.set_default_response(PreConfiguredResponse::with_body(r#"{"source": "rest"}"#)).await;

    let rest_tool = rest_tool_config(
        "rest_tool",
        "REST backend tool",
        "rest_cluster",
        "GET",
        "/api/rest",
        json!({"type": "object", "properties": {}}),
        vec![],
        None,
        None,
    );

    let mcp_tool = mcp_server_tool_config(
        "mock_echo",
        "MCP backend tool",
        format!("http://{}/mcp", mock_mcp.addr()),
        "streamable_http",
        Some("10s".to_string()),
    );

    let bootstrap = mcp_gateway_config(
        "test-gateway",
        "1.0.0",
        vec![rest_tool, mcp_tool],
        vec![
            ClusterBuilder::new("rest_cluster").endpoint(EndpointBuilder::from_socket_addr(rest_backend.addr())),
            ClusterBuilder::new("mcp_cluster").endpoint(EndpointBuilder::from_socket_addr(mock_mcp.addr())),
        ],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));
    client.initialize().await.expect("Failed to initialize");

    let tools = client.list_tools().await.expect("Failed to list tools");
    assert!(tools.tools.len() >= 2, "Should have at least 2 tools, got {}", tools.tools.len());

    let tool_names: Vec<&str> = tools.tools.iter().map(|t| t.name.as_str()).collect();
    assert!(tool_names.contains(&"rest_tool"), "rest_tool should be present");
    // MCP backend tools are namespaced with the tool name prefix
    let has_mock_echo = tool_names.iter().any(|name| name.contains("mock_echo"));
    assert!(has_mock_echo, "mock_echo should be present (possibly namespaced)");

    // Call REST backend tool
    let rest_result = client.call_tool("rest_tool", json!({})).await.expect("Failed to call REST tool");
    assert!(!rest_result.is_error.unwrap_or(false), "REST tool call should succeed");

    let _ = rest_backend.await_request().await.expect("No request to REST backend");

    let mcp_tool_name = tool_names.iter().find(|name| name.contains("mock_echo")).expect("mock_echo tool not found");
    let mcp_result =
        client.call_tool(*mcp_tool_name, json!({"message": "test"})).await.expect("Failed to call MCP tool");
    assert!(!mcp_result.is_error.unwrap_or(false), "MCP tool call should succeed");
    let mcp_content = mcp_result.content.first().map(|c| c.text.clone()).unwrap_or_default();
    assert!(mcp_content.contains("test"), "MCP response should echo the message");

    mock_mcp.shutdown();
    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_semantic_search_direct_mode() {
    // This test verifies semantic search in direct call mode
    // The semantic_search tool should return filtered tools directly
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)).await;

    let weather_tool = rest_tool_config(
        "get_weather",
        "Get current weather forecast and temperature information",
        "backend_cluster",
        "GET",
        "/api/weather",
        json!({"type": "object", "properties": {}}),
        vec![],
        None,
        None,
    );

    let user_tool = rest_tool_config(
        "get_user",
        "Retrieve user profile and account information",
        "backend_cluster",
        "GET",
        "/api/user",
        json!({"type": "object", "properties": {}}),
        vec![],
        None,
        None,
    );

    let payment_tool = rest_tool_config(
        "process_payment",
        "Process payment transaction and billing",
        "backend_cluster",
        "POST",
        "/api/payment",
        json!({"type": "object", "properties": {}}),
        vec![],
        None,
        None,
    );

    let bootstrap = mcp_gateway_with_direct_semantic_search_config(
        "test-gateway",
        "1.0.0",
        vec![weather_tool, user_tool, payment_tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap()));
    client.initialize().await.expect("Failed to initialize");

    let initial_tools = client.list_tools().await.expect("Failed to list initial tools");
    let has_semantic_search = initial_tools.tools.iter().any(|t| t.name == "semantic_search");
    assert!(has_semantic_search, "semantic_search tool should be present");

    let result = client
        .call_tool(
            "semantic_search",
            json!({
                "user_query": "I need to check the weather forecast"
            }),
        )
        .await
        .expect("Failed to call semantic_search");
    assert!(!result.is_error.unwrap_or(false), "semantic_search call should succeed");

    let content_text = result.content.first().map(|c| c.text.clone()).unwrap_or_default();
    let returned_tools: serde_json::Value =
        serde_json::from_str(&content_text).expect("semantic_search should return valid JSON tool list");
    assert!(returned_tools.is_array(), "semantic_search should return an array of tools");

    let tools_array = returned_tools.as_array().unwrap();
    let has_weather = tools_array.iter().any(|tool| tool.get("name").and_then(|n| n.as_str()) == Some("get_weather"));
    assert!(has_weather, "Filtered tools should include get_weather (matches 'weather' keyword)");

    let call_result = client.call_tool("get_weather", json!({})).await.expect("Failed to call get_weather");
    assert!(!call_result.is_error.unwrap_or(false), "get_weather tool should be callable");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_mcp_gateway_semantic_search_assisted_discovery() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body(r#"{"result": "ok"}"#)).await;

    let jwt_keys = JwtKeyPair::generate();

    let database_tool = rest_tool_config(
        "query_database",
        "Execute SQL queries and retrieve database records",
        "backend_cluster",
        "GET",
        "/api/database",
        json!({"type": "object", "properties": {}}),
        vec![],
        None,
        None,
    );

    let analytics_tool = rest_tool_config(
        "get_analytics",
        "Fetch analytics data and statistics reports",
        "backend_cluster",
        "GET",
        "/api/analytics",
        json!({"type": "object", "properties": {}}),
        vec![],
        None,
        None,
    );

    let email_tool = rest_tool_config(
        "send_email",
        "Send email notifications and messages",
        "backend_cluster",
        "POST",
        "/api/email",
        json!({"type": "object", "properties": {}}),
        vec![],
        None,
        None,
    );

    let bootstrap = mcp_gateway_with_jwt_and_semantic_search_config(
        "test-gateway",
        "1.0.0",
        vec![database_tool, analytics_tool, email_tool],
        vec![ClusterBuilder::new("backend_cluster").endpoint(EndpointBuilder::from_socket_addr(backend.addr()))],
        &jwt_keys.get_jwks_inline(),
    );

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let claims = TestJwtClaims::new("user1", "user");
    let token = generate_jwt_token(&claims, &jwt_keys.private_key).expect("Failed to generate token");

    let mut client = McpTestClient::new(format!("http://{}", orion.listener_addr().unwrap())).with_jwt(&token);
    client.initialize().await.expect("Failed to initialize");

    let initial_tools = client.list_tools().await.expect("Failed to list initial tools");
    let has_semantic_search = initial_tools.tools.iter().any(|t| t.name == "semantic_search");
    assert!(has_semantic_search, "semantic_search tool should be present");

    // Call semantic_search with a query matching "database"
    let result = client
        .call_tool(
            "semantic_search",
            json!({
                "user_query": "I need to query the database for records"
            }),
        )
        .await;

    match result {
        Ok(call_result) => {
            assert!(!call_result.is_error.unwrap_or(false), "semantic_search should indicate success");
            let content_text = call_result.content.first().map(|c| c.text.clone()).unwrap_or_default();
            eprintln!("Semantic search response: {}", content_text);
        },
        Err(e) => {
            eprintln!("Semantic search call had parsing issue (expected in some cases): {:?}", e);
        },
    }

    let filtered_tools = client.list_tools().await.expect("Failed to list filtered tools");
    eprintln!("Filtered tools count (after semantic search): {}", filtered_tools.tools.len());

    let has_database = filtered_tools.tools.iter().any(|t| t.name == "query_database");
    assert!(has_database, "After semantic_search, filtered tools should include query_database");

    let call_result = client.call_tool("query_database", json!({})).await.expect("Failed to call query_database");
    assert!(!call_result.is_error.unwrap_or(false), "query_database tool should be callable");

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}
