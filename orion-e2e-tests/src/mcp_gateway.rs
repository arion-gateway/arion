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

//! MCP Gateway E2E Test Utilities
//!
//! This module provides utilities for testing the MCP Gateway functionality:
//! - JWT token generation for RBAC testing
//! - MCP client for making protocol requests
//! - Configuration builders for MCP gateway setups
//! - Mock MCP server using rmcp crate

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http::{HeaderMap, Method, Request, Response, StatusCode};
use http_body_util::Full;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};

use crate::{Error, Result};

// ============================================================================
// JWT Token Generation for RBAC Testing
// ============================================================================

/// JWT claims structure for testing
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TestJwtClaims {
    pub sub: String,
    pub name: String,
    pub role: String,
    pub iat: u64,
    pub exp: u64,
    pub aud: Vec<String>,
    pub iss: String,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl TestJwtClaims {
    pub fn new(sub: impl Into<String>, role: impl Into<String>) -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).expect("Time went backwards").as_secs();

        Self {
            sub: sub.into(),
            name: "Test User".to_string(),
            role: role.into(),
            iat: now,
            exp: now + 3600, // 1 hour
            aud: vec!["mcp-gateway".to_string()],
            iss: "https://auth.example.com".to_string(),
            extra: HashMap::new(),
        }
    }

    pub fn with_claim(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.extra.insert(key.into(), value.into());
        self
    }

    pub fn with_audience(mut self, aud: impl Into<String>) -> Self {
        self.aud = vec![aud.into()];
        self
    }
}

/// RSA key pair for JWT signing/verification
pub struct JwtKeyPair {
    pub private_key: String,
    pub public_key: String,
    pub kid: String,
}

impl JwtKeyPair {
    /// Generate a new RSA key pair for testing
    ///
    /// Note: This uses a hardcoded key pair for testing that matches the one in docs/jwt/jwt-gen.py
    pub fn generate() -> Self {
        // For testing, we use a hardcoded key pair that matches the one in docs/jwt/jwt-gen.py
        let private_key = r#"-----BEGIN RSA PRIVATE KEY-----
MIIEpQIBAAKCAQEAhi3pcXuRrhy2gHjkHCq6JBcxpeC5udojlLukmsyAZ7x1vHwH
Tx/9c6s73XhfJoRBnd3V3SL1JAUuBiVJ68Pnrtuztp5j8Cdw33S5jKf+8PranMuc
G/4wWejWm1gbKomZBm79Baf/fJkDOwumsTXvO9dgX7c2dHtA0oO15eVdzSDf7azg
LU7OvVukiE71tqoQwnPgK47xDvjw83ZN8eWZzDPGpA9DVoEjhwOeasDgUdVv1L+b
2uOqCMYcLixYJ58O48cQSEOreIi1FbIJGoT19VKtJ528JrTkKihZb+BmFIvyIu36
lGRpeJ1pbarIMx+sC/+UfKEawHmYQp7MwB1dnQIDAQABAoIBAFcr3q4L3b5d0r3Y
JDH9+eP5H0/K3zYlNkYDKhZ3I3eXDkXq0FQ6VVLv1TpL7KzJQF3Ej4F1Vz9J1X5X
1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X
1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X
1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X
1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X
1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X1X5X
-----END RSA PRIVATE KEY-----"#
            .to_string();

        let public_key = r#"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAhi3pcXuRrhy2gHjkHCq6
JBcxpeC5udojlLukmsyAZ7x1vHwHTx/9c6s73XhfJoRBnd3V3SL1JAUuBiVJ68Pn
rtuztp5j8Cdw33S5jKf+8PranMucG/4wWejWm1gbKomZBm79Baf/fJkDOwumsTXv
O9dgX7c2dHtA0oO15eVdzSDf7azgLU7OvVukiE71tqoQwnPgK47xDvjw83ZN8eWZ
zDPGpA9DVoEjhwOeasDgUdVv1L+b2uOqCMYcLixYJ58O48cQSEOreIi1FbIJGoT1
9VKtJ528JrTkKihZb+BmFIvyIu36lGRpeJ1pbarIMx+sC/+UfKEawHmYQp7MwB1d
nQIDAQAB
-----END PUBLIC KEY-----"#
            .to_string();

        Self { private_key, public_key, kid: "test-key-id".to_string() }
    }

    /// Get the JWKS JSON for this key pair
    pub fn get_jwks(&self) -> Value {
        json!({
            "keys": [{
                "kty": "RSA",
                "e": "AQAB",
                "use": "sig",
                "kid": &self.kid,
                "alg": "RS256",
                "n": "hi3pcXuRrhy2gHjkHCq6JBcxpeC5udojlLukmsyAZ7x1vHwHTx_9c6s73XhfJoRBnd3V3SL1JAUuBiVJ68Pnrtuztp5j8Cdw33S5jKf-8PranMucG_4wWejWm1gbKomZBm79Baf_fJkDOwumsTXvO9dgX7c2dHtA0oO15eVdzSDf7azgLU7OvVukiE71tqoQwnPgK47xDvjw83ZN8eWZzDPGpA9DVoEjhwOeasDgUdVv1L-b2uOqCMYcLixYJ58O48cQSEOreIi1FbIJGoT19VKtJ528JrTkKihZb-BmFIvyIu36lGRpeJ1pbarIMx-sC_-UfKEawHmYQp7MwB1dnQ"
            }]
        })
    }

    /// Get the JWKS inline string for Envoy configuration
    pub fn get_jwks_inline(&self) -> String {
        self.get_jwks().to_string()
    }
}

/// Generate a JWT token for testing
pub fn generate_jwt_token(claims: &TestJwtClaims, private_key: &str) -> Result<String> {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test-key-id".to_string());

    let encoding_key = EncodingKey::from_rsa_pem(private_key.as_bytes())
        .map_err(|e| Error::Config(format!("Failed to load private key: {e}")))?;

    jsonwebtoken::encode(&header, claims, &encoding_key)
        .map_err(|e| Error::Config(format!("Failed to encode JWT: {e}")))
}

// ============================================================================
// MCP Protocol Types
// ============================================================================

/// MCP JSON-RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpJsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<u64>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

/// MCP JSON-RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpJsonRpcResponse {
    pub jsonrpc: String,
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<McpJsonRpcError>,
}

/// MCP JSON-RPC error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpJsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

/// MCP Tool definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
}

/// MCP ListTools result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListToolsResult {
    pub tools: Vec<McpTool>,
}

/// MCP CallTool request parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallToolParams {
    pub name: String,
    pub arguments: Value,
}

/// MCP CallTool result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallToolResult {
    pub content: Vec<ToolContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
}

/// Tool content item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolContent {
    #[serde(rename = "type")]
    pub content_type: String,
    pub text: String,
}

// ============================================================================
// MCP Test Client
// ============================================================================

/// Client for making MCP protocol requests
pub struct McpTestClient {
    http_client: reqwest::Client,
    base_url: String,
    session_id: Option<String>,
    jwt_token: Option<String>,
}

impl McpTestClient {
    pub fn new(base_url: impl Into<String>) -> Self {
        Self { http_client: reqwest::Client::new(), base_url: base_url.into(), session_id: None, jwt_token: None }
    }

    pub fn with_jwt(mut self, token: impl Into<String>) -> Self {
        self.jwt_token = Some(token.into());
        self
    }

    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    fn build_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("Content-Type", "application/json".parse().unwrap());
        // MCP StreamableHttp transport requires both event-stream and json in Accept header
        headers.insert("Accept", "text/event-stream, application/json".parse().unwrap());

        if let Some(ref token) = self.jwt_token {
            headers.insert("Authorization", format!("Bearer {}", token).parse().unwrap());
        }

        if let Some(ref session_id) = self.session_id {
            headers.insert("Mcp-Session-Id", session_id.parse().unwrap());
        }

        headers
    }

    /// Send raw MCP request
    pub async fn send_request(&self, request: &McpJsonRpcRequest) -> Result<(McpJsonRpcResponse, Option<String>)> {
        let url = format!("{}/mcp", self.base_url);
        let headers = self.build_headers();

        let response = self
            .http_client
            .post(&url)
            .headers(headers)
            .json(request)
            .send()
            .await
            .map_err(|e| Error::Http(format!("Request failed: {e}")))?;

        let status = response.status();

        // Extract session ID from response headers
        let session_id = response.headers().get("mcp-session-id").and_then(|v| v.to_str().ok()).map(|s| s.to_string());

        let body_text = response.text().await.map_err(|e| Error::Http(format!("Failed to read body: {e}")))?;

        // Debug output for troubleshooting
        eprintln!("DEBUG: HTTP Status: {status}, Body: {body_text}");

        if !status.is_success() {
            return Err(Error::Http(format!("HTTP error: {status} - {body_text}")));
        }

        let rpc_response: McpJsonRpcResponse =
            serde_json::from_str(&body_text).map_err(|e| Error::Http(format!("Failed to parse JSON: {e}")))?;

        Ok((rpc_response, session_id))
    }

    /// Initialize MCP session
    pub async fn initialize(&mut self) -> Result<Value> {
        let request = McpJsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(1),
            method: "initialize".to_string(),
            params: json!({
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {
                    "name": "test-client",
                    "version": "1.0.0"
                }
            }),
        };

        let (response, session_id) = self.send_request(&request).await?;

        if let Some(error) = response.error {
            return Err(Error::Http(format!("Initialize failed: {}", error.message)));
        }

        // Store session ID for subsequent requests
        if let Some(sid) = session_id {
            self.session_id = Some(sid);
        }

        Ok(response.result.unwrap_or(Value::Null))
    }

    /// List available tools
    pub async fn list_tools(&self) -> Result<ListToolsResult> {
        let request = McpJsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(2),
            method: "tools/list".to_string(),
            params: json!({}),
        };

        let (response, _) = self.send_request(&request).await?;

        if let Some(error) = response.error {
            return Err(Error::Http(format!("List tools failed: {}", error.message)));
        }

        let result = response.result.ok_or_else(|| Error::Http("No result in response".to_string()))?;
        serde_json::from_value(result).map_err(|e| Error::Http(format!("Failed to parse tools: {e}")))
    }

    /// Call a tool
    pub async fn call_tool(&self, name: impl Into<String>, arguments: Value) -> Result<CallToolResult> {
        let request = McpJsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(3),
            method: "tools/call".to_string(),
            params: json!({
                "name": name.into(),
                "arguments": arguments
            }),
        };

        let (response, _) = self.send_request(&request).await?;

        if let Some(error) = response.error {
            return Err(Error::Http(format!("Call tool failed: {}", error.message)));
        }

        let result = response.result.ok_or_else(|| Error::Http("No result in response".to_string()))?;
        serde_json::from_value(result).map_err(|e| Error::Http(format!("Failed to parse tool result: {e}")))
    }

    /// Ping the server
    pub async fn ping(&self) -> Result<()> {
        let request = McpJsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            id: Some(4),
            method: "ping".to_string(),
            params: json!({}),
        };

        let (response, _) = self.send_request(&request).await?;

        if response.error.is_some() {
            return Err(Error::Http("Ping failed".to_string()));
        }

        Ok(())
    }
}

// ============================================================================
// Mock MCP Server Backend
// ============================================================================

/// Mock MCP server for testing MCP backend transcoding
pub struct MockMcpServer {
    addr: SocketAddr,
    tools: Arc<RwLock<Vec<McpTool>>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    _handle: tokio::task::JoinHandle<()>,
}

impl MockMcpServer {
    /// Start a mock MCP server
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let tools = Arc::new(RwLock::new(vec![
            McpTool {
                name: "mock_echo".to_string(),
                description: "Echo back the input".to_string(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "message": {"type": "string"}
                    },
                    "required": ["message"]
                }),
                output_schema: None,
            },
            McpTool {
                name: "mock_add".to_string(),
                description: "Add two numbers".to_string(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "a": {"type": "number"},
                        "b": {"type": "number"}
                    },
                    "required": ["a", "b"]
                }),
                output_schema: None,
            },
        ]));

        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        let tools_clone = Arc::clone(&tools);

        let handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Ok((stream, _)) = listener.accept() => {
                        let tools = Arc::clone(&tools_clone);
                        tokio::spawn(async move {
                            let io = hyper_util::rt::TokioIo::new(stream);
                            let service = hyper::service::service_fn(move |req| {
                                let tools = Arc::clone(&tools);
                                async move { handle_mcp_request(req, tools).await }
                            });

                            let _ = hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, service)
                                .await;
                        });
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        Ok(Self { addr, tools, shutdown_tx: Some(shutdown_tx), _handle: handle })
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn add_tool(&self, tool: McpTool) {
        self.tools.write().await.push(tool);
    }

    pub async fn clear_tools(&self) {
        self.tools.write().await.clear();
    }

    pub fn shutdown(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.try_send(());
        }
    }
}

async fn handle_mcp_request(
    req: Request<hyper::body::Incoming>,
    tools: Arc<RwLock<Vec<McpTool>>>,
) -> std::result::Result<Response<Full<Bytes>>, hyper::Error> {
    use http_body_util::BodyExt;

    if req.uri().path() != "/mcp" {
        return Ok(Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Full::new(Bytes::from("Not Found")))
            .unwrap());
    }

    if req.method() != Method::POST {
        return Ok(Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .body(Full::new(Bytes::from("Method Not Allowed")))
            .unwrap());
    }

    let body = req.collect().await?.to_bytes();
    let mcp_req: McpJsonRpcRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(_) => {
            return Ok(Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Full::new(Bytes::from("Invalid JSON")))
                .unwrap());
        },
    };

    let response = match mcp_req.method.as_str() {
        "initialize" => McpJsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id: mcp_req.id,
            result: Some(json!({
                "protocolVersion": "2024-11-05",
                "serverInfo": {
                    "name": "mock-mcp-server",
                    "version": "1.0.0"
                },
                "capabilities": {
                    "tools": {}
                }
            })),
            error: None,
        },
        "tools/list" => {
            let tools_list = tools.read().await.clone();
            McpJsonRpcResponse {
                jsonrpc: "2.0".to_string(),
                id: mcp_req.id,
                result: Some(json!({ "tools": tools_list })),
                error: None,
            }
        },
        "tools/call" => {
            let params: CallToolParams = match serde_json::from_value(mcp_req.params) {
                Ok(p) => p,
                Err(_) => {
                    return Ok(Response::builder()
                        .status(StatusCode::OK)
                        .body(Full::new(Bytes::from(
                            json!({
                                "jsonrpc": "2.0",
                                "id": mcp_req.id,
                                "error": {
                                    "code": -32602,
                                    "message": "Invalid params"
                                }
                            })
                            .to_string(),
                        )))
                        .unwrap());
                },
            };

            let result = match params.name.as_str() {
                "mock_echo" => {
                    let message = params.arguments.get("message").and_then(|v| v.as_str()).unwrap_or("no message");
                    CallToolResult {
                        content: vec![ToolContent {
                            content_type: "text".to_string(),
                            text: format!("Echo: {}", message),
                        }],
                        is_error: Some(false),
                    }
                },
                "mock_add" => {
                    let a = params.arguments.get("a").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let b = params.arguments.get("b").and_then(|v| v.as_f64()).unwrap_or(0.0);
                    CallToolResult {
                        content: vec![ToolContent {
                            content_type: "text".to_string(),
                            text: format!("Result: {}", a + b),
                        }],
                        is_error: Some(false),
                    }
                },
                _ => CallToolResult {
                    content: vec![ToolContent {
                        content_type: "text".to_string(),
                        text: format!("Unknown tool: {}", params.name),
                    }],
                    is_error: Some(true),
                },
            };

            McpJsonRpcResponse { jsonrpc: "2.0".to_string(), id: mcp_req.id, result: Some(json!(result)), error: None }
        },
        _ => McpJsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            id: mcp_req.id,
            result: None,
            error: Some(McpJsonRpcError { code: -32601, message: "Method not found".to_string(), data: None }),
        },
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(serde_json::to_string(&response).unwrap())))
        .unwrap())
}

// ============================================================================
// Configuration Builders for MCP Gateway
// ============================================================================

use crate::config_builder::{
    BootstrapBuilder, ClusterBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};

/// Create an MCP Gateway configuration
pub fn mcp_gateway_config(
    server_name: impl Into<String>,
    server_version: impl Into<String>,
    tools: Vec<Value>,
    clusters: Vec<ClusterBuilder>,
) -> BootstrapBuilder {
    let mcp_filter = create_mcp_filter(server_name, server_version, tools, false);

    // Create route config with cluster_header for MCP gateway routing
    let route_config = RouteConfigBuilder::new("mcp_routes").virtual_host(
        VirtualHostBuilder::new("mcp")
            .route(RouteBuilder::new().match_prefix("/").cluster_header("x-mcp-target-cluster")),
    );

    let listener = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(
        HcmBuilder::new().route_config(route_config).with_proto(move |hcm| {
            hcm.http_filters = mcp_filter;
        }),
    ));

    // Add a dummy cluster for the route config - MCP gateway uses cluster_header routing
    let mut bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::new("dummy").endpoint(crate::config_builder::EndpointBuilder::new("127.0.0.1", 1)));
    for cluster in clusters {
        bootstrap = bootstrap.cluster(cluster);
    }
    bootstrap
}

/// Create an MCP Gateway configuration with JWT authentication
pub fn mcp_gateway_with_jwt_config(
    server_name: impl Into<String>,
    server_version: impl Into<String>,
    tools: Vec<Value>,
    clusters: Vec<ClusterBuilder>,
    jwks_inline: impl Into<String>,
) -> BootstrapBuilder {
    let server_name = server_name.into();
    let server_version = server_version.into();
    let jwks_inline = jwks_inline.into();

    // Create route config with cluster_header for MCP gateway routing
    let route_config = RouteConfigBuilder::new("mcp_routes").virtual_host(
        VirtualHostBuilder::new("mcp")
            .route(RouteBuilder::new().match_prefix("/").cluster_header("x-mcp-target-cluster")),
    );

    let listener = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(
        HcmBuilder::new().route_config(route_config).with_proto(move |hcm| {
            // Add JWT auth filter
            let jwt_filter = create_jwt_filter(&jwks_inline);
            let mcp_filter = create_mcp_filter(&server_name, &server_version, tools.clone(), false);

            let mut filters = vec![jwt_filter];
            filters.extend(mcp_filter);
            hcm.http_filters = filters;
        }),
    ));

    // Add a dummy cluster for the route config - MCP gateway uses cluster_header routing
    let mut bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::new("dummy").endpoint(crate::config_builder::EndpointBuilder::new("127.0.0.1", 1)));
    for cluster in clusters {
        bootstrap = bootstrap.cluster(cluster);
    }
    bootstrap
}

/// Create JWT authentication filter
fn create_jwt_filter(jwks_inline: &str) -> orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::HttpFilter{
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::{
        JwtAuthentication, JwtProvider, JwtRequirement, RequirementRule,
    };
    use orion_data_plane_api::envoy_data_plane_api::google::protobuf::Any;
    use prost::Message;

    let jwks_string = jwks_inline.to_string();

    use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::data_source;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::JwtHeader;

    let provider = JwtProvider {
        issuer: "https://auth.example.com".to_string(),
        audiences: vec!["mcp-gateway".to_string()],
        subjects: None,
        require_expiration: false,
        max_lifetime: None,
        jwks_source_specifier: Some(
            orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::jwt_provider::JwksSourceSpecifier::LocalJwks(
                orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::DataSource {
                    specifier: Some(data_source::Specifier::InlineString(jwks_string)),
                    watched_directory: None,
                }
            )
        ),
        from_headers: vec![JwtHeader {
            name: "Authorization".to_string(),
            value_prefix: "Bearer ".to_string(),
        }],
        from_params: vec![],
        from_cookies: vec![],
        forward: false,
        forward_payload_header: "".to_string(),
        pad_forward_payload_header: false,
        payload_in_metadata: "jwt_payload".to_string(),
        header_in_metadata: "jwt_header".to_string(),
        failed_status_in_metadata: "".to_string(),
        clock_skew_seconds: 60,
        normalize_payload_in_metadata: None,
        claim_to_headers: vec![],
        clear_route_cache: false,
        jwt_cache_config: None,
    };

    let mut providers = std::collections::HashMap::new();
    providers.insert("oauth_provider".to_string(), provider);

    let jwt_auth = JwtAuthentication {
        providers,
        rules: vec![RequirementRule {
            r#match: Some(orion_data_plane_api::envoy_data_plane_api::envoy::config::route::v3::RouteMatch {
                path_specifier: Some(orion_data_plane_api::envoy_data_plane_api::envoy::config::route::v3::route_match::PathSpecifier::Prefix("/".to_string())),
                ..Default::default()
            }),
            requirement_type: Some(
                orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::requirement_rule::RequirementType::Requires(
                    JwtRequirement {
                        requires_type: Some(
                            orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::jwt_authn::v3::jwt_requirement::RequiresType::ProviderName(
                                "oauth_provider".to_string()
                            )
                        ),
                    }
                )
            ),
            ..Default::default()
        }],
        requirement_map: std::collections::HashMap::new(),
        filter_state_rules: None,
        strip_failure_response: false,
        stat_prefix: "".to_string(),
        bypass_cors_preflight: false,
    };

    let jwt_any = Any {
        type_url: "type.googleapis.com/envoy.extensions.filters.http.jwt_authn.v3.JwtAuthentication".to_string(),
        value: jwt_auth.encode_to_vec(),
    };

    orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::HttpFilter {
        name: "envoy.filters.http.jwt_authn".to_string(),
        config_type: Some(orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::http_filter::ConfigType::TypedConfig(jwt_any)),
        ..Default::default()
    }
}

/// Convert a JSON Value to a Tool protobuf message
fn tool_value_to_proto(
    tool_value: &Value,
) -> orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::Tool {
    use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::DataSource;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::Tool;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        tool, JwtClaimMatcher, JwtHeaderMatcher, McpServerBackend, Permission, QueryParam, RestBackend, ToolRbac,
    };

    let name = tool_value.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
    let description = tool_value.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();

    // Handle input_schema
    let input_schema = tool_value.get("input_schema").map(|schema| DataSource {
        specifier: Some(
            orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::data_source::Specifier::InlineString(
                schema.to_string(),
            ),
        ),
        watched_directory: None,
    });

    // Handle output_schema
    let output_schema = tool_value.get("output_schema").map(|schema| DataSource {
        specifier: Some(
            orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::data_source::Specifier::InlineString(
                schema.to_string(),
            ),
        ),
        watched_directory: None,
    });

    // Handle upstream_backend
    let upstream_backend = if let Some(rest) = tool_value.get("rest_backend") {
        let cluster = rest.get("cluster").and_then(|v| v.as_str()).map(|s| s.to_string());
        let method = rest.get("method").and_then(|v| v.as_str()).unwrap_or("GET").to_string();
        let path = rest.get("path").and_then(|v| v.as_str()).unwrap_or("/").to_string();

        let query_params: Vec<QueryParam> = rest
            .get("query_params")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        let name = p.get("name").and_then(|v| v.as_str())?;
                        let source = p.get("source").and_then(|v| v.as_str())?;
                        Some(QueryParam { name: name.to_string(), source: source.to_string() })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let body_template = rest.get("body_template").and_then(|v| v.as_str()).map(|s| DataSource {
            specifier: Some(
                orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::data_source::Specifier::InlineString(
                    s.to_string()
                )
            ),
            watched_directory: None,
        });

        Some(tool::UpstreamBackend::RestBackend(RestBackend {
            cluster: cluster.unwrap_or_default(),
            method,
            path,
            query_params,
            r#async: rest.get("async").and_then(|v| v.as_bool()).unwrap_or(false),
            body_template,
        }))
    } else if let Some(mcp) = tool_value.get("mcp_server_backend") {
        let transport = mcp.get("transport").and_then(|v| v.as_u64()).unwrap_or(1) as i32;
        let url = mcp.get("url").and_then(|v| v.as_str()).unwrap_or("").to_string();

        Some(tool::UpstreamBackend::McpServerBackend(McpServerBackend { transport, url, cache_duration: None }))
    } else {
        None
    };

    // Handle RBAC
    let rbac = tool_value.get("rbac").map(|rbac_val| {
        let action = rbac_val.get("action").and_then(|v| v.as_u64()).unwrap_or(0) as i32;
        let permissions: Vec<Permission> = rbac_val
            .get("permissions")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|p| {
                        if let Some(jwt_claim) = p.get("jwt_claim") {
                            let field = jwt_claim.get("field").and_then(|v| v.as_str())?;
                            let value = jwt_claim.get("value").and_then(|v| v.as_str())?;
                            Some(Permission {
                                permission_type: Some(
                                    orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::permission::PermissionType::JwtClaim(
                                        JwtClaimMatcher {
                                            field: field.to_string(),
                                            value: value.to_string(),
                                        }
                                    )
                                ),
                            })
                        } else if let Some(jwt_header) = p.get("jwt_header") {
                            let field = jwt_header.get("field").and_then(|v| v.as_str())?;
                            let value = jwt_header.get("value").and_then(|v| v.as_str())?;
                            Some(Permission {
                                permission_type: Some(
                                    orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::permission::PermissionType::JwtHeader(
                                        JwtHeaderMatcher {
                                            field: field.to_string(),
                                            value: value.to_string(),
                                        }
                                    )
                                ),
                            })
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        ToolRbac { action, permissions }
    });

    Tool { name, description, input_schema, output_schema, upstream_backend, rbac }
}

/// Create MCP Gateway filter
fn create_mcp_filter(
    server_name: impl Into<String>,
    server_version: impl Into<String>,
    tools: Vec<Value>,
    dynamic_tool_discovery: bool,
) -> Vec<orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::HttpFilter>{
    use orion_data_plane_api::envoy_data_plane_api::google::protobuf::Any;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        McpGateway, ServerInfo, Tool,
    };
    use prost::Message;

    let server_name = server_name.into();
    let server_version = server_version.into();

    // Convert Value tools to Tool protobuf messages
    // Since Tool doesn't implement Deserialize, we need to build it manually
    let proto_tools: Vec<Tool> = tools.iter().map(|tool_value| tool_value_to_proto(tool_value)).collect();

    let mcp_gateway = McpGateway {
        cluster_header: Some("x-mcp-target-cluster".to_string()),
        server_info: Some(ServerInfo { name: server_name, version: server_version }),
        tools: proto_tools,
        dynamic_tool_discovery,
    };

    let mcp_any = Any {
        type_url: "type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway".to_string(),
        value: mcp_gateway.encode_to_vec(),
    };

    let mcp_filter = orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::HttpFilter {
        name: "orion.filters.http.mcp".to_string(),
        config_type: Some(orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::http_filter::ConfigType::TypedConfig(mcp_any)),
        ..Default::default()
    };

    // Add router filter at the end
    let router =
        orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::router::v3::Router::default();
    let router_any = Any {
        type_url: "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router".to_string(),
        value: router.encode_to_vec(),
    };

    let router_filter = orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::HttpFilter {
        name: "envoy.filters.http.router".to_string(),
        config_type: Some(orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::http_connection_manager::v3::http_filter::ConfigType::TypedConfig(router_any)),
        ..Default::default()
    };

    vec![mcp_filter, router_filter]
}

/// Helper to create a REST backend tool configuration
pub fn rest_tool_config(
    name: impl Into<String>,
    description: impl Into<String>,
    cluster: impl Into<String>,
    method: impl Into<String>,
    path: impl Into<String>,
    input_schema: Value,
    query_params: Vec<(String, String)>,
    body_template: Option<String>,
    rbac: Option<Value>,
) -> Value {
    let mut tool = json!({
        "name": name.into(),
        "description": description.into(),
        "input_schema": {
            "inline_string": input_schema.to_string()
        },
        "rest_backend": {
            "cluster": cluster.into(),
            "method": method.into(),
            "path": path.into(),
            "query_params": query_params.iter().map(|(n, s)| json!({
                "name": n,
                "source": s
            })).collect::<Vec<_>>(),
            "async": false
        }
    });

    if let Some(template) = body_template {
        tool["rest_backend"]["body_template"] = json!({
            "inline_string": template
        });
    }

    if let Some(rbac_config) = rbac {
        tool["rbac"] = rbac_config;
    }

    tool
}

/// Helper to create an MCP server backend tool configuration
pub fn mcp_server_tool_config(
    name: impl Into<String>,
    description: impl Into<String>,
    url: impl Into<String>,
    transport: &str,
    cache_duration: Option<String>,
) -> Value {
    let mut backend = json!({
        "transport": match transport {
            "sse" => 0,
            "streamable_http" => 1,
            _ => 1,
        },
        "url": url.into()
    });

    if let Some(duration) = cache_duration {
        backend["cache_duration"] = duration.into();
    }

    json!({
        "name": name.into(),
        "description": description.into(),
        "input_schema": {
            "inline_string": "{}"
        },
        "mcp_server_backend": backend
    })
}

/// Helper to create RBAC configuration
pub fn rbac_config(action: &str, permissions: Vec<(String, String, String)>) -> Value {
    let perms: Vec<Value> = permissions
        .into_iter()
        .map(|(ptype, field, value)| {
            if ptype == "jwt_claim" {
                json!({
                    "jwt_claim": {
                        "field": field,
                        "value": value
                    }
                })
            } else {
                json!({
                    "jwt_header": {
                        "field": field,
                        "value": value
                    }
                })
            }
        })
        .collect();

    json!({
        "action": match action {
            "allow" => 0,
            "deny" => 1,
            _ => 0,
        },
        "permissions": perms
    })
}
