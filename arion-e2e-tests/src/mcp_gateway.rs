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

//! MCP Gateway E2E Test Utilities
//!
//! This module provides utilities for testing the MCP Gateway functionality:
//! - JWT token generation for RBAC testing
//! - MCP client for making protocol requests
//! - Mock MCP server using rmcp crate

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http::{Method, Request, Response, StatusCode};
use http_body_util::Full;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, RwLock};

use crate::{Error, RequestBuilder, Result, TestClient};

/// JWT Token Generation for RBAC Testing
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
            name: "Test User".to_owned(),
            role: role.into(),
            iat: now,
            exp: now + 3600, // 1 hour
            aud: vec!["mcp-gateway".to_owned()],
            iss: "https://auth.example.com".to_owned(),
            extra: HashMap::new(),
        }
    }

    #[must_use]
    pub fn with_claim(mut self, key: &str, value: impl Into<Value>) -> Self {
        self.extra.insert(key.to_owned(), value.into());
        self
    }

    #[must_use]
    pub fn with_audience(mut self, aud: impl Into<String>) -> Self {
        self.aud = vec![aud.into()];
        self
    }
}

pub struct JwtKeyPair {
    pub private_key: String,
    pub public_key: String,
    pub kid: String,
}

impl JwtKeyPair {
    pub fn generate() -> Self {
        let private_key = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDCM0wgZFmF6Kgj
xYF1JWERld6n+jJZLwtoD/brIxGUvRiamB5JLEyg9kyVX/xfdi4KlAmf+dZtw3IU
VcY0jf+GxsTJpeJtfwJs9elGZnPBlRRhGhPBQrqeim1g0Pi49N+jml4OFONoSfzS
8IIujpUMy3GUWehbIk1L9sfn1dTj4vEx7pkS7StMvmy0JbOXdaKNg7w535siyMIT
eKOg+qTFRN8R9jHYfoNqvFohrdW0/m0lhAiyR6o5vuYkQDIwf2YVBUM3rGC9ZMBn
JA1+xKM1zLSbuHMSHT4tvh9dbGTACrHPaans7zrXpDO0Dfnd8AK0y3cTWBjxDw3+
LxqEBhFTAgMBAAECggEAHAFfyhAOpPP/Q2FZIPap/+o3+Mto9VmGcJRUzGX7RBLc
+HZVb8H2rwO12ZjFAVM+ooHkvXA/DwcvbWVNNwj/P4VsnZPRim7Vf7ca0+80ZEdG
cBZdoPIpjXFzApJAPBP8KFC7nZY/kSuSTS0n6OTg875m+7jXfET/FqRZAcLhd5dj
O/ihOebFtjUf/bl4ki/tg6ccYLFpvWQODFQuGkJI8lIxW3lhBcWYodjfbyt93AhD
zNrqqheNH8sqVJhmb3df65g30SXEfLk8o0xwd0HYeIlevCyTzogQPMbqA0mOwFOK
ixsYmeWNj8qFUPpuAr1nZJ9lskUy8D9ei84JkvX7IQKBgQDsVxesqiKDNkAGkJNB
xGgCrCWRnkUv2xIeBwvjYz2LD/AiHDLEELDLVlX6gxT8gJqMQcXgev7+M93xrx/9
FsAKDSLlEHPQmMefzkp+b1HE2Lid5W13ngyHeOVVmrX9svS8lzuKcOJlkSaxStW7
I7deuOPrMRiaMReViL6ORKEmsQKBgQDSWtRmJsDcjlvGvE23eX1k14Su9erNQmjx
c/j4FTS2Hqs8YvxTSQTwTOq9jm2W73os+VbAawOs4WJ0c1F6XajysKINSdKJMgeD
/f2sGx1jnyXuhvabQ6MZOfnJL7IKXL2vQ1mNL/xFPnhjFbeU6fbsd+NCDavkV2b3
cDIu4OZBQwKBgAzEF49IEV0tDQBNxuaCiWu7iLv45JvVJYFhuA6sSaK9VadCBqv4
itQw8av6cKPC/pYc52dcvXFVs+NeJkgxdmYUl5Hv9ZGK7x1+sx9pO+16F17QCb2w
V9TpftnE5Zeylu2o7ZpoxpHd6U0iUbEuGLWRHx6RJFcP18pH/KMKqfnBAoGBAI/4
uLy9s2yBRtFLmkmELk2xsE9rYuxfkqIHhRSOtwgbD4oCGb8LEAVEL7nTXLBccZuM
gFKsK9TMYe1f7Bk7N2H7gL5lk2JxSnGNimycFk5T48tQtkJoVZ3zb0HCkjHDbdQh
3Y3jlN7ztcPjXkXeqDEKkRFpeAeNxpx+PuqU5SMvAoGALp6rpXjuWmijwc4MnLoc
z/NIew06epFYIP1ZrAY+DUXgJIjK2vwxwlI1Die2REps7aE/HKc62li1ejjHA0ij
fmFAOQc8ayw2/jjJHfQMKdmNLd9s1av6fFY31XsulfKakOBpFhtJAOEbpIz28r0P
o2G9LxgDhY0dWG+u+9Rai7I=
-----END PRIVATE KEY-----"#
            .to_owned();

        let public_key = r#"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwjNMIGRZheioI8WBdSVh
EZXep/oyWS8LaA/26yMRlL0YmpgeSSxMoPZMlV/8X3YuCpQJn/nWbcNyFFXGNI3/
hsbEyaXibX8CbPXpRmZzwZUUYRoTwUK6noptYND4uPTfo5peDhTjaEn80vCCLo6V
DMtxlFnoWyJNS/bH59XU4+LxMe6ZEu0rTL5stCWzl3WijYO8Od+bIsjCE3ijoPqk
xUTfEfYx2H6DarxaIa3VtP5tJYQIskeqOb7mJEAyMH9mFQVDN6xgvWTAZyQNfsSj
Ncy0m7hzEh0+Lb4fXWxkwAqxz2mp7O8616QztA353fACtMt3E1gY8Q8N/i8ahAYR
UwIDAQAB
-----END PUBLIC KEY-----"#
            .to_owned();

        Self { private_key, public_key, kid: "test-key-id".to_owned() }
    }

    pub fn get_jwks(&self) -> Value {
        json!({
            "keys": [{
                "kty": "RSA",
                "e": "AQAB",
                "use": "sig",
                "kid": &self.kid,
                "alg": "RS256",
                "n": "wjNMIGRZheioI8WBdSVhEZXep_oyWS8LaA_26yMRlL0YmpgeSSxMoPZMlV_8X3YuCpQJn_nWbcNyFFXGNI3_hsbEyaXibX8CbPXpRmZzwZUUYRoTwUK6noptYND4uPTfo5peDhTjaEn80vCCLo6VDMtxlFnoWyJNS_bH59XU4-LxMe6ZEu0rTL5stCWzl3WijYO8Od-bIsjCE3ijoPqkxUTfEfYx2H6DarxaIa3VtP5tJYQIskeqOb7mJEAyMH9mFQVDN6xgvWTAZyQNfsSjNcy0m7hzEh0-Lb4fXWxkwAqxz2mp7O8616QztA353fACtMt3E1gY8Q8N_i8ahAYRUw"
            }]
        })
    }

    pub fn get_jwks_inline(&self) -> String {
        self.get_jwks().to_string()
    }
}

pub fn generate_jwt_token(claims: &TestJwtClaims, private_key: &str) -> Result<String> {
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test-key-id".to_owned());

    let encoding_key = EncodingKey::from_rsa_pem(private_key.as_bytes())
        .map_err(|e| Error::Config(format!("Failed to load private key: {e}")))?;

    jsonwebtoken::encode(&header, claims, &encoding_key)
        .map_err(|e| Error::Config(format!("Failed to encode JWT: {e}")))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpJsonRpcRequest {
    pub jsonrpc: String,
    pub id: Option<u64>,
    pub method: String,
    #[serde(default)]
    pub params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpJsonRpcResponse {
    pub jsonrpc: String,
    pub id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<McpJsonRpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpJsonRpcError {
    pub code: i32,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListToolsResult {
    pub tools: Vec<McpTool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallToolParams {
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallToolResult {
    pub content: Vec<ToolContent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub structured_content: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolContent {
    #[serde(rename = "type")]
    pub content_type: String,
    pub text: String,
}

pub struct McpTestClient {
    http_client: TestClient,
    session_id: Option<String>,
    jwt_token: Option<String>,
}

impl McpTestClient {
    pub fn new(addr: SocketAddr) -> Self {
        Self { http_client: TestClient::new(addr), session_id: None, jwt_token: None }
    }

    #[must_use]
    pub fn with_jwt(mut self, token: impl Into<String>) -> Self {
        self.jwt_token = Some(token.into());
        self
    }

    #[must_use]
    pub fn with_session_id(mut self, session_id: impl Into<String>) -> Self {
        self.session_id = Some(session_id.into());
        self
    }

    fn build_request_builder(&self, path: &str, body: Bytes) -> RequestBuilder {
        let mut builder = RequestBuilder::post(path)
            .header("Content-Type", "application/json")
            .header("Accept", "text/event-stream, application/json")
            .body(body);

        if let Some(ref token) = self.jwt_token {
            builder = builder.header("Authorization", format!("Bearer {token}"));
        }

        if let Some(ref session_id) = self.session_id {
            builder = builder.header("Mcp-Session-Id", session_id);
        }

        builder
    }

    pub async fn send_request(&self, request: &McpJsonRpcRequest) -> Result<(McpJsonRpcResponse, Option<String>)> {
        let json_body =
            serde_json::to_vec(request).map_err(|e| Error::Http(format!("Failed to serialize request: {e}")))?;

        let request_builder = self.build_request_builder("/mcp", Bytes::from(json_body));

        let response = self.http_client.send(request_builder).await?;

        let status = response.status;
        let session_id = response.header("mcp-session-id").map(str::to_owned);
        let content_type = response.header("content-type").unwrap_or_default().to_owned();
        let body_text =
            response.body_str().ok_or_else(|| Error::Http("Response body is not valid UTF-8".to_owned()))?;

        if !status.is_success() {
            return Err(Error::Http(format!("HTTP error: {status} - {body_text}")));
        }

        let rpc_response = if content_type.contains("text/event-stream") {
            Self::parse_sse_rpc_response(body_text, request.id)?
        } else {
            serde_json::from_str(body_text).map_err(|e| Error::Http(format!("Failed to parse JSON: {e}")))?
        };

        Ok((rpc_response, session_id))
    }

    fn parse_sse_rpc_response(body_text: &str, expected_id: Option<u64>) -> Result<McpJsonRpcResponse> {
        let mut fallback = None;
        for event in body_text.split("\n\n") {
            for line in event.lines() {
                let Some(data) = line.strip_prefix("data:") else {
                    continue;
                };
                let data = data.trim();
                if data.is_empty() {
                    continue;
                }
                let Ok(response) = serde_json::from_str::<McpJsonRpcResponse>(data) else {
                    continue;
                };
                if expected_id.is_some() && response.id == expected_id {
                    return Ok(response);
                }
                fallback.get_or_insert(response);
            }
        }

        fallback.ok_or_else(|| Error::Http(format!("Failed to parse SSE JSON-RPC response: {body_text}")))
    }

    pub async fn initialize(&mut self) -> Result<Value> {
        let request = McpJsonRpcRequest {
            jsonrpc: "2.0".to_owned(),
            id: Some(1),
            method: "initialize".to_owned(),
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

        if let Some(sid) = session_id {
            self.session_id = Some(sid);
        }

        Ok(response.result.unwrap_or(Value::Null))
    }

    pub async fn list_tools(&self) -> Result<ListToolsResult> {
        let request = McpJsonRpcRequest {
            jsonrpc: "2.0".to_owned(),
            id: Some(2),
            method: "tools/list".to_owned(),
            params: json!({}),
        };

        let (response, _) = self.send_request(&request).await?;

        if let Some(error) = response.error {
            return Err(Error::Http(format!("List tools failed: {}", error.message)));
        }

        let result = response.result.ok_or_else(|| Error::Http("No result in response".to_owned()))?;
        serde_json::from_value(result).map_err(|e| Error::Http(format!("Failed to parse tools: {e}")))
    }

    pub async fn call_tool(&self, name: impl Into<String>, arguments: Value) -> Result<CallToolResult> {
        let request = McpJsonRpcRequest {
            jsonrpc: "2.0".to_owned(),
            id: Some(3),
            method: "tools/call".to_owned(),
            params: json!({
                "name": name.into(),
                "arguments": arguments
            }),
        };

        let (response, _) = self.send_request(&request).await?;

        if let Some(error) = response.error {
            return Err(Error::Http(format!("Call tool failed: {}", error.message)));
        }

        let result = response.result.ok_or_else(|| Error::Http("No result in response".to_owned()))?;
        serde_json::from_value(result).map_err(|e| Error::Http(format!("Failed to parse tool result: {e}")))
    }

    pub async fn ping(&self) -> Result<()> {
        let request =
            McpJsonRpcRequest { jsonrpc: "2.0".to_owned(), id: Some(4), method: "ping".to_owned(), params: json!({}) };

        let (response, _) = self.send_request(&request).await?;

        if response.error.is_some() {
            return Err(Error::Http("Ping failed".to_owned()));
        }

        Ok(())
    }
}

pub struct MockMcpServer {
    addr: SocketAddr,
    tools: Arc<RwLock<Vec<McpTool>>>,
    shutdown_tx: Option<mpsc::Sender<()>>,
    _handle: tokio::task::JoinHandle<()>,
}

impl MockMcpServer {
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;

        let tools = Arc::new(RwLock::new(vec![
            McpTool {
                name: "mock_echo".to_owned(),
                description: "Echo back the input".to_owned(),
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
                name: "mock_add".to_owned(),
                description: "Add two numbers".to_owned(),
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

                            drop(hyper::server::conn::http1::Builder::new()
                                .serve_connection(io, service)
                                .await);
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
            _ = tx.try_send(());
        }
    }
}

#[allow(clippy::too_many_lines)]
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
            jsonrpc: "2.0".to_owned(),
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
                jsonrpc: "2.0".to_owned(),
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
                            content_type: "text".to_owned(),
                            text: format!("Echo: {message}"),
                        }],
                        structured_content: None,
                        is_error: Some(false),
                    }
                },
                "mock_add" => {
                    let a = params.arguments.get("a").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                    let b = params.arguments.get("b").and_then(serde_json::Value::as_f64).unwrap_or(0.0);
                    CallToolResult {
                        content: vec![ToolContent {
                            content_type: "text".to_owned(),
                            text: format!("Result: {}", a + b),
                        }],
                        structured_content: None,
                        is_error: Some(false),
                    }
                },
                _ => CallToolResult {
                    content: vec![ToolContent {
                        content_type: "text".to_owned(),
                        text: format!("Unknown tool: {}", params.name),
                    }],
                    structured_content: None,
                    is_error: Some(true),
                },
            };

            McpJsonRpcResponse { jsonrpc: "2.0".to_owned(), id: mcp_req.id, result: Some(json!(result)), error: None }
        },
        _ => McpJsonRpcResponse {
            jsonrpc: "2.0".to_owned(),
            id: mcp_req.id,
            result: None,
            error: Some(McpJsonRpcError { code: -32601, message: "Method not found".to_owned(), data: None }),
        },
    };

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(serde_json::to_string(&response).unwrap())))
        .unwrap())
}

/// Extension trait for better MCP result assertions
pub trait McpResultExt {
    fn assert_success(&self);
    fn assert_error_contains(&self, expected: &str);
    fn assert_tool_not_found(&self);
}

impl McpResultExt for crate::Result<CallToolResult> {
    fn assert_success(&self) {
        let result = self.as_ref().expect("Expected Ok result, got Err");
        assert!(
            !result.is_error.unwrap_or(false),
            "Tool returned error: {:?}",
            result.content.first().map(|c| &c.text)
        );
    }

    fn assert_error_contains(&self, expected: &str) {
        match self {
            Err(e) => {
                let error_str = e.to_string();
                assert!(error_str.contains(expected), "Error '{error_str}' doesn't contain '{expected}'");
            },
            Ok(result) if result.is_error.unwrap_or(false) => {
                let content = result.content.first().map(|c| c.text.as_str()).unwrap_or("");
                assert!(content.contains(expected), "Error content '{content}' doesn't contain '{expected}'");
            },
            Ok(_) => panic!("Expected error containing '{expected}' but got success"),
        }
    }

    fn assert_tool_not_found(&self) {
        self.assert_error_contains("not found");
    }
}
