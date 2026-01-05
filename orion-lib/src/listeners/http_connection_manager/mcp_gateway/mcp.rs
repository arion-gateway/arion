use bytes::{BufMut, Bytes, BytesMut};
use dashmap::DashMap;
use futures::SinkExt;
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty, Full};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpGateway as McpGatewayConfig;
use scopeguard::defer;
use serde::Serialize;
use serde_json::json;
use smol_str::{SmolStr, ToSmolStr};
use std::{
    panic,
    sync::{atomic::AtomicUsize, Arc},
};
use tokio::sync::Mutex;
use tracing::debug;
use uuid::Uuid;

use crate::{
    body::{
        instrumented_body::InstrumentedBody,
        response_flags::BodyKind,
        sse_body::{SseBody, SseSender},
        timeout_body::TimeoutBody,
    },
    listeners::{
        http_connection_manager::mcp_gateway::model::{self},
        http_filters::{FactoryFilter, FilterDecision},
        listener::FilterListenerContext,
        metadata::DownstreamMetadata,
    },
    OrionRequestBody, OrionResponseBody, PolyBody,
};

const SESSION_ID_PREFIX: &str = "sessionId=";
const SSE_MESSAGE_PREFIX: &str = "event: message\ndata: ";
const MCP_MESSAGE_ENDPOINT: &str = "/mcp/message";
const MAX_CONCURRENT_ASYNC_REQUESTS: usize = 8192;

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct SessionId(pub SmolStr);

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Default)]
pub struct Session {
    listener_name: &'static str, // to handle session eviction from listener.sse_map
    session_id: SessionId,
    sender: Mutex<SseSender>,
}

#[derive(Debug, Default)]
pub struct McpGatewayListenerContext {
    sse_map: DashMap<SessionId, Arc<Session>>,
    active_async_requests: AtomicUsize,
}

#[derive(Debug)]
pub struct McpGatewayInner {
    config: McpGatewayConfig,
}

#[derive(Debug, Default, Clone)]
struct SessionContext(Option<Arc<Session>>);

impl SessionContext {
    pub fn get(&self) -> &Session {
        unsafe { self.0.as_ref().unwrap_unchecked() }
    }
}

/// McpGateway filter
#[derive(Debug, Clone)]
pub struct McpGateway {
    inner: Arc<McpGatewayInner>,
    session_ctx: SessionContext,
    request_id: model::RequestId,
    version: http::Version,
    initialize_request_params: Option<model::InitializeRequestParam>,
}

impl From<McpGatewayConfig> for McpGateway {
    fn from(config: McpGatewayConfig) -> Self {
        Self {
            inner: Arc::new(McpGatewayInner { config }),
            session_ctx: SessionContext(None),
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
        }
    }
}

impl FactoryFilter for McpGateway {
    fn new_from(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            session_ctx: SessionContext(None),
            request_id: model::RequestId::Number(0),
            initialize_request_params: None,
            version: http::Version::default(),
        }
    }
}

impl McpGateway {
    pub async fn apply_request(&mut self, request: &mut Request<OrionRequestBody>) -> FilterDecision {
        debug!(target: "mcp_gateway", "apply_request: processing request: {:?}", request);

        self.version = request.version();

        let Some(metadata) = request.extensions().get::<DownstreamMetadata>() else {
            debug!(target: "mcp_gateway", "apply_request: failed to retrieve metadata");
            return FilterDecision::internal_server_error("Failed to retrieve metadata", self.version);
        };

        // get global context for this listener
        let mcp_ctx = McpGatewayListenerContext::get_filter_context(metadata.listener_name);
        debug!(target: "mcp_gateway", "apply_request: listener name: {}", metadata.listener_name);

        match (request.method(), request.uri().path()) {
            (&Method::GET, "/sse") => self.handle_sse_handshake(&mcp_ctx, request, metadata.listener_name).await,
            (&Method::OPTIONS, "/sse") | (&Method::OPTIONS, MCP_MESSAGE_ENDPOINT)  => self.handle_preflight_checks(request).await,
            (&Method::POST, MCP_MESSAGE_ENDPOINT) => {
                self.handle_message_endpoint(&mcp_ctx, request, metadata.listener_name).await
            },
            _ => {
                debug!(target: "mcp_gateway", "apply_request: no route found");
                FilterDecision::no_route_found(self.version)
            },
        }

        // Implement request routing/filtering logic here
        // let headers = request.headers_mut();
        // headers.append(&self.inner.config.cluster_header, HeaderValue::from_static("cluster_http"));
        // FilterDecision::Continue
    }

    pub async fn apply_response(&mut self, response: &mut Response<OrionResponseBody>) -> FilterDecision {
        // get global context for this listener
        let mcp_ctx = McpGatewayListenerContext::get_filter_context(self.session_ctx.get().listener_name);
        defer! {
            mcp_ctx.active_async_requests.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        };

        // collect the body...
        let Ok(body) = response.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_response: failed to collect response body");
            let msg = prepare_rpc_error(
                self.request_id.clone(),
                model::ErrorData::internal_error("failed to collect response body", None),
            );

            if let Err(e) = self.send_message(msg).await {
                return e;
            }
            return FilterDecision::Continue;
        };

        if let Err(e) = self.send_message(body.to_bytes()).await {
            return e;
        }
        FilterDecision::Continue
    }

    pub async fn handle_message_endpoint(
        &mut self,
        state: &McpGatewayListenerContext,
        request: &mut Request<OrionRequestBody>,
        _listener_name: &'static str,
    ) -> FilterDecision {
        // extract session ID from request
        let Some(session_id) = Self::get_session_id(request) else {
            debug!(target: "mcp_gateway", "handle_message_endpoint: missing session ID!");
            return FilterDecision::bad_request(self.version);
        };

        // search for session in map
        let Some(session) = state.sse_map.get(&session_id) else {
            debug!(target: "mcp_gateway", "handle_message_endpoint: session ID {session_id} not found!");
            return FilterDecision::not_found(self.version);
        };

        // save the current session ID and session
        self.session_ctx = SessionContext(Some(session.value().clone()));

        // shallow parsing the body of the request:

        let Ok(body) = request.body_mut().collect().await else {
            debug!(target: "mcp_gateway", "apply_request: failed to collect request body");
            return FilterDecision::internal_server_error("Failed to collect request body", self.version);
        };

        let Ok(accepted) = self.build_accepted_response() else {
            return FilterDecision::internal_server_error("Failed to build accepted response", self.version);
        };

        let res = self.handle_message(body.to_bytes());
        match res {
            MessageResponse::Error(json_rpc_error) => {
                if let Err(e) = self.send_message(to_json_bytes(json_rpc_error)).await {
                    return e;
                }
                return FilterDecision::DirectResponse(accepted);
            },
            MessageResponse::RpcResponse(json_rpc_response) => {
                if let Err(e) = self.send_message(to_json_bytes(json_rpc_response)).await {
                    return e;
                }
                return FilterDecision::DirectResponse(accepted);
            },
            MessageResponse::Nothing => {
                return FilterDecision::DirectResponse(accepted);
            },
            MessageResponse::Upstream(req) => {
                if state.active_async_requests.load(std::sync::atomic::Ordering::Relaxed)
                    >= MAX_CONCURRENT_ASYNC_REQUESTS
                {
                    debug!(target: "mcp_gateway", "apply_request: rate limited!");
                    return FilterDecision::rate_limited(request.version());
                }
                state.active_async_requests.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                debug!(target: "mcp_gateway", "Sending async request: {:#?}", req);
                return FilterDecision::AsyncRequest(accepted, Some(req));
            },
        }
    }

    pub async fn handle_preflight_checks(&mut self, req: &mut Request<OrionRequestBody>) -> FilterDecision {
        let builder = Response::builder()
            .header("Access-Control-Allow-Origin", "*")
            .header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            .header("Access-Control-Allow-Headers", "Content-Type, x-mcp-protocol-version")
            .header("Access-Control-Max-Age", "86400")
            .version(self.version)
            .status(StatusCode::NO_CONTENT);

        let Ok(response) = builder.body(TimeoutBody::new(None, PolyBody::from(Empty::new()))) else {
            debug!(target: "mcp_gateway", "handle_preflight_checks: failed to build body for response");
            return FilterDecision::internal_server_error("Failed to build body for response", req.version());
        };

        debug!(target: "mcp_gateway", "handle_preflight_checks: sending {response:?} back to client");
        FilterDecision::DirectResponse(response)
    }

    pub async fn handle_sse_handshake(
        &mut self,
        state: &McpGatewayListenerContext,
        req: &mut Request<OrionRequestBody>,
        listener_name: &'static str,
    ) -> FilterDecision {
        debug!(target: "mcp_gateway", "handle_sse_handshake: starting SSE handshake...");
        // generate a unique session ID
        let session_id = SessionId(Uuid::new_v4().to_smolstr());

        // build the SSE response...
        let builder = Response::builder()
            .header("Access-Control-Allow-Origin", "*")
            .header("Content-Type", "text/event-stream")
            .header("Cache-Control", "no-cache, no-transform")
            .header("Connection", "keep-alive")
            .version(self.version)
            .status(StatusCode::OK);

        let payload = format!("event: endpoint\ndata: {MCP_MESSAGE_ENDPOINT}?{SESSION_ID_PREFIX}{}\n\n", session_id.0);
        let (body, mut sender) = SseBody::new();

        let body = TimeoutBody::new(None, PolyBody::from(body));

        let Ok(_) = sender.send(Bytes::from(payload)).await else {
            debug!(target: "mcp_gateway", "handle_sse_handshake: failed to send SSE payload");
            return FilterDecision::internal_server_error("Failed to send SSE payload", req.version());
        };

        let Ok(response) = builder.body(body) else {
            debug!(target: "mcp_gateway", "handle_sse_handshake: failed to build body for response");
            return FilterDecision::internal_server_error("Failed to build body for response", req.version());
        };

        // create a new session
        let session = Arc::new(Session {
            sender: Mutex::new(sender),
            listener_name: listener_name,
            session_id: session_id.clone(),
        });

        // add the session to the map
        state.sse_map.insert(session_id.clone(), session.clone());
        self.session_ctx = SessionContext(Some(session));

        debug!(target: "mcp_gateway", "handle_sse_handshake: sending {response:?} back to client");
        FilterDecision::DirectResponse(response)
    }

    // parse session id from request URI (query part)
    pub fn get_session_id(req: &Request<OrionRequestBody>) -> Option<SessionId> {
        req.uri().query().and_then(|query| {
            query.split('&').find_map(|part| {
                debug!(target: "mcp_gateway", "get_session_id: parsing session ID from query part: {}", part);
                part.split(SESSION_ID_PREFIX).nth(1).and_then(|value| {
                    debug!(target: "mcp_gateway", "get_session_id: session ID value: {}", value);
                    Some(SessionId(value.to_smolstr()))
                })
            })
        })
    }

    fn handle_message(&mut self, body: Bytes) -> MessageResponse {
        let Ok(message): Result<model::JsonRpcMessage, _> = serde_json::from_slice(&body) else {
            debug!("get_session_id: failed to parse JSON message");
            return MessageResponse::Error(model::JsonRpcError {
                jsonrpc: model::JsonRpcVersion2_0,
                id: model::RequestId::String(Arc::from("")),
                error: model::ErrorData::parse_error("invalid JSON", None),
            });
        };

        match message {
            model::JsonRpcMessage::Request(json_rpc_request) => {
                self.request_id = json_rpc_request.id.clone();
                self.handle_rpc_request(json_rpc_request)
            },
            model::JsonRpcMessage::Response(json_rpc_response) => {
                todo!()
            },
            model::JsonRpcMessage::Notification(json_rpc_notification) => {
                todo!()
            },
            model::JsonRpcMessage::Error(json_rpc_error) => {
                todo!()
            },
        }
    }

    fn handle_rpc_request(&mut self, rpc: model::JsonRpcRequest) -> MessageResponse {
        match rpc.request.method.as_str() {
            "initialize" => {
                debug!(target: "mcp_gateway", "Initialize request received");
                let Ok(init_params): Result<model::InitializeRequestParam, _> =
                    serde_json::from_value(serde_json::Value::Object(rpc.request.params))
                else {
                    debug!(target: "mcp_gateway", "handle_rpc_request: invalid params");
                    return MessageResponse::Error(model::JsonRpcError {
                        jsonrpc: model::JsonRpcVersion2_0,
                        id: self.request_id.clone(),
                        error: model::ErrorData::invalid_params("invalid params", None),
                    });
                };

                self.initialize_request_params = Some(init_params);

                let response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    result: json!({
                        "name": "John Doe",
                        "age": 43,
                        "phones": [
                            "+44 1234567",
                            "+44 2345678"
                        ]
                    }),
                };

                return MessageResponse::RpcResponse(response);
            },
            "upstream" => {
                debug!(target: "mcp_gateway", "Upstream request received");
                let request =
                    http::Request::builder().uri("http://127.0.0.1:8000/").header("User-Agent", "my-awesome-agent/1.0");

                let body = InstrumentedBody::new(
                    BodyKind::Request,
                    TimeoutBody::new(None, PolyBody::from(Empty::new())),
                    |_, _, _| {},
                );

                return MessageResponse::Upstream(request.body(body).unwrap());
            },
            _ => {
                return MessageResponse::Error(model::JsonRpcError {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: self.request_id.clone(),
                    error: model::ErrorData::new(model::ErrorCode::METHOD_NOT_FOUND, "Method not found", None),
                })
            },
        }
    }

    async fn send_message(&self, message: Bytes) -> Result<(), FilterDecision> {
        let mut msg = BytesMut::with_capacity(message.len() + SSE_MESSAGE_PREFIX.len());
        msg.extend_from_slice(SSE_MESSAGE_PREFIX.as_bytes());
        msg.extend_from_slice(message.as_ref());
        let mut sender = self.session_ctx.get().sender.lock().await;
        if let Err(e) = sender.send(msg.into()).await {
            let mcp_ctx = McpGatewayListenerContext::get_filter_context(self.session_ctx.get().listener_name);
            mcp_ctx.sse_map.remove(&self.session_ctx.get().session_id);
            debug!(target: "mcp_gateway", "send_message: failed to send SSE payload for session {}, error {e}", self.session_ctx.get().session_id);
            return Err(FilterDecision::internal_server_error(
                format!("Failed to send SSE payload for session {}", self.session_ctx.get().session_id).as_str(),
                self.version,
            ));
        }
        Ok(())
    }

    #[inline]
    fn build_accepted_response(&self) -> Result<Response<OrionResponseBody>, FilterDecision> {
        let builder = Response::builder()
            .header("Access-Control-Allow-Origin", "*")
            .header("Connection", "keep-alive")
            .status(StatusCode::ACCEPTED)
            .version(self.version);
        let body = TimeoutBody::new(None, PolyBody::from(Full::from("Accepted")));
        let Ok(resp) = builder.body(body) else {
            return Err(FilterDecision::internal_server_error(
                format!("Failed to build accepted response for session {}", self.session_ctx.get().session_id).as_str(),
                self.version,
            ));
        };

        Ok(resp)
    }
}

enum MessageResponse {
    Nothing,
    Error(model::JsonRpcError),
    RpcResponse(model::JsonRpcResponse<serde_json::Value>),
    Upstream(Request<OrionRequestBody>),
}

fn to_json_bytes<S: Serialize>(value: S) -> Bytes {
    let Ok(msg) = serde_json::to_string(&value) else {
        panic!("Failed to serialize error message");
    };
    let mut err_msg = BytesMut::from(msg.as_str());
    err_msg.put_slice(b"\r\n\r\n");
    err_msg.freeze()
}

fn prepare_rpc_error(request_id: model::RequestId, data: model::ErrorData) -> Bytes {
    let error = model::JsonRpcError { jsonrpc: model::JsonRpcVersion2_0, id: request_id, error: data };
    let Ok(error_msg) = serde_json::to_string(&error) else {
        panic!("Failed to serialize error message");
    };
    let mut err_msg = BytesMut::from(error_msg.as_str());
    err_msg.put_slice(b"\r\n\r\n");
    err_msg.freeze()
}
