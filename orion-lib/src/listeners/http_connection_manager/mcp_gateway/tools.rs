use crate::listeners::http_connection_manager::mcp_gateway::{
    mcp::{MessageResult, Session},
    rbac::{
        Action as RbacAction, JwtClaimField, JwtHeaderField, JwtHeaderMatcher, JwtPayloadMatcher,
        Permission as RbacPermission, ToolRbac,
    },
    transcoder::{rest::DEFAULT_USER_AGENT, RestTranscoder, Transcoder},
};
use dashmap::DashMap;
use http::{header::InvalidHeaderValue, HeaderValue};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    ClusterHeader, McpBackendTransportUpstream, McpRestQueryParams, McpTool, UpstreamBackend,
};
use rmcp::{
    model::{CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation, InitializeRequestParams},
    service::ClientInitializeError,
    transport::StreamableHttpClientTransport,
    ServiceError, ServiceExt,
};

use rmcp::model;
use rmcp::model::{ListToolsResult, Tool};
use rmcp::object;
use rmcp::service::{RoleClient, RunningService};
use smol_str::{SmolStr, ToSmolStr};
use std::{borrow::Cow, sync::Arc, time::Instant};
use tracing::{debug, info};

#[derive(Debug, Clone)]
struct CachedEntry<T> {
    expiration: Instant,
    entry: T,
}

#[derive(Debug, Clone)]
pub struct ToolsRegistry {
    registry: Vec<ToolEntry>,
    cache: DashMap<SmolStr, CachedEntry<Vec<Tool>>, ahash::RandomState>,
}

#[derive(Debug, Clone)]
struct ToolEntry {
    tool: McpTool,
    rbac: Option<ToolRbac>,
}

#[derive(Debug, thiserror::Error)]
pub enum CallToolError {
    #[error("'name' parameter is missing or not a string")]
    NameNotString,
    #[error("tool '{0}' not found in registry")]
    ToolNotFound(String),
    #[error("access to tool '{0}' denied by RBAC policy")]
    RbacDenied(String),
    #[error("FunctionGraph transcoding is not yet implemented")]
    FunctionGraphNotImplemented,
    #[error("HeaderValue: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("Transcoder: tool: {tool} reason: {reason}")]
    TranscoderError { tool: String, reason: String },
    #[error("Client initialization error: {0}")]
    ClientInitializeError(#[from] ClientInitializeError),
    #[error("ServiceError: {0}")]
    ServiceError(#[from] ServiceError),
    #[error("SerdeError: {0}")]
    SerdeError(#[from] serde_json::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum ListToolsError {
    #[error("Unsupported transport")]
    UnsupportedTransport,
    #[error("Client: {0}")]
    ClientError(#[from] ClientInitializeError),
    #[error("ServiceError: {0}")]
    ServiceError(#[from] ServiceError),
}

impl ToolsRegistry {
    pub fn new() -> Self {
        ToolsRegistry { registry: Vec::new(), cache: DashMap::with_hasher(ahash::RandomState::new()) }
    }

    pub fn with_tools(tools: Vec<McpTool>) -> Self {
        let registry = tools
            .into_iter()
            .map(|tool| {
                let rbac = tool.rbac.as_ref().map(convert_config_rbac_to_runtime);
                ToolEntry { tool, rbac }
            })
            .collect();
        ToolsRegistry { registry, cache: DashMap::with_hasher(ahash::RandomState::new()) }
    }

    #[allow(dead_code)]
    pub fn with_dummy_tools() -> Self {
        let mut myself = ToolsRegistry::new();

        myself.register(McpTool {
            name: "get_name".into(),
            description: "Get weather information for a city".into(),
            input_schema: object!({
                "type": "object",
                "properties": {
                    "city": { "type": "string", "description": "City Name" }
                },
                "required": ["city"]
            }),
            output_schema: serde_json::Map::new(),
            rbac: None,
            backend: UpstreamBackend::Rest {
                cluster: "weather_api_cluster".into(),
                r#async: false,
                method: http::Method::GET,
                path: "/weather".into(),
                query_params: vec![McpRestQueryParams { name: "city".into(), source: "country".into() }],
                body_template: None,
            },
        });

        myself.register(McpTool {
            name: "post_user".into(),
            description: "Add a new username and email".into(),
            input_schema: object!({
                "type": "object",
                "properties": {
                    "username": { "type": "string" },
                    "email": { "type": "string" }
                },
                "required": ["username", "email"]
            }),
            output_schema: serde_json::Map::new(),
            rbac: None,
            backend: UpstreamBackend::Rest {
                cluster: "post_user_cluster".into(),
                r#async: false,
                method: http::Method::POST,
                path: "/user".into(),
                query_params: vec![McpRestQueryParams { name: "username".into(), source: "email".into() }],
                body_template: None,
            },
        });

        myself
    }

    pub fn register(&mut self, tool: McpTool) {
        let rbac = tool.rbac.as_ref().map(convert_config_rbac_to_runtime);
        self.registry.push(ToolEntry { tool, rbac });
    }

    /// Get a tool entry by name
    pub fn get_tool(&self, name: &str) -> Option<&McpTool> {
        self.registry.iter().find(|e| e.tool.name == name).map(|e| &e.tool)
    }

    pub async fn build_list_tools(&self, req_ext: http::Extensions) -> ListToolsResult {
        let mut tools = Vec::with_capacity(self.registry.len());
        for entry in self.registry.iter() {
            if let Some(rbac) = &entry.rbac {
                if !rbac.is_permitted(&req_ext) {
                    continue;
                }
            }

            match &entry.tool.backend {
                UpstreamBackend::Rest { .. } => {
                    tools.push(Tool {
                        name: Cow::Owned(entry.tool.name.to_string()),
                        description: Some(entry.tool.description.clone().into()),
                        input_schema: Arc::new(entry.tool.input_schema.clone()),
                        title: None,
                        output_schema: None,
                        annotations: None,
                        icons: None,
                        meta: None,
                    });
                },
                UpstreamBackend::McpServer { transport, url, cache_duration } => {
                    if let Some(r) = self.cache.get(&entry.tool.name) {
                        if std::time::Instant::now() < r.expiration {
                            tools.extend(r.entry.iter().cloned());
                            continue;
                        }
                    }

                    match self.get_list_tools_from_upstream(&transport, &url, &entry.tool.name).await {
                        Ok(up_tools) => {
                            if let Some(cache_duration) = cache_duration {
                                if let Some(expiration) = std::time::Instant::now().checked_add(cache_duration.clone())
                                {
                                    self.cache.insert(
                                        entry.tool.name.to_smolstr(),
                                        CachedEntry { entry: up_tools.clone(), expiration },
                                    );
                                }
                            }

                            tools.extend(up_tools.iter().cloned());
                        },
                        Err(err) => {
                            info!(target: "mcp_gateway", "Failed to list tools: {}!", err);
                        },
                    }
                },
                UpstreamBackend::FunctionGraph {} => todo!(),
            }
        }

        ListToolsResult { tools, next_cursor: None, meta: None }
    }

    pub async fn get_list_tools_from_upstream(
        &self,
        transport: &McpBackendTransportUpstream,
        url: &str,
        namespace: &str,
    ) -> Result<Vec<Tool>, ListToolsError> {
        debug!(target: "mcp_gateway", "Getting list of tools from upstream with transport {transport:?}");
        match transport {
            McpBackendTransportUpstream::StreamableHttp => self.get_list_tools_streamable_http(url, namespace).await,
            McpBackendTransportUpstream::Sse => Err(ListToolsError::UnsupportedTransport),
        }
    }

    pub async fn get_list_tools_streamable_http(
        &self,
        url: &str,
        namespace: &str,
    ) -> Result<Vec<Tool>, ListToolsError> {
        let client = Self::get_mcp_client(url).await?;

        // List tools
        let mut tools = client.list_tools(Default::default()).await?;

        for tool in &mut tools.tools {
            let name: String = tool.name.clone().into_owned();
            tool.name = Cow::Owned(format!("{namespace}__{name}"));
        }

        Ok(tools.tools)
    }

    async fn get_mcp_client(
        url: &str,
    ) -> Result<RunningService<RoleClient, InitializeRequestParams>, ClientInitializeError> {
        debug!(target: "mcp_gateway", "Creating MCP client for URL: {url}...");
        let transport = StreamableHttpClientTransport::from_uri(url);
        let client_info = ClientInfo {
            meta: None,
            protocol_version: Default::default(),
            capabilities: ClientCapabilities::default(),
            client_info: Implementation {
                name: DEFAULT_USER_AGENT.into(),
                title: None,
                version: "0.0.1".to_string(),
                website_url: None,
                icons: None,
            },
        };
        client_info.serve(transport).await.inspect_err(|e| {
            info!(target: "mcp_gateway", "get_mcp_client error: {}!", e);
        })
    }

    pub async fn call(
        &self,
        req_ext: &http::Extensions,
        req_headers: &http::HeaderMap,
        rpc: &model::JsonRpcRequest,
        cluster_header: &Option<ClusterHeader>,
        session: &Session,
    ) -> Result<MessageResult, CallToolError> {
        // get tool name, and in case of upstream MCP, sub-tool name as well...
        let (backend_name, tool_name) = {
            let name = rpc.request.params.get("name").ok_or(CallToolError::NameNotString)?;
            let name = name.as_str().ok_or(CallToolError::NameNotString)?;
            match name.split_once("__") {
                Some((tool, sub_name)) => (tool, sub_name),
                None => (name, name),
            }
        };

        debug!(target: "mcp_gateway", "call: method:{} tool {tool_name}@{backend_name}", rpc.request.method);

        let entry = self
            .registry
            .iter()
            .find(|e| e.tool.name == backend_name)
            .ok_or_else(|| CallToolError::ToolNotFound(backend_name.to_string()))?;

        if let Some(rbac) = &entry.rbac {
            if !rbac.is_permitted(req_ext) {
                return Err(CallToolError::RbacDenied(backend_name.to_string()));
            }
        }

        let tool = &entry.tool;
        match &tool.backend {
            UpstreamBackend::Rest { method, path, query_params, cluster, r#async, body_template } => {
                let transcoder = RestTranscoder { method, path, query_params, body_template: body_template.as_ref() };
                let mut upstream_request =
                    transcoder.encode(&entry.tool.input_schema, req_headers, &rpc.request).map_err(|e| {
                        CallToolError::TranscoderError { tool: backend_name.to_owned(), reason: e.to_string() }
                    })?;
                if let Some(cluster_header) = cluster_header {
                    let headers = upstream_request.headers_mut();
                    headers.append(cluster_header.0.clone(), HeaderValue::from_str(&cluster)?);
                }
                Ok(MessageResult::UpstreamRequest((upstream_request, *r#async)))
            },
            UpstreamBackend::McpServer { url, .. } => {
                let client = match session.mcp_upstreams.entry(url.to_owned()) {
                    dashmap::Entry::Occupied(entry) => entry.into_ref(),
                    dashmap::Entry::Vacant(vacant_entry) => {
                        let new_client = Self::get_mcp_client(url).await?;
                        vacant_entry.insert(new_client)
                    },
                };

                let arguments = match &rpc.request.params.get("arguments") {
                    Some(&serde_json::Value::Object(ref o)) => Some(o.clone()),
                    _ => None,
                };

                let tool_result = match client
                    .call_tool(CallToolRequestParams {
                        meta: None,
                        name: tool_name.to_owned().into(),
                        arguments,
                        task: None,
                    })
                    .await
                {
                    Ok(res) => res,
                    Err(err) => {
                        match &err {
                            ServiceError::TransportSend(_) | ServiceError::TransportClosed => {
                                // delete the client, will be re-created on next call:
                                // first, drop the reference to client, to avoid deadlock, then remove from session map
                                info!(target: "mcp_gateway", "removing MCP client for URL {url} due to transport error!");
                                drop(client);
                                session.mcp_upstreams.remove(url);
                            },
                            _ => (),
                        };
                        return Err(err.into());
                    },
                };

                debug!(target: "mcp_gateway", "Received result from tool{backend_name}@{tool_name}: {:?}", tool_result);

                let json_result = serde_json::to_value(tool_result)?;

                let json_rcp_response = model::JsonRpcResponse {
                    jsonrpc: model::JsonRpcVersion2_0,
                    id: rpc.id.clone(),
                    result: json_result,
                };

                Ok(MessageResult::JsonRcpResponse(json_rcp_response))
            },
            UpstreamBackend::FunctionGraph {} => {
                return Err(CallToolError::FunctionGraphNotImplemented);
            },
        }
    }
}

/// Convert configuration RBAC to runtime RBAC
fn convert_config_rbac_to_runtime(
    config_rbac: &orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpToolRbac,
) -> ToolRbac {
    use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpRbacPermission;

    let action = match config_rbac.action {
        orion_configuration::config::network_filters::network_rbac::Action::Allow => RbacAction::Allow,
        orion_configuration::config::network_filters::network_rbac::Action::Deny => RbacAction::Deny,
    };

    let permissions = config_rbac
        .permissions
        .iter()
        .map(|p| match p {
            McpRbacPermission::JwtHeader { field, value } => {
                let header_field = match field.as_str() {
                    "alg" | "algorithm" => JwtHeaderField::Algorithm,
                    "typ" | "type" => JwtHeaderField::Type,
                    "cty" | "content_type" => JwtHeaderField::ContentType,
                    "jku" | "json_key_url" => JwtHeaderField::JsonKeyURL,
                    "jwk" | "json_web_key" => JwtHeaderField::JsonWebKey,
                    "kid" | "key_id" => JwtHeaderField::KeyID,
                    "x5u" | "x509_url" => JwtHeaderField::X509URL,
                    "x5c" | "x509_certificate_chain" => JwtHeaderField::X509CertificateChain,
                    "x5t" | "x509_certificate_sha1_thumbprint" => JwtHeaderField::X509CertificateSHA1Thumbprint,
                    "x5t#S256" | "x509_certificate_sha256_thumbprint" => {
                        JwtHeaderField::X509CertificateSHA256Thumbprint
                    },
                    "crit" | "critical" => JwtHeaderField::Critical,
                    "enc" | "encryption" => JwtHeaderField::Encryption,
                    "zip" => JwtHeaderField::Zip,
                    "url" => JwtHeaderField::Url,
                    "nonce" => JwtHeaderField::Nonce,
                    _ => JwtHeaderField::KeyID, // default fallback
                };
                RbacPermission::JwtHeader(JwtHeaderMatcher { field: header_field, value: value.clone() })
            },
            McpRbacPermission::JwtClaim { field, value } => {
                let claim_field = match field.as_str() {
                    "iss" | "issuer" => JwtClaimField::Issuer,
                    "sub" | "subject" => JwtClaimField::Subject,
                    "aud" | "audience" => JwtClaimField::Audience,
                    "exp" | "expiration" => JwtClaimField::Expiration,
                    "iat" | "issued_at" => JwtClaimField::IssuedAt,
                    "nbf" | "not_before" => JwtClaimField::NotBefore,
                    "jti" | "jwt_id" => JwtClaimField::JWTID,
                    custom => JwtClaimField::Extra(custom.into()),
                };
                RbacPermission::JwtClaim(JwtPayloadMatcher { field: claim_field, value: value.clone() })
            },
        })
        .collect();

    ToolRbac { action, permissions }
}
