use crate::{
    OrionRequestBody, listeners::http_connection_manager::mcp_gateway::{
        rbac::{
            Action as RbacAction, JwtClaimField, JwtHeaderField, JwtHeaderMatcher, JwtPayloadMatcher,
            Permission as RbacPermission, ToolRbac,
        },
        transcoder::{RestTranscoder, Transcoder, rest::DEFAULT_USER_AGENT},
    }
};
use dashmap::DashMap;
use http::{header::InvalidHeaderValue, HeaderValue};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    ClusterHeader, McpBackendTransportUpstream, McpRestQueryParams, McpTool, UpstreamBackend, };
use rmcp::{
    ServiceError, ServiceExt, model::{CallToolRequestParams, ClientCapabilities, ClientInfo, Implementation}, service::ClientInitializeError, transport::StreamableHttpClientTransport
};

use rmcp::model::{ListToolsResult, Request, Tool};
use rmcp::object;
use smol_str::{SmolStr, ToSmolStr};
use std::{borrow::Cow, sync::Arc, time::Instant};
use tracing::{debug, error, warn};

#[derive(Debug, Clone)]
struct CachedEntry<T> {
    expiration: Instant,
    entry: T,
}

#[derive(Debug, Clone)]
pub struct ToolsRegistry {
    registry: Vec<ToolEntry>,
    cache: DashMap<SmolStr, CachedEntry<Vec<Tool>>>,
}

#[derive(Debug, Clone)]
struct ToolEntry {
    tool: McpTool,
    rbac: Option<ToolRbac>,
}

#[derive(Debug, thiserror::Error)]
pub enum BuildRequestError {
    #[error("missing 'name' parameter in request")]
    MissingName,
    #[error("'name' parameter is not a string")]
    NameNotString,
    #[error("tool '{0}' not found in registry")]
    ToolNotFound(String),
    #[error("access to tool '{0}' denied by RBAC policy")]
    RbacDenied(String),
    #[error("MCP transcoding is not yet implemented")]
    McpNotImplemented,
    #[error("FunctionGraph transcoding is not yet implemented")]
    FunctionGraphNotImplemented,
    #[error("HeaderValue: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("Http: {0}")]
    HttpError(#[from] http::Error),
    #[error("Transcoder: tool: {tool} reason: {reason}")]
    TranscoderError { tool: String, reason: String },
}

#[derive(Debug, thiserror::Error)]
pub enum ListToolsError {
    #[error("Upstream: error {0}")]
    UpstreamError(String),
    #[error("Unsupported transport")]
    UnsupportedTransport,
    #[error("Client: {0}")]
    ClientError(#[from] ClientInitializeError),
    #[error("ServiceError: {0}")]
    ServiceError(#[from] ServiceError)
}

impl ToolsRegistry {
    pub fn new() -> Self {
        ToolsRegistry { registry: Vec::new(), cache: DashMap::new() }
    }

    pub fn with_tools(tools: Vec<McpTool>) -> Self {
        let registry = tools
            .into_iter()
            .map(|tool| {
                let rbac = tool.rbac.as_ref().map(convert_config_rbac_to_runtime);
                ToolEntry { tool, rbac }
            })
            .collect();
        ToolsRegistry { registry, cache: DashMap::new() }
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
            rbac: None,
            backend: UpstreamBackend::Rest {
                cluster: "weather_api_cluster".into(),
                r#async: false,
                method: http::Method::GET,
                path: "/weather".into(),
                query_params: vec![McpRestQueryParams { name: "city".into(), source: "country".into() }],
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
            rbac: None,
            backend: UpstreamBackend::Rest {
                cluster: "post_user_cluster".into(),
                r#async: false,
                method: http::Method::POST,
                path: "/user".into(),
                query_params: vec![McpRestQueryParams { name: "username".into(), source: "email".into() }],
            },
        });

        myself
    }

    pub fn register(&mut self, tool: McpTool) {
        let rbac = tool.rbac.as_ref().map(convert_config_rbac_to_runtime);
        self.registry.push(ToolEntry { tool, rbac });
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
                            warn!(target: "mcp_gateway", "Failed to list tools: {}", err);
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

    pub async fn get_list_tools_streamable_http(&self, url: &str, namespace: &str) -> Result<Vec<Tool>, ListToolsError> {
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
        let client = client_info.serve(transport).await.inspect_err(|e| {
            error!("client error: {:?}", e);
        })?;

        let server_info = client.peer_info();
        tracing::info!("Connected to server: {server_info:#?}");

        // List tools
        let mut tools = client.list_tools(Default::default()).await?;

        for tool in &mut tools.tools {
            let name : String = tool.name.clone().into_owned();
            tool.name = Cow::Owned(format!("{namespace}__{name}"));
        }

        Ok(tools.tools)
    }

    pub fn build_request(
        &self,
        req_ext: &http::Extensions,
        req_headers: &http::HeaderMap,
        mcp_request: &Request,
        cluster_header: &Option<ClusterHeader>,
    ) -> Result<(http::Request<OrionRequestBody>, bool), BuildRequestError> {
        let name = mcp_request.params.get("name").ok_or(BuildRequestError::MissingName)?;
        let name = name.as_str().ok_or(BuildRequestError::NameNotString)?;

        let entry = self
            .registry
            .iter()
            .find(|e| e.tool.name == name)
            .ok_or_else(|| BuildRequestError::ToolNotFound(name.to_string()))?;

        if let Some(rbac) = &entry.rbac {
            if !rbac.is_permitted(req_ext) {
                return Err(BuildRequestError::RbacDenied(name.to_string()));
            }
        }

        let tool = &entry.tool;
        let mut async_api = false;

        let upstream_request = match &tool.backend {
            UpstreamBackend::Rest { method, path, query_params, cluster, r#async } => {
                let transcoder = RestTranscoder { method, path, query_params };
                let mut upstream_request =
                    transcoder.encode(&entry.tool.input_schema, req_headers, mcp_request).map_err(|e| {
                        BuildRequestError::TranscoderError { tool: name.to_string(), reason: e.to_string() }
                    })?;
                if let Some(cluster_header) = cluster_header {
                    let headers = upstream_request.headers_mut();
                    headers.append(cluster_header.0.clone(), HeaderValue::from_str(&cluster)?);
                    async_api = *r#async;
                }
                upstream_request
            },
            UpstreamBackend::McpServer { .. } => {
                return Err(BuildRequestError::McpNotImplemented);
            },
            UpstreamBackend::FunctionGraph {} => {
                return Err(BuildRequestError::FunctionGraphNotImplemented);
            },
        };

        Ok((upstream_request, async_api))
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
