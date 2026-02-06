use crate::{
    listeners::http_connection_manager::mcp_gateway::{
        rbac::{
            Action as RbacAction, JwtClaimField, JwtHeaderField, JwtHeaderMatcher, JwtPayloadMatcher,
            Permission as RbacPermission, ToolRbac,
        },
        transcoder::{RestTranscoder, Transcoder},
    },
    OrionRequestBody,
};
use http::{header::InvalidHeaderValue, HeaderValue};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    ClusterHeader, UpstreamBackend, McpRestQueryParams, McpTool,
};
use rmcp::model::{ListToolsResult, Request, Tool};
use rmcp::object;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ToolsRegistry {
    registry: Vec<ToolEntry>,
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

impl ToolsRegistry {
    pub fn new() -> Self {
        ToolsRegistry { registry: Vec::new() }
    }

    pub fn with_tools(tools: Vec<McpTool>) -> Self {
        let registry = tools
            .into_iter()
            .map(|tool| {
                let rbac = tool.rbac.as_ref().map(convert_config_rbac_to_runtime);
                ToolEntry { tool, rbac }
            })
            .collect();
        ToolsRegistry { registry }
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

    pub fn build_list_tools(&self, request: &http::Request<OrionRequestBody>) -> ListToolsResult {
        let mut tools = Vec::with_capacity(self.registry.len());
        for entry in self.registry.iter() {
            if let Some(rbac) = &entry.rbac {
                if !rbac.is_permitted(request) {
                    continue;
                }
            }
            tools.push(Tool {
                name: entry.tool.name.clone().into(),
                description: Some(entry.tool.description.clone().into()),
                input_schema: Arc::new(entry.tool.input_schema.clone()),
                title: None,
                output_schema: None,
                annotations: None,
                icons: None,
                meta: None,
            });
        }

        ListToolsResult { tools, next_cursor: None, meta: None }
    }

    pub fn build_request(
        &self,
        request: &http::Request<OrionRequestBody>,
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
            if !rbac.is_permitted(request) {
                return Err(BuildRequestError::RbacDenied(name.to_string()));
            }
        }

        let tool = &entry.tool;
        let mut async_api = false;

        let upstream_request = match &tool.backend {
            UpstreamBackend::Rest { method, path, query_params, cluster, r#async } => {
                let transcoder = RestTranscoder { method, path, query_params };
                let mut upstream_request = transcoder
                    .encode(&entry.tool.input_schema, request, mcp_request)
                    .map_err(|e| BuildRequestError::TranscoderError { tool: name.to_string(), reason: e.to_string() })?;
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
