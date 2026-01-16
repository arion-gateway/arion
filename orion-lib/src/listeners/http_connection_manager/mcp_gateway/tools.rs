use crate::{
    body::{instrumented_body::InstrumentedBody, response_flags::BodyKind, timeout_body::TimeoutBody},
    listeners::http_connection_manager::mcp_gateway::rbac::{
        Action as RbacAction, JwtClaimField, JwtHeaderField, JwtHeaderMatcher, JwtPayloadMatcher,
        Permission as RbacPermission, ToolRbac,
    },
    OrionRequestBody, PolyBody,
};
use http::{header::InvalidHeaderValue, HeaderValue};
use http_body_util::Empty;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    ClusterHeader, McpBackend, McpRestQueryParams, McpTool, McpTranscoding,
};
use rmcp::model::{ListToolsResult, Request, Tool};
use rmcp::object;
use std::{borrow::Cow, sync::Arc};
use url::form_urlencoded;

const DEFAULT_USER_AGENT: &str = concat!("orion/", env!("CARGO_PKG_VERSION"));

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
    #[error("REST request for tool '{tool}': {reason}")]
    RestBuildFailed { tool: String, reason: String },
    #[error("MCP transcoding is not yet implemented")]
    McpNotImplemented,
    #[error("FunctionGraph transcoding is not yet implemented")]
    FunctionGraphNotImplemented,
    #[error("HeaderValue: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
}

#[derive(Debug, Clone)]
pub struct ToolsRegistry {
    registry: Vec<ToolEntry>,
}

#[derive(Debug, Clone)]
struct ToolEntry {
    tool: McpTool,
    rbac: Option<ToolRbac>,
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
            backend: McpBackend {
                cluster: "weather_api_cluster".into(),
                r#async: false,
                transcoding: McpTranscoding::Rest {
                    method: http::Method::GET,
                    path: "/weather".into(),
                    query_params: vec![McpRestQueryParams { name: "city".into(), source: "country".into() }],
                },
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
            backend: McpBackend {
                cluster: "post_user_cluster".into(),
                r#async: false,
                transcoding: McpTranscoding::Rest {
                    method: http::Method::POST,
                    path: "/user".into(),
                    query_params: vec![McpRestQueryParams { name: "username".into(), source: "email".into() }],
                },
            },
        });

        myself
    }

    pub fn register(&mut self, tool: McpTool) {
        let rbac = tool.rbac.as_ref().map(convert_config_rbac_to_runtime);
        self.registry.push(ToolEntry { tool, rbac });
    }

    pub fn build_list_tools(&self) -> ListToolsResult {
        let mut tools = Vec::with_capacity(self.registry.len());
        for entry in self.registry.iter() {
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

    /// Returns an iterator over arguments as (key, value) pairs without allocating a Vec.
    /// Values are borrowed when possible (strings), owned only when conversion is needed.
    #[inline]
    fn extract_arguments(mcp_request: &Request) -> impl Iterator<Item = (&str, Cow<'_, str>)> {
        mcp_request.params.get("arguments").and_then(|v| v.as_object()).into_iter().flatten().map(|(k, v)| {
            let value = match v {
                serde_json::Value::String(s) => Cow::Borrowed(s.as_str()),
                serde_json::Value::Null => Cow::Borrowed("null"),
                serde_json::Value::Bool(true) => Cow::Borrowed("true"),
                serde_json::Value::Bool(false) => Cow::Borrowed("false"),
                other => Cow::Owned(other.to_string()),
            };
            (k.as_str(), value)
        })
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

        let endpoint = &entry.tool;

        let mut upstream_request = match &endpoint.backend.transcoding {
            McpTranscoding::Rest { method, path, query_params } => self
                .build_rest_request(request, mcp_request, &method, &path, &query_params)
                .map_err(|e| BuildRequestError::RestBuildFailed { tool: name.to_string(), reason: e.to_string() })?,
            McpTranscoding::FunctionGraph {} => {
                return Err(BuildRequestError::FunctionGraphNotImplemented);
            },
            McpTranscoding::Mcp {} => {
                return Err(BuildRequestError::McpNotImplemented);
            },
        };

        if let Some(cluster_header) = cluster_header {
            let headers = upstream_request.headers_mut();
            headers.append(cluster_header.0.clone(), HeaderValue::from_str(&endpoint.backend.cluster)?);
        }

        Ok((upstream_request, endpoint.backend.r#async))
    }

    fn build_rest_request(
        &self,
        request: &http::Request<OrionRequestBody>,
        mcp_request: &Request,
        method: &http::Method,
        path: &str,
        _query_params: &Vec<McpRestQueryParams>,
    ) -> Result<http::Request<OrionRequestBody>, http::Error> {
        // Build a path-only URI (no authority/scheme) - Orion will route to the correct
        // upstream cluster based on configuration. Including authority without scheme
        // causes "invalid format" error in http::Uri parser.
        let arguments = mcp_request.params.get("arguments").and_then(|v| v.as_object());
        let has_args = arguments.is_some_and(|m| !m.is_empty());

        // Estimate capacity: path + '?' + ~24 chars per argument (key=value&)
        let capacity = path.len() + if has_args { 1 + arguments.map_or(0, |m| m.len() * 24) } else { 0 };

        // Ensure path starts with '/' for a valid path-only URI
        let mut uri = String::with_capacity(capacity);
        if !path.starts_with('/') {
            uri.push('/');
        }
        uri.push_str(path);

        // Build query string directly using lazy iterator
        if has_args {
            uri.push('?');
            uri = form_urlencoded::Serializer::new(uri).extend_pairs(Self::extract_arguments(mcp_request)).finish();
        }

        // Orion will override the authority with the correct upstream endpoint.
        // This is required for the match_virtual_host to work properly.

        let headers = request.headers();
        let user_agent =
            headers.get(http::header::USER_AGENT).and_then(|ua| ua.to_str().ok()).unwrap_or(DEFAULT_USER_AGENT);

        let mut builder =
            http::Request::builder().method(method.clone()).uri(uri).header(http::header::USER_AGENT, user_agent);

        if let Some(host) = headers.get(http::header::HOST).and_then(|h| h.to_str().ok()) {
            builder = builder.header(http::header::HOST, host);
        }

        let body = InstrumentedBody::new(
            BodyKind::Request,
            TimeoutBody::new(None, PolyBody::from(Empty::new())),
            |_, _, _| {},
        );

        builder.body(body)
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
