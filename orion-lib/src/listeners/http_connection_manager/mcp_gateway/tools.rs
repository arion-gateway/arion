use crate::{
    body::{instrumented_body::InstrumentedBody, response_flags::BodyKind, timeout_body::TimeoutBody},
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
    registry: Vec<McpTool>,
}

impl ToolsRegistry {
    pub fn new() -> Self {
        ToolsRegistry { registry: Vec::new() }
    }

    pub fn with_tools(tools: Vec<McpTool>) -> Self {
        let mut myself = ToolsRegistry::new();
        for tool in tools {
            myself.register(tool);
        }
        myself
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

    pub fn register(&mut self, endpoint: McpTool) {
        self.registry.push(endpoint);
    }

    pub fn build_list_tools(&self) -> ListToolsResult {
        let mut tools = Vec::with_capacity(self.registry.len());
        for api in self.registry.iter() {
            //let tool_name = format!(
            //    "{}_{}",
            //    api.method.to_string().to_lowercase(),
            //    api.path.replace("/", "_").trim_start_matches('_')
            //);
            tools.push(Tool {
                name: api.name.clone().into(),
                description: Some(api.description.clone().into()),
                input_schema: Arc::new(api.input_schema.clone()),
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

        let endpoint = self
            .registry
            .iter()
            .find(|e| e.name == name)
            .ok_or_else(|| BuildRequestError::ToolNotFound(name.to_string()))?;

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
