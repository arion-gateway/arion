use super::model::{ListToolsResult, Tool};
use crate::{
    body::{instrumented_body::InstrumentedBody, response_flags::BodyKind, timeout_body::TimeoutBody},
    listeners::http_connection_manager::mcp_gateway::model::Request,
    object, OrionRequestBody, PolyBody,
};
use http_body_util::Empty;
use smol_str::SmolStr;
use std::{borrow::Cow, sync::Arc};
use url::form_urlencoded;

const DEFAULT_USER_AGENT: &str = concat!("orion/", env!("CARGO_PKG_VERSION"));

#[derive(Debug, Clone)]
pub struct ApiEndpoint {
    name: String,
    method: http::Method,
    path: String,
    params: Vec<SmolStr>,
    description: String,
    schema: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone)]
pub struct ToolsRegistry {
    registry: Vec<ApiEndpoint>,
}

impl ToolsRegistry {
    pub fn new() -> Self {
        ToolsRegistry { registry: Vec::new() }
    }

    pub fn with_dummy_tools() -> Self {
        let mut myself = ToolsRegistry::new();

        myself.register(ApiEndpoint {
            name: "get_name".into(),
            method: http::Method::GET,
            path: "/weather".into(),
            params: vec!["city".into(), "country".into()],
            description: "Get weather information for a city".into(),
            schema: object!({
                "type": "object",
                "properties": {
                    "city": { "type": "string", "description": "City Name" }
                },
                "required": ["city"]
            }),
        });

        myself.register(ApiEndpoint {
            name: "post_user".into(),
            method: http::Method::POST,
            path: "/user".into(),
            params: vec!["username".into(), "email".into()],
            description: "Add a new username and email".into(),
            schema: object!({
                "type": "object",
                "properties": {
                    "username": { "type": "string" },
                    "email": { "type": "string" }
                },
                "required": ["username", "email"]
            }),
        });

        myself
    }

    pub fn register(&mut self, endpoint: ApiEndpoint) {
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
                input_schema: Arc::new(api.schema.clone()),
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
    fn extract_arguments(
        request: &Request,
    ) -> impl Iterator<Item = (&str, Cow<'_, str>)> {
        request
            .params
            .get("arguments")
            .and_then(|v| v.as_object())
            .into_iter()
            .flatten()
            .map(|(k, v)| {
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
        orig_request: &http::Request<OrionRequestBody>,
        request: &Request,
    ) -> Option<http::Request<OrionRequestBody>> {
        let name = request.params.get("name")?.as_str()?;
        let endpoint = self.registry.iter().find(|e| e.name == name)?;

        // Pre-calculate capacity to minimize re-allocations
        let authority = orig_request.uri().authority();

        let arguments = request.params.get("arguments").and_then(|v| v.as_object());
        let has_args = arguments.is_some_and(|m| !m.is_empty());

        // Estimate: base + '?' + ~24 chars per argument (key=value&)
        let capacity = {
            let base_len = authority.map_or(0, |a| a.as_str().len() + 1) + endpoint.path.len();
            base_len + if has_args { 1 + arguments.map_or(0, |m| m.len() * 24) } else { 0 }
        };

        let mut uri = String::with_capacity(capacity);

        if let Some(auth) = authority {
            uri.push_str(auth.as_str());
            uri.push('/');
        }
        uri.push_str(&endpoint.path);

        // Build query string directly using lazy iterator
        if has_args {
            uri.push('?');
            uri = form_urlencoded::Serializer::new(uri)
                .extend_pairs(Self::extract_arguments(request))
                .finish();
        }

        // Orion will override the authority with the correct upstream endpoint.
        // This is required for the match_virtual_host to work properly.

        let headers = orig_request.headers();
        let user_agent = headers
            .get(http::header::USER_AGENT)
            .and_then(|ua| ua.to_str().ok())
            .unwrap_or(DEFAULT_USER_AGENT);

        let mut builder = http::Request::builder()
            .method(endpoint.method.clone())
            .uri(uri)
            .header(http::header::USER_AGENT, user_agent);

        if let Some(host) = headers.get(http::header::HOST).and_then(|h| h.to_str().ok()) {
            builder = builder.header(http::header::HOST, host);
        }

        let body = InstrumentedBody::new(
            BodyKind::Request,
            TimeoutBody::new(None, PolyBody::from(Empty::new())),
            |_, _, _| {},
        );

        builder.body(body).ok()
    }
}
