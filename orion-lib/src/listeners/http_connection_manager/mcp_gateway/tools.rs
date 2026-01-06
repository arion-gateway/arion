use super::model::{ListToolsResult, Tool};
use crate::{OrionRequestBody, PolyBody, body::{instrumented_body::InstrumentedBody, response_flags::BodyKind, timeout_body::TimeoutBody}, listeners::http_connection_manager::mcp_gateway::model::Request, object};
use http_body_util::Empty;
use smol_str::SmolStr;
use std::sync::Arc;

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

    pub fn build_request(&self, request: Request) -> Option<http::Request<OrionRequestBody>> {
        let name = request.params.get("name").and_then(|name| name.as_str()).unwrap_or("unknown");

        let endpoint = self.registry.iter().find(|e| e.name == name)?;

        let uri = format!("http://127.0.0.1:8000{}", endpoint.path);
        let builder = http::Request::builder()
            .method(endpoint.method.clone())
            .uri(uri)
            .header("User-Agent", "my-awesome-agent/1.0");

        let body = InstrumentedBody::new(
            BodyKind::Request,
            TimeoutBody::new(None, PolyBody::from(Empty::new())),
            |_, _, _| {},
        );

        builder.body(body).ok()
    }
}
