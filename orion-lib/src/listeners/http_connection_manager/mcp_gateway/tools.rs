use super::model::{ListToolsResult, Tool};
use crate::object;
use smol_str::SmolStr;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ApiEndpoint {
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
            method: http::Method::POST,
            path: "/weather".into(),
            params: vec!["city".into(), "country".into()],
            description: "Add a new weather entry".into(),
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
            let tool_name = format!(
                "{}_{}",
                api.method.to_string().to_lowercase(),
                api.path.replace("/", "_").trim_start_matches('_')
            );
            tools.push(Tool {
                name: tool_name.into(),
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
}
