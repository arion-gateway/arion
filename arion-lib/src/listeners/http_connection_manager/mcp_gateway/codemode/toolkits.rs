// Copyright 2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::BTreeSet, sync::Arc as StdArc};

use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpToolkit;
use papaya::HashMap as PapayaMap;
use serde::Serialize;
use smol_str::SmolStr;

use super::super::tools::{catalog_page, CallToolError, CatalogPage, ToolEntry, ToolId, ToolsRegistry};

#[derive(Debug, thiserror::Error)]
pub enum ToolkitError {
    #[error("duplicate toolkit name: {0}")]
    DuplicateName(SmolStr),
    #[error("invalid toolkit '{name}': {reason}")]
    Invalid { name: SmolStr, reason: String },
}

#[derive(Debug, Clone)]
struct ToolkitEntry {
    name: SmolStr,
    description: SmolStr,
    instructions: SmolStr,
    tools: Vec<ToolId>,
}

#[derive(Debug)]
pub struct ToolkitRegistry {
    entries: PapayaMap<SmolStr, StdArc<ToolkitEntry>, ahash::RandomState>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolkitSummary {
    pub name: SmolStr,
    pub description: SmolStr,
    pub tool_count: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RenderedToolkit {
    pub name: SmolStr,
    pub description: SmolStr,
    pub content: String,
}

impl ToolkitRegistry {
    pub fn with_config(toolkits: Vec<McpToolkit>) -> Result<Self, ToolkitError> {
        let registry = Self { entries: PapayaMap::with_hasher(ahash::RandomState::new()) };
        for toolkit in toolkits {
            let entry = StdArc::new(ToolkitEntry::try_from(toolkit)?);
            if registry.entries.pin().insert(entry.name.clone(), StdArc::clone(&entry)).is_some() {
                return Err(ToolkitError::DuplicateName(entry.name.clone()));
            }
        }
        Ok(registry)
    }

    pub fn add(&self, toolkit: McpToolkit) -> Result<(), ToolkitError> {
        let entry = StdArc::new(ToolkitEntry::try_from(toolkit)?);
        self.entries.pin().insert(entry.name.clone(), entry);
        Ok(())
    }

    pub fn remove(&self, name: &str) -> bool {
        self.entries.pin().remove(name).is_some()
    }

    pub fn visible_summaries(&self, tools: &ToolsRegistry, req_ext: &http::Extensions) -> Vec<ToolkitSummary> {
        let mut entries = {
            let pinned = self.entries.pin();
            pinned.iter().map(|(_, entry)| StdArc::clone(entry)).collect::<Vec<_>>()
        };
        entries.sort_by(|left, right| left.name.cmp(&right.name));

        let mut visible = Vec::with_capacity(entries.len());
        for entry in entries {
            if tools.permitted_entries_by_id(req_ext, &entry.tools).is_some() {
                visible.push(ToolkitSummary {
                    name: entry.name.clone(),
                    description: entry.description.clone(),
                    tool_count: entry.tools.len(),
                });
            }
        }
        visible
    }

    #[allow(clippy::result_large_err)]
    pub fn list(
        &self,
        tools: &ToolsRegistry,
        req_ext: &http::Extensions,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<CatalogPage<ToolkitSummary>, CallToolError> {
        catalog_page(self.visible_summaries(tools, req_ext), limit, offset)
    }

    pub fn get(&self, tools: &ToolsRegistry, req_ext: &http::Extensions, name: &str) -> Option<RenderedToolkit> {
        let entry = self.entries.pin().get(name).map(StdArc::clone)?;
        let resolved = tools.permitted_entries_by_id(req_ext, &entry.tools)?;
        Some(entry.render(&resolved))
    }
}

impl TryFrom<McpToolkit> for ToolkitEntry {
    type Error = ToolkitError;

    fn try_from(toolkit: McpToolkit) -> Result<Self, Self::Error> {
        let invalid = |reason: String| ToolkitError::Invalid { name: toolkit.name.clone(), reason };
        if toolkit.name.is_empty() || !valid_catalog_name(&toolkit.name) {
            return Err(invalid("name must contain only letters, digits, '_', '-', or '.'".to_owned()));
        }
        if toolkit.description.is_empty() || toolkit.instructions.is_empty() {
            return Err(invalid("description and instructions must be non-empty".to_owned()));
        }
        if toolkit.tools.len() < 2 {
            return Err(invalid("at least two tool references are required".to_owned()));
        }

        let mut tools = BTreeSet::new();
        for tool in toolkit.tools {
            let id = ToolId::new(tool.namespace, tool.name).map_err(|error| invalid(error.to_string()))?;
            if !tools.insert(id.clone()) {
                return Err(invalid(format!("duplicate tool reference '{}'", id.qualified_name())));
            }
        }

        Ok(Self {
            name: toolkit.name,
            description: toolkit.description,
            instructions: toolkit.instructions,
            tools: tools.into_iter().collect(),
        })
    }
}

impl ToolkitEntry {
    fn render(&self, entries: &[StdArc<ToolEntry>]) -> RenderedToolkit {
        let header = format!("{}\n\nTool definitions:", self.instructions.trim_end());
        let sections = self.tools.iter().zip(entries).map(|(id, entry)| {
            let input_schema = schema_text(&entry.input_schema);
            let output_schema = schema_text(&entry.output_schema);
            let qualified_name = id.qualified_name();
            format!(
                "\n\n{qualified_name}\nInvocation: tools[\"{qualified_name}\"](arguments)\nDescription: {}\nInput schema:\n{input_schema}\nOutput schema:\n{output_schema}",
                entry.description
            )
        });
        let content = std::iter::once(header).chain(sections).collect::<String>();
        RenderedToolkit { name: self.name.clone(), description: self.description.clone(), content }
    }
}

fn schema_text(schema: &impl Serialize) -> String {
    serde_json::to_string_pretty(schema).unwrap_or_else(|error| format!("<unserializable schema: {error}>"))
}

fn valid_catalog_name(value: &str) -> bool {
    value.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

#[cfg(test)]
mod tests {
    use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
        DisclosureMode, EmbeddingVector, HttpUpstreamPolicy, McpTool, McpToolSearch, McpToolkitToolRef,
        UpstreamBackend, UpstreamLimits,
    };

    use super::*;

    fn tool_ref(namespace: &str, name: &str) -> McpToolkitToolRef {
        McpToolkitToolRef { namespace: namespace.into(), name: name.into() }
    }

    fn toolkit() -> McpToolkit {
        McpToolkit {
            name: "support".into(),
            description: "Resolve a customer support request".into(),
            instructions: "Find the customer, then create a ticket.".into(),
            tools: vec![tool_ref("crm", "find"), tool_ref("tickets", "create")],
        }
    }

    fn tool(namespace: &str, name: &str) -> McpTool {
        McpTool {
            namespace: namespace.into(),
            name: name.into(),
            description: format!("{namespace} {name}").into(),
            input_schema: serde_json::from_value(serde_json::json!({
                "type": "object",
                "properties": { "id": { "type": "string" } }
            }))
            .unwrap(),
            output_schema: serde_json::Map::new(),
            backend: UpstreamBackend::Rest {
                method: http::Method::POST,
                path: format!("/{name}").into(),
                query_params: Vec::new(),
                cluster: "test".into(),
                upstream_policy: Box::new(HttpUpstreamPolicy::default()),
                body_template: None,
                body_json: None,
            },
            rbac: None,
            embedding: EmbeddingVector::default(),
            disclosure_mode: DisclosureMode::Progressive,
        }
    }

    #[test]
    fn validates_fixed_tool_references() {
        ToolkitEntry::try_from(toolkit()).unwrap();

        let mut duplicate = toolkit();
        duplicate.tools[1] = duplicate.tools[0].clone();
        ToolkitEntry::try_from(duplicate).unwrap_err();
    }

    #[tokio::test]
    async fn toolkit_visibility_tracks_live_tools_and_renders_definitions() {
        let tools = ToolsRegistry::with_config(
            vec![tool("tickets", "create")],
            Vec::new(),
            McpToolSearch::default(),
            UpstreamLimits::default(),
            None,
        )
        .unwrap();
        let toolkits = ToolkitRegistry::with_config(vec![toolkit()]).unwrap();
        let req_ext = http::Extensions::new();

        assert!(toolkits.visible_summaries(&tools, &req_ext).is_empty());

        let mut customer_tool = tool("crm", "find");
        customer_tool.description = "Find a customer by ID".into();
        tools.add_tool(customer_tool).await.unwrap();
        let summaries = toolkits.visible_summaries(&tools, &req_ext);
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].tool_count, 2);

        let rendered = toolkits.get(&tools, &req_ext, "support").unwrap();
        assert!(rendered.content.starts_with("Find the customer, then create a ticket.\n\nTool definitions:\n"));
        assert!(rendered.content.contains("tools[\"crm.find\"]"));
        assert!(rendered.content.contains("tools[\"tickets.create\"]"));
        assert!(rendered.content.contains("Find a customer by ID"));

        tools.remove_tool(&ToolId::new("crm", "find").unwrap());
        assert!(toolkits.get(&tools, &req_ext, "support").is_none());
    }
}
