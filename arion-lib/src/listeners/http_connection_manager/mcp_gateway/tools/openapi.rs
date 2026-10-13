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

//! OpenAPI tool sources: registration, replacement, and removal of config-time expanded tools.

use std::collections::HashSet;

use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpOpenApiToolSource;

use super::{ToolBuilderError, ToolId, ToolSource, ToolsRegistry, entry::build_tool_entry};

impl ToolsRegistry {
    pub fn with_open_api_sources(self, sources: Vec<McpOpenApiToolSource>) -> Result<Self, ToolBuilderError> {
        for source in sources {
            if self.open_api_sources.pin().contains(&source.namespace) {
                return Err(ToolBuilderError::DuplicateOpenApiSource(source.namespace));
            }
            self.add_open_api_source(source)?;
        }
        Ok(self)
    }

    pub fn add_open_api_source(&self, source: McpOpenApiToolSource) -> Result<(), ToolBuilderError> {
        Self::validate_namespace(&source.namespace)?;
        let namespace = source.namespace.clone();
        if self.namespace_has_direct_tools(&namespace) || self.dynamic_mcp_servers.pin().contains_key(&namespace) {
            return Err(ToolBuilderError::SharedNamespace(namespace));
        }

        // Build every entry first so a bad tool leaves the registry unchanged.
        let mut ids = HashSet::with_capacity(source.tools.len());
        let mut entries = Vec::with_capacity(source.tools.len());
        for tool in source.tools {
            let id = ToolId::from_tool(&tool)?;
            if id.namespace != namespace {
                return Err(ToolBuilderError::InvalidNamespace(id.namespace));
            }
            self.validate_supplied_embedding(&tool)?;
            if !ids.insert(id.clone()) {
                return Err(ToolBuilderError::DuplicateTool(id.qualified_name().into()));
            }
            entries.push(build_tool_entry(tool, ToolSource::OpenApi, &self.upstream_limits)?);
        }

        self.open_api_sources.pin().insert(namespace.clone());
        self.evict_open_api_tools_for(&namespace);
        for entry in entries {
            let id = entry.id.clone();
            self.insert_tool(id, entry);
        }
        Ok(())
    }

    pub fn remove_open_api_source(&self, namespace: &str) -> bool {
        if !self.open_api_sources.pin().remove(namespace) {
            return false;
        }
        self.evict_open_api_tools_for(namespace);
        true
    }

    fn evict_open_api_tools_for(&self, namespace: &str) {
        self.tools.pin().retain(|id, entry| id.namespace != namespace || !matches!(entry.source, ToolSource::OpenApi));
    }
}
