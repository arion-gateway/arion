// Copyright 2025-2026 The arion-gateway Authors
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

//! The MCP tool registry: tool identity, registry core, and RBAC-aware
//! discovery. Submodules cover entries and projections (`entry`), errors
//! (`errors`), dynamic upstream servers (`dynamic`), search and ranking
//! (`search`), and invocation (`invoke`).
//!
//! Discovery methods never refresh the registry themselves: request entry
//! points call [`ToolsRegistry::ensure_tools_current`] exactly once, then use
//! the synchronous cores below.

mod dynamic;
mod entry;
mod errors;
mod invoke;
mod rest;
mod search;
#[cfg(test)]
mod tests;

pub use dynamic::DynamicMcpServerEntry;
pub(crate) use entry::catalog_page;
pub use entry::{CatalogPage, CompactToolDefinition, NamespaceSummary, ToolDefinition, ToolEntry, ToolSource};
pub use errors::{CallToolError, ToolBuilderError};
pub(crate) use invoke::call_tool_result_into_value;
#[cfg(feature = "codemode")]
pub(crate) use invoke::serialized_json_size;
#[cfg(test)]
pub(crate) use invoke::TestCallContext;
pub use invoke::ToolCallContext;

use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc as StdArc,
    },
};

use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    DisclosureMode, DynamicMcpServer, McpTool, McpToolSearch, UpstreamLimits,
};
use papaya::HashMap as PapayaMap;
use rmcp::model::Tool;
use serde::{Deserialize, Serialize, Serializer};
use smol_str::SmolStr;
use tokio::sync::{Mutex, OnceCell};

use entry::build_tool_entry;

use crate::listeners::http_connection_manager::{mcp_gateway::embeddings, RequestCtx};

const MAX_MCP_TOOL_NAME_BYTES: usize = 128;
pub const TOOL_SEARCH_TOOL_NAME: &str = "tool_search";
#[cfg(any(test, feature = "codemode"))]
pub const CODEMODE_EXECUTE_TOOL_NAME: &str = "execute";

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolSearchRequest {
    pub query: String,
    pub limit: Option<usize>,
    pub namespace: Option<SmolStr>,
}

/// Canonical registry identity. Public MCP boundaries use `qualified_name()`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ToolId {
    pub namespace: SmolStr,
    pub name: SmolStr,
}

impl ToolId {
    pub fn new(namespace: impl Into<SmolStr>, name: impl Into<SmolStr>) -> Result<Self, ToolBuilderError> {
        let id = Self { namespace: namespace.into(), name: name.into() };
        id.validate()?;
        Ok(id)
    }

    pub fn from_tool(tool: &McpTool) -> Result<Self, ToolBuilderError> {
        Self::new(tool.namespace.clone(), tool.name.clone())
    }

    pub fn parse_qualified(name: &str) -> Option<Self> {
        let (namespace, local_name) = name.split_once('.')?;
        Self::new(namespace, local_name).ok()
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.namespace, self.name)
    }

    fn validate(&self) -> Result<(), ToolBuilderError> {
        if self.namespace.is_empty() || self.namespace.contains('.') || self.namespace.contains('/') {
            return Err(ToolBuilderError::InvalidNamespace(self.namespace.clone()));
        }
        if self.name.is_empty() {
            return Err(ToolBuilderError::InvalidToolName(self.name.clone()));
        }
        if !valid_mcp_name_segment(&self.namespace) || !valid_mcp_name_segment(&self.name) {
            return Err(ToolBuilderError::InvalidQualifiedToolName(self.qualified_name().into()));
        }
        let qualified = self.qualified_name();
        if qualified.len() > MAX_MCP_TOOL_NAME_BYTES {
            return Err(ToolBuilderError::InvalidQualifiedToolName(qualified.into()));
        }
        Ok(())
    }
}

impl Serialize for ToolId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.qualified_name())
    }
}

fn valid_mcp_name_segment(value: &str) -> bool {
    value.bytes().all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolSearchScope {
    Progressive,
    All,
}

#[derive(Debug)]
pub struct ToolsRegistry {
    tools: PapayaMap<ToolId, StdArc<ToolEntry>, ahash::RandomState>,
    dynamic_mcp_servers: PapayaMap<SmolStr, StdArc<DynamicMcpServerEntry>, ahash::RandomState>,
    search: McpToolSearch,
    upstream_limits: UpstreamLimits,
    bootstrapped: OnceCell<()>,
    embedding_refresh: Mutex<()>,
    embeddings_pending: AtomicBool,
    embeddings_client: Option<StdArc<embeddings::EmbeddingsClient>>,
}

impl ToolsRegistry {
    pub fn with_config(
        tools: Vec<McpTool>,
        dynamic_mcp_servers: Vec<DynamicMcpServer>,
        search: McpToolSearch,
        upstream_limits: UpstreamLimits,
        embeddings_client: Option<StdArc<embeddings::EmbeddingsClient>>,
    ) -> Result<Self, ToolBuilderError> {
        let registry = Self {
            tools: PapayaMap::with_hasher(ahash::RandomState::new()),
            dynamic_mcp_servers: PapayaMap::with_hasher(ahash::RandomState::new()),
            search,
            upstream_limits,
            bootstrapped: OnceCell::new(),
            embedding_refresh: Mutex::new(()),
            embeddings_pending: AtomicBool::new(false),
            embeddings_client,
        };

        for tool_conf in tools {
            let id = ToolId::from_tool(&tool_conf)?;
            registry.validate_supplied_embedding(&tool_conf)?;

            let entry = build_tool_entry(tool_conf, ToolSource::Direct, &registry.upstream_limits)?;
            if registry.insert_tool(id.clone(), entry).is_some() {
                return Err(ToolBuilderError::DuplicateTool(id.qualified_name().into()));
            }
        }

        for server in dynamic_mcp_servers {
            Self::validate_namespace(&server.namespace)?;
            let namespace = server.namespace.clone();
            if registry.namespace_has_direct_tools(&namespace) {
                return Err(ToolBuilderError::SharedNamespace(namespace));
            }
            let entry = StdArc::new(DynamicMcpServerEntry::new(server));
            if registry.dynamic_mcp_servers.pin().insert(namespace.clone(), entry).is_some() {
                return Err(ToolBuilderError::DuplicateDynamicServer(namespace));
            }
        }

        Ok(registry)
    }

    fn validate_supplied_embedding(&self, tool: &McpTool) -> Result<(), ToolBuilderError> {
        let supplied = tool.embedding.as_slice();
        if supplied.is_empty() {
            return Ok(());
        }
        let qualified_name = || SmolStr::from(format!("{}.{}", tool.namespace, tool.name));
        if !supplied.iter().all(|x| x.is_finite()) {
            return Err(ToolBuilderError::EmbeddingNotFinite(qualified_name()));
        }
        let Some(client) = self.embeddings_client.as_ref() else {
            return Ok(());
        };
        if supplied.len() != client.dimensions() {
            return Err(ToolBuilderError::EmbeddingDimensionMismatch {
                tool: qualified_name(),
                expected: client.dimensions(),
                got: supplied.len(),
            });
        }
        Ok(())
    }

    fn validate_namespace(namespace: &SmolStr) -> Result<(), ToolBuilderError> {
        if namespace.is_empty()
            || namespace.contains('.')
            || namespace.contains('/')
            || !valid_mcp_name_segment(namespace)
        {
            return Err(ToolBuilderError::InvalidNamespace(namespace.clone()));
        }
        Ok(())
    }

    fn namespace_has_direct_tools(&self, namespace: &str) -> bool {
        self.tools
            .pin()
            .iter()
            .any(|(id, entry)| id.namespace == namespace && matches!(entry.source, ToolSource::Direct))
    }

    fn insert_tool(&self, id: ToolId, entry: ToolEntry) -> Option<StdArc<ToolEntry>> {
        if entry.embedding.load_full().is_none() {
            self.embeddings_pending.store(true, Ordering::Release);
        }
        self.tools.pin().insert(id, StdArc::new(entry)).cloned()
    }

    fn max_allowable_results(&self) -> usize {
        self.search.max_allowable_results
    }

    pub async fn bootstrap(&self) -> Result<(), CallToolError> {
        self.bootstrapped
            .get_or_init(|| async {
                // Snapshot before awaiting so the pinned map guard never crosses an async boundary.
                let servers = {
                    let pinned = self.dynamic_mcp_servers.pin();
                    pinned.iter().map(|(_, server)| StdArc::clone(server)).collect::<Vec<_>>()
                };
                for server in servers {
                    self.fetch_and_materialise(&server).await;
                }
            })
            .await;
        Ok(())
    }

    /// Brings the registry up to date: bootstraps and refreshes dynamic
    /// servers and embeds new tools. Request entry points call this exactly
    /// once before reading.
    pub(crate) async fn ensure_tools_current(&self, req_ctx: &RequestCtx) -> Result<(), CallToolError> {
        self.bootstrap().await?;
        self.refresh_expired_dynamic_servers().await;
        self.embed_unembedded_tools(req_ctx).await;
        Ok(())
    }

    #[allow(clippy::unused_async)]
    pub async fn add_tool(&self, tool: McpTool) -> Result<(), ToolBuilderError> {
        let id = ToolId::from_tool(&tool)?;
        if self.dynamic_mcp_servers.pin().contains_key(&id.namespace) {
            return Err(ToolBuilderError::SharedNamespace(id.namespace));
        }
        self.validate_supplied_embedding(&tool)?;
        let entry = build_tool_entry(tool, ToolSource::Direct, &self.upstream_limits)?;
        self.insert_tool(id, entry);
        Ok(())
    }

    pub fn remove_tool(&self, id: &ToolId) -> bool {
        let should_remove = self.tools.pin().get(id).is_some_and(|entry| matches!(entry.source, ToolSource::Direct));
        should_remove && self.tools.pin().remove(id).is_some()
    }

    #[inline]
    pub fn get_tool(&self, id: &ToolId) -> Option<StdArc<ToolEntry>> {
        self.tools.pin().get(id).map(StdArc::clone)
    }

    #[inline]
    pub fn get_tool_by_name(&self, name: &str) -> Option<StdArc<ToolEntry>> {
        ToolId::parse_qualified(name).and_then(|id| self.get_tool(&id))
    }

    /// All registry entries the request is permitted to see.
    fn permitted_snapshot(&self, req_ext: &http::Extensions) -> Vec<StdArc<ToolEntry>> {
        let pinned = self.tools.pin();
        pinned.iter().filter(|(_, entry)| entry.is_permitted(req_ext)).map(|(_, entry)| StdArc::clone(entry)).collect()
    }

    /// Returns `None` when any requested entry is absent or unauthorized.
    pub fn permitted_entries_by_id(
        &self,
        req_ext: &http::Extensions,
        ids: &[ToolId],
    ) -> Option<Vec<StdArc<ToolEntry>>> {
        let pinned = self.tools.pin();
        let mut entries = Vec::with_capacity(ids.len());
        for id in ids {
            let entry = pinned.get(id)?;
            if !entry.is_permitted(req_ext) {
                return None;
            }
            entries.push(StdArc::clone(entry));
        }
        Some(entries)
    }

    pub fn catalog_namespaces(
        &self,
        req_ext: &http::Extensions,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<CatalogPage<NamespaceSummary>, CallToolError> {
        let mut counts = BTreeMap::<SmolStr, usize>::new();
        for entry in self.permitted_snapshot(req_ext) {
            *counts.entry(entry.id.namespace.clone()).or_default() += 1;
        }
        let items =
            counts.into_iter().map(|(namespace, tool_count)| NamespaceSummary { namespace, tool_count }).collect();
        catalog_page(items, limit, offset)
    }

    pub fn catalog_list_tools(
        &self,
        req_ext: &http::Extensions,
        namespace: &str,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<CatalogPage<CompactToolDefinition>, CallToolError> {
        let mut entries = self
            .permitted_snapshot(req_ext)
            .into_iter()
            .filter(|entry| entry.id.namespace == namespace)
            .collect::<Vec<_>>();
        entries.sort_by(|left, right| left.id.name.cmp(&right.id.name));
        catalog_page(entries.into_iter().map(|entry| entry.compact()).collect(), limit, offset)
    }

    pub fn catalog_describe_tool(&self, req_ext: &http::Extensions, id: &ToolId) -> Option<ToolDefinition> {
        self.get_tool(id).filter(|entry| entry.is_permitted(req_ext)).map(|entry| entry.definition())
    }

    pub fn list_disclosed_tools(&self, req_ext: &http::Extensions) -> (Vec<Tool>, Vec<SmolStr>) {
        let entries = self.permitted_snapshot(req_ext);
        let mut tools = Vec::with_capacity(entries.len());
        let mut progressive_names = Vec::new();
        for entry in entries {
            match entry.disclosure_mode {
                DisclosureMode::Eager => tools.push(entry.tool.clone()),
                DisclosureMode::Progressive => progressive_names.push(entry.id.qualified_name().into()),
            }
        }

        tools.sort_by(|left, right| left.name.cmp(&right.name));
        progressive_names.sort_unstable();

        (tools, progressive_names)
    }

    pub fn has_progressive_tools(&self, req_ext: &http::Extensions) -> bool {
        self.permitted_snapshot(req_ext).iter().any(|entry| entry.disclosure_mode == DisclosureMode::Progressive)
    }
}
