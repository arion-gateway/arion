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

//! Dynamic MCP upstream servers: TTL bookkeeping, refresh, and tool materialisation.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc as StdArc;
use std::time::{Duration, Instant};

use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
    DynamicMcpServer, EmbeddingVector, McpBackendTransportUpstream, McpTool, UpstreamBackend,
};
use atomic_time::AtomicInstant;
use rmcp::{
    model::{ClientCapabilities, ClientConfig, Implementation, InitializeRequestParams, Tool},
    service::{ClientInitializeError, RoleClient, RunningService},
    transport::{streamable_http_client::StreamableHttpClientTransportConfig, StreamableHttpClientTransport},
    ServiceExt,
};
use scopeguard::defer;
use smol_str::SmolStr;
use tracing::{debug, info, warn};

use super::{
    entry::{build_tool_entry, convert_config_rbac_to_runtime},
    errors::ListToolsError,
    ToolEntry, ToolId, ToolSource, ToolsRegistry,
};
use crate::listeners::http_connection_manager::mcp_gateway::rbac::ToolRbac;

use super::rest::DEFAULT_USER_AGENT;

const INDEFINITE_CACHE_LIFETIME: Duration = Duration::from_secs(100 * 365 * 24 * 3600);
const DEFAULT_DYNAMIC_REFRESH_RETRY_INTERVAL: Duration = Duration::from_secs(30);

thread_local! {
    /// Keep-alive pool local to each worker thread. Each transport retains its
    /// own MCP session state while sharing this thread's HTTP connection pool.
    static UPSTREAM_REQWEST_CLIENT: reqwest::Client = reqwest::Client::builder()
        .pool_max_idle_per_host(usize::MAX)
        .pool_idle_timeout(Duration::from_secs(90))
        .tcp_keepalive(Duration::from_secs(60))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("failed to build upstream reqwest client");
}

fn upstream_http_client() -> reqwest::Client {
    UPSTREAM_REQWEST_CLIENT.with(Clone::clone)
}

pub struct DynamicMcpServerEntry {
    pub conf: DynamicMcpServer,
    pub rbac: Option<ToolRbac>,
    pub expires_at: AtomicInstant,
    refresh_in_progress: AtomicBool,
}

impl std::fmt::Debug for DynamicMcpServerEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DynamicMcpServerEntry")
            .field("conf", &self.conf)
            .field("rbac", &self.rbac)
            .field("expires_at", &self.expires_at.load(Ordering::Relaxed))
            .field("refresh_in_progress", &self.refresh_in_progress.load(Ordering::Relaxed))
            .finish()
    }
}

impl DynamicMcpServerEntry {
    pub(super) fn new(conf: DynamicMcpServer) -> Self {
        let rbac = conf.rbac.as_ref().map(convert_config_rbac_to_runtime);
        Self { conf, rbac, expires_at: AtomicInstant::new(Instant::now()), refresh_in_progress: AtomicBool::new(false) }
    }

    #[inline]
    fn is_expired(&self, now: Instant) -> bool {
        now >= self.expires_at.load(Ordering::Relaxed)
    }

    fn bump_deadline(&self, refresh_succeeded: bool) {
        let lifetime = self.conf.cache_duration.unwrap_or(if refresh_succeeded {
            INDEFINITE_CACHE_LIFETIME
        } else {
            DEFAULT_DYNAMIC_REFRESH_RETRY_INTERVAL
        });
        let deadline = Instant::now().checked_add(lifetime).unwrap_or_else(|| Instant::now() + Duration::from_secs(60));
        self.expires_at.store(deadline, Ordering::Relaxed);
    }
}

impl ToolsRegistry {
    pub async fn add_dynamic_server(&self, server: DynamicMcpServer) -> Result<(), super::ToolBuilderError> {
        Self::validate_namespace(&server.namespace)?;
        let namespace = server.namespace.clone();
        if self.namespace_has_direct_tools(&namespace) || self.open_api_sources.pin().contains(&namespace) {
            return Err(super::ToolBuilderError::SharedNamespace(namespace));
        }
        let entry = StdArc::new(DynamicMcpServerEntry::new(server));
        let materialised = self
            .fetch_dynamic_server_tools(&entry)
            .await
            .map_err(|error| super::ToolBuilderError::DynamicRefresh(error.to_string()))?;
        if self.namespace_has_direct_tools(&namespace) || self.open_api_sources.pin().contains(&namespace) {
            return Err(super::ToolBuilderError::SharedNamespace(namespace));
        }
        entry.bump_deadline(true);
        self.dynamic_mcp_servers.pin().insert(namespace.clone(), entry);
        self.replace_dynamic_tools(&namespace, materialised);
        Ok(())
    }

    pub fn remove_dynamic_server(&self, namespace: &str) -> bool {
        if self.dynamic_mcp_servers.pin().remove(namespace).is_none() {
            return false;
        }
        self.evict_dynamic_tools_for(namespace);
        true
    }

    pub(super) async fn refresh_expired_dynamic_servers(&self) {
        let now = Instant::now();
        let expired: Vec<StdArc<DynamicMcpServerEntry>> = {
            let pinned = self.dynamic_mcp_servers.pin();
            pinned.iter().filter(|(_, entry)| entry.is_expired(now)).map(|(_, entry)| StdArc::clone(entry)).collect()
        };

        for entry in expired {
            self.refresh_if_idle(entry).await;
        }
    }

    async fn refresh_if_idle(&self, server: StdArc<DynamicMcpServerEntry>) {
        if server.refresh_in_progress.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            debug!(target: "mcp_gateway", "Refresh already in progress for dynamic MCP namespace '{}'", server.conf.namespace);
            return;
        }
        defer! {
            server.refresh_in_progress.store(false, Ordering::Release);
        }

        if !server.is_expired(Instant::now()) {
            return;
        }

        self.fetch_and_materialise(&server).await;
    }

    pub(super) async fn fetch_and_materialise(&self, server: &StdArc<DynamicMcpServerEntry>) {
        let namespace = &server.conf.namespace;
        match self.fetch_dynamic_server_tools(server).await {
            Ok(materialised) => {
                let is_current = self
                    .dynamic_mcp_servers
                    .pin()
                    .get(namespace)
                    .is_some_and(|current| StdArc::ptr_eq(current, server));
                if !is_current {
                    debug!(target: "mcp_gateway", "Discarding refresh from replaced dynamic MCP namespace '{}'", namespace);
                    return;
                }
                self.replace_dynamic_tools(namespace, materialised);
                server.bump_deadline(true);
                debug!(target: "mcp_gateway", "Refreshed dynamic MCP namespace '{}'", namespace);
            },
            Err(err) => {
                server.bump_deadline(false);
                warn!(target: "mcp_gateway", "Failed to refresh dynamic MCP namespace '{}': {}", namespace, err);
            },
        }
    }

    fn replace_dynamic_tools(&self, namespace: &str, entries: Vec<ToolEntry>) {
        self.evict_dynamic_tools_for(namespace);
        for entry in entries {
            let id = entry.id.clone();
            self.insert_tool(id, entry);
        }
    }

    fn evict_dynamic_tools_for(&self, namespace: &str) {
        self.tools.pin().retain(|id, entry| id.namespace != namespace || matches!(entry.source, ToolSource::Direct));
    }

    async fn fetch_dynamic_server_tools(
        &self,
        server: &DynamicMcpServerEntry,
    ) -> Result<Vec<ToolEntry>, ListToolsError> {
        let upstream_tools = Self::list_upstream_tools(&server.conf.transport, &server.conf.url).await?;
        self.materialise_dynamic_tools(server, upstream_tools)
    }

    pub(super) fn materialise_dynamic_tools(
        &self,
        server: &DynamicMcpServerEntry,
        upstream_tools: Vec<Tool>,
    ) -> Result<Vec<ToolEntry>, ListToolsError> {
        let namespace = server.conf.namespace.clone();
        let mut entries = Vec::with_capacity(upstream_tools.len());
        let mut ids = std::collections::HashSet::with_capacity(upstream_tools.len());
        for tool in upstream_tools {
            let upstream_tool_name: SmolStr = tool.name.as_ref().into();
            let description = tool.description.as_deref().unwrap_or_default().into();
            let input_schema = tool.input_schema.as_ref().clone();
            let output_schema = tool.output_schema.as_deref().cloned().unwrap_or_default();
            let conf = McpTool {
                namespace: namespace.clone(),
                name: upstream_tool_name.clone(),
                description,
                input_schema,
                output_schema,
                backend: UpstreamBackend::McpServer {
                    transport: server.conf.transport.clone(),
                    url: server.conf.url.clone(),
                },
                rbac: server.conf.rbac.clone(),
                embedding: EmbeddingVector::default(),
                disclosure_mode: server.conf.tool_disclosure_mode,
            };
            let id = ToolId::from_tool(&conf).map_err(|error| ListToolsError::InvalidSnapshot(error.to_string()))?;
            if !ids.insert(id.clone()) {
                return Err(ListToolsError::InvalidSnapshot(format!(
                    "duplicate upstream tool '{}'",
                    id.qualified_name()
                )));
            }
            entries.push(
                build_tool_entry(conf, ToolSource::Dynamic { upstream_tool_name }, &self.upstream_limits)
                    .map_err(|error| ListToolsError::InvalidSnapshot(error.to_string()))?,
            );
        }
        Ok(entries)
    }

    async fn list_upstream_tools(
        transport: &McpBackendTransportUpstream,
        url: &str,
    ) -> Result<Vec<Tool>, ListToolsError> {
        debug!(target: "mcp_gateway", "Listing tools from upstream with transport {transport:?} at {url}");
        match transport {
            McpBackendTransportUpstream::StreamableHttp => {
                let client = Self::get_mcp_client(url).await?;
                let tools = client.list_tools(Option::default()).await.map_err(Box::new)?;
                Ok(tools.tools)
            },
            McpBackendTransportUpstream::Sse => Err(ListToolsError::UnsupportedTransport),
        }
    }

    pub(super) async fn get_mcp_client(
        url: &str,
    ) -> Result<RunningService<RoleClient, InitializeRequestParams>, Box<ClientInitializeError>> {
        debug!(target: "mcp_gateway", "Creating MCP client for URL: {url}...");
        let transport = StreamableHttpClientTransport::with_client(
            upstream_http_client(),
            StreamableHttpClientTransportConfig::with_uri(url),
        );
        let client_info =
            ClientConfig::new(ClientCapabilities::default(), Implementation::new(DEFAULT_USER_AGENT, "0.1.0"));
        client_info.serve(transport).await.map_err(Box::new).inspect_err(|e| {
            info!(target: "mcp_gateway", "get_mcp_client error: {}!", e);
        })
    }
}
