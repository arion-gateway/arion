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

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc as StdArc, Once, OnceLock, Weak},
};

use orion_configuration::config::{
    common::GenericError,
    network_filters::http_connection_manager::http_filters::mcp_gateway::{DynamicMcpServer, McpTool},
};
use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
    DynamicMcpServer as OrionDynamicMcpServer, Tool as OrionTool,
};
use orion_xds::xds::{
    client::DeltaDiscoverySubscriptionManager,
    extension::{XdsExtensionError, XdsExtensionHandler},
    model::TypeUrl,
};
use papaya::HashMap as PapayaMap;
use prost::Message;
use smol_str::SmolStr;
use tracing::{debug, warn};

use super::tools::ToolsRegistry;

pub const MCP_TOOL_TYPE_URL: &str = "type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.Tool";
pub const MCP_DYNAMIC_SERVER_TYPE_URL: &str =
    "type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.DynamicMcpServer";

const SUPPORTED_TYPE_URLS: &[&str] = &[MCP_TOOL_TYPE_URL, MCP_DYNAMIC_SERVER_TYPE_URL];

// papaya values are immutable `&V`: the per-scope subscriber list needs interior
// mutability, hence the `Mutex<Vec<...>>`. The outer `StdArc` lets `drain` clone
// the list holder before removing the key, so concurrent `register` calls racing
// with a drain are not lost.
type SubscriptionsByScope =
    PapayaMap<SmolStr, StdArc<parking_lot::Mutex<Vec<Weak<ToolsRegistry>>>>, ahash::RandomState>;

static MCP_XDS_HANDLER: OnceLock<StdArc<McpXdsHandler>> = OnceLock::new();
static PENDING_MCP_XDS_SUBSCRIPTIONS: OnceLock<SubscriptionsByScope> = OnceLock::new();

pub fn init_mcp_xds_handler(subscriber: StdArc<DeltaDiscoverySubscriptionManager>) -> StdArc<McpXdsHandler> {
    let handler = StdArc::clone(MCP_XDS_HANDLER.get_or_init(|| StdArc::new(McpXdsHandler::new(subscriber))));
    drain_pending_subscriptions(pending_subscriptions(), &handler);
    handler
}

pub fn get_mcp_xds_handler() -> Option<&'static StdArc<McpXdsHandler>> {
    MCP_XDS_HANDLER.get()
}

pub fn subscribe_for_updates(scope: SmolStr, registry: &StdArc<ToolsRegistry>) {
    if let Some(handler) = get_mcp_xds_handler() {
        handler.register(scope, registry);
    } else {
        debug!(target: "mcp_gateway", "xDS handler not initialized; queuing MCP filter TDS subscription for '{scope}'");
        queue_subscription(pending_subscriptions(), scope, registry);
        if let Some(handler) = get_mcp_xds_handler() {
            drain_pending_subscriptions(pending_subscriptions(), handler);
        }
    }
}

pub fn unsubscribe_from_updates(scope: &str, registry: &StdArc<ToolsRegistry>) {
    if let Some(handler) = get_mcp_xds_handler() {
        handler.unregister(scope, registry);
    }
    remove_subscription(pending_subscriptions(), scope, registry);
}

fn pending_subscriptions() -> &'static SubscriptionsByScope {
    PENDING_MCP_XDS_SUBSCRIPTIONS.get_or_init(|| PapayaMap::with_hasher(ahash::RandomState::new()))
}

fn queue_subscription(subscriptions: &SubscriptionsByScope, scope: SmolStr, registry: &StdArc<ToolsRegistry>) {
    let pinned = subscriptions.pin();
    let subscribers = pinned.get_or_insert_with(scope, || StdArc::new(parking_lot::Mutex::new(Vec::new())));
    subscribers.lock().push(StdArc::downgrade(registry));
}

fn remove_subscription(subscriptions: &SubscriptionsByScope, scope: &str, registry: &StdArc<ToolsRegistry>) {
    let empty = {
        let pinned = subscriptions.pin();
        let Some(subscribers) = pinned.get(scope) else { return };
        let mut guard = subscribers.lock();
        guard.retain(|weak| match weak.upgrade() {
            Some(arc) => !StdArc::ptr_eq(&arc, registry),
            None => false,
        });
        guard.is_empty()
    };
    if empty {
        subscriptions.pin().remove(scope);
    }
}

fn drain_pending_subscriptions(subscriptions: &SubscriptionsByScope, handler: &McpXdsHandler) {
    let scopes: Vec<SmolStr> = {
        let pinned = subscriptions.pin();
        pinned.iter().map(|(scope, _)| scope.clone()).collect()
    };
    for scope in scopes {
        let subscribers: Option<StdArc<parking_lot::Mutex<Vec<Weak<ToolsRegistry>>>>> =
            subscriptions.pin().get(scope.as_str()).map(StdArc::clone);
        let Some(subscribers) = subscribers else { continue };
        subscriptions.pin().remove(scope.as_str());
        let registries = std::mem::take(&mut *subscribers.lock());
        for registry in registries.into_iter().filter_map(|weak| weak.upgrade()) {
            handler.register(scope.clone(), &registry);
        }
    }
}

#[derive(Debug)]
pub struct McpXdsHandler {
    // `Weak` so dropped registries self-clean without explicit unregister.
    subscriptions_by_scope: SubscriptionsByScope,
    subscriber: StdArc<DeltaDiscoverySubscriptionManager>,
    subscribe_once: Once,
}

impl McpXdsHandler {
    pub fn new(subscriber: StdArc<DeltaDiscoverySubscriptionManager>) -> Self {
        Self {
            subscriptions_by_scope: PapayaMap::with_hasher(ahash::RandomState::new()),
            subscriber,
            subscribe_once: Once::new(),
        }
    }

    pub fn register(&self, scope: SmolStr, registry: &StdArc<ToolsRegistry>) {
        debug!(target: "mcp_gateway", "Registering TDS registry for scope: {scope}");
        let pinned = self.subscriptions_by_scope.pin();
        let subscribers = pinned.get_or_insert_with(scope, || StdArc::new(parking_lot::Mutex::new(Vec::new())));
        subscribers.lock().push(StdArc::downgrade(registry));
        self.try_subscribe();
    }

    fn try_subscribe(&self) {
        self.subscribe_once.call_once(|| {
            let sub = StdArc::clone(&self.subscriber);
            tokio::spawn(async move {
                for url in SUPPORTED_TYPE_URLS {
                    if let Err(e) = sub.subscribe("*".to_owned(), TypeUrl::Extension((*url).to_owned())).await {
                        warn!(target: "mcp_gateway", "Failed to subscribe to MCP extension type URL {url}: {e}");
                    } else {
                        debug!(target: "mcp_gateway", "Subscribed to MCP extension type URL {url}");
                    }
                }
            });
        });
    }

    pub fn unregister(&self, scope: &str, registry: &StdArc<ToolsRegistry>) {
        remove_subscription(&self.subscriptions_by_scope, scope, registry);
        debug!(target: "mcp_gateway", "Unregistered TDS registry for scope: {scope}");
    }

    // Split `"{server_name}/{config_name}/{resource_name}"` into
    // `(scope = "{server_name}/{config_name}", resource_name)`. Resource
    // name may contain `/`; server_name and config_name may not (enforced
    // at config parse time), so the boundary is unambiguous.
    #[allow(clippy::string_slice)]
    fn split_resource_id(resource_id: &str) -> Result<(&str, &str), XdsExtensionError> {
        let mut parts = resource_id.splitn(3, '/');
        let (Some(server), Some(config), Some(name)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(XdsExtensionError::HandlerError(format!(
                "Invalid MCP resource ID '{resource_id}': expected format 'server_name/config_name/resource_name'"
            )));
        };
        if server.is_empty() || config.is_empty() || name.is_empty() {
            return Err(XdsExtensionError::HandlerError(format!(
                "Invalid MCP resource ID '{resource_id}': empty server_name, config_name, or resource_name"
            )));
        }
        let scope_len = server.len() + 1 + config.len();
        Ok((&resource_id[..scope_len], name))
    }

    fn live_registries(&self, scope: &str) -> Vec<StdArc<ToolsRegistry>> {
        let pinned = self.subscriptions_by_scope.pin();
        let Some(subscribers) = pinned.get(scope) else { return Vec::new() };
        let mut guard = subscribers.lock();
        let mut out = Vec::new();
        guard.retain(|weak| match weak.upgrade() {
            Some(arc) => {
                out.push(arc);
                true
            },
            None => false,
        });
        out
    }
}

fn decode_proto<M: Message + Default>(payload: &[u8], what: &str) -> Result<M, XdsExtensionError> {
    M::decode(payload).map_err(|e| XdsExtensionError::DecodeError(format!("Failed to decode {what}: {e}")))
}

fn convert<S, T>(source: S, what: &str) -> Result<T, XdsExtensionError>
where
    T: TryFrom<S, Error = GenericError>,
{
    T::try_from(source).map_err(|e| XdsExtensionError::HandlerError(format!("Failed to convert {what}: {e}")))
}

impl XdsExtensionHandler for McpXdsHandler {
    fn type_urls(&self) -> &[&str] {
        SUPPORTED_TYPE_URLS
    }

    fn handle_update(
        &self,
        type_url: &str,
        resource_id: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), XdsExtensionError>> + Send + '_>> {
        let type_url = type_url.to_owned();
        let resource_id = resource_id.to_owned();
        let payload = payload.to_vec();
        Box::pin(async move {
            let (scope, name) = Self::split_resource_id(&resource_id)?;
            let registries = self.live_registries(scope);
            if registries.is_empty() {
                debug!(target: "mcp_gateway", "xDS: no live registry for scope '{scope}', dropping update for '{name}'");
                return Ok(());
            }

            match type_url.as_str() {
                MCP_TOOL_TYPE_URL => {
                    let proto = decode_proto::<OrionTool>(&payload, "MCP Tool")?;
                    let mut tool: McpTool = convert(proto, "MCP Tool")?;
                    if tool.name.as_str() != name {
                        warn!(
                            target: "mcp_gateway",
                            "xDS: Tool payload name '{}' does not match resource name '{name}' in scope '{scope}'; using resource name",
                            tool.name
                        );
                        tool.name = name.into();
                    }
                    for registry in &registries {
                        registry.add_tool(tool.clone()).await.map_err(|e| {
                            XdsExtensionError::HandlerError(format!("Failed to add tool '{name}': {e}"))
                        })?;
                    }
                    debug!(target: "mcp_gateway", "xDS: added/updated tool '{name}' in scope '{scope}' ({} registries)", registries.len());
                },
                MCP_DYNAMIC_SERVER_TYPE_URL => {
                    let proto = decode_proto::<OrionDynamicMcpServer>(&payload, "MCP DynamicMcpServer")?;
                    let server: DynamicMcpServer = convert(proto, "MCP DynamicMcpServer")?;
                    for registry in &registries {
                        registry.add_dynamic_server(server.clone()).await.map_err(|e| {
                            XdsExtensionError::HandlerError(format!("Failed to add dynamic server '{name}': {e}"))
                        })?;
                    }
                    debug!(target: "mcp_gateway", "xDS: added/updated dynamic server '{name}' in scope '{scope}' ({} registries)", registries.len());
                },
                other => {
                    return Err(XdsExtensionError::HandlerError(format!("unsupported type_url: {other}")));
                },
            }
            Ok(())
        })
    }

    fn handle_remove(
        &self,
        type_url: &str,
        resource_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), XdsExtensionError>> + Send + '_>> {
        let type_url = type_url.to_owned();
        let resource_id = resource_id.to_owned();
        Box::pin(async move {
            let (scope, name) = Self::split_resource_id(&resource_id)?;
            let registries = self.live_registries(scope);
            if registries.is_empty() {
                debug!(target: "mcp_gateway", "xDS: no live registry for scope '{scope}', dropping remove for '{name}'");
                return Ok(());
            }

            match type_url.as_str() {
                MCP_TOOL_TYPE_URL => {
                    let mut any = false;
                    for registry in &registries {
                        any |= registry.remove_tool(name);
                    }
                    if any {
                        debug!(target: "mcp_gateway", "xDS: removed tool '{name}' from scope '{scope}'");
                    } else {
                        warn!(target: "mcp_gateway", "xDS: tool '{name}' not found in scope '{scope}' for removal");
                    }
                },
                MCP_DYNAMIC_SERVER_TYPE_URL => {
                    let mut any = false;
                    for registry in &registries {
                        any |= registry.remove_dynamic_server(name);
                    }
                    if any {
                        debug!(target: "mcp_gateway", "xDS: removed dynamic server '{name}' from scope '{scope}'");
                    } else {
                        warn!(target: "mcp_gateway", "xDS: dynamic server '{name}' not found in scope '{scope}' for removal");
                    }
                },
                other => {
                    return Err(XdsExtensionError::HandlerError(format!("unsupported type_url: {other}")));
                },
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing, clippy::assertions_on_result_states)]
    use super::*;
    use crate::listeners::http_connection_manager::mcp_gateway::embeddings;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        tool::UpstreamBackend as OrionUpstreamBackend, RestBackend as OrionRestBackend,
    };
    use orion_xds::xds::client::SubscriptionEvent;
    use tokio::sync::mpsc;

    fn empty_registry() -> StdArc<ToolsRegistry> {
        StdArc::new(ToolsRegistry::with_config(Vec::new(), Vec::new(), None, None).unwrap())
    }

    fn test_sub_mgr() -> (StdArc<DeltaDiscoverySubscriptionManager>, mpsc::Receiver<SubscriptionEvent>) {
        let (tx, rx) = mpsc::channel::<SubscriptionEvent>(8);
        (StdArc::new(DeltaDiscoverySubscriptionManager::from_sender(tx)), rx)
    }

    fn stub_handler() -> McpXdsHandler {
        let (sub, _rx) = test_sub_mgr();
        McpXdsHandler::new(sub)
    }

    fn scope_len(handler: &McpXdsHandler, scope: &str) -> usize {
        handler.subscriptions_by_scope.pin().get(scope).map_or(0, |subscribers| subscribers.lock().len())
    }

    fn subscription_map() -> SubscriptionsByScope {
        PapayaMap::with_hasher(ahash::RandomState::new())
    }

    fn rest_tool_proto(name: &str) -> OrionTool {
        OrionTool {
            name: name.into(),
            description: "A test tool".into(),
            input_schema: None,
            output_schema: None,
            upstream_backend: Some(OrionUpstreamBackend::RestBackend(OrionRestBackend {
                method: "GET".into(),
                path: "/test".into(),
                query_params: Vec::new(),
                cluster: "test_cluster".into(),
                body_template: None,
                upstream_policy: None,
            })),
            rbac: None,
            embedding: Vec::new(),
        }
    }

    #[test]
    fn split_resource_id_parses_three_segments() {
        let (scope, name) = McpXdsHandler::split_resource_id("srv/cfg/tool_a").unwrap();
        assert_eq!(scope, "srv/cfg");
        assert_eq!(name, "tool_a");
    }

    #[test]
    fn split_resource_id_allows_slash_in_resource_name() {
        let (scope, name) = McpXdsHandler::split_resource_id("srv/cfg/ns/tool_a").unwrap();
        assert_eq!(scope, "srv/cfg");
        assert_eq!(name, "ns/tool_a");
    }

    #[test]
    fn split_resource_id_rejects_missing_segments() {
        assert!(McpXdsHandler::split_resource_id("bad").is_err());
        assert!(McpXdsHandler::split_resource_id("srv/cfg").is_err());
        assert!(McpXdsHandler::split_resource_id("//tool").is_err());
        assert!(McpXdsHandler::split_resource_id("srv//tool").is_err());
        assert!(McpXdsHandler::split_resource_id("srv/cfg/").is_err());
    }

    #[test]
    fn queued_subscription_can_be_removed_before_handler_init() {
        let subscriptions = subscription_map();
        let scope: SmolStr = "srv/cfg".into();
        let r1 = empty_registry();
        let r2 = empty_registry();

        queue_subscription(&subscriptions, scope.clone(), &r1);
        queue_subscription(&subscriptions, scope.clone(), &r2);
        remove_subscription(&subscriptions, &scope, &r1);

        let queued = subscriptions.pin().get(&scope).map(|m| m.lock().clone()).expect("scope should remain queued");
        assert_eq!(queued.len(), 1);
        assert!(queued[0].upgrade().is_some_and(|arc| StdArc::ptr_eq(&arc, &r2)));
    }

    #[tokio::test]
    async fn drain_pending_subscriptions_registers_only_live_registries() {
        let subscriptions = subscription_map();
        let handler = stub_handler();
        let scope: SmolStr = "srv/cfg".into();
        let live = empty_registry();
        let dropped = empty_registry();

        queue_subscription(&subscriptions, scope.clone(), &live);
        queue_subscription(&subscriptions, scope.clone(), &dropped);
        drop(dropped);

        drain_pending_subscriptions(&subscriptions, &handler);

        assert!(subscriptions.pin().is_empty());
        assert_eq!(scope_len(&handler, &scope), 1);
        assert!(handler.live_registries(&scope).iter().all(|arc| StdArc::ptr_eq(arc, &live)));
    }

    #[tokio::test]
    async fn register_and_unregister_specific_registry() {
        let handler = stub_handler();
        let scope: SmolStr = "srv/cfg".into();
        let r1 = empty_registry();
        let r2 = empty_registry();
        handler.register(scope.clone(), &r1);
        handler.register(scope.clone(), &r2);
        assert_eq!(scope_len(&handler, &scope), 2);

        handler.unregister(&scope, &r1);
        assert_eq!(scope_len(&handler, &scope), 1);
        assert!(handler.live_registries(&scope).iter().all(|a| StdArc::ptr_eq(a, &r2)));

        handler.unregister(&scope, &r2);
        assert!(!handler.subscriptions_by_scope.pin().contains_key(scope.as_str()));
    }

    #[tokio::test]
    async fn live_registries_prunes_dead_weaks() {
        let handler = stub_handler();
        let scope: SmolStr = "srv/cfg".into();
        let r1 = empty_registry();
        let r2 = empty_registry();
        handler.register(scope.clone(), &r1);
        handler.register(scope.clone(), &r2);

        drop(r1);
        let live = handler.live_registries(&scope);
        assert_eq!(live.len(), 1);
        assert!(StdArc::ptr_eq(&live[0], &r2));
        assert_eq!(scope_len(&handler, &scope), 1);
    }

    #[tokio::test]
    async fn unknown_scope_update_is_noop() {
        let handler = stub_handler();
        // No scope registered — handle_update must not error and must not touch storage.
        let res = handler.handle_update(MCP_TOOL_TYPE_URL, "srv/cfg/tool_x", &[]).await;
        assert!(res.is_ok());
        assert!(handler.subscriptions_by_scope.pin().is_empty());
    }

    #[tokio::test]
    async fn tool_update_uses_resource_name_when_payload_name_differs() {
        let handler = stub_handler();
        let registry = empty_registry();
        handler.register("srv/cfg".into(), &registry);

        let proto = rest_tool_proto("payload_name");
        let payload = proto.encode_to_vec();

        handler.handle_update(MCP_TOOL_TYPE_URL, "srv/cfg/resource_name", &payload).await.unwrap();

        assert!(registry.get_tool_by_name("resource_name").is_some());
        assert!(registry.get_tool_by_name("payload_name").is_none());
        assert!(registry.remove_tool("resource_name"));
    }

    #[tokio::test]
    async fn tool_update_embedding_failure_still_inserts_tool() {
        use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
            McpSemanticSearch, SimilarityConfig,
        };

        let handler = stub_handler();
        let client = embeddings::EmbeddingsClient::test_failing();
        let registry = StdArc::new(
            ToolsRegistry::with_config(
                Vec::new(),
                Vec::new(),
                Some(McpSemanticSearch {
                    enable_assisted_discovery: false,
                    embeddings: None,
                    similarity: SimilarityConfig::default(),
                }),
                Some(client),
            )
            .unwrap(),
        );
        handler.register("srv/cfg".into(), &registry);

        let payload = rest_tool_proto("needs_embedding").encode_to_vec();
        let res = handler.handle_update(MCP_TOOL_TYPE_URL, "srv/cfg/needs_embedding", &payload).await;

        assert!(res.is_ok(), "embeddings failure should not NACK the update: {res:?}");
        let entry = registry.get_tool_by_name("needs_embedding").expect("tool should be inserted");
        assert!(entry.embedding.load_full().is_none(), "tool should remain unembedded after embeddings failure");
    }

    async fn drain_subscribes(rx: &mut mpsc::Receiver<SubscriptionEvent>) -> Vec<String> {
        let mut urls = Vec::new();
        while let Ok(Some(ev)) =
            pingora_timeout::fast_timeout::fast_timeout(std::time::Duration::from_millis(100), rx.recv()).await
        {
            if let SubscriptionEvent::Subscribe(TypeUrl::Extension(url), _) = ev {
                urls.push(url);
            }
        }
        urls
    }

    #[tokio::test]
    async fn no_subscribe_until_first_register() {
        let (sub, mut rx) = test_sub_mgr();
        let _handler = McpXdsHandler::new(sub);
        let urls = drain_subscribes(&mut rx).await;
        assert!(urls.is_empty());
    }

    #[tokio::test]
    async fn subscribe_fires_on_first_register() {
        let (sub, mut rx) = test_sub_mgr();
        let handler = McpXdsHandler::new(sub);

        handler.register("srv/cfg".into(), &empty_registry());
        let urls = drain_subscribes(&mut rx).await;
        assert_eq!(urls.len(), SUPPORTED_TYPE_URLS.len());
        for expected in SUPPORTED_TYPE_URLS {
            assert!(urls.iter().any(|u| u == expected));
        }

        handler.register("srv/cfg2".into(), &empty_registry());
        assert!(drain_subscribes(&mut rx).await.is_empty());
    }
}
