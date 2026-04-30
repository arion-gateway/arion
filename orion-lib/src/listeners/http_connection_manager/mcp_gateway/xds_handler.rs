use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Once, OnceLock, Weak},
};

use dashmap::DashMap;
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
use prost::Message;
use smol_str::SmolStr;
use tracing::{debug, warn};

use super::tools::ToolsRegistry;

pub const MCP_TOOL_TYPE_URL: &str = "type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.Tool";
pub const MCP_DYNAMIC_SERVER_TYPE_URL: &str =
    "type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.DynamicMcpServer";

const SUPPORTED_TYPE_URLS: &[&str] = &[MCP_TOOL_TYPE_URL, MCP_DYNAMIC_SERVER_TYPE_URL];

static MCP_XDS_HANDLER: OnceLock<Arc<McpXdsHandler>> = OnceLock::new();

pub fn init_mcp_xds_handler(subscriber: Arc<DeltaDiscoverySubscriptionManager>) -> Arc<McpXdsHandler> {
    Arc::clone(MCP_XDS_HANDLER.get_or_init(|| Arc::new(McpXdsHandler::new(subscriber))))
}

pub fn get_mcp_xds_handler() -> Option<&'static Arc<McpXdsHandler>> {
    MCP_XDS_HANDLER.get()
}

pub fn subscribe_for_updates(scope: SmolStr, registry: Arc<ToolsRegistry>) {
    match get_mcp_xds_handler() {
        Some(handler) => handler.register(scope, registry),
        None => {
            debug!(target: "mcp_gateway", "xDS handler not initialized; MCP filter '{scope}' will not receive TDS updates")
        },
    }
}

pub fn unsubscribe_from_updates(scope: &str, registry: &Arc<ToolsRegistry>) {
    if let Some(handler) = get_mcp_xds_handler() {
        handler.unregister(scope, registry);
    }
}

#[derive(Debug)]
pub struct McpXdsHandler {
    // `Weak` so dropped registries self-clean without explicit unregister.
    registries: DashMap<SmolStr, Vec<Weak<ToolsRegistry>>, ahash::RandomState>,
    subscriber: Arc<DeltaDiscoverySubscriptionManager>,
    subscribe_once: Once,
}

impl McpXdsHandler {
    pub fn new(subscriber: Arc<DeltaDiscoverySubscriptionManager>) -> Self {
        Self { registries: DashMap::with_hasher(ahash::RandomState::new()), subscriber, subscribe_once: Once::new() }
    }

    pub fn register(&self, scope: SmolStr, registry: Arc<ToolsRegistry>) {
        debug!(target: "mcp_gateway", "Registering TDS registry for scope: {scope}");
        self.registries.entry(scope).or_default().push(Arc::downgrade(&registry));
        self.try_subscribe();
    }

    fn try_subscribe(&self) {
        self.subscribe_once.call_once(|| {
            let sub = Arc::clone(&self.subscriber);
            tokio::spawn(async move {
                for url in SUPPORTED_TYPE_URLS {
                    if let Err(e) = sub.subscribe("*".to_owned(), TypeUrl::Extension((*url).to_string())).await {
                        warn!(target: "mcp_gateway", "Failed to subscribe to MCP extension type URL {url}: {e}");
                    } else {
                        debug!(target: "mcp_gateway", "Subscribed to MCP extension type URL {url}");
                    }
                }
            });
        });
    }

    pub fn unregister(&self, scope: &str, registry: &Arc<ToolsRegistry>) {
        let Some(mut entry) = self.registries.get_mut(scope) else { return };
        entry.retain(|weak| match weak.upgrade() {
            Some(arc) => !Arc::ptr_eq(&arc, registry),
            None => false,
        });
        let empty = entry.is_empty();
        drop(entry);
        if empty {
            self.registries.remove(scope);
        }
        debug!(target: "mcp_gateway", "Unregistered TDS registry for scope: {scope}");
    }

    // Split `"{server_name}/{config_name}/{resource_name}"` into
    // `(scope = "{server_name}/{config_name}", resource_name)`. Resource
    // name may contain `/`; server_name and config_name may not (enforced
    // at config parse time), so the boundary is unambiguous.
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

    fn live_registries(&self, scope: &str) -> Vec<Arc<ToolsRegistry>> {
        let mut out = Vec::new();
        if let Some(mut entry) = self.registries.get_mut(scope) {
            entry.retain(|weak| match weak.upgrade() {
                Some(arc) => {
                    out.push(arc);
                    true
                },
                None => false,
            });
        }
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
                    let tool: McpTool = convert(proto, "MCP Tool")?;
                    #[cfg(feature = "mcp-semantic-search")]
                    let tool_name: smol_str::SmolStr = tool.name.clone();
                    for registry in &registries {
                        registry.add_tool(tool.clone()).map_err(|e| {
                            XdsExtensionError::HandlerError(format!("Failed to add tool '{name}': {e}"))
                        })?;
                    }
                    debug!(target: "mcp_gateway", "xDS: added/updated tool '{name}' in scope '{scope}' ({} registries)", registries.len());
                    #[cfg(feature = "mcp-semantic-search")]
                    if tool.embedding.is_empty() {
                        for registry in &registries {
                            registry.embed_tool_if_unembedded(&tool_name).await;
                        }
                    }
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
    use super::*;
    use orion_xds::xds::client::{DeltaDiscoverySubscriptionManager, SubscriptionEvent};
    use tokio::sync::mpsc;

    fn empty_registry() -> Arc<ToolsRegistry> {
        Arc::new(
            ToolsRegistry::with_config(
                Vec::new(),
                Vec::new(),
                None,
                #[cfg(feature = "mcp-semantic-search")]
                None,
            )
            .unwrap(),
        )
    }

    fn test_sub_mgr() -> (Arc<DeltaDiscoverySubscriptionManager>, mpsc::Receiver<SubscriptionEvent>) {
        let (tx, rx) = mpsc::channel::<SubscriptionEvent>(8);
        (Arc::new(DeltaDiscoverySubscriptionManager::from_sender(tx)), rx)
    }

    fn stub_handler() -> McpXdsHandler {
        let (sub, _rx) = test_sub_mgr();
        McpXdsHandler::new(sub)
    }

    fn scope_len(handler: &McpXdsHandler, scope: &str) -> usize {
        handler.registries.get(scope).map_or(0, |entry| entry.len())
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

    #[tokio::test]
    async fn register_and_unregister_specific_registry() {
        let handler = stub_handler();
        let scope: SmolStr = "srv/cfg".into();
        let r1 = empty_registry();
        let r2 = empty_registry();
        handler.register(scope.clone(), Arc::clone(&r1));
        handler.register(scope.clone(), Arc::clone(&r2));
        assert_eq!(scope_len(&handler, &scope), 2);

        handler.unregister(&scope, &r1);
        assert_eq!(scope_len(&handler, &scope), 1);
        assert!(handler.live_registries(&scope).iter().all(|a| Arc::ptr_eq(a, &r2)));

        handler.unregister(&scope, &r2);
        assert!(!handler.registries.contains_key(scope.as_str()));
    }

    #[tokio::test]
    async fn live_registries_prunes_dead_weaks() {
        let handler = stub_handler();
        let scope: SmolStr = "srv/cfg".into();
        let r1 = empty_registry();
        let r2 = empty_registry();
        handler.register(scope.clone(), Arc::clone(&r1));
        handler.register(scope.clone(), Arc::clone(&r2));

        drop(r1);
        let live = handler.live_registries(&scope);
        assert_eq!(live.len(), 1);
        assert!(Arc::ptr_eq(&live[0], &r2));
        assert_eq!(scope_len(&handler, &scope), 1);
    }

    #[tokio::test]
    async fn unknown_scope_update_is_noop() {
        let handler = stub_handler();
        // No scope registered — handle_update must not error and must not touch storage.
        let res = handler.handle_update(MCP_TOOL_TYPE_URL, "srv/cfg/tool_x", &[]).await;
        assert!(res.is_ok());
        assert!(handler.registries.is_empty());
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

        handler.register("srv/cfg".into(), empty_registry());
        let urls = drain_subscribes(&mut rx).await;
        assert_eq!(urls.len(), SUPPORTED_TYPE_URLS.len());
        for expected in SUPPORTED_TYPE_URLS {
            assert!(urls.iter().any(|u| u == expected));
        }

        handler.register("srv/cfg2".into(), empty_registry());
        assert!(drain_subscribes(&mut rx).await.is_empty());
    }
}
