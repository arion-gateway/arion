// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

use abort_on_drop::ChildTask;
use anyhow::Result;
use arion_configuration::config::{Listener, bootstrap::Node, cluster::ClusterSpecifier};
use arion_interner::StringInterner;
use arion_lib::{
    ConfigurationSenders, ConversionContext, EndpointHealthUpdate, HealthCheckManager, ListenerConfigurationChange,
    ListenerFactory, PartialClusterLoadAssignment, PartialClusterType, RouteConfigurationChange, SecretManager,
    access_log::{Target, update_configuration},
    clusters::cluster::ClusterType,
};
use arion_xds::{
    start_aggregate_client_no_retry_loop,
    xds::{
        bindings::AggregatedDiscoveryType,
        client::{
            DeltaClientBackgroundWorker, DeltaDiscoveryClient, DeltaDiscoverySubscriptionManager, XdsUpdateEvent,
        },
        extension::XdsExtensionHandler,
        model::{RejectedConfig, TypeUrl, XdsResourcePayload, XdsResourceUpdate},
    },
};
use futures::future::join_all;
use parking_lot::RwLock;
use pingora_timeout::fast_timeout::fast_timeout;
use smol_str::SmolStr;
#[cfg(feature = "tracing")]
use smol_str::ToSmolStr;
use std::{sync::Arc as StdArc, time::Duration};
use tokio::{
    select,
    sync::{
        Notify,
        mpsc::{self, Receiver, Sender},
    },
};
use tracing::{debug, info, warn};
use triomphe::Arc;

const RETRY_INTERVAL: Duration = Duration::from_secs(10);
const ROUTE_UPDATE_TIMEOUT: Duration = Duration::from_secs(5);

pub struct XdsConfigurationHandler {
    secret_manager: Arc<RwLock<SecretManager>>,
    health_manager: HealthCheckManager,
    listeners_senders: Vec<Sender<ListenerConfigurationChange>>,
    route_senders: Vec<Sender<RouteConfigurationChange>>,
    health_updates_receiver: Receiver<EndpointHealthUpdate>,
    extension_handlers: Vec<StdArc<dyn XdsExtensionHandler>>,
}

impl XdsConfigurationHandler {
    pub fn new(secret_manager: Arc<RwLock<SecretManager>>, configuration_senders: Vec<ConfigurationSenders>) -> Self {
        let mut listeners_senders = Vec::with_capacity(configuration_senders.len());
        let mut route_senders = Vec::with_capacity(configuration_senders.len());
        for ConfigurationSenders { listener_configuration_sender, route_configuration_sender } in configuration_senders
        {
            listeners_senders.push(listener_configuration_sender);
            route_senders.push(route_configuration_sender);
        }
        let (health_updates_sender, health_updates_receiver) = mpsc::channel(1000);
        let health_manager = HealthCheckManager::new(health_updates_sender);
        Self {
            secret_manager,
            health_manager,
            listeners_senders,
            route_senders,
            health_updates_receiver,
            extension_handlers: Vec::new(),
        }
    }

    pub fn with_extension_handlers(mut self, handlers: Vec<StdArc<dyn XdsExtensionHandler>>) -> Self {
        self.extension_handlers = handlers;
        self
    }

    // Resolve cluster name into working endpoint(s), return working client
    fn resolve_endpoints(
        cluster_name: &str,
        node: &Node,
    ) -> Result<(
        DeltaClientBackgroundWorker<AggregatedDiscoveryType<arion_lib::clusters::SimpleRoundRobinGrpcServiceLB>>,
        DeltaDiscoveryClient,
        DeltaDiscoverySubscriptionManager,
    )> {
        let selector = ClusterSpecifier::Cluster(cluster_name.to_interned_str());
        let cluster_id = arion_lib::clusters::resolve_cluster(&selector, None)
            .ok_or_else(|| anyhow::anyhow!("Failed to resolve cluster {cluster_name} from specifier"))?;
        let grpc_connections = match arion_lib::clusters::all_grpc_connections(cluster_id.as_str()) {
            Ok(connections) => connections,
            Err(err) => {
                let msg = anyhow::anyhow!("Failed to get gRPC connections from cluster ({cluster_name}): {err}");
                warn!("{msg}");
                return Err(anyhow::anyhow!(msg));
            },
        };
        let grpc_services: Vec<arion_lib::clusters::GrpcService> = grpc_connections
            .into_iter()
            .filter_map(|result| match result {
                Ok((_, grpc_service)) => Some(grpc_service),
                Err(err) => {
                    let msg = format!("Skipping (failed) gRPC endpoint for cluster ({cluster_name}): {err}");
                    warn!("{msg}");
                    None
                },
            })
            .collect();

        if grpc_services.is_empty() {
            let msg = format!("Failed to locate any gRPC connections for cluster ({cluster_name})");
            warn!("{msg}");
            Err(anyhow::anyhow!(msg))
        } else {
            let grpc_service_lb = arion_lib::clusters::SimpleRoundRobinGrpcServiceLB::new(grpc_services);
            start_aggregate_client_no_retry_loop(node.clone(), grpc_service_lb)
                .inspect_err(|e| warn!("Failed to connect to xDS server ({cluster_name}): {e}"))
                .map_err(Into::into)
        }
    }

    pub async fn connect(
        node: &Node,
        ads_cluster_names: Vec<String>,
    ) -> Result<Option<(DeltaDiscoveryClient, StdArc<DeltaDiscoverySubscriptionManager>, ChildTask<()>)>> {
        if ads_cluster_names.is_empty() {
            info!("No xDS clusters configured");
            return Ok(None);
        }

        let mut cluster_names = ads_cluster_names.into_iter().cycle();
        let (mut worker, client, subscription_manager) = loop {
            let cluster_name = cluster_names.next().unwrap_or_else(|| unreachable!("cycle over non-empty vec"));
            if let Ok(val) = Self::resolve_endpoints(&cluster_name, node) {
                break val;
            }
            info!("Retrying XDS connection in {} seconds", RETRY_INTERVAL.as_secs());
            tokio::time::sleep(RETRY_INTERVAL).await;
        };

        let worker_task: ChildTask<_> = tokio::spawn(async move {
            let subscribe = worker.run().await;
            info!("Worker exited {subscribe:?}");
        })
        .into();

        Ok(Some((client, StdArc::new(subscription_manager), worker_task)))
    }

    pub async fn run_loop(
        &mut self,
        initial_clusters: Vec<ClusterType>,
        mut client: DeltaDiscoveryClient,
    ) -> Result<()> {
        for cluster in initial_clusters {
            self.health_manager.restart_cluster(cluster).await;
        }

        loop {
            select! {
                Some(xds_update) = client.recv() => {
                    info!("Got notification {xds_update:?}");
                    let XdsUpdateEvent { ack_channel, updates } = xds_update;
                    // Box::pin because the future from self.process_updates() is very large
                    let rejected_updates = Box::pin(self.process_updates(updates)).await;
                    let _ = ack_channel.send(rejected_updates).ok();
                },
                Some(health_update) = self.health_updates_receiver.recv() => Self::process_health_event(&health_update),
                else => break,
            }
        }

        self.health_manager.stop_all().await;
        Ok(())
    }

    async fn process_updates(&mut self, updates: Vec<XdsResourceUpdate>) -> Vec<RejectedConfig> {
        let mut rejected_updates = Vec::new();
        for update in updates {
            match update {
                XdsResourceUpdate::Update(id, resource, _) => {
                    if let Err(e) = self.process_update_event(&id, resource).await {
                        rejected_updates.push(RejectedConfig::from((id, e)));
                    }
                },
                XdsResourceUpdate::Remove(id, resource) => {
                    if let Err(e) = self.process_remove_event(&id, resource).await {
                        rejected_updates.push(RejectedConfig::from((id, e)));
                    }
                },
            }
        }
        rejected_updates
    }

    async fn process_remove_event(&mut self, id: &str, resource: TypeUrl) -> Result<()> {
        match resource {
            arion_xds::xds::model::TypeUrl::Cluster => {
                arion_lib::clusters::remove_cluster(id)?;
                self.health_manager.stop_cluster(id).await;
                Ok(())
            },
            arion_xds::xds::model::TypeUrl::Listener => {
                let change = ListenerConfigurationChange::Removed(id.into());
                let _ = send_change_to_runtimes(&self.listeners_senders, change).await.ok();
                // remove access logs configuration...
                self.access_log_listener_remove(id).await;
                // remove tracer configuration...
                #[cfg(feature = "tracing")]
                Self::tracer_listener_remove(id);
                Ok(())
            },
            arion_xds::xds::model::TypeUrl::ClusterLoadAssignment => {
                arion_lib::clusters::remove_cluster_load_assignment(id)?;
                self.health_manager.stop_cluster(id).await;
                Ok(())
            },
            arion_xds::xds::model::TypeUrl::RouteConfiguration => {
                let notify = Arc::new(Notify::new());
                let change = RouteConfigurationChange::Removed(id.into(), Some(Arc::clone(&notify)));
                let _ = send_change_to_runtimes(&self.route_senders, change).await.ok();
                match fast_timeout(ROUTE_UPDATE_TIMEOUT, notify.notified()).await {
                    Ok(()) => Ok(()),
                    Err(_) => Err(anyhow::anyhow!(
                        "RouteConfiguration '{id}' removal timed-out waiting to be applied by runtime(s)"
                    )),
                }
            },
            arion_xds::xds::model::TypeUrl::Secret => {
                // Like Envoy, listeners and clusters keep the TLS context they built until they are updated.
                self.secret_manager.write().remove(id);
                Ok(())
            },
            TypeUrl::Extension(ref type_url) => {
                debug!("Got extension removal for {type_url} resource {id}");
                self.handle_extension_remove(type_url, id).await
            },
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn process_update_event(&mut self, _: &str, resource: Box<XdsResourcePayload>) -> Result<()> {
        match *resource {
            XdsResourcePayload::Listener(id, listener) => {
                debug!("Got update for listener {id} {:?}", listener);
                let factory =
                    ListenerFactory::try_from(ConversionContext::new((listener.clone(), &*self.secret_manager.read())));

                match factory {
                    Ok(factory) => {
                        let change = ListenerConfigurationChange::Added(Box::new((factory, listener.clone())));
                        let _ = send_change_to_runtimes(&self.listeners_senders, change).await.ok();
                        // update access logs configuration...
                        self.access_log_listener_update(&id, &listener).await;

                        // update tracer configuration...
                        #[cfg(feature = "tracing")]
                        Self::tracer_listener_update(&id, &listener);
                        Ok(())
                    },
                    Err(err) => {
                        warn!("Got invalid update for listener {id}");
                        Err(err.into())
                    },
                }
            },
            XdsResourcePayload::Cluster(id, cluster) => {
                debug!("Got update for cluster: {id}: {:#?}", cluster);
                let cluster_builder = PartialClusterType::try_from((cluster, &*self.secret_manager.read()));
                match cluster_builder {
                    Ok(cluster) => self.add_cluster(cluster).await,
                    Err(err) => {
                        warn!("Got invalid update for cluster {id}");
                        Err(err.into())
                    },
                }
            },
            XdsResourcePayload::RouteConfiguration(id, route) => {
                debug!("Got update for route configuration {id}: {:#?}", route);
                let notify = Arc::new(Notify::new());
                let change = RouteConfigurationChange::Added((SmolStr::new(&id), route), Some(Arc::clone(&notify)));
                let _ = send_change_to_runtimes(&self.route_senders, change).await.ok();
                match fast_timeout(ROUTE_UPDATE_TIMEOUT, notify.notified()).await {
                    Ok(()) => Ok(()),
                    Err(_) => Err(anyhow::anyhow!(
                        "RouteConfiguration '{id}' update timedout waiting to be applied by runtime(s)"
                    )),
                }
            },
            XdsResourcePayload::Endpoints(id, cla) => {
                debug!("Got update for cluster load assignment {id}: {:#?}", cla);
                let cla = PartialClusterLoadAssignment::try_from(cla);

                match cla {
                    Ok(cla) => {
                        let cluster_name = id.clone();
                        let cluster_config = arion_lib::clusters::change_cluster_load_assignment(&cluster_name, &cla)?;
                        self.health_manager.restart_cluster(cluster_config).await;
                        Ok(())
                    },
                    Err(err) => {
                        warn!("Got invalid update for cluster load assignment {id}");
                        Err(err.into())
                    },
                }
            },
            XdsResourcePayload::Secret(id, secret) => {
                debug!("Got update for secret {id}: {:#?}", secret);
                let res = self.secret_manager.write().add(&secret);

                match res {
                    Ok(secret) => {
                        let cluster_configs = arion_lib::clusters::update_tls_context(&id, &secret)?;
                        for cluster_config in cluster_configs {
                            self.health_manager.restart_cluster(cluster_config).await;
                        }
                        let change = ListenerConfigurationChange::TlsContextChanged((SmolStr::new(&id), secret));
                        let _ = send_change_to_runtimes(&self.listeners_senders, change).await.ok();
                        Ok(())
                    },
                    Err(err) => {
                        warn!("Got invalid update for cluster load assignment {id}");
                        Err(err.into())
                    },
                }
            },
            XdsResourcePayload::Extension(id, type_url, payload) => {
                debug!("Got extension update for {type_url} resource {id}");
                self.handle_extension_update(&type_url, &id, &payload).await
            },
        }
    }

    #[cfg(feature = "tracing")]
    fn tracer_listener_update(id: &str, listener: &Listener) {
        arion_tracing::otel_update_tracers(listener.get_tracing_configurations())
            .unwrap_or_else(|err| warn!("Failed to update tracer for listener {id}: {err}"));
    }

    #[cfg(feature = "tracing")]
    fn tracer_listener_remove(id: &str) {
        arion_tracing::otel_remove_tracers_by_listeners(&[id.to_smolstr()])
            .unwrap_or_else(|err| warn!("Failed to remove tracer for listener {id}: {err}"));
    }

    async fn access_log_listener_update(&mut self, id: &str, listener: &Listener) {
        let access_logs = listener.all_access_log_configs();
        for (target, confs) in access_logs {
            if let Err(err) = update_configuration(target.into(), confs).await {
                warn!("Failed to update access log configuration for listener '{}' ({id}): {err}", listener.name);
            }
        }
    }

    async fn access_log_listener_remove(&mut self, id: &str) {
        if let Err(err) = update_configuration(Target::Listener(id.to_interned_str()), vec![]).await {
            warn!("Failed to remove access log configuration for listener {id}: {err}");
        }
    }

    async fn add_cluster(&mut self, cluster: PartialClusterType) -> Result<()> {
        let cluster_config = arion_lib::clusters::add_cluster(cluster)?;
        self.health_manager.restart_cluster(cluster_config).await;
        Ok(())
    }

    async fn handle_extension_update(&self, type_url: &str, resource_id: &str, payload: &[u8]) -> Result<()> {
        for handler in &self.extension_handlers {
            if handler.type_urls().contains(&type_url) {
                if let Err(e) = handler.handle_update(type_url, resource_id, payload).await {
                    warn!("Extension handler error for {type_url}: {e}");
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    async fn handle_extension_remove(&self, type_url: &str, resource_id: &str) -> Result<()> {
        for handler in &self.extension_handlers {
            if handler.type_urls().contains(&type_url) {
                if let Err(e) = handler.handle_remove(type_url, resource_id).await {
                    warn!("Extension handler error for {type_url} removal: {e}");
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }
    fn process_health_event(health_update: &EndpointHealthUpdate) {
        if health_update.changed {
            tracing::info!(
                "Health state changed for endpoint {:?} in cluster {:?} to {:?}",
                health_update.endpoint.endpoint,
                health_update.endpoint.cluster,
                health_update.health
            );
            arion_lib::clusters::update_endpoint_health(
                &health_update.endpoint.cluster,
                &health_update.endpoint.endpoint,
                health_update.health,
            );
        }
    }
}

pub async fn send_change_to_runtimes<Change: Clone>(channels: &[Sender<Change>], change: Change) -> Result<()> {
    let futures: Vec<_> = channels
        .iter()
        .map(|f| {
            let change = change.clone();
            f.send(change)
        })
        .collect();
    let _ = join_all(futures).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arion_configuration::config::secret::Secret;

    const CERTS: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../test_certs/beefcakeCA-gathered");

    fn secrets() -> Vec<Secret> {
        serde_json::from_value(serde_json::json!([
            {
                "name": "server-cert",
                "tls_certificate": {
                    "certificate_chain": { "path": format!("{CERTS}/beefcake-dublin.cert.pem") },
                    "private_key": { "path": format!("{CERTS}/beefcake-dublin.key.pem") },
                },
            },
            {
                "name": "client-ca",
                "validation_context": {
                    "trusted_ca": { "path": format!("{CERTS}/beefcake.intermediate.ca-chain.cert.pem") },
                    "trust_chain_verification": "VerifyTrustChain",
                },
            },
        ]))
        .unwrap()
    }

    fn update(secret: &Secret) -> XdsResourceUpdate {
        let payload = XdsResourcePayload::Secret(secret.name.to_string(), secret.clone());
        XdsResourceUpdate::Update(secret.name.to_string(), Box::new(payload), "1".to_owned())
    }

    fn remove(secret: &Secret) -> XdsResourceUpdate {
        XdsResourceUpdate::Remove(secret.name.to_string(), TypeUrl::Secret)
    }

    #[tokio::test]
    async fn secret_removal_is_acked_and_secret_can_be_added_again() {
        let secret_manager = Arc::new(RwLock::new(SecretManager::new()));
        let mut handler = XdsConfigurationHandler::new(Arc::clone(&secret_manager), vec![]);
        let secrets = secrets();

        assert!(handler.process_updates(secrets.iter().map(update).collect()).await.is_empty());
        assert_eq!(secret_manager.read().get_all_secrets().len(), 2);

        for _ in 0..2 {
            assert!(handler.process_updates(secrets.iter().map(remove).collect()).await.is_empty());
            assert!(secret_manager.read().get_all_secrets().is_empty());
        }

        assert!(handler.process_updates(secrets.iter().map(update).collect()).await.is_empty());
        assert_eq!(secret_manager.read().get_all_secrets().len(), 2);
    }
}
