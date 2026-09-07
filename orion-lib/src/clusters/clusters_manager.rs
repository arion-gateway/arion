// Copyright 2025 The kmesh Authors
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

use triomphe::Arc;
use super::{
    balancers::hash_policy::HashState,
    cached_watch::{CachedWatch, CachedWatcher},
    cluster::ClusterType,
    health::HealthStatus,
    load_assignment::{ClusterLoadAssignmentBuilder, PartialClusterLoadAssignment},
};
use crate::{
    clusters::cluster::{original_dst::DynamicDest, ClusterOps, PartialClusterType},
    secrets::TransportSecret,
    transport::{GrpcService, HttpChannel, HttpChannels, TcpChannelConnector},
    OrionRequestBody, Result,
};
use http::{header::HOST, uri::Authority, HeaderMap, HeaderName, HeaderValue, Request};
use orion_configuration::config::cluster::{Cluster as ClusterConfig, ClusterSpecifier};
use orion_interner::StringInterner;
use rand::{prelude::SliceRandom, thread_rng};
use smol_str::SmolStr;
use std::{
    borrow::Cow,
    cell::RefCell,
    collections::{btree_map::Entry as BTreeEntry, BTreeMap},
    };
use tracing::{debug, warn};

type ClusterID = &'static str;
type ClustersMap = BTreeMap<ClusterID, ClusterType>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MetadataKey(pub SmolStr);

#[derive(Debug, Clone, PartialEq)]
pub enum RoutingRequirement {
    None,
    Header(HeaderName),
    MetadataKey(MetadataKey),
    Authority,
    Hash,
    OverrideHost { header: HeaderName, fallback_requires_hash: bool },
}

pub enum RoutingContext<'a> {
    None,
    Header(&'a HeaderValue),
    DynamicDest(&'a DynamicDest),
    Authority(Cow<'a, Authority>),
    Hash(HashState<'a>),
    OverrideHost { header: &'a HeaderValue, fallback_hash: Option<HashState<'a>> },
}

impl<'a> From<&'a Authority> for RoutingContext<'a> {
    fn from(authority: &'a Authority) -> Self {
        Self::Authority(Cow::Borrowed(authority))
    }
}

struct RoutingAuthority<'a>(Cow<'a, Authority>);

impl<'a> From<&'a Authority> for RoutingAuthority<'a> {
    fn from(authority: &'a Authority) -> Self {
        Self(Cow::Borrowed(authority))
    }
}

impl<'a, B> TryFrom<&'a Request<B>> for RoutingAuthority<'a> {
    type Error = RoutingContextError;

    fn try_from(request: &'a Request<B>) -> std::result::Result<Self, Self::Error> {
        if let Some(authority) = request.uri().authority() {
            return Ok(authority.into());
        }

        let host = request.headers().get(HOST).ok_or(RoutingContextError::MissingAuthority)?;
        let authority = Authority::try_from(host.as_bytes()).map_err(RoutingContextError::InvalidAuthority)?;
        Ok(Self(Cow::Owned(authority)))
    }
}

impl<'a> From<RoutingAuthority<'a>> for RoutingContext<'a> {
    fn from(authority: RoutingAuthority<'a>) -> Self {
        Self::Authority(authority.0)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RoutingContextError {
    #[error("Missing metadata key DynamicDest for ORIGINAL_DST cluster")]
    MissingMetadataKey,
    #[error("Missing required header '{0}' for ORIGINAL_DST cluster")]
    MissingHeader(HeaderName),
    #[error("Missing required request authority for ORIGINAL_DST cluster")]
    MissingAuthority,
    #[error("Invalid request authority: {0}")]
    InvalidAuthority(#[source] http::uri::InvalidUri),
}

impl<'a> TryFrom<(&'a RoutingRequirement, &'a Request<OrionRequestBody>, HashState<'a>)> for RoutingContext<'a> {
    type Error = RoutingContextError;

    fn try_from(
        value: (&'a RoutingRequirement, &'a Request<OrionRequestBody>, HashState<'a>),
    ) -> std::result::Result<Self, Self::Error> {
        let (routing_requirement, request, hash_state) = value;
        match routing_requirement {
            RoutingRequirement::OverrideHost { header, fallback_requires_hash } => {
                debug!("RoutingContext via override host header: {}", header);
                if let Some(header_value) = request.headers().get(header) {
                    let fallback_hash = fallback_requires_hash.then_some(hash_state);
                    Ok(RoutingContext::OverrideHost { header: header_value, fallback_hash })
                } else if *fallback_requires_hash {
                    Ok(RoutingContext::Hash(hash_state))
                } else {
                    Ok(RoutingContext::None)
                }
            },
            RoutingRequirement::MetadataKey(key) => {
                debug!("RoutingContext via metadata key: {}", key.0);
                // it doesn't matter the value of the key, we always use the dynamic destination
                let dynamic_dest =
                    request.extensions().get::<DynamicDest>().ok_or_else(|| RoutingContextError::MissingMetadataKey)?;
                Ok(RoutingContext::DynamicDest(dynamic_dest))
            },
            RoutingRequirement::Header(header_name) => {
                debug!("RoutingContext via header name: {}", header_name);
                let header_value = request
                    .headers()
                    .get(header_name)
                    .ok_or_else(|| RoutingContextError::MissingHeader(header_name.clone()))?;
                Ok(RoutingContext::Header(header_value))
            },
            RoutingRequirement::Authority => {
                debug!("RoutingContext via request authority");
                RoutingAuthority::try_from(request).map(Into::into)
            },
            RoutingRequirement::Hash => {
                debug!("RoutingContext via hash: {:?}", hash_state);
                Ok(RoutingContext::Hash(hash_state))
            },
            RoutingRequirement::None => {
                debug!("RoutingContext with no requirements");
                Ok(RoutingContext::None)
            },
        }
    }
}

static CLUSTERS_MAP: CachedWatch<ClustersMap> = CachedWatch::new(ClustersMap::new());

thread_local! {
    static CLUSTERS_MAP_CACHE : RefCell<CachedWatcher<'static, ClustersMap>> = RefCell::new(CLUSTERS_MAP.watcher());
}

pub fn resolve_cluster(selector: &ClusterSpecifier, header_map: Option<&HeaderMap>) -> Option<ClusterID> {
    debug!("Resolving cluster: {:?} with header map: {:?}", selector, header_map);
    match selector {
        ClusterSpecifier::Cluster(cluster_name) => Some(cluster_name.to_static_str()),
        ClusterSpecifier::WeightedCluster(weighted_clusters) => weighted_clusters
            .choose_weighted(&mut thread_rng(), |cluster| u32::from(cluster.weight))
            .ok()
            .map(|cluster| cluster.cluster.to_static_str()),
        ClusterSpecifier::ClusterHeader(name) => {
            debug!("Resolving cluster header '{}'...", name);
            if let Some(header_map) = header_map {
                if let Some(header_value) = header_map.get(name.as_str()) {
                    header_value.to_str().ok().map(|s| s.to_static_str())
                } else {
                    debug!("Header '{}' not found in the request/header map (no cluster found)", name);
                    None
                }
            } else {
                debug!("No header map provided (no cluster found)");
                None
            }
        },
    }
}

pub fn get_cluster_routing_requirements(cluster_id: ClusterID) -> RoutingRequirement {
    with_cluster(cluster_id, |cluster| Ok(cluster.get_routing_requirements())).unwrap_or(RoutingRequirement::None)
}

pub fn change_cluster_load_assignment(name: &str, cla: &PartialClusterLoadAssignment) -> Result<ClusterType> {
    CLUSTERS_MAP.update(|current| {
        if let Some(cluster) = current.get_mut(name) {
            match cluster {
                ClusterType::Dynamic(dynamic_cluster) => {
                    let cla = ClusterLoadAssignmentBuilder::builder()
                        .with_cla(cla.clone())
                        .with_transport_socket(dynamic_cluster.transport_socket.clone())
                        .with_cluster_name(dynamic_cluster.global.name)
                        .with_bind_device(dynamic_cluster.global.bind_device.clone())
                        .with_lb_policy(dynamic_cluster.global.load_balancing_policy.clone())
                        .prepare();
                    cla.build().map(|cla| dynamic_cluster.change_load_assignment(Some(cla)))?;
                    Ok(cluster.clone())
                },
                ClusterType::Static(_) => {
                    let msg = format!("{name} Attempt to change CLA for static cluster");
                    warn!(msg);
                    Err(msg.into())
                },
                ClusterType::OnDemand(_) => {
                    let msg = format!("{name} Attempt to change CLA for ORIGINAL_DST cluster");
                    warn!(msg);
                    Err(msg.into())
                },
            }
        } else {
            let msg = format!("{name} No cluster found");
            warn!(msg);
            Err(msg.into())
        }
    })
}

pub fn remove_cluster_load_assignment(name: &str) -> Result<()> {
    CLUSTERS_MAP.update(|current| {
        let maybe_cluster = current.get_mut(name);
        if let Some(cluster) = maybe_cluster {
            match cluster {
                ClusterType::Dynamic(cluster) => {
                    cluster.change_load_assignment(None);
                    Ok(())
                },
                ClusterType::Static(_) => {
                    let msg = format!("{name} Attempt to change CLA for static cluster");
                    warn!(msg);
                    Err(msg.into())
                },
                ClusterType::OnDemand(_) => {
                    let msg = format!("{name} Attempt to change CLA for ORIGINAL_DST cluster");
                    warn!(msg);
                    Err(msg.into())
                },
            }
        } else {
            let msg = format!("{name} No cluster found");
            warn!(msg);
            Err(msg.into())
        }
    })
}

pub fn update_endpoint_health(cluster: &str, endpoint: &Authority, health: HealthStatus) {
    CLUSTERS_MAP.update(|current| {
        if let Some(cluster) = current.get_mut(cluster) {
            cluster.update_health(endpoint, health);
        }
    });
}

pub fn update_tls_context(secret_id: &str, secret: &TransportSecret) -> Result<Vec<ClusterType>> {
    CLUSTERS_MAP.update(|current| {
        let mut cluster_configs = Vec::with_capacity(current.len());
        for cluster in current.values_mut() {
            cluster.change_tls_context(secret_id, secret.clone())?;
            cluster_configs.push(cluster.clone());
        }
        Ok(cluster_configs)
    })
}

pub fn add_cluster(partial_cluster: PartialClusterType) -> Result<ClusterType> {
    let cluster_name = partial_cluster.get_name();

    CLUSTERS_MAP.update(|current| match current.entry(cluster_name) {
        BTreeEntry::Vacant(entry) => {
            let cluster = partial_cluster.build(None, None)?;
            entry.insert(cluster.clone());
            Ok(cluster)
        },
        BTreeEntry::Occupied(mut entry) => {
            let counters = entry
                .get()
                .circuit_breaker()
                .map(|cb| (Arc::clone(&cb.default_priority.counters), Arc::clone(&cb.high_priority.counters)))
                .unzip();
            let cluster = partial_cluster.build(counters.0, counters.1)?;
            *(entry.get_mut()) = cluster.clone();
            Ok(cluster)
        },
    })
}

pub fn remove_cluster(cluster_name: &str) -> Result<()> {
    CLUSTERS_MAP.update(|current| current.remove(cluster_name).map(|_| ()).ok_or("No such cluster".into()))
}

pub fn get_all_clusters() -> Vec<ClusterConfig> {
    CLUSTERS_MAP.get_clone().0.values().by_ref().filter_map(|cluster| ClusterConfig::try_from(cluster).ok()).collect()
}

pub fn get_http_connection(cluster_id: ClusterID, context: RoutingContext) -> Result<HttpChannels> {
    with_cluster(cluster_id, |cluster| cluster.get_http_connection(context))
}

pub fn get_tcp_connection(cluster_id: ClusterID, context: RoutingContext) -> Result<TcpChannelConnector> {
    with_cluster(cluster_id, |cluster| cluster.get_tcp_connection(context))
}

pub fn get_grpc_connection(cluster_id: ClusterID, context: RoutingContext) -> Result<GrpcService> {
    with_cluster(cluster_id, |cluster| cluster.get_grpc_connection(context))
}

pub fn all_http_connections(cluster_id: ClusterID) -> Result<Vec<(Authority, HttpChannel)>> {
    with_cluster(cluster_id, |cluster| Ok(cluster.all_http_channels()))
}

pub fn all_tcp_connections(cluster_id: ClusterID) -> Result<Vec<(Authority, TcpChannelConnector)>> {
    with_cluster(cluster_id, |cluster| Ok(cluster.all_tcp_channels()))
}

pub fn all_grpc_connections(cluster_id: ClusterID) -> Result<Vec<Result<(Authority, GrpcService)>>> {
    with_cluster(cluster_id, |cluster| Ok(cluster.all_grpc_channels()))
}

fn with_cluster<F, R>(cluster_id: &str, f: F) -> Result<R>
where
    F: FnOnce(&mut ClusterType) -> Result<R>,
{
    CLUSTERS_MAP_CACHE.with_borrow_mut(|watcher| {
        if let Some(cluster) = watcher.cached_or_latest().get_mut(cluster_id) {
            f(cluster)
        } else {
            Err(format!("Cluster {cluster_id} not found").into())
        }
    })
}

pub use super::circuit_breaker::{CircuitBreakerDenial, RoutingPriority};

pub fn try_increment_connections(
    cluster_id: ClusterID,
    priority: RoutingPriority,
) -> std::result::Result<(), CircuitBreakerDenial> {
    CLUSTERS_MAP_CACHE.with_borrow_mut(|watcher| {
        if let Some(cluster) = watcher.cached_or_latest().get_mut(cluster_id) {
            if let Some(cb) = cluster.circuit_breaker() {
                return cb.try_increment_connections(priority);
            }
        }
        Ok(())
    })
}

pub fn decrement_connections(cluster_id: ClusterID, priority: RoutingPriority) {
    CLUSTERS_MAP_CACHE.with_borrow_mut(|watcher| {
        if let Some(cluster) = watcher.cached_or_latest().get_mut(cluster_id) {
            if let Some(cb) = cluster.circuit_breaker() {
                cb.decrement_connections(priority);
            }
        }
    });
}

pub fn try_increment_requests(
    cluster_id: ClusterID,
    priority: RoutingPriority,
) -> std::result::Result<(), CircuitBreakerDenial> {
    CLUSTERS_MAP_CACHE.with_borrow_mut(|watcher| {
        if let Some(cluster) = watcher.cached_or_latest().get_mut(cluster_id) {
            if let Some(cb) = cluster.circuit_breaker() {
                return cb.try_increment_requests(priority);
            }
        }
        Ok(())
    })
}

pub fn decrement_requests(cluster_id: ClusterID, priority: RoutingPriority) {
    CLUSTERS_MAP_CACHE.with_borrow_mut(|watcher| {
        if let Some(cluster) = watcher.cached_or_latest().get_mut(cluster_id) {
            if let Some(cb) = cluster.circuit_breaker() {
                cb.decrement_requests(priority);
            }
        }
    });
}

pub fn try_increment_retries(
    cluster_id: ClusterID,
    priority: RoutingPriority,
) -> std::result::Result<(), CircuitBreakerDenial> {
    CLUSTERS_MAP_CACHE.with_borrow_mut(|watcher| {
        if let Some(cluster) = watcher.cached_or_latest().get_mut(cluster_id) {
            if let Some(cb) = cluster.circuit_breaker() {
                return cb.try_increment_retries(priority);
            }
        }
        Ok(())
    })
}

pub fn decrement_retries(cluster_id: ClusterID, priority: RoutingPriority) {
    CLUSTERS_MAP_CACHE.with_borrow_mut(|watcher| {
        if let Some(cluster) = watcher.cached_or_latest().get_mut(cluster_id) {
            if let Some(cb) = cluster.circuit_breaker() {
                cb.decrement_retries(priority);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::secrets::SecretManager;
    use orion_configuration::config::cluster::{
        CircuitBreakerThresholds, CircuitBreakers, ClusterDiscoveryType, LbPolicy, OriginalDstConfig,
        OriginalDstRoutingMethod, StandardLbPolicy,
    };
    use orion_configuration::config::cluster::{Cluster as ClusterConfig, HttpProtocolOptions};
    use std::sync::atomic::Ordering;

    fn make_cluster_config(name: &str, max_requests: u32) -> ClusterConfig {
        ClusterConfig {
            name: name.into(),
            discovery_settings: ClusterDiscoveryType::OriginalDst(OriginalDstConfig {
                routing_method: OriginalDstRoutingMethod::HttpHeader { http_header_name: None },
                upstream_port_override: None,
            }),
            cleanup_interval: None,
            transport_socket: None,
            bind_device: None,
            load_balancing_policy: LbPolicy::Standard(StandardLbPolicy::ClusterProvided),
            http_protocol_options: HttpProtocolOptions::default(),
            health_check: None,
            connect_timeout: None,
            circuit_breakers: Some(CircuitBreakers {
                thresholds: vec![CircuitBreakerThresholds { max_requests, ..Default::default() }],
            }),
        }
    }

    fn build_partial(config: ClusterConfig) -> PartialClusterType {
        let secrets = SecretManager::new();
        PartialClusterType::try_from((Box::new(config), &secrets)).unwrap()
    }

    #[test]
    fn circuit_breaker_survives_cluster_replacement() {
        let name = "cb-replace-test";
        let partial = build_partial(make_cluster_config(name, 10));
        let cluster = add_cluster(partial).unwrap();

        let cb = cluster.circuit_breaker().expect("CB should be present");
        cb.try_increment_requests(RoutingPriority::Default).unwrap();
        let counter = cb.get_state(RoutingPriority::Default).counters.active_requests.load(Ordering::Relaxed);
        assert_eq!(counter, 1);

        let partial2 = build_partial(make_cluster_config(name, 20));
        let replaced = add_cluster(partial2).unwrap();

        let cb_after = replaced.circuit_breaker().expect("CB should be present after replacement");
        let counter_after =
            cb_after.get_state(RoutingPriority::Default).counters.active_requests.load(Ordering::Relaxed);
        assert_eq!(counter_after, 1, "replacement must preserve in-flight counter");

        cb_after.decrement_requests(RoutingPriority::Default);
        let counter_final =
            cb_after.get_state(RoutingPriority::Default).counters.active_requests.load(Ordering::Relaxed);
        assert_eq!(counter_final, 0, "decrement after replacement must reach zero");

        remove_cluster(name).unwrap();
    }

    #[test]
    fn simulated_race_increment_replace_decrement() {
        let name = "cb-race-test";
        let partial = build_partial(make_cluster_config(name, 5));
        add_cluster(partial).unwrap();

        try_increment_requests(name, RoutingPriority::Default).unwrap();

        let partial2 = build_partial(make_cluster_config(name, 5));
        add_cluster(partial2).unwrap();

        decrement_requests(name, RoutingPriority::Default);

        CLUSTERS_MAP_CACHE.with_borrow_mut(|watcher| {
            let cluster = watcher.cached_or_latest().get_mut(name).unwrap();
            let cb = cluster.circuit_breaker().unwrap();
            let counter = cb.get_state(RoutingPriority::Default).counters.active_requests.load(Ordering::Relaxed);
            assert_eq!(counter, 0, "counter must be zero, not underflowed to u32::MAX");
        });

        remove_cluster(name).unwrap();
    }

    #[test]
    fn replacement_inherits_thresholds_from_new_config() {
        let name = "cb-threshold-test";
        let partial = build_partial(make_cluster_config(name, 5));
        add_cluster(partial).unwrap();

        let partial2 = build_partial(make_cluster_config(name, 20));
        let replaced = add_cluster(partial2).unwrap();

        let cb = replaced.circuit_breaker().unwrap();
        let thresholds = &cb.get_state(RoutingPriority::Default).thresholds;
        assert_eq!(thresholds.max_requests, 20, "preserved CB adopts new thresholds");

        remove_cluster(name).unwrap();
    }

    #[test]
    fn first_add_does_not_panic() {
        let name = "cb-first-add-test";
        let partial = build_partial(make_cluster_config(name, 10));
        let cluster = add_cluster(partial).unwrap();

        let cb = cluster.circuit_breaker().expect("CB should exist on first add");
        assert_eq!(cb.get_state(RoutingPriority::Default).counters.active_requests.load(Ordering::Relaxed), 0);

        remove_cluster(name).unwrap();
    }
}
