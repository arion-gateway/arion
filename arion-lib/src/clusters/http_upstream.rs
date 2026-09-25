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

use std::{error::Error, net::SocketAddr};

use arion_configuration::config::{
    cluster::ClusterSpecifier, network_filters::http_connection_manager::route::HashPolicy,
};
use http::Request;

use super::{
    balancers::hash_policy::HashState,
    clusters_manager::{self, RoutingContext, RoutingContextError},
    decrement_requests, try_increment_requests, CircuitBreakerDenial, RoutingPriority,
};
use crate::{transport::HttpChannels, ArionRequestBody};

#[cfg(feature = "metrics")]
use {
    crate::{get_shard_id, with_metric},
    arion_metrics::metrics::clusters,
    opentelemetry::KeyValue,
};

/// An acquired upstream request slot and the channels selected for it.
///
/// The request circuit-breaker permit is released when this value is dropped.
/// Callers should retain it until response headers (or an upstream error) have
/// been returned, matching the lifetime used by the HTTP route path.
#[derive(Debug)]
pub struct AcquiredHttpUpstream {
    channels: HttpChannels,
    cluster_id: &'static str,
    _permit: RequestPermit,
}

impl AcquiredHttpUpstream {
    #[inline]
    pub fn channels(&self) -> &HttpChannels {
        &self.channels
    }

    #[inline]
    pub fn cluster_id(&self) -> &'static str {
        self.cluster_id
    }
}

#[derive(Debug)]
struct RequestPermit {
    cluster_id: &'static str,
    priority: RoutingPriority,
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        decrement_requests(self.cluster_id, self.priority);
    }
}

/// Stable, caller-neutral categories for HTTP upstream acquisition failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireHttpUpstreamErrorKind {
    ClusterNotFound,
    CircuitBreakerOverflow,
    RoutingContext,
    Connection,
}

#[derive(Debug, thiserror::Error)]
pub enum AcquireHttpUpstreamError {
    #[error("failed to resolve upstream cluster")]
    ClusterNotFound,
    #[error("upstream request circuit breaker overflow for cluster '{cluster_id}': {denial:?}")]
    CircuitBreakerOverflow { cluster_id: &'static str, denial: CircuitBreakerDenial },
    #[error("failed to build routing context for cluster '{cluster_id}': {source}")]
    RoutingContext { cluster_id: &'static str, source: RoutingContextError },
    #[error("failed to acquire an HTTP connection for cluster '{cluster_id}': {source}")]
    Connection { cluster_id: &'static str, source: Box<dyn Error + Send + Sync> },
}

impl AcquireHttpUpstreamError {
    #[inline]
    pub const fn kind(&self) -> AcquireHttpUpstreamErrorKind {
        match self {
            Self::ClusterNotFound => AcquireHttpUpstreamErrorKind::ClusterNotFound,
            Self::CircuitBreakerOverflow { .. } => AcquireHttpUpstreamErrorKind::CircuitBreakerOverflow,
            Self::RoutingContext { .. } => AcquireHttpUpstreamErrorKind::RoutingContext,
            Self::Connection { .. } => AcquireHttpUpstreamErrorKind::Connection,
        }
    }

    #[inline]
    pub const fn cluster_id(&self) -> Option<&'static str> {
        match self {
            Self::ClusterNotFound => None,
            Self::CircuitBreakerOverflow { cluster_id, .. }
            | Self::RoutingContext { cluster_id, .. }
            | Self::Connection { cluster_id, .. } => Some(cluster_id),
        }
    }
}

/// Resolve a cluster, account for the request, build the cluster-specific
/// routing context, and select the HTTP channels used to send it.
pub fn acquire_http_upstream(
    cluster_specifier: &ClusterSpecifier,
    request: &Request<ArionRequestBody>,
    hash_policy: &[HashPolicy],
    source_address: SocketAddr,
    priority: RoutingPriority,
) -> Result<AcquiredHttpUpstream, AcquireHttpUpstreamError> {
    let Some(cluster_id) = clusters_manager::resolve_cluster(cluster_specifier, Some(request.headers())) else {
        return Err(AcquireHttpUpstreamError::ClusterNotFound);
    };

    if let Err(denial) = try_increment_requests(cluster_id, priority) {
        record_request_overflow(cluster_id, denial);
        return Err(AcquireHttpUpstreamError::CircuitBreakerOverflow { cluster_id, denial });
    }
    let permit = RequestPermit { cluster_id, priority };

    let routing_requirement = clusters_manager::get_cluster_routing_requirements(cluster_id);
    let hash_state = HashState::new(hash_policy, request, source_address);

    let routing_context = RoutingContext::try_from((&routing_requirement, request, hash_state))
        .map_err(|source| AcquireHttpUpstreamError::RoutingContext { cluster_id, source })?;
    let channels = clusters_manager::get_http_connection(cluster_id, routing_context)
        .map_err(|source| AcquireHttpUpstreamError::Connection { cluster_id, source: Box::new(source.into_inner()) })?;

    Ok(AcquiredHttpUpstream { channels, cluster_id, _permit: permit })
}

#[cfg(feature = "metrics")]
fn record_request_overflow(cluster_id: &'static str, denial: CircuitBreakerDenial) {
    let shard_id = get_shard_id!();
    let attrs = &[KeyValue::new("cluster", cluster_id.to_owned())];
    match denial {
        CircuitBreakerDenial::MaxConnections => {
            with_metric!(clusters::UPSTREAM_CX_OVERFLOW, add, 1, shard_id, attrs);
        },
        CircuitBreakerDenial::MaxRequests => {
            with_metric!(clusters::UPSTREAM_RQ_OVERFLOW, add, 1, shard_id, attrs);
        },
        CircuitBreakerDenial::MaxRetries => {},
    }
}

#[cfg(not(feature = "metrics"))]
#[inline]
fn record_request_overflow(_cluster_id: &'static str, _denial: CircuitBreakerDenial) {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use arion_configuration::config::cluster::{
        CircuitBreakerThresholds, CircuitBreakers, Cluster, ClusterDiscoveryType, HttpProtocolOptions, LbPolicy,
        OriginalDstConfig, OriginalDstRoutingMethod, StandardLbPolicy,
    };
    use http::header::HOST;

    use super::*;
    use crate::{clusters::cluster::ClusterOps, secrets::SecretManager, PartialClusterType};

    fn build_original_dst_cluster(name: &str, max_requests: u32) -> PartialClusterType {
        let config = Cluster {
            name: name.into(),
            discovery_settings: ClusterDiscoveryType::OriginalDst(OriginalDstConfig {
                routing_method: OriginalDstRoutingMethod::Default,
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
        };
        PartialClusterType::try_from((Box::new(config), &SecretManager::new())).unwrap()
    }

    #[test]
    fn request_permit_accounts_for_acquisition_lifetime() {
        let cluster_name = "http-upstream-permit-test";
        let cluster = clusters_manager::add_cluster(build_original_dst_cluster(cluster_name, 1)).unwrap();
        let circuit_breaker = cluster.circuit_breaker().unwrap();
        let request = Request::builder().uri("http://127.0.0.1:18080/tool").body(ArionRequestBody::default()).unwrap();
        let specifier = ClusterSpecifier::Cluster(cluster_name.into());

        let acquired = acquire_http_upstream(
            &specifier,
            &request,
            &[],
            SocketAddr::from(([0, 0, 0, 0], 0)),
            RoutingPriority::Default,
        )
        .unwrap();
        assert_eq!(
            circuit_breaker.get_state(RoutingPriority::Default).counters.active_requests.load(Ordering::Relaxed),
            1
        );

        let overflow = acquire_http_upstream(
            &specifier,
            &request,
            &[],
            SocketAddr::from(([0, 0, 0, 0], 0)),
            RoutingPriority::Default,
        )
        .unwrap_err();
        assert_eq!(overflow.kind(), AcquireHttpUpstreamErrorKind::CircuitBreakerOverflow);
        assert_eq!(overflow.cluster_id(), Some(cluster_name));

        drop(acquired);
        assert_eq!(
            circuit_breaker.get_state(RoutingPriority::Default).counters.active_requests.load(Ordering::Relaxed),
            0
        );
        clusters_manager::remove_cluster(cluster_name).unwrap();
    }

    #[test]
    fn authority_routing_validates_and_accepts_request_authority() {
        let cluster_name = "http-upstream-missing-authority-test";
        let cluster = clusters_manager::add_cluster(build_original_dst_cluster(cluster_name, 1)).unwrap();
        let circuit_breaker = cluster.circuit_breaker().unwrap();
        let request = Request::builder().uri("/tool").body(ArionRequestBody::default()).unwrap();
        let specifier = ClusterSpecifier::Cluster(cluster_name.into());

        let error = acquire_http_upstream(
            &specifier,
            &request,
            &[],
            SocketAddr::from(([0, 0, 0, 0], 0)),
            RoutingPriority::Default,
        )
        .unwrap_err();

        assert_eq!(error.kind(), AcquireHttpUpstreamErrorKind::RoutingContext);
        assert_eq!(error.cluster_id(), Some(cluster_name));
        assert!(matches!(
            error,
            AcquireHttpUpstreamError::RoutingContext { source: RoutingContextError::MissingAuthority, .. }
        ));
        assert_eq!(
            circuit_breaker.get_state(RoutingPriority::Default).counters.active_requests.load(Ordering::Relaxed),
            0
        );

        let request = Request::builder()
            .uri("/tool")
            .header(HOST, "http://invalid-authority")
            .body(ArionRequestBody::default())
            .unwrap();
        let error = acquire_http_upstream(
            &specifier,
            &request,
            &[],
            SocketAddr::from(([0, 0, 0, 0], 0)),
            RoutingPriority::Default,
        )
        .unwrap_err();
        assert!(matches!(
            error,
            AcquireHttpUpstreamError::RoutingContext { source: RoutingContextError::InvalidAuthority(_), .. }
        ));
        assert_eq!(
            circuit_breaker.get_state(RoutingPriority::Default).counters.active_requests.load(Ordering::Relaxed),
            0
        );

        let request =
            Request::builder().uri("/tool").header(HOST, "127.0.0.1:18080").body(ArionRequestBody::default()).unwrap();
        let acquired = acquire_http_upstream(
            &specifier,
            &request,
            &[],
            SocketAddr::from(([0, 0, 0, 0], 0)),
            RoutingPriority::Default,
        )
        .unwrap();
        assert_eq!(acquired.cluster_id(), cluster_name);
        drop(acquired);

        // An unresolved cluster fails before routing, so it carries no cluster id.
        let unresolved = ClusterSpecifier::ClusterHeader(http::HeaderName::from_static("x-missing-cluster"));
        let error = acquire_http_upstream(
            &unresolved,
            &request,
            &[],
            SocketAddr::from(([0, 0, 0, 0], 0)),
            RoutingPriority::Default,
        )
        .unwrap_err();
        assert_eq!(error.kind(), AcquireHttpUpstreamErrorKind::ClusterNotFound);
        assert_eq!(error.cluster_id(), None);

        clusters_manager::remove_cluster(cluster_name).unwrap();
    }
}
