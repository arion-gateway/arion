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

use std::fmt::{self, Write as _};

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
};
use orion_configuration::config::{
    cluster::{
        CircuitBreakerThresholds, Cluster as ClusterConfig, ClusterDiscoveryType, HealthStatus, LocalityLbEndpoints,
        RoutingPriority, DEFAULT_MAX_REQUESTS,
    },
    core::Address,
};
use orion_lib::clusters::clusters_manager::get_all_clusters;

use crate::admin::AdminState;

#[derive(Default, Clone, Copy)]
struct EndpointMetrics {
    cx_active: u64,
    cx_connect_fail: u64,
    cx_total: u64,
    rq_active: u64,
    rq_timeout: u64,
    rq_total: u64,
}

#[cfg(feature = "metrics")]
fn cluster_metrics(cluster_name: &str) -> EndpointMetrics {
    use opentelemetry::KeyValue;
    use orion_metrics::metrics::clusters;

    let key = [KeyValue::new("cluster", cluster_name.to_owned())];
    let cx_active = clusters::UPSTREAM_CX_ACTIVE.get().and_then(|m| m.value.load(&key)).unwrap_or(0);
    let cx_connect_fail = clusters::UPSTREAM_CX_CONNECT_FAIL.get().and_then(|m| m.value.load(&key)).unwrap_or(0);
    let cx_total = clusters::UPSTREAM_CX_TOTAL.get().and_then(|m| m.value.load(&key)).unwrap_or(0);
    let rq_active = clusters::UPSTREAM_RQ_ACTIVE.get().and_then(|m| m.value.load(&key)).unwrap_or(0);
    let rq_timeout = clusters::UPSTREAM_RQ_TIMEOUT.get().and_then(|m| m.value.load(&key)).unwrap_or(0);
    let rq_total = clusters::UPSTREAM_RQ_TOTAL.get().and_then(|m| m.value.load(&key)).unwrap_or(0);

    EndpointMetrics { cx_active, cx_connect_fail, cx_total, rq_active, rq_timeout, rq_total }
}

#[cfg(not(feature = "metrics"))]
fn cluster_metrics(_cluster_name: &str) -> EndpointMetrics {
    EndpointMetrics::default()
}

// Orion only tracks circuit breaker thresholds per priority; the last matching entry wins,
// mirroring `ClusterCircuitBreaker::from` in orion-lib.
fn thresholds_for(cluster: &ClusterConfig, priority: RoutingPriority) -> CircuitBreakerThresholds {
    cluster
        .circuit_breakers
        .as_ref()
        .and_then(|circuit_breakers| circuit_breakers.thresholds.iter().rfind(|t| t.priority == priority).cloned())
        .unwrap_or_default()
}

// Returns the configured endpoints for a cluster (if any are known yet) and whether the
// cluster is a STRICT_DNS cluster, which is the only case where Envoy reports a hostname.
fn cluster_endpoints(cluster: &ClusterConfig) -> (Option<&[LocalityLbEndpoints]>, bool) {
    match &cluster.discovery_settings {
        ClusterDiscoveryType::StrictDns(cla) => (Some(cla.endpoints.as_slice()), true),
        ClusterDiscoveryType::Static(cla) | ClusterDiscoveryType::Eds(Some(cla)) => {
            (Some(cla.endpoints.as_slice()), false)
        },
        ClusterDiscoveryType::Eds(None) | ClusterDiscoveryType::OriginalDst(_) => (None, false),
    }
}

fn write_cluster(out: &mut String, cluster: &ClusterConfig) -> fmt::Result {
    let name = cluster.name.as_str();
    writeln!(out, "{name}::observability_name::{name}")?;

    for (label, priority) in [("default_priority", RoutingPriority::Default), ("high_priority", RoutingPriority::High)]
    {
        let thresholds = thresholds_for(cluster, priority);
        writeln!(out, "{name}::{label}::max_connections::{}", thresholds.max_connections)?;
        // Orion has no separate max_pending_requests setting; Envoy's own default is 1024.
        writeln!(out, "{name}::{label}::max_pending_requests::{DEFAULT_MAX_REQUESTS}")?;
        writeln!(out, "{name}::{label}::max_requests::{}", thresholds.max_requests)?;
        writeln!(out, "{name}::{label}::max_retries::{}", thresholds.max_retries)?;
    }

    // Orion does not currently distinguish clusters added via CDS from statically configured ones.
    writeln!(out, "{name}::added_via_api::false")?;

    let (endpoints, is_strict_dns) = cluster_endpoints(cluster);
    let Some(localities) = endpoints else { return Ok(()) };

    // Connection/request counters are only tracked per cluster, not per endpoint, so every
    // endpoint of a cluster reports the same cluster-wide totals.
    let metrics = cluster_metrics(name);
    let rq_error = metrics.rq_timeout;
    let rq_success = metrics.rq_total.saturating_sub(rq_error);

    for locality in localities {
        for endpoint in &locality.lb_endpoints {
            let address = endpoint.address.to_string();
            let hostname = match (&endpoint.address, is_strict_dns) {
                (Address::Socket(host, _), true) => host.as_str(),
                _ => "",
            };
            // Unhealthy is only ever set by active health checks in Orion today.
            let health_flags = match endpoint.health_status {
                HealthStatus::Healthy => "healthy",
                HealthStatus::Unhealthy => "/failed_active_hc",
            };

            writeln!(out, "{name}::{address}::cx_active::{}", metrics.cx_active)?;
            writeln!(out, "{name}::{address}::cx_connect_fail::{}", metrics.cx_connect_fail)?;
            writeln!(out, "{name}::{address}::cx_total::{}", metrics.cx_total)?;
            writeln!(out, "{name}::{address}::rq_active::{}", metrics.rq_active)?;
            writeln!(out, "{name}::{address}::rq_error::{rq_error}")?;
            writeln!(out, "{name}::{address}::rq_success::{rq_success}")?;
            writeln!(out, "{name}::{address}::rq_timeout::{}", metrics.rq_timeout)?;
            writeln!(out, "{name}::{address}::rq_total::{}", metrics.rq_total)?;
            writeln!(out, "{name}::{address}::hostname::{hostname}")?;
            writeln!(out, "{name}::{address}::health_flags::{health_flags}")?;
            writeln!(out, "{name}::{address}::weight::{}", endpoint.load_balancing_weight)?;
            writeln!(out, "{name}::{address}::region::")?;
            writeln!(out, "{name}::{address}::zone::")?;
            writeln!(out, "{name}::{address}::sub_zone::")?;
            writeln!(out, "{name}::{address}::canary::false")?;
            writeln!(out, "{name}::{address}::priority::{}", locality.priority)?;
            // Orion does not implement outlier detection, so there is no success rate to report.
            writeln!(out, "{name}::{address}::success_rate::-1")?;
            writeln!(out, "{name}::{address}::local_origin_success_rate::-1")?;
        }
    }

    Ok(())
}

fn build_clusters_output() -> Result<String, fmt::Error> {
    let mut out = String::new();
    for cluster in get_all_clusters() {
        write_cluster(&mut out, &cluster)?;
    }
    Ok(out)
}

pub async fn clusters_handler(
    State(_admin_state): State<AdminState>,
) -> Result<(HeaderMap, String), (StatusCode, String)> {
    let out = build_clusters_output().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut headers = HeaderMap::new();
    // In native Envoy, the /clusters endpoint simply returns text/plain
    #[allow(clippy::unwrap_used)]
    headers.insert(::http::header::CONTENT_TYPE, "text/plain".parse().unwrap());

    Ok((headers, out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::build_admin_router;
    use axum_test::TestServer;
    use orion_configuration::config::{
        cluster::{Cluster, ClusterLoadAssignment, HttpProtocolOptions, LbEndpoint, LbPolicy, OriginalDstConfig},
        Bootstrap,
    };
    use orion_lib::clusters::{add_cluster, cluster::PartialClusterType};
    use parking_lot::RwLock;
    use smol_str::SmolStr;
    use std::{num::NonZeroU32, time::Instant};
    use triomphe::Arc;

    fn make_admin_state() -> AdminState {
        AdminState {
            bootstrap: Bootstrap::default(),
            configuration_senders: vec![],
            secret_manager: Arc::new(RwLock::new(orion_lib::SecretManager::default())),
            server_startup: Instant::now(),
        }
    }

    fn make_cluster(name: &str, discovery_settings: ClusterDiscoveryType) -> Cluster {
        Cluster {
            name: SmolStr::new(name),
            discovery_settings,
            transport_socket: None,
            bind_device: None,
            load_balancing_policy: LbPolicy::default(),
            http_protocol_options: HttpProtocolOptions::default(),
            health_check: None,
            connect_timeout: None,
            cleanup_interval: None,
            circuit_breakers: None,
        }
    }

    fn register_cluster(cluster: Cluster) {
        let secret_manager = orion_lib::SecretManager::default();
        let partial = PartialClusterType::try_from((Box::new(cluster), &secret_manager)).unwrap();
        add_cluster(partial).unwrap();
    }

    #[tokio::test]
    async fn clusters_handler_outputs_envoy_style_stats() {
        let cluster = make_cluster(
            "clusters_test_static",
            ClusterDiscoveryType::Static(ClusterLoadAssignment {
                cluster_name: "clusters_test_static".into(),
                endpoints: vec![LocalityLbEndpoints {
                    priority: 0,
                    lb_endpoints: vec![LbEndpoint {
                        address: Address::Socket("127.0.0.1".to_owned(), 8080),
                        health_status: HealthStatus::Healthy,
                        load_balancing_weight: NonZeroU32::new(1).unwrap(),
                    }],
                }],
            }),
        );
        register_cluster(cluster);

        let app = build_admin_router(make_admin_state());
        let server = TestServer::new(app).unwrap();
        let response = server.get("/clusters").await;
        response.assert_status_ok();
        let body = response.text();

        assert!(body.contains("clusters_test_static::observability_name::clusters_test_static\n"));
        assert!(body.contains("clusters_test_static::default_priority::max_connections::1024\n"));
        assert!(body.contains("clusters_test_static::default_priority::max_pending_requests::1024\n"));
        assert!(body.contains("clusters_test_static::default_priority::max_requests::1024\n"));
        assert!(body.contains("clusters_test_static::default_priority::max_retries::3\n"));
        assert!(body.contains("clusters_test_static::high_priority::max_connections::1024\n"));
        assert!(body.contains("clusters_test_static::added_via_api::false\n"));
        assert!(body.contains("clusters_test_static::127.0.0.1:8080::health_flags::healthy\n"));
        assert!(body.contains("clusters_test_static::127.0.0.1:8080::weight::1\n"));
        assert!(body.contains("clusters_test_static::127.0.0.1:8080::hostname::\n"));
        assert!(body.contains("clusters_test_static::127.0.0.1:8080::priority::0\n"));
        assert!(body.contains("clusters_test_static::127.0.0.1:8080::canary::false\n"));
        assert!(body.contains("clusters_test_static::127.0.0.1:8080::success_rate::-1\n"));
    }

    #[tokio::test]
    async fn clusters_handler_strict_dns_sets_hostname_and_unhealthy_flag() {
        let cluster = make_cluster(
            "clusters_test_dns",
            ClusterDiscoveryType::StrictDns(ClusterLoadAssignment {
                cluster_name: "clusters_test_dns".into(),
                endpoints: vec![LocalityLbEndpoints {
                    priority: 0,
                    lb_endpoints: vec![LbEndpoint {
                        address: Address::Socket("upstream.example".to_owned(), 443),
                        health_status: HealthStatus::Unhealthy,
                        load_balancing_weight: NonZeroU32::new(1).unwrap(),
                    }],
                }],
            }),
        );
        register_cluster(cluster);

        let app = build_admin_router(make_admin_state());
        let server = TestServer::new(app).unwrap();
        let response = server.get("/clusters").await;
        response.assert_status_ok();
        let body = response.text();

        assert!(body.contains("clusters_test_dns::upstream.example:443::hostname::upstream.example\n"));
        assert!(body.contains("clusters_test_dns::upstream.example:443::health_flags::/failed_active_hc\n"));
    }

    #[tokio::test]
    async fn clusters_handler_original_dst_has_no_endpoint_lines() {
        let cluster =
            make_cluster("clusters_test_odst", ClusterDiscoveryType::OriginalDst(OriginalDstConfig::default()));
        register_cluster(cluster);

        let app = build_admin_router(make_admin_state());
        let server = TestServer::new(app).unwrap();
        let response = server.get("/clusters").await;
        response.assert_status_ok();
        let body = response.text();

        assert!(body.contains("clusters_test_odst::observability_name::clusters_test_odst\n"));
        assert!(body.contains("clusters_test_odst::added_via_api::false\n"));
        assert!(!body.contains("clusters_test_odst::127.0.0.1"));
    }
}
