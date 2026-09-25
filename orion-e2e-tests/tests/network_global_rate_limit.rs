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

use std::time::Duration;

use http::StatusCode;
use orion_e2e_tests::{
    config_builder::{
        presets, BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder,
        NetworkGlobalRateLimitBuilder, RouteConfigBuilder, VirtualHostBuilder,
    },
    rls_responses, OrionInstance, RlsTestServerBuilder, SpawnOptions, TcpTestClient, TestBackend, TestClient,
};

fn http_filter_chain(rls_cluster: &str, domain: &str, failure_mode_deny: bool) -> FilterChainBuilder {
    let mut rl = NetworkGlobalRateLimitBuilder::new("rl")
        .domain(domain)
        .rls_cluster(rls_cluster)
        .descriptor("destination_cluster", "backend");

    if failure_mode_deny {
        rl = rl.failure_mode_deny();
    }

    FilterChainBuilder::new("main").network_global_rate_limit(rl).hcm(
        HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes")
                .virtual_host(VirtualHostBuilder::new("vh").route(presets::default_route("backend"))),
        ),
    )
}

#[tokio::test]
#[ignore]
async fn test_allowed_connections_reach_backend() {
    let rls = RlsTestServerBuilder::new().with_response(rls_responses::ok()).start().await.unwrap();

    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(orion_e2e_tests::PreConfiguredResponse::with_body("ok")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(http_filter_chain("rls", "test.example", false)))
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())))
        .cluster(ClusterBuilder::new("rls").http2().endpoint(EndpointBuilder::from_socket_addr(rls.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let resp = client.get("/").await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert_eq!(rls.call_count(), 1);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_over_limit_drops_tcp_connection() {
    let rls = RlsTestServerBuilder::new().with_response(rls_responses::over_limit()).start().await.unwrap();

    let backend = TestBackend::start().await.unwrap();

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(http_filter_chain("rls", "test.example", false)))
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())))
        .cluster(ClusterBuilder::new("rls").http2().endpoint(EndpointBuilder::from_socket_addr(rls.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let tcp = TcpTestClient::new(orion.listener_addr().unwrap());
    let result = tcp.receive_on_connect_with_timeout(Duration::from_millis(500)).await;
    assert!(result.is_err() || result.unwrap().is_empty(), "connection should be dropped");
    assert_eq!(rls.call_count(), 1);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_failure_mode_allow_passes_when_rls_fails() {
    let rls = RlsTestServerBuilder::always_failing().start().await.unwrap();

    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(orion_e2e_tests::PreConfiguredResponse::with_body("ok")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(http_filter_chain("rls", "test.example", false)))
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())))
        .cluster(ClusterBuilder::new("rls").http2().endpoint(EndpointBuilder::from_socket_addr(rls.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let resp = client.get("/").await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert_eq!(rls.call_count(), 1);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_failure_mode_deny_drops_when_rls_fails() {
    let rls = RlsTestServerBuilder::always_failing().start().await.unwrap();

    let backend = TestBackend::start().await.unwrap();

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(http_filter_chain("rls", "test.example", true)))
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())))
        .cluster(ClusterBuilder::new("rls").http2().endpoint(EndpointBuilder::from_socket_addr(rls.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let tcp = TcpTestClient::new(orion.listener_addr().unwrap());
    let result = tcp.receive_on_connect_with_timeout(Duration::from_millis(500)).await;
    assert!(result.is_err() || result.unwrap().is_empty(), "connection should be dropped");
    assert_eq!(rls.call_count(), 1);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_quota_caching_limits_rls_calls() {
    // quota.requests=5: first connection consumes 1, bucket stores remaining=4.
    // Connections 2-5 consume from the bucket. Connection 6 exhausts it and hits the RLS again.
    let rls = RlsTestServerBuilder::new()
        .with_response(rls_responses::ok_with_quota(5))
        .with_response(rls_responses::ok_with_quota(5))
        .start()
        .await
        .unwrap();

    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(orion_e2e_tests::PreConfiguredResponse::with_body("ok")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(http_filter_chain(
            "rls",
            "quota-test.example",
            false,
        )))
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())))
        .cluster(ClusterBuilder::new("rls").http2().endpoint(EndpointBuilder::from_socket_addr(rls.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let orion_addr = orion.listener_addr().unwrap();

    for _ in 0..6 {
        let client = TestClient::new(orion_addr);
        let resp = client.get("/").await.unwrap();
        resp.assert_status(StatusCode::OK);
    }

    assert_eq!(rls.call_count(), 2, "only connections 1 and 6 should hit the RLS");

    orion.shutdown();
}
