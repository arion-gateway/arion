// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

use http::StatusCode;
use std::net::SocketAddr;

use arion_e2e_tests::config_builder::{
    presets, BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use arion_e2e_tests::{
    cleanup_config_file, parse_metric_value, ArionInstance, PortBlock, PreConfiguredResponse, SpawnOptions,
    TestBackend, TestClient,
};

#[tokio::test]
#[ignore]
async fn test_listener_connection_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from backend!")).await;

    let bootstrap = presets::simple_proxy("backend", backend_addr).admin("127.0.0.1", admin_port);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let client = TestClient::new(arion.listener_addr().unwrap()).with_header("connection", "close");
    let admin_client = TestClient::new(admin_addr);

    // 1. Initial check: metrics should be 0 or not present yet (or 0 if initialized)
    let initial_metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    initial_metrics_resp.assert_status(StatusCode::OK);
    let initial_metrics = initial_metrics_resp.body_str().unwrap();

    let cx_total = parse_metric_value(initial_metrics, "listeners_downstream_cx_total").unwrap_or(0);
    assert_eq!(cx_total, 0);

    // 2. Send multiple requests to trigger connection metrics multiple times
    let num_requests = 3;
    for _ in 0..num_requests {
        let response = client.get("/hello").await.expect("Failed to send request");
        response.assert_status(StatusCode::OK);
        response.assert_body("Hello from backend!");
    }

    // 3. Check metrics after requests
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    let cx_total = parse_metric_value(metrics, "listeners_downstream_cx_total").expect("Missing cx_total metric");
    let cx_destroy = parse_metric_value(metrics, "listeners_downstream_cx_destroy").expect("Missing cx_destroy metric");
    let cx_active = parse_metric_value(metrics, "listeners_downstream_cx_active").expect("Missing cx_active metric");
    let cx_length_count = parse_metric_value(metrics, "listeners_downstream_cx_length_ms_count")
        .expect("Missing cx_length_ms_count metric");

    assert_eq!(cx_total, num_requests, "Expected exactly {num_requests} downstream connections");
    assert_eq!(cx_destroy, num_requests, "Expected exactly {num_requests} destroyed downstream connections");
    assert_eq!(cx_active, 0, "Expected 0 active downstream connections");
    assert_eq!(cx_length_count, num_requests, "Expected exactly {num_requests} recorded connection lengths");

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_listener_no_filter_chain_match_metric() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from backend!")).await;

    // Create a listener with a filter chain that only matches destination port 12345.
    // Since the actual listener port will be dynamically allocated (not 12345),
    // any incoming connection will fail to match the filter chain.
    let listener = ListenerBuilder::new("http").port(0).filter_chain(
        FilterChainBuilder::new("main").destination_port(12345).hcm(
            HcmBuilder::new().route_config(
                RouteConfigBuilder::new("routes")
                    .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route("backend"))),
            ),
        ),
    );

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));
    let bootstrap = BootstrapBuilder::new().listener(listener).cluster(cluster).admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let client = TestClient::new(arion.listener_addr().unwrap());
    let admin_client = TestClient::new(admin_addr);

    // Send multiple requests. They should fail or be rejected because there is no filter chain match.
    let num_requests = 3;
    for _ in 0..num_requests {
        let _ = client.get("/hello").await.ok();
    }

    // Check metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    let no_match =
        parse_metric_value(metrics, "listeners_no_filter_chain_match").expect("Missing no_filter_chain_match metric");
    assert_eq!(no_match, num_requests, "Expected exactly {num_requests} connections with no filter chain match");

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_listener_active_connection_metric() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from backend!")).await;

    let bootstrap = presets::simple_proxy("backend", backend_addr).admin("127.0.0.1", admin_port);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let admin_client = TestClient::new(admin_addr);

    // Create the client in a nested scope so we can drop it explicitly
    {
        let client = TestClient::new(arion.listener_addr().unwrap());

        // Send a request. Since keep-alive is active, the connection will remain open.
        let response = client.get("/hello").await.expect("Failed to send request");
        response.assert_status(StatusCode::OK);
        response.assert_body("Hello from backend!");

        // Check metrics: active connection should be 1.0
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        let cx_active =
            parse_metric_value(metrics, "listeners_downstream_cx_active").expect("Missing cx_active metric");
        assert_eq!(cx_active, 1, "Expected exactly 1 active downstream connection")
    } // client is dropped here, which closes the keep-alive connection

    // Poll metrics until active connections drop to 0
    let mut cx_active = 1;
    for _ in 0..20 {
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        cx_active = parse_metric_value(metrics, "listeners_downstream_cx_active").unwrap_or(0);
        if cx_active == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert_eq!(cx_active, 0, "Expected active connections to drop to 0 after client is dropped");

    // Also verify that cx_destroy is now 1.0
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    let metrics = metrics_resp.body_str().unwrap();
    let cx_destroy = parse_metric_value(metrics, "listeners_downstream_cx_destroy").expect("Missing cx_destroy metric");
    assert_eq!(cx_destroy, 1, "Expected exactly 1 destroyed connection");

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_listener_concurrent_connections_metric() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from backend!")).await;

    let bootstrap = presets::simple_proxy("backend", backend_addr).admin("127.0.0.1", admin_port);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let admin_client = TestClient::new(admin_addr);
    let listener_addr = arion.listener_addr().unwrap();

    // Open multiple concurrent clients in a nested scope
    {
        let client1 = TestClient::new(listener_addr);
        let client2 = TestClient::new(listener_addr);
        let client3 = TestClient::new(listener_addr);

        // Send a request from each client to establish 3 concurrent keep-alive connections
        let r1 = client1.get("/hello").await.expect("Failed client1 request");
        let r2 = client2.get("/hello").await.expect("Failed client2 request");
        let r3 = client3.get("/hello").await.expect("Failed client3 request");

        r1.assert_status(StatusCode::OK);
        r2.assert_status(StatusCode::OK);
        r3.assert_status(StatusCode::OK);

        // Check metrics: active connections should be exactly 3.0
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        let cx_active =
            parse_metric_value(metrics, "listeners_downstream_cx_active").expect("Missing cx_active metric");
        assert_eq!(cx_active, 3, "Expected exactly 3 active downstream connections")
    } // All 3 clients are dropped here, closing all 3 connections

    // Poll metrics until active connections drop to 0
    let mut cx_active = 3;
    for _ in 0..20 {
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        cx_active = parse_metric_value(metrics, "listeners_downstream_cx_active").unwrap_or(0);
        if cx_active == 0 {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert_eq!(cx_active, 0, "Expected active connections to drop to 0 after all clients are dropped");

    // Verify that cx_destroy is now 3
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    let metrics = metrics_resp.body_str().unwrap();
    let cx_destroy = parse_metric_value(metrics, "listeners_downstream_cx_destroy").expect("Missing cx_destroy metric");
    assert_eq!(cx_destroy, 3, "Expected exactly 3 destroyed connections");

    arion.shutdown();
    cleanup_config_file(&config_path);
}
