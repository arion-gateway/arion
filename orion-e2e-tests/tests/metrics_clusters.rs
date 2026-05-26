// Copyright 2025 The kmesh Authors
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

use std::net::SocketAddr;
use std::time::Duration;
use http::StatusCode;

use orion_e2e_tests::config_builder::{
    presets, BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder,
    ListenerBuilder, RouteConfigBuilder, VirtualHostBuilder, RouteBuilder, RetryPolicyBuilder,
};
use orion_e2e_tests::{
    cleanup_config_file, parse_metric_value, OrionInstance, PortBlock, PreConfiguredResponse, SpawnOptions,
    TestBackend, TestClient, TcpTestBackend,
};

#[tokio::test]
#[ignore]
async fn test_cluster_request_and_byte_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    // Start a TCP backend to capture exact request bytes and control exact response bytes
    let mut backend = TcpTestBackend::start().await.expect("Failed to start TCP backend");
    let backend_addr = backend.addr();
    
    // Give the backend background task a moment to start accepting connections
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    let response_bytes = b"HTTP/1.1 200 OK\r\ncontent-length: 19\r\nconnection: close\r\n\r\nHello from backend!";
    backend.set_send_on_connect(response_bytes.to_vec()).await;

    let bootstrap = presets::simple_proxy("backend", backend_addr)
        .admin("127.0.0.1", admin_port);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap()).with_header("connection", "close");
    let admin_client = TestClient::new(admin_addr);

    // Send first request
    let request_body = "Hello Orion!";
    let response = client.post("/hello", request_body).await.expect("Failed to send request");
    response.assert_status(StatusCode::OK);
    response.assert_body("Hello from backend!");

    // Wait for the backend read timeout (100ms) to expire and close the first connection
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Capture the exact request bytes received by the backend for the first connection
    let conn1 = backend.await_connection().await.expect("No connection received by backend");
    let mut total_expected_tx = conn1.received_data.len();
    let mut total_expected_rx = response_bytes.len();

    // Send second request
    let response2 = client.post("/hello", request_body).await.expect("Failed to send second request");
    response2.assert_status(StatusCode::OK);
    response2.assert_body("Hello from backend!");

    // Wait for the backend read timeout (100ms) to expire and close the second connection
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Capture the exact request bytes received by the backend for the second connection
    let conn2 = backend.await_connection().await.expect("No connection received by backend");
    total_expected_tx += conn2.received_data.len();
    total_expected_rx += response_bytes.len();

    // Check metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    let rq_total = parse_metric_value(metrics, "cluster_upstream_rq_total").expect("Missing rq_total metric");
    let cx_total = parse_metric_value(metrics, "cluster_upstream_cx_total").expect("Missing cx_total metric");
    let cx_destroy = parse_metric_value(metrics, "cluster_upstream_cx_destroy").expect("Missing cx_destroy metric");
    let cx_active = parse_metric_value(metrics, "cluster_upstream_cx_active").expect("Missing cx_active metric");
    let rx_bytes = parse_metric_value(metrics, "cluster_upstream_cx_rx_bytes_total").expect("Missing rx_bytes metric");
    let tx_bytes = parse_metric_value(metrics, "cluster_upstream_cx_tx_bytes_total").expect("Missing tx_bytes metric");

    assert_eq!(rq_total, 2.0, "Expected exactly 2 upstream requests");
    assert_eq!(cx_total, 2.0, "Expected exactly 2 upstream connections");
    assert_eq!(cx_destroy, 2.0, "Expected exactly 2 destroyed upstream connections");
    assert_eq!(cx_active, 0.0, "Expected 0 active upstream connections");
    assert_eq!(rx_bytes, total_expected_rx as f64, "Expected exact response bytes");
    assert_eq!(tx_bytes, total_expected_tx as f64, "Expected exact request bytes");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_cluster_active_request_metric() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    // Set a delay of 500ms on the backend response
    backend.set_default_response(
        PreConfiguredResponse::with_body("Hello from backend!")
            .delay(Duration::from_millis(500))
    ).await;

    let bootstrap = presets::simple_proxy("backend", backend_addr)
        .admin("127.0.0.1", admin_port);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let admin_client = TestClient::new(admin_addr);

    // Send the request in a separate task so it runs concurrently
    let handle = tokio::spawn(async move {
        client.get("/hello").await
    });

    // Poll metrics until active requests becomes 1.0 (timeout of 2 seconds)
    let mut rq_active = 0.0;
    let start_time = tokio::time::Instant::now();
    while start_time.elapsed() < Duration::from_secs(2) {
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        rq_active = parse_metric_value(metrics, "cluster_upstream_rq_active").unwrap_or(0.0);
        if rq_active == 1.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(rq_active, 1.0, "Expected exactly 1 active upstream request");

    // Wait for the request to complete
    let response = handle.await.unwrap().expect("Request failed");
    response.assert_status(StatusCode::OK);

    // Check metrics again: active requests should drop to 0.0
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    let rq_active = parse_metric_value(metrics, "cluster_upstream_rq_active").expect("Missing rq_active metric");
    assert_eq!(rq_active, 0.0, "Expected 0 active upstream requests after completion");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_cluster_connection_failure_metric() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    // Configure a cluster pointing to an unreachable port (127.0.0.1:1)
    let unreachable_addr: SocketAddr = "127.0.0.1:1".parse().unwrap();
    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(unreachable_addr));

    let listener = ListenerBuilder::new("http").port(0).filter_chain(
        FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().route_config(
                RouteConfigBuilder::new("routes")
                    .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route("backend"))),
            ),
        ),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(cluster)
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let admin_client = TestClient::new(admin_addr);

    // Send a request. It should fail because the backend is unreachable.
    let _ = client.get("/hello").await;

    // Check metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    let cx_fail = parse_metric_value(metrics, "cluster_upstream_cx_connect_fail").expect("Missing cx_connect_fail metric");
    assert_eq!(cx_fail, 1.0, "Expected exactly 1 upstream connection failure");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_cluster_request_timeout_metric() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    // Set a delay of 1000ms on the backend response
    backend.set_default_response(
        PreConfiguredResponse::with_body("Hello from backend!")
            .delay(Duration::from_millis(1000))
    ).await;

    // Configure a route with a short timeout of 100ms
    let listener = ListenerBuilder::new("http").port(0).filter_chain(
        FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().route_config(
                RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(
                        presets::default_route("backend").timeout(Duration::from_millis(100))
                    )
                )
            )
        )
    );

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));
    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(cluster)
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let admin_client = TestClient::new(admin_addr);

    // Send a request. It should fail with a timeout.
    let response = client.get("/hello").await.expect("Failed to send request");
    // Orion should return 504 Gateway Timeout, 503 Service Unavailable, or 502 Bad Gateway on timeout
    assert!(
        response.status == StatusCode::GATEWAY_TIMEOUT || response.status == StatusCode::SERVICE_UNAVAILABLE || response.status == StatusCode::BAD_GATEWAY,
        "Expected 504, 503, or 502, but got status {} and body {:?}",
        response.status,
        response.body_str()
    );

    // Check metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    let rq_timeout = parse_metric_value(metrics, "cluster_upstream_rq_timeout").expect("Missing rq_timeout metric");
    assert_eq!(rq_timeout, 1.0, "Expected exactly 1 upstream request timeout");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_cluster_retry_metric() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    // Queue a 503 Service Unavailable response for the first attempt,
    // and a 200 OK response for the second attempt (retry).
    backend.enqueue_response(
        PreConfiguredResponse::with_status(StatusCode::SERVICE_UNAVAILABLE)
    ).await;
    backend.enqueue_response(
        PreConfiguredResponse::with_body("Success after retry!")
            .header("connection", "close")
    ).await;

    // Configure a route with a retry policy (retry on 5xx, up to 3 retries)
    let retry_policy = RetryPolicyBuilder::new().on_5xx().num_retries(3);
    let route = RouteBuilder::new().match_prefix("/").cluster("backend");
    let vhost = VirtualHostBuilder::new("default").route(route).retry_policy(retry_policy);

    let listener = ListenerBuilder::new("http").port(0).filter_chain(
        FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().route_config(
                RouteConfigBuilder::new("routes").virtual_host(vhost)
            )
        )
    );

    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend_addr))
        .circuit_breaker_max_retries(3);
    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(cluster)
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let admin_client = TestClient::new(admin_addr);

    // Send the request. It should succeed (200 OK) because of the retry.
    let response = client.get("/hello").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Success after retry!");

    // Check metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    let rq_retry = parse_metric_value(metrics, "cluster_upstream_rq_retry").expect("Missing rq_retry metric");
    assert_eq!(rq_retry, 1.0, "Expected exactly 1 upstream request retry");

    orion.shutdown();
    cleanup_config_file(&config_path);
}
