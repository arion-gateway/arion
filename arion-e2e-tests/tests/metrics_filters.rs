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
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use arion_e2e_tests::config_builder::{
    presets, BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder,
    LocalRateLimitBuilder, NetworkGlobalRateLimitBuilder, RouteConfigBuilder, UserRateLimiterBuilder,
    VirtualHostBuilder,
};
use arion_e2e_tests::{
    cleanup_config_file, rls_responses, ArionInstance, PortBlock, PreConfiguredResponse, RequestBuilder,
    RlsTestServerBuilder, SpawnOptions, TcpTestClient, TestBackend, TestClient,
};

fn parse_filter_metric_value(prometheus_output: &str, metric_name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    for line in prometheus_output.lines() {
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with(metric_name) {
            let mut matches = true;
            for (name, val) in labels {
                let pattern = format!("{name}=\"{val}\"");
                if !line.contains(&pattern) {
                    matches = false;
                    break;
                }
            }
            if matches {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if let Some(val_str) = parts.last() {
                    if let Ok(val) = val_str.parse::<f64>() {
                        return Some(val);
                    }
                }
            }
        }
    }
    None
}

#[tokio::test]
#[ignore]
async fn test_filter_connection_rate_limit_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    // Connection rate limit: max 1 token, fill 1 token per 10 seconds
    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).listener_local_rate_limit("conn_rate_limit", 1, 1, 10).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().route_config(
                        RouteConfigBuilder::new("routes")
                            .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route("backend"))),
                    ),
                ),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr))
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let admin_client = TestClient::new(admin_addr);
    let listener_addr = arion.listener_addr().unwrap();

    // 1. First connection (should succeed)
    {
        let req = b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"))
    }

    // 2. Second connection (should be rejected at connection level immediately)
    {
        let req = b"GET /test HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        let _ = stream.read_to_end(&mut resp).await.ok();
        // Connection is closed immediately, so response should be empty or connection reset
        assert!(resp.is_empty())
    }

    // 3. Verify metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    assert_eq!(
        parse_filter_metric_value(
            metrics,
            "filter_connection_rate_limit",
            &[("filter", "conn_rate_limit"), ("result", "ok")]
        ),
        Some(1.0)
    );
    assert_eq!(
        parse_filter_metric_value(
            metrics,
            "filter_connection_rate_limit",
            &[("filter", "conn_rate_limit"), ("result", "rate_limited")]
        ),
        Some(1.0)
    );

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_filter_local_rate_limit_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    // HCM local rate limit: max 1 token, fill 1 token per 10 seconds
    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(1, 1, 10);

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(
                        RouteConfigBuilder::new("routes")
                            .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route("backend"))),
                    ),
                ),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr))
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let admin_client = TestClient::new(admin_addr);
    let client = TestClient::new(arion.listener_addr().unwrap());

    // 1. First request (should succeed)
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    // 2. Second request (should be rate limited)
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    // 3. Verify metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    assert_eq!(
        parse_filter_metric_value(
            metrics,
            "filter_local_rate_limit",
            &[("filter", "hcm_rate_limit"), ("result", "ok")]
        ),
        Some(1.0)
    );
    assert_eq!(
        parse_filter_metric_value(
            metrics,
            "filter_local_rate_limit",
            &[("filter", "hcm_rate_limit"), ("result", "rate_limited")]
        ),
        Some(1.0)
    );

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_filter_user_rate_limit_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    // User rate limit: max 1 token, fill 1 token per 10 seconds for user "alice"
    let user_rate_limiter = UserRateLimiterBuilder::new()
        .stat_prefix("user_rate_limiter")
        .user_id_header("x-user-id")
        .status_code(429)
        .add_user_limit_simple(Some("alice"), 1, 10);

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().user_rate_limit(user_rate_limiter).route_config(
                        RouteConfigBuilder::new("routes")
                            .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route("backend"))),
                    ),
                ),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr))
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let admin_client = TestClient::new(admin_addr);
    let client = TestClient::new(arion.listener_addr().unwrap());

    // 1. First request for alice (should succeed)
    let response = client.send(RequestBuilder::get("/test").header("x-user-id", "alice")).await.unwrap();
    response.assert_status(StatusCode::OK);

    // 2. Second request for alice (should be rate limited)
    let response = client.send(RequestBuilder::get("/test").header("x-user-id", "alice")).await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    // 3. Request without user header (should be not_applicable)
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    // 4. Verify metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    assert_eq!(
        parse_filter_metric_value(
            metrics,
            "filter_user_rate_limit",
            &[("filter", "user_rate_limiter"), ("user", "alice"), ("result", "ok")]
        ),
        Some(1.0)
    );
    assert_eq!(
        parse_filter_metric_value(
            metrics,
            "filter_user_rate_limit",
            &[("filter", "user_rate_limiter"), ("user", "alice"), ("result", "rate_limited")]
        ),
        Some(1.0)
    );
    assert_eq!(
        parse_filter_metric_value(
            metrics,
            "filter_user_rate_limit",
            &[("filter", "user_rate_limiter"), ("result", "not_applicable")]
        ),
        Some(1.0)
    );

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_filter_global_rate_limit_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    // Start mock RLS server
    let rls = RlsTestServerBuilder::new()
        .with_response(rls_responses::ok())
        .with_response(rls_responses::over_limit())
        .start()
        .await
        .unwrap();

    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    // Configure global rate limiter filter
    let rl = NetworkGlobalRateLimitBuilder::new("global_limit")
        .domain("test.example")
        .rls_cluster("rls_cluster")
        .descriptor("destination_cluster", "backend");

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").network_global_rate_limit(rl).hcm(
                    HcmBuilder::new().http1().route_config(
                        RouteConfigBuilder::new("routes")
                            .virtual_host(VirtualHostBuilder::new("vh").route(presets::default_route("backend"))),
                    ),
                ),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())))
        .cluster(ClusterBuilder::new("rls_cluster").http2().endpoint(EndpointBuilder::from_socket_addr(rls.addr())))
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let admin_client = TestClient::new(admin_addr);
    let listener_addr = arion.listener_addr().unwrap();

    // 1. First connection (should be allowed by RLS)
    {
        let req = b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"))
    }

    // 2. Second connection (should be over limit and dropped)
    {
        let tcp = TcpTestClient::new(listener_addr);
        let result = tcp.receive_on_connect_with_timeout(Duration::from_millis(500)).await;
        assert!(result.is_err() || result.unwrap().is_empty(), "connection should be dropped")
    }

    // 3. Verify metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    assert_eq!(
        parse_filter_metric_value(
            metrics,
            "filter_connection_rate_limit",
            &[("filter", "global_limit"), ("result", "ok")]
        ),
        Some(1.0)
    );
    assert_eq!(
        parse_filter_metric_value(
            metrics,
            "filter_connection_rate_limit",
            &[("filter", "global_limit"), ("result", "rate_limited")]
        ),
        Some(1.0)
    );

    arion.shutdown();
    cleanup_config_file(&config_path);
}
