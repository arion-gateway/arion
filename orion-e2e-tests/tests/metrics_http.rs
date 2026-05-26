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
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use orion_e2e_tests::config_builder::{
    presets, BootstrapBuilder, ClusterBuilder, DownstreamTlsBuilder, FilterChainBuilder, HcmBuilder,
    ListenerBuilder, RouteConfigBuilder, VirtualHostBuilder, RouteBuilder,
};
use orion_e2e_tests::{
    cleanup_config_file, parse_metric_value, OrionInstance, PortBlock, PreConfiguredResponse, SpawnOptions,
    TestBackend, TestClient, TestCerts, TlsClientConfig, RawHttpRequestBuilder,
};

#[tokio::test]
#[ignore]
async fn test_http_basic_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    // Configure simple proxy with the admin interface enabled
    let bootstrap = presets::simple_proxy("backend", backend_addr)
        .admin("127.0.0.1", admin_port);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let admin_client = TestClient::new(admin_addr);

    // 1. Initial check: HTTP metrics should not be present or should be 0
    let initial_metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    initial_metrics_resp.assert_status(StatusCode::OK);
    let initial_metrics = initial_metrics_resp.body_str().unwrap();

    let cx_total = parse_metric_value(initial_metrics, "http_downstream_cx_total").unwrap_or(0.0);
    assert_eq!(cx_total, 0.0_f64, "Initial downstream connections should be 0");

    let mut total_expected_rx = 0;
    let mut total_expected_tx = 0;

    let listener_addr = orion.listener_addr().unwrap();

    // 2. Send 2xx request
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from backend!")).await;
    let req1 = b"GET /ok HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    total_expected_rx += req1.len();
    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req1).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        total_expected_tx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"));
    }

    // 3. Send 3xx request (backend returns 302 Found)
    backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::FOUND).body("Redirecting...")).await;
    let req2 = b"GET /redirect HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    total_expected_rx += req2.len();
    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req2).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        total_expected_tx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("302 Found"));
    }

    // 4. Send 4xx request (backend returns 404 Not Found)
    backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::NOT_FOUND).body("Not Found")).await;
    let req3 = b"GET /not-found HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    total_expected_rx += req3.len();
    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req3).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        total_expected_tx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("404 Not Found"));
    }

    // 5. Send 5xx request (backend returns 500 Internal Server Error)
    backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR).body("Error")).await;
    let req4 = b"GET /server-error HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    total_expected_rx += req4.len();
    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req4).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        total_expected_tx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("500 Internal Server Error"));
    }

    // 6. Check metrics after requests
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    // Connection metrics
    let cx_total = parse_metric_value(metrics, "http_downstream_cx_total").expect("Missing cx_total metric");
    let cx_destroy = parse_metric_value(metrics, "http_downstream_cx_destroy").expect("Missing cx_destroy metric");
    let cx_active = parse_metric_value(metrics, "http_downstream_cx_active").expect("Missing cx_active metric");
    let cx_length_count = parse_metric_value(metrics, "http_downstream_cx_length_ms_count").expect("Missing cx_length_ms_count metric");

    // Request metrics
    let rq_total = parse_metric_value(metrics, "http_downstream_rq_total").expect("Missing rq_total metric");
    let rq_2xx = parse_metric_value(metrics, "http_downstream_rq_2xx").expect("Missing rq_2xx metric");
    let rq_3xx = parse_metric_value(metrics, "http_downstream_rq_3xx").expect("Missing rq_3xx metric");
    let rq_4xx = parse_metric_value(metrics, "http_downstream_rq_4xx").expect("Missing rq_4xx metric");
    let rq_5xx = parse_metric_value(metrics, "http_downstream_rq_5xx").expect("Missing rq_5xx metric");

    // Byte metrics
    let rx_bytes = parse_metric_value(metrics, "http_downstream_cx_rx_bytes_total").expect("Missing rx_bytes metric");
    let tx_bytes = parse_metric_value(metrics, "http_downstream_cx_tx_bytes_total").expect("Missing tx_bytes metric");

    // Assert connection metrics
    assert_eq!(cx_total, 4.0, "Expected exactly 4 downstream HTTP connections");
    assert_eq!(cx_destroy, 4.0, "Expected exactly 4 destroyed downstream HTTP connections");
    assert_eq!(cx_active, 0.0, "Expected 0 active downstream HTTP connections");
    assert_eq!(cx_length_count, 4.0, "Expected exactly 4 recorded connection lengths");

    // Assert request metrics
    assert_eq!(rq_total, 4.0, "Expected exactly 4 downstream HTTP requests");
    assert_eq!(rq_2xx, 1.0, "Expected exactly 1 2xx response");
    assert_eq!(rq_3xx, 1.0, "Expected exactly 1 3xx response");
    assert_eq!(rq_4xx, 1.0, "Expected exactly 1 4xx response");
    assert_eq!(rq_5xx, 1.0, "Expected exactly 1 5xx response");

    // Assert byte metrics with exact mathematical deduction
    assert_eq!(rx_bytes, total_expected_rx as f64, "Expected exact downstream rx bytes");
    assert_eq!(tx_bytes, total_expected_tx as f64, "Expected exact downstream tx bytes");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_http_tls_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("Hello from backend!")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());

    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    // Build the listener manually and call with_tls_inspector()
    let listener = ListenerBuilder::new("https")
        .port(0)
        .with_tls_inspector()
        .filter_chain(
            FilterChainBuilder::new("main")
                .downstream_tls(tls)
                .hcm(HcmBuilder::new().http1().route_config(
                    RouteConfigBuilder::new("routes")
                        .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route("backend"))),
                ))
        );

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr))
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let admin_client = TestClient::new(admin_addr);

    let config = TlsClientConfig::with_root_ca(certs.beefcake_ca_chain()).unwrap();

    {
        let tcp_stream = TcpStream::connect(orion.listener_addr().unwrap()).await.unwrap();
        let mut tls_stream = config.handshake_on(tcp_stream, "dublin.beefcake.example.com").await.unwrap();

        // Write a simple HTTP/1.1 GET request
        let req_bytes = b"GET /hello HTTP/1.1\r\nHost: dublin.beefcake.example.com\r\nConnection: keep-alive\r\n\r\n";
        tls_stream.write_all(req_bytes).await.unwrap();

        let mut buf = [0u8; 1024];
        let n = tls_stream.read(&mut buf).await.unwrap();
        let response_str = String::from_utf8_lossy(&buf[..n]);
        assert!(response_str.contains("200 OK"));

        // Check active TLS connection metric while the connection is still open
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        let ssl_active = parse_metric_value(metrics, "http_downstream_cx_ssl_active").expect("Missing ssl_active metric");
        assert_eq!(ssl_active, 1.0, "Expected exactly 1 active downstream TLS connection");
    } // tls_stream is dropped here, closing the connection

    // Poll until active TLS connection drops to 0
    let mut ssl_active = 1.0;
    for _ in 0..20 {
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        ssl_active = parse_metric_value(metrics, "http_downstream_cx_ssl_active").unwrap_or(0.0);
        if ssl_active == 0.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(ssl_active, 0.0, "Expected active TLS connections to drop to 0");

    // Check total TLS connections
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    let metrics = metrics_resp.body_str().unwrap();
    let ssl_total = parse_metric_value(metrics, "http_downstream_cx_ssl_total").expect("Missing ssl_total metric");
    assert_eq!(ssl_total, 1.0, "Expected exactly 1 total downstream TLS connection");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_http_websocket_upgrade_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    // Create an HCM with websocket upgrades enabled, but disabled on the /no-ws route
    let hcm = HcmBuilder::new().http1().upgrade_websocket().route_config(
        RouteConfigBuilder::new("routes")
            .virtual_host(VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().match_prefix("/no-ws").cluster("backend").disable_websocket_upgrade())
                .route(presets::default_route("backend"))
            ),
    );

    let listener = ListenerBuilder::new("http").port(0).filter_chain(
        FilterChainBuilder::new("main").hcm(hcm)
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr))
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let admin_client = TestClient::new(admin_addr);

    // 1. Test successful WebSocket Upgrade using a raw TcpStream to keep the connection open
    backend.enqueue_response(
        PreConfiguredResponse::with_status(StatusCode::SWITCHING_PROTOCOLS)
            .header("Connection", "Upgrade")
            .header("Upgrade", "websocket")
            .header("Sec-WebSocket-Accept", "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")
    ).await;

    let req = RawHttpRequestBuilder::new()
        .host("localhost")
        .header(b"Upgrade", b"websocket")
        .header(b"Connection", b"Upgrade")
        .header(b"Sec-WebSocket-Key", b"dGhlIHNhbXBsZSBub25jZQ==")
        .header(b"Sec-WebSocket-Version", b"13")
        .build();

    {
        let mut stream = TcpStream::connect(orion.listener_addr().unwrap()).await.expect("Failed to connect");
        stream.write_all(&req).await.expect("Failed to write");

        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.expect("Failed to read");
        let response_str = String::from_utf8_lossy(&buf[..n]);
        assert!(response_str.contains("101 Switching Protocols"));

        // Wait a short moment for the async task to process the upgrade and update metrics
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Check active and total websocket upgrade metrics while the connection is still open
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        let ws_total = parse_metric_value(metrics, "http_downstream_cx_ws_upgrades_total").expect("Missing ws_upgrades_total metric");
        let ws_active = parse_metric_value(metrics, "http_downstream_cx_ws_upgrades_active").expect("Missing ws_upgrades_active metric");

        assert_eq!(ws_total, 1.0, "Expected exactly 1 total websocket upgrade");
        assert_eq!(ws_active, 1.0, "Expected exactly 1 active websocket upgrade");
    } // stream is dropped here, closing the connection

    // Poll until active websocket connection drops to 0
    let mut ws_active = 1.0;
    for _ in 0..20 {
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        ws_active = parse_metric_value(metrics, "http_downstream_cx_ws_upgrades_active").unwrap_or(0.0);
        if ws_active == 0.0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    assert_eq!(ws_active, 0.0, "Expected active websocket connections to drop to 0");

    // 2. Test upgrade request rejected by a non-upgrade route (/no-ws)
    let req_rejected = RawHttpRequestBuilder::new()
        .uri(b"/no-ws")
        .host("localhost")
        .header(b"Upgrade", b"websocket")
        .header(b"Connection", b"Upgrade")
        .header(b"Sec-WebSocket-Key", b"dGhlIHNhbXBsZSBub25jZQ==")
        .header(b"Sec-WebSocket-Version", b"13")
        .build();

    {
        let mut stream = TcpStream::connect(orion.listener_addr().unwrap()).await.expect("Failed to connect");
        stream.write_all(&req_rejected).await.expect("Failed to write");

        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.expect("Failed to read");
        let response_str = String::from_utf8_lossy(&buf[..n]);
        println!("Rejected upgrade response: {}", response_str);
        assert!(response_str.contains("400 Bad Request"));
    }

    // Check that DOWNSTREAM_RQ_WS_ON_NON_WS_ROUTE is incremented
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    let ws_on_non_ws = parse_metric_value(metrics, "http_downstream_rq_ws_on_non_ws_route").expect("Missing ws_on_non_ws_route metric");
    assert_eq!(ws_on_non_ws, 1.0, "Expected exactly 1 upgrade request on non-upgrade route");

    orion.shutdown();
    cleanup_config_file(&config_path);
}
