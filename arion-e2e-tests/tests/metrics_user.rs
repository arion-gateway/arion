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

use http::{HeaderName, StatusCode};
use pingora::prelude::fast_timeout::fast_timeout;
use smallvec::SmallVec;
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use arion_configuration::config::metrics::{CustomMetrics, MetricsConfig, PartitionKey, SourceHeaderNameOrSni};
use arion_e2e_tests::config_builder::{
    presets, BootstrapBuilder, ClusterBuilder, DownstreamTlsBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use arion_e2e_tests::{
    cleanup_config_file, ArionInstance, PortBlock, PreConfiguredResponse, SpawnOptions, TestBackend, TestCerts,
    TestClient, TlsClientConfig,
};

use std::collections::HashMap;

#[allow(clippy::indexing_slicing)]
#[allow(clippy::string_slice)]
fn parse_user_metric_value(prometheus_output: &str, metric_name: &str, expected_attrs: &[(&str, &str)]) -> Option<u64> {
    let prefix = format!("{metric_name}{{");
    for line in prometheus_output.lines() {
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with(&prefix) {
            if let Some(brace_end) = line.find('}') {
                let attrs_str = &line[prefix.len()..brace_end];
                let mut actual_attrs = HashMap::new();
                for attr_pair in attrs_str.split(',') {
                    let parts: Vec<&str> = attr_pair.splitn(2, '=').collect();
                    if parts.len() == 2 {
                        let key = parts[0].trim();
                        let val = parts[1].trim().trim_matches('"');
                        actual_attrs.insert(key, val);
                    }
                }

                if actual_attrs.len() == expected_attrs.len() {
                    let mut all_match = true;
                    for &(k, v) in expected_attrs {
                        if actual_attrs.get(k) != Some(&v) {
                            all_match = false;
                            break;
                        }
                    }
                    if all_match {
                        let after_brace = &line[brace_end + 1..];
                        let parts: Vec<&str> = after_brace.split_whitespace().collect();
                        if let Some(val_str) = parts.last() {
                            if let Ok(val) = val_str.parse::<u64>() {
                                return Some(val);
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

#[tokio::test]
#[ignore]
#[allow(clippy::too_many_lines)]
async fn test_user_metrics_header() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    // Configure user key extraction from header "x-user-id"
    let metrics_config = MetricsConfig {
        user_key: Some(PartitionKey {
            source: SourceHeaderNameOrSni::HeaderName(HeaderName::from_static("x-user-id")),
            attribute_name: Some("user".into()),
        }),
        custom_keys: SmallVec::new(),
        rename: std::collections::HashMap::new(),
        custom_metrics: CustomMetrics::default(),
    };

    let bootstrap =
        presets::simple_proxy("backend", backend_addr).admin("127.0.0.1", admin_port).metrics(metrics_config);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let admin_client = TestClient::new(admin_addr);
    let listener_addr = arion.listener_addr().unwrap();

    let mut user1_expected_tx = 0;
    let mut user1_expected_rx = 0;

    // --- USER 1: Multiple requests of different types to verify aggregation ---
    // 1. Two 2xx requests
    backend.set_default_response(PreConfiguredResponse::with_body("Hello user-1!")).await;
    for _ in 0..2 {
        let req = b"GET /ok HTTP/1.1\r\nHost: localhost\r\nx-user-id: user-1\r\nConnection: close\r\n\r\n";
        user1_expected_tx += req.len();
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        user1_expected_rx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"))
    }

    // 2. One 3xx request
    backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::FOUND).body("Redirecting...")).await;
    {
        let req = b"GET /redirect HTTP/1.1\r\nHost: localhost\r\nx-user-id: user-1\r\nConnection: close\r\n\r\n";
        user1_expected_tx += req.len();
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        user1_expected_rx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("302 Found"))
    }

    // 3. Two 4xx requests (user errors)
    for _ in 0..2 {
        backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::NOT_FOUND).body("Not Found")).await;
        let req = b"GET /not-found HTTP/1.1\r\nHost: localhost\r\nx-user-id: user-1\r\nConnection: close\r\n\r\n";
        user1_expected_tx += req.len();
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        user1_expected_rx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("404 Not Found"))
    }

    // 4. One 5xx request (system error)
    backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR).body("Error")).await;
    {
        let req = b"GET /server-error HTTP/1.1\r\nHost: localhost\r\nx-user-id: user-1\r\nConnection: close\r\n\r\n";
        user1_expected_tx += req.len();
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        user1_expected_rx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("500 Internal Server Error"))
    }

    // 5. Two 429 requests (throttles)
    for _ in 0..2 {
        backend
            .enqueue_response(PreConfiguredResponse::with_status(StatusCode::TOO_MANY_REQUESTS).body("Throttled"))
            .await;
        let req = b"GET /throttled HTTP/1.1\r\nHost: localhost\r\nx-user-id: user-1\r\nConnection: close\r\n\r\n";
        user1_expected_tx += req.len();
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        user1_expected_rx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("429 Too Many Requests"))
    }

    let mut user2_expected_tx = 0;
    let mut user2_expected_rx = 0;

    // --- USER 2: Independent requests to verify separate partitioning ---
    // One 2xx request and one 4xx request
    backend.set_default_response(PreConfiguredResponse::with_body("Hello user-2!")).await;
    {
        let req = b"GET /ok HTTP/1.1\r\nHost: localhost\r\nx-user-id: user-2\r\nConnection: close\r\n\r\n";
        user2_expected_tx += req.len();
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        user2_expected_rx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"))
    }
    backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::BAD_REQUEST).body("Bad Request")).await;
    {
        let req = b"GET /bad HTTP/1.1\r\nHost: localhost\r\nx-user-id: user-2\r\nConnection: close\r\n\r\n";
        user2_expected_tx += req.len();
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        user2_expected_rx += resp.len();
        assert!(String::from_utf8_lossy(&resp).contains("400 Bad Request"))
    }

    // --- VERIFY METRICS ---
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    // Assertions for USER 1 (aggregated values)
    // 2 (2xx) + 1 (3xx) + 2 (4xx) + 1 (5xx) = 6 invocations (429 is throttled and doesn't count as invocation)
    assert_eq!(parse_user_metric_value(metrics, "user_invocations", &[("user", "user-1")]), Some(8));
    assert_eq!(parse_user_metric_value(metrics, "user_throttles", &[("user", "user-1")]), Some(2));
    assert_eq!(parse_user_metric_value(metrics, "user_http_2xx_response", &[("user", "user-1")]), Some(2));
    assert_eq!(parse_user_metric_value(metrics, "user_http_3xx_response", &[("user", "user-1")]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_http_4xx_response", &[("user", "user-1")]), Some(4)); // 2 (404) + 2 (429) = 4
    assert_eq!(parse_user_metric_value(metrics, "user_http_5xx_response", &[("user", "user-1")]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_user_errors", &[("user", "user-1")]), Some(4)); // 2 (404) + 2 (429) = 4
    assert_eq!(parse_user_metric_value(metrics, "user_system_errors", &[("user", "user-1")]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_total_errors", &[("user", "user-1")]), Some(5)); // 4 (user) + 1 (system) = 5
    assert_eq!(parse_user_metric_value(metrics, "user_latency_count", &[("user", "user-1")]), Some(8)); // All 8 requests
    assert_eq!(
        parse_user_metric_value(metrics, "user_bytes_tx", &[("user", "user-1"), ("listener", "http")]),
        Some(user1_expected_tx as u64)
    );
    assert_eq!(
        parse_user_metric_value(metrics, "user_bytes_rx", &[("user", "user-1"), ("listener", "http")]),
        Some(user1_expected_rx as u64)
    );
    assert_eq!(parse_user_metric_value(metrics, "user_connections", &[("user", "user-1")]), Some(8));
    assert!(parse_user_metric_value(metrics, "user_connections_active", &[("user", "user-1")]).unwrap_or(0) == 0);

    // Assertions for USER 2 (separate partition)
    assert_eq!(parse_user_metric_value(metrics, "user_invocations", &[("user", "user-2")]), Some(2));
    assert_eq!(parse_user_metric_value(metrics, "user_throttles", &[("user", "user-2")]), None);
    assert_eq!(parse_user_metric_value(metrics, "user_http_2xx_response", &[("user", "user-2")]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_http_4xx_response", &[("user", "user-2")]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_user_errors", &[("user", "user-2")]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_total_errors", &[("user", "user-2")]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_latency_count", &[("user", "user-2")]), Some(2));
    assert_eq!(
        parse_user_metric_value(metrics, "user_bytes_tx", &[("user", "user-2"), ("listener", "http")]),
        Some(user2_expected_tx as u64)
    );
    assert_eq!(
        parse_user_metric_value(metrics, "user_bytes_rx", &[("user", "user-2"), ("listener", "http")]),
        Some(user2_expected_rx as u64)
    );
    assert_eq!(parse_user_metric_value(metrics, "user_connections", &[("user", "user-2")]), Some(2));
    assert!(parse_user_metric_value(metrics, "user_connections_active", &[("user", "user-2")]).unwrap_or(0) == 0);

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
#[allow(clippy::too_many_lines)]
async fn test_user_metrics_sni() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default().ok();
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
    let listener = ListenerBuilder::new("https").port(0).with_tls_inspector().filter_chain(
        FilterChainBuilder::new("main").downstream_tls(tls).hcm(
            HcmBuilder::new().http1().route_config(
                RouteConfigBuilder::new("routes")
                    .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route("backend"))),
            ),
        ),
    );

    // Configure user key extraction from SNI
    let metrics_config = MetricsConfig {
        user_key: Some(PartitionKey { source: SourceHeaderNameOrSni::Sni, attribute_name: Some("user".into()) }),
        custom_keys: SmallVec::new(),
        rename: std::collections::HashMap::new(),
        custom_metrics: CustomMetrics::default(),
    };

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr))
        .admin("127.0.0.1", admin_port)
        .metrics(metrics_config);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let admin_client = TestClient::new(admin_addr);
    let config = TlsClientConfig::with_root_ca(certs.beefcake_ca_chain()).unwrap();

    let sni_name = "dublin.beefcake.example.com";
    let listener_addr = arion.listener_addr().unwrap();

    let mut expected_tx = 0;
    let mut expected_rx = 0;

    // --- SNI USER: Multiple requests of different types to verify aggregation ---
    // 1. Two 2xx requests
    for _ in 0..2 {
        let tcp_stream = TcpStream::connect(listener_addr).await.unwrap();
        let mut tls_stream = config.handshake_on(tcp_stream, sni_name).await.unwrap();
        let req_bytes = b"GET /hello HTTP/1.1\r\nHost: dublin.beefcake.example.com\r\nConnection: close\r\n\r\n";
        expected_tx += req_bytes.len();
        tls_stream.write_all(req_bytes).await.unwrap();
        let mut buf = [0u8; 1024];
        let n = tls_stream.read(&mut buf).await.unwrap();
        expected_rx += n;
        assert!(String::from_utf8_lossy(&buf[..n]).contains("200 OK"))
    }

    // 2. One 3xx request
    backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::FOUND).body("Redirecting...")).await;
    {
        let tcp_stream = TcpStream::connect(listener_addr).await.unwrap();
        let mut tls_stream = config.handshake_on(tcp_stream, sni_name).await.unwrap();
        let req_bytes = b"GET /redirect HTTP/1.1\r\nHost: dublin.beefcake.example.com\r\nConnection: close\r\n\r\n";
        expected_tx += req_bytes.len();
        tls_stream.write_all(req_bytes).await.unwrap();
        let mut buf = [0u8; 1024];
        let n = tls_stream.read(&mut buf).await.unwrap();
        expected_rx += n;
        assert!(String::from_utf8_lossy(&buf[..n]).contains("302 Found"))
    }

    // 3. Two 4xx requests
    for _ in 0..2 {
        backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::NOT_FOUND).body("Not Found")).await;
        let tcp_stream = TcpStream::connect(listener_addr).await.unwrap();
        let mut tls_stream = config.handshake_on(tcp_stream, sni_name).await.unwrap();
        let req_bytes = b"GET /not-found HTTP/1.1\r\nHost: dublin.beefcake.example.com\r\nConnection: close\r\n\r\n";
        expected_tx += req_bytes.len();
        tls_stream.write_all(req_bytes).await.unwrap();
        let mut buf = [0u8; 1024];
        let n = tls_stream.read(&mut buf).await.unwrap();
        expected_rx += n;
        assert!(String::from_utf8_lossy(&buf[..n]).contains("404 Not Found"))
    }

    // 4. One 5xx request
    backend.enqueue_response(PreConfiguredResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR).body("Error")).await;
    {
        let tcp_stream = TcpStream::connect(listener_addr).await.unwrap();
        let mut tls_stream = config.handshake_on(tcp_stream, sni_name).await.unwrap();
        let req_bytes = b"GET /server-error HTTP/1.1\r\nHost: dublin.beefcake.example.com\r\nConnection: close\r\n\r\n";
        expected_tx += req_bytes.len();
        tls_stream.write_all(req_bytes).await.unwrap();
        let mut buf = [0u8; 1024];
        let n = tls_stream.read(&mut buf).await.unwrap();
        expected_rx += n;
        assert!(String::from_utf8_lossy(&buf[..n]).contains("500 Internal Server Error"))
    }

    // 5. Two 429 requests
    for _ in 0..2 {
        backend
            .enqueue_response(PreConfiguredResponse::with_status(StatusCode::TOO_MANY_REQUESTS).body("Throttled"))
            .await;
        let tcp_stream = TcpStream::connect(listener_addr).await.unwrap();
        let mut tls_stream = config.handshake_on(tcp_stream, sni_name).await.unwrap();
        let req_bytes = b"GET /throttled HTTP/1.1\r\nHost: dublin.beefcake.example.com\r\nConnection: close\r\n\r\n";
        expected_tx += req_bytes.len();
        tls_stream.write_all(req_bytes).await.unwrap();
        let mut buf = [0u8; 1024];
        let n = tls_stream.read(&mut buf).await.unwrap();
        expected_rx += n;
        assert!(String::from_utf8_lossy(&buf[..n]).contains("429 Too Many Requests"))
    }

    // --- VERIFY METRICS ---
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    // Assertions for SNI USER (aggregated values)
    // 2 (2xx) + 1 (3xx) + 2 (4xx) + 1 (5xx) = 6 invocations (429 is throttled and doesn't count as invocation)
    assert_eq!(parse_user_metric_value(metrics, "user_invocations", &[("user", sni_name)]), Some(8));
    assert_eq!(parse_user_metric_value(metrics, "user_throttles", &[("user", sni_name)]), Some(2));
    assert_eq!(parse_user_metric_value(metrics, "user_http_2xx_response", &[("user", sni_name)]), Some(2));
    assert_eq!(parse_user_metric_value(metrics, "user_http_3xx_response", &[("user", sni_name)]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_http_4xx_response", &[("user", sni_name)]), Some(4)); // 2 (404) + 2 (429) = 4
    assert_eq!(parse_user_metric_value(metrics, "user_http_5xx_response", &[("user", sni_name)]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_user_errors", &[("user", sni_name)]), Some(4)); // 2 (404) + 2 (429) = 4
    assert_eq!(parse_user_metric_value(metrics, "user_system_errors", &[("user", sni_name)]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_total_errors", &[("user", sni_name)]), Some(5)); // 4 (user) + 1 (system) = 5
    assert_eq!(parse_user_metric_value(metrics, "user_latency_count", &[("user", sni_name)]), Some(8)); // All 8 requests
    assert!(
        parse_user_metric_value(metrics, "user_bytes_tx", &[("user", sni_name), ("listener", "https")]).unwrap()
            > expected_tx as u64
    );
    assert!(
        parse_user_metric_value(metrics, "user_bytes_rx", &[("user", sni_name), ("listener", "https")]).unwrap()
            > expected_rx as u64
    );
    assert_eq!(parse_user_metric_value(metrics, "user_connections", &[("user", sni_name)]), Some(8));
    assert!(parse_user_metric_value(metrics, "user_connections_active", &[("user", sni_name)]).unwrap_or(0) == 0);

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_user_metrics_websocket() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    // Create a custom TCP backend to handle the WebSocket upgrade handshake and echo data
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let backend_addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            if let Ok(n) = stream.read(&mut buf).await {
                let req_str = String::from_utf8_lossy(&buf[..n]);
                if req_str.to_lowercase().contains("upgrade: websocket") {
                    let response = "HTTP/1.1 101 Switching Protocols\r\n\
                                    Connection: Upgrade\r\n\
                                    Upgrade: websocket\r\n\
                                    Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n";
                    stream.write_all(response.as_bytes()).await.unwrap();

                    // Echo loop
                    let mut buf = [0u8; 1024];
                    while let Ok(n) = stream.read(&mut buf).await {
                        if n == 0 {
                            break;
                        }
                        if stream.write_all(&buf[..n]).await.is_err() {
                            break;
                        }
                    }
                }
            }
        }
    });

    // Configure user key extraction from header "x-user-id"
    let metrics_config = MetricsConfig {
        user_key: Some(PartitionKey {
            source: SourceHeaderNameOrSni::HeaderName(HeaderName::from_static("x-user-id")),
            attribute_name: Some("user".into()),
        }),
        custom_keys: SmallVec::new(),
        rename: std::collections::HashMap::new(),
        custom_metrics: CustomMetrics::default(),
    };

    let hcm = HcmBuilder::new().http1().upgrade_websocket().route_config(
        RouteConfigBuilder::new("routes")
            .virtual_host(VirtualHostBuilder::new("default").route(presets::default_route("backend"))),
    );

    let listener_builder = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm));

    let bootstrap = BootstrapBuilder::new()
        .listener(listener_builder)
        .cluster(ClusterBuilder::with_endpoint("backend", backend_addr))
        .admin("127.0.0.1", admin_port)
        .metrics(metrics_config);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let admin_client = TestClient::new(admin_addr);
    let user_name = "ws-user";

    // Send WebSocket upgrade request
    let req = format!(
        "GET /ws HTTP/1.1\r\n\
         Host: localhost\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\
         x-user-id: {user_name}\r\n\r\n",
    );

    {
        let mut stream = TcpStream::connect(arion.listener_addr().unwrap()).await.expect("Failed to connect");
        stream.write_all(req.as_bytes()).await.expect("Failed to write handshake");

        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await.expect("Failed to read handshake response");
        let response_str = String::from_utf8_lossy(&buf[..n]);
        assert!(response_str.contains("101 Switching Protocols"));

        // Write 10 bytes of data over the upgraded connection
        let data = b"0123456789";
        stream.write_all(data).await.expect("Failed to write data");

        // Read 10 bytes back (echoed by backend)
        let mut echo_buf = [0u8; 10];
        stream.read_exact(&mut echo_buf).await.expect("Failed to read echoed data");
        assert_eq!(&echo_buf, data);

        // Wait a short moment for the async task to process the upgrade and update metrics
        tokio::time::sleep(std::time::Duration::from_millis(100)).await
    }

    // Check metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    let inbound_streaming =
        parse_user_metric_value(metrics, "user_inbound_streaming_bytes_processed", &[("user", user_name)])
            .expect("Missing user_inbound_streaming_bytes_processed");
    let outbound_streaming =
        parse_user_metric_value(metrics, "user_outbound_streaming_bytes_processed", &[("user", user_name)])
            .expect("Missing user_outbound_streaming_bytes_processed");

    assert_eq!(inbound_streaming, 10);
    assert_eq!(outbound_streaming, 10);
    assert_eq!(parse_user_metric_value(metrics, "user_connections", &[("user", user_name)]), Some(1));
    assert!(parse_user_metric_value(metrics, "user_connections_active", &[("user", user_name)]).unwrap_or(0) == 0);

    arion.shutdown();
    cleanup_config_file(&config_path);
}

/// Verifies that `user_new_connections` counts each new connection and that
/// `user_connections` (active connections gauge) is incremented while a connection
/// is alive and decremented to zero after the connection is closed.
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_user_connections_active_gauge() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.enqueue_response(PreConfiguredResponse::with_body("ok")).await;
    backend.enqueue_response(PreConfiguredResponse::with_body("ok")).await;

    let metrics_config = MetricsConfig {
        user_key: Some(PartitionKey {
            source: SourceHeaderNameOrSni::HeaderName(HeaderName::from_static("x-user-id")),
            attribute_name: Some("user".into()),
        }),
        custom_keys: SmallVec::new(),
        rename: std::collections::HashMap::new(),
        custom_metrics: CustomMetrics::default(),
    };

    let bootstrap =
        presets::simple_proxy("backend", backend_addr).admin("127.0.0.1", admin_port).metrics(metrics_config);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    let admin_client = TestClient::new(admin_addr);
    let listener_addr = arion.listener_addr().unwrap();

    // --- Open connection 1 and keep it alive ---
    let req1 = b"GET /ok HTTP/1.1\r\nHost: localhost\r\nx-user-id: conn-user\r\n\r\n";
    let mut stream1 = TcpStream::connect(listener_addr).await.expect("Failed to connect");
    stream1.write_all(req1).await.expect("Failed to write");

    // Read just enough to confirm the request was processed
    let mut buf = [0u8; 1024];
    let n = fast_timeout(std::time::Duration::from_secs(5), stream1.read(&mut buf))
        .await
        .expect("Timed out waiting for response")
        .expect("Failed to read");
    assert!(String::from_utf8_lossy(&buf[..n]).contains("200 OK"));

    // While stream1 is still open, verify the active connections gauge is 1
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();
    assert_eq!(parse_user_metric_value(metrics, "user_connections", &[("user", "conn-user")]), Some(1));
    assert_eq!(parse_user_metric_value(metrics, "user_connections_active", &[("user", "conn-user")]), Some(1));

    // --- Open connection 2 without reading yet ---
    let req2 = b"GET /ok HTTP/1.1\r\nHost: localhost\r\nx-user-id: conn-user\r\n\r\n";
    let mut stream2 = TcpStream::connect(listener_addr).await.expect("Failed to connect");
    stream2.write_all(req2).await.expect("Failed to write");

    // Give the server a moment to process
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Now both connections are alive — gauge should be 2
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();
    assert_eq!(parse_user_metric_value(metrics, "user_connections", &[("user", "conn-user")]), Some(2));
    assert_eq!(parse_user_metric_value(metrics, "user_connections_active", &[("user", "conn-user")]), Some(2));

    // --- Close connection 1, gauge should drop to 1 ---
    drop(stream1);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();
    assert_eq!(parse_user_metric_value(metrics, "user_connections_active", &[("user", "conn-user")]), Some(1));

    // --- Close connection 2, gauge should drop to 0 ---
    drop(stream2);
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();
    assert_eq!(parse_user_metric_value(metrics, "user_connections_active", &[("user", "conn-user")]), Some(0));
    // The counter is monotonic — it should still be 2, not go back to 0
    assert_eq!(parse_user_metric_value(metrics, "user_connections", &[("user", "conn-user")]), Some(2));

    arion.shutdown();
    cleanup_config_file(&config_path);
}
