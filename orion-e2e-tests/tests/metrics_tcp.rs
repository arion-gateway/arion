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

use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, FilterChainBuilder, ListenerBuilder, TcpProxyBuilder,
};
use orion_e2e_tests::{
    cleanup_config_file, parse_metric_value, OrionInstance, PortBlock, SpawnOptions, TcpTestBackend, TestClient,
};

#[tokio::test]
#[ignore]
async fn test_tcp_proxy_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    // Start a TCP backend that echoes back "hello" on connect
    let backend = TcpTestBackend::start().await.expect("Failed to start TCP backend");
    backend.set_send_on_connect(b"hello").await;

    // Configure Orion as a TCP proxy
    let listener = ListenerBuilder::new("tcp")
        .port(0)
        .filter_chain(FilterChainBuilder::new("main").tcp_proxy(TcpProxyBuilder::new("tcp_stats").cluster("backend")));

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()))
        .admin("127.0.0.1", admin_port);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let admin_client = TestClient::new(admin_addr);

    // 1. Initial check: TCP metrics should be 0 or not present
    let initial_metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    initial_metrics_resp.assert_status(StatusCode::OK);
    let initial_metrics = initial_metrics_resp.body_str().unwrap();

    let cx_total = parse_metric_value(initial_metrics, "tcp_downstream_cx_total").unwrap_or(0);
    assert_eq!(cx_total, 0, "Initial TCP connections should be 0");

    // 2. Open a TCP connection and keep it open to verify active connection metric
    {
        let mut stream = TcpStream::connect(orion.listener_addr().unwrap()).await.expect("Failed to connect");

        // Read the "hello" sent on connect by the backend
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.expect("Failed to read");
        assert_eq!(&buf, b"hello");

        // Write some bytes to Orion to trigger the rx_bytes metric
        stream.write_all(b"ping").await.expect("Failed to write");

        // Check active connection metric while the connection is still open
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        let cx_active = parse_metric_value(metrics, "tcp_downstream_cx_active").expect("Missing cx_active metric");
        assert_eq!(cx_active, 1, "Expected exactly 1 active TCP connection")
    } // stream is dropped here, closing the connection

    // 3. Open a second TCP connection to verify that the byte counters accumulate correctly
    {
        let mut stream = TcpStream::connect(orion.listener_addr().unwrap()).await.expect("Failed to connect");

        // Read the "hello" sent on connect by the backend
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.expect("Failed to read");
        assert_eq!(&buf, b"hello");

        // Write some bytes to Orion to trigger the rx_bytes metric again
        stream.write_all(b"ping").await.expect("Failed to write")
    } // second stream is dropped here, closing the connection

    // Poll until active TCP connection drops to 0
    let mut cx_active = 1;
    for _ in 0..20 {
        let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
        metrics_resp.assert_status(StatusCode::OK);
        let metrics = metrics_resp.body_str().unwrap();

        cx_active = parse_metric_value(metrics, "tcp_downstream_cx_active").unwrap_or(0);
        if cx_active == 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await
    }

    assert_eq!(cx_active, 0, "Expected active TCP connections to drop to 0");

    // 4. Check final metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    let cx_total = parse_metric_value(metrics, "tcp_downstream_cx_total").expect("Missing cx_total metric");
    let cx_destroy = parse_metric_value(metrics, "tcp_downstream_cx_destroy").expect("Missing cx_destroy metric");
    let cx_length_count =
        parse_metric_value(metrics, "tcp_downstream_cx_length_ms_count").expect("Missing cx_length_ms_count metric");

    let rx_bytes = parse_metric_value(metrics, "tcp_cx_rx_bytes_received").expect("Missing rx_bytes metric");
    let tx_bytes = parse_metric_value(metrics, "tcp_cx_tx_bytes_sent").expect("Missing tx_bytes metric");

    assert_eq!(cx_total, 2, "Expected exactly 2 total downstream TCP connections");
    assert_eq!(cx_destroy, 2, "Expected exactly 2 destroyed downstream TCP connections");
    assert_eq!(cx_length_count, 2, "Expected exactly 2 recorded connection lengths");

    let expected_rx = (b"ping".len() * 2) as u64;
    let expected_tx = (b"hello".len() * 2) as u64;
    assert_eq!(rx_bytes, expected_rx, "Expected exact bytes received");
    assert_eq!(tx_bytes, expected_tx, "Expected exact bytes sent");

    orion.shutdown();
    cleanup_config_file(&config_path);
}
