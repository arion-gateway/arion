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

use std::time::Duration;

use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::config_builder::{BootstrapBuilder, ClusterBuilder, DownstreamTlsBuilder};
use orion_e2e_tests::{
    assert_rejected, cleanup_config_file, OrionInstance, PartialSendClient, PreConfiguredResponse,
    RawHttpRequestBuilder, RawHttpResponse, SpawnOptions, TcpTestClient, TestBackend, TestCerts, TestClient,
};

async fn setup() -> (OrionInstance, TestBackend, TcpTestClient, std::path::PathBuf) {
    let backend = TestBackend::start().await.expect("Failed to start backend");
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::simple_proxy("backend", backend.addr());
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    #[allow(clippy::unwrap_used)]
    let tcp_client = TcpTestClient::new(orion.listener_addr().unwrap());
    (orion, backend, tcp_client, config_path)
}

fn cleanup(orion: OrionInstance, config_path: std::path::PathBuf) {
    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1001_truncated_request_line() {
    let (orion, _backend, _tcp_client, config_path) = setup().await;

    let addr = orion.listener_addr().unwrap();
    let mut client = PartialSendClient::connect(addr).await.expect("Failed to connect");

    // Send partial request line, then close
    client.send_bytes(b"GE").await.expect("Failed to send");
    client.shutdown_write().await.expect("Failed to shutdown");

    let response = client.read_response(Duration::from_secs(5)).await.expect("Failed to read");
    // Proxy should handle gracefully — either error response or connection close
    assert_rejected(&response, &[400, 408]);

    // Verify proxy is still healthy
    let http_client = TestClient::new(orion.listener_addr().unwrap());
    let resp = http_client.get("/health-check").await.expect("Proxy should still be healthy");
    assert!(resp.status == http::StatusCode::OK || resp.status == http::StatusCode::NOT_FOUND);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1002_truncated_header() {
    let (orion, _backend, _tcp_client, config_path) = setup().await;

    let addr = orion.listener_addr().unwrap();
    let mut client = PartialSendClient::connect(addr).await.expect("Failed to connect");

    // Send request line + partial header, then close
    client.send_bytes(b"GET / HTTP/1.1\r\nHost: local").await.expect("Failed to send");
    client.shutdown_write().await.expect("Failed to shutdown");

    let response = client.read_response(Duration::from_secs(5)).await.expect("Failed to read");
    assert_rejected(&response, &[400, 408]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1003_slowloris() {
    let (orion, _backend, _tcp_client, config_path) = setup().await;

    let addr = orion.listener_addr().unwrap();
    let mut client = PartialSendClient::connect(addr).await.expect("Failed to connect");

    // Send request line character by character with delays
    let request_line = b"GET / HTTP/1.1\r\nHost: localhost\r\n";
    let chunks: Vec<&[u8]> = request_line.iter().map(std::slice::from_ref).collect();

    // Send with 500ms delay between bytes (not full 1s to keep test reasonable)
    client.send_bytes_with_delay(&chunks[..10], Duration::from_millis(500)).await.expect("Failed to send slow bytes");

    // The proxy should eventually time out this connection.
    let response = client.read_response(Duration::from_secs(10)).await.expect("Failed to read");
    assert_rejected(&response, &[400, 408, 504]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1004_no_data_after_connect() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Connect but send nothing — just wait for timeout
    let response =
        tcp_client.receive_on_connect_with_timeout(Duration::from_secs(10)).await.expect("Failed to receive");

    // Proxy should eventually timeout and close the connection
    // Response may be empty (connection closed) or an error
    assert_rejected(&response, &[400, 408]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1005_plaintext_on_tls_port() {
    let backend = TestBackend::start().await.expect("Failed to start backend");
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let certs = TestCerts::new();
    let cert_path = TestCerts::path_to_string(&certs.beefcake_dublin_cert());
    let key_path = TestCerts::path_to_string(&certs.beefcake_dublin_key());
    let tls = DownstreamTlsBuilder::new().cert_files(&cert_path, &key_path);

    let bootstrap = BootstrapBuilder::new()
        .listener(presets::https_listener("https", tls, "backend"))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    // Send plaintext HTTP to the TLS port
    let tcp_client = TcpTestClient::new(orion.listener_addr().unwrap());
    let req = RawHttpRequestBuilder::new().host("localhost").build();
    let response = tcp_client.send_with_timeout(&req, Duration::from_secs(3)).await.expect("Failed to send");

    assert!(
        RawHttpResponse::parse(&response).is_none(),
        "Expected TLS-level rejection but got a parseable HTTP response"
    );

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1006_tls_on_plaintext_port() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Minimal TLS ClientHello (enough to be recognized as TLS)
    let tls_client_hello: &[u8] = &[
        0x16, 0x03, 0x01, // ContentType: Handshake, Version: TLS 1.0
        0x00, 0x05, // Length: 5 bytes
        0x01, // HandshakeType: ClientHello
        0x00, 0x00, 0x01, // Length: 1
        0x03, // dummy
    ];

    let response =
        tcp_client.send_with_timeout(tls_client_hello, Duration::from_secs(3)).await.expect("Failed to send");

    // HTTP parser should reject this as invalid HTTP
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1007_http_pipelining() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Send 3 pipelined requests
    let mut data = Vec::new();
    for i in 1..=3 {
        data.extend_from_slice(
            &RawHttpRequestBuilder::new().uri(format!("/request-{i}").as_bytes()).host("localhost").build(),
        );
    }

    let response = tcp_client.send_with_timeout(&data, Duration::from_secs(5)).await.expect("Failed to send");

    // We should get at least one valid response
    let resp = RawHttpResponse::parse(&response).expect("Expected at least one response");
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1008_keep_alive_limit() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Send 100 sequential requests on one connection
    let single_req = RawHttpRequestBuilder::new().host("localhost").build();
    let mut data = Vec::new();
    for _ in 0..100 {
        data.extend_from_slice(&single_req);
    }

    let response = tcp_client.send_with_timeout(&data, Duration::from_secs(10)).await.expect("Failed to send");

    // Should get responses (possibly not all 100 if there's a limit)
    let resp = RawHttpResponse::parse(&response).expect("Expected at least one response");
    resp.assert_status(200);

    cleanup(orion, config_path);
}
