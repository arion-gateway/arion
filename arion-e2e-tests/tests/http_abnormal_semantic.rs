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

#![allow(clippy::expect_used, reason = "test infrastructure — panicking on setup failure is intentional")]

use std::time::Duration;

use arion_e2e_tests::config_builder::presets;
use arion_e2e_tests::{
    cleanup_config_file, ArionInstance, PartialSendClient, PreConfiguredResponse, RawHttpRequestBuilder,
    RawHttpResponse, SpawnOptions, TcpTestClient, TestBackend,
};

async fn setup() -> (ArionInstance, TestBackend, TcpTestClient, std::path::PathBuf) {
    let backend = TestBackend::start().await.expect("Failed to start backend");
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::simple_proxy("backend", backend.addr());
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Arion");

    #[allow(clippy::unwrap_used)]
    let tcp_client = TcpTestClient::new(arion.listener_addr().unwrap());
    (arion, backend, tcp_client, config_path)
}

fn cleanup(arion: ArionInstance, config_path: &std::path::Path) {
    arion.shutdown();
    cleanup_config_file(config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1101_content_type_body_mismatch() {
    let (arion, mut backend, tcp_client, config_path) = setup().await;

    let xml_body = b"<root><item>test</item></root>";
    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Content-Type", b"application/json")
        .content_length(xml_body.len())
        .body(xml_body.to_vec())
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Proxy should forward as-is — not its job to validate content-type vs body
    resp.assert_status(200);

    let captured = backend.await_request().await.expect("Backend should receive request");
    assert_eq!(captured.body_str(), Some("<root><item>test</item></root>"));
    assert_eq!(captured.header("content-type"), Some("application/json"));

    cleanup(arion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1102_expect_100_continue() {
    let (arion, _backend, _tcp_client, config_path) = setup().await;

    let addr = arion.listener_addr().unwrap();
    let mut client = PartialSendClient::connect(addr).await.expect("Failed to connect");

    let headers = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Expect", b"100-continue")
        .content_length(5)
        .include_header_terminator(true)
        .build();

    client.send_bytes(&headers).await.expect("Failed to send headers");

    // Wait for 100 Continue or other response
    let response = client.read_response(Duration::from_secs(5)).await.expect("Failed to read");

    let resp = RawHttpResponse::parse(&response).expect("Expected a 100 Continue response");
    // Arion honors Expect: 100-continue and sends an interim 100 before the body is sent.
    resp.assert_status(100);

    client.send_bytes(b"hello").await.expect("Failed to send body");
    let final_response = client.read_response(Duration::from_secs(5)).await.expect("Failed to read");
    let final_resp = RawHttpResponse::parse(&final_response).expect("Expected final response");
    final_resp.assert_status(200);

    cleanup(arion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1103_invalid_expect_value() {
    let (arion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Expect", b"200-ok")
        .content_length(5)
        .body(b"hello")
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Arion ignores unknown Expect values and forwards the request rather than returning 417.
    resp.assert_status(200);

    cleanup(arion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1104_data_after_connection_close() {
    let (arion, _backend, _tcp_client, config_path) = setup().await;

    let addr = arion.listener_addr().unwrap();
    let mut client = PartialSendClient::connect(addr).await.expect("Failed to connect");

    // Send first request with Connection: close
    let first_req = RawHttpRequestBuilder::new().host("localhost").header(b"Connection", b"close").build();
    client.send_bytes(&first_req).await.expect("Failed to send first request");

    // Read first response
    let response = client.read_response(Duration::from_secs(3)).await.expect("Failed to read");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status(200);

    // Try to send another request on the same connection
    let second_req = RawHttpRequestBuilder::new().uri(b"/second").host("localhost").build();
    // This may fail (broken pipe) which is expected — the connection should be closed
    let _broken_pipe = client.send_bytes(&second_req).await;

    cleanup(arion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1105_upgrade_to_unknown_protocol() {
    let (arion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .host("localhost")
        .header(b"Upgrade", b"foobar-protocol/1.0")
        .header(b"Connection", b"Upgrade")
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Arion refuses the upgrade to an unknown protocol with 403.
    resp.assert_status(403);

    cleanup(arion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1106_http10_without_host() {
    let (arion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .version(b"HTTP/1.0")
        // Deliberately omit Host header
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Arion returns 404 (no virtual host matches the empty authority) rather than 400.
    resp.assert_status(404);

    cleanup(arion, &config_path);
}
