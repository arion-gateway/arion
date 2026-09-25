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

use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, RawHttpRequestBuilder, RawHttpResponse, SpawnOptions,
    TcpTestClient, TestBackend,
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

fn cleanup(orion: OrionInstance, config_path: &std::path::Path) {
    orion.shutdown();
    cleanup_config_file(config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1202_host_header_attack() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().uri(b"/test").header(b"Host", b"evil.attacker.com").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Default vhost matches "*", so an attacker-controlled Host still routes to the backend.
    resp.assert_status(200);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1204_duplicate_header_exploit() {
    let (orion, mut backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .uri(b"/test")
        .host("localhost")
        .header(b"X-Forwarded-For", b"10.0.0.1")
        .header(b"X-Forwarded-For", b"192.168.1.1")
        .header(b"X-Forwarded-For", b"172.16.0.1")
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status(200);

    // Verify the proxy forwarded duplicate X-Forwarded-For headers consistently.
    let captured = backend
        .await_request_with_timeout(std::time::Duration::from_secs(1))
        .await
        .expect("Backend should receive request");
    let xff_values = captured.header_all("x-forwarded-for");
    assert!(!xff_values.is_empty(), "Expected X-Forwarded-For header(s) to be forwarded");

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1205_method_override_header() {
    let (orion, mut backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"X-HTTP-Method-Override", b"DELETE")
        .content_length(0)
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status(200);

    // The proxy must forward the wire-level method, not the override header's value.
    let captured = backend
        .await_request_with_timeout(std::time::Duration::from_secs(1))
        .await
        .expect("Backend should receive request");
    assert_eq!(captured.method, http::Method::POST, "Proxy should forward the actual method, not the override");

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc1206_url_encoding_bypass() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Double-encoded path traversal: %252e%252e = %2e%2e = ..
    let req = RawHttpRequestBuilder::new().uri(b"/public/%252e%252e/private/secret").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion forwards the raw percent-encoded path; normalization is the upstream's responsibility.
    resp.assert_status(200);

    cleanup(orion, &config_path);
}
