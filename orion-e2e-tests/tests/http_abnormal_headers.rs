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

#![allow(clippy::expect_used, reason = "test infrastructure — panicking on setup failure is intentional")]

use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::{
    assert_rejected, cleanup_config_file, OrionInstance, PreConfiguredResponse, RawHttpRequestBuilder, RawHttpResponse,
    SpawnOptions, TcpTestClient, TestBackend,
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
async fn test_tc0501_empty_header_name() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().host("localhost").raw_header(b": some-value").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0502_space_in_header_name() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().host("localhost").raw_header(b"Invalid Header: value").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0503_invalid_chars_in_header_name() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().host("localhost").raw_header(b"Invalid{Header}: value").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0504_missing_colon() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().host("localhost").raw_header(b"X-No-Colon-Here value").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0505_space_before_colon() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().host("localhost").raw_header(b"X-Custom : value").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0506_control_chars_in_value() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().host("localhost").header(b"X-Custom", b"value\x00with-null").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0507_oversized_header() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let big_value = "x".repeat(65536 * 16);
    let req = RawHttpRequestBuilder::new().host("localhost").header(b"X-Big-Header", big_value.as_bytes()).build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400, 431]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0508_too_many_headers() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let mut builder = RawHttpRequestBuilder::new().host("localhost");
    for i in 0..250 {
        builder = builder.header(format!("X-Header-{i}").into_bytes(), b"value".to_vec());
    }
    let req = builder.build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[431]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0509_total_header_size_exceeded() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Generate many headers whose total size exceeds 64KB
    let value = "x".repeat(4096);
    let mut builder = RawHttpRequestBuilder::new().host("localhost");
    for i in 0..99 {
        builder = builder.header(format!("X-Hdr-{i}").into_bytes(), value.as_bytes().to_vec());
    }
    let req = builder.build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400, 431]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0510_obsolete_line_folding() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req =
        RawHttpRequestBuilder::new().host("localhost").raw_header(b"X-Folded: first-part\r\n second-part").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion rejects obsolete line folding as RFC 7230 §3.2.4 recommends.
    resp.assert_status(400);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0601_missing_host() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Deliberately omit Host header
    let req = RawHttpRequestBuilder::new().build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0602_duplicate_host() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().header(b"Host", b"localhost").header(b"Host", b"other.example.com").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0603_invalid_host_port() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().header(b"Host", b"localhost:abc").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion does not validate the authority component's port; the request is forwarded.
    resp.assert_status(200);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0604_empty_host_value() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().header(b"Host", b"").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion routes the empty-Host request but the upstream connection fails → 502.
    resp.assert_status(502);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0605_non_numeric_content_length() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().host("localhost").header(b"Content-Length", b"abc").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0606_negative_content_length() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().host("localhost").header(b"Content-Length", b"-1").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0607_duplicate_inconsistent_cl() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Content-Length", b"5")
        .header(b"Content-Length", b"10")
        .body(b"hello")
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0608_cl_and_te_coexist() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Content-Length", b"5")
        .header(b"Transfer-Encoding", b"chunked")
        .body(b"5\r\nhello\r\n0\r\n\r\n")
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion prioritizes Transfer-Encoding over Content-Length and forwards.
    // RFC 7230 §3.3.3 allows rejection with 400 as a stricter option — not enforced here.
    resp.assert_status(200);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0609_invalid_transfer_encoding() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Transfer-Encoding", b"gzip")
        .body(b"some data")
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0610_stacked_te_values() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Transfer-Encoding", b"chunked")
        .header(b"Transfer-Encoding", b"chunked")
        .body(b"5\r\nhello\r\n0\r\n\r\n")
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion accepts stacked "chunked, chunked" TE and forwards.
    resp.assert_status(200);

    cleanup(orion, &config_path);
}
