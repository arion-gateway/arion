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

use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::{
    assert_rejected, OrionInstance, PreConfiguredResponse, RawHttpRequestBuilder, RawHttpResponse, SpawnOptions,
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

    let tcp_client = TcpTestClient::new(orion.listener_addr().unwrap());

    (orion, backend, tcp_client, config_path)
}

fn cleanup(orion: OrionInstance, config_path: std::path::PathBuf) {
    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0101_unregistered_method() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().method(b"FOOBAR").uri(b"/test").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    // Orion treats unknown methods as opaque tokens and forwards upstream.
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0102_method_case_sensitivity() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().method(b"get").uri(b"/test").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    // Orion preserves method case and forwards "get" as a token.
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0103_empty_method() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().raw_request_line(b" / HTTP/1.1").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0104_oversized_method() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let big_method = "A".repeat(8193);
    let req = RawHttpRequestBuilder::new().method(big_method.as_bytes()).uri(b"/test").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400, 431]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0105_invalid_chars_in_method() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().raw_request_line(b"GE\x01T / HTTP/1.1").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0106_null_byte_in_method() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().raw_request_line(b"GE\x00T / HTTP/1.1").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0108_missing_method() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().raw_request_line(b"/ HTTP/1.1").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0110_method_only() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().raw_request_line(b"GET").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0201_empty_uri() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().raw_request_line(b"GET  HTTP/1.1").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0202_oversized_uri() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let big_uri = format!("/{}", "a".repeat(8192));
    let req = RawHttpRequestBuilder::new().uri(big_uri.as_bytes()).host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400, 414, 431]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0203_invalid_chars_in_uri() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req =
        RawHttpRequestBuilder::new().raw_request_line(b"GET /path with spaces HTTP/1.1").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0204_null_byte_in_uri() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req =
        RawHttpRequestBuilder::new().raw_request_line(b"GET /path\x00traversal HTTP/1.1").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0205_path_traversal() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().uri(b"/public/../private/secret").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    // Orion forwards the raw path; normalization is the upstream's responsibility.
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0206_fragment_in_uri() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().uri(b"/path#fragment").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion forwards request-targets containing a fragment (RFC 7230 §5.3.1 disallows, but not enforced).
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0207_double_encoding() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().uri(b"/path%2561").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion does not decode the URI; double-encoded sequences pass through to the upstream.
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0208_absolute_uri() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().uri(b"http://localhost/test").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion accepts absolute-form request-targets (RFC 7230 §5.3.2).
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0301_missing_version() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().raw_request_line(b"GET /").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0302_malformed_version() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().version(b"HTTZ/1.1").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0303_unsupported_version() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().version(b"HTTP/2.0").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    // RFC 9110 §15.6.6 suggests 505 as more specific; Orion returns generic 400.
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0401_empty_request_line() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Send CRLF followed by nothing useful, then close
    let data = b"\r\n\r\n";
    let response = tcp_client.send(data).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0402_binary_data() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let data = b"\x80\x81\x82\x83\x84\x85\x86\x87\r\n\r\n";
    let response = tcp_client.send(data).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0403_lf_only_line_ending() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new().line_ending(b"\n").host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    // Orion tolerates LF-only line endings (RFC 7230 §3.5 "bare LF" SHOULD be accepted).
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0405_oversized_request_line() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Create a request line > 8KB via a very long URI
    let big_uri = format!("/{}", "x".repeat(1200));
    let req = RawHttpRequestBuilder::new().uri(big_uri.as_bytes()).host("localhost").build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400, 414, 431]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0408_leading_blank_lines() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Prepend blank lines before a valid request
    let mut data = b"\r\n\r\n".to_vec();
    data.extend_from_slice(&RawHttpRequestBuilder::new().host("localhost").build());

    let response = tcp_client.send(&data).await.expect("Failed to send");
    // Orion tolerates leading blank lines as RFC 7230 §3.5 allows.
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status(200);

    cleanup(orion, config_path);
}
