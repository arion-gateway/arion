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
use orion_e2e_tests::{
    assert_rejected, cleanup_config_file, OrionInstance, PartialSendClient, PreConfiguredResponse,
    RawHttpRequestBuilder, RawHttpResponse, SpawnOptions, TcpTestClient, TestBackend,
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
async fn test_tc0801_truncated_body() {
    let (orion, _backend, _tcp_client, config_path) = setup().await;

    let addr = orion.listener_addr().unwrap();
    let mut client = PartialSendClient::connect(addr).await.expect("Failed to connect");

    let headers = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .content_length(100)
        .include_header_terminator(true)
        .build();

    client.send_bytes(&headers).await.expect("Failed to send headers");
    client.send_bytes(&[b'x'; 50]).await.expect("Failed to send partial body");

    client.shutdown_write().await.expect("Failed to shutdown");
    let response = client.read_response(Duration::from_secs(5)).await.expect("Failed to read");
    // Orion surfaces an upstream-side failure as 503 when the body is truncated.
    assert_rejected(&response, &[503]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0802_excess_body() {
    let (orion, mut backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Content-Length", b"10")
        .body(b"0123456789EXCESS_DATA_SHOULD_BE_IGNORED")
        .build();

    let response = tcp_client.send_with_timeout(&req, Duration::from_secs(3)).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status(200);

    if let Ok(captured) = backend.await_request_with_timeout(Duration::from_secs(1)).await {
        assert!(captured.body.len() <= 10, "Backend received {} bytes, expected at most 10", captured.body.len());
    }

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0803_cl_integer_overflow() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Content-Length", b"99999999999999999999")
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0804_cl_zero_with_body() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Content-Length", b"0")
        .body(b"unexpected body data")
        .build();

    let response = tcp_client.send_with_timeout(&req, Duration::from_secs(3)).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion forwards the request and the excess body is ignored (RFC 7230 §3.3.3 case 4).
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0805_get_with_body() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"GET")
        .uri(b"/test")
        .host("localhost")
        .header(b"Content-Length", b"5")
        .body(b"hello")
        .build();

    let response = tcp_client.send(&req).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // RFC 7230 allows body on any method; Orion forwards GET-with-body as-is.
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0901_non_hex_chunk_size() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Transfer-Encoding", b"chunked")
        .body(b"xyz\r\nhello\r\n0\r\n\r\n")
        .build();

    let response = tcp_client.send_with_timeout(&req, Duration::from_secs(3)).await.expect("Failed to send");
    // Orion fails the upstream body forward and returns 503.
    assert_rejected(&response, &[503]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0902_negative_chunk_size() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Transfer-Encoding", b"chunked")
        .body(b"-5\r\nhello\r\n0\r\n\r\n")
        .build();

    let response = tcp_client.send_with_timeout(&req, Duration::from_secs(3)).await.expect("Failed to send");
    assert_rejected(&response, &[503]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0903_chunk_size_overflow() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Transfer-Encoding", b"chunked")
        .body(b"FFFFFFFFFFFFFFFF\r\nhello\r\n0\r\n\r\n")
        .build();

    let response = tcp_client.send_with_timeout(&req, Duration::from_secs(3)).await.expect("Failed to send");
    assert_rejected(&response, &[400]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0904_missing_terminator_chunk() {
    let (orion, _backend, _tcp_client, config_path) = setup().await;

    let addr = orion.listener_addr().unwrap();
    let mut client = PartialSendClient::connect(addr).await.expect("Failed to connect");

    let headers = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Transfer-Encoding", b"chunked")
        .include_header_terminator(true)
        .build();

    client.send_bytes(&headers).await.expect("Failed to send headers");
    client.send_bytes(b"5\r\nhello\r\n").await.expect("Failed to send chunk");
    // Don't send terminator — close connection
    client.shutdown_write().await.expect("Failed to shutdown");

    let response = client.read_response(Duration::from_secs(5)).await.expect("Failed to read");
    assert_rejected(&response, &[503]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0905_insufficient_chunk_data() {
    let (orion, _backend, _tcp_client, config_path) = setup().await;

    let addr = orion.listener_addr().unwrap();
    let mut client = PartialSendClient::connect(addr).await.expect("Failed to connect");

    let headers = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Transfer-Encoding", b"chunked")
        .include_header_terminator(true)
        .build();

    client.send_bytes(&headers).await.expect("Failed to send headers");
    // Declare 10 bytes but only send 5
    client.send_bytes(b"a\r\nhello").await.expect("Failed to send chunk");
    client.shutdown_write().await.expect("Failed to shutdown");

    let response = client.read_response(Duration::from_secs(5)).await.expect("Failed to read");
    assert_rejected(&response, &[503]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0906_missing_crlf_after_chunk() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Transfer-Encoding", b"chunked")
        // Missing CRLF after "hello" — goes straight to next chunk size
        .body(b"5\r\nhello0\r\n\r\n")
        .build();

    let response = tcp_client.send_with_timeout(&req, Duration::from_secs(3)).await.expect("Failed to send");
    assert_rejected(&response, &[503]);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0907_forbidden_trailer_header() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Transfer-Encoding", b"chunked")
        .header(b"Trailer", b"Host")
        .body(b"5\r\nhello\r\n0\r\nHost: evil.com\r\n\r\n")
        .build();

    let response = tcp_client.send_with_timeout(&req, Duration::from_secs(3)).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion ignores the forbidden Host trailer as per RFC 7230 §4.1.2.
    resp.assert_status(200);

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0908_oversized_chunk_extension() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let big_extension = "x".repeat(8192);
    let chunk_line = format!("5;{big_extension}\r\nhello\r\n0\r\n\r\n");
    let req = RawHttpRequestBuilder::new()
        .method(b"POST")
        .uri(b"/test")
        .host("localhost")
        .header(b"Transfer-Encoding", b"chunked")
        .body(chunk_line.into_bytes())
        .build();

    let response = tcp_client.send_with_timeout(&req, Duration::from_secs(3)).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion tolerates chunk extensions up to the header-size limit.
    resp.assert_status(200);

    cleanup(orion, config_path);
}
