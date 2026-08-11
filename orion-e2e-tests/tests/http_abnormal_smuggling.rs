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

use std::time::Duration;

use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, RawHttpResponse, SpawnOptions, TcpTestClient,
    TestBackend,
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
async fn test_tc0701_cl_te_smuggling() {
    let (orion, mut backend, tcp_client, config_path) = setup().await;

    // CL-TE smuggling payload:
    // CL says the body is short (covering only the chunked preamble),
    // but TE:chunked contains a second request after the terminator.
    let payload = b"POST /test HTTP/1.1\r\n\
        Host: localhost\r\n\
        Content-Length: 13\r\n\
        Transfer-Encoding: chunked\r\n\
        \r\n\
        0\r\n\
        \r\n\
        GET /smuggled HTTP/1.1\r\n\
        Host: localhost\r\n\
        \r\n";

    let response = tcp_client.send_with_timeout(payload, Duration::from_secs(3)).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");

    // hyper uses TE:chunked for framing (stripping CL per RFC 9112 6.3), so POST /test is
    // forwarded correctly. hyper also sets keep_alive=false when both CL and TE are present,
    // closing the connection after the response - the bytes after the chunked terminator
    // (GET /smuggled) are therefore discarded and never reach Orion's service layer.
    resp.assert_status(200);

    // Backend sees POST /test with no framing ambiguity: hyper stripped CL (TE wins),
    // Orion stripped TE as a hop-by-hop header, and the chunked body decodes to zero bytes.
    let post_req = backend.await_request_with_timeout(Duration::from_secs(1)).await.expect("Expected POST /test");
    assert_eq!(post_req.path(), "/test");
    assert!(
        !(post_req.headers.contains_key("content-length") && post_req.headers.contains_key("transfer-encoding")),
        "backend must not receive both Content-Length and Transfer-Encoding simultaneously"
    );
    assert!(post_req.body.is_empty(), "chunked body 0\\r\\n\\r\\n decodes to zero bytes");

    // GET /smuggled must NOT reach the backend: hyper discards the trailing bytes by closing
    // the connection instead of reading them as a new pipelined request.
    assert!(
        backend.try_recv_request().is_none(),
        "GET /smuggled must not reach the backend: hyper closes the connection on CL+TE requests"
    );

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0703_te_obfuscation() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    // Obfuscated TE with trailing whitespace
    let payload = b"POST /test HTTP/1.1\r\n\
        Host: localhost\r\n\
        Transfer-Encoding: \tchunked \r\n\
        \r\n\
        5\r\nhello\r\n0\r\n\r\n";

    let response = tcp_client.send_with_timeout(payload, Duration::from_secs(3)).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // Orion trims whitespace around the TE value and treats it as chunked (correct per RFC 7230 §3.2.3).
    resp.assert_status(200);

    cleanup(orion, &config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0704_te_case_obfuscation() {
    let (orion, _backend, tcp_client, config_path) = setup().await;

    let payload = b"POST /test HTTP/1.1\r\n\
        Host: localhost\r\n\
        Transfer-Encoding: ChUnKeD\r\n\
        \r\n\
        5\r\nhello\r\n0\r\n\r\n";

    let response = tcp_client.send_with_timeout(payload, Duration::from_secs(3)).await.expect("Failed to send");
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    // RFC 7230 §3.2 requires case-insensitive field-value parsing; Orion accepts "ChUnKeD".
    resp.assert_status(200);

    cleanup(orion, &config_path);
}
