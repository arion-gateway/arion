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

use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, RawHttpResponse, SpawnOptions, TcpTestBackend,
    TcpTestClient, TestBackend,
};

async fn setup() -> (OrionInstance, TestBackend, TcpTestClient, std::path::PathBuf) {
    let backend = TestBackend::start().await.expect("Failed to start backend");
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = presets::simple_proxy("backend", backend.addr());
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default().with_test_admin())
        .await
        .expect("Failed to spawn Orion");
    orion.wait_for_upstream_ready(Duration::from_secs(5)).await.expect("upstream not ready");

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
    let mut backend = TcpTestBackend::start().await.expect("Failed to start TCP backend");
    backend.set_send_after_read(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK".to_vec()).await;
    backend.set_read_timeout(Duration::from_secs(2)).await;

    let bootstrap = presets::simple_proxy("backend", backend.addr());
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default().with_test_admin())
        .await
        .expect("Failed to spawn Orion");
    orion.wait_for_upstream_ready(Duration::from_secs(5)).await.expect("upstream not ready");

    #[allow(clippy::unwrap_used)]
    let tcp_client = TcpTestClient::new(orion.listener_addr().unwrap());

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
    // closing the downstream connection after the response.
    resp.assert_status(200);

    // Inspect raw bytes at the upstream TCP connection to verify the upstream connection closed
    // before the smuggled bytes could arrive. The backend captures everything Orion forwarded;
    // if GET /smuggled is absent, the connection was closed before those bytes were sent.
    let conn =
        backend.await_connection_with_timeout(Duration::from_secs(2)).await.expect("Expected backend connection");
    let received = String::from_utf8_lossy(&conn.received_data);
    assert!(received.contains("POST /test"), "backend must receive the legitimate POST /test");
    assert!(!received.contains("GET /smuggled"), "smuggled request must not reach the backend");

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
