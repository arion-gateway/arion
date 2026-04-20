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
    OrionInstance, PreConfiguredResponse, RawHttpResponse, SpawnOptions, TcpTestBackend, TcpTestClient, TestBackend,
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

async fn setup_with_tcp_backend() -> (OrionInstance, TcpTestBackend, TcpTestClient, std::path::PathBuf) {
    let backend = TcpTestBackend::start().await.expect("Failed to start TCP backend");
    // Configure TCP backend to send a minimal HTTP response
    backend.set_send_on_connect(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK".to_vec()).await;
    backend.set_read_timeout(Duration::from_secs(2)).await;

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
async fn test_tc0701_cl_te_smuggling() {
    let (orion, mut backend, tcp_client, config_path) = setup_with_tcp_backend().await;

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

    // Orion rejects the ambiguous CL+TE request per RFC 7230 §3.3.3.
    // It returns 400 when the frame parser catches the conflict, or 502 when the upstream
    // connection aborts — both indicate safe rejection. The smuggled request must not reach the backend.
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status_in(&[400, 502]);

    // Verify backend did NOT receive a smuggled request to /smuggled.
    let conn = backend.await_connection_with_timeout(Duration::from_secs(1)).await;
    if let Ok(conn) = conn {
        let received = String::from_utf8_lossy(&conn.received_data);
        assert!(!received.contains("/smuggled"), "Backend received smuggled request! Data: {received}");
    }

    cleanup(orion, config_path);
}

#[tokio::test]
#[ignore]
async fn test_tc0702_te_cl_smuggling() {
    let (orion, mut backend, tcp_client, config_path) = setup_with_tcp_backend().await;

    // TE-CL payload: TE takes priority, body is chunked.
    // After the chunk terminator (0\r\n\r\n), we inject a second request.
    let payload = b"POST /test HTTP/1.1\r\n\
        Host: localhost\r\n\
        Transfer-Encoding: chunked\r\n\
        Content-Length: 50\r\n\
        \r\n\
        5\r\n\
        hello\r\n\
        0\r\n\
        \r\n\
        GET /smuggled HTTP/1.1\r\n\
        Host: localhost\r\n\
        \r\n";

    let response = tcp_client.send_with_timeout(payload, Duration::from_secs(3)).await.expect("Failed to send");

    // Orion prioritizes Transfer-Encoding over Content-Length and forwards the chunked body;
    // the smuggled request after 0\r\n\r\n is not forwarded to the backend.
    let resp = RawHttpResponse::parse(&response).expect("Expected a response");
    resp.assert_status(200);

    // Verify no smuggled request reached the backend.
    let conn = backend.await_connection_with_timeout(Duration::from_secs(1)).await;
    if let Ok(conn) = conn {
        let received = String::from_utf8_lossy(&conn.received_data);
        assert!(!received.contains("/smuggled"), "Backend received smuggled request! Data: {received}");
    }

    cleanup(orion, config_path);
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

    cleanup(orion, config_path);
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

    cleanup(orion, config_path);
}
