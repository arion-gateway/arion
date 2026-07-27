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

//! E2E tests verifying that sensitive values are redacted in log files.
//!
//! When Orion is configured with a file log sink, the `RedactingMakeWriter`
//! intercepts every event before it reaches the file. The RBAC filter emits a
//! debug-level line that includes the full request `HeaderMap`, which provides
//! a deterministic vehicle for injecting a secret and asserting its redaction.

#![allow(clippy::expect_used)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, HttpRbacBuilder,
    HttpRbacPolicyBuilder, ListenerBuilder, RouteBuilder, RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PortBlock, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend,
    TestClient,
};

fn find_log_file(dir: &Path, prefix: &str) -> Option<PathBuf> {
    std::fs::read_dir(dir).ok()?.filter_map(Result::ok).find_map(|e| {
        let name = e.file_name();
        name.to_string_lossy().starts_with(prefix).then(|| e.path())
    })
}

async fn wait_for_log_file(dir: &Path, prefix: &str, timeout: Duration) -> PathBuf {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(path) = find_log_file(dir, prefix) {
            if std::fs::metadata(&path).map(|m| m.len() > 0).unwrap_or(false) {
                return path;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for log file with prefix '{prefix}' in {}", dir.display());
}

/// Verifies that the value of a `X-HW-AgentGateway-Workload-Access-Token` header
/// is not stored verbatim in the proxy log file.
///
/// The header name ends with the word "token" (preceded by `-`, followed by `"`
/// in the `HeaderMap` debug representation). The word-boundary + separator rules
/// both match, so the value is replaced with `*` characters.
#[tokio::test]
#[ignore]
async fn test_workload_access_token_header_redacted_in_log_file() {
    let ports = PortBlock::reserve().expect("reserve port block");
    let port = ports.allocate().expect("allocate port");
    let listener_addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse socket addr");

    let backend = TestBackend::start().await.expect("start backend");
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let log_dir = std::env::temp_dir();
    let log_filename = format!("orion-desensitization-workload-{}", std::process::id());

    let rbac =
        HttpRbacBuilder::allow().policy("allow-all", HttpRbacPolicyBuilder::new().permission_any().principal_any());

    let hcm = HcmBuilder::new().http1().http_rbac(rbac).route_config(RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    ));

    let bootstrap = BootstrapBuilder::new()
        .log_file(log_dir.to_str().expect("temp dir is valid UTF-8"), &log_filename)
        .listener(ListenerBuilder::new("http").port(port).filter_chain(FilterChainBuilder::new("main").hcm(hcm)))
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().expect("build config");

    let mut orion =
        OrionInstance::spawn_no_wait(&config_path, listener_addr, SpawnOptions::default().with_log_level("debug"))
            .await
            .expect("spawn orion");

    orion.wait_for_listener(Duration::from_secs(10)).await.expect("orion ready");

    let client = TestClient::new(listener_addr);

    let secret = "gw-secret-workload-token-xyz789";
    client
        .send(RequestBuilder::get("/check").header("x-hw-agentgateway-workload-access-token", secret))
        .await
        .expect("send request")
        .assert_status(StatusCode::OK);

    tokio::time::sleep(Duration::from_millis(500)).await;

    orion.shutdown();
    cleanup_config_file(&config_path);

    let log_path = wait_for_log_file(&log_dir, &log_filename, Duration::from_secs(3)).await;
    let content = std::fs::read_to_string(&log_path).expect("read log file");
    std::fs::remove_file(&log_path).unwrap();

    assert!(
        content.to_lowercase().contains("x-hw-agentgateway-workload-access-token"),
        "the header key should appear in the debug log; got:\n{content}"
    );
    assert!(!content.contains(secret), "the raw secret value must not appear in the log file; got:\n{content}");
    assert!(content.contains("**"), "redaction markers ('**') should appear after the keyword; got:\n{content}");
}

/// Verifies that an `Authorization` header value sent in a request is not stored
/// verbatim in the proxy log file.
///
/// Mechanism: the RBAC filter logs
/// `debug!("Applying authorization rules {rbac:?} {:?}", &req.headers())`
/// which includes the full request `HeaderMap`. With desensitization enabled, the
/// value following the "authorization" keyword is replaced with `*` characters.
///
/// Because file logging redirects all tracing output away from stdout, readiness
/// is detected via TCP polling (`spawn_no_wait` + `wait_for_listener`) rather
/// than the usual stdout-based `spawn_auto_port`.
#[tokio::test]
#[ignore]
async fn test_authorization_header_redacted_in_log_file() {
    let ports = PortBlock::reserve().expect("reserve port block");
    let port = ports.allocate().expect("allocate port");
    let listener_addr: SocketAddr = format!("127.0.0.1:{port}").parse().expect("parse socket addr");

    let backend = TestBackend::start().await.expect("start backend");
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let log_dir = std::env::temp_dir();
    let log_filename = format!("orion-desensitization-test-{}", std::process::id());

    let rbac =
        HttpRbacBuilder::allow().policy("allow-all", HttpRbacPolicyBuilder::new().permission_any().principal_any());

    let hcm = HcmBuilder::new().http1().http_rbac(rbac).route_config(RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    ));

    let bootstrap = BootstrapBuilder::new()
        .log_file(log_dir.to_str().expect("temp dir is valid UTF-8"), &log_filename)
        .listener(ListenerBuilder::new("http").port(port).filter_chain(FilterChainBuilder::new("main").hcm(hcm)))
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().expect("build config");

    // Debug level triggers the RBAC header log line.
    // File logging redirects stdout, so readiness is detected via TCP polling.
    let mut orion =
        OrionInstance::spawn_no_wait(&config_path, listener_addr, SpawnOptions::default().with_log_level("debug"))
            .await
            .expect("spawn orion");

    orion.wait_for_listener(Duration::from_secs(10)).await.expect("orion ready");

    let client = TestClient::new(listener_addr);

    let secret = "eyJhbGciOiJSUzI1NiJ9.superSecretPayload.signature";
    client
        .send(RequestBuilder::get("/check").header("authorization", format!("Bearer {secret}")))
        .await
        .expect("send request")
        .assert_status(StatusCode::OK);

    // Give the non-blocking writer time to drain before the process is killed.
    tokio::time::sleep(Duration::from_millis(500)).await;

    orion.shutdown();
    cleanup_config_file(&config_path);

    let log_path = wait_for_log_file(&log_dir, &log_filename, Duration::from_secs(3)).await;
    let content = std::fs::read_to_string(&log_path).expect("read log file");
    std::fs::remove_file(&log_path).unwrap();

    assert!(
        content.to_lowercase().contains("authorization"),
        "the 'authorization' keyword should appear in the debug log; got:\n{content}"
    );
    assert!(!content.contains(secret), "the raw secret value must not appear in the log file; got:\n{content}");
    assert!(content.contains("**"), "redaction markers ('**') should appear after the keyword; got:\n{content}");
}
