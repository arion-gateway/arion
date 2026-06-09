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

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use base64::Engine;
use http::HeaderName;
use orion_configuration::config::log::AccessLogConfig;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{cleanup_config_file, OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend};

/// Format string with both standard operators and custom operators.
/// The custom operators `%user_id%`, `%session_id%`, and `%app_version%` are registered
/// via `AccessLogConfig.custom_operators` and extracted from the JSON object
/// carried by the `x-orion-metadata` header (configured via `incoming_request_header`).
const CUSTOM_OPERATOR_FORMAT: &str = "\
||START_TIME=%START_TIME%\
||BYTES_RECEIVED=%BYTES_RECEIVED%\
||BYTES_SENT=%BYTES_SENT%\
||REQ(:METHOD)=%REQ(:METHOD)%\
||REQ(:PATH)=%REQ(:PATH)%\
||RESPONSE_CODE=%RESPONSE_CODE%\
||DURATION=%DURATION%\
||USER_ID=%user_id%\
||SESSION_ID=%session_id%\
||APP_VERSION=%app_version%\
\n";

use std::collections::HashMap;

fn parse_log_line(line: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let line = line.strip_prefix("||").unwrap_or(line);
    for segment in line.split("||") {
        if let Some((k, v)) = segment.split_once('=') {
            map.insert(k.to_owned(), v.to_owned());
        }
    }
    map
}

async fn read_log_file(path: &std::path::Path, timeout: Duration) -> String {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Ok(content) = std::fs::read_to_string(path) {
            if !content.trim().is_empty() {
                return content;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for access log file content");
}

/// Creates a base64-encoded JSON value suitable for use in an access-log header.
fn make_metadata_header(user_id: &str, session_id: &str, app_version: &str) -> String {
    let json = serde_json::json!({
        "user_id": user_id,
        "session_id": session_id,
        "app_version": app_version,
    });
    let json_str = serde_json::to_string(&json).expect("Failed to serialize JSON");
    base64::engine::general_purpose::STANDARD.encode(json_str.as_bytes())
}

#[tokio::test]
#[ignore]
async fn test_access_log_custom_operator_incoming_request() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    backend
        .set_default_response(
            PreConfiguredResponse::with_body("Hello!").header("x-custom-response-header", "resp-value"),
        )
        .await;

    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-custom-{}.txt", std::process::id()));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));

    let hcm =
        HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), CUSTOM_OPERATOR_FORMAT).route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default")
                    .route(RouteBuilder::new().name("hello-route").match_prefix("/").cluster("backend")),
            ),
        );

    let listener = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm));

    let access_log_config = AccessLogConfig {
        blocking: true,
        incoming_request_header: Some(HeaderName::from_static("x-orion-metadata")),
        custom_operators: vec![
            smol_str::SmolStr::new("user_id"),
            smol_str::SmolStr::new("session_id"),
            smol_str::SmolStr::new("app_version"),
        ],
        ..AccessLogConfig::default()
    };

    let bootstrap = BootstrapBuilder::new().listener(listener).cluster(cluster).access_log(access_log_config);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default().with_verbose())
        .await
        .expect("Failed to spawn Orion");

    let listener_addr = orion.listener_addr().expect("Missing listener address");

    // Build the base64-encoded JSON header value
    let json_value = make_metadata_header("user-123", "sess-456", "2.1.0");

    // Send a GET request with the custom header containing base64-encoded JSON
    let req = format!(
        "GET /hello HTTP/1.1\r\nHost: localhost\r\nx-orion-metadata: {json_value}\r\nConnection: close\r\n\r\n"
    );

    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req.as_bytes()).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        let resp_str = String::from_utf8_lossy(&resp);
        assert!(resp_str.contains("200 OK"), "Unexpected response: {resp_str}");
    };

    tokio::time::sleep(Duration::from_millis(500)).await;

    let log_content = read_log_file(&log_path, Duration::from_secs(5)).await;
    let log_line = log_content.lines().next().expect("No log line found");
    let parsed = parse_log_line(log_line);

    // Standard operators should work as usual
    assert!(!parsed.get("START_TIME").expect("START_TIME missing").is_empty());
    assert_eq!(parsed.get("REQ(:METHOD)").map(String::as_str), Some("GET"));
    assert_eq!(parsed.get("REQ(:PATH)").map(String::as_str), Some("/hello"));
    assert_eq!(parsed.get("RESPONSE_CODE").map(String::as_str), Some("200"));

    // Custom operators extracted from the x-orion-metadata header JSON
    assert_eq!(parsed.get("USER_ID").map(String::as_str), Some("user-123"));
    assert_eq!(parsed.get("SESSION_ID").map(String::as_str), Some("sess-456"));
    assert_eq!(parsed.get("APP_VERSION").map(String::as_str), Some("2.1.0"));

    // ── CLEANUP ──
    orion.shutdown();
    cleanup_config_file(&config_path);
    let _ = std::fs::remove_file(&log_path);
}

#[tokio::test]
#[ignore]
async fn test_access_log_custom_operator_not_configured_is_graceful() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-custom-noop-{}.txt", std::process::id()));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));

    let hcm =
        HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), CUSTOM_OPERATOR_FORMAT).route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default")
                    .route(RouteBuilder::new().name("hello-route").match_prefix("/").cluster("backend")),
            ),
        );

    let listener = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm));

    // Headers are configured (so custom operators are registered in CUSTOM_OPERATORS),
    // but no request will carry them — so the operators should render as "-"
    let access_log_config = AccessLogConfig {
        blocking: true,
        incoming_request_header: Some(HeaderName::from_static("x-orion-metadata")),
        custom_operators: vec![
            smol_str::SmolStr::new("user_id"),
            smol_str::SmolStr::new("session_id"),
            smol_str::SmolStr::new("app_version"),
        ],
        ..AccessLogConfig::default()
    };

    let bootstrap = BootstrapBuilder::new().listener(listener).cluster(cluster).access_log(access_log_config);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default().with_verbose())
        .await
        .expect("Failed to spawn Orion");

    let listener_addr = orion.listener_addr().expect("Missing listener address");

    // Send a request WITHOUT the custom headers — operators should remain unpopulated
    let req = "GET /hello HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";

    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req.as_bytes()).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"));
    };

    tokio::time::sleep(Duration::from_millis(500)).await;

    let log_content = read_log_file(&log_path, Duration::from_secs(5)).await;
    let log_line = log_content.lines().next().expect("No log line found");
    let parsed = parse_log_line(log_line);

    // Standard operators should still work
    assert_eq!(parsed.get("REQ(:METHOD)").map(String::as_str), Some("GET"));
    assert_eq!(parsed.get("RESPONSE_CODE").map(String::as_str), Some("200"));

    // Custom operators should be "-" (headers configured but not present in request)
    assert_eq!(parsed.get("USER_ID").map(String::as_str), Some("-"));
    assert_eq!(parsed.get("SESSION_ID").map(String::as_str), Some("-"));
    assert_eq!(parsed.get("APP_VERSION").map(String::as_str), Some("-"));

    // ── CLEANUP ──
    orion.shutdown();
    cleanup_config_file(&config_path);
    let _ = std::fs::remove_file(&log_path);
}

#[tokio::test]
#[ignore]
async fn test_access_log_custom_operator_multiple_fields() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-custom-multi-{}.txt", std::process::id()));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));

    let hcm =
        HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), CUSTOM_OPERATOR_FORMAT).route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default")
                    .route(RouteBuilder::new().name("hello-route").match_prefix("/").cluster("backend")),
            ),
        );

    let listener = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm));

    let access_log_config = AccessLogConfig {
        blocking: true,
        incoming_request_header: Some(HeaderName::from_static("x-orion-metadata")),
        custom_operators: vec![
            smol_str::SmolStr::new("user_id"),
            smol_str::SmolStr::new("session_id"),
            smol_str::SmolStr::new("app_version"),
        ],
        ..AccessLogConfig::default()
    };

    let bootstrap = BootstrapBuilder::new().listener(listener).cluster(cluster).access_log(access_log_config);

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default().with_verbose())
        .await
        .expect("Failed to spawn Orion");

    let listener_addr = orion.listener_addr().expect("Missing listener address");

    // Send a JSON value with only some of the fields populated
    let json = serde_json::json!({
        "user_id": "alpha",
        "app_version": "beta",
    });
    let json_str = serde_json::to_string(&json).expect("Failed to serialize JSON");
    let base64_val = base64::engine::general_purpose::STANDARD.encode(json_str.as_bytes());

    let req = format!(
        "GET /multi HTTP/1.1\r\nHost: localhost\r\nx-orion-metadata: {base64_val}\r\nConnection: close\r\n\r\n"
    );

    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req.as_bytes()).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"));
    };

    tokio::time::sleep(Duration::from_millis(500)).await;

    let log_content = read_log_file(&log_path, Duration::from_secs(5)).await;
    let log_line = log_content.lines().next().expect("No log line found");
    let parsed = parse_log_line(log_line);

    assert_eq!(parsed.get("REQ(:PATH)").map(String::as_str), Some("/multi"));
    assert_eq!(parsed.get("RESPONSE_CODE").map(String::as_str), Some("200"));

    // user_id is populated
    assert_eq!(parsed.get("USER_ID").map(String::as_str), Some("alpha"));

    // session_id is NOT populated in the JSON payload, so it should be "-"
    assert_eq!(parsed.get("SESSION_ID").map(String::as_str), Some("-"));

    // app_version is populated
    assert_eq!(parsed.get("APP_VERSION").map(String::as_str), Some("beta"));

    // ── CLEANUP ──
    orion.shutdown();
    cleanup_config_file(&config_path);
    let _ = std::fs::remove_file(&log_path);
}
