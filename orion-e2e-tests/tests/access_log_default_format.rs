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

#![allow(clippy::expect_used, clippy::indexing_slicing)]

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use orion_configuration::config::log::AccessLogConfig;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{cleanup_config_file, OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend};

/// The default access log format defined in `orion-format::DEFAULT_ACCESS_LOG_FORMAT`.
const DEFAULT_FORMAT: &str = r#"[%START_TIME%] "%REQ(:METHOD)% %REQ(X-ENVOY-ORIGINAL-PATH?:PATH)% %PROTOCOL%" %RESPONSE_CODE% %RESPONSE_FLAGS% %BYTES_RECEIVED% %BYTES_SENT% %DURATION% %RESP(X-ENVOY-UPSTREAM-SERVICE-TIME)% "%REQ(X-FORWARDED-FOR)%" "%REQ(USER-AGENT)%" "%REQ(X-REQUEST-ID)%" "%REQ(:AUTHORITY)%" "%UPSTREAM_HOST%"
"#;

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

#[tokio::test]
#[ignore]
async fn test_access_log_default_format_basic() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    backend
        .set_default_response(PreConfiguredResponse::with_body("Hello!").header("x-envoy-upstream-service-time", "42"))
        .await;

    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-default-{}.txt", std::process::id()));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));

    let hcm = HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), DEFAULT_FORMAT).route_config(
        RouteConfigBuilder::new("routes").virtual_host(
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().name("hello-route").match_prefix("/").cluster("backend")),
        ),
    );

    let listener = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm));

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(cluster)
        .access_log(AccessLogConfig { blocking: true, ..AccessLogConfig::default() });

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let listener_addr = orion.listener_addr().expect("Missing listener address");

    // Send a simple GET request
    let req = "GET /hello HTTP/1.1\r\nHost: localhost\r\nUser-Agent: test-agent/1.0\r\nX-Request-Id: my-req-id-001\r\nConnection: close\r\n\r\n".to_owned();

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

    // The default format produces output like:
    // [2024-...] "GET /hello HTTP/1.1" 200 - 0 19 1234 42 "-" "test-agent/1.0" "my-req-id-001" "localhost" "127.0.0.1:PORT"
    //
    // We validate the overall structure using a regex-like approach on the space-delimited parts.

    // Extract timestamp: [%START_TIME%]
    let (timestamp, rest) = log_line.split_once("] ").expect("Expected timestamp prefix like [YYYY-...] ");
    assert!(timestamp.starts_with('['), "Timestamp should start with '[': {timestamp}");
    assert!(timestamp.len() > 5, "Timestamp too short: {timestamp}");

    // The default format produces:
    // "GET /hello HTTP/1.1" 200 - 0 19 DURATION 42 "-" "test-agent/1.0" "my-req-id-001" "localhost" "UPSTREAM_HOST"
    //
    // Pattern: "METHOD PATH PROTOCOL" STATUS FLAGS BYTES_RECV BYTES_SENT DURATION UPSTREAM_TIME "FORWARDED_FOR" "USER_AGENT" "REQUEST_ID" "AUTHORITY" "UPSTREAM_HOST"

    let rest = rest.trim();

    // Extract quoted METHOD PATH PROTOCOL
    let (method_path_proto, after_quote1) = extract_quoted(rest);
    let mpp_parts: Vec<&str> = method_path_proto.splitn(3, ' ').collect();
    assert_eq!(mpp_parts.len(), 3, "Expected METHOD PATH PROTOCOL inside quotes, got: {method_path_proto}");
    assert_eq!(mpp_parts[0], "GET");
    assert_eq!(mpp_parts[1], "/hello");
    assert_eq!(mpp_parts[2], "HTTP/1.1");

    // Remaining: STATUS FLAGS BYTES_RECV BYTES_SENT DURATION UPSTREAM_TIME "FORWARDED_FOR" "USER_AGENT" "REQUEST_ID" "AUTHORITY" UPSTREAM_HOST
    // That's 11 fields total
    let fields: Vec<&str> = split_mixed(after_quote1.trim_start());
    assert_eq!(fields.len(), 11, "Expected 11 fields, got {}: {fields:?}", fields.len());

    // STATUS
    assert_eq!(fields[0], "200");

    // FLAGS
    assert!(!fields[1].is_empty());

    // BYTES_RECEIVED
    assert_eq!(fields[2].parse::<u64>().ok(), Some(0));

    // BYTES_SENT
    let bytes_sent = fields[3].parse::<u64>().expect("BYTES_SENT should be a u64");
    assert!(bytes_sent > 0, "BYTES_SENT should be > 0");

    // DURATION
    fields[4].parse::<u64>().expect("DURATION should be a u64");

    // UPSTREAM_SERVICE_TIME
    assert_eq!(fields[5], "42");

    // FORWARDED_FOR
    assert_eq!(fields[6], "-");

    // USER_AGENT
    assert_eq!(fields[7], "test-agent/1.0");

    // REQUEST_ID
    assert_eq!(fields[8], "my-req-id-001");

    // AUTHORITY
    assert_eq!(fields[9], "localhost");

    // UPSTREAM_HOST
    assert!(!fields[10].is_empty() && fields[10] != "-");

    // ── CLEANUP ──
    orion.shutdown();
    cleanup_config_file(&config_path);
    _ = std::fs::remove_file(&log_path);
}

#[tokio::test]
#[ignore]
async fn test_access_log_default_format_with_original_path() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-default-op-{}.txt", std::process::id()));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));

    let hcm = HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), DEFAULT_FORMAT).route_config(
        RouteConfigBuilder::new("routes").virtual_host(
            VirtualHostBuilder::new("default")
                .route(RouteBuilder::new().name("hello-route").match_prefix("/").cluster("backend")),
        ),
    );

    let listener = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm));

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(cluster)
        .access_log(AccessLogConfig { blocking: true, ..AccessLogConfig::default() });

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let listener_addr = orion.listener_addr().expect("Missing listener address");

    // Request with x-envoy-original-path header — the operator should return the original path
    let req = "GET /rewritten HTTP/1.1\r\nHost: example.com\r\nX-Envoy-Original-Path: /original-path\r\nConnection: close\r\n\r\n".to_owned();

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

    // Extract quoted METHOD PATH PROTOCOL
    let rest = log_line.split_once("] ").expect("Expected timestamp prefix").1.trim();
    let (method_path_proto, _after) = extract_quoted(rest);
    let mpp_parts: Vec<&str> = method_path_proto.splitn(3, ' ').collect();

    // The PATH in the log should be the original path from the header, not the rewritten URI
    assert_eq!(mpp_parts[1], "/original-path", "Expected original-path from X-Envoy-Original-Path header");

    // ── CLEANUP ──
    orion.shutdown();
    cleanup_config_file(&config_path);
    _ = std::fs::remove_file(&log_path);
}

/// Extracts a double-quoted section from the start of a string.
/// Returns `(quoted_content, rest_of_string)`.
#[allow(clippy::string_slice)]
fn extract_quoted(s: &str) -> (&str, &str) {
    let s = s.trim_start();
    assert!(s.starts_with('"'), "Expected quote at start: {s}");
    // Find closing quote position (relative to original string)
    let end = s[1..].find('"').expect("Missing closing quote");
    // end is the position within s[1..], so the closing quote is at 1+end in s
    // Content is s[1..1+end] (excludes both quotes)
    let content = &s[1..=end];
    let rest = &s[1 + end + 1..]; // Skip closing quote
    (content, rest)
}

/// Splits a string by spaces, but treats double-quoted sections as single tokens.
#[allow(clippy::string_slice)]
fn split_mixed(s: &str) -> Vec<&str> {
    let s = s.trim();
    let mut result = Vec::new();
    let mut i = 0;
    let bytes = s.as_bytes();

    while i < bytes.len() {
        // Skip leading spaces
        while i < bytes.len() && bytes[i] == b' ' {
            i += 1;
        }
        if i >= bytes.len() {
            break;
        }

        if bytes[i] == b'"' {
            // Quoted token: find closing quote
            let start = i + 1;
            let end = s[start..].find('"').map(|p| start + p).expect("Unclosed quote in split_mixed");
            result.push(&s[start..end]);
            i = end + 1;
        } else {
            // Unquoted token: find next space
            let start = i;
            while i < bytes.len() && bytes[i] != b' ' {
                i += 1;
            }
            result.push(&s[start..i]);
        }
    }

    result
}
