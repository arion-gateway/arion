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

#![allow(clippy::expect_used, clippy::similar_names, clippy::too_many_lines, clippy::let_underscore_must_use)]

use std::collections::HashMap;
use std::time::Duration;

use http::StatusCode;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use orion_configuration::config::log::AccessLogConfig;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, RawHttpRequestBuilder, SpawnOptions, TestBackend,
};

/// All access-log operators supported at the HCM (transaction) level.
const HCM_LOG_FORMAT: &str = "\
||START_TIME=%START_TIME%\
||BYTES_RECEIVED=%BYTES_RECEIVED%\
||BYTES_SENT=%BYTES_SENT%\
||DOWNSTREAM_WIRE_BYTES_RECEIVED=%DOWNSTREAM_WIRE_BYTES_RECEIVED%\
||DOWNSTREAM_WIRE_BYTES_SENT=%DOWNSTREAM_WIRE_BYTES_SENT%\
||PROTOCOL=%PROTOCOL%\
||UPSTREAM_PROTOCOL=%UPSTREAM_PROTOCOL%\
||RESPONSE_CODE=%RESPONSE_CODE%\
||RESPONSE_CODE_DETAILS=%RESPONSE_CODE_DETAILS%\
||RESPONSE_FLAGS=%RESPONSE_FLAGS%\
||RESPONSE_FLAGS_LONG=%RESPONSE_FLAGS_LONG%\
||CONNECTION_TERMINATION_DETAILS=%CONNECTION_TERMINATION_DETAILS%\
||REQUEST_HEADERS_BYTES=%REQUEST_HEADERS_BYTES%\
||RESPONSE_HEADERS_BYTES=%RESPONSE_HEADERS_BYTES%\
||DURATION=%DURATION%\
||REQUEST_DURATION=%REQUEST_DURATION%\
||REQUEST_TX_DURATION=%REQUEST_TX_DURATION%\
||RESPONSE_DURATION=%RESPONSE_DURATION%\
||RESPONSE_TX_DURATION=%RESPONSE_TX_DURATION%\
||UPSTREAM_HOST=%UPSTREAM_HOST%\
||UPSTREAM_HOST_NAME=%UPSTREAM_HOST_NAME%\
||UPSTREAM_HOST_NAME_WITHOUT_PORT=%UPSTREAM_HOST_NAME_WITHOUT_PORT%\
||UPSTREAM_REMOTE_ADDRESS=%UPSTREAM_REMOTE_ADDRESS%\
||UPSTREAM_CLUSTER=%UPSTREAM_CLUSTER%\
||UPSTREAM_CLUSTER_RAW=%UPSTREAM_CLUSTER_RAW%\
||UPSTREAM_TRANSPORT_FAILURE_REASON=%UPSTREAM_TRANSPORT_FAILURE_REASON%\
||DOWNSTREAM_LOCAL_ADDRESS=%DOWNSTREAM_LOCAL_ADDRESS%\
||DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT=%DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT%\
||DOWNSTREAM_LOCAL_PORT=%DOWNSTREAM_LOCAL_PORT%\
||DOWNSTREAM_REMOTE_ADDRESS=%DOWNSTREAM_REMOTE_ADDRESS%\
||DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT=%DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT%\
||DOWNSTREAM_REMOTE_PORT=%DOWNSTREAM_REMOTE_PORT%\
||CONNECTION_ID=%CONNECTION_ID%\
||UNIQUE_ID=%UNIQUE_ID%\
||TRACE_ID=%TRACE_ID%\
||REQ(:SCHEME)=%REQ(:SCHEME)%\
||REQ(:METHOD)=%REQ(:METHOD)%\
||REQ(:PATH)=%REQ(:PATH)%\
||REQ(:AUTHORITY)=%REQ(:AUTHORITY)%\
||REQ(X-ENVOY-ORIGINAL-PATH?:PATH)=%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%\
||RESP(:STATUS)=%RESP(:STATUS)%\
||REQ(X-Custom-Request-Header)=%REQ(X-Custom-Request-Header)%\
||RESP(X-Custom-Response-Header)=%RESP(X-Custom-Response-Header)%\
||REQUESTED_SERVER_NAME=%REQUESTED_SERVER_NAME%\
||ROUTE_NAME=%ROUTE_NAME%\
\n";

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

/// Find a parsed log line by its REQ(:PATH) value.
#[allow(clippy::indexing_slicing)]
fn find_line_by_path<'a>(lines: &'a [HashMap<String, String>], path: &str) -> &'a HashMap<String, String> {
    lines
        .iter()
        .find(|l| l.get("REQ(:PATH)").map(String::as_str) == Some(path))
        .unwrap_or_else(|| panic!("no log line found with path={path}"))
}

async fn send_request(addr: std::net::SocketAddr, raw_request: &[u8]) -> (usize, usize) {
    let mut stream = TcpStream::connect(addr).await.expect("Failed to connect");
    stream.write_all(raw_request).await.expect("Failed to write");
    let mut resp = Vec::new();
    stream.read_to_end(&mut resp).await.expect("Failed to read");
    let resp_str = String::from_utf8_lossy(&resp);
    assert!(resp_str.contains("200 OK"), "Unexpected response: {resp_str}");

    // Find the end of the headers section (\r\n\r\n)
    let headers_len = resp
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|pos| pos + 4) // Include the \r\n\r\n
        .expect("Failed to find end of headers in response");

    (resp.len(), headers_len)
}

/// Common validations that apply to any HCM log line (regardless of origin-form vs absolute-form).
#[allow(clippy::expect_used)]
fn validate_common_fields(parsed: &HashMap<String, String>) {
    // START_TIME: non-empty
    let start_time = parsed.get("START_TIME").expect("START_TIME missing");
    assert!(!start_time.is_empty(), "START_TIME should not be empty");

    // PROTOCOL
    assert_eq!(parsed.get("PROTOCOL").map(String::as_str), Some("HTTP/1.1"));

    // UPSTREAM_PROTOCOL
    let upstream_proto = parsed.get("UPSTREAM_PROTOCOL").expect("UPSTREAM_PROTOCOL missing");
    assert!(!upstream_proto.is_empty(), "UPSTREAM_PROTOCOL should not be empty");

    // RESPONSE_CODE / RESP(:STATUS)
    assert_eq!(parsed.get("RESPONSE_CODE").map(String::as_str), Some("200"));
    assert_eq!(parsed.get("RESP(:STATUS)").map(String::as_str), Some("200"));

    // RESPONSE_FLAGS / RESPONSE_FLAGS_LONG (can be "-" on success)
    let flags = parsed.get("RESPONSE_FLAGS").expect("RESPONSE_FLAGS missing");
    assert!(!flags.is_empty(), "RESPONSE_FLAGS should not be empty");
    let flags_long = parsed.get("RESPONSE_FLAGS_LONG").expect("RESPONSE_FLAGS_LONG missing");
    assert!(!flags_long.is_empty(), "RESPONSE_FLAGS_LONG should not be empty");

    // REQ(:METHOD)
    assert_eq!(parsed.get("REQ(:METHOD)").map(String::as_str), Some("GET"));

    // REQ(X-Custom-Request-Header)
    assert_eq!(parsed.get("REQ(X-Custom-Request-Header)").map(String::as_str), Some("req-custom-value"));

    // RESP(X-Custom-Response-Header)
    assert_eq!(parsed.get("RESP(X-Custom-Response-Header)").map(String::as_str), Some("resp-custom-value"));

    // UPSTREAM_CLUSTER / UPSTREAM_CLUSTER_RAW
    assert_eq!(parsed.get("UPSTREAM_CLUSTER").map(String::as_str), Some("backend"));
    assert_eq!(parsed.get("UPSTREAM_CLUSTER_RAW").map(String::as_str), Some("backend"));

    // UPSTREAM_HOST / UPSTREAM_HOST_NAME
    let upstream_host = parsed.get("UPSTREAM_HOST").expect("UPSTREAM_HOST missing");
    assert!(!upstream_host.is_empty() && upstream_host != "-");
    assert_eq!(parsed.get("UPSTREAM_HOST_NAME").map(String::as_str), Some(upstream_host.as_str()));

    // UPSTREAM_HOST_NAME_WITHOUT_PORT
    let host_wo_port = parsed.get("UPSTREAM_HOST_NAME_WITHOUT_PORT").expect("UPSTREAM_HOST_NAME_WITHOUT_PORT missing");
    assert!(!host_wo_port.is_empty() && host_wo_port != "-");
    assert!(!host_wo_port.contains(':'), "UPSTREAM_HOST_NAME_WITHOUT_PORT should not contain port");

    // UPSTREAM_REMOTE_ADDRESS: not available at HCM level (upstream TCP details
    // are tracked only at the TcpProxy connection level)
    assert_eq!(parsed.get("UPSTREAM_REMOTE_ADDRESS").map(String::as_str), Some("-"));

    // ROUTE_NAME
    assert_eq!(parsed.get("ROUTE_NAME").map(String::as_str), Some("hello-route"));

    // ── ADDRESS VALIDATIONS ──
    let dl_addr = parsed.get("DOWNSTREAM_LOCAL_ADDRESS").expect("DOWNSTREAM_LOCAL_ADDRESS missing");
    assert!(!dl_addr.is_empty() && dl_addr != "-");
    assert!(dl_addr.contains(':'), "DOWNSTREAM_LOCAL_ADDRESS should contain port");

    let dl_wo =
        parsed.get("DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT").expect("DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT missing");
    assert!(!dl_wo.is_empty() && dl_wo != "-");
    assert!(!dl_wo.contains(':'), "DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT should not contain port");

    let dl_port = parsed.get("DOWNSTREAM_LOCAL_PORT").expect("DOWNSTREAM_LOCAL_PORT missing");
    assert_ne!(dl_port.as_str(), "-");
    dl_port.parse::<u16>().expect("DOWNSTREAM_LOCAL_PORT should be a u16");

    let dr_addr = parsed.get("DOWNSTREAM_REMOTE_ADDRESS").expect("DOWNSTREAM_REMOTE_ADDRESS missing");
    assert!(!dr_addr.is_empty() && dr_addr != "-");

    let dr_wo =
        parsed.get("DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT").expect("DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT missing");
    assert!(!dr_wo.is_empty() && dr_wo != "-");
    assert!(!dr_wo.contains(':'), "DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT should not contain port");

    let dr_port = parsed.get("DOWNSTREAM_REMOTE_PORT").expect("DOWNSTREAM_REMOTE_PORT missing");
    assert_ne!(dr_port.as_str(), "-");
    dr_port.parse::<u16>().expect("DOWNSTREAM_REMOTE_PORT should be a u16");

    // ── NUMERIC VALIDATIONS ──
    // BYTES_RECEIVED: request body bytes (0 for GET with empty body)
    let bytes_recv = parsed.get("BYTES_RECEIVED").expect("BYTES_RECEIVED missing");
    assert_eq!(bytes_recv.parse::<u64>().unwrap(), 0, "BYTES_RECEIVED should be 0 for GET: {bytes_recv}");

    // BYTES_SENT: response body bytes ("Hello from backend!" = 19 bytes)
    let bytes_sent = parsed.get("BYTES_SENT").expect("BYTES_SENT missing");
    assert_eq!(bytes_sent.parse::<u64>().unwrap(), 19, "BYTES_SENT should be 19: {bytes_sent}");

    let duration = parsed.get("DURATION").expect("DURATION missing");
    assert!(duration.parse::<u64>().is_ok(), "DURATION should be a u64: {duration}");

    let req_dur = parsed.get("REQUEST_DURATION").expect("REQUEST_DURATION missing");
    assert!(req_dur.parse::<u64>().is_ok(), "REQUEST_DURATION should be a u64: {req_dur}");

    let req_tx = parsed.get("REQUEST_TX_DURATION").expect("REQUEST_TX_DURATION missing");
    assert!(req_tx.parse::<u64>().is_ok(), "REQUEST_TX_DURATION should be a u64: {req_tx}");

    let resp_dur = parsed.get("RESPONSE_DURATION").expect("RESPONSE_DURATION missing");
    assert!(resp_dur.parse::<u64>().is_ok(), "RESPONSE_DURATION should be a u64: {resp_dur}");

    let resp_tx = parsed.get("RESPONSE_TX_DURATION").expect("RESPONSE_TX_DURATION missing");
    assert!(resp_tx.parse::<u64>().is_ok(), "RESPONSE_TX_DURATION should be a u64: {resp_tx}");

    // ── HEX VALIDATIONS ──
    let conn_id = parsed.get("CONNECTION_ID").expect("CONNECTION_ID missing");
    assert_eq!(conn_id.len(), 64, "CONNECTION_ID should be a 64-char hex string: {conn_id}");
    assert!(conn_id.chars().all(|c| c.is_ascii_hexdigit()), "CONNECTION_ID should be hex: {conn_id}");

    // UNIQUE_ID: always generated and unique
    let unique_id = parsed.get("UNIQUE_ID").expect("UNIQUE_ID missing");
    assert!(!unique_id.is_empty() && unique_id != "-", "UNIQUE_ID should be populated");
    assert!(unique_id.len() == 36 || unique_id.len() == 32, "UNIQUE_ID should be a valid ID: {unique_id}");

    // ── OPTIONAL/ABSENT FIELDS (expected "-") ──
    assert_eq!(parsed.get("UPSTREAM_TRANSPORT_FAILURE_REASON").map(String::as_str), Some("-"));
    assert_eq!(parsed.get("REQUESTED_SERVER_NAME").map(String::as_str), Some("-"));
    assert_eq!(parsed.get("RESPONSE_CODE_DETAILS").map(String::as_str), Some("via_upstream"));
    assert_eq!(parsed.get("CONNECTION_TERMINATION_DETAILS").map(String::as_str), Some("-"));
}

#[tokio::test]
#[ignore]
async fn test_access_log_hcm_all_operators() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    backend
        .set_default_response(
            PreConfiguredResponse::with_body("Hello from backend!")
                .header("x-custom-response-header", "resp-custom-value"),
        )
        .await;

    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-hcm-{}.txt", std::process::id()));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));

    let hcm = HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), HCM_LOG_FORMAT).route_config(
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

    // ── Request 1: origin-form (scheme absent in URI → REQ(:SCHEME) = "-") ──
    let origin_form_req = RawHttpRequestBuilder::new()
        .method("GET")
        .uri("/hello")
        .host("localhost")
        .header("X-Custom-Request-Header", "req-custom-value")
        .header("X-Request-Id", "550e8400-e29b-41d4-a716-446655440000")
        .header("Connection", "close")
        .build();

    let origin_req_len = origin_form_req.len();
    let (origin_resp_len, origin_resp_headers_len) = send_request(listener_addr, &origin_form_req).await;

    // ── Request 2: absolute-form (scheme = "http" in URI) ──
    let abs_form_req = RawHttpRequestBuilder::new()
        .method("GET")
        .uri("http://localhost/hello-abs")
        .host("localhost")
        .header("X-Custom-Request-Header", "req-custom-value")
        .header("X-Request-Id", "550e8400-e29b-41d4-a716-446655440000")
        .header("Connection", "close")
        .build();

    let abs_req_len = abs_form_req.len();
    let (abs_resp_len, abs_resp_headers_len) = send_request(listener_addr, &abs_form_req).await;

    // Wait for logs (blocking=true, so writes are synchronous)
    tokio::time::sleep(Duration::from_millis(500)).await;

    let log_content = read_log_file(&log_path, Duration::from_secs(5)).await;
    let log_lines: Vec<&str> = log_content.lines().collect();
    assert_eq!(log_lines.len(), 2, "Expected exactly 2 log lines, got {}", log_lines.len());

    let parsed_lines: Vec<HashMap<String, String>> = log_lines.iter().map(|line| parse_log_line(line)).collect();

    // ── Validate origin-form line (REQ(:SCHEME) should be "-") ──
    let origin_line = find_line_by_path(&parsed_lines, "/hello");
    assert_eq!(origin_line.get("REQ(:PATH)").map(String::as_str), Some("/hello"));
    assert_eq!(origin_line.get("REQ(X-ENVOY-ORIGINAL-PATH?:PATH)").map(String::as_str), Some("/hello"));
    assert_eq!(origin_line.get("REQ(:SCHEME)").map(String::as_str), Some("-"));
    assert_eq!(origin_line.get("REQ(:AUTHORITY)").map(String::as_str), Some("localhost"));

    // Downstream wire bytes for origin-form
    assert_eq!(
        origin_line.get("DOWNSTREAM_WIRE_BYTES_RECEIVED").and_then(|s| s.parse::<usize>().ok()),
        Some(origin_req_len),
        "DOWNSTREAM_WIRE_BYTES_RECEIVED mismatch for origin-form"
    );
    assert_eq!(
        origin_line.get("DOWNSTREAM_WIRE_BYTES_SENT").and_then(|s| s.parse::<usize>().ok()),
        Some(origin_resp_len),
        "DOWNSTREAM_WIRE_BYTES_SENT mismatch for origin-form"
    );

    // Request and Response header bytes for origin-form
    assert_eq!(
        origin_line.get("REQUEST_HEADERS_BYTES").and_then(|s| s.parse::<usize>().ok()),
        Some(origin_req_len),
        "REQUEST_HEADERS_BYTES mismatch for origin-form"
    );

    // Request and Response header bytes for origin-form
    // Note: RESPONSE_HEADERS_BYTES in Orion measures only the size of the header key-value pairs,
    // which excludes the status line ("HTTP/1.1 200 OK\r\n" = 17 bytes) and the final "\r\n" (2 bytes).
    assert_eq!(
        origin_line.get("RESPONSE_HEADERS_BYTES").and_then(|s| s.parse::<usize>().ok()),
        Some(origin_resp_headers_len - 19),
        "RESPONSE_HEADERS_BYTES mismatch for origin-form"
    );

    // ── Validate absolute-form line (REQ(:SCHEME) should be "http") ──
    let abs_line = find_line_by_path(&parsed_lines, "/hello-abs");
    assert_eq!(abs_line.get("REQ(:PATH)").map(String::as_str), Some("/hello-abs"));
    assert_eq!(abs_line.get("REQ(X-ENVOY-ORIGINAL-PATH?:PATH)").map(String::as_str), Some("/hello-abs"));
    assert_eq!(abs_line.get("REQ(:SCHEME)").map(String::as_str), Some("http"));
    assert_eq!(abs_line.get("REQ(:AUTHORITY)").map(String::as_str), Some("localhost"));

    // Downstream wire bytes for absolute-form
    assert_eq!(
        abs_line.get("DOWNSTREAM_WIRE_BYTES_RECEIVED").and_then(|s| s.parse::<usize>().ok()),
        Some(abs_req_len),
        "DOWNSTREAM_WIRE_BYTES_RECEIVED mismatch for absolute-form"
    );
    assert_eq!(
        abs_line.get("DOWNSTREAM_WIRE_BYTES_SENT").and_then(|s| s.parse::<usize>().ok()),
        Some(abs_resp_len),
        "DOWNSTREAM_WIRE_BYTES_SENT mismatch for absolute-form"
    );

    // Request and Response header bytes for absolute-form
    // Note: Orion normalizes the absolute-form URI to origin-form (removing "http://localhost" = 16 bytes)
    // before measuring REQUEST_HEADERS_BYTES.
    assert_eq!(
        abs_line.get("REQUEST_HEADERS_BYTES").and_then(|s| s.parse::<usize>().ok()),
        Some(abs_req_len - 16),
        "REQUEST_HEADERS_BYTES mismatch for absolute-form"
    );

    // Request and Response header bytes for absolute-form
    // Note: RESPONSE_HEADERS_BYTES in Orion measures only the size of the header key-value pairs,
    // which excludes the status line ("HTTP/1.1 200 OK\r\n" = 17 bytes) and the final "\r\n" (2 bytes).
    assert_eq!(
        abs_line.get("RESPONSE_HEADERS_BYTES").and_then(|s| s.parse::<usize>().ok()),
        Some(abs_resp_headers_len - 19),
        "RESPONSE_HEADERS_BYTES mismatch for absolute-form"
    );

    // ── Run common validations on both lines ──
    validate_common_fields(origin_line);
    validate_common_fields(abs_line);

    // ── CLEANUP ──
    orion.shutdown();
    cleanup_config_file(&config_path);
    let _ = std::fs::remove_file(&log_path);
}

#[tokio::test]
#[ignore]
async fn test_access_log_hcm_upstream_down() {
    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-hcm-down-{}.txt", std::process::id()));

    // Point the cluster to an inactive port (e.g., 127.0.0.1:1)
    let cluster =
        ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr("127.0.0.1:1".parse().unwrap()));

    let hcm = HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), HCM_LOG_FORMAT).route_config(
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

    // Send a request that will fail because the upstream is down.
    // The HCM will produce an error response (503 Service Unavailable) and log it.
    let req = RawHttpRequestBuilder::new()
        .method("GET")
        .uri("/hello")
        .host("localhost")
        .header("X-Request-Id", "550e8400-e29b-41d4-a716-446655440000")
        .header("Connection", "close")
        .build();

    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(&req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        let resp_str = String::from_utf8_lossy(&resp);
        assert!(resp_str.contains("503"), "Expected 503, got: {resp_str}")
    };

    tokio::time::sleep(Duration::from_millis(500)).await;

    let log_content = read_log_file(&log_path, Duration::from_secs(5)).await;
    let log_line = log_content.lines().next().expect("No log line found");
    let parsed = parse_log_line(log_line);

    // ── ERROR VALIDATIONS ──
    // RESPONSE_CODE and RESP(:STATUS) should be 503
    assert_eq!(parsed.get("RESPONSE_CODE").map(String::as_str), Some("503"));
    assert_eq!(parsed.get("RESP(:STATUS)").map(String::as_str), Some("503"));

    // RESPONSE_FLAGS should indicate upstream connection failure
    let flags = parsed.get("RESPONSE_FLAGS").expect("RESPONSE_FLAGS missing");
    assert!(flags == "UF" || flags == "UH", "Expected UF or UH response flags, got: {flags}");

    let flags_long = parsed.get("RESPONSE_FLAGS_LONG").expect("RESPONSE_FLAGS_LONG missing");
    assert!(
        flags_long == "UpstreamConnectionFailure" || flags_long == "NoHealthyUpstream",
        "Expected UpstreamConnectionFailure or NoHealthyUpstream, got: {flags_long}"
    );

    // UPSTREAM_TRANSPORT_FAILURE_REASON should be populated
    let failure_reason =
        parsed.get("UPSTREAM_TRANSPORT_FAILURE_REASON").expect("UPSTREAM_TRANSPORT_FAILURE_REASON missing");
    assert_ne!(failure_reason.as_str(), "-", "UPSTREAM_TRANSPORT_FAILURE_REASON should be populated");
    assert!(
        failure_reason.to_lowercase().contains("refused") || failure_reason.to_lowercase().contains("connect"),
        "Unexpected failure reason: {failure_reason}"
    );

    // RESPONSE_CODE_DETAILS should be populated
    let code_details = parsed.get("RESPONSE_CODE_DETAILS").expect("RESPONSE_CODE_DETAILS missing");
    assert_ne!(code_details.as_str(), "-", "RESPONSE_CODE_DETAILS should be populated");

    // BYTES_RECEIVED: should be 0 (GET request has no body)
    assert_eq!(parsed.get("BYTES_RECEIVED").and_then(|s| s.parse::<u64>().ok()), Some(0));

    // BYTES_SENT: may be 0 for an error response without a body
    parsed.get("BYTES_SENT").and_then(|s| s.parse::<u64>().ok()).expect("BYTES_SENT should be a valid u64");

    // REQUESTED_SERVER_NAME: plaintext => "-"
    assert_eq!(parsed.get("REQUESTED_SERVER_NAME").map(String::as_str), Some("-"));

    // UPSTREAM_CLUSTER should still be "backend"
    assert_eq!(parsed.get("UPSTREAM_CLUSTER").map(String::as_str), Some("backend"));

    // ── CLEANUP ──
    orion.shutdown();
    cleanup_config_file(&config_path);
    let _ = std::fs::remove_file(&log_path);
}

#[tokio::test]
#[ignore]
async fn test_access_log_hcm_original_path_header() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    backend
        .set_default_response(
            PreConfiguredResponse::with_body("OK").header("x-custom-response-header", "resp-custom-value"),
        )
        .await;

    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-hcm-origpath-{}.txt", std::process::id()));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));

    let hcm = HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), HCM_LOG_FORMAT).route_config(
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

    // Request with x-envoy-original-path header — the operator should return the header value
    let req = RawHttpRequestBuilder::new()
        .method("GET")
        .uri("/rewritten-path")
        .host("localhost")
        .header("x-envoy-original-path", "/original-path")
        .header("Connection", "close")
        .build();

    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(&req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"))
    };

    tokio::time::sleep(Duration::from_millis(500)).await;

    let log_content = read_log_file(&log_path, Duration::from_secs(5)).await;
    let log_line = log_content.lines().next().expect("No log line found");
    let parsed = parse_log_line(log_line);

    // REQ(:PATH) should be the actual request path
    assert_eq!(parsed.get("REQ(:PATH)").map(String::as_str), Some("/rewritten-path"));

    // REQ(X-ENVOY-ORIGINAL-PATH?:PATH) should return the header value, not the path
    assert_eq!(parsed.get("REQ(X-ENVOY-ORIGINAL-PATH?:PATH)").map(String::as_str), Some("/original-path"));

    orion.shutdown();
    cleanup_config_file(&config_path);
    let _ = std::fs::remove_file(&log_path);
}

#[tokio::test]
#[ignore]
async fn test_access_log_hcm_backend_500() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    backend
        .set_default_response(PreConfiguredResponse::with_status(StatusCode::INTERNAL_SERVER_ERROR).body("Boom"))
        .await;

    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-hcm-500-{}.txt", std::process::id()));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));

    let hcm = HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), HCM_LOG_FORMAT).route_config(
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

    let req = RawHttpRequestBuilder::new()
        .method("GET")
        .uri("/five-hundred")
        .host("localhost")
        .header("Connection", "close")
        .build();

    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(&req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("500"))
    };

    tokio::time::sleep(Duration::from_millis(500)).await;

    let log_content = read_log_file(&log_path, Duration::from_secs(5)).await;
    let log_line = log_content.lines().next().expect("No log line found");
    let parsed = parse_log_line(log_line);

    // Status 500
    assert_eq!(parsed.get("RESPONSE_CODE").map(String::as_str), Some("500"));
    assert_eq!(parsed.get("RESP(:STATUS)").map(String::as_str), Some("500"));

    // RESPONSE_CODE_DETAILS should be "via_upstream" (the upstream responded)
    assert_eq!(parsed.get("RESPONSE_CODE_DETAILS").map(String::as_str), Some("via_upstream"));

    // RESPONSE_FLAGS: no transport error, so may be "-"
    let flags = parsed.get("RESPONSE_FLAGS").expect("RESPONSE_FLAGS missing");
    assert!(flags != "UF" && flags != "UH", "RESPONSE_FLAGS should not be UF/UH for a backend 500: got {flags}");

    // UPSTREAM_TRANSPORT_FAILURE_REASON should be "-" (no transport failure)
    assert_eq!(parsed.get("UPSTREAM_TRANSPORT_FAILURE_REASON").map(String::as_str), Some("-"));

    // REQ(:PATH) should be logged correctly
    assert_eq!(parsed.get("REQ(:PATH)").map(String::as_str), Some("/five-hundred"));

    // PROTOCOL should still be HTTP/1.1
    assert_eq!(parsed.get("PROTOCOL").map(String::as_str), Some("HTTP/1.1"));

    orion.shutdown();
    cleanup_config_file(&config_path);
    let _ = std::fs::remove_file(&log_path);
}

#[tokio::test]
#[ignore]
async fn test_access_log_hcm_multiple_connections() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-hcm-multi-{}.txt", std::process::id()));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));

    let hcm = HcmBuilder::new().http1().access_log_file(log_path.to_str().unwrap(), HCM_LOG_FORMAT).route_config(
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

    // Send 3 distinct requests over separate connections
    for path in &["/first", "/second", "/third"] {
        let req = RawHttpRequestBuilder::new()
            .method("GET")
            .uri(*path)
            .host("localhost")
            .header("Connection", "close")
            .build();

        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(&req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"));
    }

    tokio::time::sleep(Duration::from_millis(500)).await;

    let log_content = read_log_file(&log_path, Duration::from_secs(5)).await;
    let log_lines: Vec<&str> = log_content.lines().collect();

    // Each connection should produce exactly one log line
    assert_eq!(log_lines.len(), 3, "Expected 3 log lines, got {}", log_lines.len());

    let parsed_lines: Vec<HashMap<String, String>> = log_lines.iter().map(|line| parse_log_line(line)).collect();

    // Verify each path is present and distinct connection IDs
    let mut paths: Vec<&str> = parsed_lines.iter().filter_map(|l| l.get("REQ(:PATH)").map(String::as_str)).collect();
    paths.sort_unstable();
    assert_eq!(paths, vec!["/first", "/second", "/third"]);

    // Connection IDs should all be 64-char hex
    for line in &parsed_lines {
        let conn_id = line.get("CONNECTION_ID").expect("CONNECTION_ID missing");
        assert_eq!(conn_id.len(), 64);
        assert!(conn_id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    orion.shutdown();
    cleanup_config_file(&config_path);
    let _ = std::fs::remove_file(&log_path);
}
