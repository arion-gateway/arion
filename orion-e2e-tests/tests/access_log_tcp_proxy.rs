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

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use orion_configuration::config::log::AccessLogConfig;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, ListenerBuilder, TcpProxyBuilder,
};
use orion_e2e_tests::{cleanup_config_file, OrionInstance, SpawnOptions, TcpTestBackend};

/// All access-log operators supported at the `TCPProxy` (connection) level.
const TCP_LOG_FORMAT: &str = "\
||START_TIME=%START_TIME%\
||BYTES_RECEIVED=%BYTES_RECEIVED%\
||BYTES_SENT=%BYTES_SENT%\
||DOWNSTREAM_WIRE_BYTES_RECEIVED=%DOWNSTREAM_WIRE_BYTES_RECEIVED%\
||DOWNSTREAM_WIRE_BYTES_SENT=%DOWNSTREAM_WIRE_BYTES_SENT%\
||RESPONSE_CODE_DETAILS=%RESPONSE_CODE_DETAILS%\
||DURATION=%DURATION%\
||RESPONSE_FLAGS=%RESPONSE_FLAGS%\
||RESPONSE_FLAGS_LONG=%RESPONSE_FLAGS_LONG%\
||CONNECTION_TERMINATION_DETAILS=%CONNECTION_TERMINATION_DETAILS%\
||UPSTREAM_HOST=%UPSTREAM_HOST%\
||UPSTREAM_LOCAL_ADDRESS=%UPSTREAM_LOCAL_ADDRESS%\
||UPSTREAM_LOCAL_ADDRESS_WITHOUT_PORT=%UPSTREAM_LOCAL_ADDRESS_WITHOUT_PORT%\
||UPSTREAM_LOCAL_PORT=%UPSTREAM_LOCAL_PORT%\
||UPSTREAM_REMOTE_ADDRESS=%UPSTREAM_REMOTE_ADDRESS%\
||UPSTREAM_REMOTE_ADDRESS_WITHOUT_PORT=%UPSTREAM_REMOTE_ADDRESS_WITHOUT_PORT%\
||UPSTREAM_REMOTE_PORT=%UPSTREAM_REMOTE_PORT%\
||DOWNSTREAM_LOCAL_ADDRESS=%DOWNSTREAM_LOCAL_ADDRESS%\
||DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT=%DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT%\
||DOWNSTREAM_LOCAL_PORT=%DOWNSTREAM_LOCAL_PORT%\
||DOWNSTREAM_REMOTE_ADDRESS=%DOWNSTREAM_REMOTE_ADDRESS%\
||DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT=%DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT%\
||DOWNSTREAM_REMOTE_PORT=%DOWNSTREAM_REMOTE_PORT%\
||UPSTREAM_CLUSTER=%UPSTREAM_CLUSTER%\
||UPSTREAM_CLUSTER_RAW=%UPSTREAM_CLUSTER_RAW%\
||UPSTREAM_TRANSPORT_FAILURE_REASON=%UPSTREAM_TRANSPORT_FAILURE_REASON%\
||CONNECTION_ID=%CONNECTION_ID%\
||UPSTREAM_CONNECTION_ID=%UPSTREAM_CONNECTION_ID%\
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

#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_access_log_tcp_proxy_all_operators() {
    let mut backend = TcpTestBackend::start().await.expect("Failed to start TCP backend");
    let backend_addr = backend.addr();

    // Configure backend to send a message on connect and wait for client message
    backend.set_send_on_connect("Hello from backend!").await;
    backend.set_read_timeout(Duration::from_millis(500)).await;

    // Create a temporary file for the access log
    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-tcp-{}.txt", std::process::id()));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend_addr));

    let tcp_proxy = TcpProxyBuilder::new("tcp_proxy")
        .cluster("backend")
        .access_log_file(log_path.to_str().unwrap(), TCP_LOG_FORMAT);

    let listener =
        ListenerBuilder::new("tcp").port(0).filter_chain(FilterChainBuilder::new("main").tcp_proxy(tcp_proxy));

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(cluster)
        .access_log(AccessLogConfig { blocking: true, ..AccessLogConfig::default() });

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let listener_addr = orion.listener_addr().expect("Missing listener address");

    // Connect to Orion, read backend's greeting, send client message, and close connection
    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");

        let mut buf = [0u8; 128];
        let n = stream.read(&mut buf).await.expect("Failed to read greeting");
        assert_eq!(&buf[..n], b"Hello from backend!");

        stream.write_all(b"Hello from client!").await.expect("Failed to write client message");
        stream.flush().await.expect("Failed to flush")
    };

    // Wait for backend to capture the connection and close
    let captured = backend.await_connection().await.expect("Failed to capture connection");
    assert_eq!(captured.received_data, b"Hello from client!");

    // Wait briefly for the access log to be written
    tokio::time::sleep(Duration::from_millis(500)).await;

    let log_content = read_log_file(&log_path, Duration::from_secs(5)).await;
    let log_line = log_content.lines().next().expect("No log line found");
    let parsed = parse_log_line(log_line);

    // ── STRING VALIDATIONS ──
    let start_time = parsed.get("START_TIME").expect("START_TIME missing");
    assert!(!start_time.is_empty(), "START_TIME should not be empty");

    // UPSTREAM_CLUSTER / RAW
    assert_eq!(parsed.get("UPSTREAM_CLUSTER").map(String::as_str), Some("backend"));
    assert_eq!(parsed.get("UPSTREAM_CLUSTER_RAW").map(String::as_str), Some("backend"));

    // UPSTREAM_HOST
    let upstream_host = parsed.get("UPSTREAM_HOST").expect("UPSTREAM_HOST missing");
    assert_eq!(upstream_host.as_str(), backend_addr.to_string().as_str());

    // UPSTREAM_REMOTE_ADDRESS / PORT / WITHOUT_PORT
    assert_eq!(parsed.get("UPSTREAM_REMOTE_ADDRESS").map(String::as_str), Some(backend_addr.to_string().as_str()));
    assert_eq!(
        parsed.get("UPSTREAM_REMOTE_ADDRESS_WITHOUT_PORT").map(String::as_str),
        Some(backend_addr.ip().to_string().as_str())
    );
    assert_eq!(parsed.get("UPSTREAM_REMOTE_PORT").map(String::as_str), Some(backend_addr.port().to_string().as_str()));

    // UPSTREAM_LOCAL_ADDRESS / PORT / WITHOUT_PORT
    let upstream_local = parsed.get("UPSTREAM_LOCAL_ADDRESS").expect("UPSTREAM_LOCAL_ADDRESS missing");
    assert!(!upstream_local.is_empty() && upstream_local != "-");
    assert!(upstream_local.contains(':'));

    let upstream_local_wo =
        parsed.get("UPSTREAM_LOCAL_ADDRESS_WITHOUT_PORT").expect("UPSTREAM_LOCAL_ADDRESS_WITHOUT_PORT missing");
    assert!(!upstream_local_wo.is_empty() && upstream_local_wo != "-");
    assert!(!upstream_local_wo.contains(':'));

    let upstream_local_port = parsed.get("UPSTREAM_LOCAL_PORT").expect("UPSTREAM_LOCAL_PORT missing");
    upstream_local_port.parse::<u16>().expect("UPSTREAM_LOCAL_PORT should be u16");

    // DOWNSTREAM_LOCAL_ADDRESS / PORT / WITHOUT_PORT
    let dl_addr = parsed.get("DOWNSTREAM_LOCAL_ADDRESS").expect("DOWNSTREAM_LOCAL_ADDRESS missing");
    assert_eq!(dl_addr.as_str(), listener_addr.to_string().as_str());

    let dl_wo =
        parsed.get("DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT").expect("DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT missing");
    assert_eq!(dl_wo.as_str(), listener_addr.ip().to_string().as_str());

    assert_eq!(
        parsed.get("DOWNSTREAM_LOCAL_PORT").map(String::as_str),
        Some(listener_addr.port().to_string().as_str())
    );

    // DOWNSTREAM_REMOTE_ADDRESS / PORT / WITHOUT_PORT
    let dr_addr = parsed.get("DOWNSTREAM_REMOTE_ADDRESS").expect("DOWNSTREAM_REMOTE_ADDRESS missing");
    assert!(!dr_addr.is_empty() && dr_addr != "-");

    let dr_wo =
        parsed.get("DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT").expect("DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT missing");
    assert!(!dr_wo.contains(':'));

    let dr_port = parsed.get("DOWNSTREAM_REMOTE_PORT").expect("DOWNSTREAM_REMOTE_PORT missing");
    dr_port.parse::<u16>().expect("DOWNSTREAM_REMOTE_PORT should be u16");

    // ── NUMERIC VALIDATIONS ──
    // BYTES_RECEIVED: bytes read from downstream ("Hello from client!" = 18 bytes)
    let bytes_recv = parsed.get("BYTES_RECEIVED").expect("BYTES_RECEIVED missing");
    assert_eq!(bytes_recv.parse::<u64>().unwrap(), 18, "BYTES_RECEIVED should be 18: {bytes_recv}");

    // BYTES_SENT: bytes written to downstream ("Hello from backend!" = 19 bytes)
    let bytes_sent = parsed.get("BYTES_SENT").expect("BYTES_SENT missing");
    assert_eq!(bytes_sent.parse::<u64>().unwrap(), 19, "BYTES_SENT should be 19: {bytes_sent}");

    // DOWNSTREAM_WIRE_BYTES_RECEIVED / SENT (identical to BYTES_RECEIVED/SENT at TCP level)
    let wire_recv = parsed.get("DOWNSTREAM_WIRE_BYTES_RECEIVED").expect("DOWNSTREAM_WIRE_BYTES_RECEIVED missing");
    assert_eq!(wire_recv.parse::<u64>().unwrap(), 18);

    let wire_sent = parsed.get("DOWNSTREAM_WIRE_BYTES_SENT").expect("DOWNSTREAM_WIRE_BYTES_SENT missing");
    assert_eq!(wire_sent.parse::<u64>().unwrap(), 19);

    // DURATION
    let duration = parsed.get("DURATION").expect("DURATION missing");
    assert!(duration.parse::<u64>().is_ok(), "DURATION should be a u64: {duration}");

    // ── HEX VALIDATIONS ──
    let conn_id = parsed.get("CONNECTION_ID").expect("CONNECTION_ID missing");
    assert_eq!(conn_id.len(), 64);
    assert!(conn_id.chars().all(|c| c.is_ascii_hexdigit()));

    let upstream_conn_id = parsed.get("UPSTREAM_CONNECTION_ID").expect("UPSTREAM_CONNECTION_ID missing");
    assert_eq!(upstream_conn_id.len(), 64);
    assert!(upstream_conn_id.chars().all(|c| c.is_ascii_hexdigit()));

    // ── OPTIONAL/ABSENT FIELDS (expected "-") ──
    assert_eq!(parsed.get("UPSTREAM_TRANSPORT_FAILURE_REASON").map(String::as_str), Some("-"));
    assert_eq!(parsed.get("RESPONSE_FLAGS").map(String::as_str), Some("-"));
    assert_eq!(parsed.get("RESPONSE_FLAGS_LONG").map(String::as_str), Some("-"));
    assert_eq!(parsed.get("RESPONSE_CODE_DETAILS").map(String::as_str), Some("-"));
    assert_eq!(parsed.get("CONNECTION_TERMINATION_DETAILS").map(String::as_str), Some("-"));

    // ── CLEANUP ──
    orion.shutdown();
    cleanup_config_file(&config_path);
    let _ = std::fs::remove_file(&log_path);
}

#[tokio::test]
#[ignore]
async fn test_access_log_tcp_proxy_upstream_down() {
    // Create a temporary file for the access log
    let log_dir = std::env::temp_dir();
    let log_path = log_dir.join(format!("orion-test-access-log-tcp-down-{}.txt", std::process::id()));

    // Point the cluster to an inactive port (e.g., 127.0.0.1:1)
    let cluster =
        ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr("127.0.0.1:1".parse().unwrap()));

    let tcp_proxy = TcpProxyBuilder::new("tcp_proxy")
        .cluster("backend")
        .access_log_file(log_path.to_str().unwrap(), TCP_LOG_FORMAT);

    let listener =
        ListenerBuilder::new("tcp").port(0).filter_chain(FilterChainBuilder::new("main").tcp_proxy(tcp_proxy));

    let bootstrap = BootstrapBuilder::new()
        .listener(listener)
        .cluster(cluster)
        .access_log(AccessLogConfig { blocking: true, ..AccessLogConfig::default() });

    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let mut options = SpawnOptions::default().with_verbose();
    options.log_level = Some("debug".to_owned());

    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", options).await.expect("Failed to spawn Orion");

    let listener_addr = orion.listener_addr().expect("Missing listener address");

    // Connect to Orion and send some data. Orion will try to connect to the upstream,
    // fail, and close the connection.
    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        let _ = stream.write_all(b"Hello?").await;
        let mut buf = [0u8; 128];
        // Expect connection to be closed by Orion
        let n = stream.read(&mut buf).await.unwrap_or(0);
        assert_eq!(n, 0, "Connection should be closed by Orion")
    };

    // Wait briefly for the access log to be written and Orion to flush debug logs
    tokio::time::sleep(Duration::from_secs(2)).await;

    let log_content = read_log_file(&log_path, Duration::from_secs(5)).await;
    let log_line = log_content.lines().next().expect("No log line found");
    println!("DEBUG LOG LINE: {log_line}");
    let parsed = parse_log_line(log_line);

    // ── ERROR VALIDATIONS ──
    // Expect RESPONSE_FLAGS to be "UF" (Upstream Connection Failure) or "UH" (No Healthy Upstream)
    let flags = parsed.get("RESPONSE_FLAGS").expect("RESPONSE_FLAGS missing");
    assert!(flags == "UF" || flags == "UH", "Expected UF or UH response flags, got: {flags}");

    let flags_long = parsed.get("RESPONSE_FLAGS_LONG").expect("RESPONSE_FLAGS_LONG missing");
    assert!(
        flags_long == "UpstreamConnectionFailure" || flags_long == "NoHealthyUpstream",
        "Expected UpstreamConnectionFailure or NoHealthyUpstream, got: {flags_long}"
    );

    // Expect UPSTREAM_TRANSPORT_FAILURE_REASON to contain connection failure details
    let failure_reason =
        parsed.get("UPSTREAM_TRANSPORT_FAILURE_REASON").expect("UPSTREAM_TRANSPORT_FAILURE_REASON missing");
    println!("DEBUG FAILURE REASON: {failure_reason}");
    assert_ne!(failure_reason.as_str(), "-", "UPSTREAM_TRANSPORT_FAILURE_REASON should be populated");

    // ── CLEANUP ──
    orion.shutdown();
    cleanup_config_file(&config_path);
    let _ = std::fs::remove_file(&log_path);
}
