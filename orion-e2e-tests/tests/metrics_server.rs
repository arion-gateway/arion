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

use http::StatusCode;
use std::net::SocketAddr;
use std::time::Duration;

use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::{cleanup_config_file, parse_metric_value, OrionInstance, PortBlock, SpawnOptions, TestClient};

#[tokio::test]
#[ignore]
async fn test_server_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    // We don't need a real backend for server metrics, but we need a valid address to build the config
    let dummy_backend_addr: SocketAddr = "127.0.0.1:8080".parse().unwrap();

    let bootstrap = presets::simple_proxy("backend", dummy_backend_addr).admin("127.0.0.1", admin_port);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let admin_client = TestClient::new(admin_addr);

    // 1. Initial check: read the server metrics
    let initial_metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    initial_metrics_resp.assert_status(StatusCode::OK);
    let initial_metrics = initial_metrics_resp.body_str().unwrap();

    let initial_uptime = parse_metric_value(initial_metrics, "server_uptime").expect("Missing server_uptime metric");
    let concurrency =
        parse_metric_value(initial_metrics, "server_concurrency").expect("Missing server_concurrency metric");
    let memory_heap_size =
        parse_metric_value(initial_metrics, "server_memory_heap_size").expect("Missing server_memory_heap_size metric");
    let memory_physical_size = parse_metric_value(initial_metrics, "server_memory_physical_size")
        .expect("Missing server_memory_physical_size metric");
    let memory_allocated =
        parse_metric_value(initial_metrics, "server_memory_allocated").expect("Missing server_memory_allocated metric");

    assert!(concurrency > 0, "Concurrency should be greater than 0");
    assert!(memory_heap_size > 0, "Memory heap size should be greater than 0");
    assert!(memory_physical_size > 0, "Memory physical size should be greater than 0");
    assert!(memory_allocated > 0, "Memory allocated should be greater than 0");

    // 2. Wait for 2 seconds and read again to verify uptime increases
    tokio::time::sleep(Duration::from_secs(2)).await;

    let second_metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    second_metrics_resp.assert_status(StatusCode::OK);
    let second_metrics = second_metrics_resp.body_str().unwrap();

    let second_uptime = parse_metric_value(second_metrics, "server_uptime").expect("Missing server_uptime metric");
    assert!(
        second_uptime > initial_uptime,
        "Uptime should increase over time (initial: {initial_uptime}, second: {second_uptime})"
    );

    orion.shutdown();
    cleanup_config_file(&config_path);
}
