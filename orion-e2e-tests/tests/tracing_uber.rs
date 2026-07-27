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

//! E2E tests for Jaeger uber-trace-id propagation.
//!
//! All tests run against localhost (`is_internal=true`).

use http::StatusCode;
use orion_e2e_tests::config_builder::{presets, FilterChainBuilder, HcmBuilder, ListenerBuilder};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
};

const TRACE_ID: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const SPAN_ID: &str = "00f067aa0ba902b7";

async fn setup() -> (OrionInstance, TestClient, TestBackend, std::path::PathBuf) {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;
    let backend_addr = backend.addr();
    let cluster = presets::static_cluster("backend", backend_addr);

    let hcm = HcmBuilder::new()
        .http1()
        .generate_request_id(true)
        .always_set_request_id_in_response(true)
        .tracing(Some(100), Some(100), Some(100))
        .route_config(
            orion_e2e_tests::config_builder::RouteConfigBuilder::new("routes").virtual_host(
                orion_e2e_tests::config_builder::VirtualHostBuilder::new("default")
                    .route(presets::default_route("backend")),
            ),
        );
    let listener = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm));
    let bootstrap = orion_e2e_tests::config_builder::BootstrapBuilder::new().listener(listener).cluster(cluster);
    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());
    (orion, client, backend, config_path)
}

// ─── Basic uber-trace-id (sampled=1, no parent) ───
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_uber_basic() {
    let (orion, client, mut backend, config_path) = setup().await;

    let uber_in = format!("{TRACE_ID}:{SPAN_ID}:0:1");
    let resp = client.send(RequestBuilder::get("/test").header("uber-trace-id", &uber_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_some());

    let cap = backend.await_request().await.unwrap();
    let uber_out = cap.header("uber-trace-id").expect("uber-trace-id should be propagated");
    let parts: Vec<&str> = uber_out.split(':').collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], TRACE_ID, "trace_id should be preserved");
    assert_ne!(parts[1], SPAN_ID, "span_id should be a new child");
    assert_eq!(parts[2], SPAN_ID, "parent span_id should be the incoming span_id");
    assert_eq!(parts[3], "1", "sampled flag should be 1");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Not sampled (sampled=0) ───
#[tokio::test]
#[ignore]
async fn test_uber_not_sampled() {
    let (orion, client, mut backend, config_path) = setup().await;

    let uber_in = format!("{TRACE_ID}:{SPAN_ID}:0:0");
    let resp = client.send(RequestBuilder::get("/test").header("uber-trace-id", &uber_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    // uber-trace-id should still be propagated (with its identifiers)
    assert_eq!(cap.header("uber-trace-id"), Some(uber_in.as_str()));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Invalid uber-trace-id ───
#[tokio::test]
#[ignore]
async fn test_uber_invalid() {
    let (orion, client, mut backend, config_path) = setup().await;

    let resp = client.send(RequestBuilder::get("/test").header("uber-trace-id", "garbage")).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    // Malformed uber-trace-id is not parseable — passed through as-is
    assert_eq!(cap.header("uber-trace-id"), Some("garbage"));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── uber-trace-id with parent span ───
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_uber_with_parent() {
    let (orion, client, mut backend, config_path) = setup().await;

    // trace_id:span_id:parent_span_id:sampled
    let uber_in = format!("{TRACE_ID}:{SPAN_ID}:05e3ac9a4f6e3b90:1");
    let resp = client.send(RequestBuilder::get("/test").header("uber-trace-id", &uber_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    let uber_out = cap.header("uber-trace-id").expect("uber-trace-id should be propagated");
    let parts: Vec<&str> = uber_out.split(':').collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], TRACE_ID);
    // The incoming span_id becomes the parent for the new child
    assert_eq!(parts[2], SPAN_ID, "incoming span_id should become the parent");

    orion.shutdown();
    cleanup_config_file(&config_path);
}
