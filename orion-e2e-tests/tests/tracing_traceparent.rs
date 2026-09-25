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

//! E2E tests for W3C traceparent propagation.
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

// ─── Basic traceparent (sampled=01) ───
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_traceparent_basic() {
    let (orion, client, mut backend, config_path) = setup().await;

    let tp_in = format!("00-{TRACE_ID}-{SPAN_ID}-01");
    let resp = client.send(RequestBuilder::get("/test").header("traceparent", &tp_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_some());

    let cap = backend.await_request().await.unwrap();
    let tp_out = cap.header("traceparent").expect("traceparent should be propagated");
    let parts: Vec<&str> = tp_out.split('-').collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], "00");
    assert_eq!(parts[1], TRACE_ID, "trace_id should be preserved");
    assert_ne!(parts[2], SPAN_ID, "span_id should be a new child");
    assert_eq!(parts[3], "01", "sampled flag should be 01");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Not-sampled traceparent (flags=00) ───
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_traceparent_not_sampled() {
    let (orion, client, mut backend, config_path) = setup().await;

    let tp_in = format!("00-{TRACE_ID}-{SPAN_ID}-00");
    let resp = client.send(RequestBuilder::get("/test").header("traceparent", &tp_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    let tp_out = cap.header("traceparent").expect("traceparent should be propagated even when not sampled");
    let parts: Vec<&str> = tp_out.split('-').collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], "00");
    assert_eq!(parts[1], TRACE_ID, "trace_id should be preserved");
    assert_ne!(parts[2], SPAN_ID, "span_id should be a new child");
    assert_eq!(parts[3], "00", "sampled flag should remain 00");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Invalid traceparent (malformed) ───
// Garbage traceparent is not parseable, so no trace context is extracted.
// Orion may pass it through or generate a new traceparent (root span).
#[tokio::test]
#[ignore]
async fn test_traceparent_invalid() {
    let (orion, client, mut backend, config_path) = setup().await;

    let resp = client.send(RequestBuilder::get("/test").header("traceparent", "garbage")).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    // Either passed through or a new traceparent is generated
    let tp = cap.header("traceparent").expect("traceparent should be present");
    assert!(tp.contains('-'), "traceparent should contain hyphens");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── traceparent takes precedence over b3 ───
#[tokio::test]
#[ignore]
async fn test_traceparent_precedence_over_b3() {
    let (orion, client, mut backend, config_path) = setup().await;

    let tp_in = format!("00-{TRACE_ID}-{SPAN_ID}-01");
    let other_trace_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab";

    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("traceparent", &tp_in)
                .header("b3", format!("{other_trace_id}-0000000000000001-1")),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    let tp_out = cap.header("traceparent").expect("traceparent should take precedence");
    assert!(tp_out.starts_with(&format!("00-{TRACE_ID}")), "traceparent trace_id should win over b3");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── traceparent takes precedence over uber-trace-id ───
#[tokio::test]
#[ignore]
async fn test_traceparent_precedence_over_uber() {
    let (orion, client, mut backend, config_path) = setup().await;

    let tp_in = format!("00-{TRACE_ID}-{SPAN_ID}-01");

    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("traceparent", &tp_in)
                .header("uber-trace-id", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:0000000000000001:0:1"),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    let tp_out = cap.header("traceparent").expect("traceparent should take precedence");
    assert!(tp_out.starts_with(&format!("00-{TRACE_ID}")), "traceparent trace_id should win over uber");

    orion.shutdown();
    cleanup_config_file(&config_path);
}
