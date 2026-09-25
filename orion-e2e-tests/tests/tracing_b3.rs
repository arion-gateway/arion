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

//! E2E tests for B3 propagation (single and multi-header encodings).
//!
//! Based on the [B3 Propagation specification](https://github.com/openzipkin/b3-propagation).
//!
//! All tests run against localhost (`is_internal=true`).

use http::StatusCode;
use orion_e2e_tests::config_builder::{presets, FilterChainBuilder, HcmBuilder, ListenerBuilder};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
};

const TRACE_ID_128: &str = "4bf92f3577b34da6a3ce929d0e0e4736";
const SPAN_ID: &str = "00f067aa0ba902b7";
const PARENT_SPAN_ID: &str = "05e3ac9a4f6e3b90";

/// Spins up Orion with tracing enabled at 100% sampling.
async fn setup_tracing() -> (OrionInstance, TestClient, TestBackend, std::path::PathBuf) {
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

// ─── B3 single header: basic (3 fields: trace-span-sampled) ───
// The most common B3 encoding. Server should spawn a child span and propagate.
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_b3_single_basic() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    let b3_in = format!("{TRACE_ID_128}-{SPAN_ID}-1");
    let resp = client.send(RequestBuilder::get("/test").header("b3", &b3_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    // Tracing is active: X-Request-ID should be generated
    assert!(resp.header("x-request-id").is_some());

    let cap = backend.await_request().await.unwrap();

    // B3 child span should be propagated
    let b3_out = cap.header("b3").expect("b3 should be propagated");
    println!("DEBUG: b3_out = {b3_out}");
    let parts: Vec<&str> = b3_out.split('-').collect();
    assert_eq!(parts.len(), 4, "B3 child should have 4 fields (trace-span-sampled-parent)");
    assert_eq!(parts[0], TRACE_ID_128, "trace ID should be preserved");
    assert_ne!(parts[1], SPAN_ID, "span ID should be newly generated (child)");
    assert_eq!(parts[2], "1", "sampled should be 1");
    // parent should be the incoming span ID
    assert_eq!(parts[3], SPAN_ID, "parent span ID should be the incoming span ID");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 single header: with ParentSpanId (4 fields) ───
// Incoming context already has a parent. Orion should generate a new
// span ID and set the incoming span ID as the new parent.
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_b3_single_with_parent() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    let b3_in = format!("{TRACE_ID_128}-{SPAN_ID}-1-{PARENT_SPAN_ID}");
    let resp = client.send(RequestBuilder::get("/test").header("b3", &b3_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    let b3_out = cap.header("b3").expect("b3 should be propagated");
    let parts: Vec<&str> = b3_out.split('-').collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], TRACE_ID_128);
    // New child span ID
    assert_ne!(parts[1], SPAN_ID);
    assert_ne!(parts[1], PARENT_SPAN_ID);
    // The previous span becomes the parent
    assert_eq!(parts[3], SPAN_ID);

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 single header: Deny sampling (b3: 0) ───
// Should propagate the deny decision. No tracing/spans should be created.
#[tokio::test]
#[ignore]
async fn test_b3_single_deny() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    let resp = client.send(RequestBuilder::get("/test").header("b3", "0")).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();

    // The deny should be propagated downstream
    assert_eq!(cap.header("b3"), Some("0"));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 single header: Deny with identifiers ───
// Per spec: "if trace identifiers are established, they should be propagated, too"
// even with a deny (sampled=0) decision.
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_b3_single_deny_with_ids() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    let b3_in = format!("{TRACE_ID_128}-{SPAN_ID}-0");
    let resp = client.send(RequestBuilder::get("/test").header("b3", &b3_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    let b3_out = cap.header("b3").expect("b3 should be propagated even with deny");

    let parts: Vec<&str> = b3_out.split('-').collect();
    // Should propagate the deny decision
    assert_eq!(parts[2], "0", "sampled should remain 0 (deny)");
    assert_eq!(parts[0], TRACE_ID_128);

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 single header: Debug flag (b3: ...-d) ───
// Debug is an emphasized accept (implies sampled=true, span.debug=true).
#[tokio::test]
#[ignore]
async fn test_b3_single_debug() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    let b3_in = format!("{TRACE_ID_128}-{SPAN_ID}-d");
    let resp = client.send(RequestBuilder::get("/test").header("b3", &b3_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    // Debug => tracing should be active
    assert!(resp.header("x-request-id").is_some());

    let cap = backend.await_request().await.unwrap();
    let b3_out = cap.header("b3").expect("b3 should be propagated");
    // Debug should be preserved (or converted to sampled=1)
    assert!(
        b3_out.contains("-d") || b3_out.contains("-1-"),
        "debug should be preserved or converted to sampled=1, got: {b3_out}"
    );

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 single header: Defer (no sampling field) ───
// Per spec: absence of sampling state means "defer" — the receiver decides.
// Orion should accept and spawn a child span.
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_b3_single_defer() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    // 2 fields only: trace-span, no sampling state
    let b3_in = format!("{TRACE_ID_128}-{SPAN_ID}");
    let resp = client.send(RequestBuilder::get("/test").header("b3", &b3_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    // Tracing should be active (defer→accept)
    assert!(resp.header("x-request-id").is_some());

    let cap = backend.await_request().await.unwrap();
    let b3_out = cap.header("b3").expect("b3 should be propagated");
    let parts: Vec<&str> = b3_out.split('-').collect();
    assert!(parts.len() >= 3, "should have at least 3 fields after server decision");
    assert_eq!(parts[0], TRACE_ID_128);
    assert_ne!(parts[1], SPAN_ID, "span ID should be newly generated (child)");
    assert_eq!(parts[2], "1", "server should accept by default");
    // parent should be the incoming span ID
    assert_eq!(parts[3], SPAN_ID, "parent span ID should be the incoming span ID");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 multi-header: basic ───
// X-B3-TraceId + X-B3-SpanId + X-B3-Sampled
#[tokio::test]
#[ignore]
async fn test_b3_multi_basic() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("x-b3-traceid", TRACE_ID_128)
                .header("x-b3-spanid", SPAN_ID)
                .header("x-b3-sampled", "1"),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    assert!(resp.header("x-request-id").is_some());

    let cap = backend.await_request().await.unwrap();

    // Multi-header variant should propagate
    assert_eq!(cap.header("x-b3-traceid"), Some(TRACE_ID_128));
    assert!(cap.header("x-b3-spanid").is_some());
    // Sampled should still be 1
    assert_eq!(cap.header("x-b3-sampled"), Some("1"));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 multi-header: with ParentSpanId ───
#[tokio::test]
#[ignore]
async fn test_b3_multi_with_parent() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("x-b3-traceid", TRACE_ID_128)
                .header("x-b3-spanid", SPAN_ID)
                .header("x-b3-parentspanid", PARENT_SPAN_ID)
                .header("x-b3-sampled", "1"),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-b3-traceid"), Some(TRACE_ID_128));
    // Incoming span ID becomes the parent in the child
    assert_eq!(cap.header("x-b3-parentspanid"), Some(SPAN_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 multi-header: Deny sampling ───
#[tokio::test]
#[ignore]
async fn test_b3_multi_deny() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("x-b3-traceid", TRACE_ID_128)
                .header("x-b3-spanid", SPAN_ID)
                .header("x-b3-sampled", "0"),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();

    // Even with deny, trace IDs should be propagated
    assert_eq!(cap.header("x-b3-traceid"), Some(TRACE_ID_128));
    assert_eq!(cap.header("x-b3-sampled"), Some("0"));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 multi-header: only sampling state (no IDs) ───
// Valid per spec: send only X-B3-Sampled to pre-define a decision.
#[tokio::test]
#[ignore]
async fn test_b3_multi_sampling_only_deny() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    let resp = client.send(RequestBuilder::get("/test").header("x-b3-sampled", "0")).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();

    // The deny decision should be propagated
    assert_eq!(cap.header("x-b3-sampled"), Some("0"));

    // No trace IDs should be generated (no context to propagate)
    assert!(cap.header("x-b3-traceid").is_none());

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 single-header takes precedence over multi-header ───
// Per spec: "The single-header variant takes precedence over the multiple
// header one when extracting fields."
#[tokio::test]
#[ignore]
async fn test_b3_single_precedence_over_multi() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    // Send BOTH single and multi-header with different trace IDs.
    // Single-header should win.
    let b3_single = format!("{TRACE_ID_128}-{SPAN_ID}-1");
    let multi_trace_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab"; // different

    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("b3", &b3_single)
                .header("x-b3-traceid", multi_trace_id)
                .header("x-b3-spanid", "0000000000000001")
                .header("x-b3-sampled", "1"),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();

    // The single-header trace ID should win
    let b3_out = cap.header("b3").expect("single-header b3 should be propagated");
    assert!(b3_out.starts_with(TRACE_ID_128), "single-header trace ID should take precedence");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 multi-header: defer (no x-b3-sampled) ───
// Per spec: absence of sampling state means "defer" — the receiver decides.
#[tokio::test]
#[ignore]
async fn test_b3_multi_defer() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    // trace/span IDs present, but no x-b3-sampled → defer
    let resp = client
        .send(RequestBuilder::get("/test").header("x-b3-traceid", TRACE_ID_128).header("x-b3-spanid", SPAN_ID))
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    // Tracing should be active (defer → accept)
    assert!(resp.header("x-request-id").is_some());

    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-b3-traceid"), Some(TRACE_ID_128));
    // Should have made a sampling decision (sampled=1)
    assert_eq!(cap.header("x-b3-sampled"), Some("1"));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 multi-header: Debug flag (X-B3-Flags: 1) ───
// Per spec: debug implies an accept decision, is encoded as X-B3-Flags: 1.
#[tokio::test]
#[ignore]
async fn test_b3_multi_debug() {
    let (orion, client, mut backend, config_path) = setup_tracing().await;

    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("x-b3-traceid", TRACE_ID_128)
                .header("x-b3-spanid", SPAN_ID)
                .header("x-b3-flags", "1"),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    // Debug → tracing should be active
    assert!(resp.header("x-request-id").is_some());

    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-b3-traceid"), Some(TRACE_ID_128));
    // Debug flag should be propagated
    assert_eq!(cap.header("x-b3-flags"), Some("1"));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── B3 with HTTP/2 codec ───
// Verify tracing works when the HCM is configured for HTTP/2 downstream.
#[tokio::test]
#[ignore]
async fn test_b3_http2() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;
    let backend_addr = backend.addr();
    let cluster = presets::static_cluster("backend", backend_addr);

    let hcm = HcmBuilder::new()
        .http2()
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
    let mut backend = backend;

    let b3_in = format!("{TRACE_ID_128}-{SPAN_ID}-1");
    let resp = client.send(RequestBuilder::get("/test").header("b3", &b3_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    assert!(resp.header("x-request-id").is_some());

    let cap = backend.await_request().await.unwrap();
    assert!(cap.header("b3").is_some(), "b3 should be propagated with HTTP/2 codec");

    orion.shutdown();
    cleanup_config_file(&config_path);
}
