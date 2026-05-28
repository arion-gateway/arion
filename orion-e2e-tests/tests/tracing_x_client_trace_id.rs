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

//! E2E tests for X-Client-Trace-Id header propagation and its interaction with
//! existing trace contexts (traceparent, b3, uber-trace-id).
//!
//! All tests run against localhost (`is_internal=true`).

use http::StatusCode;
use orion_e2e_tests::config_builder::{presets, FilterChainBuilder, HcmBuilder, ListenerBuilder};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
};

const CLIENT_TRACE_ID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
const VALID_TRACEPARENT: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
const INVALID_CLIENT_TRACE_ID: &str = "not-a-uuid";

/// Spins up Orion with tracing enabled at the given sampling rates.
/// sampling `None` = 100% (Envoy default when unset).
async fn setup_with_sampling(
    client_sampling: Option<u32>,
    random_sampling: Option<u32>,
    overall_sampling: Option<u32>,
) -> (OrionInstance, TestClient, TestBackend, std::path::PathBuf) {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;
    let backend_addr = backend.addr();

    let cluster = presets::static_cluster("backend", backend_addr);

    let hcm = HcmBuilder::new()
        .http1()
        .generate_request_id(true)
        .preserve_external_request_id(true)
        .always_set_request_id_in_response(true)
        .tracing(client_sampling, random_sampling, overall_sampling)
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

// ─── X-Client-Trace-Id with 100% sampling triggers tracing ───
// No existing trace context → Orion creates a new root trace.
// Expected: X-Client-Trace-Id propagated, X-Request-ID generated,
// traceparent created (W3C root span).
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_client_trace_id_sampling_100_triggers_tracing() {
    let (orion, client, mut backend, config_path) = setup_with_sampling(Some(100), Some(100), Some(100)).await;

    let resp = client.send(RequestBuilder::get("/test").header("x-client-trace-id", CLIENT_TRACE_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    // Tracing should be active: X-Request-ID should be generated and returned
    let req_id = resp.header("x-request-id").expect("x-request-id should be generated when tracing is active");
    uuid::Uuid::parse_str(req_id).unwrap();

    let cap = backend.await_request().await.unwrap();
    // X-Client-Trace-Id should be propagated
    assert_eq!(cap.header("x-client-trace-id"), Some(CLIENT_TRACE_ID));
    // X-Request-ID should be present and match the response
    assert_eq!(cap.header("x-request-id"), Some(req_id));

    // A new traceparent (W3C root span) should be generated for the downstream
    let tp = cap.header("traceparent").expect("traceparent should be generated for new trace");
    let parts: Vec<&str> = tp.split('-').collect();
    assert_eq!(parts.len(), 4, "traceparent should have 4 parts (version-trace-span-flags)");
    assert_eq!(parts[0], "00", "traceparent version should be 00");
    // trace_id should be == CLIENT_TRACE_ID (the UUID parsed as u128)
    assert_eq!(parts[1].len(), 32, "trace ID should be 32 hex chars");
    assert_eq!(parts[3], "01", "flags should be 01 (sampled)");
    // span_id should be 16 hex chars, non-zero
    assert_eq!(parts[2].len(), 16, "span ID should be 16 hex chars");
    assert_ne!(parts[2], "0000000000000000", "span ID should be non-zero");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── X-Client-Trace-Id with 0% client_sampling does NOT trigger tracing ───
// Expected: X-Client-Trace-Id propagated, but no X-Request-ID generated.
// NOTE: with is_internal=true, X-Request-ID is always generated if
// generate_request_id=true. So this test uses generate_request_id=false
// to isolate the tracing trigger behavior.
#[tokio::test]
#[ignore]
async fn test_client_trace_id_sampling_0_no_tracing() {
    // Use generate=false so X-Request-ID only appears if tracing triggers it
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;
    let backend_addr = backend.addr();

    let cluster = presets::static_cluster("backend", backend_addr);

    let hcm = HcmBuilder::new()
        .http1()
        .generate_request_id(false)
        .preserve_external_request_id(true)
        .always_set_request_id_in_response(true)
        .tracing(Some(0), Some(0), Some(0))
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

    let resp = client.send(RequestBuilder::get("/test").header("x-client-trace-id", CLIENT_TRACE_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    // X-Client-Trace-Id should be propagated regardless
    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-client-trace-id"), Some(CLIENT_TRACE_ID));

    // No X-Request-ID should be generated (sampling=0, generate=false)
    assert!(resp.header("x-request-id").is_none());
    assert!(cap.header("x-request-id").is_none());

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── traceparent takes priority over X-Client-Trace-Id ───
// Incoming traceparent should be used for the trace context.
// The propagated traceparent should be a child of the incoming one
// (same trace_id, different span_id, parent set to the incoming span_id).
// X-Client-Trace-Id is still propagated but does NOT override the trace context.
#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
async fn test_client_trace_id_with_traceparent() {
    let (orion, client, mut backend, config_path) = setup_with_sampling(Some(100), Some(100), Some(100)).await;

    // VALID_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    // trace_id = 4bf92f3577b34da6a3ce929d0e0e4736, span_id = 00f067aa0ba902b7
    let incoming_trace_id = "4bf92f3577b34da6a3ce929d0e0e4736";
    let incoming_span_id = "00f067aa0ba902b7";

    // Use a DIFFERENT X-Client-Trace-Id from the traceparent's trace_id.
    // If Orion incorrectly used X-Client-Trace-Id as the trace_id, the
    // downstream traceparent would have CLIENT_TRACE_ID's trace_id instead.
    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("x-client-trace-id", CLIENT_TRACE_ID)
                .header("traceparent", VALID_TRACEPARENT),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();

    // traceparent should be propagated as a child of the incoming traceparent
    let tp = cap.header("traceparent").expect("traceparent should be propagated");
    let parts: Vec<&str> = tp.split('-').collect();
    assert_eq!(parts.len(), 4);
    assert_eq!(parts[0], "00", "version should be 00");
    // trace_id must be from the incoming traceparent, NOT from X-Client-Trace-Id
    assert_eq!(parts[1], incoming_trace_id, "trace_id must come from traceparent, not X-Client-Trace-Id");
    // span_id must be newly generated (child), not the incoming span_id
    assert_ne!(parts[2], incoming_span_id, "span_id should be a new child, not the incoming span_id");
    assert_eq!(parts[3], "01", "flags should be 01 (sampled)");

    // X-Client-Trace-Id should still be propagated alongside
    assert_eq!(cap.header("x-client-trace-id"), Some(CLIENT_TRACE_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── X-Client-Trace-Id with existing b3 header ───
// b3 takes priority for the trace context.
// X-Client-Trace-Id is still propagated to upstream.
#[tokio::test]
#[ignore]
async fn test_client_trace_id_with_b3() {
    let (orion, client, mut backend, config_path) = setup_with_sampling(Some(100), Some(100), Some(100)).await;

    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("x-client-trace-id", CLIENT_TRACE_ID)
                .header("b3", "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1"),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();

    // b3 header should be propagated to upstream (spawned child)
    assert!(cap.header("b3").is_some(), "b3 header should be propagated");

    // X-Client-Trace-Id should be propagated to upstream
    assert_eq!(cap.header("x-client-trace-id"), Some(CLIENT_TRACE_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── X-Client-Trace-Id with existing uber-trace-id (Jaeger) header ───
// uber-trace-id takes priority for the trace context.
// X-Client-Trace-Id is still propagated to upstream.
#[tokio::test]
#[ignore]
async fn test_client_trace_id_with_uber() {
    let (orion, client, mut backend, config_path) = setup_with_sampling(Some(100), Some(100), Some(100)).await;

    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("x-client-trace-id", CLIENT_TRACE_ID)
                .header("uber-trace-id", "4bf92f3577b34da6a3ce929d0e0e4736:00f067aa0ba902b7:0:1"),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();

    // uber-trace-id should be propagated to upstream (spawned child)
    assert!(cap.header("uber-trace-id").is_some(), "uber-trace-id should be propagated");

    // X-Client-Trace-Id should be propagated to upstream
    assert_eq!(cap.header("x-client-trace-id"), Some(CLIENT_TRACE_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── X-Client-Trace-Id with non-UUID value does not trigger tracing ───
// The header is still propagated, but no tracing context is created.
#[tokio::test]
#[ignore]
async fn test_client_trace_id_invalid_no_tracing() {
    // generate=false to isolate tracing trigger
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;
    let backend_addr = backend.addr();

    let cluster = presets::static_cluster("backend", backend_addr);

    let hcm = HcmBuilder::new()
        .http1()
        .generate_request_id(false)
        .preserve_external_request_id(true)
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

    let resp =
        client.send(RequestBuilder::get("/test").header("x-client-trace-id", INVALID_CLIENT_TRACE_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();

    // Non-UUID value should still be propagated
    assert_eq!(cap.header("x-client-trace-id"), Some(INVALID_CLIENT_TRACE_ID));

    // No tracing should be triggered (generate=false, non-UUID, no x-request-id)
    assert!(resp.header("x-request-id").is_none());
    assert!(cap.header("x-request-id").is_none());

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── traceparent alone (no X-Client-Trace-Id) triggers tracing ───
#[tokio::test]
#[ignore]
async fn test_traceparent_alone_triggers_tracing() {
    let (orion, client, mut backend, config_path) = setup_with_sampling(Some(100), Some(100), Some(100)).await;

    let resp = client.send(RequestBuilder::get("/test").header("traceparent", VALID_TRACEPARENT)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();

    // traceparent should be propagated to upstream (child span)
    assert!(cap.header("traceparent").is_some(), "traceparent should be propagated");

    // No X-Client-Trace-Id in request, none expected upstream
    assert!(cap.header("x-client-trace-id").is_none());

    orion.shutdown();
    cleanup_config_file(&config_path);
}
