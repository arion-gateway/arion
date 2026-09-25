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

//! E2E tests for X-Envoy-Force-Trace header.
//!
//! When present, this header forces tracing regardless of sampling rates.
//! All tests run against localhost (`is_internal=true`).

use arion_e2e_tests::config_builder::{presets, FilterChainBuilder, HcmBuilder, ListenerBuilder};
use arion_e2e_tests::{
    cleanup_config_file, ArionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
};
use http::StatusCode;

async fn setup(
    client_sampling: Option<u32>,
    random_sampling: Option<u32>,
    overall_sampling: Option<u32>,
) -> (ArionInstance, TestClient, TestBackend, std::path::PathBuf) {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;
    let backend_addr = backend.addr();
    let cluster = presets::static_cluster("backend", backend_addr);

    let hcm = HcmBuilder::new()
        .http1()
        .generate_request_id(true)
        .always_set_request_id_in_response(true)
        .tracing(client_sampling, random_sampling, overall_sampling)
        .route_config(
            arion_e2e_tests::config_builder::RouteConfigBuilder::new("routes").virtual_host(
                arion_e2e_tests::config_builder::VirtualHostBuilder::new("default")
                    .route(presets::default_route("backend")),
            ),
        );
    let listener = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm));
    let bootstrap = arion_e2e_tests::config_builder::BootstrapBuilder::new().listener(listener).cluster(cluster);
    let config_path = bootstrap.build_to_temp().unwrap();
    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(arion.listener_addr().unwrap());
    (arion, client, backend, config_path)
}

// ─── Force trace without any trace context ───
// Should create a new trace and generate X-Request-ID + traceparent.
#[tokio::test]
#[ignore]
async fn test_force_trace_no_context() {
    let (arion, client, mut backend, config_path) = setup(Some(100), Some(100), Some(100)).await;

    let resp = client.send(RequestBuilder::get("/test").header("x-envoy-force-trace", "")).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let req_id = resp.header("x-request-id").expect("force-trace should generate x-request-id");
    uuid::Uuid::parse_str(req_id).unwrap();

    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-request-id"), Some(req_id));
    assert!(cap.header("traceparent").is_some(), "force-trace should create a traceparent");

    arion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Force trace with existing traceparent ───
// Existing trace context should be used (force-trace is redundant).
#[tokio::test]
#[ignore]
async fn test_force_trace_with_traceparent() {
    let (arion, client, mut backend, config_path) = setup(Some(100), Some(100), Some(100)).await;

    let tp_in = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let resp = client
        .send(RequestBuilder::get("/test").header("traceparent", tp_in).header("x-envoy-force-trace", ""))
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    let tp_out = cap.header("traceparent").expect("traceparent should be propagated");
    assert!(tp_out.starts_with("00-4bf92f3577b34da6a3ce929d0e0e4736-"), "existing trace context should be used");

    arion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Force trace overrides 0% client_sampling ───
#[tokio::test]
#[ignore]
async fn test_force_trace_overrides_0_sampling() {
    let (arion, client, mut backend, config_path) = setup(Some(0), Some(0), Some(0)).await;

    let resp = client
        .send(
            RequestBuilder::get("/test")
                .header("x-client-trace-id", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
                .header("x-envoy-force-trace", ""),
        )
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    // Force-trace should override 0% sampling: tracing must be active
    assert!(resp.header("x-request-id").is_some(), "force-trace should override 0% sampling");
    let cap = backend.await_request().await.unwrap();
    assert!(cap.header("traceparent").is_some(), "traceparent should be generated despite 0% sampling");

    arion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Force trace alone with 0% sampling ───
// Even without X-Client-Trace-Id, force-trace should work.
#[tokio::test]
#[ignore]
async fn test_force_trace_alone_0_sampling() {
    let (arion, client, _ibackend, config_path) = setup(Some(0), Some(0), Some(0)).await;

    let resp = client.send(RequestBuilder::get("/test").header("x-envoy-force-trace", "")).await.unwrap();
    resp.assert_status(StatusCode::OK);

    assert!(resp.header("x-request-id").is_some(), "force-trace alone should override 0% sampling");

    arion.shutdown();
    cleanup_config_file(&config_path);
}
