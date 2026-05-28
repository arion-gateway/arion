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

//! E2E tests for behavior when tracing is NOT configured on the HCM.
//!
//! All tests run against localhost (is_internal=true).

use http::StatusCode;
use orion_e2e_tests::config_builder::{presets, FilterChainBuilder, HcmBuilder, ListenerBuilder};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
};

async fn setup_no_tracing() -> (OrionInstance, TestClient, TestBackend, std::path::PathBuf) {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;
    let backend_addr = backend.addr();
    let cluster = presets::static_cluster("backend", backend_addr);

    // HCM WITHOUT .tracing() — no tracing config at all
    let hcm = HcmBuilder::new()
        .http1()
        .generate_request_id(true)
        .always_set_request_id_in_response(true)
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

// ─── No tracing config: traceparent passes through unchanged ───
#[tokio::test]
#[ignore]
async fn test_no_tracing_traceparent_passthrough() {
    let (orion, client, mut backend, config_path) = setup_no_tracing().await;

    let tp_in = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    let resp = client.send(RequestBuilder::get("/test").header("traceparent", tp_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    // Without tracing config, traceparent should pass through unchanged
    assert_eq!(cap.header("traceparent"), Some(tp_in));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── No tracing config: b3 passes through unchanged ───
#[tokio::test]
#[ignore]
async fn test_no_tracing_b3_passthrough() {
    let (orion, client, mut backend, config_path) = setup_no_tracing().await;

    let b3_in = "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1";
    let resp = client.send(RequestBuilder::get("/test").header("b3", b3_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("b3"), Some(b3_in));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── No tracing config: uber-trace-id passes through unchanged ───
#[tokio::test]
#[ignore]
async fn test_no_tracing_uber_passthrough() {
    let (orion, client, mut backend, config_path) = setup_no_tracing().await;

    let uber_in = "4bf92f3577b34da6a3ce929d0e0e4736:00f067aa0ba902b7:0:1";
    let resp = client.send(RequestBuilder::get("/test").header("uber-trace-id", uber_in)).await.unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("uber-trace-id"), Some(uber_in));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── No tracing config: X-Client-Trace-Id does not trigger tracing ───
#[tokio::test]
#[ignore]
async fn test_no_tracing_client_trace_id_no_trigger() {
    let (orion, client, mut backend, config_path) = setup_no_tracing().await;

    let resp = client
        .send(RequestBuilder::get("/test").header("x-client-trace-id", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    // X-Client-Trace-Id should still be propagated
    assert_eq!(cap.header("x-client-trace-id"), Some("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"));
    // But no traceparent should be generated (tracing disabled)
    assert!(cap.header("traceparent").is_none());

    orion.shutdown();
    cleanup_config_file(&config_path);
}