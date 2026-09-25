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

//! E2E tests for sampling rate interactions (`client_sampling`, `random_sampling`, `overall_sampling`).
//!
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

// ─── X-Client-Trace-Id: client_sampling=100%, overall_sampling=0% ───
// overall_sampling gate blocks even if client_sampling allows.
#[tokio::test]
#[ignore]
async fn test_sampling_overall_blocks_client() {
    let (arion, client, mut backend, config_path) = setup(Some(100), Some(100), Some(0)).await;

    let resp = client
        .send(RequestBuilder::get("/test").header("x-client-trace-id", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    // overall_sampling=0% should block tracing even with valid X-Client-Trace-Id
    assert!(cap.header("traceparent").is_none(), "overall 0% should block tracing");

    arion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── X-Client-Trace-Id: client_sampling=0%, overall_sampling=100% ───
// client_sampling gate blocks even if overall_sampling allows.
#[tokio::test]
#[ignore]
async fn test_sampling_client_blocks_overall() {
    let (arion, client, mut backend, config_path) = setup(Some(0), Some(100), Some(100)).await;

    let resp = client
        .send(RequestBuilder::get("/test").header("x-client-trace-id", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    // client_sampling=0% should block tracing even if overall allows it
    assert!(cap.header("traceparent").is_none(), "client_sampling 0% should block tracing");

    arion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── X-Request-ID: random_sampling=50%, overall_sampling=100% ───
// With 50% sampling, eventually we should get a traced request.
// We send multiple requests to trigger sampling at least once.
// NOTE: probabilistic — very unlikely to miss over 20 tries at 50%.
#[tokio::test]
#[ignore]
async fn test_sampling_random_50_triggers_eventually() {
    let (arion, client, mut backend, config_path) = setup(Some(0), Some(50), Some(100)).await;

    let mut found = false;
    for _ in 0..30 {
        let request_id = uuid::Uuid::new_v4().to_string();
        let resp = client.send(RequestBuilder::get("/test").header("x-request-id", &request_id)).await.unwrap();
        resp.assert_status(StatusCode::OK);

        let cap = backend.await_request().await.unwrap();
        if cap.header("traceparent").is_some() {
            found = true;
            break;
        }
    }

    assert!(found, "should have triggered tracing at least once with 50% random_sampling");

    arion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Sampling rates unset (None) → default 100% ───
// None means the Envoy default (100%).
#[tokio::test]
#[ignore]
async fn test_sampling_none_means_100() {
    let (arion, client, mut backend, config_path) = setup(None, None, None).await;

    let resp = client
        .send(RequestBuilder::get("/test").header("x-client-trace-id", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))
        .await
        .unwrap();
    resp.assert_status(StatusCode::OK);

    let cap = backend.await_request().await.unwrap();
    // With all rates unset (=100%), tracing should be active
    assert!(cap.header("traceparent").is_some(), "unset rates should default to 100%");

    arion.shutdown();
    cleanup_config_file(&config_path);
}
