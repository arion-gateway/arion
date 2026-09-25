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

//! E2E tests for X-Request-ID behavior across all 8 combinations of:
//! - `generate_request_id`
//! - `preserve_external_request_id`
//! - `always_set_request_id_in_response`
//!
//! Each test sends two requests: one without X-Request-ID and one with a known X-Request-ID.
//! Assertions are made on both the upstream request (backend) and the downstream response.
//!
//! # Internal-only tests
//!
//! All tests run against localhost (127.0.0.1), so `is_internal` is always `true`.
//! When `is_internal=true`, an incoming X-Request-ID is always treated as authoritative
//! and forwarded upstream, regardless of `preserve_external_request_id`.
//!
//! This means the `preserve_external_request_id` setting is **not distinguishable** from
//! `generate_request_id` when an external X-Request-ID is present — both will preserve it.
//! Cases 1-2-5-6 vs 3-4-7-8 produce identical behavior for the "with X-Request-ID" leg.
//! The full matrix is kept so tests are ready when a non-localhost test harness becomes
//! available (e.g. via network namespaces or containerized tests).

use http::StatusCode;
use orion_e2e_tests::config_builder::{presets, FilterChainBuilder, HcmBuilder, ListenerBuilder};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
};

const KNOWN_REQUEST_ID: &str = "123e4567-e89b-12d3-a456-426614174000";

async fn setup(
    generate: bool,
    preserve: bool,
    always_set: bool,
) -> (OrionInstance, TestClient, TestBackend, std::path::PathBuf) {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;
    let backend_addr = backend.addr();

    let cluster = presets::static_cluster("backend", backend_addr);

    let hcm = HcmBuilder::new()
        .http1()
        .generate_request_id(generate)
        .preserve_external_request_id(preserve)
        .always_set_request_id_in_response(always_set)
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

// ─── Case 1: generate=false, preserve=false, always_set=false ───
// w/o X-Request-ID: not forwarded, not in response
// with X-Request-ID: preserved (is_internal=true), not in response
#[tokio::test]
#[ignore]
async fn test_x_request_id_gen_false_preserve_false_always_false() {
    let (orion, client, mut backend, config_path) = setup(false, false, false).await;

    let resp = client.get("/test1").await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_none());
    let cap = backend.await_request().await.unwrap();
    assert!(cap.header("x-request-id").is_none());

    let resp = client.send(RequestBuilder::get("/test2").header("x-request-id", KNOWN_REQUEST_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_none());
    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-request-id"), Some(KNOWN_REQUEST_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Case 2: generate=false, preserve=false, always_set=true ───
// w/o X-Request-ID: not forwarded, not in response
// with X-Request-ID: preserved (is_internal=true) AND set in response (always_set=true)
#[tokio::test]
#[ignore]
async fn test_x_request_id_gen_false_preserve_false_always_true() {
    let (orion, client, mut backend, config_path) = setup(false, false, true).await;

    let resp = client.get("/test1").await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_none());
    let cap = backend.await_request().await.unwrap();
    assert!(cap.header("x-request-id").is_none());

    let resp = client.send(RequestBuilder::get("/test2").header("x-request-id", KNOWN_REQUEST_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert_eq!(resp.header("x-request-id"), Some(KNOWN_REQUEST_ID));
    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-request-id"), Some(KNOWN_REQUEST_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Case 3: generate=false, preserve=true, always_set=false ───
// w/o X-Request-ID: not forwarded, not in response
// with X-Request-ID: preserved to upstream (is_internal=true), not in response
#[tokio::test]
#[ignore]
async fn test_x_request_id_gen_false_preserve_true_always_false() {
    let (orion, client, mut backend, config_path) = setup(false, true, false).await;

    let resp = client.get("/test1").await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_none());
    let cap = backend.await_request().await.unwrap();
    assert!(cap.header("x-request-id").is_none());

    let resp = client.send(RequestBuilder::get("/test2").header("x-request-id", KNOWN_REQUEST_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_none());
    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-request-id"), Some(KNOWN_REQUEST_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Case 4: generate=false, preserve=true, always_set=true ───
// w/o X-Request-ID: not forwarded, not in response
// with X-Request-ID: preserved to upstream AND set in response
#[tokio::test]
#[ignore]
async fn test_x_request_id_gen_false_preserve_true_always_true() {
    let (orion, client, mut backend, config_path) = setup(false, true, true).await;

    let resp = client.get("/test1").await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_none());
    let cap = backend.await_request().await.unwrap();
    assert!(cap.header("x-request-id").is_none());

    let resp = client.send(RequestBuilder::get("/test2").header("x-request-id", KNOWN_REQUEST_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert_eq!(resp.header("x-request-id"), Some(KNOWN_REQUEST_ID));
    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-request-id"), Some(KNOWN_REQUEST_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Case 5: generate=true, preserve=false, always_set=false ───
// w/o X-Request-ID: generated ID forwarded to upstream, not in response
// with X-Request-ID: preserved (is_internal=true), not in response
#[tokio::test]
#[ignore]
#[allow(clippy::expect_used)]
async fn test_x_request_id_gen_true_preserve_false_always_false() {
    let (orion, client, mut backend, config_path) = setup(true, false, false).await;

    let resp = client.get("/test1").await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_none());
    let cap = backend.await_request().await.unwrap();
    let id1 = cap.header("x-request-id").expect("should have generated x-request-id");
    uuid::Uuid::parse_str(id1).unwrap();

    let resp = client.send(RequestBuilder::get("/test2").header("x-request-id", KNOWN_REQUEST_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_none());
    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-request-id"), Some(KNOWN_REQUEST_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Case 6: generate=true, preserve=false, always_set=true ───
// w/o X-Request-ID: generated ID to upstream AND in response
// with X-Request-ID: preserved (is_internal=true) to upstream AND in response
#[tokio::test]
#[ignore]
async fn test_x_request_id_gen_true_preserve_false_always_true() {
    let (orion, client, mut backend, config_path) = setup(true, false, true).await;

    let resp = client.get("/test1").await.unwrap();
    resp.assert_status(StatusCode::OK);
    let resp_id1 = resp.header("x-request-id").expect("should have x-request-id in response");
    uuid::Uuid::parse_str(resp_id1).unwrap();
    let cap = backend.await_request().await.unwrap();
    let up_id1 = cap.header("x-request-id").expect("should have generated x-request-id");
    assert_eq!(resp_id1, up_id1);

    let resp = client.send(RequestBuilder::get("/test2").header("x-request-id", KNOWN_REQUEST_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert_eq!(resp.header("x-request-id"), Some(KNOWN_REQUEST_ID));
    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-request-id"), Some(KNOWN_REQUEST_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Case 7: generate=true, preserve=true, always_set=false ───
// w/o X-Request-ID: generated ID to upstream, not in response
// with X-Request-ID: preserved to upstream, not in response
#[tokio::test]
#[ignore]
async fn test_x_request_id_gen_true_preserve_true_always_false() {
    let (orion, client, mut backend, config_path) = setup(true, true, false).await;

    let resp = client.get("/test1").await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_none());
    let cap = backend.await_request().await.unwrap();
    let id1 = cap.header("x-request-id").expect("should have generated x-request-id");
    uuid::Uuid::parse_str(id1).unwrap();

    let resp = client.send(RequestBuilder::get("/test2").header("x-request-id", KNOWN_REQUEST_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert!(resp.header("x-request-id").is_none());
    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-request-id"), Some(KNOWN_REQUEST_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}

// ─── Case 8: generate=true, preserve=true, always_set=true ───
// w/o X-Request-ID: generated ID to upstream AND in response
// with X-Request-ID: preserved to upstream AND in response
#[tokio::test]
#[ignore]
async fn test_x_request_id_gen_true_preserve_true_always_true() {
    let (orion, client, mut backend, config_path) = setup(true, true, true).await;

    let resp = client.get("/test1").await.unwrap();
    resp.assert_status(StatusCode::OK);
    let resp_id1 = resp.header("x-request-id").expect("should have x-request-id in response");
    uuid::Uuid::parse_str(resp_id1).unwrap();
    let cap = backend.await_request().await.unwrap();
    let up_id1 = cap.header("x-request-id").expect("should have generated x-request-id");
    assert_eq!(resp_id1, up_id1);

    let resp = client.send(RequestBuilder::get("/test2").header("x-request-id", KNOWN_REQUEST_ID)).await.unwrap();
    resp.assert_status(StatusCode::OK);
    assert_eq!(resp.header("x-request-id"), Some(KNOWN_REQUEST_ID));
    let cap = backend.await_request().await.unwrap();
    assert_eq!(cap.header("x-request-id"), Some(KNOWN_REQUEST_ID));

    orion.shutdown();
    cleanup_config_file(&config_path);
}
