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

use std::net::SocketAddr;

use arion_e2e_tests::config_builder::{
    presets, BootstrapBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder, RouteConfigBuilder,
    VirtualHostBuilder,
};
use arion_e2e_tests::{
    cleanup_config_file, ArionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient,
};
use http::StatusCode;

fn hcm_with_default_route() -> HcmBuilder {
    HcmBuilder::new().route_config(RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    ))
}

fn bootstrap_with_hcm(hcm: HcmBuilder, backend: SocketAddr) -> BootstrapBuilder {
    let listener = ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm));
    BootstrapBuilder::new().listener(listener).cluster(presets::static_cluster("backend", backend))
}

#[tokio::test]
#[ignore]
async fn test_early_append_header() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap =
        bootstrap_with_hcm(hcm_with_default_route().early_append_header("x-early-added", "early"), backend.addr());
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-early-added"), Some("early"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_early_remove_header() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = bootstrap_with_hcm(hcm_with_default_route().early_remove_header("x-strip-me"), backend.addr());
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client
        .send(RequestBuilder::get("/test").header("x-strip-me", "gone").header("x-keep-me", "stays"))
        .await
        .unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert!(captured.header("x-strip-me").is_none());
    assert_eq!(captured.header("x-keep-me"), Some("stays"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_early_remove_on_match_prefix() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap =
        bootstrap_with_hcm(hcm_with_default_route().early_remove_on_match_prefix("x-internal-"), backend.addr());
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client
        .send(
            RequestBuilder::get("/test")
                .header("x-internal-token", "secret-1")
                .header("x-internal-key", "secret-2")
                .header("x-public", "visible"),
        )
        .await
        .unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert!(captured.header("x-internal-token").is_none());
    assert!(captured.header("x-internal-key").is_none());
    assert_eq!(captured.header("x-public"), Some("visible"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_early_mutations_combined() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = bootstrap_with_hcm(
        hcm_with_default_route()
            .early_append_header("x-early-added", "early")
            .early_remove_header("x-strip-me")
            .early_remove_on_match_prefix("x-internal-"),
        backend.addr(),
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client
        .send(
            RequestBuilder::get("/test")
                .header("x-strip-me", "gone")
                .header("x-internal-token", "secret")
                .header("x-keep-me", "stays"),
        )
        .await
        .unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-early-added"), Some("early"));
    assert!(captured.header("x-strip-me").is_none());
    assert!(captured.header("x-internal-token").is_none());
    assert_eq!(captured.header("x-keep-me"), Some("stays"));

    arion.shutdown();
    cleanup_config_file(&config_path);
}
