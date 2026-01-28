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

//! Tests for header manipulation inheritance across configuration levels.
//!
//! This file tests the three-level hierarchy of header manipulation:
//! - RouteConfiguration (global level)
//! - VirtualHost (group level)
//! - Route (specific level)
//!
//! It also tests the `most_specific_header_mutations_wins` flag which controls
//! the priority order of mutations.

use http::StatusCode;
use orion_e2e_tests::config_builder::{presets, RouteBuilder, RouteConfigBuilder, VirtualHostBuilder};
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient};

#[tokio::test]
#[ignore]
async fn test_vhost_request_headers_inherited() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let vhost = VirtualHostBuilder::new("default")
        .add_request_header("x-vhost", "from-vhost")
        .route(RouteBuilder::new().match_prefix("/").cluster("backend"));

    let bootstrap = presets::routed_proxy_with_vhost(vhost, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-vhost"), Some("from-vhost"));

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_route_request_header_overrides_vhost() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes").most_specific_header_mutations_wins(true).virtual_host(
        VirtualHostBuilder::new("default").add_request_header("x-version", "vhost-value").route(
            RouteBuilder::new()
                .match_prefix("/")
                .overwrite_request_header("x-version", "route-value")
                .cluster("backend"),
        ),
    );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    let values = captured.header_all("x-version");
    assert_eq!(values, vec!["route-value"], "Expected single value, got: {:?}", values);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_vhost_and_route_request_headers_combine() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let vhost = VirtualHostBuilder::new("default")
        .add_request_header("x-vhost", "vhost-value")
        .route(RouteBuilder::new().match_prefix("/").add_request_header("x-route", "route-value").cluster("backend"));

    let bootstrap = presets::routed_proxy_with_vhost(vhost, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-vhost"), Some("vhost-value"));
    assert_eq!(captured.header("x-route"), Some("route-value"));

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_route_removes_vhost_request_header() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes").most_specific_header_mutations_wins(true).virtual_host(
        VirtualHostBuilder::new("default")
            .add_request_header("x-remove-me", "vhost-added")
            .route(RouteBuilder::new().match_prefix("/").remove_request_header("x-remove-me").cluster("backend")),
    );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert!(captured.header("x-remove-me").is_none());

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_vhost_response_headers_inherited() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let vhost = VirtualHostBuilder::new("default")
        .add_response_header("x-vhost", "from-vhost")
        .route(RouteBuilder::new().match_prefix("/").cluster("backend"));

    let bootstrap = presets::routed_proxy_with_vhost(vhost, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-vhost", "from-vhost");

    backend.await_request().await.unwrap();

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_route_response_header_overrides_vhost() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes").most_specific_header_mutations_wins(true).virtual_host(
        VirtualHostBuilder::new("default").add_response_header("x-version", "vhost-value").route(
            RouteBuilder::new()
                .match_prefix("/")
                .overwrite_response_header("x-version", "route-value")
                .cluster("backend"),
        ),
    );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    let values = response.header_all("x-version");
    assert_eq!(values, vec!["route-value"], "Expected single value, got: {:?}", values);

    backend.await_request().await.unwrap();

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_vhost_and_route_response_headers_combine() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let vhost = VirtualHostBuilder::new("default")
        .add_response_header("x-vhost", "vhost-value")
        .route(RouteBuilder::new().match_prefix("/").add_response_header("x-route", "route-value").cluster("backend"));

    let bootstrap = presets::routed_proxy_with_vhost(vhost, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-vhost", "vhost-value");
    response.assert_header("x-route", "route-value");

    backend.await_request().await.unwrap();

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_route_removes_vhost_response_header() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK").header("x-backend", "from-backend")).await;

    let route_config = RouteConfigBuilder::new("routes").most_specific_header_mutations_wins(true).virtual_host(
        VirtualHostBuilder::new("default")
            .add_response_header("x-remove-me", "vhost-added")
            .route(RouteBuilder::new().match_prefix("/").remove_response_header("x-remove-me").cluster("backend")),
    );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    assert!(response.header("x-remove-me").is_none());
    response.assert_header("x-backend", "from-backend");

    backend.await_request().await.unwrap();

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_route_config_request_headers_inherited() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes").add_request_header("x-config", "from-config").virtual_host(
        VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-config"), Some("from-config"));

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_route_config_response_headers_inherited() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes").add_response_header("x-config", "from-config").virtual_host(
        VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-config", "from-config");

    backend.await_request().await.unwrap();

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_route_config_headers_with_multiple_vhosts() {
    let mut backend_a = TestBackend::start().await.unwrap();
    let mut backend_b = TestBackend::start().await.unwrap();
    backend_a.set_default_response(PreConfiguredResponse::with_body("backend-a")).await;
    backend_b.set_default_response(PreConfiguredResponse::with_body("backend-b")).await;

    let route_config = RouteConfigBuilder::new("routes")
        .add_request_header("x-config", "from-config")
        .virtual_host(
            VirtualHostBuilder::new("vhost-a")
                .domains(["a.example.com"])
                .route(RouteBuilder::new().match_prefix("/").cluster("backend-a")),
        )
        .virtual_host(
            VirtualHostBuilder::new("vhost-b")
                .domains(["b.example.com"])
                .route(RouteBuilder::new().match_prefix("/").cluster("backend-b")),
        );

    let bootstrap = presets::routed_proxy_with_config(
        route_config,
        [
            presets::static_cluster("backend-a", backend_a.addr()),
            presets::static_cluster("backend-b", backend_b.addr()),
        ],
    );
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.send(RequestBuilder::get("/test").header("host", "a.example.com")).await.unwrap();
    response.assert_status(StatusCode::OK);
    let captured_a = backend_a.await_request().await.unwrap();
    assert_eq!(captured_a.header("x-config"), Some("from-config"));

    let response = client.send(RequestBuilder::get("/test").header("host", "b.example.com")).await.unwrap();
    response.assert_status(StatusCode::OK);
    let captured_b = backend_b.await_request().await.unwrap();
    assert_eq!(captured_b.header("x-config"), Some("from-config"));

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_all_three_levels_request_headers_combine() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes").add_request_header("x-config", "from-config").virtual_host(
        VirtualHostBuilder::new("default").add_request_header("x-vhost", "from-vhost").route(
            RouteBuilder::new().match_prefix("/").add_request_header("x-route", "from-route").cluster("backend"),
        ),
    );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-config"), Some("from-config"));
    assert_eq!(captured.header("x-vhost"), Some("from-vhost"));
    assert_eq!(captured.header("x-route"), Some("from-route"));

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_all_three_levels_response_headers_combine() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes").add_response_header("x-config", "from-config").virtual_host(
        VirtualHostBuilder::new("default").add_response_header("x-vhost", "from-vhost").route(
            RouteBuilder::new().match_prefix("/").add_response_header("x-route", "from-route").cluster("backend"),
        ),
    );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_header("x-config", "from-config");
    response.assert_header("x-vhost", "from-vhost");
    response.assert_header("x-route", "from-route");

    backend.await_request().await.unwrap();

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_route_overrides_vhost_overrides_config() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes")
        .most_specific_header_mutations_wins(true)
        .overwrite_request_header("x-version", "config-value")
        .virtual_host(
            VirtualHostBuilder::new("default").overwrite_request_header("x-version", "vhost-value").route(
                RouteBuilder::new()
                    .match_prefix("/")
                    .overwrite_request_header("x-version", "route-value")
                    .cluster("backend"),
            ),
        );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    let values = captured.header_all("x-version");
    assert_eq!(values, vec!["route-value"], "Expected single value, got: {:?}", values);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_most_specific_wins_true_route_priority() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes")
        .most_specific_header_mutations_wins(true)
        .overwrite_request_header("x-value", "config-value")
        .virtual_host(VirtualHostBuilder::new("default").overwrite_request_header("x-value", "vhost-value").route(
            RouteBuilder::new().match_prefix("/").overwrite_request_header("x-value", "route-value").cluster("backend"),
        ));

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-value"), Some("route-value"));

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_most_specific_wins_false_config_priority() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes")
        .most_specific_header_mutations_wins(false)
        .overwrite_request_header("x-value", "config-value")
        .virtual_host(VirtualHostBuilder::new("default").overwrite_request_header("x-value", "vhost-value").route(
            RouteBuilder::new().match_prefix("/").overwrite_request_header("x-value", "route-value").cluster("backend"),
        ));

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert_eq!(captured.header("x-value"), Some("config-value"));

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_priority_affects_execution_order() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes")
        .most_specific_header_mutations_wins(true)
        .add_request_header("x-header", "from-config")
        .virtual_host(
            VirtualHostBuilder::new("default")
                .remove_request_header("x-header")
                .route(RouteBuilder::new().match_prefix("/").cluster("backend")),
        );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert!(captured.header("x-header").is_none());

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_vhost_add_if_absent_route_overwrite() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config = RouteConfigBuilder::new("routes").most_specific_header_mutations_wins(true).virtual_host(
        VirtualHostBuilder::new("default").add_request_header_if_absent("x-header", "vhost-default").route(
            RouteBuilder::new()
                .match_prefix("/")
                .overwrite_request_header("x-header", "route-override")
                .cluster("backend"),
        ),
    );

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    let values = captured.header_all("x-header");
    assert_eq!(values, vec!["route-override"], "Expected single value, got: {:?}", values);

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_route_removes_config_header() {
    let mut backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let route_config =
        RouteConfigBuilder::new("routes")
            .most_specific_header_mutations_wins(true)
            .add_request_header("x-config-header", "config-value")
            .virtual_host(VirtualHostBuilder::new("default").route(
                RouteBuilder::new().match_prefix("/").remove_request_header("x-config-header").cluster("backend"),
            ));

    let bootstrap =
        presets::routed_proxy_with_config(route_config, [presets::static_cluster("backend", backend.addr())]);
    let config_path = bootstrap.build_to_temp().unwrap();

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.unwrap();
    assert!(captured.header("x-config-header").is_none());

    orion.shutdown();
    let _ = std::fs::remove_file(&config_path);
}
