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

use http::StatusCode;
use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestClient,
};

#[tokio::test]
#[ignore]
async fn test_basic_http_proxy() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    backend.set_default_response(PreConfiguredResponse::with_body("Hello from backend!")).await;

    let bootstrap = presets::simple_proxy("backend", backend_addr);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/hello").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Hello from backend!");

    let captured_request = backend.await_request().await.expect("No request received by backend");
    assert_eq!(captured_request.path(), "/hello");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_multiple_requests() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("response")).await;

    let bootstrap = presets::simple_proxy("backend", backend.addr());
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());

    for i in 0..5 {
        let response = client.get(&format!("/request/{i}")).await.expect("Failed to send request");
        response.assert_status(StatusCode::OK);

        let req = backend.await_request().await.expect("No request received");
        assert_eq!(req.path(), format!("/request/{i}"));
    }

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_direct_response() {
    let bootstrap = presets::routed_proxy(
        [presets::direct_response_route("/health", 200, "OK"), presets::default_route("dummy_backend")],
        [presets::static_cluster("dummy_backend", "127.0.0.1:9999".parse().unwrap())],
    );
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/health").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("OK");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_post_with_body() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_status(StatusCode::CREATED)).await;

    let bootstrap = presets::simple_proxy("backend", backend.addr());
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let client = TestClient::new(orion.listener_addr().unwrap());
    let body = r#"{"name": "test", "value": 42}"#;
    let response = client.post("/api/data", body).await.expect("Failed to send request");

    response.assert_status(StatusCode::CREATED);

    let req = backend.await_request().await.expect("No request received");
    assert_eq!(req.path(), "/api/data");
    assert_eq!(req.body_str(), Some(body));

    orion.shutdown();
    cleanup_config_file(&config_path);
}
