// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, HttpRbacBuilder,
    HttpRbacPolicyBuilder, ListenerBuilder, RouteBuilder, RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestClient};

fn simple_route_config() -> RouteConfigBuilder {
    RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    )
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_allow_any() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac =
        HttpRbacBuilder::allow().policy("allow-all", HttpRbacPolicyBuilder::new().permission_any().principal_any());

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);
    response.assert_body("OK");

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_deny_any() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac =
        HttpRbacBuilder::deny().policy("deny-all", HttpRbacPolicyBuilder::new().permission_any().principal_any());

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_allow_no_match_returns_403() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::allow().policy(
        "allow-other",
        HttpRbacPolicyBuilder::new().permission_header_exact("x-custom", "special").principal_any(),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_deny_no_match_allows() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::deny().policy(
        "deny-other",
        HttpRbacPolicyBuilder::new().permission_header_exact("x-custom", "special").principal_any(),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_allow_header_permission_match() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::allow().policy(
        "allow-custom-header",
        HttpRbacPolicyBuilder::new().permission_header_exact("x-custom", "allowed").principal_any(),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap()).with_header("x-custom", "allowed");
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_allow_host_header() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::allow().policy(
        "allow-host",
        HttpRbacPolicyBuilder::new().permission_header_exact("host", "allowed.example.com").principal_any(),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap()).with_header("host", "allowed.example.com");
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_deny_header_blocks() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::deny().policy(
        "deny-header",
        HttpRbacPolicyBuilder::new().permission_header_exact("x-blocked", "true").principal_any(),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap()).with_header("x-blocked", "true");
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_principal_header_match() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::allow().policy(
        "allow-auth",
        HttpRbacPolicyBuilder::new().permission_any().principal_header_exact("authorization", "Bearer valid-token"),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap()).with_header("authorization", "Bearer valid-token");
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_principal_header_no_match() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::allow().policy(
        "allow-auth",
        HttpRbacPolicyBuilder::new().permission_any().principal_header_exact("authorization", "Bearer valid-token"),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap()).with_header("authorization", "Bearer invalid-token");
    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_multiple_policies() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::allow()
        .policy("aaa-admin", HttpRbacPolicyBuilder::new().permission_any().principal_header_exact("x-role", "admin"))
        .policy("bbb-user", HttpRbacPolicyBuilder::new().permission_any().principal_header_exact("x-role", "user"));

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let admin_client = TestClient::new(orion.listener_addr().unwrap()).with_header("x-role", "admin");
    let response = admin_client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let user_client = TestClient::new(orion.listener_addr().unwrap()).with_header("x-role", "user");
    let response = user_client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let guest_client = TestClient::new(orion.listener_addr().unwrap()).with_header("x-role", "guest");
    let response = guest_client.get("/test").await.unwrap();
    response.assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_multiple_permissions_or() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::allow().policy(
        "allow-multiple",
        HttpRbacPolicyBuilder::new()
            .permission_header_exact("x-api-key", "key1")
            .permission_header_exact("x-api-key", "key2")
            .principal_any(),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client1 = TestClient::new(orion.listener_addr().unwrap()).with_header("x-api-key", "key1");
    let response = client1.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let client2 = TestClient::new(orion.listener_addr().unwrap()).with_header("x-api-key", "key2");
    let response = client2.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let client3 = TestClient::new(orion.listener_addr().unwrap()).with_header("x-api-key", "key3");
    let response = client3.get("/test").await.unwrap();
    response.assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_permission_and_principal() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::allow().policy(
        "require-both",
        HttpRbacPolicyBuilder::new()
            .permission_header_exact("x-service", "allowed-service")
            .principal_header_exact("x-user", "authorized-user"),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let both_client = TestClient::new(orion.listener_addr().unwrap())
        .with_header("x-service", "allowed-service")
        .with_header("x-user", "authorized-user");
    let response = both_client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let service_only = TestClient::new(orion.listener_addr().unwrap()).with_header("x-service", "allowed-service");
    let response = service_only.get("/test").await.unwrap();
    response.assert_status(StatusCode::FORBIDDEN);

    let user_only = TestClient::new(orion.listener_addr().unwrap()).with_header("x-user", "authorized-user");
    let response = user_only.get("/test").await.unwrap();
    response.assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_http_rbac_header_present() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let rbac = HttpRbacBuilder::allow().policy(
        "require-auth-header",
        HttpRbacPolicyBuilder::new().permission_any().principal_header_present("authorization"),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().http_rbac(rbac).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let with_auth = TestClient::new(orion.listener_addr().unwrap()).with_header("authorization", "any-value");
    let response = with_auth.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let without_auth = TestClient::new(orion.listener_addr().unwrap());
    let response = without_auth.get("/test").await.unwrap();
    response.assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}
