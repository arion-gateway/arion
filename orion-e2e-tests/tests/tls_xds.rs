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

use std::net::SocketAddr;
use std::time::Duration;

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    ClusterBuilder, DownstreamTlsBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder,
    RouteBuilder, RouteConfigBuilder, SecretBuilder, UpstreamTlsBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{
    PreConfiguredResponse, TestBackend, TestCerts, TlsTestBackend, TlsTestClientBuilder, XdsEnabledHarness,
};

#[tokio::test]
#[ignore]
async fn test_xds_push_tls_secret() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("SDS TLS OK!")).await;

    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");
    let certs = TestCerts::new();

    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let secret = SecretBuilder::new("server-cert")
        .tls_certificate_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .expect("Failed to load cert files")
        .build();

    harness.push_secret(&secret).await.expect("Failed to push secret");

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).build();

    let listener = ListenerBuilder::new("https")
        .port(listener_port)
        .filter_chain(
            FilterChainBuilder::new("main").downstream_tls(DownstreamTlsBuilder::new().sds_secret("server-cert")).hcm(
                HcmBuilder::new().route_config(RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                )),
            ),
        )
        .build();

    harness.push_cluster(&cluster).await.expect("Failed to push cluster");
    harness.push_listener(&listener).await.expect("Failed to push listener");

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.expect("Listener not ready");

    let client = TlsTestClientBuilder::new(listener_addr)
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let response = client.get("/test").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("SDS TLS OK!");

    let captured_request = backend.await_request().await.expect("No request received by backend");
    assert_eq!(captured_request.path(), "/test");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_xds_upstream_tls_sds_validation() {
    let certs = TestCerts::new();

    let backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Upstream SDS validation OK!")).await;

    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");

    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let ca_secret = SecretBuilder::new("upstream-ca")
        .validation_context_file(certs.beefcake_ca_chain())
        .expect("Failed to load CA file")
        .build();

    harness.push_secret(&ca_secret).await.expect("Failed to push CA secret");

    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(
            UpstreamTlsBuilder::new().sni("dublin.beefcake.example.com").validation_context_sds("upstream-ca"),
        )
        .build();

    let listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_cluster(&cluster).await.expect("Failed to push cluster");
    harness.push_listener(&listener).await.expect("Failed to push listener");

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.expect("Listener not ready");

    let client = orion_e2e_tests::TestClient::new(listener_addr);
    let response = client.get("/upstream-sds").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Upstream SDS validation OK!");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_xds_mtls_downstream_sds() {
    let backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("mTLS SDS OK!")).await;

    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");
    let certs = TestCerts::new();

    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let server_cert_secret = SecretBuilder::new("server-cert")
        .tls_certificate_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .expect("Failed to load server cert files")
        .build();

    let client_ca_secret = SecretBuilder::new("client-ca")
        .validation_context_file(certs.beefcake_ca_chain())
        .expect("Failed to load client CA file")
        .build();

    harness.push_secret(&server_cert_secret).await.expect("Failed to push server cert");
    harness.push_secret(&client_ca_secret).await.expect("Failed to push client CA");

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).build();

    let listener = ListenerBuilder::new("https")
        .port(listener_port)
        .filter_chain(
            FilterChainBuilder::new("main")
                .downstream_tls(
                    DownstreamTlsBuilder::new()
                        .sds_secret("server-cert")
                        .require_client_cert()
                        .validation_context_sds("client-ca"),
                )
                .hcm(HcmBuilder::new().route_config(RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                ))),
        )
        .build();

    harness.push_cluster(&cluster).await.expect("Failed to push cluster");
    harness.push_listener(&listener).await.expect("Failed to push listener");

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.expect("Listener not ready");

    let client = TlsTestClientBuilder::new(listener_addr)
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .client_cert(certs.beefcake_athlone_cert(), certs.beefcake_athlone_key())
        .build()
        .expect("Failed to build mTLS client");

    let response = client.get("/mtls-sds").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("mTLS SDS OK!");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_xds_full_tls_chain() {
    let certs = TestCerts::new();

    let mut backend = TlsTestBackend::start_with_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .await
        .expect("Failed to start TLS backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Full xDS TLS chain OK!")).await;

    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");

    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let server_cert = SecretBuilder::new("server-cert")
        .tls_certificate_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .expect("Failed to load server cert files")
        .build();

    let upstream_ca = SecretBuilder::new("upstream-ca")
        .validation_context_file(certs.beefcake_ca_chain())
        .expect("Failed to load upstream CA file")
        .build();

    harness.push_secret(&server_cert).await.expect("Failed to push server cert");
    harness.push_secret(&upstream_ca).await.expect("Failed to push upstream CA");

    let cluster = ClusterBuilder::new("backend")
        .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
        .upstream_tls(
            UpstreamTlsBuilder::new().sni("dublin.beefcake.example.com").validation_context_sds("upstream-ca"),
        )
        .build();

    let listener = ListenerBuilder::new("https")
        .port(listener_port)
        .filter_chain(
            FilterChainBuilder::new("main").downstream_tls(DownstreamTlsBuilder::new().sds_secret("server-cert")).hcm(
                HcmBuilder::new().route_config(RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                )),
            ),
        )
        .build();

    harness.push_cluster(&cluster).await.expect("Failed to push cluster");
    harness.push_listener(&listener).await.expect("Failed to push listener");

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.expect("Listener not ready");

    let client = TlsTestClientBuilder::new(listener_addr)
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let response = client.get("/full-chain").await.expect("Failed to send request");

    response.assert_status(StatusCode::OK);
    response.assert_body("Full xDS TLS chain OK!");

    let captured_request = backend.await_request().await.expect("No request received by backend");
    assert_eq!(captured_request.path(), "/full-chain");

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_xds_add_tls_dynamically() {
    let mut backend = TestBackend::start().await.expect("Failed to start test backend");
    backend.set_default_response(PreConfiguredResponse::with_body("Dynamic TLS added!")).await;

    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");
    let certs = TestCerts::new();

    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let cluster = ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())).build();
    harness.push_cluster(&cluster).await.expect("Failed to push cluster");

    let http_listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(FilterChainBuilder::new("main").hcm(HcmBuilder::new().route_config(
            RouteConfigBuilder::new("routes").virtual_host(
                VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
            ),
        )))
        .build();

    harness.push_listener(&http_listener).await.expect("Failed to push HTTP listener");

    harness
        .orion_mut()
        .wait_for_listener_at(listener_addr, Duration::from_secs(10))
        .await
        .expect("HTTP Listener not ready");

    let http_client = orion_e2e_tests::TestClient::new(listener_addr);
    let response = http_client.get("/http-phase").await.expect("Failed to send HTTP request");
    response.assert_status(StatusCode::OK);
    assert!(backend.await_request().await.is_ok(), "backend should have received a request");

    let server_cert = SecretBuilder::new("server-cert")
        .tls_certificate_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .expect("Failed to load server cert files")
        .build();

    harness.push_secret(&server_cert).await.expect("Failed to push server cert");

    let tls_listener = ListenerBuilder::new("http")
        .port(listener_port)
        .filter_chain(
            FilterChainBuilder::new("main").downstream_tls(DownstreamTlsBuilder::new().sds_secret("server-cert")).hcm(
                HcmBuilder::new().route_config(RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                )),
            ),
        )
        .build();

    harness.push_listener(&tls_listener).await.expect("Failed to push HTTPS listener");

    let tls_client = TlsTestClientBuilder::new(listener_addr)
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build TLS client");

    let mut last_error = None;
    for _ in 0..20 {
        match tls_client.get("/https-phase").await {
            Ok(response) => {
                response.assert_status(StatusCode::OK);
                response.assert_body("Dynamic TLS added!");
                last_error = None;
                break;
            },
            Err(e) => {
                last_error = Some(e);
                tokio::time::sleep(Duration::from_millis(100)).await;
            },
        }
    }
    if let Some(e) = last_error {
        panic!("TLS connection failed after retries: {e}");
    }

    harness.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_xds_sni_routing() {
    let mut backend1 = TestBackend::start().await.expect("Failed to start backend 1");
    let mut backend2 = TestBackend::start().await.expect("Failed to start backend 2");
    backend1.set_default_response(PreConfiguredResponse::with_body("backend1")).await;
    backend2.set_default_response(PreConfiguredResponse::with_body("backend2")).await;

    let mut harness = XdsEnabledHarness::start().await.expect("Failed to start harness");
    let certs = TestCerts::new();

    let listener_port = harness.allocate_listener_port().expect("Failed to allocate listener port");
    let listener_addr = SocketAddr::from(([127, 0, 0, 1], listener_port));

    let server_cert = SecretBuilder::new("server-cert")
        .tls_certificate_files(certs.beefcake_dublin_cert(), certs.beefcake_dublin_key())
        .expect("Failed to load server cert files")
        .build();

    harness.push_secret(&server_cert).await.expect("Failed to push server cert");

    let cluster1 = ClusterBuilder::new("backend1").endpoint(EndpointBuilder::from_socket_addr(backend1.addr())).build();
    let cluster2 = ClusterBuilder::new("backend2").endpoint(EndpointBuilder::from_socket_addr(backend2.addr())).build();

    harness.push_cluster(&cluster1).await.expect("Failed to push cluster1");
    harness.push_cluster(&cluster2).await.expect("Failed to push cluster2");

    let listener = ListenerBuilder::new("https")
        .port(listener_port)
        .with_tls_inspector()
        .filter_chain(
            FilterChainBuilder::new("dublin")
                .server_name("dublin.beefcake.example.com")
                .downstream_tls(DownstreamTlsBuilder::new().sds_secret("server-cert"))
                .hcm(HcmBuilder::new().route_config(RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend1")),
                ))),
        )
        .filter_chain(
            FilterChainBuilder::new("athlone")
                .server_name("athlone.beefcake.example.com")
                .downstream_tls(DownstreamTlsBuilder::new().sds_secret("server-cert"))
                .hcm(HcmBuilder::new().route_config(RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend2")),
                ))),
        )
        .build();

    harness.push_listener(&listener).await.expect("Failed to push listener");

    harness.orion_mut().wait_for_listener_at(listener_addr, Duration::from_secs(10)).await.expect("Listener not ready");

    let client_dublin = TlsTestClientBuilder::new(listener_addr)
        .server_name("dublin.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build dublin client");

    let response = client_dublin.get("/dublin").await.expect("Failed to send dublin request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend1");
    assert!(backend1.await_request().await.is_ok(), "backend should have received a request");

    let client_athlone = TlsTestClientBuilder::new(listener_addr)
        .server_name("athlone.beefcake.example.com")
        .root_ca(certs.beefcake_ca_chain())
        .build()
        .expect("Failed to build athlone client");

    let response = client_athlone.get("/athlone").await.expect("Failed to send athlone request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend2");
    assert!(backend2.await_request().await.is_ok(), "backend should have received a request");

    harness.shutdown();
}
