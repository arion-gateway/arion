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

#![allow(clippy::expect_used, reason = "test infrastructure — panicking on setup failure is intentional")]

use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use arion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::ext_proc::v3::{
    ExtProcOverrides, ExtProcPerRoute,
    ext_proc_per_route::Override,
    processing_mode::{BodySendMode, HeaderSendMode},
};
use arion_data_plane_api::envoy_data_plane_api::{
    google::protobuf::{Any, BoolValue, value::Kind},
    prost::Message,
};
use arion_e2e_tests::config_builder::presets;
use arion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, ExtProcBuilder, FilterChainBuilder, HcmBuilder, LbPolicy,
    ListenerBuilder, Route, RouteBuilder, RouteConfigBuilder, VirtualHostBuilder,
};
use arion_e2e_tests::{
    ArionInstance, CapturedProcessingRequest, ExtProcTestServer, ExtProcTestServerBuilder, PreConfiguredResponse,
    RequestBuilder, SpawnOptions, TestBackend, TestClient, ext_proc_responses,
};
use http::StatusCode;

async fn setup(
    ext_proc_builder: ExtProcBuilder,
    ext_proc_server_builder: ExtProcTestServerBuilder,
) -> (TestBackend, ExtProcTestServer, ArionInstance, TestClient, PathBuf) {
    let backend = TestBackend::start().await.expect("backend start");
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    let ext_proc_server = ext_proc_server_builder.start().await.expect("ext_proc start");

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(
            HcmBuilder::new().http1().ext_proc(ext_proc_builder).route_config(
                RouteConfigBuilder::new("routes").virtual_host(
                    VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                ),
            ),
        )))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()))
        .cluster(presets::ext_proc_cluster("ext-proc-cluster", ext_proc_server.addr()));

    let config_path = bootstrap.build_to_temp().expect("build config");
    let arion =
        ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn arion");
    #[allow(clippy::unwrap_used)]
    let client = TestClient::new(arion.listener_addr().unwrap());

    (backend, ext_proc_server, arion, client, config_path)
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_request_headers_add_header() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only(),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::mutate_request_headers(&[("x-ext-proc", "added")], &[])),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.header("x-ext-proc"), Some("added"));
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_request_headers_remove_header() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only(),
        ExtProcTestServerBuilder::new().with_response(ext_proc_responses::mutate_request_headers(&[], &["x-custom"])),
    )
    .await;

    let response = client.send(RequestBuilder::get("/test").header("x-custom", "value")).await.expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.header("x-custom"), None);
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_response_headers_add_header() {
    let (_backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").response_only(),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::mutate_response_headers(&[("x-ext-proc-resp", "modified")], &[])),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_header("x-ext-proc-resp", "modified");
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_response_headers_remove_header() {
    let (backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").response_only(),
        ExtProcTestServerBuilder::new().with_response(ext_proc_responses::mutate_response_headers(&[], &["x-backend"])),
    )
    .await;

    backend.set_default_response(PreConfiguredResponse::with_body("ok").header("x-backend", "value")).await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::OK);
    assert_eq!(response.header("x-backend"), None);
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_immediate_response() {
    let (_backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only(),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::immediate_response(403, "Forbidden by ext_proc")),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::FORBIDDEN);
    response.assert_body("Forbidden by ext_proc");
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_immediate_response_with_custom_headers() {
    let (_backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only(),
        ExtProcTestServerBuilder::new().with_response(ext_proc_responses::immediate_response_with_headers(
            403,
            "blocked",
            &[("x-block-reason", "policy")],
        )),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::FORBIDDEN);
    response.assert_body("blocked");
    response.assert_header("x-block-reason", "policy");
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_request_body_passthrough() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only().request_body_mode(BodySendMode::None),
        ExtProcTestServerBuilder::new().with_response(ext_proc_responses::continue_request_headers()),
    )
    .await;

    let response = client.post("/test", "original body").await.expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("original body"));
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_request_body_buffered() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only().request_body_mode(BodySendMode::Buffered),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_request_headers())
            .with_response(ext_proc_responses::continue_request_body(None)),
    )
    .await;

    let response = client.post("/test", "original body").await.expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("original body"));
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_request_body_streamed() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only().request_body_mode(BodySendMode::Streamed),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_request_headers())
            .with_response(ext_proc_responses::continue_request_body(None)),
    )
    .await;

    let response = client.post("/test", "streamed body").await.expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("streamed body"));
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_request_body_buffered_replace() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only().request_body_mode(BodySendMode::Buffered),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_request_headers())
            .with_response(ext_proc_responses::continue_request_body(Some("replaced body"))),
    )
    .await;

    let response = client.post("/test", "original body").await.expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("replaced body"));
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_request_body_streamed_replace() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only().request_body_mode(BodySendMode::Streamed),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_request_headers())
            .with_response(ext_proc_responses::continue_request_body(Some("replaced streamed"))),
    )
    .await;

    let response = client.post("/test", "original body").await.expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("replaced streamed"));
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_response_body_passthrough() {
    let (_backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").response_only().response_body_mode(BodySendMode::None),
        ExtProcTestServerBuilder::new().with_response(ext_proc_responses::continue_response_headers()),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");
}

#[tokio::test]
#[test_log::test]
#[ignore]
async fn test_ext_proc_request_body_passthrough_multi_chunk() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only().request_body_mode(BodySendMode::None),
        ExtProcTestServerBuilder::new().with_response(ext_proc_responses::continue_request_headers()),
    )
    .await;

    let response = client
        .post_multichunk(
            "/test",
            vec![
                "o".into(),
                "r".into(),
                "i".into(),
                "g".into(),
                "i".into(),
                "n".into(),
                "a".into(),
                "l".into(),
                " ".into(),
                "b".into(),
                "o".into(),
                "d".into(),
                "y".into(),
            ],
        )
        .await
        .expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("original body"));
}

#[tokio::test]
#[test_log::test]
#[ignore]
async fn test_ext_proc_request_body_buffered_multi_chunk() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only().request_body_mode(BodySendMode::Buffered),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_request_headers())
            .with_response(ext_proc_responses::continue_request_body(None)),
    )
    .await;

    let response = client
        .post_multichunk(
            "/test",
            vec![
                "o".into(),
                "r".into(),
                "i".into(),
                "g".into(),
                "i".into(),
                "n".into(),
                "a".into(),
                "l".into(),
                " ".into(),
                "b".into(),
                "o".into(),
                "d".into(),
                "y".into(),
            ],
        )
        .await
        .expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("original body"));
}

#[tokio::test]
#[test_log::test]
#[ignore]
async fn test_ext_proc_request_body_streamed_multi_chunk() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only().request_body_mode(BodySendMode::Streamed),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_request_headers())
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_request_body(None)),
    )
    .await;

    let response = client
        .post_multichunk(
            "/test",
            vec![
                "s".into(),
                "t".into(),
                "r".into(),
                "e".into(),
                "a".into(),
                "m".into(),
                "e".into(),
                "d".into(),
                " ".into(),
                "b".into(),
                "o".into(),
                "d".into(),
                "y".into(),
            ],
        )
        .await
        .expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("streamed body"));
}

#[tokio::test]
#[test_log::test]
#[ignore]
async fn test_ext_proc_request_body_buffered_replace_multi_chunk() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").request_only().request_body_mode(BodySendMode::Buffered),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_request_headers())
            .with_response(ext_proc_responses::continue_request_body(Some("replaced body"))),
    )
    .await;

    let response = client
        .post_multichunk(
            "/test",
            vec![
                "o".into(),
                "r".into(),
                "i".into(),
                "g".into(),
                "i".into(),
                "n".into(),
                "a".into(),
                "l".into(),
                " ".into(),
                "b".into(),
                "o".into(),
                "d".into(),
                "y".into(),
            ],
        )
        .await
        .expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.body_str(), Some("replaced body"));
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_response_body_buffered() {
    let (_backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").response_only().response_body_mode(BodySendMode::Buffered),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_response_headers())
            .with_response(ext_proc_responses::continue_response_body(None)),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_response_body_streamed() {
    let (_backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").response_only().response_body_mode(BodySendMode::Streamed),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_response_headers())
            .with_response(ext_proc_responses::continue_response_body(None)),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend response");
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_response_body_buffered_replace() {
    let (_backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").response_only().response_body_mode(BodySendMode::Buffered),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_response_headers())
            .with_response(ext_proc_responses::continue_response_body(Some("replaced response"))),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("replaced response");
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_response_body_streamed_replace() {
    let (_backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").response_only().response_body_mode(BodySendMode::Streamed),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_response_headers())
            .with_response(ext_proc_responses::continue_response_body(Some("replaced streamed response"))),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("replaced streamed response");
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_skip_all_headers() {
    let (mut backend, ext_proc_server, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster")
            .request_header_mode(HeaderSendMode::Skip)
            .response_header_mode(HeaderSendMode::Skip),
        ExtProcTestServerBuilder::new(),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::OK);

    let _ = backend.await_request().await.expect("backend request");

    tokio::time::sleep(Duration::from_millis(100)).await;
    let captured = ext_proc_server.captured_requests().await;
    assert!(captured.is_empty(), "ext_proc should receive nothing when all headers skipped");
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_headers_and_body_mode() {
    let (_backend, ext_proc_server, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").headers_and_body(),
        ExtProcTestServerBuilder::new()
            .with_response(ext_proc_responses::continue_request_headers())
            .with_response(ext_proc_responses::continue_request_body(None))
            .with_response(ext_proc_responses::continue_response_headers())
            .with_response(ext_proc_responses::continue_response_body(None)),
    )
    .await;

    let response = client.post("/test", "test body").await.expect("request");
    response.assert_status(StatusCode::OK);

    tokio::time::sleep(Duration::from_millis(100)).await;
    let captured = ext_proc_server.captured_requests().await;
    assert!(captured.iter().any(arion_e2e_tests::CapturedProcessingRequest::is_request_headers));
    assert!(captured.iter().any(arion_e2e_tests::CapturedProcessingRequest::is_request_body));
    assert!(captured.iter().any(arion_e2e_tests::CapturedProcessingRequest::is_response_headers));
    assert!(captured.iter().any(arion_e2e_tests::CapturedProcessingRequest::is_response_body));
}

#[tokio::test]
#[test_log::test]
#[ignore]
async fn test_ext_proc_failure_mode_allow() {
    let unused_addr = "127.0.0.1:1".parse().unwrap();

    let backend = TestBackend::start().await.expect("backend start");
    backend.set_default_response(PreConfiguredResponse::with_body("backend ok")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new()
                        .http1()
                        .ext_proc(ExtProcBuilder::new("ext-proc-cluster").headers_only().failure_mode_allow(true))
                        .route_config(
                            RouteConfigBuilder::new("routes").virtual_host(
                                VirtualHostBuilder::new("default")
                                    .route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                            ),
                        ),
                ),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()))
        .cluster(presets::ext_proc_cluster("ext-proc-cluster", unused_addr));

    let config_path = bootstrap.build_to_temp().expect("build config");
    let arion =
        ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn arion");
    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.expect("request");
    response.assert_status(StatusCode::OK);
    response.assert_body("backend ok");
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_failure_mode_deny() {
    let unused_addr = "127.0.0.1:1".parse().unwrap();

    let backend = TestBackend::start().await.expect("backend start");
    backend.set_default_response(PreConfiguredResponse::with_body("backend ok")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new()
                        .http1()
                        .ext_proc(ExtProcBuilder::new("ext-proc-cluster").headers_only().failure_mode_allow(false))
                        .route_config(
                            RouteConfigBuilder::new("routes").virtual_host(
                                VirtualHostBuilder::new("default")
                                    .route(RouteBuilder::new().match_prefix("/").cluster("backend")),
                            ),
                        ),
                ),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()))
        .cluster(presets::ext_proc_cluster("ext-proc-cluster", unused_addr));

    let config_path = bootstrap.build_to_temp().expect("build config");
    let arion =
        ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn arion");
    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.expect("request");
    assert!(response.status.is_server_error(), "expected server error, got {}", response.status);
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_message_timeout() {
    let (mut _backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").headers_only().message_timeout(Duration::from_millis(100)),
        ExtProcTestServerBuilder::new().with_delay(Duration::from_secs(5)),
    )
    .await;

    let response = client.get("/test").await.expect("request");
    assert!(response.status.is_server_error(), "expected timeout error, got {}", response.status);
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_multiple_sequential_requests() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").headers_only(),
        ExtProcTestServerBuilder::new().with_responses((0..10).flat_map(|_| {
            vec![ext_proc_responses::continue_request_headers(), ext_proc_responses::continue_response_headers()]
        })),
    )
    .await;

    for i in 0..10 {
        let response = client.get(&format!("/test/{i}")).await.expect("request");
        response.assert_status(StatusCode::OK);

        let captured = backend.await_request().await.expect("backend request");
        assert_eq!(captured.path(), format!("/test/{i}"));
    }
}

#[tokio::test]
#[test_log::test]
#[ignore]
async fn test_ext_proc_observability_mode() {
    let (mut backend, _ext_proc, _arion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").headers_only().observability_mode(true),
        ExtProcTestServerBuilder::new(),
    )
    .await;

    let response = client.send(RequestBuilder::get("/test").header("x-original", "keep")).await.expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.header("x-original"), Some("keep"));
}

fn ext_proc_per_route(per_route: Override) -> impl FnOnce(&mut Route) {
    move |route| {
        let any = Any {
            type_url: "type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute".into(),
            value: ExtProcPerRoute { r#override: Some(per_route) }.encode_to_vec(),
        };
        route.typed_per_filter_config.insert("envoy.filters.http.ext_proc".into(), any);
    }
}

fn ext_proc_overrides(cluster_name: &str, failure_mode_allow: bool) -> Override {
    let processor = ExtProcBuilder::new(cluster_name).request_only().build();
    Override::Overrides(ExtProcOverrides {
        grpc_service: processor.grpc_service,
        processing_mode: processor.processing_mode,
        failure_mode_allow: Some(BoolValue { value: failure_mode_allow }),
        ..Default::default()
    })
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_per_route_disabled_and_overrides() {
    let mut backend = TestBackend::start().await.expect("backend start");
    backend.set_default_response(PreConfiguredResponse::with_body("backend ok")).await;
    let base_ext_proc = ExtProcTestServerBuilder::new()
        .with_response(ext_proc_responses::immediate_response(403, "base ext_proc"))
        .start()
        .await
        .expect("ext_proc start");
    let route_ext_proc = ExtProcTestServerBuilder::new()
        .with_response(ext_proc_responses::mutate_request_headers(&[("x-ext-proc", "route")], &[]))
        .start()
        .await
        .expect("ext_proc start");

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new()
                        .http1()
                        .ext_proc(ExtProcBuilder::new("ext-proc-base").request_only())
                        .route_config(
                            RouteConfigBuilder::new("routes").virtual_host(
                                VirtualHostBuilder::new("default")
                                    .route(
                                        RouteBuilder::new().match_prefix("/pool").cluster("backend").with_proto(
                                            ext_proc_per_route(ext_proc_overrides("ext-proc-route", false)),
                                        ),
                                    )
                                    .route(
                                        RouteBuilder::new()
                                            .match_prefix("/fail-open")
                                            .cluster("backend")
                                            .with_proto(ext_proc_per_route(ext_proc_overrides("ext-proc-down", true))),
                                    )
                                    .route(
                                        RouteBuilder::new()
                                            .match_prefix("/")
                                            .cluster("backend")
                                            .with_proto(ext_proc_per_route(Override::Disabled(true))),
                                    ),
                            ),
                        ),
                ),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()))
        .cluster(presets::ext_proc_cluster("ext-proc-base", base_ext_proc.addr()))
        .cluster(presets::ext_proc_cluster("ext-proc-route", route_ext_proc.addr()))
        .cluster(presets::ext_proc_cluster("ext-proc-down", "127.0.0.1:1".parse().unwrap()));

    let config_path = bootstrap.build_to_temp().expect("build config");
    let arion =
        ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn arion");
    let client = TestClient::new(arion.listener_addr().unwrap());

    client.get("/plain").await.expect("request").assert_status(StatusCode::OK);
    assert_eq!(backend.await_request().await.expect("backend request").header("x-ext-proc"), None);

    client.get("/pool").await.expect("request").assert_status(StatusCode::OK);
    assert_eq!(backend.await_request().await.expect("backend request").header("x-ext-proc"), Some("route"));

    client.get("/fail-open").await.expect("request").assert_status(StatusCode::OK);

    assert!(base_ext_proc.captured_requests().await.is_empty());
}

const DESTINATION_HEADER: &str = "x-gateway-destination-endpoint";

/// The served endpoint reported on each response headers message, in arrival order.
async fn served_endpoints(ext_proc: &ExtProcTestServer) -> Vec<Option<String>> {
    let served = |request: &CapturedProcessingRequest| {
        let lb = request.metadata_context()?.filter_metadata.get("envoy.lb")?;
        match &lb.fields.get("x-gateway-destination-endpoint-served")?.kind {
            Some(Kind::StringValue(endpoint)) => Some(endpoint.clone()),
            _ => None,
        }
    };
    ext_proc.captured_requests().await.iter().filter(|request| request.is_response_headers()).map(served).collect()
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_served_endpoint_metadata() {
    let b1 = TestBackend::start().await.expect("backend start");
    let b2 = TestBackend::start().await.expect("backend start");
    b1.set_default_response(PreConfiguredResponse::with_body("b1")).await;
    b2.set_default_response(PreConfiguredResponse::with_body("b2")).await;
    let closed = std::net::TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).expect("free port");
    let (b1_addr, b2_addr) = (b1.addr().to_string(), b2.addr().to_string());
    let override_host = |name: &str, endpoints: [SocketAddr; 2]| {
        ClusterBuilder::new(name)
            .override_host(DESTINATION_HEADER, LbPolicy::RoundRobin)
            .endpoints(endpoints.map(EndpointBuilder::from_socket_addr))
    };

    for forward in [true, false] {
        let ext_proc_server = ExtProcTestServerBuilder::new()
            .with_responses(std::iter::repeat_with(ext_proc_responses::continue_response_headers).take(8))
            .start()
            .await
            .expect("ext_proc start");
        let mut ext_proc = ExtProcBuilder::new("ext-proc-cluster").response_only();
        if forward {
            ext_proc = ext_proc.forward_metadata_namespaces(&["envoy.lb"]);
        }
        let bootstrap = BootstrapBuilder::new()
            .listener(
                ListenerBuilder::new("http").port(0).filter_chain(
                    FilterChainBuilder::new("main").hcm(
                        HcmBuilder::new().http1().ext_proc(ext_proc).route_config(
                            RouteConfigBuilder::new("routes").virtual_host(
                                VirtualHostBuilder::new("default")
                                    .route(RouteBuilder::new().match_prefix("/pool").cluster("pool"))
                                    .route(RouteBuilder::new().match_prefix("/failover").cluster("failover"))
                                    .route(RouteBuilder::new().match_prefix("/").cluster("plain")),
                            ),
                        ),
                    ),
                ),
            )
            .cluster(override_host("pool", [b1.addr(), b2.addr()]))
            .cluster(override_host("failover", [closed, b2.addr()]))
            .cluster(ClusterBuilder::with_endpoint("plain", b1.addr()))
            .cluster(presets::ext_proc_cluster("ext-proc-cluster", ext_proc_server.addr()));
        let config_path = bootstrap.build_to_temp().expect("build config");
        let arion =
            ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn arion");
        let client = TestClient::new(arion.listener_addr().expect("listener address"));

        // Picked, fallback (no or unknown pick) and failover answers name the endpoint that answered;
        // the plain cluster reports nothing, and neither does a filter that does not forward envoy.lb.
        let mut expected = vec![];
        for (path, destination, answering, reported) in [
            ("/pool", Some(b2_addr.clone()), Some(&b2_addr), true),
            ("/pool", Some(b1_addr.clone()), Some(&b1_addr), true),
            ("/pool", None, None, true),
            ("/pool", Some("127.0.0.1:1".to_owned()), None, true),
            ("/failover", Some(format!("{closed},{b2_addr}")), Some(&b2_addr), true),
            ("/plain", None, Some(&b1_addr), false),
        ] {
            let request = destination.iter().fold(RequestBuilder::get(path), |request, destination| {
                request.header(DESTINATION_HEADER, destination)
            });
            let response = client.send(request).await.expect("request");
            response.assert_status(StatusCode::OK);
            let answered = match response.body_str() {
                Some("b1") => &b1_addr,
                Some("b2") => &b2_addr,
                other => panic!("unexpected body {other:?}"),
            };
            if let Some(answering) = answering {
                assert_eq!(answered, answering, "{path} {destination:?}");
            }
            expected.push((forward && reported).then(|| answered.clone()));
        }
        assert_eq!(served_endpoints(&ext_proc_server).await, expected, "forward: {forward}");
        arion.shutdown();
    }
}
