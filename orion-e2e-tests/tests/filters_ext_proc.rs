#![allow(clippy::expect_used, reason = "test infrastructure — panicking on setup failure is intentional")]

use std::path::PathBuf;
use std::time::Duration;

use http::StatusCode;
use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::ext_proc::v3::processing_mode::{
    BodySendMode, HeaderSendMode,
};
use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, ExtProcBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{
    ext_proc_responses, ExtProcTestServer, ExtProcTestServerBuilder, OrionInstance, PreConfiguredResponse,
    RequestBuilder, SpawnOptions, TestBackend, TestClient,
};

async fn setup(
    ext_proc_builder: ExtProcBuilder,
    ext_proc_server_builder: ExtProcTestServerBuilder,
) -> (TestBackend, ExtProcTestServer, OrionInstance, TestClient, PathBuf) {
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
    let orion =
        OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn orion");
    #[allow(clippy::unwrap_used)]
    let client = TestClient::new(orion.listener_addr().unwrap());

    (backend, ext_proc_server, orion, client, config_path)
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_request_headers_add_header() {
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (_backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (_backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (_backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (_backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (_backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (_backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (_backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (_backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, ext_proc_server, _orion, client, _cfg) = setup(
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
    let (_backend, ext_proc_server, _orion, client, _cfg) = setup(
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
    assert!(captured.iter().any(orion_e2e_tests::CapturedProcessingRequest::is_request_headers));
    assert!(captured.iter().any(orion_e2e_tests::CapturedProcessingRequest::is_request_body));
    assert!(captured.iter().any(orion_e2e_tests::CapturedProcessingRequest::is_response_headers));
    assert!(captured.iter().any(orion_e2e_tests::CapturedProcessingRequest::is_response_body));
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
    let orion =
        OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn orion");
    let client = TestClient::new(orion.listener_addr().unwrap());

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
    let orion =
        OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn orion");
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.expect("request");
    assert!(response.status.is_server_error(), "expected server error, got {}", response.status);
}

#[tokio::test]
#[ignore]
async fn test_ext_proc_message_timeout() {
    let (mut _backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
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
    let (mut backend, _ext_proc, _orion, client, _cfg) = setup(
        ExtProcBuilder::new("ext-proc-cluster").headers_only().observability_mode(true),
        ExtProcTestServerBuilder::new(),
    )
    .await;

    let response = client.send(RequestBuilder::get("/test").header("x-original", "keep")).await.expect("request");
    response.assert_status(StatusCode::OK);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.header("x-original"), Some("keep"));
}
