use http::StatusCode;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, LocalRateLimitBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient};

#[tokio::test]

async fn test_route_match_collision_bug() {
    let mut backend = TestBackend::start().await.expect("backend start");
    backend.set_default_response(PreConfiguredResponse::with_body("backend response")).await;

    // Base HCM local rate limit (does not actually limit anything here, we rely on overrides)
    let hcm_rate_limit = LocalRateLimitBuilder::new().stat_prefix("base_limit");

    // Restrictive override: 1 token max.
    let restrictive_override = LocalRateLimitBuilder::new().stat_prefix("restrictive").token_bucket(1, 1, 10);

    // Permissive override: 1000 tokens max.
    let permissive_override = LocalRateLimitBuilder::new().stat_prefix("permissive").token_bucket(1000, 1000, 10);

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(
            HcmBuilder::new()
                .http1()
                .local_rate_limit(hcm_rate_limit)
                .route_config(
                    RouteConfigBuilder::new("routes")
                        .virtual_host(
                            VirtualHostBuilder::new("vhost-1")
                                .domains(["api.example.com"])
                                .route(RouteBuilder::new().match_prefix("/").cluster("backend").local_rate_limit_override(restrictive_override)),
                        )
                        .virtual_host(
                            VirtualHostBuilder::new("vhost-2")
                                .domains(["admin.example.com"])
                                .route(RouteBuilder::new().match_prefix("/").cluster("backend").local_rate_limit_override(permissive_override)),
                        ),
                ),
        )))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("build config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn orion");
    let client = TestClient::new(orion.listener_addr().unwrap());

    // We send traffic ONLY to VirtualHost 2 (admin.example.com), which has a PERMISSIVE limit (1000 requests).
    // The first request will consume 1 token (or 1 from restrictive and 1 from permissive due to the bug).
    let response1 = client
        .send(RequestBuilder::get("/test").header("host", "admin.example.com"))
        .await
        .expect("request 1");
    response1.assert_status(StatusCode::OK);

    // The second request to VirtualHost 2.
    // If the proxy is correct, it has 999 tokens left, so it should return 200 OK.
    // If the bug exists, the restrictive filter from VirtualHost 1 is ALSO in the chain for VirtualHost 2!
    // The restrictive filter has 0 tokens left, so it will block this request with 429 Too Many Requests!
    let response2 = client
        .send(RequestBuilder::get("/test").header("host", "admin.example.com"))
        .await
        .expect("request 2");

    assert_eq!(
        response2.status,
        StatusCode::OK,
        "BUG DETECTED: The restrictive override from VirtualHost 1 leaked into VirtualHost 2! Status: {}",
        response2.status
    );
}
