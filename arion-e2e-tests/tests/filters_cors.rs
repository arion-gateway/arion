// Copyright 2026 The arion-gateway Authors
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

use arion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::cors::v3::CorsPolicy;
use arion_data_plane_api::envoy_data_plane_api::envoy::r#type::matcher::v3::{
    StringMatcher, string_matcher::MatchPattern,
};
use arion_data_plane_api::envoy_data_plane_api::{
    envoy::extensions::filters::network::http_connection_manager::v3::HttpFilter,
    envoy::extensions::filters::network::http_connection_manager::v3::http_filter::ConfigType as HttpFilterConfigType,
    google::protobuf::Any, prost::Message,
};
use arion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, RouteBuilder,
    RouteConfigBuilder, VirtualHostBuilder,
};
use arion_e2e_tests::{
    ArionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient, cleanup_config_file,
};
use http::StatusCode;

fn exact_origin(origin: &str) -> StringMatcher {
    StringMatcher { match_pattern: Some(MatchPattern::Exact(origin.into())), ..Default::default() }
}

fn cors_policy(origin: &str) -> CorsPolicy {
    CorsPolicy {
        allow_origin_string_match: vec![exact_origin(origin)],
        allow_methods: "GET,POST,OPTIONS".into(),
        allow_headers: "x-test-header".into(),
        max_age: "3600".into(),
        ..Default::default()
    }
}

fn cors_hcm_filter(policy: &CorsPolicy) -> HttpFilter {
    let any = Any {
        type_url: "type.googleapis.com/envoy.extensions.filters.http.cors.v3.CorsPolicy".into(),
        value: policy.encode_to_vec(),
    };
    HttpFilter {
        name: "envoy.filters.http.cors".into(),
        config_type: Some(HttpFilterConfigType::TypedConfig(any)),
        ..Default::default()
    }
}

fn cors_route_override(route: RouteBuilder, policy: &CorsPolicy) -> RouteBuilder {
    let any = Any {
        type_url: "type.googleapis.com/envoy.extensions.filters.http.cors.v3.CorsPolicy".into(),
        value: policy.encode_to_vec(),
    };
    route.with_proto(move |proto| {
        proto.typed_per_filter_config.insert("envoy.filters.http.cors".into(), any);
    })
}

async fn spawn_cors_proxy(routes: Vec<RouteBuilder>, hcm_policy: &CorsPolicy) -> (TestBackend, ArionInstance) {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("backend ok")).await;

    let mut vhost = VirtualHostBuilder::new("default");
    for route in routes {
        vhost = vhost.route(route);
    }

    let hcm = HcmBuilder::new().http1().route_config(RouteConfigBuilder::new("routes").virtual_host(vhost)).with_proto(
        move |proto| {
            proto.http_filters.insert(0, cors_hcm_filter(hcm_policy));
        },
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).filter_chain(FilterChainBuilder::new("main").hcm(hcm)))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    cleanup_config_file(&config_path);
    (backend, arion)
}

#[tokio::test]
#[ignore]
async fn test_cors_hcm_filter_preflight_and_response() {
    let (_backend, arion) = spawn_cors_proxy(
        vec![RouteBuilder::new().match_prefix("/").cluster("backend")],
        &cors_policy("https://allowed.example"),
    )
    .await;
    let client = TestClient::new(arion.listener_addr().unwrap());

    let preflight = client
        .send(
            RequestBuilder::options("/api")
                .header("origin", "https://allowed.example")
                .header("access-control-request-method", "POST"),
        )
        .await
        .unwrap();
    preflight.assert_status(StatusCode::NO_CONTENT);
    preflight.assert_header("access-control-allow-origin", "https://allowed.example");
    preflight.assert_header("access-control-max-age", "3600");

    let simple = client.send(RequestBuilder::get("/api").header("origin", "https://allowed.example")).await.unwrap();
    simple.assert_status(StatusCode::OK);
    simple.assert_header("access-control-allow-origin", "https://allowed.example");

    let denied = client
        .send(
            RequestBuilder::options("/api")
                .header("origin", "https://evil.example")
                .header("access-control-request-method", "POST"),
        )
        .await
        .unwrap();
    // A disallowed origin gets no preflight grant; the request continues upstream.
    denied.assert_status(StatusCode::OK);
    assert!(denied.header("access-control-allow-origin").is_none());

    arion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_cors_per_route_policy_override() {
    let routes = vec![
        cors_route_override(
            RouteBuilder::new().match_prefix("/override").cluster("backend"),
            &cors_policy("https://route.example"),
        ),
        RouteBuilder::new().match_prefix("/").cluster("backend"),
    ];
    let (_backend, arion) = spawn_cors_proxy(routes, &cors_policy("https://hcm.example")).await;
    let client = TestClient::new(arion.listener_addr().unwrap());

    let preflight = client
        .send(
            RequestBuilder::options("/override")
                .header("origin", "https://route.example")
                .header("access-control-request-method", "GET"),
        )
        .await
        .unwrap();
    preflight.assert_status(StatusCode::NO_CONTENT);
    preflight.assert_header("access-control-allow-origin", "https://route.example");

    // The route override replaces the HCM policy entirely.
    let hcm_origin_on_override_route = client
        .send(
            RequestBuilder::options("/override")
                .header("origin", "https://hcm.example")
                .header("access-control-request-method", "GET"),
        )
        .await
        .unwrap();
    hcm_origin_on_override_route.assert_status(StatusCode::OK);
    assert!(hcm_origin_on_override_route.header("access-control-allow-origin").is_none());

    let other_route = client
        .send(
            RequestBuilder::options("/plain")
                .header("origin", "https://hcm.example")
                .header("access-control-request-method", "GET"),
        )
        .await
        .unwrap();
    other_route.assert_status(StatusCode::NO_CONTENT);
    other_route.assert_header("access-control-allow-origin", "https://hcm.example");

    arion.shutdown();
}
