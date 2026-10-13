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

use std::time::Duration;

use arion_e2e_tests::config_builder::{ClusterBuilder, RetryPolicyBuilder, RouteBuilder, VirtualHostBuilder, presets};
use arion_e2e_tests::{
    ArionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestClient, cleanup_config_file,
};
use http::StatusCode;

async fn requests_seen(route_policy: Option<RetryPolicyBuilder>, vhost_policy: Option<RetryPolicyBuilder>) -> usize {
    let mut backend = TestBackend::start().await.unwrap();
    backend
        .set_default_response(
            PreConfiguredResponse::with_status(StatusCode::SERVICE_UNAVAILABLE).delay(Duration::from_millis(50)),
        )
        .await;

    let cluster = ClusterBuilder::with_endpoint("backend", backend.addr()).circuit_breaker_max_retries(3).build();

    let mut route = RouteBuilder::new().match_prefix("/").cluster("backend");
    if let Some(policy) = route_policy {
        route = route.retry_policy(policy);
    }
    let mut vhost = VirtualHostBuilder::new("default");
    if let Some(policy) = vhost_policy {
        vhost = vhost.retry_policy(policy);
    }
    let vhost = vhost.route(route);

    let bootstrap = presets::routed_proxy_with_vhost(vhost, [cluster]);
    let config_path = bootstrap.build_to_temp().unwrap();
    let arion = ArionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(arion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::SERVICE_UNAVAILABLE);

    let mut count = 0;
    while backend.await_request_with_timeout(Duration::from_millis(500)).await.is_ok() {
        count += 1;
    }

    arion.shutdown();
    cleanup_config_file(&config_path);
    count
}

#[tokio::test]
#[ignore]
async fn test_route_level_retry_policy_is_honored() {
    let count = requests_seen(Some(RetryPolicyBuilder::new().on_5xx().num_retries(2)), None).await;
    assert!(count > 1, "expected retries from a route-level policy, backend saw {count} request(s)");
}

#[tokio::test]
#[ignore]
async fn test_route_level_retry_policy_replaces_vhost_policy() {
    let count = requests_seen(
        Some(RetryPolicyBuilder::new().on_connect_failure().num_retries(2)),
        Some(RetryPolicyBuilder::new().on_5xx().num_retries(2)),
    )
    .await;
    assert_eq!(count, 1, "route-level policy must replace the vhost policy");
}

#[tokio::test]
#[ignore]
async fn test_vhost_retry_policy_applies_without_route_policy() {
    let count = requests_seen(None, Some(RetryPolicyBuilder::new().on_5xx().num_retries(2))).await;
    assert!(count > 1, "expected retries from the vhost policy, backend saw {count} request(s)");
}
