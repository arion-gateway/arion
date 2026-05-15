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

use std::time::Duration;

use http::StatusCode;
use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, FilterChainBuilder, HcmBuilder, ListenerBuilder, LocalRateLimitBuilder,
    RouteBuilder, RouteConfigBuilder, UserRateLimiterBuilder, VirtualHostBuilder,
};
use orion_e2e_tests::{OrionInstance, PreConfiguredResponse, RequestBuilder, SpawnOptions, TestBackend, TestClient};

fn simple_route_config() -> RouteConfigBuilder {
    RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    )
}

#[tokio::test]
#[ignore]
async fn test_listener_local_rate_limit_enforced() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http")
                .port(0)
                .listener_local_rate_limit("listener_rate_limit", 2, 1, 10)
                .filter_chain(
                    FilterChainBuilder::new("main").hcm(HcmBuilder::new().http1().route_config(simple_route_config())),
                ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let addr = orion.listener_addr().unwrap();

    let client1 = TestClient::new(addr);
    let response = client1.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let client2 = TestClient::new(addr);
    let response = client2.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let client3 = TestClient::new(addr);
    let result = client3.get("/test").await;
    assert!(result.is_err(), "Expected network error when listener rate limit exceeded (3rd connection)");

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_listener_local_rate_limit_refill() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http")
                .port(0)
                .listener_local_rate_limit("listener_rate_limit", 1, 1, 2)
                .filter_chain(
                    FilterChainBuilder::new("main").hcm(HcmBuilder::new().http1().route_config(simple_route_config())),
                ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let addr = orion.listener_addr().unwrap();

    // Instantiating multiple clients otherwise a single client uses connection pooling
    // and does not reopen connection between requests
    let client1 = TestClient::new(addr);
    let response = client1.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let client2 = TestClient::new(addr);
    let result = client2.get("/test").await;
    assert!(result.is_err(), "Expected network error when listener rate limit exceeded");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let client3 = TestClient::new(addr);
    let response = client3.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_hcm_local_rate_limit_enforced() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(3, 1, 10);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    for _ in 0..3 {
        let response = client.get("/test").await.unwrap();
        response.assert_status(StatusCode::OK);
        response.assert_body("OK");
    }

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_hcm_local_rate_limit_custom_status_code() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(503).token_bucket(1, 1, 10);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::SERVICE_UNAVAILABLE);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_hcm_local_rate_limit_refill() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(1, 1, 2);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    tokio::time::sleep(Duration::from_secs(3)).await;

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_per_route_local_rate_limit_override() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let hcm_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(10, 1, 10);

    let route_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("route_rate_limit").status_code(429).token_bucket(2, 1, 10);

    let routes = RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default")
            .route(
                RouteBuilder::new()
                    .match_prefix("/limited")
                    .cluster("backend")
                    .local_rate_limit_override(route_rate_limit),
            )
            .route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().local_rate_limit(hcm_rate_limit).route_config(routes)),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/limited/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let response = client.get("/limited/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let response = client.get("/limited/test").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    for _ in 0..10 {
        let response = client.get("/other").await.unwrap();
        response.assert_status(StatusCode::OK);
    }

    let response = client.get("/other").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_user_rate_limiter_missing_header() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let user_rate_limiter = UserRateLimiterBuilder::new()
        .stat_prefix("user_rate_limiter")
        .user_id_header("x-user-id")
        .status_code(429)
        .add_user_limit_local(Some("alice"), 2, 1, 10);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().user_rate_limit(user_rate_limiter).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_user_rate_limiter_simple_rate_limit() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let user_rate_limiter = UserRateLimiterBuilder::new()
        .stat_prefix("user_rate_limiter")
        .user_id_header("x-user-id")
        .status_code(429)
        .add_user_limit_simple(Some("alice"), 2, 1);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().user_rate_limit(user_rate_limiter).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    for _ in 0..2 {
        let response = client.send(RequestBuilder::get("/test").header("x-user-id", "alice")).await.unwrap();
        response.assert_status(StatusCode::OK);
    }

    let response = client.send(RequestBuilder::get("/test").header("x-user-id", "alice")).await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_rate_limit_applies_before_routing() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(1, 1, 10);

    let routes = RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default")
            .route(RouteBuilder::new().match_prefix("/nonexistent").direct_response_empty(404))
            .route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(routes)),
            ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();
    let client = TestClient::new(orion.listener_addr().unwrap());

    let response = client.get("/test").await.unwrap();
    response.assert_status(StatusCode::OK);

    let response = client.get("/nonexistent").await.unwrap();
    response.assert_status(StatusCode::TOO_MANY_REQUESTS);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_hcm_local_rate_limit_statistical_multi_runtime() {
    use orion_e2e_tests::PortBlock;

    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let port_block = PortBlock::reserve().unwrap();
    let port = port_block.allocate().unwrap();

    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(50, 10, 1);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(port).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let spawn_options = SpawnOptions::default().with_num_runtimes(4).with_num_cpus(4);
    let orion = OrionInstance::spawn_with_fixed_port(&config_path, "http", port, spawn_options).await.unwrap();
    let addr = orion.listener_addr().unwrap();

    let num_concurrent_clients = 50;
    let requests_per_client = 4;
    let total_requests = num_concurrent_clients * requests_per_client;

    let mut tasks = vec![];
    for _ in 0..num_concurrent_clients {
        let task = tokio::spawn(async move {
            let client = TestClient::new(addr);
            let mut successful = 0;
            let mut rate_limited = 0;

            for _ in 0..requests_per_client {
                if let Ok(response) = client.get("/test").await {
                    if response.status == StatusCode::OK {
                        successful += 1;
                    } else if response.status == StatusCode::TOO_MANY_REQUESTS {
                        rate_limited += 1;
                    }
                }
            }
            (successful, rate_limited)
        });
        tasks.push(task);
    }

    let mut total_successful = 0;
    let mut total_rate_limited = 0;
    for task in tasks {
        let (successful, rate_limited) = task.await.unwrap();
        total_successful += successful;
        total_rate_limited += rate_limited;
    }

    let expected_allowed = 50;
    let tolerance = 0.3;
    let min_allowed = (f64::from(expected_allowed) * (1.0 - tolerance)) as usize;
    let max_allowed = (f64::from(expected_allowed) * (1.0 + tolerance)) as usize;

    assert!(
        total_successful >= min_allowed && total_successful <= max_allowed,
        "Expected approximately {} successful requests (±{}%), got {} successful and {} rate limited out of {} total",
        expected_allowed,
        (tolerance * 100.0) as usize,
        total_successful,
        total_rate_limited,
        total_requests
    );

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_listener_local_rate_limit_statistical_multi_runtime() {
    use orion_e2e_tests::PortBlock;

    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let port_block = PortBlock::reserve().unwrap();
    let port = port_block.allocate().unwrap();

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http")
                .port(port)
                .listener_local_rate_limit("listener_rate_limit", 30, 10, 1)
                .filter_chain(
                    FilterChainBuilder::new("main").hcm(HcmBuilder::new().http1().route_config(simple_route_config())),
                ),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let spawn_options = SpawnOptions::default().with_num_runtimes(4).with_num_cpus(4);
    let orion = OrionInstance::spawn_with_fixed_port(&config_path, "http", port, spawn_options).await.unwrap();
    let addr = orion.listener_addr().unwrap();

    let num_concurrent_attempts = 100;
    let mut tasks = vec![];

    for _ in 0..num_concurrent_attempts {
        let addr = addr;
        let task = tokio::spawn(async move {
            let client = TestClient::new(addr);
            match client.get("/test").await {
                Ok(response) => {
                    if response.status == StatusCode::OK {
                        Ok(())
                    } else {
                        Err(())
                    }
                },
                Err(_) => Err(()),
            }
        });
        tasks.push(task);
    }

    let mut successful = 0;
    let mut failed = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(()) => successful += 1,
            Err(()) => failed += 1,
        }
    }

    let expected_allowed = 80;
    let tolerance = 0.5;
    let min_allowed = (f64::from(expected_allowed) * (1.0 - tolerance)) as usize;
    let max_allowed = (f64::from(expected_allowed) * (1.0 + tolerance)) as usize;

    assert!(
        successful >= min_allowed && successful <= max_allowed,
        "Expected approximately {} successful connections (±{}%) with 4 runtimes × 30 tokens/runtime ≈ 120 total capacity, got {} successful and {} failed out of {} total",
        expected_allowed,
        (tolerance * 100.0) as usize,
        successful,
        failed,
        num_concurrent_attempts
    );

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_hcm_rate_limit_aggregate_over_time() {
    use orion_e2e_tests::PortBlock;

    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let port_block = PortBlock::reserve().unwrap();
    let port = port_block.allocate().unwrap();

    let local_rate_limit =
        LocalRateLimitBuilder::new().stat_prefix("hcm_rate_limit").status_code(429).token_bucket(10, 5, 1);

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("http").port(port).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new().http1().local_rate_limit(local_rate_limit).route_config(simple_route_config()),
                ),
            ))
            .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().unwrap();
    let spawn_options = SpawnOptions::default().with_num_runtimes(4).with_num_cpus(4);
    let orion = OrionInstance::spawn_with_fixed_port(&config_path, "http", port, spawn_options).await.unwrap();
    let addr = orion.listener_addr().unwrap();

    let num_burst_clients = 200;
    let requests_per_client = 1;
    let mut tasks = vec![];

    for _ in 0..num_burst_clients {
        let addr = addr;
        let task = tokio::spawn(async move {
            let client = TestClient::new(addr);
            let mut successful = 0;
            for _ in 0..requests_per_client {
                if let Ok(response) = client.get("/test").await {
                    if response.status == StatusCode::OK {
                        successful += 1;
                    }
                }
            }
            successful
        });
        tasks.push(task);
    }

    let mut burst_successful = 0;
    for task in tasks {
        burst_successful += task.await.unwrap();
    }

    let expected_burst = 10;
    let burst_tolerance = 0.50;
    let min_burst = (f64::from(expected_burst) * (1.0 - burst_tolerance)) as usize;
    let max_burst = (f64::from(expected_burst) * (1.0 + burst_tolerance)) as usize;

    assert!(
        burst_successful >= min_burst && burst_successful <= max_burst,
        "Burst phase: expected ~{} successful (±{}%), got {}",
        expected_burst,
        (burst_tolerance * 100.0) as usize,
        burst_successful
    );

    tokio::time::sleep(Duration::from_secs(5)).await;

    let mut tasks = vec![];
    for _ in 0..200 {
        let addr = addr;
        let task = tokio::spawn(async move {
            let client = TestClient::new(addr);
            let mut successful = 0;
            for _ in 0..1 {
                if let Ok(response) = client.get("/test").await {
                    if response.status == StatusCode::OK {
                        successful += 1;
                    }
                }
            }
            successful
        });
        tasks.push(task);
    }

    let mut refill_successful = 0;
    for task in tasks {
        refill_successful += task.await.unwrap();
    }

    let expected_refill = 10;
    let refill_tolerance = 0.50;
    let min_refill = (f64::from(expected_refill) * (1.0 - refill_tolerance)) as usize;
    let max_refill = (f64::from(expected_refill) * (1.0 + refill_tolerance)) as usize;

    assert!(
        refill_successful >= min_refill && refill_successful <= max_refill,
        "Refill phase: after 5s (>4s fill interval with 4 runtimes), buckets refill to max capacity, expected ~{} successful (±{}%), got {}",
        expected_refill,
        (refill_tolerance * 100.0) as usize,
        refill_successful
    );

    orion.shutdown();
}
