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

use futures::future::join_all;
use http::StatusCode;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use orion_data_plane_api::envoy_data_plane_api::envoy::{
    config::{
        core::v3::{data_source::Specifier, DataSource},
        route::v3::{route_match::PathSpecifier, RouteMatch},
    },
    extensions::filters::http::jwt_authn::v3::{
        jwt_provider::JwksSourceSpecifier, jwt_requirement::RequiresType, requirement_rule::RequirementType,
        JwtAuthentication, JwtHeader, JwtProvider, JwtRequirement, RequirementRule,
    },
};
use orion_e2e_tests::{
    config_builder::{
        BootstrapBuilder, CedarPolicyBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, HcmBuilder,
        ListenerBuilder, RouteBuilder, RouteConfigBuilder, VirtualHostBuilder,
    },
    OrionInstance, PreConfiguredResponse, SpawnOptions, TestBackend, TestClient,
};
use serde::Serialize;

// RSA-2048 test key pair (generated for tests only, not used outside this file).
// Public JWK is embedded in the JWT filter config below.
const TEST_RSA_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDnXLveqqWR5t4S
75CgNLtwxA3/gDYEMpekKO1y9SJY2SHZ/ipMeHTT5tT4kElHtEd9FRLLEM+OnYN8
u2L+uaKHVwBkBPwYxhlZEu1s6IVXS1eWWrd7izb4DeFw4TCjoHMvRmUDN61pjPFk
416XH1BUtO1vCF69p6MNGSN/ql/CSNHSC1m6Itfa6Kgl8Ya+PHZOR4dso6WFTZoX
mNma+KmjkuncdVXoKV6ZSbiJSRbZFZRCg3RC545KAHXqgdNKnggtK474ULYx4086
t+KBSNxsvzuuVs5glHA02Rm9tdt9XE8ytUjyTdjImroYxjgRshSIpWUDwRWT1OEC
kzzkJyNbAgMBAAECggEAYuJZL52a32Wfs9MtarOvC07YNsQrEhc3hcOyXQhVkkjX
dY7ysDVppWnKy6QLlfiA936KxjzcoTVETgrfEETyKMswEQ+qWcJNcisrS/mDiCBd
ApeqRUTmjRWNrcupmL3KjUGWic4BsZO6VqbxNkD9ef7MXkDjEUc1YrNEX1vHysa5
srTW9TSrmYJPF9gwR4sYreZwt3ICeNInak5av13hVH3GxNTJ7wvyWq9jSodUpSZK
lJCWqLPsS0fDp4SpMrhd/WL3ZsNUX85nMYk19lgbauzuHARt+SC4DBuHh+JOOR2F
mArCjKgAb6RHcoaVzdiU//iwpOd1dLQPToHhNTOulQKBgQD72WFzI06edE5kI+Lh
4FPbg0PAMt8AghimXc6M0htQN2Yc8Z7GjiWPwJX8fOu6RdXOKTXKeM6zvs9Q+y+0
bszhz1vxvu72LNZ2B6yaNAZTt8v4U8hFg4ZaaXbFT1XrBDeZYjkYRXw1kn21Y1Sy
KNJo1Tx2vn7QDUDdhfvssoP0LQKBgQDrLOnX39C4lV2GKZLSZuQ6un+r2E/1KYzY
AsL0tw4oGSWZzK1vRz5bKIvnt55DgYO/CQ52SBHVqn1KP6FxuhndppyMrf+0oXk7
obEDJdYZ9s0dY5c7Gs3HTChwfESoPGMFW9zdvqGU4k6wvMqPm3M4FPw62VXA6Tz5
YxJzjuSCpwKBgQCcL2SK5fOEuvY+ji7PC7KVqKMkl6fKhePJkNVeaZJ8vc561rEz
y8Wpj7K0YbhCzbpZXx830JHH0OZ6/zvHdwtiYplPo6xISOg7TGkTPH5L/ujkuPiz
e2yft1Xr6VaMKBJe8hYcYkM0agPBsLc+wagzUUJtFZhJaF64wrXIRbElhQKBgEHA
PeToI3/n6sz+xJjkwXyV9eoCwWAm7MTcCMvIfkHBvhyA+CB7h7iO3oa7dJklFcOM
camqPqpBT2Q55BZa1K5+zZgbcbl7x9xfOZFKu9BoizJjTL3uoYfOCCRi6gMrVvgB
lf+9M4nft+Z78hoyeQU+AMMnTm1wCGclRtxeIA9TAoGBAK+x1GYY7EhCSBPqeVka
LROfs/XSIzl4oNF4VXUyRpY3cUFMDJaPCLW1mwyRdsWbghDvchAP3FyC4leoZgCc
8Ng1xWqM8ZgoGyWBsLIZMu/pFf85oPCvqcWK3khNj/Oj+ZXCNJdj+kI0Kdh5sE5j
p06gtMzumMO/KYRADXdZHKjN
-----END PRIVATE KEY-----
";

const TEST_JWKS: &str = r#"{"keys":[{"kty":"RSA","use":"sig","kid":"test-key-2025","alg":"RS256","e":"AQAB","n":"51y73qqlkebeEu-QoDS7cMQN_4A2BDKXpCjtcvUiWNkh2f4qTHh00-bU-JBJR7RHfRUSyxDPjp2DfLti_rmih1cAZAT8GMYZWRLtbOiFV0tXllq3e4s2-A3hcOEwo6BzL0ZlAzetaYzxZONelx9QVLTtbwhevaejDRkjf6pfwkjR0gtZuiLX2uioJfGGvjx2TkeHbKOlhU2aF5jZmvipo5Lp3HVV6ClemUm4iUkW2RWUQoN0QueOSgB16oHTSp4ILSuO-FC2MeNPOrfigUjcbL87rlbOYJRwNNkZvbXbfVxPMrVI8k3YyJq6GMY4EbIUiKVlA8EVk9ThApM85CcjWw"}]}"#;

// Cedar schema for anonymous-only tests: only http context, no jwt namespace.
const ANON_SCHEMA: &str = r#"
entity User;
entity HttpPath;

action "GET" appliesTo {
  principal: [User],
  resource:  [HttpPath],
  context: {
    http: { method: String, path: String, query?: String }
  }
};

action "POST" appliesTo {
  principal: [User],
  resource:  [HttpPath],
  context: {
    http: { method: String, path: String, query?: String }
  }
};
"#;

// Cedar schema for JWT-principal tests: jwt context declared to match JwtClaims serialization.
// Only standard claims that are present in test tokens need to be declared; absent ones are
// omitted by the skip_serializing_if fix on JwtClaims.
const JWT_SCHEMA: &str = r#"
entity User;
entity HttpPath;

action "GET" appliesTo {
  principal: [User],
  resource:  [HttpPath],
  context: {
    jwt: {
      sub?: String,
      iss?: String,
      aud?: Set<String>,
      exp?: Long,
      iat?: Long,
    },
    http: { method: String, path: String, query?: String }
  }
};

action "POST" appliesTo {
  principal: [User],
  resource:  [HttpPath],
  context: {
    jwt: {
      sub?: String,
      iss?: String,
      aud?: Set<String>,
      exp?: Long,
      iat?: Long,
    },
    http: { method: String, path: String, query?: String }
  }
};
"#;

fn simple_route_config() -> RouteConfigBuilder {
    RouteConfigBuilder::new("routes").virtual_host(
        VirtualHostBuilder::new("default").route(RouteBuilder::new().match_prefix("/").cluster("backend")),
    )
}

fn make_jwt(sub: &str) -> String {
    #[derive(Serialize)]
    struct Claims {
        sub: String,
        iss: String,
        aud: Vec<String>,
        exp: u64,
        iat: u64,
    }
    let claims = Claims {
        sub: sub.to_owned(),
        iss: "test-issuer".to_owned(),
        aud: vec!["mcp-gateway".to_owned()],
        exp: 9_999_999_999,
        iat: 0,
    };
    let key = EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY_PEM.as_bytes()).unwrap();
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test-key-2025".to_owned());
    encode(&header, &claims, &key).unwrap()
}

fn jwt_authn_for_tests() -> JwtAuthentication {
    let provider = JwtProvider {
        audiences: vec!["mcp-gateway".to_owned()],
        from_headers: vec![JwtHeader { name: "Authorization".to_owned(), value_prefix: "Bearer ".to_owned() }],
        jwks_source_specifier: Some(JwksSourceSpecifier::LocalJwks(DataSource {
            specifier: Some(Specifier::InlineString(TEST_JWKS.to_owned())),
            ..Default::default()
        })),
        // Required so that orion inserts JwtClaims into request extensions, making them
        // available to downstream filters (Cedar).
        payload_in_metadata: "jwt_payload".to_owned(),
        ..Default::default()
    };

    let rule = RequirementRule {
        r#match: Some(RouteMatch { path_specifier: Some(PathSpecifier::Prefix("/".to_owned())), ..Default::default() }),
        requirement_type: Some(RequirementType::Requires(JwtRequirement {
            requires_type: Some(RequiresType::ProviderName("test_provider".to_owned())),
        })),
    };

    JwtAuthentication {
        providers: [("test_provider".to_owned(), provider)].into_iter().collect(),
        rules: vec![rule],
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Group 1 — Anonymous Cedar RBAC
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_cedar_anon_allow() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let cedar = CedarPolicyBuilder::new(
        ANON_SCHEMA,
        r#"permit(
        principal == User::"anonymous",
        action    == Action::"GET",
        resource  == HttpPath::"/health"
    );"#,
    )
    .fail_open();

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().cedar_policy(cedar).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    client.get("/health").await.unwrap().assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_cedar_anon_deny_wrong_method() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let cedar = CedarPolicyBuilder::new(
        ANON_SCHEMA,
        r#"permit(
        principal == User::"anonymous",
        action    == Action::"GET",
        resource  == HttpPath::"/health"
    );"#,
    )
    .fail_open();

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().cedar_policy(cedar).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    client.post("/health", "").await.unwrap().assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_cedar_anon_deny_wrong_resource() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let cedar = CedarPolicyBuilder::new(
        ANON_SCHEMA,
        r#"permit(
        principal == User::"anonymous",
        action    == Action::"GET",
        resource  == HttpPath::"/health"
    );"#,
    )
    .fail_open();

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().cedar_policy(cedar).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    client.get("/other").await.unwrap().assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

// ---------------------------------------------------------------------------
// Group 2 — JWT principal + Cedar RBAC
// ---------------------------------------------------------------------------

const JWT_POLICIES: &str = r#"
permit(principal == User::"svc-frontend", action == Action::"GET", resource);
permit(principal == User::"svc-admin",    action,                  resource);
"#;

async fn spawn_jwt_cedar_instance(backend: &TestBackend) -> OrionInstance {
    let cedar = CedarPolicyBuilder::new(JWT_SCHEMA, JWT_POLICIES).fail_open();

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main").hcm(
                    HcmBuilder::new()
                        .http1()
                        .jwt_authn(jwt_authn_for_tests())
                        .cedar_policy(cedar)
                        .route_config(simple_route_config()),
                ),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap()
}

#[tokio::test]
#[ignore]
async fn test_cedar_jwt_frontend_get_allowed() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let orion = spawn_jwt_cedar_instance(&backend).await;
    let token = make_jwt("svc-frontend");
    let client =
        TestClient::new(orion.listener_addr().unwrap()).with_header("authorization", format!("Bearer {token}"));

    client.get("/api").await.unwrap().assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_cedar_jwt_frontend_post_denied() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let orion = spawn_jwt_cedar_instance(&backend).await;
    let token = make_jwt("svc-frontend");
    let client =
        TestClient::new(orion.listener_addr().unwrap()).with_header("authorization", format!("Bearer {token}"));

    client.post("/api", "").await.unwrap().assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_cedar_jwt_admin_post_allowed() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let orion = spawn_jwt_cedar_instance(&backend).await;
    let token = make_jwt("svc-admin");
    let client =
        TestClient::new(orion.listener_addr().unwrap()).with_header("authorization", format!("Bearer {token}"));

    client.post("/api", "").await.unwrap().assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_cedar_jwt_no_token_rejected_by_jwt_filter() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let orion = spawn_jwt_cedar_instance(&backend).await;
    let client = TestClient::new(orion.listener_addr().unwrap());

    // JWT filter rejects before Cedar runs
    client.get("/api").await.unwrap().assert_status(StatusCode::UNAUTHORIZED);

    orion.shutdown();
}

// ---------------------------------------------------------------------------
// Group 3 — Enforcement and failure modes
// ---------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn test_cedar_log_only_passes_despite_deny() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    // deny-all policy, but LOG_ONLY — requests must still pass through
    let cedar = CedarPolicyBuilder::new(ANON_SCHEMA, "forbid(principal, action, resource);").log_only();

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().cedar_policy(cedar).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    client.get("/anything").await.unwrap().assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_cedar_fail_open_on_context_mismatch() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    // Schema declares no context at all for the action; the filter always injects an http
    // context record → Cedar context validation fails → evaluation error → FAIL_OPEN → 200.
    let empty_context_schema = r#"
entity User;
entity HttpPath;
action "GET" appliesTo { principal: [User], resource: [HttpPath] };
action "POST" appliesTo { principal: [User], resource: [HttpPath] };
"#;
    let cedar = CedarPolicyBuilder::new(
        empty_context_schema,
        r#"permit(principal == User::"anonymous", action == Action::"GET", resource);"#,
    )
    .fail_open();

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().cedar_policy(cedar).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    // Context mismatch causes evaluation error; FAIL_OPEN passes the request through.
    client.get("/health").await.unwrap().assert_status(StatusCode::OK);

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_cedar_fail_closed_on_bad_schema() {
    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    // FAIL_CLOSED (default) with a deny-all policy: every request → 403.
    let cedar = CedarPolicyBuilder::new(ANON_SCHEMA, "forbid(principal, action, resource);");

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().cedar_policy(cedar).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());
    client.get("/anything").await.unwrap().assert_status(StatusCode::FORBIDDEN);

    orion.shutdown();
}

// ---------------------------------------------------------------------------
// Group 4 — reproduce the load-test regression: Cedar decisions flip once
// requests are pipelined over persistent/keep-alive connections, or once many
// connections are opened concurrently across worker threads.

/// Same deny-wrong-resource policy as `test_cedar_anon_deny_wrong_resource`,
/// but issues many sequential requests over the *same* `TestClient`, whose
/// underlying hyper client keeps the connection alive and reuses it — mirroring
/// a load test with persistent/keep-alive connections. Every single request
/// must be denied; if any request after the first one comes back 200, the
/// Cedar filter is not being re-evaluated (or is caching a decision) on
/// subsequent requests of a reused connection.
#[tokio::test]
#[ignore]
async fn test_cedar_anon_deny_repeated_requests_same_connection() {
    const REQUESTS: usize = 10000;

    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let cedar = CedarPolicyBuilder::new(
        ANON_SCHEMA,
        r#"permit(
        principal == User::"anonymous",
        action    == Action::"GET",
        resource  == HttpPath::"/health"
    );"#,
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().cedar_policy(cedar).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.unwrap();

    let client = TestClient::new(orion.listener_addr().unwrap());

    let mut wrongly_allowed = Vec::new();
    for i in 0..REQUESTS {
        let response = client.get("/other").await.unwrap();
        if response.status != StatusCode::FORBIDDEN {
            wrongly_allowed.push((i, response.status));
        }
    }

    assert!(
        wrongly_allowed.is_empty(),
        "expected every request on the reused connection to be denied, but {}/{REQUESTS} were not: {wrongly_allowed:?}",
        wrongly_allowed.len()
    );

    orion.shutdown();
}

/// Heavier variant closer to the reported load-test shape: many *concurrent*
/// persistent connections (one `TestClient`/hyper connection per task, spread
/// across many Orion worker threads), each hammering the same denied path for
/// a couple of seconds. Every response, from the first to the last, must be
/// FORBIDDEN. Failures are reported with the index within their connection so
/// a "correct for the first few requests, then flips" pattern is visible.
#[tokio::test]
#[ignore]
async fn test_cedar_anon_deny_sustained_load_many_persistent_connections() {
    use std::time::{Duration, Instant};
    const CONNECTIONS: usize = 128;
    const TEST_DURATION: Duration = Duration::from_secs(2);

    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let cedar = CedarPolicyBuilder::new(
        ANON_SCHEMA,
        r#"permit(
        principal == User::"anonymous",
        action    == Action::"GET",
        resource  == HttpPath::"/health"
    );"#,
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().cedar_policy(cedar).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion =
        OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default().with_num_cpus(8)).await.unwrap();
    let addr = orion.listener_addr().unwrap();

    let handles: Vec<_> = (0..CONNECTIONS)
        .map(|conn_idx| {
            tokio::spawn(async move {
                let client = TestClient::new(addr);
                let start = Instant::now();
                let mut request_idx = 0usize;
                let mut first_wrong: Option<(usize, StatusCode, std::time::Duration)> = None;
                let mut wrong_count = 0usize;
                while start.elapsed() < TEST_DURATION {
                    let response = client.get("/other").await.unwrap();
                    if response.status != StatusCode::FORBIDDEN {
                        wrong_count += 1;
                        if first_wrong.is_none() {
                            first_wrong = Some((request_idx, response.status, start.elapsed()));
                        }
                    }
                    request_idx += 1;
                }
                (conn_idx, request_idx, wrong_count, first_wrong)
            })
        })
        .collect();

    let results: Vec<_> = join_all(handles).await.into_iter().map(|r| r.unwrap()).collect();

    let total_requests: usize = results.iter().map(|(_, n, _, _)| n).sum();
    let total_wrong: usize = results.iter().map(|(_, _, w, _)| w).sum();
    let offenders: Vec<_> = results.iter().filter(|(_, _, w, _)| *w > 0).collect();

    assert!(
        total_wrong == 0,
        "expected every request across {CONNECTIONS} persistent connections to be denied, but {total_wrong}/{total_requests} were not.\n\
         First few offending connections (conn_idx, requests_sent, wrong_count, first_wrong(request_idx, status, elapsed)): {:?}",
        offenders.iter().take(10).collect::<Vec<_>>()
    );

    orion.shutdown();
}

/// Same policy, but each request opens a brand-new connection (no keep-alive
/// reuse) and many of these are fired concurrently across multiple Orion
/// worker threads — mirroring a load test with one request per connection.
#[tokio::test]
#[ignore]
async fn test_cedar_anon_deny_concurrent_single_shot_connections() {
    const REQUESTS: usize = 256;

    let backend = TestBackend::start().await.unwrap();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    let cedar = CedarPolicyBuilder::new(
        ANON_SCHEMA,
        r#"permit(
        principal == User::"anonymous",
        action    == Action::"GET",
        resource  == HttpPath::"/health"
    );"#,
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .hcm(HcmBuilder::new().http1().cedar_policy(cedar).route_config(simple_route_config())),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion =
        OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default().with_num_cpus(8)).await.unwrap();
    let addr = orion.listener_addr().unwrap();

    let handles: Vec<_> = (0..REQUESTS)
        .map(|_| {
            tokio::spawn(async move {
                // A fresh TestClient per task => a fresh connection per request, no pooling reuse.
                let client = TestClient::new(addr);
                client.get("/other").await
            })
        })
        .collect();

    let results: Vec<_> = join_all(handles).await.into_iter().map(|r| r.unwrap().unwrap()).collect();

    let wrongly_allowed: Vec<_> = results
        .iter()
        .enumerate()
        .filter(|(_, r)| r.status != StatusCode::FORBIDDEN)
        .map(|(i, r)| (i, r.status))
        .collect();

    assert!(
        wrongly_allowed.is_empty(),
        "expected every single-shot-connection request to be denied, but {}/{REQUESTS} were not: {wrongly_allowed:?}",
        wrongly_allowed.len()
    );

    orion.shutdown();
}
