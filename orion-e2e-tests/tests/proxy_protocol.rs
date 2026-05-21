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

use pingora::prelude::fast_timeout::fast_timeout;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use orion_e2e_tests::config_builder::{
    presets, BootstrapBuilder, ClusterBuilder, DownstreamTlsBuilder, EndpointBuilder, FilterChainBuilder,
    ListenerBuilder, NetworkRbacBuilder, NetworkRbacPolicyBuilder, ProxyProtocolConfig, ProxyProtocolPassThroughTlvs,
    ProxyProtocolVersion, TcpProxyBuilder, UpstreamProxyProtocolBuilder,
};
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PreConfiguredResponse, ProxyProtocolTcpClient, SpawnOptions, TcpTestBackend,
    TestBackend, TestCerts, TlsClientConfig,
};

const CLAIMED_SRC: &str = "192.0.2.7:55001";
const CLAIMED_DST: &str = "198.51.100.10:443";

fn claimed_src() -> SocketAddr {
    CLAIMED_SRC.parse().unwrap()
}

fn claimed_dst() -> SocketAddr {
    CLAIMED_DST.parse().unwrap()
}

async fn http_get_raw<S>(stream: &mut S, path: &str, host: &str) -> u16
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let req = format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n");
    stream.write_all(req.as_bytes()).await.expect("write request");
    stream.flush().await.expect("flush");
    let mut buf = Vec::with_capacity(1024);
    drop(stream.read_to_end(&mut buf).await);
    parse_status(&buf)
}

fn parse_status(bytes: &[u8]) -> u16 {
    let s = String::from_utf8_lossy(bytes);
    let first = s.lines().next().unwrap_or("");
    first.split_whitespace().nth(1).and_then(|c| c.parse().ok()).unwrap_or(0)
}

#[tokio::test]
#[ignore]
async fn pp_v2_passes_original_source_to_backend() {
    let mut backend = TestBackend::start().await.expect("backend");
    backend.set_default_response(PreConfiguredResponse::with_body("ok")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).with_proxy_protocol().filter_chain(
            presets::http_filter_chain_with_network_rbac(
                "main",
                "backend",
                NetworkRbacBuilder::allow().policy(
                    "allow-claimed-source",
                    NetworkRbacPolicyBuilder::new().permission_any().principal_source_ip("192.0.2.7", 32),
                ),
            ),
        ))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn");

    let mut stream = ProxyProtocolTcpClient::v2(claimed_src(), claimed_dst())
        .connect(orion.listener_addr().unwrap())
        .await
        .expect("connect");
    let status = http_get_raw(&mut stream, "/hello", "test").await;
    assert_eq!(status, 200);

    let captured = backend.await_request().await.expect("backend request");
    assert_eq!(captured.path(), "/hello");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn pp_v1_text_header_accepted() {
    let mut backend = TestBackend::start().await.expect("backend");
    backend.set_default_response(PreConfiguredResponse::with_body("ok")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(ListenerBuilder::new("http").port(0).with_proxy_protocol().filter_chain(
            presets::http_filter_chain_with_network_rbac(
                "main",
                "backend",
                NetworkRbacBuilder::allow().policy(
                    "allow-claimed-source",
                    NetworkRbacPolicyBuilder::new().permission_any().principal_source_ip("192.0.2.7", 32),
                ),
            ),
        ))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn");

    let mut stream = ProxyProtocolTcpClient::v1(claimed_src(), claimed_dst())
        .connect(orion.listener_addr().unwrap())
        .await
        .expect("connect");
    let status = http_get_raw(&mut stream, "/v1", "test").await;
    assert_eq!(status, 200);

    backend.await_request().await.expect("backend got request");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn pp_required_but_missing_rejects_connection() {
    let mut backend = TestBackend::start().await.expect("backend");
    backend.set_default_response(PreConfiguredResponse::with_body("ok")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http")
                .port(0)
                .with_proxy_protocol()
                .filter_chain(presets::http_filter_chain("main", "backend")),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn");

    let mut stream =
        ProxyProtocolTcpClient::no_header().connect(orion.listener_addr().unwrap()).await.expect("connect");
    drop(stream.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await);
    drop(stream.flush().await);
    let mut buf = [0u8; 64];
    let n = fast_timeout(Duration::from_millis(500), stream.read(&mut buf)).await;
    let response_received = matches!(n, Ok(Ok(read)) if read > 0);
    assert!(!response_received, "connection should be closed before any HTTP response");

    assert!(backend.try_recv_request().is_none(), "backend received a request despite missing PP header");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn pp_allow_passthrough_when_no_header() {
    let mut backend = TestBackend::start().await.expect("backend");
    backend.set_default_response(PreConfiguredResponse::with_body("ok")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http")
                .port(0)
                .with_proxy_protocol_config(ProxyProtocolConfig {
                    allow_requests_without_proxy_protocol: true,
                    ..ProxyProtocolConfig::default()
                })
                .filter_chain(presets::http_filter_chain("main", "backend")),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn");

    let mut stream =
        ProxyProtocolTcpClient::no_header().connect(orion.listener_addr().unwrap()).await.expect("connect");
    let status = http_get_raw(&mut stream, "/passthrough", "x").await;
    assert_eq!(status, 200);
    backend.await_request().await.expect("backend got request");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn pp_v1_disallowed_when_v2_only() {
    let mut backend = TestBackend::start().await.expect("backend");
    backend.set_default_response(PreConfiguredResponse::with_body("ok")).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("http")
                .port(0)
                .with_proxy_protocol_config(ProxyProtocolConfig {
                    disallowed_versions: vec![ProxyProtocolVersion::V1],
                    ..ProxyProtocolConfig::default()
                })
                .filter_chain(presets::http_filter_chain("main", "backend")),
        )
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default()).await.expect("spawn");

    let mut stream = ProxyProtocolTcpClient::v1(claimed_src(), claimed_dst())
        .connect(orion.listener_addr().unwrap())
        .await
        .expect("connect");
    drop(stream.write_all(b"GET / HTTP/1.1\r\nHost: x\r\n\r\n").await);
    drop(stream.flush().await);
    let mut buf = [0u8; 64];
    let n = fast_timeout(Duration::from_millis(500), stream.read(&mut buf)).await;
    let response_received = matches!(n, Ok(Ok(read)) if read > 0);
    assert!(!response_received, "v1 should be rejected when disallowed");
    assert!(backend.try_recv_request().is_none());

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn pp_v2_tlv_pass_through_to_upstream() {
    let mut backend = TcpTestBackend::start().await.expect("tcp backend");
    backend.set_read_timeout(Duration::from_millis(800)).await;

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp")
                .port(0)
                .with_proxy_protocol_config(ProxyProtocolConfig {
                    pass_through_tlvs: Some(ProxyProtocolPassThroughTlvs { match_all: true, tlv_types: vec![] }),
                    ..ProxyProtocolConfig::default()
                })
                .filter_chain(
                    FilterChainBuilder::new("main").tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
                ),
        )
        .cluster(
            ClusterBuilder::new("backend")
                .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
                .with_proxy_protocol(UpstreamProxyProtocolBuilder::v2().pass_all_tlvs()),
        );

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.expect("spawn");

    let mut stream = ProxyProtocolTcpClient::v2(claimed_src(), claimed_dst())
        .with_tlv(0x05, b"alpha".to_vec())
        .with_tlv(0xEE, b"beta".to_vec())
        .connect(orion.listener_addr().unwrap())
        .await
        .expect("connect");
    drop(stream.write_all(b"ping\n").await);
    drop(stream.shutdown().await);

    let captured = backend.await_connection().await.expect("backend connection");
    let parsed = ppp::HeaderResult::parse(&captured.received_data);
    let v2 = match parsed {
        ppp::HeaderResult::V2(Ok(h)) => h,
        other => panic!("upstream did not start with a v2 PROXY header: {other:?}"),
    };
    let mut tlv_kinds: Vec<u8> = v2.tlvs().flatten().map(|t| t.kind).collect();
    tlv_kinds.sort_unstable();
    assert!(tlv_kinds.contains(&0x05), "TLV 0x05 missing, saw {tlv_kinds:?}");
    assert!(tlv_kinds.contains(&0xEE), "TLV 0xEE missing, saw {tlv_kinds:?}");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn pp_v2_with_tls_inspector_routes_by_sni() {
    let mut dublin = TestBackend::start().await.expect("dublin backend");
    let mut athlone = TestBackend::start().await.expect("athlone backend");
    dublin.set_default_response(PreConfiguredResponse::with_body("dublin")).await;
    athlone.set_default_response(PreConfiguredResponse::with_body("athlone")).await;

    let certs = TestCerts::new();
    let tls_dublin = DownstreamTlsBuilder::new().cert_files(
        TestCerts::path_to_string(&certs.beefcake_dublin_cert()),
        TestCerts::path_to_string(&certs.beefcake_dublin_key()),
    );
    let tls_athlone = DownstreamTlsBuilder::new().cert_files(
        TestCerts::path_to_string(&certs.beefcake_athlone_cert()),
        TestCerts::path_to_string(&certs.beefcake_athlone_key()),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("https")
                .port(0)
                .with_proxy_protocol()
                .with_tls_inspector()
                .filter_chain(presets::https_sni_filter_chain(
                    "dublin",
                    &["dublin.beefcake.example.com"],
                    tls_dublin,
                    "dublin",
                ))
                .filter_chain(presets::https_sni_filter_chain(
                    "athlone",
                    &["athlone.beefcake.example.com"],
                    tls_athlone,
                    "athlone",
                )),
        )
        .cluster(ClusterBuilder::with_endpoint("dublin", dublin.addr()))
        .cluster(ClusterBuilder::with_endpoint("athlone", athlone.addr()));

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default()).await.expect("spawn");
    let addr = orion.listener_addr().unwrap();

    let tls = TlsClientConfig::with_root_ca(certs.beefcake_ca_chain()).expect("tls config");
    let mut tls_stream = ProxyProtocolTcpClient::v2(claimed_src(), claimed_dst())
        .connect_tls(addr, "dublin.beefcake.example.com", tls)
        .await
        .expect("connect_tls dublin");
    let status = http_get_raw(&mut tls_stream, "/d", "dublin.beefcake.example.com").await;
    assert_eq!(status, 200);
    let req = dublin.await_request().await.expect("dublin got request");
    assert_eq!(req.path(), "/d");
    assert!(athlone.try_recv_request().is_none());

    let tls = TlsClientConfig::with_root_ca(certs.beefcake_ca_chain()).expect("tls config");
    let mut tls_stream = ProxyProtocolTcpClient::v2(claimed_src(), claimed_dst())
        .connect_tls(addr, "athlone.beefcake.example.com", tls)
        .await
        .expect("connect_tls athlone");
    let status = http_get_raw(&mut tls_stream, "/a", "athlone.beefcake.example.com").await;
    assert_eq!(status, 200);
    let req = athlone.await_request().await.expect("athlone got request");
    assert_eq!(req.path(), "/a");
    assert!(dublin.try_recv_request().is_none());

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn pp_v1_with_tls_inspector_routes_by_sni() {
    let mut dublin = TestBackend::start().await.expect("dublin backend");
    let mut athlone = TestBackend::start().await.expect("athlone backend");
    dublin.set_default_response(PreConfiguredResponse::with_body("dublin")).await;
    athlone.set_default_response(PreConfiguredResponse::with_body("athlone")).await;

    let certs = TestCerts::new();
    let tls_dublin = DownstreamTlsBuilder::new().cert_files(
        TestCerts::path_to_string(&certs.beefcake_dublin_cert()),
        TestCerts::path_to_string(&certs.beefcake_dublin_key()),
    );
    let tls_athlone = DownstreamTlsBuilder::new().cert_files(
        TestCerts::path_to_string(&certs.beefcake_athlone_cert()),
        TestCerts::path_to_string(&certs.beefcake_athlone_key()),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("https")
                .port(0)
                .with_proxy_protocol()
                .with_tls_inspector()
                .filter_chain(presets::https_sni_filter_chain(
                    "dublin",
                    &["dublin.beefcake.example.com"],
                    tls_dublin,
                    "dublin",
                ))
                .filter_chain(presets::https_sni_filter_chain(
                    "athlone",
                    &["athlone.beefcake.example.com"],
                    tls_athlone,
                    "athlone",
                )),
        )
        .cluster(ClusterBuilder::with_endpoint("dublin", dublin.addr()))
        .cluster(ClusterBuilder::with_endpoint("athlone", athlone.addr()));

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default()).await.expect("spawn");
    let addr = orion.listener_addr().unwrap();

    let tls = TlsClientConfig::with_root_ca(certs.beefcake_ca_chain()).expect("tls config");
    let mut tls_stream = ProxyProtocolTcpClient::v1(claimed_src(), claimed_dst())
        .connect_tls(addr, "dublin.beefcake.example.com", tls)
        .await
        .expect("connect_tls");
    let status = http_get_raw(&mut tls_stream, "/d", "dublin.beefcake.example.com").await;
    assert_eq!(status, 200);
    dublin.await_request().await.expect("dublin got request");
    assert!(athlone.try_recv_request().is_none());

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn pp_allow_passthrough_with_tls_inspector_routes_by_sni() {
    let mut dublin = TestBackend::start().await.expect("dublin backend");
    let mut athlone = TestBackend::start().await.expect("athlone backend");
    dublin.set_default_response(PreConfiguredResponse::with_body("dublin")).await;
    athlone.set_default_response(PreConfiguredResponse::with_body("athlone")).await;

    let certs = TestCerts::new();
    let tls_dublin = DownstreamTlsBuilder::new().cert_files(
        TestCerts::path_to_string(&certs.beefcake_dublin_cert()),
        TestCerts::path_to_string(&certs.beefcake_dublin_key()),
    );
    let tls_athlone = DownstreamTlsBuilder::new().cert_files(
        TestCerts::path_to_string(&certs.beefcake_athlone_cert()),
        TestCerts::path_to_string(&certs.beefcake_athlone_key()),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("https")
                .port(0)
                .with_proxy_protocol_config(ProxyProtocolConfig {
                    allow_requests_without_proxy_protocol: true,
                    ..ProxyProtocolConfig::default()
                })
                .with_tls_inspector()
                .filter_chain(presets::https_sni_filter_chain(
                    "dublin",
                    &["dublin.beefcake.example.com"],
                    tls_dublin,
                    "dublin",
                ))
                .filter_chain(presets::https_sni_filter_chain(
                    "athlone",
                    &["athlone.beefcake.example.com"],
                    tls_athlone,
                    "athlone",
                )),
        )
        .cluster(ClusterBuilder::with_endpoint("dublin", dublin.addr()))
        .cluster(ClusterBuilder::with_endpoint("athlone", athlone.addr()));

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "https", SpawnOptions::default()).await.expect("spawn");
    let addr = orion.listener_addr().unwrap();

    let tls = TlsClientConfig::with_root_ca(certs.beefcake_ca_chain()).expect("tls config");
    let mut tls_stream = ProxyProtocolTcpClient::no_header()
        .connect_tls(addr, "dublin.beefcake.example.com", tls)
        .await
        .expect("connect_tls dublin (no PP header)");
    let status = http_get_raw(&mut tls_stream, "/d", "dublin.beefcake.example.com").await;
    assert_eq!(status, 200);
    let req = dublin.await_request().await.expect("dublin got request");
    assert_eq!(req.path(), "/d");
    assert!(athlone.try_recv_request().is_none());

    let tls = TlsClientConfig::with_root_ca(certs.beefcake_ca_chain()).expect("tls config");
    let mut tls_stream = ProxyProtocolTcpClient::v2(claimed_src(), claimed_dst())
        .connect_tls(addr, "athlone.beefcake.example.com", tls)
        .await
        .expect("connect_tls athlone (with PP header)");
    let status = http_get_raw(&mut tls_stream, "/a", "athlone.beefcake.example.com").await;
    assert_eq!(status, 200);
    let req = athlone.await_request().await.expect("athlone got request");
    assert_eq!(req.path(), "/a");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn cluster_emits_pp_v2_to_plain_tcp_upstream() {
    let mut backend = TcpTestBackend::start().await.expect("tcp backend");
    backend.set_read_timeout(Duration::from_millis(800)).await;

    let bootstrap =
        BootstrapBuilder::new()
            .listener(ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main").tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ))
            .cluster(
                ClusterBuilder::new("backend")
                    .endpoint(EndpointBuilder::from_socket_addr(backend.addr()))
                    .with_proxy_protocol_v2(),
            );

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.expect("spawn");

    let mut stream = tokio::net::TcpStream::connect(orion.listener_addr().unwrap()).await.expect("connect");
    stream.write_all(b"payload\n").await.expect("write payload");
    drop(stream.shutdown().await);

    let captured = backend.await_connection().await.expect("backend connection");
    let parsed = ppp::HeaderResult::parse(&captured.received_data);
    assert!(matches!(parsed, ppp::HeaderResult::V2(Ok(_))), "expected v2 PROXY header, got {parsed:?}");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn internal_listener_basic_chain() {
    let mut backend = TestBackend::start().await.expect("backend");
    backend.set_default_response(PreConfiguredResponse::with_body("inner")).await;

    let outer = ListenerBuilder::new("outer")
        .port(0)
        .filter_chain(FilterChainBuilder::new("main").tcp_proxy(TcpProxyBuilder::new("tcp").cluster("inner_cluster")));

    let inner =
        ListenerBuilder::new("inner").internal().filter_chain(presets::http_filter_chain("inner_main", "backend"));

    let bootstrap = BootstrapBuilder::new()
        .listener(outer)
        .listener(inner)
        .cluster(ClusterBuilder::new("inner_cluster").endpoint(EndpointBuilder::internal("inner")))
        .cluster(ClusterBuilder::with_endpoint("backend", backend.addr()));

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "outer", SpawnOptions::default()).await.expect("spawn");

    let addr = orion.listener_addr().unwrap();
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let status = http_get_raw(&mut stream, "/inner", "x").await;
    assert_eq!(status, 200);
    backend.await_request().await.expect("backend got request");

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn internal_listener_chain_with_pp_outer_and_tls_inner() {
    let mut dublin = TestBackend::start().await.expect("dublin backend");
    let mut athlone = TestBackend::start().await.expect("athlone backend");
    dublin.set_default_response(PreConfiguredResponse::with_body("dublin")).await;
    athlone.set_default_response(PreConfiguredResponse::with_body("athlone")).await;

    let certs = TestCerts::new();
    let tls_dublin = DownstreamTlsBuilder::new().cert_files(
        TestCerts::path_to_string(&certs.beefcake_dublin_cert()),
        TestCerts::path_to_string(&certs.beefcake_dublin_key()),
    );
    let tls_athlone = DownstreamTlsBuilder::new().cert_files(
        TestCerts::path_to_string(&certs.beefcake_athlone_cert()),
        TestCerts::path_to_string(&certs.beefcake_athlone_key()),
    );

    let dublin_chain =
        presets::https_sni_filter_chain("dublin", &["dublin.beefcake.example.com"], tls_dublin, "dublin");
    let athlone_chain =
        presets::https_sni_filter_chain("athlone", &["athlone.beefcake.example.com"], tls_athlone, "athlone");

    let outer = ListenerBuilder::new("outer")
        .port(0)
        .with_proxy_protocol()
        .filter_chain(FilterChainBuilder::new("main").tcp_proxy(TcpProxyBuilder::new("tcp").cluster("inner_cluster")));

    let inner = ListenerBuilder::new("inner")
        .internal()
        .with_tls_inspector()
        .filter_chain(dublin_chain)
        .filter_chain(athlone_chain);

    let bootstrap = BootstrapBuilder::new()
        .listener(outer)
        .listener(inner)
        .cluster(ClusterBuilder::new("inner_cluster").endpoint(EndpointBuilder::internal("inner")))
        .cluster(ClusterBuilder::with_endpoint("dublin", dublin.addr()))
        .cluster(ClusterBuilder::with_endpoint("athlone", athlone.addr()));

    let config_path = bootstrap.build_to_temp().expect("config");
    let orion = OrionInstance::spawn_auto_port(&config_path, "outer", SpawnOptions::default()).await.expect("spawn");
    let addr = orion.listener_addr().unwrap();

    let tls = TlsClientConfig::with_root_ca(certs.beefcake_ca_chain()).expect("tls config");
    let mut tls_stream = ProxyProtocolTcpClient::v2(claimed_src(), claimed_dst())
        .connect_tls(addr, "dublin.beefcake.example.com", tls)
        .await
        .expect("connect_tls");
    let status = http_get_raw(&mut tls_stream, "/d", "dublin.beefcake.example.com").await;
    assert_eq!(status, 200);
    dublin.await_request().await.expect("dublin got request");
    assert!(athlone.try_recv_request().is_none());

    orion.shutdown();
    cleanup_config_file(&config_path);
}
