use std::time::Duration;

use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, ListenerBuilder, NetworkRbacBuilder,
    NetworkRbacPolicyBuilder, TcpProxyBuilder,
};
use orion_e2e_tests::{OrionInstance, SpawnOptions, TcpTestBackend, TcpTestClient};

#[tokio::test]
#[ignore]
async fn test_network_rbac_allow_any() {
    let mut backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::allow()
        .policy("allow-all", NetworkRbacPolicyBuilder::new().permission_any().principal_any());

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let response = client.receive_on_connect().await.unwrap();
    assert_eq!(response, b"hello");

    backend.await_connection().await.unwrap();
    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_deny_any() {
    let backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac =
        NetworkRbacBuilder::deny().policy("deny-all", NetworkRbacPolicyBuilder::new().permission_any().principal_any());

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let result = client.receive_on_connect_with_timeout(Duration::from_millis(500)).await;
    assert!(result.is_err() || result.unwrap().is_empty());

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_allow_no_match_denies() {
    let backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::allow()
        .policy("allow-other", NetworkRbacPolicyBuilder::new().permission_destination_port(9999).principal_any());

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let result = client.receive_on_connect_with_timeout(Duration::from_millis(500)).await;
    assert!(result.is_err() || result.unwrap().is_empty());

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_deny_no_match_allows() {
    let mut backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::deny()
        .policy("deny-other", NetworkRbacPolicyBuilder::new().permission_destination_port(9999).principal_any());

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let response = client.receive_on_connect().await.unwrap();
    assert_eq!(response, b"hello");

    backend.await_connection().await.unwrap();
    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_allow_source_ip_match() {
    let mut backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::allow().policy(
        "allow-localhost",
        NetworkRbacPolicyBuilder::new().permission_any().principal_source_ip("127.0.0.1", 32),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let response = client.receive_on_connect().await.unwrap();
    assert_eq!(response, b"hello");

    backend.await_connection().await.unwrap();
    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_allow_source_ip_no_match() {
    let backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::allow()
        .policy("allow-other-ip", NetworkRbacPolicyBuilder::new().permission_any().principal_source_ip("10.0.0.1", 32));

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let result = client.receive_on_connect_with_timeout(Duration::from_millis(500)).await;
    assert!(result.is_err() || result.unwrap().is_empty());

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_deny_source_ip_blocks() {
    let backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::deny().policy(
        "deny-localhost",
        NetworkRbacPolicyBuilder::new().permission_any().principal_source_ip("127.0.0.1", 32),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let result = client.receive_on_connect_with_timeout(Duration::from_millis(500)).await;
    assert!(result.is_err() || result.unwrap().is_empty());

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_allow_destination_ip_cidr() {
    let mut backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::allow().policy(
        "allow-localhost-ip",
        NetworkRbacPolicyBuilder::new().permission_destination_ip("127.0.0.0", 8).principal_any(),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let response = client.receive_on_connect().await.unwrap();
    assert_eq!(response, b"hello");

    backend.await_connection().await.unwrap();
    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_allow_source_ip_cidr() {
    let mut backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::allow().policy(
        "allow-localhost-cidr",
        NetworkRbacPolicyBuilder::new().permission_any().principal_source_ip("127.0.0.0", 8),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let response = client.receive_on_connect().await.unwrap();
    assert_eq!(response, b"hello");

    backend.await_connection().await.unwrap();
    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_allow_destination_ip() {
    let mut backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::allow().policy(
        "allow-localhost-dest",
        NetworkRbacPolicyBuilder::new().permission_destination_ip("127.0.0.1", 32).principal_any(),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let response = client.receive_on_connect().await.unwrap();
    assert_eq!(response, b"hello");

    backend.await_connection().await.unwrap();
    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_multiple_policies_first_match() {
    let mut backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::allow()
        .policy("aaa-first-policy", NetworkRbacPolicyBuilder::new().permission_any().principal_any())
        .policy("zzz-second-policy", NetworkRbacPolicyBuilder::new().permission_destination_port(9999).principal_any());

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let response = client.receive_on_connect().await.unwrap();
    assert_eq!(response, b"hello");

    backend.await_connection().await.unwrap();
    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_multiple_permissions_or() {
    let mut backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::allow().policy(
        "allow-multiple",
        NetworkRbacPolicyBuilder::new().permission_destination_port(9999).permission_any().principal_any(),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let response = client.receive_on_connect().await.unwrap();
    assert_eq!(response, b"hello");

    backend.await_connection().await.unwrap();
    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_permission_and_principal_and() {
    let mut backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac = NetworkRbacBuilder::allow().policy(
        "allow-both",
        NetworkRbacPolicyBuilder::new().permission_destination_ip("127.0.0.1", 32).principal_source_ip("127.0.0.1", 32),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let response = client.receive_on_connect().await.unwrap();
    assert_eq!(response, b"hello");

    backend.await_connection().await.unwrap();
    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_network_rbac_multiple_filters_all_must_pass() {
    let backend = TcpTestBackend::start().await.unwrap();
    backend.set_send_on_connect(b"hello").await;

    let rbac1 = NetworkRbacBuilder::allow()
        .policy("allow-all", NetworkRbacPolicyBuilder::new().permission_any().principal_any());

    let rbac2 = NetworkRbacBuilder::deny().policy(
        "deny-localhost",
        NetworkRbacPolicyBuilder::new().permission_any().principal_source_ip("127.0.0.1", 32),
    );

    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .network_rbac(rbac1)
                    .network_rbac(rbac2)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();

    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    let result = client.receive_on_connect_with_timeout(Duration::from_millis(500)).await;
    assert!(result.is_err() || result.unwrap().is_empty());

    orion.shutdown();
}
