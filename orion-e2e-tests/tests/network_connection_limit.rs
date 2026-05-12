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

use orion_e2e_tests::config_builder::{
    BootstrapBuilder, ClusterBuilder, EndpointBuilder, FilterChainBuilder, ListenerBuilder, TcpProxyBuilder,
};
use orion_e2e_tests::{OrionInstance, SpawnOptions, TcpTestBackend, TcpTestClient};
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

const MAX_CONNECTIONS: u64 = 3;
const BACKEND_HOLD_DURATION: Duration = Duration::from_secs(5);
const ALIVE_CHECK_TIMEOUT: Duration = Duration::from_millis(200);

// Returns true if the connection is alive (read times out with no data),
// false if Orion closed it (EOF or error).
async fn connection_is_alive(stream: &mut TcpStream) -> bool {
    let mut buf = [0u8; 1];
    match tokio::time::timeout(ALIVE_CHECK_TIMEOUT, stream.read(&mut buf)).await {
        Err(_timeout) => true,
        Ok(Ok(0)) => false,
        Ok(Ok(_)) => true,
        Ok(Err(_)) => false,
    }
}

async fn setup(backend: &TcpTestBackend) -> (OrionInstance, TcpTestClient) {
    let bootstrap = BootstrapBuilder::new()
        .listener(
            ListenerBuilder::new("tcp").port(0).filter_chain(
                FilterChainBuilder::new("main")
                    .connection_limit(MAX_CONNECTIONS, None)
                    .tcp_proxy(TcpProxyBuilder::new("tcp").cluster("backend")),
            ),
        )
        .cluster(ClusterBuilder::new("backend").endpoint(EndpointBuilder::from_socket_addr(backend.addr())));

    let config_path = bootstrap.build_to_temp().unwrap();
    let orion = OrionInstance::spawn_auto_port(&config_path, "tcp", SpawnOptions::default()).await.unwrap();
    let client = TcpTestClient::new(orion.listener_addr().unwrap());
    (orion, client)
}

#[tokio::test]
#[ignore]
async fn test_connection_limit_enforced() {
    let backend = TcpTestBackend::start().await.unwrap();
    backend.set_read_timeout(BACKEND_HOLD_DURATION).await;

    let (orion, client) = setup(&backend).await;

    let mut open_streams: Vec<TcpStream> = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        let mut stream = client.connect().await.unwrap();
        assert!(connection_is_alive(&mut stream).await, "connection under the cap should be accepted");
        open_streams.push(stream);
    }

    let mut extra = client.connect().await.unwrap();
    assert!(!connection_is_alive(&mut extra).await, "connection over the cap should be rejected");

    orion.shutdown();
}

#[tokio::test]
#[ignore]
async fn test_connection_limit_restored_after_close() {
    let backend = TcpTestBackend::start().await.unwrap();
    backend.set_read_timeout(BACKEND_HOLD_DURATION).await;

    let (orion, client) = setup(&backend).await;

    let mut open_streams: Vec<TcpStream> = Vec::new();
    for _ in 0..MAX_CONNECTIONS {
        let mut stream = client.connect().await.unwrap();
        assert!(connection_is_alive(&mut stream).await, "connection under the cap should be accepted");
        open_streams.push(stream);
    }

    let mut extra = client.connect().await.unwrap();
    assert!(!connection_is_alive(&mut extra).await, "connection over the cap should be rejected");
    drop(extra);

    drop(open_streams);
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut new_stream = client.connect().await.unwrap();
    assert!(connection_is_alive(&mut new_stream).await, "connection should be accepted after slots are freed");

    orion.shutdown();
}
