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

use std::{net::SocketAddr, time::Duration};

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::endpoint::v3::{ClusterLoadAssignment, LocalityLbEndpoints},
        extensions::transport_sockets::tls::v3::Secret as EnvoySecret,
        service::discovery::v3::Resource,
    },
    google::protobuf::Any,
    orion::extensions::filters::http::mcp::mcp_gateway::v3::{
        DynamicMcpServer as OrionDynamicMcpServer, Tool as OrionTool,
    },
    prost::Message,
};
use orion_xds::xds::model::TypeUrl;

use crate::config_builder::mcp_gateway::{
    dynamic_mcp_server_xds_resource, mcp_tool_xds_resource, MCP_DYNAMIC_SERVER_TYPE_URL, MCP_TOOL_TYPE_URL,
};
use pingora_timeout::timeout as fast_timeout;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::config_builder::{Cluster, Endpoint, Listener, RouteConfig, Secret};
use crate::xds_server::{ServerAction, TrackedPush};
use orion_data_plane_api::envoy_data_plane_api::envoy::config::endpoint::v3::LbEndpoint;

#[derive(Debug, Error)]
pub enum XdsError {
    #[error("failed to send resource to xDS server: {0}")]
    SendError(String),

    #[error("failed to convert config to protobuf: {0}")]
    ConversionError(String),

    #[error("timeout waiting for ACK")]
    AckTimeout,

    #[error("timeout waiting for connection")]
    ConnectionTimeout,

    #[error("ACK channel closed")]
    AckChannelClosed,
}

pub trait ToXdsResource {
    fn to_xds_resource(&self) -> Resource;
}

impl ToXdsResource for orion_data_plane_api::envoy_data_plane_api::envoy::config::cluster::v3::Cluster {
    fn to_xds_resource(&self) -> Resource {
        let value = self.encode_to_vec();
        let any = Any { type_url: TypeUrl::Cluster.to_string(), value };

        Resource { name: self.name.clone(), resource: Some(any), ..Default::default() }
    }
}

impl ToXdsResource for orion_data_plane_api::envoy_data_plane_api::envoy::config::listener::v3::Listener {
    fn to_xds_resource(&self) -> Resource {
        let value = self.encode_to_vec();
        let any = Any { type_url: TypeUrl::Listener.to_string(), value };

        Resource { name: self.name.clone(), resource: Some(any), ..Default::default() }
    }
}

impl ToXdsResource for orion_data_plane_api::envoy_data_plane_api::envoy::config::route::v3::RouteConfiguration {
    fn to_xds_resource(&self) -> Resource {
        let value = self.encode_to_vec();
        let any = Any { type_url: TypeUrl::RouteConfiguration.to_string(), value };

        Resource { name: self.name.clone(), resource: Some(any), ..Default::default() }
    }
}

impl ToXdsResource for ClusterLoadAssignment {
    fn to_xds_resource(&self) -> Resource {
        let value = self.encode_to_vec();
        let any = Any { type_url: TypeUrl::ClusterLoadAssignment.to_string(), value };

        Resource { name: self.cluster_name.clone(), resource: Some(any), ..Default::default() }
    }
}

impl ToXdsResource for EnvoySecret {
    fn to_xds_resource(&self) -> Resource {
        let value = self.encode_to_vec();
        let any = Any { type_url: TypeUrl::Secret.to_string(), value };

        Resource { name: self.name.clone(), resource: Some(any), ..Default::default() }
    }
}

fn build_cluster_load_assignment(cluster_name: &str, endpoints: &[Endpoint]) -> ClusterLoadAssignment {
    let lb_endpoints: Vec<LbEndpoint> = endpoints.to_vec();
    let locality_endpoints = LocalityLbEndpoints { lb_endpoints, priority: 0, ..Default::default() };

    ClusterLoadAssignment {
        cluster_name: cluster_name.to_owned(),
        endpoints: vec![locality_endpoints],
        ..Default::default()
    }
}

fn build_cluster_load_assignment_with_priorities(
    cluster_name: &str,
    priority_endpoints: &[(u32, Vec<Endpoint>)],
) -> ClusterLoadAssignment {
    let endpoints: Vec<LocalityLbEndpoints> = priority_endpoints
        .iter()
        .map(|(priority, eps)| LocalityLbEndpoints {
            lb_endpoints: eps.clone(),
            priority: *priority,
            ..Default::default()
        })
        .collect();

    ClusterLoadAssignment { cluster_name: cluster_name.to_owned(), endpoints, ..Default::default() }
}

fn create_removal_resource(name: &str, type_url: TypeUrl) -> Resource {
    let any = Any { type_url: type_url.to_string(), value: vec![] };
    Resource { name: name.to_owned(), resource: Some(any), ..Default::default() }
}

pub struct ServerEventReceiver {
    rx: broadcast::Receiver<ServerEvent>,
}

impl ServerEventReceiver {
    pub fn new(rx: broadcast::Receiver<ServerEvent>) -> Self {
        Self { rx }
    }

    pub async fn wait_for_connection(&mut self, timeout: Duration) -> Result<(SocketAddr, String), XdsError> {
        let result = fast_timeout(timeout, async {
            loop {
                match self.rx.recv().await {
                    Ok(ServerEvent::ClientConnected { remote_addr, node_id }) => {
                        return Ok((remote_addr, node_id));
                    },
                    Ok(_) => {},
                    Err(_) => return Err(XdsError::ConnectionTimeout),
                }
            }
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Err(XdsError::ConnectionTimeout),
        }
    }
}

pub struct ConfigPusher {
    tx: mpsc::Sender<TrackedPush>,
}

impl ConfigPusher {
    pub fn new(tx: mpsc::Sender<TrackedPush>) -> Self {
        Self { tx }
    }

    pub async fn push_cluster(&self, cluster: &Cluster, timeout: Duration) -> Result<PushResult, XdsError> {
        let resource = cluster.to_xds_resource();
        self.send(ServerAction::Add(resource), timeout).await
    }

    pub async fn push_listener(&self, listener: &Listener, timeout: Duration) -> Result<PushResult, XdsError> {
        let resource = listener.to_xds_resource();
        self.send(ServerAction::Add(resource), timeout).await
    }

    pub async fn push_route_config(
        &self,
        route_config: &RouteConfig,
        timeout: Duration,
    ) -> Result<PushResult, XdsError> {
        let resource = route_config.to_xds_resource();
        self.send(ServerAction::Add(resource), timeout).await
    }

    pub async fn push_resource(&self, resource: Resource, timeout: Duration) -> Result<PushResult, XdsError> {
        self.send(ServerAction::Add(resource), timeout).await
    }

    pub async fn remove_cluster(&self, name: &str, timeout: Duration) -> Result<PushResult, XdsError> {
        let resource = create_removal_resource(name, TypeUrl::Cluster);
        self.send(ServerAction::Remove(resource), timeout).await
    }

    pub async fn remove_listener(&self, name: &str, timeout: Duration) -> Result<PushResult, XdsError> {
        let resource = create_removal_resource(name, TypeUrl::Listener);
        self.send(ServerAction::Remove(resource), timeout).await
    }

    pub async fn remove_route_config(&self, name: &str, timeout: Duration) -> Result<PushResult, XdsError> {
        let resource = create_removal_resource(name, TypeUrl::RouteConfiguration);
        self.send(ServerAction::Remove(resource), timeout).await
    }

    pub async fn push_endpoints(
        &self,
        cluster_name: &str,
        endpoints: &[Endpoint],
        timeout: Duration,
    ) -> Result<PushResult, XdsError> {
        let cla = build_cluster_load_assignment(cluster_name, endpoints);
        let resource = cla.to_xds_resource();
        self.send(ServerAction::Add(resource), timeout).await
    }

    pub async fn push_endpoints_with_priorities(
        &self,
        cluster_name: &str,
        priority_endpoints: &[(u32, Vec<Endpoint>)],
        timeout: Duration,
    ) -> Result<PushResult, XdsError> {
        let cla = build_cluster_load_assignment_with_priorities(cluster_name, priority_endpoints);
        let resource = cla.to_xds_resource();
        self.send(ServerAction::Add(resource), timeout).await
    }

    pub async fn remove_endpoints(&self, cluster_name: &str, timeout: Duration) -> Result<PushResult, XdsError> {
        let resource = create_removal_resource(cluster_name, TypeUrl::ClusterLoadAssignment);
        self.send(ServerAction::Remove(resource), timeout).await
    }

    pub async fn push_mcp_tool(
        &self,
        resource_id: &str,
        tool: &OrionTool,
        timeout: Duration,
    ) -> Result<PushResult, XdsError> {
        let resource = mcp_tool_xds_resource(resource_id, tool);
        self.send(ServerAction::Add(resource), timeout).await
    }

    pub async fn remove_mcp_tool(&self, resource_id: &str, timeout: Duration) -> Result<PushResult, XdsError> {
        let resource = create_removal_resource(resource_id, TypeUrl::Extension(MCP_TOOL_TYPE_URL.to_string()));
        self.send(ServerAction::Remove(resource), timeout).await
    }

    pub async fn push_dynamic_mcp_server(
        &self,
        resource_id: &str,
        server: &OrionDynamicMcpServer,
        timeout: Duration,
    ) -> Result<PushResult, XdsError> {
        let resource = dynamic_mcp_server_xds_resource(resource_id, server);
        self.send(ServerAction::Add(resource), timeout).await
    }

    pub async fn remove_dynamic_mcp_server(
        &self,
        resource_id: &str,
        timeout: Duration,
    ) -> Result<PushResult, XdsError> {
        let resource =
            create_removal_resource(resource_id, TypeUrl::Extension(MCP_DYNAMIC_SERVER_TYPE_URL.to_string()));
        self.send(ServerAction::Remove(resource), timeout).await
    }

    pub async fn push_secret(&self, secret: &Secret, timeout: Duration) -> Result<PushResult, XdsError> {
        let resource = secret.to_xds_resource();
        self.send(ServerAction::Add(resource), timeout).await
    }

    pub async fn remove_secret(&self, name: &str, timeout: Duration) -> Result<PushResult, XdsError> {
        let resource = create_removal_resource(name, TypeUrl::Secret);
        self.send(ServerAction::Remove(resource), timeout).await
    }

    async fn send(&self, action: ServerAction, timeout: Duration) -> Result<PushResult, XdsError> {
        let nonce = uuid::Uuid::new_v4().to_string();
        let (result_tx, result_rx) = oneshot::channel();

        let tracked = TrackedPush { action, nonce, result_tx };
        self.tx.send(tracked).await.map_err(|e| XdsError::SendError(e.to_string()))?;

        match fast_timeout(timeout, result_rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(XdsError::AckChannelClosed),
            Err(_) => Err(XdsError::AckTimeout),
        }
    }
}

pub use crate::xds_server::{start_tracked_aggregate_server, PushResult, ServerEvent, TrackedXdsServer};
