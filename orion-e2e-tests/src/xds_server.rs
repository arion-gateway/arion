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

use std::{net::SocketAddr, pin::Pin, sync::Arc};

use atomic_take::AtomicTake;
use dashmap::DashMap;
use orion_data_plane_api::envoy_data_plane_api::{
    envoy::service::discovery::v3::{
        aggregated_discovery_service_server::{AggregatedDiscoveryService, AggregatedDiscoveryServiceServer},
        DeltaDiscoveryRequest, DeltaDiscoveryResponse, DiscoveryRequest, DiscoveryResponse, ResourceName,
    },
    tonic::{self, transport::Server, IntoStreamingRequest, Response, Status},
};
use tokio::sync::{
    broadcast,
    mpsc::{self, Receiver},
    oneshot,
};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tracing::{debug, info};

pub use orion_xds::xds::server::ServerAction;

#[derive(Clone, Debug)]
pub enum ServerEvent {
    ClientConnected { remote_addr: SocketAddr, node_id: String },
    ClientDisconnected { remote_addr: SocketAddr },
    Subscribed { type_url: String, resource_names: Vec<String> },
}

#[derive(Clone, Debug)]
pub enum PushResult {
    Ack,
    Nack { error_code: i32, error_message: String },
}

pub struct TrackedPush {
    pub action: ServerAction,
    pub nonce: String,
    pub result_tx: oneshot::Sender<PushResult>,
}

pub struct AckTracker {
    pending: DashMap<String, oneshot::Sender<PushResult>>,
}

impl AckTracker {
    pub fn new() -> Self {
        Self { pending: DashMap::new() }
    }

    pub fn register(&self, nonce: String, result_tx: oneshot::Sender<PushResult>) {
        self.pending.insert(nonce, result_tx);
    }

    pub fn complete(&self, nonce: &str, result: PushResult) -> bool {
        if let Some((_, tx)) = self.pending.remove(nonce) {
            let _ = tx.send(result);
            true
        } else {
            false
        }
    }

    pub fn clear_all(&self) {
        self.pending.clear();
    }
}

impl Default for AckTracker {
    fn default() -> Self {
        Self::new()
    }
}

pub struct TrackedAggregateServer {
    delta_resources_rx: AtomicTake<Receiver<TrackedPush>>,
    stream_resources_rx: AtomicTake<Receiver<ServerAction>>,
    event_tx: broadcast::Sender<ServerEvent>,
    ack_tracker: Arc<AckTracker>,
}

impl std::fmt::Debug for TrackedAggregateServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackedAggregateServer").finish()
    }
}

impl TrackedAggregateServer {
    pub fn new(
        delta_resources_rx: Receiver<TrackedPush>,
        stream_resources_rx: Receiver<ServerAction>,
        event_tx: broadcast::Sender<ServerEvent>,
        ack_tracker: Arc<AckTracker>,
    ) -> Self {
        Self {
            delta_resources_rx: AtomicTake::new(delta_resources_rx),
            stream_resources_rx: AtomicTake::new(stream_resources_rx),
            event_tx,
            ack_tracker,
        }
    }
}

type AggregatedDiscoveryServiceResult<T> = std::result::Result<Response<T>, Status>;

#[tonic::async_trait]
impl AggregatedDiscoveryService for TrackedAggregateServer {
    type StreamAggregatedResourcesStream =
        Pin<Box<dyn Stream<Item = std::result::Result<DiscoveryResponse, Status>> + Send>>;

    async fn stream_aggregated_resources(
        &self,
        req: tonic::Request<tonic::Streaming<DiscoveryRequest>>,
    ) -> AggregatedDiscoveryServiceResult<Self::StreamAggregatedResourcesStream> {
        info!("TrackedAggregateServer::stream_aggregated_resources");
        info!("\tclient connected from: {:?}", req.remote_addr());

        let (tx, rx) = mpsc::channel(128);
        let mut resources_rx =
            self.stream_resources_rx.take().ok_or(Status::internal("Resource stream is unavailable"))?;
        tokio::spawn(async move {
            while let Some(action) = resources_rx.recv().await {
                let item = match action {
                    ServerAction::Add(resource) => {
                        let Some(resource) = resource.resource else {
                            continue;
                        };
                        DiscoveryResponse {
                            type_url: resource.type_url.clone(),
                            resources: vec![resource],
                            nonce: uuid::Uuid::new_v4().to_string(),
                            ..Default::default()
                        }
                    },
                    ServerAction::Remove(resource) => {
                        let Some(resource) = resource.resource else {
                            continue;
                        };
                        DiscoveryResponse {
                            type_url: resource.type_url,
                            nonce: uuid::Uuid::new_v4().to_string(),
                            ..Default::default()
                        }
                    },
                };

                match tx.send(std::result::Result::<_, Status>::Ok(item)).await {
                    Ok(()) => {},
                    Err(_item) => break,
                }
            }
            info!("\tclient disconnected");
        });

        let mut incoming_stream = req.into_streaming_request().into_inner();
        tokio::spawn(async move {
            while let Some(item) = incoming_stream.next().await {
                debug!("TrackedServer stream: Got item {item:?}");
            }
            info!("TrackedServer stream side closed");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::StreamAggregatedResourcesStream))
    }

    type DeltaAggregatedResourcesStream =
        Pin<Box<dyn Stream<Item = std::result::Result<DeltaDiscoveryResponse, Status>> + Send>>;

    async fn delta_aggregated_resources(
        &self,
        req: tonic::Request<tonic::Streaming<DeltaDiscoveryRequest>>,
    ) -> AggregatedDiscoveryServiceResult<Self::DeltaAggregatedResourcesStream> {
        info!("TrackedAggregateServer::delta_aggregated_resources");
        let remote_addr = req.remote_addr().unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
        info!("\tclient connected from: {:?}", remote_addr);

        let (tx, rx) = mpsc::channel(128);
        let mut resources_rx = self.delta_resources_rx.take().ok_or(Status::internal("Delta stream is unavailable"))?;
        let ack_tracker = self.ack_tracker.clone();

        tokio::spawn(async move {
            while let Some(TrackedPush { action, nonce, result_tx }) = resources_rx.recv().await {
                let Some(item) = build_delta_response(action, Some(nonce.clone())) else {
                    continue;
                };
                ack_tracker.register(nonce.clone(), result_tx);
                match tx.send(std::result::Result::<_, Status>::Ok(item)).await {
                    Ok(()) => {
                        debug!("Sent tracked push with nonce: {}", nonce);
                    },
                    Err(_) => {
                        break;
                    },
                }
            }
            ack_tracker.clear_all();
            info!("\tclient disconnected");
        });

        let mut incoming_stream = req.into_streaming_request().into_inner();
        let ack_tracker_for_incoming = self.ack_tracker.clone();
        let event_tx = self.event_tx.clone();
        let mut first_message = true;

        tokio::spawn(async move {
            while let Some(Ok(item)) = incoming_stream.next().await {
                if first_message {
                    first_message = false;
                    let node_id = item.node.as_ref().map(|n| n.id.clone()).unwrap_or_default();
                    let _ = event_tx.send(ServerEvent::ClientConnected { remote_addr, node_id });
                }

                if !item.response_nonce.is_empty() {
                    let result = if let Some(error) = item.error_detail {
                        PushResult::Nack { error_code: error.code, error_message: error.message }
                    } else {
                        PushResult::Ack
                    };
                    if ack_tracker_for_incoming.complete(&item.response_nonce, result) {
                        debug!("Completed ACK for nonce: {}", item.response_nonce);
                    }
                } else if !item.resource_names_subscribe.is_empty() {
                    let _ = event_tx.send(ServerEvent::Subscribed {
                        type_url: item.type_url.clone(),
                        resource_names: item.resource_names_subscribe.clone(),
                    });
                }
            }
            let _ = event_tx.send(ServerEvent::ClientDisconnected { remote_addr });
            info!("TrackedServer delta side closed");
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream) as Self::DeltaAggregatedResourcesStream))
    }
}

fn build_delta_response(action: ServerAction, nonce: Option<String>) -> Option<DeltaDiscoveryResponse> {
    let nonce = nonce.unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
    match action {
        ServerAction::Add(r) => {
            let resource = r.resource.as_ref()?;
            Some(DeltaDiscoveryResponse {
                type_url: resource.type_url.clone(),
                resources: vec![r],
                nonce,
                system_version_info: "system_version_info".to_owned(),
                ..Default::default()
            })
        },
        ServerAction::Remove(r) => {
            let resource = r.resource.as_ref()?;
            Some(DeltaDiscoveryResponse {
                type_url: resource.type_url.clone(),
                nonce,
                system_version_info: "system_version_info".to_owned(),
                removed_resource_names: vec![ResourceName {
                    name: r.name.clone(),
                    dynamic_parameter_constraints: None,
                }],
                removed_resources: vec![r.name],
                ..Default::default()
            })
        },
    }
}

pub struct TrackedXdsServer {
    pub event_rx: broadcast::Receiver<ServerEvent>,
    pub action_tx: mpsc::Sender<TrackedPush>,
}

pub fn start_tracked_aggregate_server(
    addr: SocketAddr,
    stream_resources_rx: Receiver<ServerAction>,
) -> TrackedXdsServer {
    info!("TrackedServer starting at {addr:?}");

    let (event_tx, event_rx) = broadcast::channel(16);
    let (action_tx, action_rx) = mpsc::channel::<TrackedPush>(128);
    let ack_tracker = Arc::new(AckTracker::new());

    let server = TrackedAggregateServer::new(action_rx, stream_resources_rx, event_tx, ack_tracker.clone());
    let aggregate_server = AggregatedDiscoveryServiceServer::new(server);

    tokio::spawn(async move {
        let result =
            Server::builder().concurrency_limit_per_connection(256).add_service(aggregate_server).serve(addr).await;
        info!("TrackedServer exited {result:?}");
    });

    TrackedXdsServer { event_rx, action_tx }
}
