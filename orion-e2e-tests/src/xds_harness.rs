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
use std::path::PathBuf;
use std::time::Duration;

use thiserror::Error;
use tokio::sync::mpsc;
use tracing::info;

use crate::config_builder::xds::{ConfigPusher, PushResult, ServerEventReceiver, XdsError};
use crate::config_builder::{BootstrapBuilder, Cluster, Endpoint, Listener, RouteConfig, Secret};
use crate::port_allocator::PortBlock;
use crate::xds_server::{start_tracked_aggregate_server, ServerAction, TrackedXdsServer};
use crate::{OrionInstance, SpawnOptions};

#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("xDS NACK: {error_message} (code: {error_code})")]
    Nack { error_code: i32, error_message: String },

    #[error("xDS error: {0}")]
    Xds(#[from] XdsError),

    #[error("{0}")]
    Other(#[from] crate::Error),
}

#[derive(Debug, Clone)]
pub struct HarnessTimeouts {
    pub connection: Duration,
    pub push: Duration,
}

impl Default for HarnessTimeouts {
    fn default() -> Self {
        Self { connection: Duration::from_secs(10), push: Duration::from_secs(10) }
    }
}

#[derive(Debug, Clone)]
pub struct XdsHarnessOptions {
    pub timeouts: HarnessTimeouts,
    pub log_level: String,
    pub orion_options: SpawnOptions,
}

impl Default for XdsHarnessOptions {
    fn default() -> Self {
        Self { timeouts: HarnessTimeouts::default(), log_level: "debug".into(), orion_options: SpawnOptions::default() }
    }
}

pub struct XdsEnabledHarness {
    port_block: PortBlock,
    pusher: ConfigPusher,
    orion: OrionInstance,
    config_path: PathBuf,
    timeouts: HarnessTimeouts,
    _xds_server: TrackedXdsServer,
    _stream_tx: mpsc::Sender<ServerAction>,
}

impl XdsEnabledHarness {
    pub async fn start() -> Result<Self, HarnessError> {
        Self::start_with_options(XdsHarnessOptions::default()).await
    }

    pub async fn start_with_options(options: XdsHarnessOptions) -> Result<Self, HarnessError> {
        let port_block = PortBlock::reserve()?;
        let xds_port = port_block.allocate()?;

        info!(xds_port, block_id = port_block.block_id(), "Starting xDS enabled harness");

        let (stream_tx, stream_rx) = mpsc::channel::<ServerAction>(128);
        let xds_addr = SocketAddr::from(([127, 0, 0, 1], xds_port));

        let xds_server = start_tracked_aggregate_server(xds_addr, stream_rx);
        let mut event_receiver = ServerEventReceiver::new(xds_server.event_rx.resubscribe());
        let pusher = ConfigPusher::new(xds_server.action_tx.clone());

        let bootstrap = BootstrapBuilder::new().xds("127.0.0.1", xds_port).log_level(&options.log_level);
        let config_path = bootstrap.build_to_temp()?;

        let orion = OrionInstance::spawn_no_listener(&config_path, options.orion_options).await?;

        event_receiver.wait_for_connection(options.timeouts.connection).await?;
        info!("Orion connected to xDS server");

        Ok(Self {
            port_block,
            pusher,
            orion,
            config_path,
            timeouts: options.timeouts,
            _xds_server: xds_server,
            _stream_tx: stream_tx,
        })
    }

    pub async fn push_cluster(&self, cluster: &Cluster) -> Result<(), HarnessError> {
        let result = self.pusher.push_cluster(cluster, self.timeouts.push).await?;
        Self::check_result(result)
    }

    pub async fn push_listener(&self, listener: &Listener) -> Result<(), HarnessError> {
        let result = self.pusher.push_listener(listener, self.timeouts.push).await?;
        Self::check_result(result)
    }

    pub async fn push_route_config(&self, route_config: &RouteConfig) -> Result<(), HarnessError> {
        let result = self.pusher.push_route_config(route_config, self.timeouts.push).await?;
        Self::check_result(result)
    }

    pub async fn push_endpoints(&self, cluster_name: &str, endpoints: &[Endpoint]) -> Result<(), HarnessError> {
        let result = self.pusher.push_endpoints(cluster_name, endpoints, self.timeouts.push).await?;
        Self::check_result(result)
    }

    pub async fn push_endpoints_with_priorities(
        &self,
        cluster_name: &str,
        priority_endpoints: &[(u32, Vec<Endpoint>)],
    ) -> Result<(), HarnessError> {
        let result =
            self.pusher.push_endpoints_with_priorities(cluster_name, priority_endpoints, self.timeouts.push).await?;
        Self::check_result(result)
    }

    pub async fn push_secret(&self, secret: &Secret) -> Result<(), HarnessError> {
        let result = self.pusher.push_secret(secret, self.timeouts.push).await?;
        Self::check_result(result)
    }

    #[must_use]
    pub fn pusher(&self) -> &ConfigPusher {
        &self.pusher
    }

    #[must_use]
    pub fn orion(&self) -> &OrionInstance {
        &self.orion
    }

    #[must_use]
    pub fn orion_mut(&mut self) -> &mut OrionInstance {
        &mut self.orion
    }

    #[must_use]
    pub fn config_path(&self) -> &PathBuf {
        &self.config_path
    }

    pub fn shutdown(self) {
        self.orion.shutdown();
    }

    pub fn allocate_listener_port(&self) -> Result<u16, HarnessError> {
        Ok(self.port_block.allocate()?)
    }

    #[must_use]
    pub fn port_block(&self) -> &PortBlock {
        &self.port_block
    }

    fn check_result(result: PushResult) -> Result<(), HarnessError> {
        match result {
            PushResult::Ack => Ok(()),
            PushResult::Nack { error_code, error_message } => Err(HarnessError::Nack { error_code, error_message }),
        }
    }
}
