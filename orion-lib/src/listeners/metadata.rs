// Copyright 2025 The kmesh Authors
//
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
//
//

use std::{collections::HashMap, net::SocketAddr, sync::Arc};

use orion_configuration::config::common::TlvType;
use smol_str::SmolStr;

use crate::utils::instrumented_stream::StreamMetrics;

#[derive(Debug, Clone)]
pub enum DownstreamConnectionMetadata {
    FromSocket {
        peer_address: SocketAddr,
        local_address: SocketAddr,
    },
    FromProxyProtocol {
        original_peer_address: SocketAddr,
        original_destination_address: SocketAddr,
        protocol: ppp::v2::Protocol,
        tlv_data: HashMap<TlvType, Vec<u8>>,
        proxy_peer_address: SocketAddr,
        proxy_local_address: SocketAddr,
    },
}

impl DownstreamConnectionMetadata {
    pub fn peer_address(&self) -> SocketAddr {
        match self {
            Self::FromSocket { peer_address, .. } => *peer_address,
            Self::FromProxyProtocol { original_peer_address, .. } => *original_peer_address,
        }
    }
    pub fn local_address(&self) -> SocketAddr {
        match self {
            Self::FromSocket { local_address, .. } => *local_address,
            Self::FromProxyProtocol { original_destination_address, .. } => *original_destination_address,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DownstreamMetadata {
    pub connection: DownstreamConnectionMetadata,
    pub sni: Option<SmolStr>,
    pub listener_name: &'static str,
}

impl DownstreamMetadata {
    #[inline]
    pub fn new<S>(connection: DownstreamConnectionMetadata, sni: Option<S>, listener_name: &'static str) -> Self
    where
        S: Into<SmolStr>,
    {
        Self { connection, sni: sni.map(Into::into), listener_name }
    }
}

/// Connection/stream-scoped metadata shared for the lifetime of a downstream stream.
#[derive(Debug, Clone)]
pub struct ConnMeta {
    pub downstream: Arc<DownstreamMetadata>,
    pub stream_metrics: Arc<StreamMetrics>,
}

impl ConnMeta {
    #[inline]
    pub fn new(downstream: Arc<DownstreamMetadata>, stream_metrics: Arc<StreamMetrics>) -> Self {
        Self { downstream, stream_metrics }
    }

    #[inline]
    pub fn downstream_peer_address(&self) -> SocketAddr {
        self.downstream.connection.peer_address()
    }

    #[inline]
    pub fn downstream_local_address(&self) -> SocketAddr {
        self.downstream.connection.local_address()
    }

    #[inline]
    pub fn listener_name(&self) -> &'static str {
        self.downstream.listener_name
    }

    /// Socket addresses for access-log / header formatters.
    #[inline]
    pub fn downstream_socket_addr_context(&self) -> orion_format::context::SocketAddrContext {
        orion_format::context::SocketAddrContext {
            downstream_local_addr: Some(self.downstream_local_address()),
            downstream_peer_addr: Some(self.downstream_peer_address()),
            upstream_local_addr: None,
            upstream_peer_addr: None,
        }
    }
}

impl Default for ConnMeta {
    fn default() -> Self {
        let unspecified = SocketAddr::new(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED), 0);
        let downstream = Arc::new(DownstreamMetadata::new(
            DownstreamConnectionMetadata::FromSocket { peer_address: unspecified, local_address: unspecified },
            None::<&str>,
            "synthetic",
        ));
        Self::new(downstream, Arc::new(StreamMetrics::default()))
    }
}
