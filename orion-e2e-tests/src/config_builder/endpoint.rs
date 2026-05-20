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

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::config::{
        core::v3::{
            address::Address as AddressType, envoy_internal_address::AddressNameSpecifier,
            socket_address::PortSpecifier, Address, EnvoyInternalAddress, HealthStatus as ProtoHealthStatus,
            SocketAddress,
        },
        endpoint::v3::{lb_endpoint::HostIdentifier, Endpoint as EnvoyEndpoint, LbEndpoint},
    },
    google::protobuf::UInt32Value,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HealthStatus {
    #[default]
    Healthy,
    Unhealthy,
    Draining,
    Timeout,
    Degraded,
}

impl HealthStatus {
    fn to_proto(self) -> i32 {
        match self {
            Self::Healthy => ProtoHealthStatus::Healthy.into(),
            Self::Unhealthy => ProtoHealthStatus::Unhealthy.into(),
            Self::Draining => ProtoHealthStatus::Draining.into(),
            Self::Timeout => ProtoHealthStatus::Timeout.into(),
            Self::Degraded => ProtoHealthStatus::Degraded.into(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EndpointBuilder {
    proto: LbEndpoint,
}

impl EndpointBuilder {
    #[must_use]
    pub fn new(address: impl Into<String>, port: u16) -> Self {
        let socket_address = SocketAddress {
            address: address.into(),
            port_specifier: Some(PortSpecifier::PortValue(u32::from(port))),
            ..Default::default()
        };

        let address = Address {
            address: Some(
                orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::address::Address::SocketAddress(
                    socket_address,
                ),
            ),
        };

        let endpoint = EnvoyEndpoint { address: Some(address), ..Default::default() };

        Self { proto: LbEndpoint { host_identifier: Some(HostIdentifier::Endpoint(endpoint)), ..Default::default() } }
    }

    #[must_use]
    pub fn from_socket_addr(addr: SocketAddr) -> Self {
        Self::new(addr.ip().to_string(), addr.port())
    }

    #[must_use]
    pub fn internal(listener_name: impl Into<String>) -> Self {
        let internal = EnvoyInternalAddress {
            endpoint_id: String::new(),
            address_name_specifier: Some(AddressNameSpecifier::ServerListenerName(listener_name.into())),
        };
        let address = Address { address: Some(AddressType::EnvoyInternalAddress(internal)) };
        let endpoint = EnvoyEndpoint { address: Some(address), ..Default::default() };
        Self { proto: LbEndpoint { host_identifier: Some(HostIdentifier::Endpoint(endpoint)), ..Default::default() } }
    }

    #[must_use]
    pub fn weight(mut self, weight: u32) -> Self {
        self.proto.load_balancing_weight = Some(UInt32Value { value: weight.max(1) });
        self
    }

    #[must_use]
    pub fn health_status(mut self, status: HealthStatus) -> Self {
        self.proto.health_status = status.to_proto();
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut LbEndpoint)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> LbEndpoint {
        self.proto
    }
}

impl From<SocketAddr> for EndpointBuilder {
    fn from(addr: SocketAddr) -> Self {
        Self::from_socket_addr(addr)
    }
}

impl From<(String, u16)> for EndpointBuilder {
    fn from((address, port): (String, u16)) -> Self {
        Self::new(address, port)
    }
}

impl From<(&str, u16)> for EndpointBuilder {
    fn from((address, port): (&str, u16)) -> Self {
        Self::new(address, port)
    }
}

impl From<EndpointBuilder> for LbEndpoint {
    fn from(builder: EndpointBuilder) -> Self {
        builder.build()
    }
}

pub type Endpoint = LbEndpoint;
