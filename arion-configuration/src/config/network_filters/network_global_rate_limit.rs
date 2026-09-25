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

use arion_interner::InternedStr;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::config::network_filters::http_connection_manager::http_filters::ext_proc::GrpcService;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DescriptorEntry {
    pub key: SmolStr,
    pub value: SmolStr,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Descriptor {
    pub entries: Vec<DescriptorEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkGlobalRateLimit {
    pub stat_prefix: InternedStr,
    pub domain: Option<SmolStr>,
    pub grpc_service: GrpcService,
    #[serde(default)]
    pub failure_mode_deny: bool,
    #[serde(default)]
    pub descriptors: Vec<Descriptor>,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use super::{Descriptor, DescriptorEntry, NetworkGlobalRateLimit};
    use crate::config::network_filters::http_connection_manager::http_filters::ext_proc::{
        ClusterGrpc, GoogleGrpc, GrpcService, GrpcServiceSpecifier,
    };
    use crate::config::{common::*, core::RustType};
    use arion_data_plane_api::envoy_data_plane_api::envoy::{
        config::ratelimit::v3::RateLimitServiceConfig,
        extensions::filters::network::ratelimit::v3::RateLimit as EnvoyRateLimit,
    };
    use std::time::Duration;

    impl TryFrom<EnvoyRateLimit> for NetworkGlobalRateLimit {
        type Error = GenericError;
        fn try_from(value: EnvoyRateLimit) -> Result<Self, Self::Error> {
            let EnvoyRateLimit { stat_prefix, domain, descriptors, timeout, failure_mode_deny, rate_limit_service } =
                value;

            unsupported_field!(
                //stat_prefix,
                //domain,
                //failure_mode_deny,
                //rate_limit_service,
                //descriptors,
                timeout // timeout is already part of the grpc service configuration
            )?;

            let domain = (!domain.is_empty()).then(|| domain.into());
            let descriptors = required!(descriptors)?;
            let stat_prefix = required!(stat_prefix)?;

            let descriptors = descriptors
                .into_iter()
                .map(|d| Descriptor {
                    entries: d
                        .entries
                        .into_iter()
                        .map(|e| DescriptorEntry { key: e.key.into(), value: e.value.into() })
                        .collect(),
                })
                .collect();

            let rls_config = required!(rate_limit_service).with_node("rate_limit_service")?;
            let grpc_service = rls_config.try_into()?;

            Ok(Self { stat_prefix: stat_prefix.into(), domain, grpc_service, failure_mode_deny, descriptors })
        }
    }

    impl TryFrom<RateLimitServiceConfig> for GrpcService {
        type Error = GenericError;
        fn try_from(rls_config: RateLimitServiceConfig) -> Result<Self, Self::Error> {
            use arion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::grpc_service::TargetSpecifier;
            let inner = rls_config.grpc_service;
            let grpc_service = required!(inner).with_node("grpc_service")?;
            let timeout = grpc_service
                .timeout
                .map(|d| RustType::<Duration>::try_from(d).with_node("timeout").map(RustType::into_inner))
                .transpose()?;

            let target_specifier = grpc_service.target_specifier;
            let specifier = match required!(target_specifier).with_node("target_specifier")? {
                TargetSpecifier::EnvoyGrpc(envoy_grpc) => GrpcServiceSpecifier::Cluster(ClusterGrpc {
                    cluster_name: envoy_grpc.cluster_name.into(),
                    max_receive_message_length: None,
                }),
                TargetSpecifier::GoogleGrpc(google_grpc) => {
                    GrpcServiceSpecifier::GoogleGrpc(GoogleGrpc { target_uri: google_grpc.target_uri })
                },
            };

            Ok(GrpcService { service_specifier: specifier, timeout })
        }
    }
}
