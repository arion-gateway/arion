// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

#![allow(deprecated)]

use crate::config::{access_log::AccessLog, cluster::ClusterSpecifier};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct TcpProxy {
    pub cluster_specifier: ClusterSpecifier,
    #[serde(skip_serializing_if = "Vec::is_empty", default = "Default::default")]
    pub access_log: Vec<AccessLog>,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    #![allow(deprecated)]
    use super::TcpProxy;
    use crate::config::{access_log::AccessLog, common::*};
    use arion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::tcp_proxy::v3::TcpProxy as EnvoyTcpProxy;

    impl TryFrom<EnvoyTcpProxy> for TcpProxy {
        type Error = GenericError;
        fn try_from(value: EnvoyTcpProxy) -> Result<Self, Self::Error> {
            let EnvoyTcpProxy {
                stat_prefix,
                on_demand,
                metadata_match,
                idle_timeout,
                downstream_idle_timeout,
                upstream_idle_timeout,
                access_log,
                max_connect_attempts,
                hash_policy,
                tunneling_config,
                max_downstream_connection_duration,
                access_log_flush_interval,
                flush_access_log_on_connected,
                access_log_options,
                cluster_specifier,
                backoff_options,
                proxy_protocol_tlvs,
                max_downstream_connection_duration_jitter_percentage,
                upstream_connect_mode,
                max_early_data_bytes,
            } = value;
            unsupported_field!(
                // stat_prefix,
                on_demand,
                metadata_match,
                idle_timeout,
                downstream_idle_timeout,
                upstream_idle_timeout,
                // access_log,
                max_connect_attempts,
                hash_policy,
                tunneling_config,
                max_downstream_connection_duration,
                access_log_flush_interval,
                flush_access_log_on_connected,
                access_log_options,
                backoff_options,
                // cluster_specifier,
                proxy_protocol_tlvs,
                max_downstream_connection_duration_jitter_percentage,
                upstream_connect_mode,
                max_early_data_bytes
            )?;
            if stat_prefix.is_used() {
                tracing::warn!("unsupported field stat_prefix used in tcp_proxy. This field will be ignored.");
            }
            let cluster_specifier = convert_opt!(cluster_specifier)?;

            let access_log =
                access_log.iter().map(|al| AccessLog::try_from(al.clone())).collect::<Result<Vec<_>, _>>()?;

            Ok(Self { cluster_specifier, access_log })
        }
    }
}
