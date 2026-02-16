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

use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::tcp_proxy::v3::TcpProxy;

#[derive(Debug, Clone)]
pub struct TcpProxyBuilder {
    stat_prefix: String,
    cluster: String,
}

impl TcpProxyBuilder {
    #[must_use]
    pub fn new(stat_prefix: impl Into<String>) -> Self {
        Self { stat_prefix: stat_prefix.into(), cluster: String::new() }
    }

    #[must_use]
    pub fn cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = cluster.into();
        self
    }

    #[must_use]
    pub fn build(self) -> TcpProxy {
        use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::tcp_proxy::v3::tcp_proxy::ClusterSpecifier;

        TcpProxy {
            stat_prefix: self.stat_prefix,
            cluster_specifier: Some(ClusterSpecifier::Cluster(self.cluster)),
            ..Default::default()
        }
    }
}

impl From<TcpProxyBuilder> for TcpProxy {
    fn from(builder: TcpProxyBuilder) -> Self {
        builder.build()
    }
}
