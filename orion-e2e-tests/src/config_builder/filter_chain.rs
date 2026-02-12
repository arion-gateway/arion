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

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::listener::v3::{filter::ConfigType, Filter, FilterChain as EnvoyFilterChain},
        extensions::filters::network::http_connection_manager::v3::HttpConnectionManager,
    },
    google::protobuf::Any,
    prost::Message,
};

use super::hcm::Hcm;

#[derive(Debug, Clone)]
pub struct FilterChainBuilder {
    proto: EnvoyFilterChain,
}

impl FilterChainBuilder {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { proto: EnvoyFilterChain { name: name.into(), ..Default::default() } }
    }

    #[must_use]
    pub fn hcm(mut self, hcm: impl Into<Hcm>) -> Self {
        let hcm_proto: HttpConnectionManager = hcm.into();
        let hcm_any = Any {
            type_url:
                "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager"
                    .into(),
            value: hcm_proto.encode_to_vec(),
        };

        self.proto.filters.push(Filter {
            name: "envoy.filters.network.http_connection_manager".into(),
            config_type: Some(ConfigType::TypedConfig(hcm_any)),
            ..Default::default()
        });
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyFilterChain)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyFilterChain {
        self.proto
    }
}

impl From<FilterChainBuilder> for EnvoyFilterChain {
    fn from(builder: FilterChainBuilder) -> Self {
        builder.build()
    }
}

pub type FilterChain = EnvoyFilterChain;
