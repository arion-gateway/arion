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

use orion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::core::v3::{
            async_data_source::Specifier as AsyncDataSourceSpecifier, data_source::Specifier as DataSourceSpecifier,
            AsyncDataSource, DataSource,
        },
        extensions::{
            filters::http::wasm::v3::Wasm as EnvoyWasm,
            wasm::v3::{plugin_config::Vm as EnvoyVm, PluginConfig as EnvoyPluginConfig, VmConfig as EnvoyVmConfig},
        },
    },
    google::protobuf::{Any, StringValue},
    prost::Message,
};

#[derive(Debug, Clone, Default)]
pub struct WasmBuilder {
    name: String,
    root_id: String,
    vm_id: String,
    configuration: Option<String>,
    code_filename: String,
}

impl WasmBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }

    #[must_use]
    pub fn root_id(mut self, root_id: impl Into<String>) -> Self {
        self.root_id = root_id.into();
        self
    }

    #[must_use]
    pub fn vm_id(mut self, vm_id: impl Into<String>) -> Self {
        self.vm_id = vm_id.into();
        self
    }

    #[must_use]
    pub fn configuration(mut self, configuration: impl Into<String>) -> Self {
        self.configuration = Some(configuration.into());
        self
    }

    #[must_use]
    pub fn code_filename(mut self, code_filename: impl Into<String>) -> Self {
        self.code_filename = code_filename.into();
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyWasm {
        let configuration = self.configuration.map(|val| {
            let string_val = StringValue { value: val };
            Any {
                type_url: "type.googleapis.com/google.protobuf.StringValue".into(),
                value: string_val.encode_to_vec(),
            }
        });

        let vm_config = EnvoyVmConfig {
            vm_id: self.vm_id,
            runtime: "envoy.wasm.runtime.v8".into(),
            code: Some(AsyncDataSource {
                specifier: Some(AsyncDataSourceSpecifier::Local(DataSource {
                    specifier: Some(DataSourceSpecifier::Filename(self.code_filename)),
                    ..Default::default()
                })),
            }),
            ..Default::default()
        };

        EnvoyWasm {
            config: Some(EnvoyPluginConfig {
                name: self.name,
                root_id: self.root_id,
                configuration,
                vm: Some(EnvoyVm::VmConfig(vm_config)),
                ..Default::default()
            }),
        }
    }
}

impl From<WasmBuilder> for EnvoyWasm {
    fn from(builder: WasmBuilder) -> Self {
        builder.build()
    }
}
