// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

use orion_data_plane_api::envoy_data_plane_api::prost_reflect::ReflectMessage;

#[derive(Debug, thiserror::Error)]
pub enum ProtoYamlError {
    #[error("Failed to serialize proto to JSON: {0}")]
    JsonSerialize(#[from] serde_json::Error),
    #[error("Failed to convert JSON to YAML: {0}")]
    YamlConvert(#[from] serde_yaml::Error),
}

pub fn proto_to_yaml_value<T: ReflectMessage>(proto: &T) -> Result<serde_yaml::Value, ProtoYamlError> {
    let dynamic = proto.transcode_to_dynamic();
    let json_value = serde_json::to_value(&dynamic)?;
    Ok(serde_yaml::to_value(&json_value)?)
}

pub fn proto_to_yaml_string<T: ReflectMessage>(proto: &T) -> Result<String, ProtoYamlError> {
    let yaml_value = proto_to_yaml_value(proto)?;
    Ok(serde_yaml::to_string(&yaml_value)?)
}
