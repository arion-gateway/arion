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

use arion_data_plane_api::envoy_data_plane_api::{
    envoy::{
        config::accesslog::v3::{access_log::ConfigType as AccessLogConfigType, AccessLog as EnvoyAccessLog},
        config::core::v3::{substitution_format_string::Format as SubstitutionFormat, SubstitutionFormatString},
        extensions::{
            access_loggers::file::v3::{
                file_access_log::AccessLogFormat as FileAccessLogFormat, FileAccessLog as EnvoyFileAccessLog,
            },
            filters::network::tcp_proxy::v3::TcpProxy,
        },
    },
    google::protobuf::Any,
    prost::Message,
};

#[derive(Debug, Clone)]
pub struct TcpProxyBuilder {
    stat_prefix: String,
    cluster: String,
    access_logs: Vec<EnvoyAccessLog>,
}

impl TcpProxyBuilder {
    #[must_use]
    pub fn new(stat_prefix: impl Into<String>) -> Self {
        Self { stat_prefix: stat_prefix.into(), cluster: String::new(), access_logs: Vec::new() }
    }

    #[must_use]
    pub fn cluster(mut self, cluster: impl Into<String>) -> Self {
        self.cluster = cluster.into();
        self
    }

    #[must_use]
    #[allow(deprecated)]
    pub fn access_log_file(mut self, path: impl Into<String>, text_format: impl Into<String>) -> Self {
        let file_access_log = EnvoyFileAccessLog {
            path: path.into(),
            access_log_format: Some(FileAccessLogFormat::LogFormat(SubstitutionFormatString {
                format: Some(SubstitutionFormat::TextFormat(text_format.into())),
                ..Default::default()
            })),
        };

        let typed_config = Any {
            type_url: "type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog".into(),
            value: file_access_log.encode_to_vec(),
        };

        let access_log = EnvoyAccessLog {
            name: "envoy.access_loggers.file".into(),
            config_type: Some(AccessLogConfigType::TypedConfig(typed_config)),
            ..Default::default()
        };

        self.access_logs.push(access_log);
        self
    }

    #[must_use]
    pub fn build(self) -> TcpProxy {
        use arion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::tcp_proxy::v3::tcp_proxy::ClusterSpecifier;

        TcpProxy {
            stat_prefix: self.stat_prefix,
            cluster_specifier: Some(ClusterSpecifier::Cluster(self.cluster)),
            access_log: self.access_logs,
            ..Default::default()
        }
    }
}

impl From<TcpProxyBuilder> for TcpProxy {
    fn from(builder: TcpProxyBuilder) -> Self {
        builder.build()
    }
}
