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
        config::core::v3::{data_source::Specifier, DataSource},
        extensions::transport_sockets::tls::v3::{
            CommonTlsContext, DownstreamTlsContext as EnvoyDownstreamTlsContext, SdsSecretConfig, TlsCertificate,
            UpstreamTlsContext as EnvoyUpstreamTlsContext,
        },
    },
    google::protobuf::BoolValue,
};

#[derive(Debug, Clone, Default)]
pub struct DownstreamTlsBuilder {
    proto: EnvoyDownstreamTlsContext,
}

impl DownstreamTlsBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn cert_files(mut self, cert_chain: impl Into<String>, private_key: impl Into<String>) -> Self {
        let tls_cert = TlsCertificate {
            certificate_chain: Some(DataSource {
                specifier: Some(Specifier::Filename(cert_chain.into())),
                watched_directory: None,
            }),
            private_key: Some(DataSource {
                specifier: Some(Specifier::Filename(private_key.into())),
                watched_directory: None,
            }),
            ..Default::default()
        };
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            ctx.tls_certificates.push(tls_cert);
        }
        self
    }

    #[must_use]
    pub fn sds_secret(mut self, secret_name: impl Into<String>) -> Self {
        let sds_config = SdsSecretConfig { name: secret_name.into(), ..Default::default() };
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            ctx.tls_certificate_sds_secret_configs.push(sds_config);
        }
        self
    }

    #[must_use]
    pub fn require_client_cert(mut self) -> Self {
        self.proto.require_client_certificate = Some(BoolValue { value: true });
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyDownstreamTlsContext)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyDownstreamTlsContext {
        self.proto
    }

    fn ensure_common_tls_context(&mut self) {
        if self.proto.common_tls_context.is_none() {
            self.proto.common_tls_context = Some(CommonTlsContext::default());
        }
    }
}

impl From<DownstreamTlsBuilder> for EnvoyDownstreamTlsContext {
    fn from(builder: DownstreamTlsBuilder) -> Self {
        builder.build()
    }
}

pub type DownstreamTls = EnvoyDownstreamTlsContext;

#[derive(Debug, Clone, Default)]
pub struct UpstreamTlsBuilder {
    proto: EnvoyUpstreamTlsContext,
}

impl UpstreamTlsBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn sni(mut self, sni: impl Into<String>) -> Self {
        self.proto.sni = sni.into();
        self
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoyUpstreamTlsContext)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoyUpstreamTlsContext {
        self.proto
    }
}

impl From<UpstreamTlsBuilder> for EnvoyUpstreamTlsContext {
    fn from(builder: UpstreamTlsBuilder) -> Self {
        builder.build()
    }
}

pub type UpstreamTls = EnvoyUpstreamTlsContext;
