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
            certificate_validation_context::TrustChainVerification, common_tls_context::ValidationContextType,
            CertificateValidationContext, CommonTlsContext, DownstreamTlsContext as EnvoyDownstreamTlsContext,
            SdsSecretConfig, TlsCertificate, TlsParameters as EnvoyTlsParameters,
            UpstreamTlsContext as EnvoyUpstreamTlsContext,
        },
    },
    google::protobuf::BoolValue,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsVersion {
    Tls1_2,
    Tls1_3,
}

impl TlsVersion {
    fn to_envoy(self) -> i32 {
        match self {
            TlsVersion::Tls1_2 => 3, // TlsProtocol::TlsV1_2
            TlsVersion::Tls1_3 => 4, // TlsProtocol::TlsV1_3
        }
    }
}

fn make_file_data_source(path: impl Into<String>) -> DataSource {
    DataSource { specifier: Some(Specifier::Filename(path.into())), watched_directory: None }
}

fn make_validation_context(trusted_ca_path: impl Into<String>) -> CertificateValidationContext {
    CertificateValidationContext {
        trusted_ca: Some(make_file_data_source(trusted_ca_path)),
        trust_chain_verification: TrustChainVerification::VerifyTrustChain as i32,
        ..Default::default()
    }
}

fn make_validation_context_accept_untrusted(trusted_ca_path: impl Into<String>) -> CertificateValidationContext {
    CertificateValidationContext {
        trusted_ca: Some(make_file_data_source(trusted_ca_path)),
        trust_chain_verification: TrustChainVerification::AcceptUntrusted as i32,
        ..Default::default()
    }
}

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
            certificate_chain: Some(make_file_data_source(cert_chain)),
            private_key: Some(make_file_data_source(private_key)),
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
    pub fn validation_context(mut self, trusted_ca_path: impl Into<String>) -> Self {
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            ctx.validation_context_type =
                Some(ValidationContextType::ValidationContext(make_validation_context(trusted_ca_path)));
        }
        self
    }

    #[must_use]
    pub fn validation_context_sds(mut self, secret_name: impl Into<String>) -> Self {
        let sds_config = SdsSecretConfig { name: secret_name.into(), ..Default::default() };
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            ctx.validation_context_type = Some(ValidationContextType::ValidationContextSdsSecretConfig(sds_config));
        }
        self
    }

    #[must_use]
    pub fn tls_minimum_version(mut self, version: TlsVersion) -> Self {
        self.ensure_tls_params();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            if let Some(ref mut params) = ctx.tls_params {
                params.tls_minimum_protocol_version = version.to_envoy();
            }
        }
        self
    }

    #[must_use]
    pub fn tls_maximum_version(mut self, version: TlsVersion) -> Self {
        self.ensure_tls_params();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            if let Some(ref mut params) = ctx.tls_params {
                params.tls_maximum_protocol_version = version.to_envoy();
            }
        }
        self
    }

    #[must_use]
    pub fn tls_1_2_only(self) -> Self {
        self.tls_minimum_version(TlsVersion::Tls1_2).tls_maximum_version(TlsVersion::Tls1_2)
    }

    #[must_use]
    pub fn tls_1_3_only(self) -> Self {
        self.tls_minimum_version(TlsVersion::Tls1_3).tls_maximum_version(TlsVersion::Tls1_3)
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

    fn ensure_tls_params(&mut self) {
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            if ctx.tls_params.is_none() {
                ctx.tls_params = Some(EnvoyTlsParameters::default());
            }
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
    pub fn client_cert_files(mut self, cert_chain: impl Into<String>, private_key: impl Into<String>) -> Self {
        let tls_cert = TlsCertificate {
            certificate_chain: Some(make_file_data_source(cert_chain)),
            private_key: Some(make_file_data_source(private_key)),
            ..Default::default()
        };
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            ctx.tls_certificates.push(tls_cert);
        }
        self
    }

    #[must_use]
    pub fn client_cert_sds(mut self, secret_name: impl Into<String>) -> Self {
        let sds_config = SdsSecretConfig { name: secret_name.into(), ..Default::default() };
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            ctx.tls_certificate_sds_secret_configs.push(sds_config);
        }
        self
    }

    #[must_use]
    pub fn validation_context(mut self, trusted_ca_path: impl Into<String>) -> Self {
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            ctx.validation_context_type =
                Some(ValidationContextType::ValidationContext(make_validation_context(trusted_ca_path)));
        }
        self
    }

    #[must_use]
    pub fn validation_context_sds(mut self, secret_name: impl Into<String>) -> Self {
        let sds_config = SdsSecretConfig { name: secret_name.into(), ..Default::default() };
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            ctx.validation_context_type = Some(ValidationContextType::ValidationContextSdsSecretConfig(sds_config));
        }
        self
    }

    #[must_use]
    pub fn skip_server_verification(mut self, sni: impl Into<String>, trusted_ca_path: impl Into<String>) -> Self {
        self.proto.sni = sni.into();
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            ctx.validation_context_type = Some(ValidationContextType::ValidationContext(
                make_validation_context_accept_untrusted(trusted_ca_path),
            ));
        }
        self
    }

    #[must_use]
    pub fn tls_minimum_version(mut self, version: TlsVersion) -> Self {
        self.ensure_tls_params();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            if let Some(ref mut params) = ctx.tls_params {
                params.tls_minimum_protocol_version = version.to_envoy();
            }
        }
        self
    }

    #[must_use]
    pub fn tls_maximum_version(mut self, version: TlsVersion) -> Self {
        self.ensure_tls_params();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            if let Some(ref mut params) = ctx.tls_params {
                params.tls_maximum_protocol_version = version.to_envoy();
            }
        }
        self
    }

    #[must_use]
    pub fn tls_1_2_only(self) -> Self {
        self.tls_minimum_version(TlsVersion::Tls1_2).tls_maximum_version(TlsVersion::Tls1_2)
    }

    #[must_use]
    pub fn tls_1_3_only(self) -> Self {
        self.tls_minimum_version(TlsVersion::Tls1_3).tls_maximum_version(TlsVersion::Tls1_3)
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

    fn ensure_common_tls_context(&mut self) {
        if self.proto.common_tls_context.is_none() {
            self.proto.common_tls_context = Some(CommonTlsContext::default());
        }
    }

    fn ensure_tls_params(&mut self) {
        self.ensure_common_tls_context();
        if let Some(ref mut ctx) = self.proto.common_tls_context {
            if ctx.tls_params.is_none() {
                ctx.tls_params = Some(EnvoyTlsParameters::default());
            }
        }
    }
}

impl From<UpstreamTlsBuilder> for EnvoyUpstreamTlsContext {
    fn from(builder: UpstreamTlsBuilder) -> Self {
        builder.build()
    }
}

pub type UpstreamTls = EnvoyUpstreamTlsContext;
