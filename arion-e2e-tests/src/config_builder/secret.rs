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

use std::path::Path;

use arion_data_plane_api::envoy_data_plane_api::envoy::{
    config::core::v3::{data_source::Specifier, DataSource},
    extensions::transport_sockets::tls::v3::{
        secret::Type as SecretType, CertificateValidationContext, Secret as EnvoySecret, TlsCertificate,
    },
};

#[derive(Debug, Clone)]
pub struct SecretBuilder {
    proto: EnvoySecret,
}

impl SecretBuilder {
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { proto: EnvoySecret { name: name.into(), ..Default::default() } }
    }

    #[must_use]
    pub fn tls_certificate(mut self, certificate_chain: Vec<u8>, private_key: Vec<u8>) -> Self {
        let tls_cert = TlsCertificate {
            certificate_chain: Some(DataSource {
                specifier: Some(Specifier::InlineBytes(certificate_chain)),
                watched_directory: None,
            }),
            private_key: Some(DataSource {
                specifier: Some(Specifier::InlineBytes(private_key)),
                watched_directory: None,
            }),
            ..Default::default()
        };
        self.proto.r#type = Some(SecretType::TlsCertificate(tls_cert));
        self
    }

    pub fn tls_certificate_files(
        self,
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
    ) -> std::io::Result<Self> {
        let cert_bytes = std::fs::read(cert_path)?;
        let key_bytes = std::fs::read(key_path)?;
        Ok(self.tls_certificate(cert_bytes, key_bytes))
    }

    #[must_use]
    pub fn validation_context(mut self, trusted_ca: Vec<u8>) -> Self {
        let validation_ctx = CertificateValidationContext {
            trusted_ca: Some(DataSource {
                specifier: Some(Specifier::InlineBytes(trusted_ca)),
                watched_directory: None,
            }),
            ..Default::default()
        };
        self.proto.r#type = Some(SecretType::ValidationContext(validation_ctx));
        self
    }

    pub fn validation_context_file(self, ca_path: impl AsRef<Path>) -> std::io::Result<Self> {
        let ca_bytes = std::fs::read(ca_path)?;
        Ok(self.validation_context(ca_bytes))
    }

    #[must_use]
    pub fn with_proto<F: FnOnce(&mut EnvoySecret)>(mut self, f: F) -> Self {
        f(&mut self.proto);
        self
    }

    #[must_use]
    pub fn build(self) -> EnvoySecret {
        self.proto
    }
}

impl From<SecretBuilder> for EnvoySecret {
    fn from(builder: SecretBuilder) -> Self {
        builder.build()
    }
}

pub type Secret = EnvoySecret;
