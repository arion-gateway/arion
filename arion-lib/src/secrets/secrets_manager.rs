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

use crate::Result;
use ahash::HashSet;
use arion_configuration::{
    config::{
        core::DataSource,
        secret::{Secret, TlsCertificate, Type, ValidationContext},
    },
    VerifySingleIter,
};
use chrono::{DateTime, Utc};
use rustc_hash::FxHashMap as HashMap;
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer},
    RootCertStore,
};
use rustls_pemfile::{certs, pkcs8_private_keys};
use serde::Serialize;
use smol_str::{format_smolstr, SmolStr, ToSmolStr};
use std::sync::Arc as StdArc;
use tracing::{debug, warn};
use webpki::types::ServerName;
use x509_parser::extensions::GeneralName;

#[derive(Clone, Debug)]
pub struct CertStore {
    pub store: StdArc<RootCertStore>,
    pub config: ValidationContext,
}

impl From<CertStore> for StdArc<RootCertStore> {
    fn from(value: CertStore) -> Self {
        value.store
    }
}

impl TryFrom<&ValidationContext> for CertStore {
    type Error = crate::Error;

    fn try_from(validation_context: &ValidationContext) -> Result<Self> {
        let mut ca_reader = validation_context.trusted_ca().into_buf_read()?;
        let mut root_store = rustls::RootCertStore::empty();
        let ca_certs = certs(&mut ca_reader)
            .map(|f| f.map_err(|e| format!("Can't parse certificate {e:?}").into()))
            .collect::<Result<Vec<_>>>()?;

        if ca_certs.is_empty() {
            return Err("No certificates have been configured".into());
        }

        let (good, bad) = root_store.add_parsable_certificates(ca_certs);
        debug!("Added certs {good} rejected certs {bad}");
        if bad > 0 {
            Err("Some certs in the trust store were invalid".into())
        } else {
            Ok(CertStore { store: StdArc::new(root_store), config: validation_context.clone() })
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SecretManager {
    certificate_secrets: HashMap<SmolStr, StdArc<CertificateSecret>>,
    validation_contexts: HashMap<SmolStr, StdArc<CertStore>>,
}

#[derive(Debug, Clone)]
pub struct CertificateSecret {
    pub name: Option<SmolStr>,
    pub key: StdArc<PrivateKeyDer<'static>>,
    pub certs: StdArc<Vec<CertificateDer<'static>>>,
    pub config: TlsCertificate,
}

#[derive(Debug, Clone)]
pub enum TransportSecret {
    Certificate(StdArc<CertificateSecret>),
    ValidationContext(StdArc<CertStore>),
}

impl TryFrom<&TlsCertificate> for CertificateSecret {
    type Error = crate::Error;

    fn try_from(certificate: &TlsCertificate) -> Result<Self> {
        let mut cert_reader = certificate.certificate_chain().into_buf_read()?;
        let mut key_reader = certificate.private_key().into_buf_read()?;
        let key = pkcs8_private_keys(&mut key_reader)
            .map(|f| f.map_err(|e| format!("Can't parse private key: {e}")))
            .verify_single()??;

        let certificates = certs(&mut cert_reader)
            .map(|f| f.map_err(|e| format!("Can't parse certificate {e:?}").into()))
            .collect::<Result<Vec<_>>>()?;

        let Some(cert) = certificates.first() else {
            return Err("No certificates have been configured".into());
        };

        let mut server_name = None;
        let (_, x509_cert) = x509_parser::parse_x509_certificate(cert)?;
        let subject = x509_cert.subject();
        if let Ok(Some(san)) = x509_cert.subject_alternative_name() {
            for san_name in &san.value.general_names {
                let GeneralName::DNSName(name) = *san_name else {
                    continue;
                };

                let is_server_name = ServerName::try_from(name).is_ok();
                debug!("Certificate SAN name {san_name} {name } is server name {is_server_name}");
                if is_server_name {
                    server_name = Some(name.to_smolstr());
                }
            }
        }
        let common_name = subject.iter_common_name().next().and_then(|cn| cn.as_str().ok());

        debug!("Certificate Subject's common name {common_name:?}");

        let key = StdArc::new(PrivateKeyDer::Pkcs8(key));
        Ok(CertificateSecret { name: server_name, key, certs: StdArc::new(certificates), config: certificate.clone() })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct SubjectAltName {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dns: Option<SmolStr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<SmolStr>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uri: Option<SmolStr>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CertDetails {
    pub path: String,
    pub serial_number: SmolStr,
    pub subject_alt_names: Vec<SubjectAltName>,
    pub days_until_expiration: SmolStr,
    pub valid_from: SmolStr,
    pub expiration_time: SmolStr,
}

#[derive(Debug, Clone, Serialize)]
pub struct CertInfo {
    pub ca_cert: Vec<CertDetails>,
    pub cert_chain: Vec<CertDetails>,
}

fn data_source_path(ds: &DataSource) -> &str {
    match ds {
        DataSource::Path(p) => p.as_str(),
        _ => "<inline>",
    }
}

fn parse_cert_details(der: &[u8], path: &str) -> Option<CertDetails> {
    let (_, x509) = x509_parser::parse_x509_certificate(der).ok()?;
    let serial_number = {
        use std::io::Write;
        let mut builder = smol_str::SmolStrBuilder::new();
        for b in x509.raw_serial() {
            let mut buf = [0u8; 4];
            // Create a mutable reference to the slice
            let mut slice = &mut buf[..];
            _ = write!(slice, "{b:02x}");
            // Extract exactly the 2 written bytes from the original buffer
            // and convert them to a string slice
            let hex_str = unsafe { std::str::from_utf8_unchecked(&buf[..2]) };
            builder.push_str(hex_str);
        }

        builder.finish()
    };
    let validity = x509.validity();
    let valid_from =
        DateTime::<Utc>::from_timestamp(validity.not_before.timestamp(), 0)?.format("%Y-%m-%dT%H:%M:%SZ").to_smolstr();
    let expiration_time =
        DateTime::<Utc>::from_timestamp(validity.not_after.timestamp(), 0)?.format("%Y-%m-%dT%H:%M:%SZ").to_smolstr();
    let days_until_expiration = ((validity.not_after.timestamp() - Utc::now().timestamp()) / 86400).max(0).to_smolstr();
    let subject_alt_names = x509
        .subject_alternative_name()
        .ok()
        .flatten()
        .map(|san| {
            san.value
                .general_names
                .iter()
                .filter_map(|name| match name {
                    GeneralName::DNSName(dns) => {
                        Some(SubjectAltName { dns: Some((*dns).to_smolstr()), ip_address: None, uri: None })
                    },
                    GeneralName::IPAddress(bytes) => {
                        let ip_str = if let [a, b, c, d] = bytes {
                            format_smolstr!("{a}.{b}.{c}.{d}")
                        } else {
                            let addr: [u8; 16] = (*bytes).try_into().ok()?;
                            std::net::Ipv6Addr::from(addr).to_smolstr()
                        };
                        Some(SubjectAltName { dns: None, ip_address: Some(ip_str), uri: None })
                    },
                    GeneralName::URI(uri) => {
                        Some(SubjectAltName { dns: None, ip_address: None, uri: Some((*uri).to_smolstr()) })
                    },
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    Some(CertDetails {
        path: path.to_owned(),
        serial_number,
        subject_alt_names,
        days_until_expiration,
        valid_from,
        expiration_time,
    })
}

impl SecretManager {
    pub fn new() -> Self {
        Self { certificate_secrets: HashMap::default(), validation_contexts: HashMap::default() }
    }

    pub fn add(&mut self, secret: &Secret) -> Result<TransportSecret> {
        let secret_id = secret.name();
        let secret = match secret.kind() {
            Type::TlsCertificate(certificate) => {
                let secret = StdArc::new(CertificateSecret::try_from(certificate)?);
                let _ = self.certificate_secrets.insert(secret_id.to_smolstr(), StdArc::clone(&secret));
                TransportSecret::Certificate(secret)
            },
            Type::ValidationContext(validation_context) => {
                let store = StdArc::new(CertStore::try_from(validation_context)?);
                let _ = self.validation_contexts.insert(secret_id.to_smolstr(), StdArc::clone(&store));
                TransportSecret::ValidationContext(store)
            },
        };
        Ok(secret)
    }
    pub fn remove(&mut self, secret_id: &str, secret_type: &Type) -> Result<()> {
        match secret_type {
            Type::TlsCertificate(_) => {
                let _ = self.certificate_secrets.remove(secret_id);
            },
            Type::ValidationContext(_) => {
                let _ = self.validation_contexts.remove(secret_id);
            },
        }
        Ok(())
    }

    pub fn get_certificate(&self, secret_id: &str) -> Result<Option<TransportSecret>> {
        let value = self.certificate_secrets.get(secret_id);
        if value.is_none() {
            warn!("SDS secret '{secret_id}' is missing");
        }
        Ok(value.map(|s| TransportSecret::Certificate(StdArc::clone(s))))
    }

    pub fn get_validation_context(&self, secret_id: &str) -> Result<Option<TransportSecret>> {
        let value = self.validation_contexts.get(secret_id);
        Ok(value.map(|s| TransportSecret::ValidationContext(StdArc::clone(s))))
    }
    pub fn get_all_secrets(&self) -> Vec<Secret> {
        let mut secrets = Vec::new();
        secrets.extend(
            self.certificate_secrets.iter().map(|(name, secret)| Secret {
                name: name.to_owned(),
                kind: Type::TlsCertificate(secret.config.clone()),
            }),
        );
        secrets.extend(self.validation_contexts.iter().map(|(name, secret)| Secret {
            name: name.to_owned(),
            kind: Type::ValidationContext(secret.config.clone()),
        }));
        secrets
    }

    pub fn get_certs_info(&self, cert_names: &HashSet<SmolStr>, ca_names: &HashSet<SmolStr>) -> Vec<CertInfo> {
        let mut result = Vec::new();
        for name in cert_names {
            let Some(cert_secret) = self.certificate_secrets.get(name.as_str()) else { continue };
            let path = data_source_path(cert_secret.config.certificate_chain());
            let cert_chain: Vec<CertDetails> =
                cert_secret.certs.iter().filter_map(|der| parse_cert_details(der.as_ref(), path)).collect();
            if !cert_chain.is_empty() {
                result.push(CertInfo { ca_cert: vec![], cert_chain });
            }
        }
        for name in ca_names {
            let Some(cert_store) = self.validation_contexts.get(name.as_str()) else { continue };
            let path = data_source_path(cert_store.config.trusted_ca());
            let Ok(mut reader) = cert_store.config.trusted_ca().into_buf_read() else { continue };
            let ca_cert: Vec<CertDetails> = certs(&mut reader)
                .filter_map(std::result::Result::ok)
                .filter_map(|der| parse_cert_details(der.as_ref(), path))
                .collect();
            if !ca_cert.is_empty() {
                result.push(CertInfo { ca_cert, cert_chain: vec![] });
            }
        }
        result
    }
}
