// Copyright 2025 The kmesh Authors
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

use super::tls_configurator_builder::{WantsToBuildClient, WantsToBuildServer};
use crate::{
    secrets::{
        secrets_manager::CertStore,
        tls_configurator::tls_configurator_builder::{SecretHolder, TlsContextBuilder},
        CertificateSecret, TransportSecret,
    },
    Result, SecretManager,
};
use orion_configuration::config::{
    cluster::{TlsConfig as TlsClientConfig, TlsSecret},
    listener::TlsConfig as TlsServerConfig,
    secret::{TlsCertificate as TlsCertificateConfig, TrustChainVerification},
    transport::{CommonTlsValidationContext, Secrets, TlsVersion},
};
use rustls::{
    client::danger::ServerCertVerifier,
    crypto::KeyProvider,
    pki_types::{CertificateDer, PrivateKeyDer},
    version::{TLS12, TLS13},
    ClientConfig, RootCertStore, ServerConfig,
};
use rustls_platform_verifier::Verifier;
use smol_str::SmolStr;
use std::{borrow::Cow, collections::HashMap, result::Result as StdResult, sync::Arc as StdArc};
use str_utils::ToLowercase;
use tracing::{debug, info, warn};

pub fn get_crypto_key_provider() -> Result<&'static dyn KeyProvider> {
    rustls::crypto::CryptoProvider::get_default()
        .map(|p| p.key_provider)
        .ok_or("Unable to get rustls crypto provider".into())
}

#[allow(dead_code)]
#[derive(Debug)]
struct IgnoreCertVerifier(Verifier);

impl ServerCertVerifier for IgnoreCertVerifier {
    fn verify_server_cert(
        &self,
        _: &rustls::pki_types::CertificateDer<'_>,
        _: &[rustls::pki_types::CertificateDer<'_>],
        _: &rustls::pki_types::ServerName<'_>,
        _: &[u8],
        _: rustls::pki_types::UnixTime,
    ) -> StdResult<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> StdResult<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> StdResult<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}

#[derive(Debug, Clone)]
pub struct ClientCert {
    pub key: StdArc<PrivateKeyDer<'static>>,
    pub certs: StdArc<Vec<CertificateDer<'static>>>,
}

impl From<CertificateSecret> for ClientCert {
    fn from(secret: CertificateSecret) -> Self {
        let CertificateSecret { key, certs, .. } = secret;
        ClientCert { key, certs }
    }
}

impl TryFrom<TransportSecret> for StdArc<RootCertStore> {
    type Error = crate::Error;
    fn try_from(value: TransportSecret) -> Result<Self> {
        match value {
            TransportSecret::ValidationContext(context) => {
                let cert_store = StdArc::unwrap_or_clone(context);
                Ok(cert_store.into())
            },
            TransportSecret::Certificate(_) => {
                Err("TransportSecret certificate is not supported for root cert store".into())
            },
        }
    }
}

impl TryFrom<TransportSecret> for ServerCert {
    type Error = crate::Error;
    fn try_from(value: TransportSecret) -> Result<Self> {
        match value {
            TransportSecret::Certificate(certificate) => {
                let certificate = StdArc::unwrap_or_clone(certificate);
                ServerCert::try_from(certificate)
            },
            TransportSecret::ValidationContext(_) => {
                Err("TransportSecret ValidationContext is not supported for server certificate".into())
            },
        }
    }
}

impl TryFrom<TransportSecret> for ClientCert {
    type Error = crate::Error;
    fn try_from(value: TransportSecret) -> Result<Self> {
        match value {
            TransportSecret::Certificate(certificate) => {
                let certificate = StdArc::unwrap_or_clone(certificate);
                Ok(ClientCert::from(certificate))
            },
            TransportSecret::ValidationContext(_) => {
                Err("TransportSecret ValidationContext is not supported for client certificate".into())
            },
        }
    }
}

impl TryFrom<CertificateSecret> for ServerCert {
    type Error = crate::Error;
    fn try_from(secret: CertificateSecret) -> Result<Self> {
        let CertificateSecret { name, key, certs, .. } = secret;
        if let Some(name) = name {
            Ok(ServerCert { name, key: StdArc::new(key.clone_key()), certs })
        } else {
            Err("secret doesn't contain server name".into())
        }
    }
}

impl TryFrom<Vec<TlsCertificateConfig>> for ClientCert {
    type Error = crate::Error;

    fn try_from(mut tls_certificates: Vec<TlsCertificateConfig>) -> Result<Self> {
        if tls_certificates.len() > 1 {
            return Err("Only one client certificate should be configured".into());
        }
        let certificate = tls_certificates.remove(0);
        ClientCert::try_from(certificate)
    }
}

impl TryFrom<TlsCertificateConfig> for ClientCert {
    type Error = crate::Error;

    fn try_from(certificate: TlsCertificateConfig) -> Result<Self> {
        let secret = CertificateSecret::try_from(&certificate)?;
        Ok(ClientCert::from(secret))
    }
}

#[derive(Clone)]
pub struct ServerCert {
    pub name: SmolStr,
    pub key: StdArc<PrivateKeyDer<'static>>,
    pub certs: StdArc<Vec<CertificateDer<'static>>>,
}

impl std::fmt::Debug for ServerCert {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServerCert")
            .field("key", &"Secret")
            .field("certs", &self.certs)
            .field("name", &self.name)
            .finish()
    }
}

#[derive(Debug, Clone)]
pub struct TlsConfigurator<S: Clone, CtxType> {
    context_builder: TlsContextBuilder<CtxType>,
    config: StdArc<S>,
}

impl TlsConfigurator<ClientConfig, WantsToBuildClient> {
    pub fn update(self, secret_id: &str, secret: &TransportSecret) -> Result<Self> {
        let TlsContextBuilder { state } = self.context_builder;
        let WantsToBuildClient {
            supported_versions,
            validation_context_secret_id,
            certificate_store,
            certificate_secret_id,
            trust_chain_verification,
            client_certificate,
            sni,
        } = state;
        let new_builder = match secret {
            TransportSecret::Certificate(certificate) => {
                if Some(secret_id) == certificate_secret_id.as_deref() {
                    let client_cert: ClientCert = certificate.as_ref().to_owned().into();
                    TlsContextBuilder::with_supported_versions(supported_versions)
                        .with_client_certificate_store(validation_context_secret_id, certificate_store)
                        .with_client_certificate(certificate_secret_id, StdArc::new(client_cert))
                        .with_sni(sni)
                        .with_trust_chain_verification(trust_chain_verification)
                } else {
                    let msg = format!("Secret name doesn't match {secret_id}  {:?}", certificate_secret_id.as_deref());
                    warn!("{msg}");
                    return Err(msg.into());
                }
            },
            TransportSecret::ValidationContext(cert_store) => {
                if Some(secret_id) == validation_context_secret_id.as_deref() {
                    let cert_store = cert_store.as_ref().clone().into();
                    let builder = TlsContextBuilder::with_supported_versions(supported_versions)
                        .with_client_certificate_store(validation_context_secret_id, cert_store);
                    if let Some(client_certificate) = client_certificate {
                        builder.with_client_certificate(certificate_secret_id, client_certificate)
                    } else {
                        builder.with_no_client_auth()
                    }
                    .with_sni(sni)
                    .with_trust_chain_verification(trust_chain_verification)
                } else {
                    let msg = format!("Secret name doesn't match {secret_id} {validation_context_secret_id:?}");
                    warn!("{msg}");
                    return Err(msg.into());
                }
            },
        };

        let new_config = new_builder.build()?;
        Ok(TlsConfigurator { context_builder: new_builder, config: StdArc::new(new_config) })
    }
}

impl TlsConfigurator<ServerConfig, WantsToBuildServer> {
    pub fn update(self, secret_id: &str, secret: &TransportSecret) -> Result<Self> {
        let TlsContextBuilder { state } = self.context_builder;
        let WantsToBuildServer {
            supported_versions,
            validation_context_secret_id,
            certificate_store,
            mut server_ids_and_certificates,
            require_client_cert,
            ticketer,
        } = state;
        let new_builder = match secret {
            TransportSecret::Certificate(certificate) => {
                if let Some(secret) = server_ids_and_certificates.iter_mut().find(|s| s.name == secret_id) {
                    let server_cert: ServerCert = certificate.as_ref().to_owned().try_into()?;
                    secret.server_cert = server_cert;

                    let builder = TlsContextBuilder::with_supported_versions(supported_versions);

                    if let Some(certificate_store) = certificate_store {
                        builder.with_server_certificate_store(validation_context_secret_id, certificate_store)
                    } else {
                        builder.with_no_client_auth()
                    }
                    .with_certificates(server_ids_and_certificates)
                    .with_client_authentication(require_client_cert)?
                    .with_ticketer(ticketer)
                } else {
                    let msg = format!("Can't find secret {secret_id}");
                    debug!("{msg}");
                    return Err(msg.into());
                }
            },
            TransportSecret::ValidationContext(cert_store) => {
                if Some(secret_id) == validation_context_secret_id.as_deref() {
                    let cert_store = StdArc::clone(&cert_store.store);
                    TlsContextBuilder::with_supported_versions(supported_versions)
                        .with_server_certificate_store(validation_context_secret_id, cert_store)
                        .with_certificates(server_ids_and_certificates)
                        .with_client_authentication(require_client_cert)?
                        .with_ticketer(ticketer)
                } else {
                    let msg = format!("Can't find secret {secret_id} {validation_context_secret_id:?}");
                    debug!("{msg}");
                    return Err(msg.into());
                }
            },
        };

        let new_config = new_builder.build()?;
        Ok(TlsConfigurator { context_builder: new_builder, config: StdArc::new(new_config) })
    }
}

impl TryFrom<(TlsServerConfig, &SecretManager)> for TlsConfigurator<ServerConfig, WantsToBuildServer> {
    type Error = crate::Error;
    fn try_from((config, secret_manager): (TlsServerConfig, &SecretManager)) -> StdResult<Self, Self::Error> {
        let require_client_cert = config.require_client_certificate;
        let common_context = config.common_tls_context;
        let supported_versions = common_context
            .parameters
            .supported_version()
            .iter()
            .map(|version| match version {
                TlsVersion::TLSv1_2 => &TLS12,
                TlsVersion::TLSv1_3 => &TLS13,
            })
            .collect();
        debug!("DownstreamTlsContext : Selected TLS versions {supported_versions:?}");
        let (certificate_store_secret_id, certificate_store) =
            TlsConfigurator::create_certificate_store(secret_manager, common_context.validation_context)?;

        let certs_and_secret_ids = match common_context.secrets {
            Secrets::Certificates(certs) => {
                let mut certs_and_secret_ids = vec![];
                for certificate in certs {
                    certs_and_secret_ids.push(SecretHolder::new(
                        SmolStr::default(),
                        ServerCert::try_from(CertificateSecret::try_from(&certificate)?)?,
                    ));
                }
                certs_and_secret_ids
            },
            Secrets::SdsConfig(sds) => {
                let mut certs_and_secret_ids = vec![];
                for sds_config_name in sds {
                    if let Some(certificate) = secret_manager.get_certificate(&sds_config_name)? {
                        let server_cert: ServerCert = certificate.try_into()?;
                        let secret = SecretHolder::new(sds_config_name.clone(), server_cert);
                        if certs_and_secret_ids.contains(&secret) {
                            let msg = format!("DownstreamTlsContext : Duplicate secret name {}", &sds_config_name);
                            warn!("{msg}");
                            return Err(msg.into());
                        }
                        certs_and_secret_ids.push(secret);
                    }
                }
                certs_and_secret_ids
            },
        };

        let ctx_builder = TlsContextBuilder::with_supported_versions(supported_versions);
        let ctx_builder = if let Some(certificate_store) = certificate_store {
            ctx_builder.with_server_certificate_store(certificate_store_secret_id.map(SmolStr::into), certificate_store)
        } else {
            ctx_builder.with_no_client_auth()
        }
        .with_certificates(certs_and_secret_ids)
        .with_client_authentication(require_client_cert)?;

        let config = ctx_builder.build()?;

        Ok(TlsConfigurator::<ServerConfig, WantsToBuildServer> {
            context_builder: ctx_builder,
            config: StdArc::new(config),
        })
    }
}

impl TryFrom<(TlsClientConfig, &SecretManager)> for TlsConfigurator<ClientConfig, WantsToBuildClient> {
    type Error = crate::Error;

    fn try_from((context, secret_manager): (TlsClientConfig, &SecretManager)) -> StdResult<Self, Self::Error> {
        let sni = context.sni;
        let supported_versions = context
            .parameters
            .supported_version()
            .iter()
            .map(|version| match version {
                TlsVersion::TLSv1_2 => &TLS12,
                TlsVersion::TLSv1_3 => &TLS13,
            })
            .collect();
        debug!("DownstreamTlsContext : Selected TLS versions {supported_versions:?}");

        let trust_chain_verification = match context.validation_context.as_ref() {
            Some(CommonTlsValidationContext::ValidationContext(validation_context)) => {
                validation_context.trust_chain_verification()
            },
            _ => TrustChainVerification::default(),
        };

        let (certificate_store_secret_id, certificate_store) =
            TlsConfigurator::create_certificate_store(secret_manager, context.validation_context)?;

        let (secret_id, client_certificate) = match context.secret {
            Some(TlsSecret::Certificate(cert)) => (None, Some(ClientCert::try_from(cert)?)),
            Some(TlsSecret::SdsConfig(sds_config_name)) => {
                let cert = secret_manager.get_certificate(&sds_config_name)?.map(ClientCert::try_from).transpose()?;
                (Some(sds_config_name), cert)
            },
            None => (None, None),
        };

        let Some(certificate_store) = certificate_store else {
            return Err("UpstreamContext : no TLS validation options found".into());
        };
        let ctx_builder = TlsContextBuilder::with_supported_versions(supported_versions)
            .with_client_certificate_store(certificate_store_secret_id.map(SmolStr::into), certificate_store);

        let ctx_builder = if let Some(client_certificate) = client_certificate {
            ctx_builder.with_client_certificate(secret_id.map(SmolStr::into), StdArc::new(client_certificate))
        } else {
            ctx_builder.with_no_client_auth()
        }
        .with_sni(sni)
        .with_trust_chain_verification(trust_chain_verification);

        let config = ctx_builder.build()?;

        Ok(TlsConfigurator::<ClientConfig, WantsToBuildClient> {
            context_builder: ctx_builder,
            config: StdArc::new(config),
        })
    }
}

impl TlsConfigurator<(), ()> {
    /// Create a certificate store from the provided configuration options
    ///
    /// Returns an sds secret id and a TLS certificate store. If no validation context
    /// is configured returns Ok((None, None))
    fn create_certificate_store(
        secret_manager: &SecretManager,
        validation_options: Option<CommonTlsValidationContext>,
    ) -> Result<(Option<SmolStr>, Option<StdArc<RootCertStore>>)> {
        match validation_options {
            Some(CommonTlsValidationContext::ValidationContext(validation_context)) => {
                Ok((None, Some(CertStore::try_from(&validation_context)?.into())))
            },
            Some(CommonTlsValidationContext::SdsConfig(sds_config_name)) => {
                if let Some(cert_store) = secret_manager.get_validation_context(&sds_config_name)? {
                    Ok((Some(sds_config_name.clone()), Some(cert_store.try_into()?)))
                } else {
                    Ok((Some(sds_config_name.clone()), Some(StdArc::new(RootCertStore::empty()))))
                }
            },
            None => Ok((None, None)),
        }
    }
}

impl TlsConfigurator<ServerConfig, WantsToBuildServer> {
    #[inline]
    pub fn server_config(&self) -> StdArc<ServerConfig> {
        StdArc::clone(&self.config)
    }

    pub fn into_inner(self) -> ServerConfig {
        (*self.config).clone()
    }
}

impl TlsConfigurator<ClientConfig, WantsToBuildClient> {
    pub fn into_inner(self) -> ClientConfig {
        (*self.config).clone()
    }
    pub fn sni(&self) -> &str {
        self.context_builder.state.sni.as_str()
    }
}

/// More relaxed version of `ResolvesServerCertUsingSni`
/// Allowing `ServerName` instead of `DNSNames`

#[derive(Debug)]
pub struct RelaxedResolvesServerCertUsingSni {
    by_name: HashMap<String, StdArc<rustls::sign::CertifiedKey>, ahash::RandomState>,
    by_wildcard: HashMap<String, StdArc<rustls::sign::CertifiedKey>, ahash::RandomState>,
    default_cert: Option<StdArc<rustls::sign::CertifiedKey>>,
}

impl RelaxedResolvesServerCertUsingSni {
    pub fn new() -> Self {
        Self {
            by_name: HashMap::with_hasher(ahash::RandomState::default()),
            by_wildcard: HashMap::with_hasher(ahash::RandomState::default()),
            default_cert: None,
        }
    }

    pub fn add(&mut self, name: &str, ck: StdArc<rustls::sign::CertifiedKey>) -> StdResult<(), rustls::Error> {
        let name = name.to_ascii_lowercase_cow();
        // 1. Check if it's a wildcard early on
        let is_wildcard = name.starts_with("*.");
        let base_domain = name.strip_prefix("*.").unwrap_or(&name);

        // 2. Create a valid DNS name for rustls validation.
        // If it's a wildcard like "*.example.com", we test if the cert covers "dummy.example.com"
        let test_name_str = if is_wildcard { Cow::Owned(format!("dummy.{base_domain}")) } else { name.clone() };

        let server_name = rustls::pki_types::ServerName::try_from(test_name_str.as_ref())
            .map_err(|_e| rustls::Error::General("Bad Server/DNS name".into()))?;

        // 3. Sanity check: verify the certificate actually covers the domain/wildcard
        ck.end_entity_cert()
            .and_then(rustls::server::ParsedCertificate::try_from)
            .and_then(|c| rustls::client::verify_server_name(&c, &server_name))?;

        if self.default_cert.is_none() {
            self.default_cert = Some(StdArc::clone(&ck));
        }

        // 4. Insert into the correct map

        let cert_id = {
            use std::hash::{DefaultHasher, Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            ck.cert.hash(&mut hasher);
            hasher.finish()
        };

        info!(
            "ServerCert: adding certified key for server name {name} -> cert_id:{:x}, key:{:?}, ocsp:{:?}",
            cert_id, ck.key, ck.ocsp
        );
        if is_wildcard {
            self.by_wildcard.insert(base_domain.to_owned(), ck);
        } else {
            self.by_name.insert(name.into(), ck);
        }

        Ok(())
    }
}

impl rustls::server::ResolvesServerCert for RelaxedResolvesServerCertUsingSni {
    fn resolve(&self, client_hello: rustls::server::ClientHello) -> Option<StdArc<rustls::sign::CertifiedKey>> {
        debug!("Resolving certificate with server name: {:?}...", client_hello.server_name());

        if let Some(sni) = client_hello.server_name() {
            // 1. Exact match lookup
            if let Some(cert) = self.by_name.get(sni).cloned() {
                debug!("Matched full certificate: {cert:?}");
                return Some(cert);
            }

            // 2. Wildcard lookup with zero allocations
            if let Some((_, base_domain)) = sni.split_once('.') {
                if let Some(cert) = self.by_wildcard.get(base_domain).cloned() {
                    debug!("Matched with wildcard certificate: {cert:?}");
                    return Some(cert);
                }
            }
        }

        // Nicola: if sni is not specified in the client hello, or if it doesn't match any certificate,
        // including SAN with wildcards, we return the default certificate. This behavior is consistent
        // with envoy.
        // See full_scan_certs_on_sni_mismatch:
        // https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/transport_sockets/tls/v3/tls.proto#envoy-v3-api-field-extensions-transport-sockets-tls-v3-downstreamtlscontext-full-scan-certs-on-sni-mismatch
        //
        debug!("Matched with wildcard certificate: {:?}", self.default_cert);
        self.default_cert.clone()
    }
}
