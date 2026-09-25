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

use std::sync::Arc as StdArc;

use orion_configuration::config::secret::TrustChainVerification;
use rustls::{
    client::WebPkiServerVerifier,
    crypto::aws_lc_rs::Ticketer,
    server::{NoServerSessionStorage, ProducesTickets, WebPkiClientVerifier},
    sign::CertifiedKey,
    ClientConfig, RootCertStore, ServerConfig, SupportedProtocolVersion,
};
use smol_str::{SmolStr, ToSmolStr};
use tracing::{debug, warn};
use x509_parser::prelude::{FromDer, GeneralName, X509Certificate};

use super::configurator::{get_crypto_key_provider, ClientCert, RelaxedResolvesServerCertUsingSni, ServerCert};

#[derive(Debug, Clone)]
pub struct WantsCertStore {
    pub supported_versions: Vec<&'static SupportedProtocolVersion>,
}

#[derive(Debug, Clone)]
pub struct WantsServerCert {
    supported_versions: Vec<&'static SupportedProtocolVersion>,
    validation_context_secret_id: Option<String>,
    certificate_store: Option<StdArc<RootCertStore>>,
}

#[derive(Debug, Clone)]
pub struct WantsClientCert {
    supported_versions: Vec<&'static SupportedProtocolVersion>,
    validation_context_secret_id: Option<String>,
    certificate_store: StdArc<RootCertStore>,
}

#[derive(Debug, Clone)]
pub struct SecretHolder {
    pub name: SmolStr,
    pub server_cert: ServerCert,
}

impl PartialEq for SecretHolder {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for SecretHolder {}

impl PartialOrd for SecretHolder {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SecretHolder {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.name.cmp(&other.name)
    }
}
impl SecretHolder {
    pub fn new(name: SmolStr, server_cert: ServerCert) -> Self {
        Self { name, server_cert }
    }
}

#[derive(Debug, Clone)]
pub struct WantsToBuildServer {
    pub supported_versions: Vec<&'static SupportedProtocolVersion>,
    pub validation_context_secret_id: Option<String>,
    pub certificate_store: Option<StdArc<RootCertStore>>,
    pub server_ids_and_certificates: Vec<SecretHolder>,
    pub require_client_cert: bool,
    /// Encrypts and decrypts session tickets, making TLS resumption stateless.
    ///
    /// Held in the builder state rather than created in `build`, so that every `ServerConfig`
    /// derived from this builder shares one set of ticket keys. Rebuilds happen independently in
    /// each proxy runtime on SDS updates, and a fresh ticketer per rebuild would leave the runtimes
    /// unable to decrypt each other's tickets.
    pub ticketer: StdArc<dyn ProducesTickets>,
}

#[derive(Debug, Clone)]
pub struct WantsToVerifyClientCert {
    supported_versions: Vec<&'static SupportedProtocolVersion>,
    validation_context_secret_id: Option<String>,
    certificate_store: Option<StdArc<RootCertStore>>,
    server_ids_and_certificates: Vec<SecretHolder>,
}

#[derive(Debug, Clone)]
pub struct WantsSni {
    supported_versions: Vec<&'static SupportedProtocolVersion>,
    validation_context_secret_id: Option<String>,
    certificate_store: StdArc<RootCertStore>,
    certificate_secret_id: Option<String>,
    client_certificate: Option<StdArc<ClientCert>>,
}

#[derive(Debug, Clone)]
pub struct WantsToBuildClient {
    pub supported_versions: Vec<&'static SupportedProtocolVersion>,
    pub validation_context_secret_id: Option<String>,
    pub certificate_store: StdArc<RootCertStore>,
    pub certificate_secret_id: Option<String>,
    pub trust_chain_verification: TrustChainVerification,
    pub client_certificate: Option<StdArc<ClientCert>>,
    pub sni: String,
}

#[derive(Debug, Clone)]
pub struct TlsContextBuilder<S> {
    pub state: S,
}

use crate::{secrets::no_cert_verification::NoCertificateVerification, Result};

impl TlsContextBuilder<()> {
    pub fn with_supported_versions(
        supported_versions: Vec<&'static SupportedProtocolVersion>,
    ) -> TlsContextBuilder<WantsCertStore> {
        TlsContextBuilder { state: WantsCertStore { supported_versions } }
    }
}

impl TlsContextBuilder<WantsCertStore> {
    pub fn with_server_certificate_store(
        self,
        secret_id: Option<String>,
        certificate_store: StdArc<RootCertStore>,
    ) -> TlsContextBuilder<WantsServerCert> {
        let state = WantsServerCert {
            supported_versions: self.state.supported_versions,
            validation_context_secret_id: secret_id,
            certificate_store: Some(certificate_store),
        };
        TlsContextBuilder { state }
    }

    pub fn with_no_client_auth(self) -> TlsContextBuilder<WantsServerCert> {
        let state = WantsServerCert {
            supported_versions: self.state.supported_versions,
            validation_context_secret_id: None,
            certificate_store: None,
        };
        TlsContextBuilder { state }
    }

    pub fn with_client_certificate_store(
        self,
        secret_id: Option<String>,
        certificate_store: StdArc<RootCertStore>,
    ) -> TlsContextBuilder<WantsClientCert> {
        let state = WantsClientCert {
            supported_versions: self.state.supported_versions,
            validation_context_secret_id: secret_id,
            certificate_store,
        };
        TlsContextBuilder { state }
    }
}

impl TlsContextBuilder<WantsServerCert> {
    pub fn with_certificates(
        self,
        server_ids_and_certificates: Vec<SecretHolder>,
    ) -> TlsContextBuilder<WantsToVerifyClientCert> {
        TlsContextBuilder {
            state: WantsToVerifyClientCert {
                supported_versions: self.state.supported_versions,
                validation_context_secret_id: self.state.validation_context_secret_id,
                certificate_store: self.state.certificate_store,
                server_ids_and_certificates,
            },
        }
    }
}

impl TlsContextBuilder<WantsToVerifyClientCert> {
    pub fn with_client_authentication(
        self,
        require_client_cert: bool,
    ) -> Result<TlsContextBuilder<WantsToBuildServer>> {
        Ok(TlsContextBuilder {
            state: WantsToBuildServer {
                supported_versions: self.state.supported_versions,
                validation_context_secret_id: self.state.validation_context_secret_id,
                certificate_store: self.state.certificate_store,
                server_ids_and_certificates: self.state.server_ids_and_certificates,
                require_client_cert,
                ticketer: Ticketer::new()?,
            },
        })
    }
}

impl TlsContextBuilder<WantsClientCert> {
    pub fn with_client_certificate(
        self,
        secret_id: Option<String>,
        client_certificate: StdArc<ClientCert>,
    ) -> TlsContextBuilder<WantsSni> {
        TlsContextBuilder {
            state: WantsSni {
                supported_versions: self.state.supported_versions,
                certificate_store: self.state.certificate_store,
                validation_context_secret_id: self.state.validation_context_secret_id,
                client_certificate: Some(client_certificate),
                certificate_secret_id: secret_id,
            },
        }
    }
    pub fn with_no_client_auth(self) -> TlsContextBuilder<WantsSni> {
        TlsContextBuilder {
            state: WantsSni {
                supported_versions: self.state.supported_versions,
                certificate_store: self.state.certificate_store,
                validation_context_secret_id: self.state.validation_context_secret_id,
                client_certificate: None,
                certificate_secret_id: None,
            },
        }
    }
}

impl TlsContextBuilder<WantsSni> {
    pub fn with_sni(self, sni: String) -> TlsContextBuilder<WantsToBuildClient> {
        TlsContextBuilder {
            state: WantsToBuildClient {
                supported_versions: self.state.supported_versions,
                certificate_store: self.state.certificate_store,
                validation_context_secret_id: self.state.validation_context_secret_id,
                client_certificate: self.state.client_certificate,
                certificate_secret_id: self.state.certificate_secret_id,
                trust_chain_verification: TrustChainVerification::default(),
                sni,
            },
        }
    }
}

impl TlsContextBuilder<WantsToBuildServer> {
    /// Keep the ticket keys of a previous builder, so that configs rebuilt after an SDS update can
    /// still decrypt tickets issued before it, and by the other runtimes.
    pub fn with_ticketer(mut self, ticketer: StdArc<dyn ProducesTickets>) -> Self {
        self.state.ticketer = ticketer;
        self
    }

    pub fn build(&self) -> Result<ServerConfig> {
        let builder = ServerConfig::builder_with_protocol_versions(&self.state.supported_versions.clone());

        let verifier = match (self.state.require_client_cert, &self.state.certificate_store) {
            (true, None) => {
                return Err("requireClientCertificate is true but no validation_context is configured".into());
            },
            (true, Some(certificate_store)) => {
                Some(WebPkiClientVerifier::builder(StdArc::clone(certificate_store)).build()?)
            },
            (false, Some(certificate_store)) => {
                Some(WebPkiClientVerifier::builder(StdArc::clone(certificate_store)).allow_unauthenticated().build()?)
            },
            (false, None) => None,
        };

        let builder = if let Some(verifier) = verifier {
            builder.with_client_cert_verifier(verifier)
        } else {
            builder.with_no_client_auth()
        };
        let provider = get_crypto_key_provider()?;

        if let [SecretHolder { server_cert: ServerCert { certs, key, .. }, .. }] =
            self.state.server_ids_and_certificates.as_slice()
        {
            // If only a single certificate exists, do not install SNI resolver, just accept all
            // connections using the provided certificate (SANs is not relevant here)
            let mut server_config = builder.with_single_cert(certs.to_vec(), key.clone_key())?;
            self.use_stateless_resumption(&mut server_config);
            return Ok(server_config);
        }
        let mut resolver = RelaxedResolvesServerCertUsingSni::new();
        let errors = self
            .state
            .server_ids_and_certificates
            .iter()
            .map(|SecretHolder { name: secret_name, server_cert: ServerCert { certs, key, name } }| {
                provider
                    .load_private_key(key.clone_key())
                    .map(|private_key| {
                        let certs_vec = (**certs).clone();
                        // Extract the DER bytes of the first certificate (end-entity cert)
                        let first_cert_der = certs_vec.first().map(|c| c.as_ref().to_vec()).unwrap_or_default();

                        (secret_name, name, CertifiedKey::new(certs_vec, private_key), first_cert_der)
                    })
                    .map_err(|e| format!("UpstreamContext: Can't load private key {secret_name} {name} - {e}").into())
                    .inspect_err(|e| warn!("{e}"))
            })
            .filter_map(Result::ok)
            .map(|(secret_name, config_name, ck, first_cert_der)| {
                // Start with the name provided in the configuration
                let mut names_to_register = vec![config_name.to_owned()];

                // Extract and append all SANs from the actual certificate, extend the config name and remove
                // possible duplicates...
                names_to_register.extend(Self::extract_dns_sans(&first_cert_der));
                names_to_register.sort_unstable();
                names_to_register.dedup();

                // Wrap the CertifiedKey in an StdArc once, so it can be shared across multiple keys
                let ck_arc = StdArc::new(ck);
                let mut has_errors = false;

                // Register the certificate for every extracted name
                for name in names_to_register {
                    // Note: you will need to update resolver.add to accept StdArc<CertifiedKey>
                    // or create a new method resolver.add_arc(name, ck_arc)
                    if let Err(e) = resolver.add(&name, StdArc::clone(&ck_arc)) {
                        warn!("UpstreamContext: Can't add certificate for secret '{secret_name}' {name} - {e}");
                        has_errors = true;
                    }
                }

                if has_errors {
                    Err(())
                } else {
                    Ok(())
                }
            })
            .filter(std::result::Result::is_err)
            .count();
        if errors > 0 {
            Err(format!("Found {errors} errors in Tls context").into())
        } else {
            let mut server_config = builder.with_cert_resolver(StdArc::new(resolver));
            self.use_stateless_resumption(&mut server_config);
            Ok(server_config)
        }
    }

    /// Resume sessions from encrypted tickets instead of rustls' default in-memory session cache.
    ///
    /// The cache is a single `Mutex` that all proxy runtimes would share, since one `ServerConfig`
    /// is built per filter chain and then handed to every runtime behind an `Arc`. It is written to
    /// twice per TLS 1.3 handshake, which makes it a global contention point under
    /// connections-per-second load. Tickets move that state to the client.
    ///
    /// Clearing `session_storage` also disables TLS 1.2 session-id resumption: `can_cache` returning
    /// false makes rustls skip allocating a session id, leaving TLS 1.2 to resume via tickets too.
    fn use_stateless_resumption(&self, server_config: &mut ServerConfig) {
        server_config.ticketer = StdArc::clone(&self.state.ticketer);
        server_config.session_storage = StdArc::new(NoServerSessionStorage {});
    }

    // Extracts DNS names from the Subject Alternative Name extension
    fn extract_dns_sans(der: &[u8]) -> Vec<SmolStr> {
        let mut sans = Vec::new();

        // Parse the DER encoded certificate
        if let Ok((_, cert)) = X509Certificate::from_der(der) {
            // Look for the SAN extension
            if let Ok(Some(san_ext)) = cert.subject_alternative_name() {
                for name in &san_ext.value.general_names {
                    // We only care about DNS names for SNI matching
                    if let GeneralName::DNSName(dns) = name {
                        sans.push(dns.to_smolstr());
                    }
                }
            }
        }
        sans
    }
}

impl TlsContextBuilder<WantsToBuildClient> {
    pub fn build(&self) -> Result<ClientConfig> {
        let builder = ClientConfig::builder_with_protocol_versions(&self.state.supported_versions.clone());

        let builder = match self.state.trust_chain_verification {
            TrustChainVerification::VerifyTrustChain => {
                let verifier = WebPkiServerVerifier::builder(StdArc::clone(&self.state.certificate_store)).build()?;
                builder.with_webpki_verifier(verifier)
            },
            TrustChainVerification::AcceptUntrusted => {
                let verifier = StdArc::new(NoCertificateVerification {});
                warn!("TrustChainVerification::AcceptUntrusted : dangerous not verifying upstream certificate for cluster with sni: {}", self.state.sni);
                builder.dangerous().with_custom_certificate_verifier(verifier)
            },
        };

        if let Some(ClientCert { key, certs: auth_certs }) = self.state.client_certificate.as_deref() {
            debug!("UpstreamContext :  Selected Client Cert");
            let certs: Vec<webpki::types::CertificateDer<'_>> = auth_certs.as_ref().clone();
            Ok(builder.with_client_auth_cert(certs, key.clone_key())?)
        } else {
            Ok(builder.with_no_client_auth())
        }
    }

    pub fn with_trust_chain_verification(mut self, trust_chain_verification: TrustChainVerification) -> Self {
        self.state.trust_chain_verification = trust_chain_verification;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustls::{
        pki_types::ServerName,
        version::{TLS12, TLS13},
        ClientConnection, HandshakeKind, ServerConnection,
    };

    const CERT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../test_certs/demo/backend.cert.pem");
    const KEY: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../test_certs/demo/backend.key.pem");

    fn server_builder_with(version: &'static SupportedProtocolVersion) -> TlsContextBuilder<WantsToBuildServer> {
        // Fails when another test in this binary already installed it, which is fine.
        _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(std::fs::File::open(CERT).unwrap()))
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(KEY).unwrap()))
            .unwrap()
            .unwrap();

        let server_cert = ServerCert { name: "backend".into(), key: StdArc::new(key), certs: StdArc::new(certs) };

        TlsContextBuilder::with_supported_versions(vec![version])
            .with_no_client_auth()
            .with_certificates(vec![SecretHolder::new("backend".into(), server_cert)])
            .with_client_authentication(false)
            .unwrap()
    }

    fn server_builder() -> TlsContextBuilder<WantsToBuildServer> {
        server_builder_with(&TLS13)
    }

    fn client_config_with(version: &'static SupportedProtocolVersion) -> StdArc<ClientConfig> {
        StdArc::new(
            ClientConfig::builder_with_protocol_versions(&[version])
                .dangerous()
                .with_custom_certificate_verifier(StdArc::new(NoCertificateVerification {}))
                .with_no_client_auth(),
        )
    }

    fn client_config() -> StdArc<ClientConfig> {
        client_config_with(&TLS13)
    }

    /// Drives a full in-memory handshake, including the post-handshake tickets, and reports how the
    /// client classified it.
    fn handshake(client_config: &StdArc<ClientConfig>, server_config: &StdArc<ServerConfig>) -> HandshakeKind {
        let mut client =
            ClientConnection::new(StdArc::clone(client_config), ServerName::try_from("backend").unwrap()).unwrap();
        let mut server = ServerConnection::new(StdArc::clone(server_config)).unwrap();

        // Keep going after the handshake completes: TLS 1.3 tickets arrive afterwards, and without
        // them the client has nothing to resume with.
        for _ in 0..16 {
            let mut to_server = Vec::new();
            while client.wants_write() {
                client.write_tls(&mut to_server).unwrap();
            }
            let mut buf = to_server.as_slice();
            while !buf.is_empty() {
                server.read_tls(&mut buf).unwrap();
                server.process_new_packets().unwrap();
            }

            let mut to_client = Vec::new();
            while server.wants_write() {
                server.write_tls(&mut to_client).unwrap();
            }
            let mut buf = to_client.as_slice();
            while !buf.is_empty() {
                client.read_tls(&mut buf).unwrap();
                client.process_new_packets().unwrap();
            }

            if to_server.is_empty() && to_client.is_empty() {
                break;
            }
        }

        client.handshake_kind().expect("handshake did not complete")
    }

    #[test]
    fn server_config_uses_stateless_resumption() {
        let config = server_builder().build().unwrap();
        assert!(config.ticketer.enabled(), "ticketer must be enabled, otherwise rustls falls back to session_storage");
        assert!(!config.session_storage.can_cache(), "the mutex-backed session cache must be out of the hot path");
    }

    #[test]
    fn client_resumes_against_the_same_config() {
        let client_config = client_config();
        let server_config = StdArc::new(server_builder().build().unwrap());

        assert_eq!(handshake(&client_config, &server_config), HandshakeKind::Full);
        assert_eq!(handshake(&client_config, &server_config), HandshakeKind::Resumed);
    }

    /// Each proxy runtime holds its own clone of the builder and rebuilds independently on SDS
    /// updates. Tickets must survive that, or resumption silently stops working once a client is
    /// balanced onto a different runtime.
    #[test]
    fn client_resumes_across_rebuilds_of_a_cloned_builder() {
        let client_config = client_config();
        let builder = server_builder();
        let first = StdArc::new(builder.build().unwrap());
        let rebuilt = StdArc::new(builder.clone().build().unwrap());

        assert_eq!(handshake(&client_config, &first), HandshakeKind::Full);
        assert_eq!(handshake(&client_config, &rebuilt), HandshakeKind::Resumed);
    }

    /// Clearing `session_storage` removes TLS 1.2 session-id resumption, so TLS 1.2 has to resume
    /// from tickets now. It could not before: the previous `NeverProducesTickets` default made
    /// rustls refuse to acknowledge the client's `session_ticket` extension.
    #[test]
    fn tls12_client_resumes_from_a_ticket() {
        let client_config = client_config_with(&TLS12);
        let server_config = StdArc::new(server_builder_with(&TLS12).build().unwrap());

        assert_eq!(handshake(&client_config, &server_config), HandshakeKind::Full);
        assert_eq!(handshake(&client_config, &server_config), HandshakeKind::Resumed);
    }

    /// The counterpart to the test above: ticket keys are per filter chain, so a session must not
    /// carry over to an unrelated one.
    #[test]
    fn client_does_not_resume_against_an_independent_config() {
        let client_config = client_config();
        let first = StdArc::new(server_builder().build().unwrap());
        let unrelated = StdArc::new(server_builder().build().unwrap());

        assert_eq!(handshake(&client_config, &first), HandshakeKind::Full);
        assert_eq!(handshake(&client_config, &unrelated), HandshakeKind::Full);
    }
}
