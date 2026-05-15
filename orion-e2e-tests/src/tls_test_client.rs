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

use pingora::prelude::fast_timeout::fast_timeout;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Request, Uri};
use http_body_util::{BodyExt, Full};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, RootCertStore, SignatureScheme};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tracing::debug;

use crate::{Error, RequestBuilder, Result, TestResponse};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Default)]
pub struct TlsClientConfig {
    pub root_ca: Option<RootCertStore>,
    pub client_cert: Option<Vec<CertificateDer<'static>>>,
    pub client_key: Option<PrivateKeyDer<'static>>,
    pub skip_verification: bool,
    pub tls_min_version: Option<&'static rustls::SupportedProtocolVersion>,
    pub tls_max_version: Option<&'static rustls::SupportedProtocolVersion>,
}

impl TlsClientConfig {
    pub fn with_root_ca(ca_path: impl AsRef<Path>) -> Result<Self> {
        let root_ca = Self::load_root_store(ca_path)?;
        Ok(Self { root_ca: Some(root_ca), ..Default::default() })
    }

    pub fn with_client_cert(mut self, cert_path: impl AsRef<Path>, key_path: impl AsRef<Path>) -> Result<Self> {
        self.client_cert = Some(Self::load_certs(cert_path)?);
        self.client_key = Some(Self::load_key(key_path)?);
        Ok(self)
    }

    #[must_use]
    pub fn skip_verification(mut self) -> Self {
        self.skip_verification = true;
        self
    }

    fn load_certs(path: impl AsRef<Path>) -> Result<Vec<CertificateDer<'static>>> {
        let file =
            std::fs::File::open(path.as_ref()).map_err(|e| Error::Config(format!("Failed to open cert file: {e}")))?;
        let mut reader = BufReader::new(file);
        let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| Error::Config(format!("Failed to parse certificates: {e}")))?;
        if certs.is_empty() {
            return Err(Error::Config("No certificates found in file".into()));
        }
        Ok(certs)
    }

    fn load_key(path: impl AsRef<Path>) -> Result<PrivateKeyDer<'static>> {
        let file =
            std::fs::File::open(path.as_ref()).map_err(|e| Error::Config(format!("Failed to open key file: {e}")))?;
        let mut reader = BufReader::new(file);
        rustls_pemfile::private_key(&mut reader)
            .map_err(|e| Error::Config(format!("Failed to parse private key: {e}")))?
            .ok_or_else(|| Error::Config("No private key found in file".into()))
    }

    fn load_root_store(path: impl AsRef<Path>) -> Result<RootCertStore> {
        let certs = Self::load_certs(path)?;
        let mut store = RootCertStore::empty();
        for cert in certs {
            store.add(cert).map_err(|e| Error::Config(format!("Failed to add root certificate: {e}")))?;
        }
        Ok(store)
    }

    fn build_client_config(&self) -> Result<rustls::ClientConfig> {
        let builder = match (self.tls_min_version, self.tls_max_version) {
            (Some(min), Some(max)) if std::ptr::eq(min, max) => {
                rustls::ClientConfig::builder_with_protocol_versions(&[min])
            },
            _ => rustls::ClientConfig::builder(),
        };

        let config = if self.skip_verification {
            builder.dangerous().with_custom_certificate_verifier(Arc::new(NoVerifier)).with_no_client_auth()
        } else if let Some(ref root_ca) = self.root_ca {
            let builder_with_roots = builder.with_root_certificates(root_ca.clone());

            match (&self.client_cert, &self.client_key) {
                (Some(cert), Some(key)) => builder_with_roots
                    .with_client_auth_cert(cert.clone(), key.clone_key())
                    .map_err(|e| Error::Config(format!("Failed to configure client cert: {e}")))?,
                _ => builder_with_roots.with_no_client_auth(),
            }
        } else {
            return Err(Error::Config("TlsClientConfig requires either root_ca or skip_verification".into()));
        };

        Ok(config)
    }
}

#[derive(Debug)]
struct NoVerifier;

impl ServerCertVerifier for NoVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

pub struct TlsTestClient {
    addr: SocketAddr,
    server_name: ServerName<'static>,
    tls_config: Arc<rustls::ClientConfig>,
    timeout: Duration,
    default_headers: Vec<(String, String)>,
}

impl TlsTestClient {
    pub fn new(addr: SocketAddr, server_name: impl Into<String>, config: TlsClientConfig) -> Result<Self> {
        let server_name_str = server_name.into();
        let server_name = ServerName::try_from(server_name_str.clone())
            .map_err(|_e| Error::Config(format!("Invalid server name: {server_name_str}")))?
            .to_owned();
        let tls_config = Arc::new(config.build_client_config()?);
        Ok(Self { addr, server_name, tls_config, timeout: DEFAULT_TIMEOUT, default_headers: vec![] })
    }

    #[must_use]
    pub fn builder(addr: SocketAddr) -> TlsTestClientBuilder {
        TlsTestClientBuilder::new(addr)
    }

    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.default_headers.push((name.into(), value.into()));
        self
    }

    pub async fn get(&self, path: &str) -> Result<TestResponse> {
        self.request(Method::GET, path, None).await
    }

    pub async fn post(&self, path: &str, body: impl Into<Bytes>) -> Result<TestResponse> {
        self.request(Method::POST, path, Some(body.into())).await
    }

    pub async fn put(&self, path: &str, body: impl Into<Bytes>) -> Result<TestResponse> {
        self.request(Method::PUT, path, Some(body.into())).await
    }

    pub async fn delete(&self, path: &str) -> Result<TestResponse> {
        self.request(Method::DELETE, path, None).await
    }

    #[allow(clippy::disallowed_methods)]
    pub async fn request(&self, method: Method, path: &str, body: Option<Bytes>) -> Result<TestResponse> {
        let uri: Uri =
            format!("https://{}{}", self.addr, path).parse().map_err(|e| Error::Http(format!("Invalid URI: {e}")))?;

        debug!(?method, ?uri, "Sending TLS request");

        let mut builder = Request::builder().method(method).uri(&uri);

        builder = builder.header("host", self.server_name.to_str().as_ref());

        for (name, value) in &self.default_headers {
            builder = builder.header(name.as_str(), value.as_str());
        }

        let request = match body {
            Some(b) => builder.body(Full::new(b)).map_err(|e| Error::Http(format!("Failed to build request: {e}")))?,
            None => builder
                .body(Full::new(Bytes::new()))
                .map_err(|e| Error::Http(format!("Failed to build request: {e}")))?,
        };

        fast_timeout(self.timeout, self.send_tls_request(request))
            .await
            .map_err(|_e| Error::RequestTimeout(self.timeout))?
    }

    #[allow(clippy::disallowed_methods)]
    pub async fn send(&self, request: RequestBuilder) -> Result<TestResponse> {
        let uri: Uri = format!("https://{}{}", self.addr, request.path())
            .parse()
            .map_err(|e| Error::Http(format!("Invalid URI: {e}")))?;

        let mut builder = Request::builder().method(request.method()).uri(&uri);

        builder = builder.header("host", self.server_name.to_str().as_ref());

        for (name, value) in &self.default_headers {
            builder = builder.header(name.as_str(), value.as_str());
        }

        for (name, value) in request.headers() {
            builder = builder.header(name.as_str(), value.as_str());
        }

        let http_request = builder
            .body(Full::new(request.body_bytes().clone()))
            .map_err(|e| Error::Http(format!("Failed to build request: {e}")))?;

        fast_timeout(self.timeout, self.send_tls_request(http_request))
            .await
            .map_err(|_e| Error::RequestTimeout(self.timeout))?
    }

    async fn send_tls_request(&self, request: Request<Full<Bytes>>) -> Result<TestResponse> {
        let tcp_stream =
            TcpStream::connect(self.addr).await.map_err(|e| Error::Http(format!("Failed to connect: {e}")))?;

        let connector = TlsConnector::from(Arc::clone(&self.tls_config));
        let tls_stream = connector
            .connect(self.server_name.clone(), tcp_stream)
            .await
            .map_err(|e| Error::Http(format!("TLS handshake failed: {e}")))?;

        let io = hyper_util::rt::TokioIo::new(tls_stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|e| Error::Http(format!("HTTP handshake failed: {e}")))?;

        tokio::spawn(async move {
            if let Err(e) = conn.await {
                debug!(?e, "Connection error");
            }
        });

        let response = sender.send_request(request).await.map_err(|e| Error::Http(format!("Request failed: {e}")))?;

        let status = response.status();
        let headers = response.headers().clone();
        let body: Bytes =
            response.collect().await.map_err(|e| Error::Http(format!("Failed to read response body: {e}")))?.to_bytes();

        debug!(?status, body_len = body.len(), "Received TLS response");

        Ok(TestResponse { status, headers, body })
    }
}

pub struct TlsTestClientBuilder {
    addr: SocketAddr,
    server_name: Option<String>,
    root_ca_path: Option<std::path::PathBuf>,
    client_cert_path: Option<std::path::PathBuf>,
    client_key_path: Option<std::path::PathBuf>,
    skip_verification: bool,
    timeout: Duration,
    default_headers: Vec<(String, String)>,
    tls_min_version: Option<&'static rustls::SupportedProtocolVersion>,
    tls_max_version: Option<&'static rustls::SupportedProtocolVersion>,
}

impl TlsTestClientBuilder {
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            server_name: None,
            root_ca_path: None,
            client_cert_path: None,
            client_key_path: None,
            skip_verification: false,
            timeout: DEFAULT_TIMEOUT,
            default_headers: vec![],
            tls_min_version: None,
            tls_max_version: None,
        }
    }

    #[must_use]
    pub fn server_name(mut self, name: impl Into<String>) -> Self {
        self.server_name = Some(name.into());
        self
    }

    #[must_use]
    pub fn root_ca(mut self, path: impl AsRef<Path>) -> Self {
        self.root_ca_path = Some(path.as_ref().to_path_buf());
        self
    }

    #[must_use]
    pub fn client_cert(mut self, cert_path: impl AsRef<Path>, key_path: impl AsRef<Path>) -> Self {
        self.client_cert_path = Some(cert_path.as_ref().to_path_buf());
        self.client_key_path = Some(key_path.as_ref().to_path_buf());
        self
    }

    #[must_use]
    pub fn skip_verification(mut self) -> Self {
        self.skip_verification = true;
        self
    }

    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    #[must_use]
    pub fn tls_1_2_only(mut self) -> Self {
        self.tls_min_version = Some(&rustls::version::TLS12);
        self.tls_max_version = Some(&rustls::version::TLS12);
        self
    }

    #[must_use]
    pub fn tls_1_3_only(mut self) -> Self {
        self.tls_min_version = Some(&rustls::version::TLS13);
        self.tls_max_version = Some(&rustls::version::TLS13);
        self
    }

    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.default_headers.push((name.into(), value.into()));
        self
    }

    pub fn build(self) -> Result<TlsTestClient> {
        let server_name = self.server_name.ok_or_else(|| Error::Config("server_name is required".into()))?;

        let mut config = if self.skip_verification {
            TlsClientConfig::default().skip_verification()
        } else if let Some(ref ca_path) = self.root_ca_path {
            TlsClientConfig::with_root_ca(ca_path)?
        } else {
            return Err(Error::Config("Either root_ca or skip_verification is required".into()));
        };

        if let (Some(cert_path), Some(key_path)) = (self.client_cert_path, self.client_key_path) {
            config = config.with_client_cert(cert_path, key_path)?;
        }

        config.tls_min_version = self.tls_min_version;
        config.tls_max_version = self.tls_max_version;

        let mut client = TlsTestClient::new(self.addr, server_name, config)?;
        client.timeout = self.timeout;
        client.default_headers = self.default_headers;

        Ok(client)
    }
}
