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

use pingora::prelude::fast_timeout::fast_timeout;
use std::collections::VecDeque;
use std::io::BufReader;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::RootCertStore;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio_rustls::TlsAcceptor;
use tracing::{debug, error, info, warn};

use crate::{CapturedRequest, Error, PreConfiguredResponse, Result};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub struct TlsBackendConfig {
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
    pub client_ca: Option<RootCertStore>,
    pub require_client_cert: bool,
}

impl TlsBackendConfig {
    pub fn from_files(cert_path: impl AsRef<Path>, key_path: impl AsRef<Path>) -> Result<Self> {
        let cert_chain = Self::load_certs(cert_path)?;
        let private_key = Self::load_key(key_path)?;
        Ok(Self { cert_chain, private_key, client_ca: None, require_client_cert: false })
    }

    pub fn with_client_ca(mut self, ca_path: impl AsRef<Path>) -> Result<Self> {
        self.client_ca = Some(Self::load_root_store(ca_path)?);
        Ok(self)
    }

    #[must_use]
    pub fn require_client_cert(mut self) -> Self {
        self.require_client_cert = true;
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

    fn build_server_config(&self) -> Result<rustls::ServerConfig> {
        // When both ring and aws-lc-rs are enabled transitively, rustls cannot pick a default.
        _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let client_auth = match (&self.client_ca, self.require_client_cert) {
            (Some(ca_store), true) => {
                let verifier = WebPkiClientVerifier::builder(Arc::new(ca_store.clone()))
                    .build()
                    .map_err(|e| Error::Config(format!("Failed to build client verifier: {e}")))?;
                verifier
            },
            (Some(ca_store), false) => {
                let verifier = WebPkiClientVerifier::builder(Arc::new(ca_store.clone()))
                    .allow_unauthenticated()
                    .build()
                    .map_err(|e| Error::Config(format!("Failed to build client verifier: {e}")))?;
                verifier
            },
            (None, _) => WebPkiClientVerifier::no_client_auth(),
        };

        let config = rustls::ServerConfig::builder()
            .with_client_cert_verifier(client_auth)
            .with_single_cert(self.cert_chain.clone(), self.private_key.clone_key())
            .map_err(|e| Error::Config(format!("Failed to build server config: {e}")))?;

        Ok(config)
    }
}

pub struct TlsTestBackend {
    addr: SocketAddr,
    request_rx: mpsc::Receiver<CapturedRequest>,
    responses: Arc<Mutex<VecDeque<PreConfiguredResponse>>>,
    default_response: Arc<Mutex<PreConfiguredResponse>>,
    shutdown: Arc<Notify>,
    _server_handle: tokio::task::JoinHandle<()>,
}

impl TlsTestBackend {
    pub async fn start(config: TlsBackendConfig) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        Self::start_with_listener(listener, &config)
    }

    pub async fn start_with_files(cert_path: impl AsRef<Path>, key_path: impl AsRef<Path>) -> Result<Self> {
        let config = TlsBackendConfig::from_files(cert_path, key_path)?;
        Self::start(config).await
    }

    pub async fn start_mtls(
        cert_path: impl AsRef<Path>,
        key_path: impl AsRef<Path>,
        client_ca_path: impl AsRef<Path>,
    ) -> Result<Self> {
        let config =
            TlsBackendConfig::from_files(cert_path, key_path)?.with_client_ca(client_ca_path)?.require_client_cert();
        Self::start(config).await
    }

    pub async fn start_on_port(port: u16, config: TlsBackendConfig) -> Result<Self> {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let listener = TcpListener::bind(addr).await?;
        Self::start_with_listener(listener, &config)
    }

    fn start_with_listener(listener: TcpListener, config: &TlsBackendConfig) -> Result<Self> {
        let addr = listener.local_addr()?;
        let server_config = config.build_server_config()?;
        let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));

        info!(?addr, "Starting TLS test backend server");

        let (request_tx, request_rx) = mpsc::channel(100);
        let responses = Arc::new(Mutex::new(VecDeque::new()));
        let default_response = Arc::new(Mutex::new(PreConfiguredResponse::default()));
        let shutdown = Arc::new(Notify::new());

        let server_handle = {
            let responses = Arc::clone(&responses);
            let default_response = Arc::clone(&default_response);
            let shutdown = Arc::clone(&shutdown);

            tokio::spawn(async move {
                Self::run_server(listener, tls_acceptor, request_tx, responses, default_response, shutdown).await;
            })
        };

        Ok(Self { addr, request_rx, responses, default_response, shutdown, _server_handle: server_handle })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "server state is flat by design — grouping into a struct adds boilerplate with no clarity benefit"
    )]
    async fn run_server(
        listener: TcpListener,
        tls_acceptor: TlsAcceptor,
        request_tx: mpsc::Sender<CapturedRequest>,
        responses: Arc<Mutex<VecDeque<PreConfiguredResponse>>>,
        default_response: Arc<Mutex<PreConfiguredResponse>>,
        shutdown: Arc<Notify>,
    ) {
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            debug!(?peer_addr, "Accepted TCP connection, starting TLS handshake");
                            let tls_acceptor = tls_acceptor.clone();
                            let request_tx = request_tx.clone();
                            let responses = Arc::clone(&responses);
                            let default_response = Arc::clone(&default_response);

                            tokio::spawn(async move {
                                match tls_acceptor.accept(stream).await {
                                    Ok(tls_stream) => {
                                        debug!(?peer_addr, "TLS handshake complete");
                                        let io = hyper_util::rt::TokioIo::new(tls_stream);
                                        let service = service_fn(move |req: Request<Incoming>| {
                                            let request_tx = request_tx.clone();
                                            let responses = Arc::clone(&responses);
                                            let default_response = Arc::clone(&default_response);
                                            async move {
                                                Self::handle_request(req, peer_addr, request_tx, responses, default_response).await
                                            }
                                        });

                                        if let Err(e) = http1::Builder::new()
                                            .serve_connection(io, service)
                                            .await
                                        {
                                            warn!(?e, "Error serving TLS connection");
                                        }
                                    }
                                    Err(e) => {
                                        debug!(?peer_addr, ?e, "TLS handshake failed");
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            error!(?e, "Error accepting connection");
                        }
                    }
                }
                () = shutdown.notified() => {
                    info!("TLS test backend shutting down");
                    break;
                }
            }
        }
    }

    async fn handle_request(
        req: Request<Incoming>,
        peer_addr: SocketAddr,
        request_tx: mpsc::Sender<CapturedRequest>,
        responses: Arc<Mutex<VecDeque<PreConfiguredResponse>>>,
        default_response: Arc<Mutex<PreConfiguredResponse>>,
    ) -> std::result::Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let version = req.version();
        let headers = req.headers().clone();

        debug!(?method, ?uri, "TLS backend received request");

        let body: Bytes = match req.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                error!(?e, "Error reading request body");
                Bytes::new()
            },
        };

        let captured = CapturedRequest { method, uri, version, headers, body, peer_addr };
        if let Err(e) = request_tx.send(captured).await {
            warn!(?e, "Failed to send captured request");
        }

        let mock_response = {
            let mut queue = responses.lock().await;
            queue.pop_front()
        };

        let response = match mock_response {
            Some(r) => r,
            None => default_response.lock().await.clone(),
        };

        if let Some(delay) = response.delay {
            tokio::time::sleep(delay).await;
        }

        let mut builder = Response::builder().status(response.status);
        for (name, value) in &response.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }

        Ok(builder.body(Full::new(response.body)).unwrap_or_else(|_| Response::new(Full::new(Bytes::new()))))
    }

    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[must_use]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub async fn enqueue_response(&self, response: PreConfiguredResponse) {
        self.responses.lock().await.push_back(response);
    }

    pub async fn set_default_response(&self, response: PreConfiguredResponse) {
        *self.default_response.lock().await = response;
    }

    pub async fn await_request(&mut self) -> Result<CapturedRequest> {
        self.await_request_with_timeout(DEFAULT_REQUEST_TIMEOUT).await
    }

    #[allow(clippy::disallowed_methods)]
    pub async fn await_request_with_timeout(&mut self, timeout: Duration) -> Result<CapturedRequest> {
        match fast_timeout(timeout, self.request_rx.recv()).await {
            Ok(Some(req)) => Ok(req),
            Ok(None) | Err(_) => Err(Error::NoRequestReceived(timeout)),
        }
    }

    pub fn try_recv_request(&mut self) -> Option<CapturedRequest> {
        self.request_rx.try_recv().ok()
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }
}

impl Drop for TlsTestBackend {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}
