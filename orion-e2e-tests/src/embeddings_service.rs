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

use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Notify};
use tracing::{debug, error, info, warn};

use crate::{Error, Result};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_CHANNEL_CAPACITY: usize = 100;

#[derive(Debug, Clone)]
pub struct CapturedEmbeddingsTestRequest {
    pub method: http::Method,
    pub uri: http::Uri,
    pub version: http::Version,
    pub headers: http::HeaderMap,
    pub body: Bytes,
    pub model: Option<String>,
    pub input: Vec<String>,
}

impl CapturedEmbeddingsTestRequest {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    #[must_use]
    pub fn path(&self) -> &str {
        self.uri.path()
    }

    #[must_use]
    pub fn body_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.body).ok()
    }
}

pub struct EmbeddingsTestService {
    addr: SocketAddr,
    request_rx: mpsc::Receiver<CapturedEmbeddingsTestRequest>,
    shutdown: std::sync::Arc<Notify>,
    _server_handle: tokio::task::JoinHandle<()>,
}

impl EmbeddingsTestService {
    pub async fn start() -> Result<Self> {
        Self::start_with_capacity(DEFAULT_CHANNEL_CAPACITY).await
    }

    pub async fn start_with_capacity(channel_capacity: usize) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        Self::start_with_listener_and_capacity(listener, channel_capacity).await
    }

    pub async fn start_with_listener_and_capacity(listener: TcpListener, channel_capacity: usize) -> Result<Self> {
        let addr = listener.local_addr()?;
        info!(?addr, "Starting embeddings test service");

        let (request_tx, request_rx) = mpsc::channel(channel_capacity);
        let shutdown = std::sync::Arc::new(Notify::new());

        let server_handle = {
            let shutdown = std::sync::Arc::clone(&shutdown);
            tokio::spawn(async move {
                Self::run_server(listener, request_tx, shutdown).await;
            })
        };

        Ok(Self { addr, request_rx, shutdown, _server_handle: server_handle })
    }

    async fn run_server(
        listener: TcpListener,
        request_tx: mpsc::Sender<CapturedEmbeddingsTestRequest>,
        shutdown: std::sync::Arc<Notify>,
    ) {
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            debug!(?peer_addr, "Accepted embeddings connection");
                            let request_tx = request_tx.clone();
                            tokio::spawn(async move {
                                let io = hyper_util::rt::TokioIo::new(stream);
                                let service = service_fn(|req: Request<Incoming>| {
                                    let request_tx = request_tx.clone();
                                    async move {
                                        Self::handle_request(req, request_tx).await
                                    }
                                });

                                if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                                    warn!(?e, "Error serving embeddings connection");
                                }
                            });
                        },
                        Err(e) => {
                            error!(?e, "Error accepting embeddings connection");
                        },
                    }
                },
                () = shutdown.notified() => {
                    info!("Embeddings test service shutting down");
                    break;
                },
            }
        }
    }

    async fn handle_request(
        req: Request<Incoming>,
        request_tx: mpsc::Sender<CapturedEmbeddingsTestRequest>,
    ) -> std::result::Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let version = req.version();
        let headers = req.headers().clone();

        let body = match req.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                error!(?e, "Error reading embeddings request body");
                Bytes::new()
            },
        };

        let parsed = serde_json::from_slice::<EmbeddingsRequest>(&body).ok();
        let model = parsed.as_ref().and_then(|p| p.model.clone());
        let input = parsed.map_or_else(Vec::new, |p| p.input.into_vec());

        let captured =
            CapturedEmbeddingsTestRequest { method, uri, version, headers, body, model, input: input.clone() };
        if let Err(e) = request_tx.send(captured).await {
            warn!(?e, "Failed to send captured embeddings request");
        }

        let data: Vec<_> = input
            .iter()
            .enumerate()
            .map(|(index, text)| {
                json!({
                    "index": index,
                    "embedding": embedding_for_text(text),
                })
            })
            .collect();

        let response_body = json!({ "data": data }).to_string();
        let response = Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(response_body)))
            .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())));

        Ok(response)
    }

    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[must_use]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub async fn await_request(&mut self) -> Result<CapturedEmbeddingsTestRequest> {
        self.await_request_with_timeout(DEFAULT_REQUEST_TIMEOUT).await
    }

    #[allow(clippy::disallowed_methods)]
    pub async fn await_request_with_timeout(&mut self, timeout: Duration) -> Result<CapturedEmbeddingsTestRequest> {
        match tokio::time::timeout(timeout, self.request_rx.recv()).await {
            Ok(Some(req)) => Ok(req),
            Ok(None) | Err(_) => Err(Error::NoRequestReceived(timeout)),
        }
    }

    pub fn try_recv_request(&mut self) -> Option<CapturedEmbeddingsTestRequest> {
        self.request_rx.try_recv().ok()
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }
}

impl Drop for EmbeddingsTestService {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}

#[derive(Debug, Deserialize)]
struct EmbeddingsRequest {
    model: Option<String>,
    input: EmbeddingsInput,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum EmbeddingsInput {
    One(String),
    Many(Vec<String>),
}

impl EmbeddingsInput {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::One(input) => vec![input],
            Self::Many(inputs) => inputs,
        }
    }
}

fn embedding_for_text(text: &str) -> [f32; 3] {
    let text = text.to_ascii_lowercase();
    if text.contains("weather") || text.contains("forecast") || text.contains("temperature") {
        [1.0, 0.0, 0.0]
    } else if text.contains("database") || text.contains("sql") || text.contains("records") {
        [1.0, 0.0, 0.0]
    } else if text.contains("user") || text.contains("profile") || text.contains("account") {
        [0.0, 1.0, 0.0]
    } else if text.contains("analytics") || text.contains("statistics") || text.contains("reports") {
        [0.0, 1.0, 0.0]
    } else if text.contains("payment") || text.contains("billing") || text.contains("transaction") {
        [0.0, 0.0, 1.0]
    } else if text.contains("email") || text.contains("notification") || text.contains("message") {
        [0.0, 0.0, 1.0]
    } else {
        [0.577_350_26, 0.577_350_26, 0.577_350_26]
    }
}
