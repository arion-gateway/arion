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
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex, Notify, RwLock};
use tracing::{debug, error, info, warn};

use crate::{Error, Result};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_CHANNEL_CAPACITY: usize = 100;

#[derive(Debug, Clone)]
pub struct CapturedRequest {
    pub method: http::Method,
    pub uri: http::Uri,
    pub version: http::Version,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

impl CapturedRequest {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    #[must_use]
    pub fn header_all(&self, name: &str) -> Vec<&str> {
        self.headers.get_all(name).iter().filter_map(|v| v.to_str().ok()).collect()
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

#[derive(Debug, Clone)]
pub struct PreConfiguredResponse {
    pub status: StatusCode,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
    pub delay: Option<Duration>,
}

impl Default for PreConfiguredResponse {
    fn default() -> Self {
        Self {
            status: StatusCode::OK,
            headers: vec![("content-type".to_owned(), "text/plain".to_owned())],
            body: Bytes::from_static(b"OK"),
            delay: None,
        }
    }
}

impl PreConfiguredResponse {
    #[must_use]
    pub fn with_status(status: StatusCode) -> Self {
        Self { status, ..Default::default() }
    }

    #[must_use]
    pub fn with_body(body: impl Into<Bytes>) -> Self {
        Self { body: body.into(), ..Default::default() }
    }

    #[must_use]
    pub fn header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    #[must_use]
    pub fn body(mut self, body: impl Into<Bytes>) -> Self {
        self.body = body.into();
        self
    }

    #[must_use]
    pub fn delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }
}

pub struct TestBackend {
    addr: SocketAddr,
    request_rx: mpsc::Receiver<CapturedRequest>,
    responses: Arc<Mutex<VecDeque<PreConfiguredResponse>>>,
    default_response: Arc<RwLock<PreConfiguredResponse>>,
    shutdown: Arc<Notify>,
    _server_handle: tokio::task::JoinHandle<()>,
}

impl TestBackend {
    pub async fn start() -> Result<Self> {
        Self::start_with_capacity(DEFAULT_CHANNEL_CAPACITY).await
    }

    pub async fn start_with_capacity(channel_capacity: usize) -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        Self::start_with_listener_and_capacity(listener, channel_capacity)
    }

    pub async fn start_on_port(port: u16) -> Result<Self> {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let listener = TcpListener::bind(addr).await?;
        Self::start_with_listener_and_capacity(listener, DEFAULT_CHANNEL_CAPACITY)
    }

    pub fn start_with_listener(listener: TcpListener) -> Result<Self> {
        Self::start_with_listener_and_capacity(listener, DEFAULT_CHANNEL_CAPACITY)
    }

    pub fn start_with_listener_and_capacity(listener: TcpListener, channel_capacity: usize) -> Result<Self> {
        let addr = listener.local_addr()?;

        info!(?addr, "Starting test backend server");

        let (request_tx, request_rx) = mpsc::channel(channel_capacity);
        let responses = Arc::new(Mutex::new(VecDeque::new()));
        let default_response = Arc::new(RwLock::new(PreConfiguredResponse::default()));
        let shutdown = Arc::new(Notify::new());

        let server_handle = {
            let responses = Arc::clone(&responses);
            let default_response = Arc::clone(&default_response);
            let shutdown = Arc::clone(&shutdown);

            tokio::spawn(async move {
                Self::run_server(listener, request_tx, responses, default_response, shutdown).await;
            })
        };

        Ok(Self { addr, request_rx, responses, default_response, shutdown, _server_handle: server_handle })
    }

    async fn run_server(
        listener: TcpListener,
        request_tx: mpsc::Sender<CapturedRequest>,
        responses: Arc<Mutex<VecDeque<PreConfiguredResponse>>>,
        default_response: Arc<RwLock<PreConfiguredResponse>>,
        shutdown: Arc<Notify>,
    ) {
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, peer_addr)) => {
                            debug!(?peer_addr, "Accepted connection");
                            let request_tx = request_tx.clone();
                            let responses = Arc::clone(&responses);
                            let default_response = Arc::clone(&default_response);

                            tokio::spawn(async move {
                                let io = hyper_util::rt::TokioIo::new(stream);
                                let service = service_fn(|req: Request<Incoming>| {
                                    let request_tx = request_tx.clone();
                                    let responses = Arc::clone(&responses);
                                    let default_response = Arc::clone(&default_response);
                                    async move {
                                        Self::handle_request(req, request_tx, responses, default_response).await
                                    }
                                });

                                if let Err(e) = http1::Builder::new()
                                    .serve_connection(io, service)
                                    .await
                                {
                                    warn!(?e, "Error serving connection");
                                }
                            });
                        }
                        Err(e) => {
                            error!(?e, "Error accepting connection");
                        }
                    }
                }
                () = shutdown.notified() => {
                    info!("Test backend shutting down");
                    break;
                }
            }
        }
    }

    async fn handle_request(
        req: Request<Incoming>,
        request_tx: mpsc::Sender<CapturedRequest>,
        responses: Arc<Mutex<VecDeque<PreConfiguredResponse>>>,
        default_response: Arc<RwLock<PreConfiguredResponse>>,
    ) -> std::result::Result<Response<Full<Bytes>>, hyper::Error> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let version = req.version();
        let headers = req.headers().clone();

        debug!(?method, ?uri, "Received request");

        let body: Bytes = match req.collect().await {
            Ok(collected) => collected.to_bytes(),
            Err(e) => {
                error!(?e, "Error reading request body");
                Bytes::new()
            },
        };

        let captured = CapturedRequest { method, uri, version, headers, body };
        if let Err(e) = request_tx.send(captured).await {
            warn!(?e, "Failed to send captured request");
        }

        let mock_response = {
            let mut queue = responses.lock().await;
            queue.pop_front()
        };

        let response = match mock_response {
            Some(r) => r,
            None => default_response.read().await.clone(),
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
        *self.default_response.write().await = response;
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

    pub async fn await_request_to_path(&mut self, path: &str, timeout: Duration) -> Result<CapturedRequest> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::NoRequestReceived(timeout));
            }
            match fast_timeout(remaining, self.request_rx.recv()).await {
                Ok(Some(req)) if req.path() == path => return Ok(req),
                Ok(Some(_)) => {},
                Ok(None) | Err(_) => return Err(Error::NoRequestReceived(timeout)),
            }
        }
    }

    pub async fn await_path_request_count(&mut self, path: &str, count: usize, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut received = 0;
        while received < count {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::NoRequestReceived(timeout));
            }
            match fast_timeout(remaining, self.request_rx.recv()).await {
                Ok(Some(req)) if req.path() == path => received += 1,
                Ok(Some(_)) => {},
                Ok(None) | Err(_) => return Err(Error::NoRequestReceived(timeout)),
            }
        }
        Ok(())
    }

    pub fn drain_requests_for_path(&mut self, path: &str) -> usize {
        let mut count = 0;
        while let Ok(req) = self.request_rx.try_recv() {
            if req.path() == path {
                count += 1;
            }
        }
        count
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }
}

impl Drop for TestBackend {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}
