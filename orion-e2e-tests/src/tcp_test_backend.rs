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
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex, Notify};
use tracing::{debug, error, info, warn};

use crate::{Error, Result};

const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);
const READ_BUFFER_SIZE: usize = 4096;

#[derive(Debug, Clone)]
pub struct CapturedTcpConnection {
    pub peer_addr: SocketAddr,
    pub received_data: Vec<u8>,
}

#[derive(Debug, Clone)]
struct TcpBehavior {
    send_on_connect: Option<Vec<u8>>,
    send_after_read: Option<Vec<u8>>,
    close_after_send: bool,
    read_timeout: Duration,
}

impl Default for TcpBehavior {
    fn default() -> Self {
        Self {
            send_on_connect: None,
            send_after_read: None,
            close_after_send: false,
            read_timeout: Duration::from_millis(100),
        }
    }
}

pub struct TcpTestBackend {
    addr: SocketAddr,
    connection_rx: mpsc::Receiver<CapturedTcpConnection>,
    behavior: Arc<Mutex<TcpBehavior>>,
    shutdown: Arc<Notify>,
    _server_handle: tokio::task::JoinHandle<()>,
}

impl TcpTestBackend {
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        Self::start_with_listener(listener)
    }

    pub async fn start_on_port(port: u16) -> Result<Self> {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let listener = TcpListener::bind(addr).await?;
        Self::start_with_listener(listener)
    }

    fn start_with_listener(listener: TcpListener) -> Result<Self> {
        let addr = listener.local_addr()?;

        info!(?addr, "Starting TCP test backend");

        let (connection_tx, connection_rx) = mpsc::channel(100);
        let behavior = Arc::new(Mutex::new(TcpBehavior::default()));
        let shutdown = Arc::new(Notify::new());

        let server_handle = {
            let behavior = Arc::clone(&behavior);
            let shutdown = Arc::clone(&shutdown);

            tokio::spawn(async move {
                Self::run_server(listener, connection_tx, behavior, shutdown).await;
            })
        };

        Ok(Self { addr, connection_rx, behavior, shutdown, _server_handle: server_handle })
    }

    async fn run_server(
        listener: TcpListener,
        connection_tx: mpsc::Sender<CapturedTcpConnection>,
        behavior: Arc<Mutex<TcpBehavior>>,
        shutdown: Arc<Notify>,
    ) {
        loop {
            tokio::select! {
                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((mut stream, peer_addr)) => {
                            debug!(?peer_addr, "TCP backend accepted connection");

                            let connection_tx = connection_tx.clone();
                            let behavior = Arc::clone(&behavior);

                            tokio::spawn(async move {
                                let current_behavior = behavior.lock().await.clone();
                                let mut received_data = Vec::new();

                                if let Some(ref data) = current_behavior.send_on_connect {
                                    if let Err(e) = stream.write_all(data).await {
                                        warn!(?e, "Failed to send data on connect");
                                    }
                                    debug!(?peer_addr, bytes = data.len(), "Sent data on connect");
                                }

                                if current_behavior.close_after_send {
                                    debug!(?peer_addr, "Closing connection after send");
                                } else {
                                    let mut buf = [0u8; READ_BUFFER_SIZE];
                                    match fast_timeout(
                                        current_behavior.read_timeout,
                                        stream.read(&mut buf),
                                    )
                                    .await
                                    {
                                        Ok(Ok(n)) if n > 0 => {
                                            if let Some(slice) = buf.get(..n) {
                                                received_data.extend_from_slice(slice);
                                            }
                                            debug!(?peer_addr, bytes = n, "Read data from connection");
                                        }
                                        Ok(Ok(_)) => {
                                            debug!(?peer_addr, "Connection closed by peer");
                                        }
                                        Ok(Err(e)) => {
                                            debug!(?peer_addr, ?e, "Read error");
                                        }
                                        Err(_) => {
                                            debug!(?peer_addr, "Read timeout, no data received");
                                        }
                                    }

                                    if let Some(ref data) = current_behavior.send_after_read {
                                        if let Err(e) = stream.write_all(data).await {
                                            warn!(?e, "Failed to send after-read response");
                                        }
                                        debug!(?peer_addr, bytes = data.len(), "Sent after-read response");
                                    }
                                }

                                let captured = CapturedTcpConnection { peer_addr, received_data };
                                if let Err(e) = connection_tx.send(captured).await {
                                    warn!(?e, "Failed to send captured connection");
                                }
                            });
                        }
                        Err(e) => {
                            error!(?e, "Error accepting TCP connection");
                        }
                    }
                }
                () = shutdown.notified() => {
                    info!("TCP test backend shutting down");
                    break;
                }
            }
        }
    }

    #[must_use]
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    #[must_use]
    pub fn port(&self) -> u16 {
        self.addr.port()
    }

    pub async fn set_send_on_connect(&self, data: impl Into<Vec<u8>>) {
        self.behavior.lock().await.send_on_connect = Some(data.into());
    }

    pub async fn clear_send_on_connect(&self) {
        self.behavior.lock().await.send_on_connect = None;
    }

    pub async fn set_send_after_read(&self, data: impl Into<Vec<u8>>) {
        self.behavior.lock().await.send_after_read = Some(data.into());
    }

    pub async fn set_close_after_send(&self, close: bool) {
        self.behavior.lock().await.close_after_send = close;
    }

    pub async fn set_read_timeout(&self, timeout: Duration) {
        self.behavior.lock().await.read_timeout = timeout;
    }

    pub async fn await_connection(&mut self) -> Result<CapturedTcpConnection> {
        self.await_connection_with_timeout(DEFAULT_CONNECTION_TIMEOUT).await
    }

    #[allow(clippy::disallowed_methods)]
    pub async fn await_connection_with_timeout(&mut self, timeout: Duration) -> Result<CapturedTcpConnection> {
        match fast_timeout(timeout, self.connection_rx.recv()).await {
            Ok(Some(conn)) => Ok(conn),
            Ok(None) | Err(_) => Err(Error::NoConnectionReceived(timeout)),
        }
    }

    pub fn try_recv_connection(&mut self) -> Option<CapturedTcpConnection> {
        self.connection_rx.try_recv().ok()
    }

    pub async fn await_connection_count(&mut self, count: usize, timeout: Duration) -> Result<()> {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut received = 0;
        while received < count {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(Error::NoConnectionReceived(timeout));
            }
            match fast_timeout(remaining, self.connection_rx.recv()).await {
                Ok(Some(_)) => received += 1,
                Ok(None) | Err(_) => return Err(Error::NoConnectionReceived(timeout)),
            }
        }
        Ok(())
    }

    pub fn drain_connections(&mut self) -> usize {
        let mut count = 0;
        while self.connection_rx.try_recv().is_ok() {
            count += 1;
        }
        count
    }

    pub fn shutdown(&self) {
        self.shutdown.notify_one();
    }
}

impl Drop for TcpTestBackend {
    fn drop(&mut self) {
        self.shutdown.notify_one();
    }
}
