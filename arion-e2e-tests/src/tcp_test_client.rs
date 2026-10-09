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
use std::net::SocketAddr;
use std::sync::LazyLock;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::Result;

/// Env var to override [`READ_TIMEOUT`] without touching source, e.g. if CI runners turn
/// out to be slower than the default below. Takes a plain integer number of milliseconds.
const READ_TIMEOUT_ENV_VAR: &str = "E2E_READ_TIMEOUT_MS";
const READ_TIMEOUT_DEFAULT_MS: u64 = 500;
const READ_BUFFER_SIZE: usize = 4096;

/// Ceiling for raw-socket reads that loop until the peer closes the connection (EOF) or this
/// timeout fires, whichever comes first (see `send_and_receive` below). Tests that expect Arion
/// to keep the connection alive after responding never see that EOF, so they always pay this
/// ceiling in full. It is not tied to any real Arion-side timing, so the default is small: real
/// local round trips in this suite complete in well under 50ms, even including spawning Arion
/// itself. Override with the `E2E_READ_TIMEOUT_MS` env var (milliseconds) if a given
/// environment (e.g. a slower CI runner) needs more headroom. Read once and cached on first use:
/// changing the env var mid-run has no effect, only set it before the test binary starts.
pub static READ_TIMEOUT: LazyLock<Duration> = LazyLock::new(|| {
    let ms = std::env::var(READ_TIMEOUT_ENV_VAR)
        .ok()
        .and_then(|v| match v.parse() {
            Ok(ms) => Some(ms),
            Err(e) => {
                tracing::warn!(
                    value = %v, error = %e,
                    "{READ_TIMEOUT_ENV_VAR} is set but not a valid number of milliseconds, using the default"
                );
                None
            },
        })
        .unwrap_or(READ_TIMEOUT_DEFAULT_MS);
    Duration::from_millis(ms)
});

#[derive(Debug, Clone)]
pub struct TcpTestClient {
    addr: SocketAddr,
}

impl TcpTestClient {
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        Self { addr }
    }

    pub async fn connect(&self) -> Result<TcpStream> {
        TcpStream::connect(self.addr).await.map_err(Into::into)
    }

    pub async fn send_and_receive(&self, data: Option<&[u8]>, timeout: Duration) -> Result<Vec<u8>> {
        let mut stream = self.connect().await?;

        if let Some(data) = data {
            stream.write_all(data).await?;
        }

        let mut response = Vec::new();
        let mut buf = [0u8; READ_BUFFER_SIZE];

        match fast_timeout(timeout, async {
            loop {
                match stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if let Some(slice) = buf.get(..n) {
                            response.extend_from_slice(slice);
                        }
                    },
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {
                        tracing::debug!("Connection reset by peer, returning received data so far");
                        break;
                    },
                    Err(e) => return Err(e),
                }
            }
            Ok::<(), std::io::Error>(())
        })
        .await
        {
            Ok(Ok(())) | Err(_) => {},
            Ok(Err(e)) => return Err(e.into()),
        }

        Ok(response)
    }

    pub async fn receive_on_connect(&self) -> Result<Vec<u8>> {
        self.send_and_receive(None, *READ_TIMEOUT).await
    }

    pub async fn receive_on_connect_with_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
        self.send_and_receive(None, timeout).await
    }

    pub async fn send(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.send_and_receive(Some(data), *READ_TIMEOUT).await
    }

    pub async fn send_with_timeout(&self, data: &[u8], timeout: Duration) -> Result<Vec<u8>> {
        self.send_and_receive(Some(data), timeout).await
    }
}
