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
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::Result;

const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(5);
const READ_BUFFER_SIZE: usize = 4096;

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
        self.send_and_receive(None, DEFAULT_READ_TIMEOUT).await
    }

    pub async fn receive_on_connect_with_timeout(&self, timeout: Duration) -> Result<Vec<u8>> {
        self.send_and_receive(None, timeout).await
    }

    pub async fn send(&self, data: &[u8]) -> Result<Vec<u8>> {
        self.send_and_receive(Some(data), DEFAULT_READ_TIMEOUT).await
    }

    pub async fn send_with_timeout(&self, data: &[u8], timeout: Duration) -> Result<Vec<u8>> {
        self.send_and_receive(Some(data), timeout).await
    }
}
