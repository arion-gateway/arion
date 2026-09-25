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

use std::net::SocketAddr;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_rustls::client::TlsStream;

use crate::{Error, Result, TlsClientConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Version {
    V1,
    V2,
}

#[derive(Debug, Clone)]
enum HeaderSource {
    Standard(Version, SocketAddr, SocketAddr, Vec<(u8, Vec<u8>)>),
    None,
    Raw(Vec<u8>),
}

pub struct ProxyProtocolTcpClient {
    header: HeaderSource,
}

impl ProxyProtocolTcpClient {
    #[must_use]
    pub fn v1(claimed_src: SocketAddr, claimed_dst: SocketAddr) -> Self {
        Self { header: HeaderSource::Standard(Version::V1, claimed_src, claimed_dst, Vec::new()) }
    }

    #[must_use]
    pub fn v2(claimed_src: SocketAddr, claimed_dst: SocketAddr) -> Self {
        Self { header: HeaderSource::Standard(Version::V2, claimed_src, claimed_dst, Vec::new()) }
    }

    #[must_use]
    pub fn no_header() -> Self {
        Self { header: HeaderSource::None }
    }

    #[must_use]
    pub fn raw_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        Self { header: HeaderSource::Raw(bytes.into()) }
    }

    #[must_use]
    pub fn with_tlv(mut self, kind: u8, value: impl Into<Vec<u8>>) -> Self {
        if let HeaderSource::Standard(Version::V2, _, _, ref mut tlvs) = self.header {
            tlvs.push((kind, value.into()));
        }
        self
    }

    fn build_header(&self) -> Result<Vec<u8>> {
        match &self.header {
            HeaderSource::None => Ok(Vec::new()),
            HeaderSource::Raw(bytes) => Ok(bytes.clone()),
            HeaderSource::Standard(Version::V1, src, dst, _) => Self::build_v1(*src, *dst),
            HeaderSource::Standard(Version::V2, src, dst, tlvs) => Self::build_v2(*src, *dst, tlvs),
        }
    }

    fn build_v1(src: SocketAddr, dst: SocketAddr) -> Result<Vec<u8>> {
        match (src, dst) {
            (SocketAddr::V4(s), SocketAddr::V4(d)) => {
                Ok(format!("PROXY TCP4 {} {} {} {}\r\n", s.ip(), d.ip(), s.port(), d.port()).into_bytes())
            },
            (SocketAddr::V6(s), SocketAddr::V6(d)) => {
                Ok(format!("PROXY TCP6 {} {} {} {}\r\n", s.ip(), d.ip(), s.port(), d.port()).into_bytes())
            },
            _ => Err(Error::Config("PROXY v1 requires matching IPv4 or IPv6 family".into())),
        }
    }

    fn build_v2(src: SocketAddr, dst: SocketAddr, tlvs: &[(u8, Vec<u8>)]) -> Result<Vec<u8>> {
        let mut builder = ppp::v2::Builder::with_addresses(
            ppp::v2::Version::Two | ppp::v2::Command::Proxy,
            ppp::v2::Protocol::Stream,
            (src, dst),
        );
        for (kind, value) in tlvs {
            builder =
                builder.write_tlv(*kind, value).map_err(|e| Error::Config(format!("Failed to write TLV: {e}")))?;
        }
        builder.build().map_err(|e| Error::Config(format!("Failed to build PROXY v2 header: {e}")))
    }

    pub async fn connect(&self, addr: SocketAddr) -> Result<TcpStream> {
        let header = self.build_header()?;
        let mut stream = TcpStream::connect(addr).await?;
        if !header.is_empty() {
            stream.write_all(&header).await?;
            stream.flush().await?;
        }
        Ok(stream)
    }

    pub async fn connect_tls(
        &self,
        addr: SocketAddr,
        server_name: impl Into<String>,
        tls: TlsClientConfig,
    ) -> Result<TlsStream<TcpStream>> {
        let stream = self.connect(addr).await?;
        tls.handshake_on(stream, server_name).await
    }
}
