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

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::Result;

const READ_BUFFER_SIZE: usize = 4096;

// ---------------------------------------------------------------------------
// RawHttpRequestBuilder
// ---------------------------------------------------------------------------

/// Builder for constructing raw HTTP request bytes with full control over
/// malformation. Unlike hyper-based clients, this allows sending invalid
/// methods, URIs, headers, and arbitrary binary data.
pub struct RawHttpRequestBuilder {
    raw_request_line: Option<Vec<u8>>,
    method: Vec<u8>,
    uri: Vec<u8>,
    version: Vec<u8>,
    headers: Vec<HeaderEntry>,
    line_ending: Vec<u8>,
    body: Option<Vec<u8>>,
    include_header_terminator: bool,
}

enum HeaderEntry {
    Structured { name: Vec<u8>, value: Vec<u8> },
    Raw(Vec<u8>),
}

impl Default for RawHttpRequestBuilder {
    fn default() -> Self {
        Self {
            raw_request_line: None,
            method: b"GET".to_vec(),
            uri: b"/".to_vec(),
            version: b"HTTP/1.1".to_vec(),
            headers: Vec::new(),
            line_ending: b"\r\n".to_vec(),
            body: None,
            include_header_terminator: true,
        }
    }
}

impl RawHttpRequestBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn method(mut self, method: impl Into<Vec<u8>>) -> Self {
        self.method = method.into();
        self
    }

    #[must_use]
    pub fn uri(mut self, uri: impl Into<Vec<u8>>) -> Self {
        self.uri = uri.into();
        self
    }

    #[must_use]
    pub fn version(mut self, version: impl Into<Vec<u8>>) -> Self {
        self.version = version.into();
        self
    }

    #[must_use]
    pub fn raw_request_line(mut self, line: impl Into<Vec<u8>>) -> Self {
        self.raw_request_line = Some(line.into());
        self
    }

    #[must_use]
    pub fn header(mut self, name: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push(HeaderEntry::Structured { name: name.into(), value: value.into() });
        self
    }

    #[must_use]
    pub fn raw_header(mut self, line: impl Into<Vec<u8>>) -> Self {
        self.headers.push(HeaderEntry::Raw(line.into()));
        self
    }

    #[must_use]
    pub fn line_ending(mut self, ending: impl Into<Vec<u8>>) -> Self {
        self.line_ending = ending.into();
        self
    }

    #[must_use]
    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = Some(body.into());
        self
    }

    #[must_use]
    pub fn include_header_terminator(mut self, include: bool) -> Self {
        self.include_header_terminator = include;
        self
    }

    #[must_use]
    pub fn host(self, host: &str) -> Self {
        self.header(b"Host", host.as_bytes())
    }

    #[must_use]
    pub fn content_length(self, len: usize) -> Self {
        self.header(b"Content-Length", len.to_string().into_bytes())
    }

    #[must_use]
    pub fn build(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(256);

        match self.raw_request_line {
            Some(line) => {
                buf.extend_from_slice(&line);
                buf.extend_from_slice(&self.line_ending);
            },
            None => {
                buf.extend_from_slice(&self.method);
                buf.push(b' ');
                buf.extend_from_slice(&self.uri);
                buf.push(b' ');
                buf.extend_from_slice(&self.version);
                buf.extend_from_slice(&self.line_ending);
            },
        }

        for entry in &self.headers {
            match entry {
                HeaderEntry::Structured { name, value } => {
                    buf.extend_from_slice(name);
                    buf.extend_from_slice(b": ");
                    buf.extend_from_slice(value);
                    buf.extend_from_slice(&self.line_ending);
                },
                HeaderEntry::Raw(line) => {
                    buf.extend_from_slice(line);
                    buf.extend_from_slice(&self.line_ending);
                },
            }
        }

        if self.include_header_terminator {
            buf.extend_from_slice(&self.line_ending);
        }

        if let Some(body) = &self.body {
            buf.extend_from_slice(body);
        }

        buf
    }
}

// ---------------------------------------------------------------------------
// RawHttpResponse
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct RawHttpResponse {
    pub status_code: u16,
    pub reason: String,
    pub version: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RawHttpResponse {
    pub fn parse(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }

        let text = String::from_utf8_lossy(data);
        let (head, _body_str) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));

        let mut lines = head.split("\r\n");
        let status_line = lines.next()?;

        let mut parts = status_line.splitn(3, ' ');
        let version = parts.next()?.to_string();
        // Reject input that isn't a real HTTP status line — otherwise garbage like
        // a TLS alert record gets misread as an HTTP/0 response.
        if !version.starts_with("HTTP/") {
            return None;
        }
        let status_code: u16 = parts.next()?.parse().ok()?;
        if !(100..=599).contains(&status_code) {
            return None;
        }
        let reason = parts.next().unwrap_or("").to_string();

        let mut headers = Vec::new();
        for line in lines {
            if let Some((name, value)) = line.split_once(':') {
                headers.push((name.trim().to_string(), value.trim().to_string()));
            }
        }

        let body = if let Some(pos) = find_header_end(data) { data[pos..].to_vec() } else { Vec::new() };

        Some(Self { status_code, reason, version, headers, body })
    }

    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        let name_lower = name.to_lowercase();
        self.headers.iter().find(|(n, _)| n.to_lowercase() == name_lower).map(|(_, v)| v.as_str())
    }

    #[must_use]
    pub fn header_all(&self, name: &str) -> Vec<&str> {
        let name_lower = name.to_lowercase();
        self.headers.iter().filter(|(n, _)| n.to_lowercase() == name_lower).map(|(_, v)| v.as_str()).collect()
    }

    #[must_use]
    pub fn has_header(&self, name: &str) -> bool {
        self.header(name).is_some()
    }

    #[must_use]
    pub fn body_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.body).ok()
    }

    pub fn assert_status(&self, expected: u16) {
        assert_eq!(self.status_code, expected, "Expected status {expected}, got {}", self.status_code);
    }

    pub fn assert_status_in(&self, codes: &[u16]) {
        assert!(codes.contains(&self.status_code), "Expected status in {codes:?}, got {}", self.status_code);
    }
}

pub fn assert_rejected(data: &[u8], acceptable_statuses: &[u16]) {
    if data.is_empty() {
        return;
    }
    match RawHttpResponse::parse(data) {
        Some(resp) => {
            assert!(
                acceptable_statuses.contains(&resp.status_code),
                "Expected status in {acceptable_statuses:?} or connection close, got {}",
                resp.status_code
            );
        },
        None => {
            // Unparseable response — treat as rejection
        },
    }
}

fn find_header_end(data: &[u8]) -> Option<usize> {
    data.windows(4).position(|w| w == b"\r\n\r\n").map(|pos| pos + 4)
}

// ---------------------------------------------------------------------------
// PartialSendClient
// ---------------------------------------------------------------------------

/// A TCP client that supports incremental byte sending for tests like slowloris.
/// Unlike `TcpTestClient`, this holds a persistent connection.
pub struct PartialSendClient {
    stream: TcpStream,
}

impl PartialSendClient {
    pub async fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self { stream })
    }

    pub async fn send_bytes(&mut self, data: &[u8]) -> Result<()> {
        self.stream.write_all(data).await?;
        self.stream.flush().await?;
        Ok(())
    }

    pub async fn send_bytes_with_delay(&mut self, chunks: &[&[u8]], delay: Duration) -> Result<()> {
        for (i, chunk) in chunks.iter().enumerate() {
            self.stream.write_all(chunk).await?;
            self.stream.flush().await?;
            if i < chunks.len() - 1 {
                tokio::time::sleep(delay).await;
            }
        }
        Ok(())
    }

    #[allow(clippy::disallowed_methods)]
    pub async fn read_response(&mut self, timeout: Duration) -> Result<Vec<u8>> {
        let mut response = Vec::new();
        let mut buf = [0u8; READ_BUFFER_SIZE];

        match tokio::time::timeout(timeout, async {
            loop {
                match self.stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => response.extend_from_slice(&buf[..n]),
                    Err(e) => return Err(e),
                }
            }
            Ok::<(), std::io::Error>(())
        })
        .await
        {
            Ok(Ok(())) => {},
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => {},
        }

        Ok(response)
    }

    pub async fn shutdown_write(&mut self) -> Result<()> {
        self.stream.shutdown().await?;
        Ok(())
    }
}
