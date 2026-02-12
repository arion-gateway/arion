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
use http::{Method, Request, StatusCode, Uri};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use tracing::debug;

use crate::{Error, Result};

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct TestResponse {
    pub status: StatusCode,
    pub headers: http::HeaderMap,
    pub body: Bytes,
}

impl TestResponse {
    #[must_use]
    pub fn is_success(&self) -> bool {
        self.status.is_success()
    }

    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|v| v.to_str().ok())
    }

    #[must_use]
    pub fn header_all(&self, name: &str) -> Vec<&str> {
        self.headers.get_all(name).iter().filter_map(|v| v.to_str().ok()).collect()
    }

    #[must_use]
    pub fn body_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.body).ok()
    }

    #[track_caller]
    pub fn assert_status(&self, expected: StatusCode) {
        assert_eq!(self.status, expected, "Expected status {expected}, got {}", self.status);
    }

    #[track_caller]
    pub fn assert_body(&self, expected: &str) {
        let actual = self.body_str().unwrap_or("<non-utf8>");
        assert_eq!(actual, expected, "Body mismatch");
    }

    #[track_caller]
    pub fn assert_body_contains(&self, expected: &str) {
        let actual = self.body_str().unwrap_or("<non-utf8>");
        assert!(actual.contains(expected), "Body does not contain '{expected}'. Body: {actual}");
    }

    #[track_caller]
    pub fn assert_header(&self, name: &str, expected: &str) {
        let actual = self.header(name);
        assert_eq!(actual, Some(expected), "Header '{name}' expected '{expected}', got {actual:?}");
    }
}

pub struct TestClient {
    addr: SocketAddr,
    client: Client<HttpConnector, Full<Bytes>>,
    timeout: Duration,
    default_headers: Vec<(String, String)>,
}

impl TestClient {
    #[must_use]
    pub fn new(addr: SocketAddr) -> Self {
        let client = Client::builder(TokioExecutor::new()).build_http();
        Self { addr, client, timeout: DEFAULT_TIMEOUT, default_headers: vec![] }
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
            format!("http://{}{}", self.addr, path).parse().map_err(|e| Error::Http(format!("Invalid URI: {e}")))?;

        debug!(?method, ?uri, "Sending request");

        let mut builder = Request::builder().method(method).uri(uri);

        for (name, value) in &self.default_headers {
            builder = builder.header(name.as_str(), value.as_str());
        }

        let request = match body {
            Some(b) => builder.body(Full::new(b)).map_err(|e| Error::Http(format!("Failed to build request: {e}")))?,
            None => builder
                .body(Full::new(Bytes::new()))
                .map_err(|e| Error::Http(format!("Failed to build request: {e}")))?,
        };

        let response = tokio::time::timeout(self.timeout, self.client.request(request))
            .await
            .map_err(|_| Error::RequestTimeout(self.timeout))?
            .map_err(Error::Hyper)?;

        self.convert_response(response).await
    }

    #[allow(clippy::disallowed_methods)]
    pub async fn send(&self, request: RequestBuilder) -> Result<TestResponse> {
        let uri: Uri = format!("http://{}{}", self.addr, request.path)
            .parse()
            .map_err(|e| Error::Http(format!("Invalid URI: {e}")))?;

        let mut builder = Request::builder().method(request.method).uri(uri);

        for (name, value) in &self.default_headers {
            builder = builder.header(name.as_str(), value.as_str());
        }

        for (name, value) in &request.headers {
            builder = builder.header(name.as_str(), value.as_str());
        }

        let http_request =
            builder.body(Full::new(request.body)).map_err(|e| Error::Http(format!("Failed to build request: {e}")))?;

        let response = tokio::time::timeout(self.timeout, self.client.request(http_request))
            .await
            .map_err(|_| Error::RequestTimeout(self.timeout))?
            .map_err(Error::Hyper)?;

        self.convert_response(response).await
    }

    async fn convert_response(&self, response: http::Response<Incoming>) -> Result<TestResponse> {
        let status = response.status();
        let headers = response.headers().clone();

        let body: Bytes =
            response.collect().await.map_err(|e| Error::Http(format!("Failed to read response body: {e}")))?.to_bytes();

        debug!(?status, body_len = body.len(), "Received response");

        Ok(TestResponse { status, headers, body })
    }
}

#[derive(Debug, Clone)]
pub struct RequestBuilder {
    method: Method,
    path: String,
    headers: Vec<(String, String)>,
    body: Bytes,
}

impl RequestBuilder {
    #[must_use]
    pub fn get(path: impl Into<String>) -> Self {
        Self { method: Method::GET, path: path.into(), headers: vec![], body: Bytes::new() }
    }

    #[must_use]
    pub fn post(path: impl Into<String>) -> Self {
        Self { method: Method::POST, path: path.into(), headers: vec![], body: Bytes::new() }
    }

    #[must_use]
    pub fn put(path: impl Into<String>) -> Self {
        Self { method: Method::PUT, path: path.into(), headers: vec![], body: Bytes::new() }
    }

    #[must_use]
    pub fn delete(path: impl Into<String>) -> Self {
        Self { method: Method::DELETE, path: path.into(), headers: vec![], body: Bytes::new() }
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
    pub fn host(self, host: impl Into<String>) -> Self {
        self.header("host", host)
    }
}
