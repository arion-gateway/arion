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

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::RemoteEmbeddings;
use arion_interner::InternedStr;
use bytes::Bytes;
use http::{HeaderValue, Method, Request, Uri, header};
use http_body_util::{BodyExt, Full};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::body::poly_body::PolyBody;
use crate::body::response_flags::BodyKind;
use crate::body::timeout_body::TimeoutBody;
use crate::clusters::clusters_manager;
use crate::clusters::clusters_manager::RoutingContext;
use crate::listeners::http_connection_manager::RequestHandler;
use crate::{ArionRequestBody, UpstreamCallOpts};
use crate::{body::instrumented_body::InstrumentedBody, listeners::http_connection_manager::RequestCtx};

use super::{Embedding, EmbeddingError, normalise_in_place};

const DEFAULT_EMBEDDINGS_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_EMBEDDINGS_MAX_BATCH: usize = 64;
const CONTENT_TYPE_JSON: HeaderValue = HeaderValue::from_static("application/json");
const USER_AGENT_EMBEDDINGS: HeaderValue = HeaderValue::from_static("arion/embeddings");

#[derive(Debug)]
pub struct EmbeddingsClient {
    cluster_id: InternedStr,
    model_id: SmolStr,
    path: SmolStr,
    static_uri: Option<Uri>,
    timeout: Duration,
    max_batch_size: usize,
    description: String,
    dimensions: AtomicUsize,
    #[cfg(test)]
    test_mode: Option<TestMode>,
}

impl EmbeddingsClient {
    pub fn from_config(config: RemoteEmbeddings) -> Self {
        let RemoteEmbeddings { cluster, model_id, path, timeout, dimensions, max_batch_size } = config;
        let description = format!("remote://{cluster}{path} ({model_id})");
        let dimensions = AtomicUsize::new(dimensions);
        let static_uri = path.parse::<Uri>().ok();
        Self {
            cluster_id: cluster,
            model_id,
            path,
            static_uri,
            timeout: timeout.unwrap_or(DEFAULT_EMBEDDINGS_TIMEOUT),
            max_batch_size: max_batch_size.unwrap_or(DEFAULT_EMBEDDINGS_MAX_BATCH),
            description,
            dimensions,
            #[cfg(test)]
            test_mode: None,
        }
    }

    /// Maximum inputs per embeddings request; larger catalogs are chunked.
    pub fn max_batch_size(&self) -> usize {
        self.max_batch_size
    }

    async fn post_for_embeddings(
        &self,
        inputs: &[&str],
        req_ctx: &RequestCtx,
    ) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let payload = EmbeddingsRequest { model: self.model_id.as_str(), input: inputs };
        let body_bytes = serde_json::to_vec(&payload).map_err(|e| EmbeddingError::Service(format!("encode: {e}")))?;

        let channels = clusters_manager::get_http_connection(&self.cluster_id, RoutingContext::None)
            .map_err(|e| EmbeddingError::Service(format!("cluster '{}' lookup failed: {e}", self.cluster_id)))?;
        let upstream_authority = channels.upstream_authority().as_str();

        let body: ArionRequestBody = InstrumentedBody::new(
            BodyKind::Request,
            TimeoutBody::new(Some(self.timeout), PolyBody::from(Full::new(Bytes::from(body_bytes)))),
            None,
            |_, _, _, _| {},
        );

        let uri = match &self.static_uri {
            Some(uri) => uri.clone(),
            None => self.path.parse::<Uri>().map_err(|e| EmbeddingError::Service(format!("request: {e}")))?,
        };
        let host = HeaderValue::from_str(upstream_authority)
            .map_err(|e| EmbeddingError::Service(format!("request: {e}")))?;

        let mut request = Request::new(body);
        *request.method_mut() = Method::POST;
        *request.uri_mut() = uri;
        let headers = request.headers_mut();
        headers.reserve(4);
        headers.insert(header::CONTENT_TYPE, CONTENT_TYPE_JSON);
        headers.insert(header::ACCEPT, CONTENT_TYPE_JSON);
        headers.insert(header::HOST, host);
        headers.insert(header::USER_AGENT, USER_AGENT_EMBEDDINGS);

        let upstream_call_opts =
            UpstreamCallOpts { route_timeout: Some(self.timeout), retry_policy: None, ..Default::default() };
        let response = (&channels)
            .to_response(req_ctx, request, upstream_call_opts)
            .await
            .map_err(|e| EmbeddingError::Service(format!("upstream call failed: {e}")))?;

        if !response.status().is_success() {
            return Err(EmbeddingError::Service(format!("upstream returned status {}", response.status())));
        }

        let body_bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|e| EmbeddingError::Service(format!("read response body: {e}")))?
            .to_bytes();

        let parsed: EmbeddingsResponse = serde_json::from_slice(&body_bytes)
            .map_err(|e| EmbeddingError::Service(format!("decode response: {e}")))?;

        if parsed.data.len() != inputs.len() {
            return Err(EmbeddingError::Service(format!(
                "expected {} embeddings, got {}",
                inputs.len(),
                parsed.data.len()
            )));
        }

        let mut data = parsed.data;
        data.sort_by_key(|e| e.index);
        Ok(data.into_iter().map(|e| e.embedding).collect())
    }

    fn validate_dimensions(&self, actual: usize) -> Result<(), EmbeddingError> {
        let expected = self.dimensions.load(Ordering::Acquire);
        if expected == actual { Ok(()) } else { Err(EmbeddingError::DimensionMismatch { expected, got: actual }) }
    }

    fn finalize(&self, mut v: Vec<f32>) -> Result<Embedding, EmbeddingError> {
        if !v.iter().all(|x| x.is_finite()) {
            return Err(EmbeddingError::Service("embedding contains non-finite components".into()));
        }
        self.validate_dimensions(v.len())?;
        normalise_in_place(&mut v);
        Ok(Arc::new(v))
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions.load(Ordering::Acquire)
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub async fn embed_query(&self, text: &str, req_ctx: &RequestCtx) -> Result<Embedding, EmbeddingError> {
        #[cfg(test)]
        if let Some(mode) = &self.test_mode {
            return Self::test_embed_query(mode, text);
        }

        let mut vectors = self.post_for_embeddings(&[text], req_ctx).await?;
        let v = vectors.pop().ok_or_else(|| EmbeddingError::Service("empty result".into()))?;
        self.finalize(v)
    }

    pub async fn embed_batch(&self, texts: &[String], req_ctx: &RequestCtx) -> Result<Vec<Embedding>, EmbeddingError> {
        #[cfg(test)]
        if let Some(mode) = &self.test_mode {
            return Self::test_embed_batch(mode, texts);
        }

        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let inputs: Vec<&str> = texts.iter().map(String::as_str).collect();
        let vectors = self.post_for_embeddings(&inputs, req_ctx).await?;
        let mut out = Vec::with_capacity(vectors.len());
        for v in vectors {
            out.push(self.finalize(v)?);
        }
        Ok(out)
    }

    #[cfg(test)]
    pub fn test_one_hot() -> Arc<Self> {
        Arc::new(Self::new_test("test://one-hot", TestMode::OneHot))
    }

    #[cfg(test)]
    pub fn test_failing() -> Arc<Self> {
        Arc::new(Self::new_test("test://failing", TestMode::Failing))
    }

    #[cfg(test)]
    pub fn test_flaky() -> Arc<Self> {
        Arc::new(Self::new_test("test://flaky", TestMode::Flaky { attempts: AtomicUsize::new(0) }))
    }

    /// Records the input count of every batch request; embeds like `OneHot`.
    #[cfg(test)]
    pub fn test_counting() -> Arc<Self> {
        Arc::new(Self::new_test(
            "test://counting",
            TestMode::Counting { batch_sizes: std::sync::Mutex::new(Vec::new()) },
        ))
    }

    #[cfg(test)]
    pub fn recorded_batch_sizes(&self) -> Vec<usize> {
        match &self.test_mode {
            Some(TestMode::Counting { batch_sizes }) => {
                batch_sizes.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone()
            },
            _ => Vec::new(),
        }
    }

    #[cfg(test)]
    fn new_test(description: &str, test_mode: TestMode) -> Self {
        use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::REMOTE_EMBEDDINGS_PATH;

        Self {
            cluster_id: "test".into(),
            model_id: "test-model".into(),
            path: SmolStr::new_inline(REMOTE_EMBEDDINGS_PATH),
            static_uri: Some(Uri::from_static(REMOTE_EMBEDDINGS_PATH)),
            timeout: DEFAULT_EMBEDDINGS_TIMEOUT,
            max_batch_size: DEFAULT_EMBEDDINGS_MAX_BATCH,
            description: description.to_owned(),
            dimensions: AtomicUsize::new(3),
            test_mode: Some(test_mode),
        }
    }

    #[cfg(test)]
    fn test_embed_query(mode: &TestMode, text: &str) -> Result<Embedding, EmbeddingError> {
        match mode {
            TestMode::OneHot | TestMode::Counting { .. } => Ok(Arc::new(one_hot_for(text))),
            TestMode::Failing => Err(EmbeddingError::Service("boom".into())),
            TestMode::Flaky { .. } => Err(EmbeddingError::Service("never used".into())),
        }
    }

    #[cfg(test)]
    fn test_embed_batch(mode: &TestMode, texts: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        match mode {
            TestMode::OneHot => Ok(texts.iter().map(|t| Arc::new(one_hot_for(t))).collect()),
            TestMode::Counting { batch_sizes } => {
                batch_sizes.lock().unwrap_or_else(std::sync::PoisonError::into_inner).push(texts.len());
                Ok(texts.iter().map(|t| Arc::new(one_hot_for(t))).collect())
            },
            TestMode::Failing => Err(EmbeddingError::Service("boom".into())),
            TestMode::Flaky { attempts } => {
                if attempts.fetch_add(1, Ordering::AcqRel) == 0 {
                    Err(EmbeddingError::Service("transient boom".into()))
                } else {
                    Ok(texts.iter().map(|t| Arc::new(one_hot_for(t))).collect())
                }
            },
        }
    }
}

#[cfg(test)]
#[derive(Debug)]
enum TestMode {
    OneHot,
    Failing,
    Flaky { attempts: AtomicUsize },
    Counting { batch_sizes: std::sync::Mutex<Vec<usize>> },
}

#[cfg(test)]
fn one_hot_for(text: &str) -> Vec<f32> {
    let lower = text.to_ascii_lowercase();
    let w = if lower.contains("weather") || lower.contains("forecast") || lower.contains("temperature") {
        1.0
    } else {
        0.0
    };
    let u = if lower.contains("user") || lower.contains("profile") || lower.contains("account") { 1.0 } else { 0.0 };
    let a = if lower.contains("admin") || lower.contains("delete") { 1.0 } else { 0.0 };
    let mut v = vec![w, u, a];
    if v.iter().all(|&x| x == 0.0) {
        v = vec![1.0, 1.0, 1.0];
    }
    normalise_in_place(&mut v);
    v
}

#[derive(Serialize)]
struct EmbeddingsRequest<'a> {
    model: &'a str,
    input: &'a [&'a str],
}

#[derive(Deserialize)]
struct EmbeddingsResponse {
    data: Vec<EmbeddingObject>,
}

#[derive(Deserialize)]
struct EmbeddingObject {
    index: usize,
    embedding: Vec<f32>,
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn one_hot_test_client_embeds_and_distinguishes_topics() {
        let client = EmbeddingsClient::test_one_hot();
        assert_eq!(client.dimensions(), 3);
        let req_ctx = RequestCtx::default();

        let weather = client.embed_query("weather forecast", &req_ctx).await.unwrap();
        let profile = client.embed_query("user profile", &req_ctx).await.unwrap();
        assert_eq!(weather.len(), 3);
        assert!(weather[0] > 0.9);
        assert!(profile[1] > 0.9);
    }

    #[tokio::test]
    async fn failing_test_client_returns_service_error() {
        let client = EmbeddingsClient::test_failing();
        let req_ctx = RequestCtx::default();
        assert!(matches!(client.embed_query("anything", &req_ctx).await, Err(EmbeddingError::Service(_))));
        assert!(matches!(client.embed_batch(&["a".into()], &req_ctx).await, Err(EmbeddingError::Service(_))));
    }

    #[test]
    fn finalize_rejects_wrong_dimension_and_non_finite() {
        let client = EmbeddingsClient::test_one_hot();
        assert!(matches!(client.finalize(vec![1.0, 2.0]), Err(EmbeddingError::DimensionMismatch { .. })));
        assert!(matches!(client.finalize(vec![f32::NAN, 0.0, 0.0]), Err(EmbeddingError::Service(_))));
    }
}
