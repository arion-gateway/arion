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

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::RemoteEmbeddings;
use arion_interner::StringInterner;
use bytes::Bytes;
use http::{header, Method, Request};
use http_body_util::{BodyExt, Full};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::body::poly_body::PolyBody;
use crate::body::response_flags::BodyKind;
use crate::body::timeout_body::TimeoutBody;
use crate::clusters::clusters_manager;
use crate::clusters::clusters_manager::RoutingContext;
use crate::listeners::http_connection_manager::RequestHandler;
use crate::{body::instrumented_body::InstrumentedBody, listeners::http_connection_manager::RequestCtx};
use crate::{ArionRequestBody, UpstreamCallOpts};

use super::{normalise_in_place, Embedding, EmbeddingError};

const DEFAULT_EMBEDDINGS_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub struct EmbeddingsClient {
    cluster_id: &'static str,
    cluster_label: SmolStr,
    model_id: SmolStr,
    path: SmolStr,
    timeout: Duration,
    description: String,
    dimensions: AtomicUsize,
    #[cfg(test)]
    test_mode: Option<TestMode>,
}

impl EmbeddingsClient {
    pub fn from_config(config: RemoteEmbeddings) -> Self {
        Self::new(config.cluster, config.model_id, config.path, config.timeout, config.dimensions)
    }

    pub fn new(
        cluster: SmolStr,
        model_id: SmolStr,
        path: SmolStr,
        timeout: Option<Duration>,
        dimensions: usize,
    ) -> Self {
        let description = format!("remote://{cluster}{path} ({model_id})");
        let dimensions = AtomicUsize::new(dimensions);
        let cluster_id = cluster.as_str().to_static_str();
        Self {
            cluster_id,
            cluster_label: cluster,
            model_id,
            path,
            timeout: timeout.unwrap_or(DEFAULT_EMBEDDINGS_TIMEOUT),
            description,
            dimensions,
            #[cfg(test)]
            test_mode: None,
        }
    }

    async fn post_for_embeddings(
        &self,
        inputs: &[&str],
        req_ctx: &RequestCtx,
    ) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let payload = EmbeddingsRequest { model: self.model_id.as_str(), input: inputs };
        let body_bytes = serde_json::to_vec(&payload).map_err(|e| EmbeddingError::Service(format!("encode: {e}")))?;

        let channels = clusters_manager::get_http_connection(self.cluster_id, RoutingContext::None)
            .map_err(|e| EmbeddingError::Service(format!("cluster '{}' lookup failed: {e}", self.cluster_label)))?;
        let upstream_authority = channels.upstream_authority().as_str();

        let body: ArionRequestBody = InstrumentedBody::new(
            BodyKind::Request,
            TimeoutBody::new(Some(self.timeout), PolyBody::from(Full::new(Bytes::from(body_bytes)))),
            None,
            |_, _, _, _| {},
        );

        let request = Request::builder()
            .method(Method::POST)
            .uri(self.path.as_str())
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json")
            .header(header::HOST, upstream_authority)
            .header(header::USER_AGENT, "arion/embeddings")
            .body(body)
            .map_err(|e| EmbeddingError::Service(format!("request: {e}")))?;

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
        if expected == actual {
            Ok(())
        } else {
            Err(EmbeddingError::DimensionMismatch { expected, got: actual })
        }
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

    #[cfg(test)]
    fn new_test(description: &str, test_mode: TestMode) -> Self {
        use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::REMOTE_EMBEDDINGS_PATH;

        Self {
            cluster_id: "",
            cluster_label: "test".into(),
            model_id: "test-model".into(),
            path: SmolStr::new_inline(REMOTE_EMBEDDINGS_PATH),
            timeout: DEFAULT_EMBEDDINGS_TIMEOUT,
            description: description.to_owned(),
            dimensions: AtomicUsize::new(3),
            test_mode: Some(test_mode),
        }
    }

    #[cfg(test)]
    fn test_embed_query(mode: &TestMode, text: &str) -> Result<Embedding, EmbeddingError> {
        match mode {
            TestMode::OneHot => Ok(Arc::new(one_hot_for(text))),
            TestMode::Failing => Err(EmbeddingError::Service("boom".into())),
            TestMode::Flaky { .. } => Err(EmbeddingError::Service("never used".into())),
        }
    }

    #[cfg(test)]
    fn test_embed_batch(mode: &TestMode, texts: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        match mode {
            TestMode::OneHot => Ok(texts.iter().map(|text| Arc::new(one_hot_for(text))).collect()),
            TestMode::Failing => Err(EmbeddingError::Service("boom".into())),
            TestMode::Flaky { attempts } => {
                if attempts.fetch_add(1, Ordering::Relaxed) == 0 {
                    return Err(EmbeddingError::Service("first call fails".into()));
                }
                Ok(texts.iter().map(|_| Arc::new(vec![1.0_f32, 0.0, 0.0])).collect())
            },
        }
    }
}

#[derive(Serialize)]
struct EmbeddingsRequest<'a> {
    model: &'a str,
    input: &'a [&'a str],
}

#[derive(Deserialize)]
struct EmbeddingsResponse {
    data: Vec<EmbeddingDatum>,
}

#[derive(Deserialize)]
struct EmbeddingDatum {
    #[serde(default)]
    index: usize,
    embedding: Vec<f32>,
}

#[cfg(test)]
#[derive(Debug)]
enum TestMode {
    OneHot,
    Failing,
    Flaky { attempts: AtomicUsize },
}

#[cfg(test)]
#[allow(clippy::indexing_slicing)]
fn one_hot_for(text: &str) -> Vec<f32> {
    let lower = text.to_lowercase();
    let mut v = vec![0.0_f32; 3];
    if lower.contains("weather") {
        v[0] = 1.0;
    } else if lower.contains("user") {
        v[1] = 1.0;
    } else if lower.contains("admin") {
        v[2] = 1.0;
    } else {
        let value = 1.0_f32 / 3.0_f32.sqrt();
        v = vec![value, value, value];
    }
    v
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing, clippy::assertions_on_result_states)]
    use super::*;

    #[test]
    fn finalize_rejects_non_finite_components() {
        let client = EmbeddingsClient::test_one_hot();
        assert!(matches!(client.finalize(vec![f32::INFINITY, 0.0, 0.0]), Err(EmbeddingError::Service(_))));
        assert!(matches!(client.finalize(vec![f32::NAN, 0.0, 0.0]), Err(EmbeddingError::Service(_))));
        assert!(client.finalize(vec![1.0, 0.0, 0.0]).is_ok());
    }
}
