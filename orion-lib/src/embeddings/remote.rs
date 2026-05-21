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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::{header, Method, Request};
use http_body_util::{BodyExt, Full};
use orion_interner::StringInterner;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::body::instrumented_body::InstrumentedBody;
use crate::body::poly_body::PolyBody;
use crate::body::response_flags::BodyKind;
use crate::body::timeout_body::TimeoutBody;
use crate::clusters::clusters_manager;
use crate::clusters::clusters_manager::RoutingContext;
use crate::listeners::http_connection_manager::{RequestHandler, TransactionContext};
use crate::{OrionRequestBody, RequestContext};

use super::{normalise_in_place, Embedding, EmbeddingError, EmbeddingsProvider};

#[derive(Debug)]
pub struct RemoteEmbeddingsProvider {
    cluster_id: &'static str,
    cluster_label: SmolStr,
    model_id: SmolStr,
    path: String,
    timeout: Option<Duration>,
    description: String,
    dimensions: usize,
}

impl RemoteEmbeddingsProvider {
    pub fn new(
        cluster: SmolStr,
        model_id: SmolStr,
        path: String,
        timeout: Option<Duration>,
        dimensions: Option<usize>,
    ) -> Self {
        let description = format!("remote://{cluster}{path} ({model_id})");
        let dimensions = dimensions.unwrap_or_else(|| default_dimensions_for_model(&model_id));
        let cluster_id = cluster.as_str().to_static_str();
        Self { cluster_id, cluster_label: cluster, model_id, path, timeout, description, dimensions }
    }

    async fn post_for_embeddings(&self, inputs: Vec<String>) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let payload = EmbeddingsRequest { model: &self.model_id, input: &inputs };
        let body_bytes = serde_json::to_vec(&payload).map_err(|e| EmbeddingError::Provider(format!("encode: {e}")))?;

        let channels = clusters_manager::get_http_connection(self.cluster_id, RoutingContext::None)
            .map_err(|e| EmbeddingError::Provider(format!("cluster '{}' lookup failed: {e}", self.cluster_label)))?;
        let upstream_authority = channels.upstream_authority().as_str();

        let body: OrionRequestBody = InstrumentedBody::new(
            BodyKind::Request,
            TimeoutBody::new(self.timeout, PolyBody::from(Full::new(Bytes::from(body_bytes)))),
            None,
            |_, _, _, _| {},
        );

        let request = Request::builder()
            .method(Method::POST)
            .uri(self.path.as_str())
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::ACCEPT, "application/json")
            .header(header::HOST, upstream_authority)
            .header(header::USER_AGENT, "orion/embeddings")
            .body(body)
            .map_err(|e| EmbeddingError::Provider(format!("request: {e}")))?;

        let request_context = RequestContext { route_timeout: self.timeout, retry_policy: None, ..Default::default() };
        let response = (&channels)
            .to_response(&TransactionContext::default(), request, request_context)
            .await
            .map_err(|e| EmbeddingError::Provider(format!("upstream call failed: {e}")))?;

        if !response.status().is_success() {
            return Err(EmbeddingError::Provider(format!("upstream returned status {}", response.status())));
        }

        let body_bytes = response
            .into_body()
            .collect()
            .await
            .map_err(|e| EmbeddingError::Provider(format!("read response body: {e}")))?
            .to_bytes();

        let parsed: EmbeddingsResponse = serde_json::from_slice(&body_bytes)
            .map_err(|e| EmbeddingError::Provider(format!("decode response: {e}")))?;

        if parsed.data.len() != inputs.len() {
            return Err(EmbeddingError::Provider(format!(
                "expected {} embeddings, got {}",
                inputs.len(),
                parsed.data.len()
            )));
        }

        let mut data = parsed.data;
        data.sort_by_key(|e| e.index);
        Ok(data.into_iter().map(|e| e.embedding).collect())
    }
}

#[async_trait::async_trait]
impl EmbeddingsProvider for RemoteEmbeddingsProvider {
    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn description(&self) -> &str {
        &self.description
    }

    async fn embed_query(&self, text: &str) -> Result<Embedding, EmbeddingError> {
        let mut vectors = self.post_for_embeddings(vec![text.to_string()]).await?;
        let mut v = vectors.pop().ok_or_else(|| EmbeddingError::Provider("empty result".into()))?;
        if v.len() != self.dimensions {
            return Err(EmbeddingError::DimensionMismatch { expected: self.dimensions, got: v.len() });
        }
        normalise_in_place(&mut v);
        Ok(Arc::new(v))
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let vectors = self.post_for_embeddings(texts.to_vec()).await?;
        let mut out = Vec::with_capacity(vectors.len());
        for mut v in vectors {
            if v.len() != self.dimensions {
                return Err(EmbeddingError::DimensionMismatch { expected: self.dimensions, got: v.len() });
            }
            normalise_in_place(&mut v);
            out.push(Arc::new(v));
        }
        Ok(out)
    }
}

#[derive(Serialize)]
struct EmbeddingsRequest<'a> {
    model: &'a str,
    input: &'a [String],
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

fn default_dimensions_for_model(model_id: &str) -> usize {
    match model_id {
        "BAAI/bge-small-en-v1.5" | "bge-small-en-v1.5" => 384,
        "sentence-transformers/all-MiniLM-L6-v2" | "all-MiniLM-L6-v2" => 384,
        "BAAI/bge-small-zh-v1.5" | "bge-small-zh-v1.5" => 512,
        _ => 384,
    }
}
