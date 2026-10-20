// Copyright 2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Tool search: candidate selection, vector/BM25 ranking, and lazy embedding.

use std::sync::{Arc as StdArc, atomic::Ordering};

#[cfg(feature = "metrics")]
use crate::get_shard_id;
use crate::with_metric;
use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::DisclosureMode;
#[cfg(feature = "metrics")]
use opentelemetry::KeyValue;
use rmcp::model::Tool;
use smol_str::SmolStr;
use tracing::{debug, warn};

use super::{
    CallToolError, CompactToolDefinition, ToolEntry, ToolSearchRequest, ToolSearchScope, ToolsRegistry,
    entry::validate_positive_limit,
};
use crate::listeners::http_connection_manager::{
    RequestCtx,
    mcp_gateway::embeddings::{self, Bm25Document},
};

impl ToolsRegistry {
    pub(super) async fn embed_unembedded_tools(&self, req_ctx: &RequestCtx) {
        let Some(client) = self.embeddings_client.as_ref() else {
            return;
        };
        if !self.embeddings_pending.swap(false, Ordering::AcqRel) {
            return;
        }
        let _refresh = self.embedding_refresh.lock().await;

        let mut entries: Vec<StdArc<ToolEntry>> = Vec::new();
        let mut texts: Vec<String> = Vec::new();
        {
            let pinned = self.tools.pin();
            for (id, tool) in &pinned {
                if tool.embedding.load().is_some() {
                    continue;
                }
                texts.push(embeddings::build_search_text(&id.qualified_name(), &tool.description, &tool.input_schema));
                entries.push(StdArc::clone(tool));
            }
        }

        if texts.is_empty() {
            return;
        }

        // One bounded request per chunk, storing each chunk's vectors as soon
        // as it succeeds so a later failure never discards completed work.
        let batch_size = client.max_batch_size();
        for (chunk_entries, chunk_texts) in entries.chunks(batch_size).zip(texts.chunks(batch_size)) {
            match client.embed_batch(chunk_texts, req_ctx).await {
                Ok(vectors) => {
                    for (entry, vec) in chunk_entries.iter().zip(vectors) {
                        entry.embedding.store(Some(vec));
                    }
                    debug!(target: "mcp_gateway", "Embedded {} tools via {}", chunk_entries.len(), client.description());
                },
                Err(err) => {
                    self.embeddings_pending.store(true, Ordering::Release);
                    warn!(
                        target: "mcp_gateway",
                        "embed_batch failed for {} tools: {err}; tool_search will use BM25 fallback",
                        chunk_entries.len()
                    );
                    #[cfg(feature = "metrics")]
                    let shard_id = get_shard_id!();
                    with_metric!(
                        arion_metrics::metrics::mcp::EMBEDDING_FAILURES_TOTAL,
                        add,
                        1,
                        shard_id,
                        &[KeyValue::new("endpoint", client.description().to_owned())]
                    );
                },
            }
        }
    }

    fn validate_search_request(
        &self,
        request: ToolSearchRequest,
    ) -> Result<(String, Option<SmolStr>, usize), CallToolError> {
        if request.query.trim().is_empty() {
            return Err(CallToolError::ValidationError("query must be non-empty".to_owned()));
        }
        validate_positive_limit(request.limit)?;
        let limit = request.limit.unwrap_or_else(|| self.max_allowable_results()).min(self.max_allowable_results());
        Ok((request.query, request.namespace, limit))
    }

    pub async fn search_tools(
        &self,
        req_ext: &http::Extensions,
        req_ctx: &RequestCtx,
        request: ToolSearchRequest,
        scope: ToolSearchScope,
    ) -> Result<Vec<Tool>, CallToolError> {
        Ok(self
            .ranked_entries(req_ext, req_ctx, request, scope)
            .await?
            .into_iter()
            .map(|entry| entry.tool.clone())
            .collect())
    }

    pub async fn catalog_search_tools(
        &self,
        req_ext: &http::Extensions,
        req_ctx: &RequestCtx,
        request: ToolSearchRequest,
    ) -> Result<Vec<CompactToolDefinition>, CallToolError> {
        Ok(self
            .ranked_entries(req_ext, req_ctx, request, ToolSearchScope::All)
            .await?
            .into_iter()
            .map(|entry| entry.compact())
            .collect())
    }

    async fn ranked_entries(
        &self,
        req_ext: &http::Extensions,
        req_ctx: &RequestCtx,
        request: ToolSearchRequest,
        scope: ToolSearchScope,
    ) -> Result<Vec<StdArc<ToolEntry>>, CallToolError> {
        let (query, namespace, limit) = self.validate_search_request(request)?;

        let mut candidates = self.permitted_snapshot(req_ext);
        candidates.retain(|entry| {
            namespace.as_ref().is_none_or(|namespace| namespace == &entry.id.namespace)
                && (scope != ToolSearchScope::Progressive || entry.disclosure_mode == DisclosureMode::Progressive)
        });

        let mut scored = match self.vector_scores(&query, req_ctx, &candidates).await {
            Some(scored) => scored,
            None => Self::bm25_scores(&query, &candidates),
        };

        scored.sort_by(|a, b| b.0.total_cmp(&a.0).then_with(|| a.1.id.cmp(&b.1.id)));
        scored.truncate(limit);
        Ok(scored.into_iter().map(|(_, entry)| entry).collect())
    }

    async fn vector_scores(
        &self,
        query: &str,
        req_ctx: &RequestCtx,
        candidates: &[StdArc<ToolEntry>],
    ) -> Option<Vec<(f32, StdArc<ToolEntry>)>> {
        let client = self.embeddings_client.as_ref()?;
        let query_embedding = match client.embed_query(query, req_ctx).await {
            Ok(v) => v,
            Err(err) => {
                warn!(target: "mcp_gateway", "embed_query failed: {err}; cosine ranking unavailable");
                #[cfg(feature = "metrics")]
                let shard_id = get_shard_id!();
                with_metric!(
                    arion_metrics::metrics::mcp::EMBEDDING_FAILURES_TOTAL,
                    add,
                    1,
                    shard_id,
                    &[KeyValue::new("endpoint", client.description().to_owned())]
                );
                return None;
            },
        };

        let mut scored = Vec::with_capacity(candidates.len());
        for entry in candidates {
            let Some(tool_embedding) = entry.embedding.load_full() else {
                debug!(
                    target: "mcp_gateway",
                    "tool '{}' has no embedding yet; using BM25 for the candidate set",
                    entry.id.name
                );
                return None;
            };
            let score = match embeddings::cosine_similarity(query_embedding.as_slice(), tool_embedding.as_slice()) {
                Ok(score) => score,
                Err(err) => {
                    warn!(
                        target: "mcp_gateway",
                        "cosine similarity failed for tool '{}': {err}; cosine ranking unavailable",
                        entry.id.name
                    );
                    return None;
                },
            };
            scored.push((score, StdArc::clone(entry)));
        }
        Some(scored)
    }

    fn bm25_scores(query: &str, candidates: &[StdArc<ToolEntry>]) -> Vec<(f32, StdArc<ToolEntry>)> {
        let documents: Vec<&Bm25Document> = candidates.iter().map(|entry| &entry.bm25_doc).collect();
        embeddings::bm25_scores(query, &documents)
            .into_iter()
            .zip(candidates.iter())
            .map(|(score, entry)| (score, StdArc::clone(entry)))
            .collect()
    }
}
