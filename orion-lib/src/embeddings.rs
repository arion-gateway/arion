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

use std::{
    collections::BTreeMap,
    sync::{Arc, OnceLock},
};

use orion_configuration::config::embeddings::{
    EmbeddingsService, EmbeddingsServiceProvider, LocalEmbeddingsService, RemoteEmbeddingsService,
};
use orion_interner::StringInterner;
use smol_str::SmolStr;

pub mod local;
pub mod remote;

pub type Embedding = Arc<Vec<f32>>;
pub type EmbeddingsServiceId = &'static str;
type EmbeddingsServicesMap = BTreeMap<EmbeddingsServiceId, Arc<dyn EmbeddingsProvider>>;

static EMBEDDINGS_SERVICES: OnceLock<EmbeddingsServicesMap> = OnceLock::new();

#[derive(Debug, thiserror::Error)]
pub enum EmbeddingsServiceError {
    #[error("embeddings services have already been initialized")]
    AlreadyInitialized,
    #[error("duplicate embeddings service: {0}")]
    DuplicateService(SmolStr),
    #[error("embeddings service '{0}' is not configured")]
    ServiceNotFound(String),
    #[error("failed to construct embeddings service '{service}': {reason}")]
    ProviderConstructionFailed { service: SmolStr, reason: String },
}

#[derive(Debug, thiserror::Error)]
pub enum EmbeddingError {
    #[error("embeddings provider is unavailable")]
    Unavailable,
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    #[error("embeddings provider error: {0}")]
    Provider(String),
}

#[async_trait::async_trait]
pub trait EmbeddingsProvider: Send + Sync + std::fmt::Debug {
    fn dimensions(&self) -> usize;

    fn description(&self) -> &str;

    async fn embed_query(&self, text: &str) -> Result<Embedding, EmbeddingError>;

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Embedding>, EmbeddingError> {
        let mut out = Vec::with_capacity(texts.len());
        for text in texts {
            out.push(self.embed_query(text).await?);
        }
        Ok(out)
    }
}

pub fn start_services(services: Vec<EmbeddingsService>) -> Result<(), EmbeddingsServiceError> {
    let mut registry = EmbeddingsServicesMap::new();
    for service in services {
        let name = service.name.clone();
        let id = name.to_static_str();
        let provider = build_provider(&name, service.provider)?;
        if registry.insert(id, provider).is_some() {
            return Err(EmbeddingsServiceError::DuplicateService(name));
        }
    }
    EMBEDDINGS_SERVICES.set(registry).map_err(|_| EmbeddingsServiceError::AlreadyInitialized)
}

pub fn resolve_service(name: &str) -> Option<Arc<dyn EmbeddingsProvider>> {
    EMBEDDINGS_SERVICES.get().and_then(|services| services.get(name).map(Arc::clone))
}

pub fn require_service(name: &str) -> Result<Arc<dyn EmbeddingsProvider>, EmbeddingsServiceError> {
    resolve_service(name).ok_or_else(|| EmbeddingsServiceError::ServiceNotFound(name.to_owned()))
}

fn build_provider(
    service: &SmolStr,
    provider: EmbeddingsServiceProvider,
) -> Result<Arc<dyn EmbeddingsProvider>, EmbeddingsServiceError> {
    match provider {
        EmbeddingsServiceProvider::Local(LocalEmbeddingsService { model_id, model_dir, dimensions }) => {
            let provider =
                local::LocalEmbeddingsProvider::try_new(&model_id, dimensions, model_dir.as_deref()).map_err(|e| {
                    EmbeddingsServiceError::ProviderConstructionFailed { service: service.clone(), reason: e }
                })?;
            Ok(Arc::new(provider))
        },
        EmbeddingsServiceProvider::Remote(RemoteEmbeddingsService { cluster, model_id, path, timeout, dimensions }) => {
            let provider = remote::RemoteEmbeddingsProvider::new(cluster, model_id, path, timeout, dimensions);
            Ok(Arc::new(provider))
        },
    }
}

#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    debug_assert_eq!(a.len(), b.len(), "cosine_similarity: length mismatch");
    let n = a.len().min(b.len());
    let mut acc = 0.0f32;
    for i in 0..n {
        acc += a[i] * b[i];
    }
    acc
}

#[inline]
pub fn normalise_in_place(v: &mut [f32]) {
    let mag2: f32 = v.iter().map(|x| x * x).sum();
    let mag = mag2.sqrt();
    if mag > 0.0 {
        for x in v.iter_mut() {
            *x /= mag;
        }
    }
}

pub fn build_search_text(
    name: &str,
    description: &str,
    input_schema: &serde_json::Map<String, serde_json::Value>,
) -> String {
    let mut buf = String::with_capacity(name.len() + description.len() + 64);
    buf.push_str(name);
    buf.push('\n');
    buf.push_str(description);
    if let Some(serde_json::Value::Object(props)) = input_schema.get("properties") {
        for (arg_name, arg_def) in props {
            buf.push('\n');
            buf.push_str(arg_name);
            if let Some(serde_json::Value::String(arg_desc)) = arg_def.get("description") {
                buf.push_str(": ");
                buf.push_str(arg_desc);
            }
        }
    }
    buf
}

pub fn keyword_overlap_score(description: &str, query_words: &[String]) -> f32 {
    if query_words.is_empty() {
        return 0.0;
    }
    let desc_words: Vec<String> = description.split_whitespace().map(str::to_lowercase).collect();
    let matches = query_words.iter().filter(|w| desc_words.contains(w)).count();
    matches as f32 / query_words.len() as f32
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn cosine_of_identical_normalised_vectors_is_one() {
        let mut a = vec![3.0_f32, 4.0];
        let mut b = vec![3.0_f32, 4.0];
        normalise_in_place(&mut a);
        normalise_in_place(&mut b);
        let s = cosine_similarity(&a, &b);
        assert!((s - 1.0).abs() < 1e-6, "expected 1.0, got {s}");
    }

    #[test]
    fn cosine_of_orthogonal_normalised_vectors_is_zero() {
        let a = vec![1.0_f32, 0.0];
        let b = vec![0.0_f32, 1.0];
        let s = cosine_similarity(&a, &b);
        assert!(s.abs() < 1e-6, "expected 0.0, got {s}");
    }

    #[test]
    fn cosine_of_opposite_normalised_vectors_is_negative_one() {
        let mut a = vec![1.0_f32, 1.0];
        let mut b = vec![-1.0_f32, -1.0];
        normalise_in_place(&mut a);
        normalise_in_place(&mut b);
        let s = cosine_similarity(&a, &b);
        assert!((s + 1.0).abs() < 1e-6, "expected -1.0, got {s}");
    }

    #[test]
    fn normalise_zero_vector_is_unchanged() {
        let mut v = vec![0.0_f32, 0.0, 0.0];
        normalise_in_place(&mut v);
        assert_eq!(v, vec![0.0, 0.0, 0.0]);
    }

    #[test]
    fn build_search_text_includes_name_description_and_arg_metadata() {
        let schema_value = json!({
            "type": "object",
            "properties": {
                "latitude":  { "type": "number", "description": "Decimal latitude" },
                "longitude": { "type": "number", "description": "Decimal longitude" },
                "days":      { "type": "integer" }
            }
        });
        let serde_json::Value::Object(schema) = schema_value else { unreachable!() };
        let text = build_search_text("get_weather_forecast", "Fetches the weather", &schema);
        assert!(text.starts_with("get_weather_forecast\nFetches the weather"));
        assert!(text.contains("latitude: Decimal latitude"));
        assert!(text.contains("longitude: Decimal longitude"));
        assert!(text.contains("\ndays"));
        assert!(!text.contains("days:"));
    }

    #[test]
    fn keyword_overlap_score_is_fraction_of_matching_words() {
        let words = vec!["weather".to_string(), "forecast".to_string(), "today".to_string()];
        assert!((keyword_overlap_score("Get the weather forecast", &words) - 2.0 / 3.0).abs() < 1e-6);
        assert_eq!(keyword_overlap_score("Get the weather forecast", &[]), 0.0);
        assert_eq!(keyword_overlap_score("unrelated description", &words), 0.0);
    }

    #[test]
    fn keyword_overlap_score_is_case_insensitive_via_lowered_query_words() {
        let words = vec!["weather".to_string()];
        assert!((keyword_overlap_score("Get The Weather", &words) - 1.0).abs() < 1e-6);
    }
}
