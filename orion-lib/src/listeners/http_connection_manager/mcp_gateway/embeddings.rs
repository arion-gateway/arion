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
    collections::{HashMap, HashSet},
    sync::Arc,
};

use ahash::RandomState;
use smol_str::SmolStr;

mod remote;

pub(crate) use remote::EmbeddingsClient;

pub type Embedding = Arc<Vec<f32>>;

#[derive(Debug, thiserror::Error)]
pub enum EmbeddingError {
    #[error("dimension mismatch: expected {expected}, got {got}")]
    DimensionMismatch { expected: usize, got: usize },
    #[error("embeddings service error: {0}")]
    Service(String),
}

#[inline]
pub fn cosine_similarity(a: &[f32], b: &[f32]) -> Result<f32, EmbeddingError> {
    if a.len() != b.len() {
        return Err(EmbeddingError::DimensionMismatch { expected: a.len(), got: b.len() });
    }
    Ok(a.iter().zip(b.iter()).map(|(x, y)| x * y).sum())
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

#[derive(Debug)]
pub struct Bm25Document {
    term_frequencies: HashMap<SmolStr, u32, RandomState>,
    token_count: u32,
}

impl Bm25Document {
    pub fn from_text(text: &str) -> Self {
        let mut document = Self { term_frequencies: HashMap::with_hasher(RandomState::new()), token_count: 0 };
        document.add_text(text, 1);
        document
    }

    fn add_text(&mut self, text: &str, weight: u32) {
        for term in tokenize(text) {
            self.token_count += weight;
            *self.term_frequencies.entry(SmolStr::new(&term)).or_default() += weight;
        }
    }

    pub fn from_tool_parts(
        name: &str,
        description: &str,
        _input_schema: &serde_json::Map<String, serde_json::Value>,
    ) -> Self {
        const TOOL_NAME_WEIGHT: u32 = 2;
        const TOOL_DESCRIPTION_WEIGHT: u32 = 1;

        let mut document = Self { term_frequencies: HashMap::with_hasher(RandomState::new()), token_count: 0 };
        document.add_text(name, TOOL_NAME_WEIGHT);
        document.add_text(description, TOOL_DESCRIPTION_WEIGHT);
        document
    }
}

#[allow(clippy::cast_precision_loss)]
pub fn bm25_scores(query: &str, documents: &[&Bm25Document]) -> Vec<f32> {
    const K1: f32 = 1.2;
    const B: f32 = 0.75;

    if documents.is_empty() {
        return Vec::new();
    }

    let query_terms = unique_tokens(query);
    if query_terms.is_empty() {
        return vec![0.0; documents.len()];
    }

    let avg_doc_len = documents.iter().map(|doc| doc.token_count).sum::<u32>() as f32 / documents.len() as f32;
    if avg_doc_len == 0.0 {
        return vec![0.0; documents.len()];
    }

    let document_count = documents.len() as f32;
    let idfs: Vec<f32> = query_terms
        .iter()
        .map(|term| {
            let df = documents.iter().filter(|doc| doc.term_frequencies.contains_key(term.as_str())).count() as f32;
            (1.0 + (document_count - df + 0.5) / (df + 0.5)).ln()
        })
        .collect();

    documents
        .iter()
        .map(|doc| {
            if doc.token_count == 0 {
                return 0.0;
            }

            let doc_len = doc.token_count as f32;
            query_terms
                .iter()
                .zip(&idfs)
                .map(|(term, idf)| {
                    let Some(tf) = doc.term_frequencies.get(term.as_str()).copied() else {
                        return 0.0;
                    };
                    let tf = tf as f32;
                    let denominator = tf + K1 * (1.0 - B + B * (doc_len / avg_doc_len));
                    *idf * (tf * (K1 + 1.0)) / denominator
                })
                .sum()
        })
        .collect()
}

fn unique_tokens(text: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    tokenize(text).into_iter().filter(|term| seen.insert(term.clone())).collect()
}

fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric()).filter(|term| !term.is_empty()).map(str::to_lowercase).collect()
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::indexing_slicing,
        clippy::assertions_on_result_states,
        clippy::cast_precision_loss,
        clippy::items_after_statements
    )]
    use super::*;
    use serde_json::json;

    fn docs(texts: &[&str]) -> Vec<Bm25Document> {
        texts.iter().map(|text| Bm25Document::from_text(text)).collect()
    }

    fn doc_refs(documents: &[Bm25Document]) -> Vec<&Bm25Document> {
        documents.iter().collect()
    }

    fn reference_bm25_scores(query: &str, documents: &[String]) -> Vec<f32> {
        if documents.is_empty() {
            return Vec::new();
        }

        let query_terms = unique_tokens(query);
        if query_terms.is_empty() {
            return vec![0.0; documents.len()];
        }

        let tokenized_documents: Vec<Vec<String>> = documents.iter().map(|doc| tokenize(doc)).collect();
        let avg_doc_len =
            tokenized_documents.iter().map(Vec::len).sum::<usize>() as f32 / tokenized_documents.len() as f32;
        if avg_doc_len == 0.0 {
            return vec![0.0; documents.len()];
        }

        let mut document_frequencies: HashMap<&str, usize> = HashMap::new();
        for terms in &tokenized_documents {
            let unique_doc_terms: HashSet<&str> = terms.iter().map(String::as_str).collect();
            for term in unique_doc_terms {
                *document_frequencies.entry(term).or_default() += 1;
            }
        }

        const K1: f32 = 1.2;
        const B: f32 = 0.75;
        let document_count = tokenized_documents.len() as f32;

        tokenized_documents
            .iter()
            .map(|terms| {
                if terms.is_empty() {
                    return 0.0;
                }
                let mut term_frequencies: HashMap<&str, usize> = HashMap::new();
                for term in terms {
                    *term_frequencies.entry(term.as_str()).or_default() += 1;
                }

                let doc_len = terms.len() as f32;
                query_terms
                    .iter()
                    .map(|term| {
                        let Some(tf) = term_frequencies.get(term.as_str()).copied() else {
                            return 0.0;
                        };
                        let df = document_frequencies.get(term.as_str()).copied().unwrap_or(0) as f32;
                        let idf = (1.0 + (document_count - df + 0.5) / (df + 0.5)).ln();
                        let tf = tf as f32;
                        let denominator = tf + K1 * (1.0 - B + B * (doc_len / avg_doc_len));
                        idf * (tf * (K1 + 1.0)) / denominator
                    })
                    .sum()
            })
            .collect()
    }

    #[test]
    fn cosine_of_identical_normalised_vectors_is_one() {
        let mut a = vec![3.0_f32, 4.0];
        let mut b = vec![3.0_f32, 4.0];
        normalise_in_place(&mut a);
        normalise_in_place(&mut b);
        let s = cosine_similarity(&a, &b).expect("matching dimensions");
        assert!((s - 1.0).abs() < 1e-6, "expected 1.0, got {s}");
    }

    #[test]
    fn cosine_of_orthogonal_normalised_vectors_is_zero() {
        let a = vec![1.0_f32, 0.0];
        let b = vec![0.0_f32, 1.0];
        let s = cosine_similarity(&a, &b).expect("matching dimensions");
        assert!(s.abs() < 1e-6, "expected 0.0, got {s}");
    }

    #[test]
    fn cosine_of_opposite_normalised_vectors_is_negative_one() {
        let mut a = vec![1.0_f32, 1.0];
        let mut b = vec![-1.0_f32, -1.0];
        normalise_in_place(&mut a);
        normalise_in_place(&mut b);
        let s = cosine_similarity(&a, &b).expect("matching dimensions");
        assert!((s + 1.0).abs() < 1e-6, "expected -1.0, got {s}");
    }

    #[test]
    fn cosine_similarity_rejects_dimension_mismatch() {
        let err = cosine_similarity(&[1.0_f32, 0.0], &[1.0]).expect_err("mismatched dimensions should fail");
        assert!(matches!(err, EmbeddingError::DimensionMismatch { expected: 2, got: 1 }));
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
    fn bm25_scores_rank_matching_document_first() {
        let documents = docs(&[
            "get_weather\nGet the weather forecast",
            "get_user\nRetrieve user profile",
            "process_payment\nProcess billing transaction",
        ]);
        let scores = bm25_scores("weather forecast", &doc_refs(&documents));
        assert_eq!(scores.len(), 3);
        assert!(scores[0] > scores[1], "weather doc should outrank user doc: {scores:?}");
        assert!(scores[0] > scores[2], "weather doc should outrank payment doc: {scores:?}");
    }

    #[test]
    fn bm25_tool_document_ignores_argument_metadata() {
        let schema_value = json!({
            "type": "object",
            "properties": {
                "invoice_id": { "type": "string", "description": "Invoice identifier" }
            }
        });
        let serde_json::Value::Object(schema) = schema_value else { unreachable!() };

        let document = Bm25Document::from_tool_parts("billing_lookup", "Billing records", &schema);

        assert_eq!(document.token_count, 6);
        assert_eq!(document.term_frequencies.get("billing"), Some(&3));
        assert_eq!(document.term_frequencies.get("lookup"), Some(&2));
        assert_eq!(document.term_frequencies.get("records"), Some(&1));
        assert_eq!(document.term_frequencies.get("invoice"), None);
        assert_eq!(document.term_frequencies.get("id"), None);
        assert_eq!(document.term_frequencies.get("identifier"), None);
    }

    #[test]
    fn bm25_scores_are_case_insensitive() {
        let documents = docs(&["Get The Weather", "unrelated description"]);
        let scores = bm25_scores("weather", &doc_refs(&documents));
        assert!(scores[0] > scores[1], "weather doc should rank first: {scores:?}");
    }

    #[test]
    fn bm25_document_from_text_counts_tokens_and_terms() {
        let document = Bm25Document::from_text("Weather, WEATHER... invoices_v2!");
        assert_eq!(document.token_count, 4);
        assert_eq!(document.term_frequencies.get("weather"), Some(&2));
        assert_eq!(document.term_frequencies.get("invoices"), Some(&1));
        assert_eq!(document.term_frequencies.get("v2"), Some(&1));
    }

    #[test]
    fn bm25_tool_document_does_not_rank_on_argument_only_matches() {
        let schema_value = json!({
            "type": "object",
            "properties": {
                "longitude": { "type": "number", "description": "Decimal longitude" }
            }
        });
        let serde_json::Value::Object(schema) = schema_value else { unreachable!() };

        let weather = Bm25Document::from_tool_parts("weather", "Fetch forecast", &schema);
        let matching_intent = Bm25Document::from_tool_parts(
            "decimal_longitude_lookup",
            "Find location metadata",
            &serde_json::Map::new(),
        );
        let documents = [&weather, &matching_intent];
        let scores = bm25_scores("decimal longitude", &documents);

        assert!(scores[1] > scores[0], "argument-only metadata should not outrank intent fields: {scores:?}");
    }

    #[test]
    fn bm25_tool_name_match_beats_description_only_match() {
        let name_match = Bm25Document::from_tool_parts("billing", "", &serde_json::Map::new());
        let description_match = Bm25Document::from_tool_parts("lookup", "billing", &serde_json::Map::new());
        let documents = [&name_match, &description_match];
        let scores = bm25_scores("billing", &documents);

        assert!(scores[0] > scores[1], "name match should beat description-only match: {scores:?}");
    }

    #[test]
    fn cached_bm25_scores_match_reference_algorithm() {
        let raw_docs = vec![
            "alpha alpha billing invoices".to_owned(),
            String::new(),
            "!!!".to_owned(),
            "Mixed CASE weather forecast".to_owned(),
            "profile account user".to_owned(),
        ];
        let documents: Vec<Bm25Document> = raw_docs.iter().map(|doc| Bm25Document::from_text(doc)).collect();

        let scores = bm25_scores("ALPHA missing weather", &doc_refs(&documents));
        let reference = reference_bm25_scores("ALPHA missing weather", &raw_docs);

        assert_eq!(scores.len(), reference.len());
        for (score, reference) in scores.iter().zip(reference) {
            assert!((*score - reference).abs() < 1e-6, "score {score} differed from reference {reference}");
        }
    }

    #[test]
    fn bm25_scores_empty_documents_returns_empty_vec() {
        let scores = bm25_scores("weather", &[]);
        assert!(scores.is_empty());
    }

    #[test]
    fn bm25_scores_empty_query_returns_zeroes() {
        let documents = docs(&["weather forecast", "billing invoices"]);
        let scores = bm25_scores("!!!", &doc_refs(&documents));
        assert_eq!(scores, vec![0.0, 0.0]);
    }
}
