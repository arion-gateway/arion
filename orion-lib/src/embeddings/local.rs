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

use std::{path::Path, sync::Arc};

use fastembed::{
    EmbeddingModel, InitOptions, InitOptionsUserDefined, Pooling, QuantizationMode, TextEmbedding, TokenizerFiles,
    UserDefinedEmbeddingModel,
};
use tokio::task;

use super::{normalise_in_place, Embedding, EmbeddingError, EmbeddingsProvider};

pub struct LocalEmbeddingsProvider {
    model_id: String,
    inner: Arc<TextEmbedding>,
    dimensions: usize,
}

impl std::fmt::Debug for LocalEmbeddingsProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LocalEmbeddingsProvider")
            .field("model_id", &self.model_id)
            .field("dimensions", &self.dimensions)
            .finish_non_exhaustive()
    }
}

impl LocalEmbeddingsProvider {
    pub fn try_new(model_id: &str, dimensions: Option<usize>, model_dir: Option<&str>) -> Result<Self, String> {
        let model = resolve_model(model_id)?;
        let (model_file, default_dimensions) = {
            let model_info = TextEmbedding::get_model_info(&model).map_err(|e| e.to_string())?;
            (model_info.model_file.clone(), model_info.dim)
        };
        let dimensions = dimensions.unwrap_or(default_dimensions);
        let embedder = match model_dir {
            Some(model_dir) => {
                let pooling = TextEmbedding::get_default_pooling_method(&model);
                let quantization = TextEmbedding::get_quantization_mode(&model);
                load_from_model_dir(Path::new(model_dir), &model_file, pooling, quantization)?
            },
            None => TextEmbedding::try_new(InitOptions::new(model)).map_err(|e| e.to_string())?,
        };
        Ok(Self { model_id: model_id.to_string(), inner: Arc::new(embedder), dimensions })
    }

    async fn embed_texts(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let embedder = Arc::clone(&self.inner);
        task::spawn_blocking(move || embedder.embed(texts, None).map_err(|e| EmbeddingError::Provider(e.to_string())))
            .await
            .map_err(|e| EmbeddingError::Provider(format!("blocking embed task failed: {e}")))?
    }
}

#[async_trait::async_trait]
impl EmbeddingsProvider for LocalEmbeddingsProvider {
    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn description(&self) -> &str {
        &self.model_id
    }

    async fn embed_query(&self, text: &str) -> Result<Embedding, EmbeddingError> {
        let result = self.embed_texts(vec![text.to_string()]).await?;
        let mut v = result.into_iter().next().ok_or_else(|| EmbeddingError::Provider("empty result".into()))?;
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
        let result = self.embed_texts(texts.to_vec()).await?;
        let mut out = Vec::with_capacity(result.len());
        for mut v in result {
            if v.len() != self.dimensions {
                return Err(EmbeddingError::DimensionMismatch { expected: self.dimensions, got: v.len() });
            }
            normalise_in_place(&mut v);
            out.push(Arc::new(v));
        }
        Ok(out)
    }
}

fn load_from_model_dir(
    model_dir: &Path,
    model_file: &str,
    pooling: Option<Pooling>,
    quantization: QuantizationMode,
) -> Result<TextEmbedding, String> {
    let tokenizer_files = TokenizerFiles {
        tokenizer_file: read_model_file(model_dir, "tokenizer.json")?,
        config_file: read_model_file(model_dir, "config.json")?,
        special_tokens_map_file: read_model_file(model_dir, "special_tokens_map.json")?,
        tokenizer_config_file: read_model_file(model_dir, "tokenizer_config.json")?,
    };
    let model = UserDefinedEmbeddingModel::new(read_model_file(model_dir, model_file)?, tokenizer_files)
        .with_quantization(quantization);
    let model = match pooling {
        Some(pooling) => model.with_pooling(pooling),
        None => model,
    };
    TextEmbedding::try_new_from_user_defined(model, InitOptionsUserDefined::default())
        .map_err(|e| format!("failed to load local embeddings model from '{}': {e}", model_dir.display()))
}

fn read_model_file(model_dir: &Path, relative_path: &str) -> Result<Vec<u8>, String> {
    let path = model_dir.join(relative_path);
    std::fs::read(&path).map_err(|e| format!("failed to read local embeddings model file '{}': {e}", path.display()))
}

fn resolve_model(id: &str) -> Result<EmbeddingModel, String> {
    match id {
        "BAAI/bge-small-en-v1.5" | "bge-small-en-v1.5" => Ok(EmbeddingModel::BGESmallENV15),
        "BAAI/bge-base-en-v1.5" | "bge-base-en-v1.5" => Ok(EmbeddingModel::BGEBaseENV15),
        "BAAI/bge-large-en-v1.5" | "bge-large-en-v1.5" => Ok(EmbeddingModel::BGELargeENV15),
        "sentence-transformers/all-MiniLM-L6-v2" | "all-MiniLM-L6-v2" => Ok(EmbeddingModel::AllMiniLML6V2),
        other => Err(format!("unknown local embeddings model: '{other}'")),
    }
}
