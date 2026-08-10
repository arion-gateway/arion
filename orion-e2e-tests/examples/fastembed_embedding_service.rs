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

use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use clap::Parser;
use fastembed::{
    EmbeddingModel, InitOptions, InitOptionsUserDefined, Pooling, QuantizationMode, TextEmbedding, TokenizerFiles,
    UserDefinedEmbeddingModel,
};
use http::{Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;

const DEFAULT_BIND: &str = "127.0.0.1:53350";
const DEFAULT_MODEL_ID: &str = "BAAI/bge-small-en-v1.5";
const DEFAULT_MODEL_DIR_ENV: &str = "ORION_MCP_EMBEDDINGS_MODEL_DIR";

#[derive(Debug, Parser)]
#[command(name = "fastembed_embedding_service")]
#[command(about = "Serve FastEmbed embeddings through an OpenAI-compatible /v1/embeddings endpoint.")]
struct Args {
    #[arg(long, default_value = DEFAULT_BIND)]
    bind: SocketAddr,

    #[arg(long, default_value = DEFAULT_MODEL_ID)]
    model_id: String,

    #[arg(long, value_name = "DIR")]
    model_dir: Option<PathBuf>,

    #[arg(long, value_name = "N")]
    dimensions: Option<usize>,
}

#[derive(Clone)]
struct AppState {
    model_id: String,
    dimensions: usize,
    embedder: Arc<TextEmbedding>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = Args::parse();
    if args.model_dir.is_none() {
        args.model_dir = std::env::var_os(DEFAULT_MODEL_DIR_ENV).map(PathBuf::from);
    }
    if args.dimensions == Some(0) {
        return Err("--dimensions must be greater than zero".into());
    }

    let state = Arc::new(load_state(&args)?);
    let listener = TcpListener::bind(args.bind).await?;
    let local_addr = listener.local_addr()?;

    println!(
        "FastEmbed embedding service listening on http://{local_addr}; model_id={}, dimensions={}",
        state.model_id, state.dimensions
    );

    loop {
        let (stream, peer_addr) = listener.accept().await?;
        let state = Arc::clone(&state);

        tokio::spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            let service = service_fn(move |req| handle_request(req, Arc::clone(&state)));

            if let Err(err) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("error serving {peer_addr}: {err}");
            }
        });
    }
}

fn load_state(args: &Args) -> Result<AppState, Box<dyn std::error::Error>> {
    let model = resolve_model(&args.model_id)?;
    let (model_file, default_dimensions) = {
        let model_info = TextEmbedding::get_model_info(&model)?;
        (model_info.model_file.clone(), model_info.dim)
    };
    let dimensions = args.dimensions.unwrap_or(default_dimensions);

    let embedder = match args.model_dir.as_deref() {
        Some(model_dir) => {
            let pooling = TextEmbedding::get_default_pooling_method(&model);
            let quantization = TextEmbedding::get_quantization_mode(&model);
            load_from_model_dir(model_dir, &model_file, pooling, quantization)?
        },
        None => TextEmbedding::try_new(InitOptions::new(model))?,
    };

    Ok(AppState { model_id: args.model_id.clone(), dimensions, embedder: Arc::new(embedder) })
}

async fn handle_request(req: Request<Incoming>, state: Arc<AppState>) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = match (req.method(), req.uri().path()) {
        (&Method::GET, "/health") => json_response(
            StatusCode::OK,
            json!({
                "status": "ok",
                "model": state.model_id,
                "dimensions": state.dimensions,
            }),
        ),
        (&Method::POST, "/v1/embeddings") => handle_embeddings(req, state).await,
        _ => json_response(
            StatusCode::NOT_FOUND,
            json!({
                "error": {
                    "message": "not found",
                    "type": "not_found"
                }
            }),
        ),
    };

    Ok(response)
}

async fn handle_embeddings(req: Request<Incoming>, state: Arc<AppState>) -> Response<Full<Bytes>> {
    let body = match req.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                json!({
                    "error": {
                        "message": format!("failed to read request body: {err}"),
                        "type": "invalid_request"
                    }
                }),
            );
        },
    };

    let request = match serde_json::from_slice::<EmbeddingsRequest>(&body) {
        Ok(request) => request,
        Err(err) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                json!({
                    "error": {
                        "message": format!("invalid embeddings request: {err}"),
                        "type": "invalid_request"
                    }
                }),
            );
        },
    };

    let inputs = request.input.into_vec();
    let model = request.model.unwrap_or_else(|| state.model_id.clone());

    let embedder = Arc::clone(&state.embedder);
    let embeddings = match tokio::task::spawn_blocking(move || embedder.embed(inputs, None)).await {
        Ok(Ok(embeddings)) => embeddings,
        Ok(Err(err)) => {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": {
                        "message": format!("embedding failed: {err}"),
                        "type": "embedding_error"
                    }
                }),
            );
        },
        Err(err) => {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({
                    "error": {
                        "message": format!("embedding task failed: {err}"),
                        "type": "embedding_error"
                    }
                }),
            );
        },
    };

    if let Some(actual) = embeddings.iter().map(Vec::len).find(|actual| *actual != state.dimensions) {
        return json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({
                "error": {
                    "message": format!(
                        "embedding dimension mismatch: expected {}, got {}",
                        state.dimensions, actual
                    ),
                    "type": "dimension_mismatch"
                }
            }),
        );
    }

    let data = embeddings
        .into_iter()
        .enumerate()
        .map(|(index, embedding)| EmbeddingData { object: "embedding", index, embedding })
        .collect::<Vec<_>>();

    json_response(StatusCode::OK, EmbeddingsResponse { object: "list", model, data })
}

fn json_response<T: Serialize>(status: StatusCode, body: T) -> Response<Full<Bytes>> {
    let encoded = serde_json::to_vec(&body).unwrap_or_else(|_| b"{}".to_vec());

    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(encoded)))
        .unwrap_or_else(|_| Response::new(Full::new(Bytes::new())))
}

#[derive(Debug, Deserialize)]
struct EmbeddingsRequest {
    model: Option<String>,
    input: EmbeddingsInput,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum EmbeddingsInput {
    One(String),
    Many(Vec<String>),
}

impl EmbeddingsInput {
    fn into_vec(self) -> Vec<String> {
        match self {
            Self::One(input) => vec![input],
            Self::Many(inputs) => inputs,
        }
    }
}

#[derive(Debug, Serialize)]
struct EmbeddingsResponse {
    object: &'static str,
    model: String,
    data: Vec<EmbeddingData>,
}

#[derive(Debug, Serialize)]
struct EmbeddingData {
    object: &'static str,
    index: usize,
    embedding: Vec<f32>,
}

fn load_from_model_dir(
    model_dir: &Path,
    model_file: &str,
    pooling: Option<Pooling>,
    quantization: QuantizationMode,
) -> Result<TextEmbedding, Box<dyn std::error::Error>> {
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

    Ok(TextEmbedding::try_new_from_user_defined(model, InitOptionsUserDefined::default())?)
}

fn read_model_file(model_dir: &Path, relative_path: &str) -> Result<Vec<u8>, std::io::Error> {
    std::fs::read(model_dir.join(relative_path))
}

fn resolve_model(id: &str) -> Result<EmbeddingModel, Box<dyn std::error::Error>> {
    let model = match id {
        "BAAI/bge-small-en-v1.5" | "bge-small-en-v1.5" => EmbeddingModel::BGESmallENV15,
        "sentence-transformers/all-MiniLM-L6-v2" | "all-MiniLM-L6-v2" => EmbeddingModel::AllMiniLML6V2,
        "BAAI/bge-small-zh-v1.5" | "bge-small-zh-v1.5" => EmbeddingModel::BGESmallZHV15,
        other => return Err(format!("unknown local embeddings model: '{other}'").into()),
    };

    Ok(model)
}
