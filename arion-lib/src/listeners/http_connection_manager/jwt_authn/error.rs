// Copyright 2025-2026 The arion-gateway Authors
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

use std::string::FromUtf8Error;

use arion_configuration::config::core::DataSourceReadError;
use http::StatusCode;
use smol_str::SmolStr;
use thiserror::Error;

use crate::body::{poly_body::PolyBodyError, timeout_body::TimeoutBodyError};

#[derive(Debug, Error)]
pub enum JwkError {
    #[error("Invalid JWK: {0}")]
    InvalidJWK(#[from] serde_json::Error),

    #[error("MissingKeyId")]
    MissingKeyId,

    #[error("No alg in JWK")]
    NoAlgInJwk,

    #[error("Missing keys array")]
    MissingKeysArray,

    #[error("JsonWebToken: {0}")]
    JsonWebToken(#[from] jsonwebtoken::errors::Error),

    #[error("Data source: {0}")]
    DataSourceError(#[from] DataSourceReadError),

    #[error("Utf8: {0}")]
    FromUtf8Error(#[from] FromUtf8Error),

    #[error("No validation key found")]
    NoValidationKey,

    #[error("No provider found: {0}")]
    NoProviderFound(SmolStr),

    #[error("Cluster resolution failed: {0}")]
    ClusterResolutionFailed(SmolStr),

    #[error("Error: {0}")]
    ArionError(#[from] crate::Error),

    #[error("Http error: {0}")]
    HttpError(#[from] http::Error),

    #[error("Hyper error: {0}")]
    HyperError(#[from] hyper::Error),

    #[error("Bad status received: {0}")]
    BadStatus(StatusCode),

    #[error("Http timeout: {0}")]
    Timeout(#[from] pingora_timeout::Elapsed),

    #[error("HTTP body timeout: {0}")]
    HttpTimeoutError(#[from] TimeoutBodyError<PolyBodyError>),
}
