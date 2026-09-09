use std::string::FromUtf8Error;

use http::StatusCode;
use orion_configuration::config::core::DataSourceReadError;
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
    OrionError(#[from] crate::Error),

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
