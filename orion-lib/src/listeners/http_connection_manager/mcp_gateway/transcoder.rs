use http::header::InvalidHeaderValue;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpRestQueryParams;
use rmcp::model::{JsonObject, Request};

use crate::OrionRequestBody;

pub mod function_graph;
pub mod rest;

#[derive(Debug, thiserror::Error)]
pub enum TranscoderError {
    #[error("HeaderValue: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("Http: {0}")]
    HttpError(#[from] http::Error),
    #[error("Validation error: {0}")]
    ValidationError(String),
}

pub struct RestTranscoder<'a> {
    pub method: &'a http::Method,
    pub path: &'a str,
    pub query_params: &'a Vec<McpRestQueryParams>,
}

pub struct FunctionGraphTranscoder {}

pub trait Transcoder {
    fn encode(
        &self,
        input_schema: &JsonObject,
        http_headers: &http::HeaderMap,
        mcp_request: &Request,
    ) -> Result<http::Request<OrionRequestBody>, TranscoderError>;
}
