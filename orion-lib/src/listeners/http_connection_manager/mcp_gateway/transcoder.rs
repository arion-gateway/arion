use bytes::Bytes;
use http::header::InvalidHeaderValue;
use http::StatusCode;
use orion_configuration::config::core::DataSource;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpRestQueryParams;
use rmcp::model::Request;

use crate::OrionRequestBody;

pub mod function_graph;
pub mod rest;

#[derive(Debug, thiserror::Error)]
pub enum TranscoderError {
    #[error("HeaderValue: {0}")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),
    #[error("Http: {0}")]
    HttpError(#[from] http::Error),
    #[error("JSON parse error: {0}")]
    JsonParseError(String),
    #[error("UpstreamBodyValidation error: {0}")]
    UpstreamRequestBodyValidationError(String),
    #[error("UpstreamError error: {0}")]
    UpstreamError(String),
}

pub struct RestTranscoder<'a> {
    pub method: &'a http::Method,
    pub path: &'a str,
    pub query_params: &'a Vec<McpRestQueryParams>,
    pub body_template: Option<&'a DataSource>,
}

pub struct FunctionGraphTranscoder {}

pub trait Transcoder {
    fn encode(
        &self,
        http_headers: &http::HeaderMap,
        mcp_request: &Request,
    ) -> Result<http::Request<OrionRequestBody>, TranscoderError>;

    fn decode(&self, upstream_body: Bytes, upstream_status: StatusCode) -> Result<serde_json::Value, TranscoderError>;
}
