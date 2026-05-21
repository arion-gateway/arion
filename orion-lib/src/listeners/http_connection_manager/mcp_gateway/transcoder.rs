use bytes::Bytes;
use http::header::InvalidHeaderValue;
use http::StatusCode;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpRestQueryParams;
use rmcp::model::Request;
use upon::Engine;

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
    #[error("UpstreamError error: {0}")]
    UpstreamError(String),
    #[error("Template render error: {0}")]
    TemplateRenderError(#[from] upon::Error),
}

#[derive(Debug)]
pub enum TranscoderType {
    Rest(RestTranscoder),
    FunctionGraph(FunctionGraphTranscoder),
    // No transcoding for MCP usptreams
    NoTranscoder,
}

#[derive(Debug)]
pub struct RestTranscoder {
    pub method: http::Method,
    pub query_params: Vec<McpRestQueryParams>,
    pub has_body_template: bool,
    pub template_engine: Engine<'static>,
}

#[derive(Debug)]
pub struct FunctionGraphTranscoder;

pub trait Transcoder {
    fn encode(
        &self,
        http_headers: &http::HeaderMap,
        mcp_request: &Request,
    ) -> Result<http::Request<OrionRequestBody>, TranscoderError>;

    fn decode(&self, upstream_body: Bytes, upstream_status: StatusCode) -> Result<serde_json::Value, TranscoderError>;
}
