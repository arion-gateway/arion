use bytes::Bytes;
use http::header::InvalidHeaderValue;
use http::StatusCode;
use orion_configuration::config::core::DataSource;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::McpRestQueryParams;
use rmcp::model::{JsonObject, Request};
use serde_json::Value;

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
    #[error("JSON parse error: {0}")]
    JsonParseError(String),
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
        input_schema: &JsonObject,
        http_headers: &http::HeaderMap,
        mcp_request: &Request,
    ) -> Result<http::Request<OrionRequestBody>, TranscoderError>;

    /// Decodes an upstream HTTP response into a JSON value for JRPC response.
    ///
    /// # Arguments
    /// * `output_schema` - The JSON schema to validate the response against
    /// * `upstream_body` - The raw body bytes from the upstream response
    /// * `upstream_status` - The HTTP status code from the upstream response
    ///
    /// # Returns
    /// * `Ok(Value)` - The parsed and validated JSON value to use as JRPC result
    /// * `Err(TranscoderError)` - If parsing or validation fails
    fn decode(
        &self,
        output_schema: &JsonObject,
        upstream_body: Bytes,
        upstream_status: StatusCode,
    ) -> Result<Value, TranscoderError>;
}
