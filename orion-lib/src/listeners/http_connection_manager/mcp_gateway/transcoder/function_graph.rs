use super::{FunctionGraphTranscoder, Transcoder, TranscoderError};
use crate::OrionRequestBody;
use bytes::Bytes;
use http::StatusCode;
use rmcp::model::JsonObject;
use serde_json::Value;

impl Transcoder for FunctionGraphTranscoder {
    fn encode(
        &self,
        _input_schema: &JsonObject,
        _http_headers: &http::HeaderMap,
        _mcp_request: &rmcp::model::Request,
    ) -> Result<http::Request<OrionRequestBody>, TranscoderError> {
        todo!()
    }

    fn decode(
        &self,
        _output_schema: &JsonObject,
        _upstream_body: Bytes,
        _upstream_status: StatusCode,
    ) -> Result<Value, TranscoderError> {
        todo!()
    }
}
