use super::{McpTranscoder, Transcoder, TranscoderError};
use crate::OrionRequestBody;
use rmcp::model::{JsonObject, Request};

impl Transcoder for McpTranscoder {
    fn encode(
        &self,
        _input_schema: &JsonObject,
        _http_headers: &http::HeaderMap,
        _mcp_request: &Request,
    ) -> Result<http::Request<OrionRequestBody>, TranscoderError> {
        todo!()
    }
}
