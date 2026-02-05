use super::{FunctionGraphTranscoder, Transcoder, TranscoderError};
use crate::OrionRequestBody;
use rmcp::model::{JsonObject, Request};

impl Transcoder for FunctionGraphTranscoder {
    fn encode(
        &self,
        _input_schema: &JsonObject,
        _http_request: &http::Request<OrionRequestBody>,
        _mcp_request: &Request,
    ) -> Result<http::Request<OrionRequestBody>, TranscoderError> {
        todo!()
    }
}
