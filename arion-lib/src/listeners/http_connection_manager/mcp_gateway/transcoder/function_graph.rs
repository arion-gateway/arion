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

use super::{FunctionGraphTranscoder, Transcoder, TranscoderError};
use crate::ArionRequestBody;
use bytes::Bytes;
use http::StatusCode;
use serde_json::Value;

impl Transcoder for FunctionGraphTranscoder {
    fn encode(
        &self,
        _http_headers: &http::HeaderMap,
        _mcp_request: &rmcp::model::Request,
    ) -> Result<http::Request<ArionRequestBody>, TranscoderError> {
        todo!()
    }

    fn decode(&self, _upstream_body: Bytes, _upstream_status: StatusCode) -> Result<Value, TranscoderError> {
        todo!()
    }
}
