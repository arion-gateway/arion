// Copyright 2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::{http1_pool::Http1Permit, http2_pool::Http2StreamPermit};
use crate::{
    ArionResponseBody,
    body::{
        on_end_body::{OnBodyEnd, OnEndBody},
        poly_body::PolyBody,
        timeout_body::TimeoutBody,
    },
};
use http::Response;
use hyper::body::Incoming;

/// Recycles an HTTP/1 connection or releases an HTTP/2 stream slot when the response body ends.
pub enum BodyEndPermit {
    Http1(Http1Permit),
    Http2(Http2StreamPermit),
}

impl OnBodyEnd for BodyEndPermit {
    #[inline]
    fn on_body_end(self, completed: bool) {
        match self {
            Self::Http1(permit) => permit.on_body_end(completed),
            // Releasing the stream slot does not depend on how the body ended.
            Self::Http2(permit) => drop(permit),
        }
    }
}

/// Wraps a pooled upstream response, attaching the permit to its body.
#[inline]
pub(crate) fn pooled_response(response: Response<Incoming>, permit: BodyEndPermit) -> Response<ArionResponseBody> {
    response.map(|body| OnEndBody::new(TimeoutBody::new(None, PolyBody::from(body))).with_on_end(permit))
}
