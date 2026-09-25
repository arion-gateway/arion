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

//
// This module implements dynamic `HeaderValue` rendering based on access log operators.
// It provides a simplified access log mechanism to be used with
// `request_headers_to_add` and `response_headers_to_add`.
//

use http::{header::InvalidHeaderValue, HeaderValue};
use serde::{Deserialize, Serialize};

use crate::{context::Context, FormatError, LogFormatter};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct HeaderFormatter {
    fmt: LogFormatter,
}

impl HeaderFormatter {
    pub fn try_new(input: &str) -> Result<Self, FormatError> {
        let fmt = LogFormatter::try_new(input, true)?;
        Ok(Self { fmt })
    }

    pub fn with_context<C: Context>(&mut self, ctx: &C) -> &Self {
        self.fmt.with_context(ctx);
        self
    }

    #[inline]
    pub fn into_header_value(self) -> Result<HeaderValue, InvalidHeaderValue> {
        let fmt = self.fmt.into_message();
        let value = format!("{fmt}");
        HeaderValue::from_str(&value)
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.fmt.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use http::Request;

    use crate::context::{DownstreamContext, SocketAddrContext};

    use super::*;

    fn build_request() -> Request<()> {
        Request::builder().uri("https://www.rust-lang.org/").header("User-Agent", "awesome/1.0").body(()).unwrap()
    }

    #[test]
    fn test_into_header_path() {
        let req = build_request();
        let source = HeaderFormatter::try_new("%REQ(:PATH)%").unwrap();
        let mut formatter = source.clone();

        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });

        let header_value = formatter.into_header_value().expect("Failed to create header value");
        assert_eq!(header_value.as_bytes(), b"/");
    }

    #[test]
    fn test_into_header_scheme() {
        let req = build_request();
        let source = HeaderFormatter::try_new("%REQ(:SCHEME)%").unwrap();
        let mut formatter = source.clone();

        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });

        let header_value = formatter.into_header_value().expect("Failed to create header value");
        assert_eq!(header_value.as_bytes(), b"https");
    }

    #[test]
    fn test_into_header_x_request_id() {
        let mut req = build_request();
        req.headers_mut().append("X-Request-Id", HeaderValue::from_static("123"));

        println!("REQ: {req:?}");
        let source = HeaderFormatter::try_new("%REQ(X-REQUEST-ID)%").unwrap();
        let mut formatter = source.clone();

        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });

        let header_value = formatter.into_header_value().expect("Failed to create header value");
        assert_eq!(header_value.as_bytes(), b"123");
    }
}
