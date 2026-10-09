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
// This module implements dynamic URI rendering based on access log operators.
// It is intended for formatting redirect URIs (e.g. `%REQ(x-forwarded-proto)%://%REQ(:authority)%/callback`)
// on-demand without performance overhead when static.
//

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::{context::Context, FormatError, LogFormatter};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum UriFormatter {
    Static(SmolStr),
    Dynamic(LogFormatter),
}

impl UriFormatter {
    pub fn try_new(input: &str) -> Result<Self, FormatError> {
        if !input.contains('%') {
            return Ok(Self::Static(SmolStr::new(input)));
        }
        let fmt = LogFormatter::try_new(input, true)?;
        Ok(Self::Dynamic(fmt))
    }

    #[inline]
    pub fn format<C: Context>(&self, ctx: &C) -> SmolStr {
        match self {
            Self::Static(s) => s.clone(),
            Self::Dynamic(fmt) => {
                let mut f = fmt.clone();
                f.with_context(ctx);
                SmolStr::new(f.into_message().to_string())
            },
        }
    }

    #[inline]
    pub fn is_static(&self) -> bool {
        matches!(self, Self::Static(_))
    }
}

#[cfg(test)]
mod tests {
    use http::Request;

    use crate::context::{DownstreamContext, SocketAddrContext};

    use super::*;

    #[test]
    fn test_static_uri() {
        let formatter = UriFormatter::try_new("https://example.com/callback").unwrap();
        assert!(formatter.is_static());
        let req = Request::builder().uri("https://example.com/").body(()).unwrap();
        let ctx = DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        };
        assert_eq!(formatter.format(&ctx), "https://example.com/callback");
    }

    #[test]
    fn test_dynamic_uri_with_proto_and_authority() {
        let formatter = UriFormatter::try_new("%REQ(x-forwarded-proto)%://%REQ(:authority)%/callback").unwrap();
        assert!(!formatter.is_static());
        let req = Request::builder()
            .uri("/some/path")
            .header("Host", "my-gateway.example.com")
            .header("x-forwarded-proto", "https")
            .body(())
            .unwrap();
        let ctx = DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        };
        assert_eq!(formatter.format(&ctx), "https://my-gateway.example.com/callback");
    }
}
