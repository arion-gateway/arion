// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
//
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
//
//

#[cfg(feature = "access-log")]
use crate::with_access_log;

use super::{RequestCtx, RequestHandler};
use crate::{body::timeout_body::TimeoutBody, ArionRequestBody, ArionResponseBody, Result};
use arion_configuration::config::network_filters::http_connection_manager::route::DirectResponseAction;
use http_body_util::Full;
use hyper::{Request, Response};

#[cfg(feature = "access-log")]
use arion_format::context::UpstreamContext;

impl<'a> RequestHandler<Request<ArionRequestBody>, &'a str> for &DirectResponseAction {
    async fn to_response(
        self,
        #[allow(unused_variables)] ctx: &RequestCtx,
        request: Request<ArionRequestBody>,
        #[allow(unused_variables)] arg: &'a str,
    ) -> Result<Response<ArionResponseBody>> {
        #[cfg(feature = "access-log")]
        let route_name = arg;
        #[cfg(feature = "access-log")]
        ctx.tx.with_loggers(|loggers| {
            with_access_log!(loggers, UpstreamContext { authority: None, cluster_name: None, route_name });
        });

        let body = Full::new(self.body.as_ref().map(|b| bytes::Bytes::copy_from_slice(b.data())).unwrap_or_default());
        let mut resp = Response::new(TimeoutBody::new(None, body.into()).into());
        *resp.status_mut() = self.status;
        *resp.version_mut() = request.version();
        Ok(resp)
    }
}
