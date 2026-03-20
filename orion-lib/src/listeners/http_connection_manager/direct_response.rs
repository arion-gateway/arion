// Copyright 2025 The kmesh Authors
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
use super::{RequestHandler, TransactionHandler};
use crate::{body::timeout_body::TimeoutBody, OrionRequestBody, OrionResponseBody, Result};
use http_body_util::Full;
use hyper::{Request, Response};
use orion_configuration::config::network_filters::http_connection_manager::route::DirectResponseAction;

#[cfg(feature = "access-log")]
use {crate::listeners::access_log::AccessLogContext, orion_format::context::UpstreamContext};

impl<'a> RequestHandler<Request<OrionRequestBody>, &'a str> for &DirectResponseAction {
    async fn to_response(
        self,
        _trans_handler: &TransactionHandler,
        request: Request<OrionRequestBody>,
        _route_name: &'a str,
    ) -> Result<Response<OrionResponseBody>> {
        #[cfg(feature = "access-log")]
        _trans_handler.trans_ctx.lock().loggers.with_context(&UpstreamContext {
            authority: None,
            cluster_name: None,
            route_name: _route_name,
        });

        let body = Full::new(self.body.as_ref().map(|b| bytes::Bytes::copy_from_slice(b.data())).unwrap_or_default());
        let mut resp = Response::new(TimeoutBody::new(None, body.into()));
        *resp.status_mut() = self.status;
        *resp.version_mut() = request.version();
        Ok(resp)
    }
}
