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

use crate::{listeners::http_connection_manager::ext_proc::kind::MessageKind, ArionResponseBody};

use arion_data_plane_api::envoy_data_plane_api::envoy::service::ext_proc::v3::HeaderMutation;

#[derive(Debug)]
pub enum ProcessingStatus {
    RequestReady(ReadyStatus),
    ResponseReady(ReadyStatus),
    HaltedOnError,
    EndWithDirectResponse(Box<http::Response<ArionResponseBody>>),
}

impl ProcessingStatus {
    pub fn with_request_ready<F>(&mut self, f: F)
    where
        F: FnOnce(&mut ReadyStatus),
    {
        if let ProcessingStatus::RequestReady(ready) = self {
            f(ready);
        }
    }

    pub fn with_response_ready<F>(&mut self, f: F)
    where
        F: FnOnce(&mut ReadyStatus),
    {
        if let ProcessingStatus::ResponseReady(ready) = self {
            f(ready);
        }
    }

    #[inline]
    pub fn ready<M: MessageKind>() -> ProcessingStatus {
        if M::IS_REQUEST {
            Self::RequestReady(ReadyStatus::default())
        } else {
            Self::ResponseReady(ReadyStatus::default())
        }
    }
}

#[derive(Debug, Default)]
pub struct ReadyStatus {
    pub headers_modifications: Option<HeaderMutation>,
    pub clear_route_cache: bool,
}
