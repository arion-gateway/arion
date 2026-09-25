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

use std::{future::Future, pin::Pin};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum XdsExtensionError {
    #[error("failed to decode extension resource: {0}")]
    DecodeError(String),
    #[error("extension handler error: {0}")]
    HandlerError(String),
}

pub trait XdsExtensionHandler: Send + Sync {
    fn type_urls(&self) -> &[&str];

    fn handle_update(
        &self,
        type_url: &str,
        resource_id: &str,
        payload: &[u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), XdsExtensionError>> + Send + '_>>;

    fn handle_remove(
        &self,
        type_url: &str,
        resource_id: &str,
    ) -> Pin<Box<dyn Future<Output = Result<(), XdsExtensionError>> + Send + '_>>;
}
