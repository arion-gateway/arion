// Copyright 2025 The kmesh Authors
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

pub mod config_builder;
mod error;
pub mod orion_instance;
pub(crate) mod port_allocator;
mod test_backend;
mod test_client;
mod xds_harness;
pub mod xds_server;

pub use error::{Error, Result};
pub use orion_instance::{OrionInstance, SpawnOptions};
pub use port_allocator::PortBlock;
pub use test_backend::{CapturedRequest, PreConfiguredResponse, TestBackend};
pub use test_client::{RequestBuilder, TestClient};
pub use xds_harness::{HarnessError, HarnessTimeouts, XdsEnabledHarness, XdsHarnessOptions};
