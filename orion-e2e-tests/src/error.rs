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

use std::io;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("Failed to start Orion process: {0}")]
    ProcessStartFailed(String),

    #[error("Orion process exited unexpectedly with code: {0:?}")]
    ProcessExitedUnexpectedly(Option<i32>),

    #[error("Orion process did not become ready within {0:?}")]
    ReadyTimeout(std::time::Duration),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("HTTP error: {0}")]
    Http(String),

    #[error("Hyper error: {0}")]
    Hyper(#[from] hyper_util::client::legacy::Error),

    #[error("Request timed out after {0:?}")]
    RequestTimeout(std::time::Duration),

    #[error("No request received within {0:?}")]
    NoRequestReceived(std::time::Duration),

    #[error("Failed to allocate port: {0}")]
    PortAllocationFailed(String),

    #[error("Invalid HTTP response: {0}")]
    InvalidResponse(String),
}
