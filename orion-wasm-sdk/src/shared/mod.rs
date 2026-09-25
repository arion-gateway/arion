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

pub mod blob;
pub mod i64;
pub mod u64;

pub use blob::{BlobData, SharedBlob};
pub use i64::SharedAtomicI64;
pub use u64::SharedAtomicU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedVarError {
    InitFailed,
}

impl core::fmt::Display for SharedVarError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Failed to initialize shared variable (e.g. type mismatch or capacity exceeded)")
    }
}

impl std::error::Error for SharedVarError {}
