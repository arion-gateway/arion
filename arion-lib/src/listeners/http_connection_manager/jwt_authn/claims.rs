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

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol_str::SmolStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iss: Option<SmolStr>, // issuer
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sub: Option<SmolStr>, // subject
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aud: Option<Vec<SmolStr>>, // audience
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exp: Option<u64>, // expiration time
    #[serde(skip_serializing_if = "Option::is_none")]
    pub iat: Option<u64>, // issued at
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nbf: Option<u64>, // not before
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jti: Option<SmolStr>, // JWT ID

    // all other custom claims
    #[serde(flatten)]
    pub extra: HashMap<String, Value, ahash::RandomState>,
}
