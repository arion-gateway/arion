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
