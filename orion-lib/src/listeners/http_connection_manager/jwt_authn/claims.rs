use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use smol_str::SmolStr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtClaims {
    pub iss: Option<SmolStr>,      // issuer
    pub sub: Option<SmolStr>,      // subject
    pub aud: Option<Vec<SmolStr>>, // audience
    pub exp: Option<u64>,          // expiration time
    pub iat: Option<u64>,          // issued at
    pub nbf: Option<u64>,          // not before
    pub jti: Option<SmolStr>,      // JWT ID

    // all other custom claims
    #[serde(flatten)]
    pub extra: HashMap<String, Value, ahash::RandomState>,
}
