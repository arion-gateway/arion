use std::{collections::HashMap, str::FromStr, string::FromUtf8Error, sync::Arc};

use crate::{PolyBody, body::{instrumented_body::InstrumentedBody, timeout_body::TimeoutBody}, listeners::http_connection_manager::FilterDecision};
use http::Request;
use jsonwebtoken::{DecodingKey, Validation, Algorithm, jwk::Jwk};
use smallvec::{SmallVec};
use smol_str::SmolStr;
use tracing::{debug, error, info, warn};
use thiserror::Error;

use orion_configuration::config::{
    core::{DataSourceReadError}, network_filters::http_connection_manager::http_filters::jwt::{JwksSourceSpecifier, JwtAuthentication as JwtAuthenticationConfig}
};

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Kid(SmolStr);

impl std::fmt::Display for Kid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone)]
pub struct ValidationKey {
    decoding_key: DecodingKey,
    validation: Validation,
}

#[derive(Debug, Clone)]
struct ProviderContext {
    /// Exact string expected in the 'iss' claim.
    expected_issuer: SmolStr,

    /// Set of acceptable audiences (e.g., "my-backend", "my-api").
    expected_audiences: SmallVec<[SmolStr; 4]>,

    /// Valid keys from static config or remote endpoint derived from Jwk
    keys: HashMap<Kid, ValidationKey, ahash::RandomState>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Provider(SmolStr);

#[derive(Debug, Error)]
enum JwkError {
    #[error("Invalid JWK: {0}")]
    InvalidJWK(#[from] serde_json::Error),

    #[error("MissingKeyId")]
    MissingKeyId,

    #[error("No alg in JWK")]
    NoAlgInJwk,

    #[error("Missing keys array")]
    MissingKeysArray,

    #[error("JsonWebToken: {0}")]
    JsonWebToken(#[from] jsonwebtoken::errors::Error),

    #[error("Data source: {0}")]
    DataSourceError(#[from] DataSourceReadError),

    #[error("Utf8: {0}")]
    FromUtf8Error(#[from] FromUtf8Error),
}

#[derive(Debug, Clone)]
pub struct JwtAuthenticationInner {
    config: JwtAuthenticationConfig,
    providers: HashMap<Provider, ProviderContext, ahash::RandomState>,
}


#[derive(Debug, Clone)]
pub struct JwtAuthentication {
    inner: Arc<JwtAuthenticationInner>,
}

impl JwtAuthentication {
    fn parse_and_validate_keys(src_spec :&JwksSourceSpecifier) -> Result<HashMap<Kid, ValidationKey, ahash::RandomState>, JwkError> {
        match src_spec {
            JwksSourceSpecifier::LocalJwks(data_src) => {
                let bytes = data_src.to_bytes_blocking()?;
                let string = String::from_utf8(bytes)?;
                let jwks : serde_json::Value = serde_json::from_str(&string)?;
                let mut keys = HashMap::<Kid, ValidationKey, ahash::RandomState>::default();
                if let Some(keys_array) = jwks.get("keys").and_then(|v| v.as_array()) {
                    for key_value in keys_array {
                        info!(target: "jwt", "JWK: {:?}", key_value);
                        let jwk: Jwk = serde_json::from_str(&key_value.to_string())?;
                        let kid = Kid(jwk.common.key_id.as_ref().ok_or(JwkError::MissingKeyId)?.into());
                        let key_alg = jwk.common.key_algorithm.ok_or(JwkError::NoAlgInJwk)?;
                        let alg = Algorithm::from_str(&key_alg.to_string())?;
                        let decoding_key = DecodingKey::from_jwk(&jwk)?;
                        let validation = Validation::new(alg);
                        keys.insert(kid, ValidationKey { decoding_key, validation });
                    }
                } else {
                    return Err(JwkError::MissingKeysArray);
                }

                Ok(keys)
            },
            JwksSourceSpecifier::RemoteJwks(_conf) => {
                warn!(target: "jwt", "RemoteJwks not implemented yet");
                Ok(HashMap::default())
            },
        }
    }

    pub fn new(config: JwtAuthenticationConfig) -> Self {
        debug!(target: "jwt", "Creating new JWT authentication filter");
        let mut providers = HashMap::<Provider, ProviderContext, ahash::RandomState>::default();
        for (provider, prov_config) in config.providers.iter() {
            let expected_issuer = prov_config.issuer.clone();
            let expected_audiences = prov_config.audiences.clone();
            match Self::parse_and_validate_keys(&prov_config.jwks_source_specifier) {
                Ok(keys) => {
                    providers.insert(Provider(provider.to_owned()), ProviderContext {
                        expected_issuer,
                        expected_audiences: expected_audiences.into(),
                        keys,
                    });
                },
                Err(e) => {
                    error!(target: "jwt", "Provider '{}'. {}", provider, e);
                },
            }
        }

        Self{ inner: Arc::new(JwtAuthenticationInner{ config, providers }) }
    }

    #[allow(clippy::too_many_lines)]
    pub async fn apply_request(
        &mut self,
        _request: &mut Request<InstrumentedBody<TimeoutBody<PolyBody>>>,
    ) -> FilterDecision {
        debug!(target: "jwt", "Applying JWT authentication filter");
        debug!(target: "jwt", "{:#?}", self.inner.config);
        FilterDecision::Continue
    }
}
