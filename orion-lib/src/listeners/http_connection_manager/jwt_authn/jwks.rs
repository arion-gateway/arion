use std::{collections::HashMap, str::FromStr, sync::Arc, time::Instant};

use ahash::RandomState;
use http::{request, Method, StatusCode};
use http_body_util::BodyExt;
use jsonwebtoken::{jwk::Jwk, Algorithm, DecodingKey, Validation};
use orion_configuration::config::{
    cluster::ClusterSpecifier,
    network_filters::http_connection_manager::http_filters::jwt::{JwtProvider, RemoteJwks},
};
use tracing::error;

use crate::{
    clusters::{clusters_manager, RoutingContext, RoutingPriority},
    listeners::http_connection_manager::jwt_authn::{error::JwkError, Kid, ValidationKey},
    OrionRequestBody,
};

pub fn parse_jwks(
    bytes: &[u8],
    provider: &JwtProvider,
) -> Result<HashMap<Kid, Arc<ValidationKey>, ahash::RandomState>, JwkError> {
    let jwks: serde_json::Value = serde_json::from_slice(bytes)?;
    let mut keys = HashMap::<Kid, Arc<ValidationKey>, ahash::RandomState>::default();
    if let Some(keys_array) = jwks.get("keys").and_then(|v| v.as_array()) {
        for key_value in keys_array {
            let jwk: Jwk = serde_json::from_str(&key_value.to_string())?;
            let kid = Kid(jwk.common.key_id.as_ref().ok_or(JwkError::MissingKeyId)?.into());
            let key_alg = jwk.common.key_algorithm.ok_or(JwkError::NoAlgInJwk)?;
            let alg = Algorithm::from_str(&key_alg.to_string())?;
            let decoding_key = DecodingKey::from_jwk(&jwk)?;
            let validation = build_validation(alg, provider);
            keys.insert(kid, Arc::new(ValidationKey { decoding_key, validation }));
        }
    } else {
        return Err(JwkError::MissingKeysArray);
    }

    Ok(keys)
}

pub async fn fetch_remote_jwks(
    remote: &RemoteJwks,
    provider_name: &str,
    provider_config: &JwtProvider,
) -> Result<HashMap<Kid, Arc<ValidationKey>, RandomState>, JwkError> {
    #[cfg(feature = "instrumentation")]
    let clock = quanta::Clock::new();

    let cluster_spec = ClusterSpecifier::Cluster(remote.http_uri.cluster.clone().into());

    let cluster_id = clusters_manager::resolve_cluster(&cluster_spec, None).ok_or(
        JwkError::ClusterResolutionFailed(cluster_spec.name().into())
    ).inspect_err(|err| error!(target: "jwt", "{provider_name}: failed to resolve cluster {} for jwks: {}", remote.http_uri.cluster, err))?;

    let http_service = clusters_manager::get_http_connection(cluster_id, RoutingContext::None).
                            inspect_err(|err| error!(target: "jwt", "{provider_name}: failed to get http connection for cluster {}: {}", remote.http_uri.cluster, err))?;

    // prepare the request to send...
    let request = request::Builder::new().method(Method::GET).uri(&remote.http_uri.uri).body(OrionRequestBody::default()).
        inspect_err(|err| error!(target: "jwt", "{provider_name}: failed to build request for cluster {}: {}", remote.http_uri.cluster, err))?;

    // get the http channel to send the request
    let channel = http_service.channel();

    let start_time = Instant::now();
    let res = channel
        .send_request(
            request,
            Some(remote.http_uri.timeout),
            remote.retry_policy.as_ref(),
            RoutingPriority::Default,
            None,
            #[cfg(feature = "instrumentation")]
            &clock,
        )
        .await?;

    if res.status() != StatusCode::OK {
        return Err(JwkError::BadStatus(res.status()));
    }

    let mut body = res.into_body();
    body.timeout = Some(remote.http_uri.timeout.saturating_sub(start_time.elapsed()));
    let bytes = body.collect().await?.to_bytes();

    parse_jwks(&bytes, provider_config).inspect_err(|err| {
        error!(target: "jwt", "{provider_name}: failed to parse JWKS. Reason: {}", err);
    })
}

pub fn build_validation(alg: Algorithm, provider: &JwtProvider) -> Validation {
    let mut validation = Validation::new(alg);

    validation.leeway = u64::from(provider.clock_skew_seconds);
    validation.validate_exp = true;

    if !provider.issuer.is_empty() {
        validation.required_spec_claims.insert("iss".into());
        validation.set_issuer(std::slice::from_ref(&provider.issuer));
    }

    if provider.audiences.is_empty() {
        validation.validate_aud = false;
    } else {
        validation.required_spec_claims.insert("aud".into());
        validation.set_audience(&provider.audiences);
        validation.validate_aud = true;
    }

    validation
}
