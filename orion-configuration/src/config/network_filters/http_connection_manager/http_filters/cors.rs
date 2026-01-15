use http::Method;
use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::cors::v3::Cors as EnvoyCors;
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CorsConfig {
    /// List of allowed origins. Examples: "https://foo.com", "*".
    pub allow_origins: Vec<SmolStr>,
    /// List of allowed methods. Examples: "GET", "POST".
    #[serde(with = "http_serde_ext::method::vec", default)]
    pub allow_methods: Vec<http::Method>,
    /// List of allowed headers in requests.
    pub allow_headers: Vec<SmolStr>,
    /// List of headers exposed to the client JS.
    pub expose_headers: Vec<SmolStr>,
    /// Whether to allow credentials (cookies, auth headers).
    pub allow_credentials: bool,
    /// Max age for preflight caching in seconds.
    pub max_age: Option<u64>,
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            allow_origins: vec!["*".into()],
            allow_methods: vec![
                Method::GET,
                Method::POST,
                Method::PUT,
                Method::PATCH,
                Method::DELETE,
                Method::HEAD,
                Method::OPTIONS,
            ],
            allow_headers: vec!["*".into()],
            expose_headers: vec!["*".into()],
            allow_credentials: false,
            max_age: Some(86400),
        }
    }
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use super::*;
    use crate::config::GenericError;

    impl TryFrom<EnvoyCors> for CorsConfig {
        type Error = GenericError;
        fn try_from(_: EnvoyCors) -> Result<Self, Self::Error> {
            Ok(CorsConfig::default())
        }
    }
}
