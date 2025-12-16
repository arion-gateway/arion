use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct MetadataMatcher {
    pub filter: String,
    pub path: String,
    pub value: String,
}

#[cfg(feature = "envoy-conversions")]
pub(crate) use envoy_conversions::*;

use super::is_default;

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use orion_data_plane_api::envoy_data_plane_api::MetadataMatcher as EnvoyMetadataMatcher;

    impl From<EnvoyMetadataMatcher> for MetadataMatcher {
        fn from(envoy_metadata_matcher: EnvoyMetadataMatcher) -> Self {
            MetadataMatcher {
                // Implement conversion logic here
            }
        }
    }
}
