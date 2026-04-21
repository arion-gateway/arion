use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::config::network_filters::http_connection_manager::http_filters::local_rate_limit::LocalRateLimit;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserLocalRateLimit {
    pub user_id_header: SmolStr,
    pub local_rate_limit: LocalRateLimit,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::user_local_rate_limit::v3::UserLocalRateLimit as OrionUserLocalRateLimit;

    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::{GenericError, required};
    use super::UserLocalRateLimit;

    impl TryFrom<OrionUserLocalRateLimit> for UserLocalRateLimit {
         type Error = GenericError;

        fn try_from(value: OrionUserLocalRateLimit) -> Result<Self, Self::Error> {
            let OrionUserLocalRateLimit { user_id_header, local_rate_limit } = value;
            let local_rate_limit = required!(local_rate_limit)?.try_into()?;
            Ok(Self {
                user_id_header: user_id_header.into(),
                local_rate_limit,
            })
        }
    }
}
