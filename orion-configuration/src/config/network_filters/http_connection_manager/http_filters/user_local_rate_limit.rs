use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::config::network_filters::http_connection_manager::http_filters::local_rate_limit::LocalRateLimit;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SimpleRateLimit {
    pub max_tokens: u32,
    pub rate: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Limit {
    LocalRateLimit(LocalRateLimit),
    SimpleRateLimit(SimpleRateLimit),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserRateLimit {
    pub user_id: Option<SmolStr>,
    pub limit: Limit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserLocalRateLimit {
    pub user_id_header: SmolStr,
    pub user_rate_limits: Vec<UserRateLimit>,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::user_local_rate_limit::v3::UserLocalRateLimit as OrionUserLocalRateLimit;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::user_local_rate_limit::v3::UserRateLimit as OrionUserRateLimit;

    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::{GenericError, required};
    use super::{UserLocalRateLimit, UserRateLimit, Limit, SimpleRateLimit};
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::user_local_rate_limit::v3::user_rate_limit::Limit as OrionLimit;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::user_local_rate_limit::v3::SimpleRateLimit as OrionSimpleRateLimit;

    impl TryFrom<OrionSimpleRateLimit> for SimpleRateLimit {
        type Error = GenericError;

        fn try_from(value: OrionSimpleRateLimit) -> Result<Self, Self::Error> {
            Ok(Self {
                max_tokens: value.max_tokens,
                rate: value.rate,
            })
        }
    }

    impl TryFrom<OrionUserRateLimit> for UserRateLimit {
        type Error = GenericError;

        fn try_from(value: OrionUserRateLimit) -> Result<Self, Self::Error> {
            let OrionUserRateLimit { user_id, limit } = value;
            let limit = required!(limit)?;
            let limit = match limit {
                OrionLimit::LocalRateLimit(l) => Limit::LocalRateLimit(l.try_into()?),
                OrionLimit::SimpleRateLimit(s) => Limit::SimpleRateLimit(s.try_into()?),
            };
            Ok(Self {
                user_id: user_id.map(Into::into),
                limit,
            })
        }
    }

    impl TryFrom<OrionUserLocalRateLimit> for UserLocalRateLimit {
         type Error = GenericError;

        fn try_from(value: OrionUserLocalRateLimit) -> Result<Self, Self::Error> {
            let OrionUserLocalRateLimit { user_id_header_name, user_rate_limits } = value;

            let user_rate_limits = user_rate_limits
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Self {
                user_id_header: user_id_header_name.into(),
                user_rate_limits,
            })
        }
    }
}
