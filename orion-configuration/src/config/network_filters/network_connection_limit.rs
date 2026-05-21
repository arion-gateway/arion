use std::time::Duration;

use orion_interner::InternedStr;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectionLimit {
    pub stat_prefix: InternedStr,
    pub max_connections: u64,
    pub delay: Option<Duration>,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use super::ConnectionLimit;
    use crate::config::{common::*, core::RustType};
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::network::connection_limit::v3::ConnectionLimit as EnvoyConnectionLimit;
    use std::time::Duration;

    impl TryFrom<EnvoyConnectionLimit> for ConnectionLimit {
        type Error = GenericError;
        fn try_from(value: EnvoyConnectionLimit) -> Result<Self, Self::Error> {
            let EnvoyConnectionLimit { stat_prefix, max_connections, delay, runtime_enabled } = value;

            unsupported_field!(
                //stat_prefix,
                //max_connections,
                //delay,
                runtime_enabled
            )?;

            let max_connections = required!(max_connections)?.value;
            let delay = delay
                .map(|d| RustType::<Duration>::try_from(d).with_node("delay").map(RustType::into_inner))
                .transpose()?;

            Ok(Self { stat_prefix: stat_prefix.into(), max_connections, delay })
        }
    }
}
