use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct CedarPolicy {
    pub policies: SmolStr,
    pub schema: SmolStr,
    #[serde(default)]
    pub entities: SmolStr,
    #[serde(default)]
    pub enforcement_mode: EnforcementMode,
    #[serde(default)]
    pub failure_mode: FailureMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EnforcementMode {
    #[default]
    Enforce,
    LogOnly,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureMode {
    #[default]
    FailClosed,
    FailOpen,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use super::{CedarPolicy, EnforcementMode, FailureMode};
    use crate::config::common::GenericError;
    use orion_data_plane_api::envoy_data_plane_api::orion::extensions::filters::http::cedar::v3::{
        CedarPolicy as ProtoCedarPolicy, EnforcementMode as ProtoEnforcementMode, FailureMode as ProtoFailureMode,
    };

    impl TryFrom<ProtoCedarPolicy> for CedarPolicy {
        type Error = GenericError;

        fn try_from(proto: ProtoCedarPolicy) -> Result<Self, Self::Error> {
            let ProtoCedarPolicy { policies, schema, entities, enforcement_mode, failure_mode } = proto;

            let enforcement_mode = match ProtoEnforcementMode::try_from(enforcement_mode) {
                Ok(ProtoEnforcementMode::Enforce) => EnforcementMode::Enforce,
                Ok(ProtoEnforcementMode::LogOnly) => EnforcementMode::LogOnly,
                Err(_) => {
                    return Err(GenericError::unsupported_variant(format!("enforcement_mode={enforcement_mode}")))
                },
            };

            let failure_mode = match ProtoFailureMode::try_from(failure_mode) {
                Ok(ProtoFailureMode::FailClosed) => FailureMode::FailClosed,
                Ok(ProtoFailureMode::FailOpen) => FailureMode::FailOpen,
                Err(_) => return Err(GenericError::unsupported_variant(format!("failure_mode={failure_mode}"))),
            };

            Ok(Self {
                policies: policies.into(),
                schema: schema.into(),
                entities: entities.into(),
                enforcement_mode,
                failure_mode,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "policies": "permit(principal, action, resource);",
            "schema": "entity User;",
        });
        let config: CedarPolicy = serde_json::from_value(json).unwrap();
        assert_eq!(config.enforcement_mode, EnforcementMode::Enforce);
        assert_eq!(config.failure_mode, FailureMode::FailClosed);
        assert_eq!(config.entities.as_str(), "");
    }

    #[test]
    fn deserialize_full() {
        let json = serde_json::json!({
            "policies": "permit(principal, action, resource);",
            "schema": "entity User;",
            "entities": "[]",
            "enforcement_mode": "log_only",
            "failure_mode": "fail_open",
        });
        let config: CedarPolicy = serde_json::from_value(json).unwrap();
        assert_eq!(config.enforcement_mode, EnforcementMode::LogOnly);
        assert_eq!(config.failure_mode, FailureMode::FailOpen);
    }

    #[test]
    fn defaults_are_strict() {
        assert_eq!(EnforcementMode::default(), EnforcementMode::Enforce);
        assert_eq!(FailureMode::default(), FailureMode::FailClosed);
    }
}
