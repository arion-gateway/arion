// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//

use serde::{Deserialize, Serialize};

use crate::config::{
    core::StringMatcherPattern, network_filters::http_connection_manager::header_modifier::HeaderValueOption,
};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum EarlyHeaderMutation {
    Remove(#[serde(with = "http_serde_ext::header_name")] http::HeaderName),
    Append(HeaderValueOption),
    RemoveOnMatch(StringMatcherPattern),
}

#[cfg(feature = "envoy-conversions")]
pub(crate) use envoy_conversions::mutations_from_typed_extension;

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    #![allow(deprecated)]
    use super::*;
    use crate::config::{common::*, core::StringMatcher};
    use orion_data_plane_api::envoy_data_plane_api::{
        envoy::{
            config::{
                common::mutation_rules::v3::{
                    header_mutation::{Action as EnvoyMutationAction, RemoveOnMatch as EnvoyRemoveOnMatch},
                    HeaderMutation as EnvoyEarlyHeaderMutation,
                },
                core::v3::TypedExtensionConfig as EnvoyTypedExtensionConfig,
            },
            extensions::http::early_header_mutation::header_mutation::v3::HeaderMutation as EnvoyHeaderMutationConfig,
        },
        google::protobuf::Any,
        prost::Message,
    };
    use std::str::FromStr;

    impl TryFrom<EnvoyEarlyHeaderMutation> for EarlyHeaderMutation {
        type Error = GenericError;
        fn try_from(envoy: EnvoyEarlyHeaderMutation) -> Result<Self, Self::Error> {
            let EnvoyEarlyHeaderMutation { action } = envoy;
            match required!(action)? {
                EnvoyMutationAction::Remove(header) => {
                    let name = http::HeaderName::from_str(header.as_str()).map_err(|e| {
                        GenericError::from_msg_with_cause(format!("failed to convert \"{header}\" into HeaderName"), e)
                            .with_node("remove")
                    })?;
                    Ok(Self::Remove(name))
                },
                EnvoyMutationAction::Append(header_value_option) => {
                    Ok(Self::Append(header_value_option.try_into().with_node("append")?))
                },
                EnvoyMutationAction::RemoveOnMatch(remove_on_match) => {
                    let EnvoyRemoveOnMatch { key_matcher } = remove_on_match;
                    let matcher: StringMatcher = convert_opt!(key_matcher)?;
                    Ok(Self::RemoveOnMatch(matcher.pattern))
                },
            }
        }
    }

    // NOTE: `TryFrom` cannot target `Vec<EarlyHeaderMutation>` directly (orphan rule:
    // neither `TryFrom` nor `Vec` is defined in this crate), so the multi-mutation
    // wrappers are exposed as `pub(crate)` helpers instead.
    pub(crate) fn mutations_from_config(
        envoy: EnvoyHeaderMutationConfig,
    ) -> Result<Vec<EarlyHeaderMutation>, GenericError> {
        let EnvoyHeaderMutationConfig { mutations } = envoy;
        convert_vec!(mutations)
    }

    pub(crate) fn mutations_from_typed_extension(
        envoy: EnvoyTypedExtensionConfig,
    ) -> Result<Vec<EarlyHeaderMutation>, GenericError> {
        let EnvoyTypedExtensionConfig { name, typed_config } = envoy;
        let typed_config: Any = required!(typed_config)?;
        let config = match typed_config.type_url.as_str() {
            "type.googleapis.com/envoy.extensions.http.early_header_mutation.header_mutation.v3.HeaderMutation" => {
                EnvoyHeaderMutationConfig::decode(typed_config.value.as_slice()).map_err(|e| {
                    GenericError::from_msg_with_cause(
                        format!("failed to parse protobuf for \"{}\"", typed_config.type_url),
                        e,
                    )
                })?
            },
            _ => return Err(GenericError::unsupported_variant(typed_config.type_url).with_name(name)),
        };
        mutations_from_config(config).with_name(name)
    }
}
