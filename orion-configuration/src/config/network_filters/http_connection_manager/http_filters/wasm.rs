use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::config::core::DataSource;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WasmConfig {
    pub name: SmolStr,
    pub root_id: SmolStr,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vm_id: Option<SmolStr>,
    pub runtime: SmolStr,
    pub code: DataSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub configuration: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_precompiled: Option<bool>,
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use super::*;
    use crate::config::common::envoy_conversions::IsUsed;
    use crate::config::{required, unsupported_field, GenericError};
    use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::async_data_source::Specifier as EnvoyAsyncSpecifier;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::filters::http::wasm::v3::Wasm as EnvoyWasm;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::wasm::v3::plugin_config::Vm as EnvoyVm;
    use orion_data_plane_api::envoy_data_plane_api::envoy::extensions::wasm::v3::PluginConfig as EnvoyPluginConfig;

    impl TryFrom<EnvoyWasm> for WasmConfig {
        type Error = GenericError;
        fn try_from(wasm: EnvoyWasm) -> Result<Self, Self::Error> {
            let config = wasm.config;
            let config = required!(config)?;
            config.try_into()
        }
    }

    impl TryFrom<EnvoyPluginConfig> for WasmConfig {
        type Error = GenericError;
        fn try_from(plugin: EnvoyPluginConfig) -> Result<Self, Self::Error> {
            let EnvoyPluginConfig {
                name,
                root_id,
                configuration,
                fail_open,
                failure_policy,
                reload_config,
                capability_restriction_config,
                allow_on_headers_stop_iteration,
                vm,
            } = plugin;

            unsupported_field!(
                fail_open,
                failure_policy,
                reload_config,
                capability_restriction_config,
                allow_on_headers_stop_iteration
            )?;

            let vm = required!(vm)?;
            let EnvoyVm::VmConfig(vm_config) = vm;

            let environment_variables = vm_config.environment_variables;
            unsupported_field!(environment_variables)?;

            let code = vm_config.code;
            let code_async = required!(code)?;

            let specifier = code_async.specifier;
            let specifier = required!(specifier)?;

            let code_local = match specifier {
                EnvoyAsyncSpecifier::Local(local) => local,
                EnvoyAsyncSpecifier::Remote(_) => return Err(GenericError::unsupported_variant("RemoteDataSource")),
            };
            let code = DataSource::try_from(code_local)?;

            let config_string = if let Some(any) = configuration {
                use orion_data_plane_api::envoy_data_plane_api::google::protobuf::StringValue;
                use orion_data_plane_api::envoy_data_plane_api::prost::Message;

                if any.type_url == "type.googleapis.com/google.protobuf.StringValue" {
                    let string_val = StringValue::decode(any.value.as_slice())
                        .map_err(|e| GenericError::from_msg_with_cause("failed to decode StringValue", e))?;
                    Some(string_val.value)
                } else {
                    return Err(GenericError::unsupported_variant(any.type_url));
                }
            } else {
                None
            };

            Ok(WasmConfig {
                name: name.into(),
                root_id: root_id.into(),
                vm_id: if vm_config.vm_id.is_empty() { None } else { Some(vm_config.vm_id.into()) },
                runtime: vm_config.runtime.into(),
                code,
                configuration: config_string,
                allow_precompiled: vm_config.allow_precompiled.then_some(true),
            })
        }
    }
}
