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

use super::GenericError;
use http::HeaderName;
use orion_format::header_formatter::HeaderFormatter;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeaderValueOption {
    pub header: HeaderKeyValue,
    pub append_action: HeaderAppendAction,
    pub keep_empty_value: bool,
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub enum HeaderAppendAction {
    AppendIfExistsOrAdd,
    AppendIfAbsent,
    OverwriteIfExistsOrAdd,
    OverwriteIfExists,
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, Deserialize, Serialize)]
pub struct HeaderKeyValue {
    #[serde(with = "http_serde_ext::header_name")]
    pub key: HeaderName,
    pub value: HeaderFormatter,
}

impl TryFrom<(String, Vec<u8>)> for HeaderKeyValue {
    type Error = GenericError;
    fn try_from(value: (String, Vec<u8>)) -> Result<Self, Self::Error> {
        let key = HeaderName::from_str(&value.0).map_err(|e| {
            GenericError::from_msg_with_cause(format!("failed to parse \"{}\" as a HeaderName", value.0), e)
        })?;
        let value_str = String::from_utf8(value.1)
            .map_err(|e| GenericError::from_msg_with_cause("failed to parse bytes as a utf".to_owned(), e))?;

        let value = HeaderFormatter::try_new(&value_str).map_err(|e| {
            GenericError::from_msg_with_cause(format!("failed to parse \"{value_str}\" as a HeaderFormatter"), e)
        })?;

        Ok(Self { key, value })
    }
}
impl TryFrom<(String, String)> for HeaderKeyValue {
    type Error = GenericError;
    fn try_from(value: (String, String)) -> Result<Self, Self::Error> {
        let key = HeaderName::from_str(&value.0).map_err(|e| {
            GenericError::from_msg_with_cause(format!("failed to parse \"{}\" as a HeaderName", value.0), e)
        })?;

        let value = HeaderFormatter::try_new(&value.1).map_err(|e| {
            GenericError::from_msg_with_cause(format!("failed to parse \"{}\" as a HeaderFormatter", value.1), e)
        })?;

        Ok(Self { key, value })
    }
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    #![allow(deprecated)]
    use super::{HeaderAppendAction, HeaderKeyValue, HeaderValueOption};
    use crate::config::common::*;
    use orion_data_plane_api::envoy_data_plane_api::envoy::config::core::v3::{
        header_value_option::HeaderAppendAction as EnvoyHeaderAppendAction, HeaderValue as EnvoyHeaderValue,
        HeaderValueOption as EnvoyHeaderValueOption,
    };

    impl TryFrom<EnvoyHeaderValueOption> for HeaderValueOption {
        type Error = GenericError;
        fn try_from(value: EnvoyHeaderValueOption) -> Result<Self, Self::Error> {
            let EnvoyHeaderValueOption { header, append, append_action, keep_empty_value } = value;
            unsupported_field!(append)?;
            let header = convert_opt!(header)?;
            let append_action = HeaderAppendAction::try_from(append_action).with_node("append_action")?;
            Ok(Self { header, append_action, keep_empty_value })
        }
    }

    impl From<EnvoyHeaderAppendAction> for HeaderAppendAction {
        fn from(value: EnvoyHeaderAppendAction) -> Self {
            match value {
                EnvoyHeaderAppendAction::AppendIfExistsOrAdd => Self::AppendIfExistsOrAdd,
                EnvoyHeaderAppendAction::AddIfAbsent => Self::AppendIfAbsent,
                EnvoyHeaderAppendAction::OverwriteIfExists => Self::OverwriteIfExists,
                EnvoyHeaderAppendAction::OverwriteIfExistsOrAdd => Self::OverwriteIfExistsOrAdd,
            }
        }
    }

    impl TryFrom<i32> for HeaderAppendAction {
        type Error = GenericError;
        fn try_from(value: i32) -> Result<Self, Self::Error> {
            EnvoyHeaderAppendAction::from_i32(value)
                .ok_or(GenericError::unsupported_variant("[unknown header append action]"))
                .map(Self::from)
        }
    }

    impl TryFrom<EnvoyHeaderValue> for HeaderKeyValue {
        type Error = GenericError;
        fn try_from(value: EnvoyHeaderValue) -> Result<Self, Self::Error> {
            let EnvoyHeaderValue { key, value, raw_value } = value;
            match (value.is_used(), raw_value.is_used()) {
                (true, true) => {
                    Err(GenericError::from_msg(format!("both value ({value}) and raw_value ({raw_value:?}) were set"))
                        .with_node("value"))
                },
                (true, false) => Self::try_from((key, value)),
                (false, true) => Self::try_from((key, raw_value)),
                (false, false) => Err(GenericError::MissingField("value OR raw_value")),
            }
        }
    }
}
