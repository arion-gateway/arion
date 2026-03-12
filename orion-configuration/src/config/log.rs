// Copyright 2025 The kmesh Authors
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

use orion_error::{Context, Error};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::num::NonZeroUsize;
use tracing_rolling_file::RollingFrequency;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollingFrequencyConfig(pub RollingFrequency);

impl Serialize for RollingFrequencyConfig {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = match self.0 {
            RollingFrequency::EveryDay => "daily",
            RollingFrequency::EveryHour => "hourly",
            RollingFrequency::EveryMinute => "minutely",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> Deserialize<'de> for RollingFrequencyConfig {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        let rot = match s.as_str() {
            "minutely" | "MINUTELY" => RollingFrequency::EveryMinute,
            "hourly" | "HOURLY" => RollingFrequency::EveryHour,
            "daily" | "DAILY" => RollingFrequency::EveryDay,
            _ => return Err(serde::de::Error::custom(format!("invalid frequency rotation: {s}"))),
        };
        Ok(RollingFrequencyConfig(rot))
    }
}

impl Default for RollingFrequencyConfig {
    fn default() -> Self {
        RollingFrequencyConfig(RollingFrequency::EveryDay)
    }
}
#[derive(Debug, Deserialize, Serialize, Default)]
pub struct LogConfig {
    #[serde(deserialize_with = "deserialize_log_level", serialize_with = "serialize_log_level")]
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub log_level: Option<EnvFilter>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub log_directory: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", default = "Default::default")]
    pub log_file: Option<String>,
}

fn const_value<const N: usize>() -> NonZeroUsize {
    unsafe { NonZeroUsize::new_unchecked(N) }
}

impl PartialEq for LogConfig {
    fn eq(&self, other: &Self) -> bool {
        self.log_file == other.log_file
            && self.log_directory == other.log_directory
            && self.log_level.as_ref().map(EnvFilter::to_string) == other.log_level.as_ref().map(EnvFilter::to_string)
    }
}
impl Eq for LogConfig {}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct AccessLogConfig {
    #[serde(default = "const_value::<1>")]
    pub num_instances: NonZeroUsize,
    #[serde(default = "const_value::<1024>")]
    pub queue_length: NonZeroUsize,
    pub log_rotation: Option<RollingFrequencyConfig>,
    pub log_max_size: Option<u64>,
    #[serde(default = "const_value::<10>")]
    pub max_log_files: NonZeroUsize,
}

impl Default for AccessLogConfig {
    fn default() -> Self {
        Self {
            num_instances: const_value::<1>(),
            queue_length: const_value::<1024>(),
            log_rotation: None,
            log_max_size: None,
            max_log_files: const_value::<10>(),
        }
    }
}

fn deserialize_log_level<'de, D>(deserializer: D) -> std::result::Result<Option<EnvFilter>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer).and_then(|maybe_string| {
        maybe_string.map(|s| EnvFilter::builder().parse(s)).transpose().map_err(
            |e: tracing_subscriber::filter::ParseError| {
                serde::de::Error::custom(Error::from(e).with_context_msg("failed to parse log level config"))
            },
        )
    })
}

#[allow(clippy::ref_option)]
fn serialize_log_level<S: Serializer>(
    value: &Option<EnvFilter>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    value.as_ref().map(EnvFilter::to_string).serialize(serializer)
}
