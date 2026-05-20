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
use crate::config::{common::envoy_conversions::IsUsed, grpc::GrpcService, unsupported_field, GenericError};
use http::HeaderName;
use orion_data_plane_api::envoy_data_plane_api::{
    envoy::extensions::stat_sinks::open_telemetry::v3::SinkConfig as EnvoySinkConfig, google::protobuf::Any,
    prost::Message,
};
use serde::{Deserialize, Deserializer, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StatsSink {
    OpenTelemetry(SinkConfig),
}

impl TryFrom<Any> for StatsSink {
    type Error = GenericError;

    fn try_from(typed_config: Any) -> Result<Self, Self::Error> {
        match typed_config.type_url.as_str() {
            "type.googleapis.com/envoy.extensions.stat_sinks.open_telemetry.v3.SinkConfig" => {
                let sink_config = EnvoySinkConfig::decode(typed_config.value.as_slice()).map_err(|e| {
                    GenericError::from_msg_with_cause(
                        format!("failed to parse protobuf for \"{}\"", typed_config.type_url),
                        e,
                    )
                })?;
                SinkConfig::try_from(sink_config).map(Self::OpenTelemetry)
            },
            _ => Err(GenericError::unsupported_variant(typed_config.type_url)),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct SinkConfig {
    pub grpc_service: GrpcService,
    pub prefix: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum CustomMetric {
    Counter {
        name: String,
        description: String,
        #[serde(with = "http_serde_ext::header_name")]
        header_name: HeaderName,
        attribute_name: Option<String>,
    },
    Histogram {
        name: String,
        description: String,
        #[serde(with = "http_serde_ext::header_name")]
        header_name: HeaderName,
        attribute_name: Option<String>,
        #[serde(deserialize_with = "vec_max_u64")]
        buckets: Vec<u64>,
    },
    Gauge {
        name: String,
        description: String,
        #[serde(with = "http_serde_ext::header_name")]
        header_name: HeaderName,
        attribute_name: Option<String>,
    },
}

fn vec_max_u64<'de, D>(deserializer: D) -> Result<Vec<u64>, D::Error>
where
    D: Deserializer<'de>,
{
    // Helper enum to handle either a number or the "MAX" string
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Item {
        Num(u64),
        Str(String),
    }

    // Deserialize into a temporary vector of items first
    let temp_vec: Vec<Item> = Vec::deserialize(deserializer)?;

    // Convert each item to its corresponding u64 value
    temp_vec
        .into_iter()
        .map(|item| match item {
            Item::Num(n) => Ok(n),
            Item::Str(s) if s == "MAX" || s == "max" || s == "+inf" => Ok(u64::MAX),
            Item::Str(s) => Err(serde::de::Error::custom(format!("Invalid string: {s}"))),
        })
        .collect()
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum SourceHeaderName {
    #[serde(with = "http_serde_ext::header_name")]
    HeaderName(HeaderName),
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub enum SourceHeaderNameOrSni {
    #[serde(with = "http_serde_ext::header_name")]
    HeaderName(HeaderName),
    Sni,
}

pub trait PartitionKeySource {}
impl PartitionKeySource for SourceHeaderName {}
impl PartitionKeySource for SourceHeaderNameOrSni {}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct PartitionKey<P: PartitionKeySource> {
    pub source: P,
    pub attribute_name: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct CustomMetrics {
    #[serde(default)]
    pub incoming_request: Vec<CustomMetric>,
    #[serde(default)]
    pub upstream_request: Vec<CustomMetric>,
    #[serde(default)]
    pub incoming_response: Vec<CustomMetric>,
    #[serde(default)]
    pub downstream_response: Vec<CustomMetric>,
}

#[derive(Clone, Debug, Deserialize, Default, Serialize, PartialEq, Eq)]
pub struct MetricsConfig {
    #[serde(default)]
    pub user_key: Option<PartitionKey<SourceHeaderNameOrSni>>, // for user metrics (invocations, throttles, etc.)
    #[serde(default)]
    pub custom_key: Option<PartitionKey<SourceHeaderName>>, // for custom metrics (might use a different partition key)
    #[serde(default)]
    pub rename: std::collections::HashMap<String, String>,
    #[serde(default)]
    pub custom_metrics: CustomMetrics,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_key_header_name_deserialization() {
        let yaml = "source: !HeaderName x-user-id\nattribute_name: user\n";
        let key: PartitionKey<SourceHeaderName> = serde_yaml::from_str(yaml).expect("failed to parse HeaderName");
        assert_eq!(key.attribute_name, Some("user".to_owned()));
        assert!(matches!(key.source, SourceHeaderName::HeaderName(_)));
        println!("HeaderName YAML roundtrip:\n{}", serde_yaml::to_string(&key).unwrap());
    }

    #[test]
    fn test_partition_key_sni_deserialization() {
        let yaml = "source: Sni\nattribute_name: user\n";
        let key: PartitionKey<SourceHeaderNameOrSni> = serde_yaml::from_str(yaml).expect("failed to parse Sni");
        assert_eq!(key.attribute_name, Some("user".to_owned()));
        assert!(matches!(key.source, SourceHeaderNameOrSni::Sni));
        println!("Sni YAML roundtrip:\n{}", serde_yaml::to_string(&key).unwrap());
    }
}

#[cfg(feature = "envoy-conversions")]
mod envoy_conversions {
    use super::*;

    impl TryFrom<EnvoySinkConfig> for SinkConfig {
        type Error = GenericError;
        fn try_from(value: EnvoySinkConfig) -> Result<Self, Self::Error> {
            let EnvoySinkConfig {
                report_counters_as_deltas,
                report_histograms_as_deltas,
                emit_tags_as_attributes,
                use_tag_extracted_name,
                prefix,
                protocol_specifier,
                resource_detectors,
                custom_metric_conversions,
            } = value;
            unsupported_field!(
                // prefix,
                // protocol_specifier
                report_counters_as_deltas,
                report_histograms_as_deltas,
                emit_tags_as_attributes,
                use_tag_extracted_name,
                resource_detectors,
                custom_metric_conversions
            )?;

            let orion_data_plane_api::envoy_data_plane_api::envoy::extensions::stat_sinks::open_telemetry::v3::sink_config::ProtocolSpecifier::GrpcService(grpc_srv)
                = protocol_specifier.ok_or_else(|| GenericError::from_msg("ProtocolSpecifier unspecified"))?;
            let grpc_service = GrpcService::try_from(grpc_srv)?;
            Ok(Self { grpc_service, prefix })
        }
    }
}
