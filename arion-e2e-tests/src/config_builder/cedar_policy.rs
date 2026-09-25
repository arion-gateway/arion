// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use arion_data_plane_api::envoy_data_plane_api::{
    arion::extensions::filters::http::cedar::cedar_policy::v3::CedarPolicy as ProtoCedarPolicy, google::protobuf::Any,
    prost::Message,
};

pub struct CedarPolicyBuilder {
    schema: String,
    policies: String,
    entities: String,
    enforcement_mode: i32,
    failure_mode: i32,
    principal_entity_type: String,
    resource_entity_type: String,
    validate_schema_per_request: Option<bool>,
}

impl CedarPolicyBuilder {
    #[must_use]
    pub fn new(schema: impl Into<String>, policies: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            policies: policies.into(),
            entities: String::new(),
            enforcement_mode: 0, // ENFORCE
            failure_mode: 0,     // FAIL_CLOSED
            principal_entity_type: String::new(),
            resource_entity_type: String::new(),
            validate_schema_per_request: Some(false),
        }
    }

    #[must_use]
    pub fn log_only(mut self) -> Self {
        self.enforcement_mode = 1;
        self
    }

    #[must_use]
    pub fn fail_open(mut self) -> Self {
        self.failure_mode = 1;
        self
    }

    #[must_use]
    pub fn entities(mut self, entities: impl Into<String>) -> Self {
        self.entities = entities.into();
        self
    }

    #[must_use]
    pub fn principal_entity_type(mut self, t: impl Into<String>) -> Self {
        self.principal_entity_type = t.into();
        self
    }

    #[must_use]
    pub fn resource_entity_type(mut self, t: impl Into<String>) -> Self {
        self.resource_entity_type = t.into();
        self
    }
}

impl From<CedarPolicyBuilder> for Any {
    fn from(b: CedarPolicyBuilder) -> Any {
        let proto = ProtoCedarPolicy {
            schema: b.schema,
            policies: b.policies,
            entities: b.entities,
            enforcement_mode: b.enforcement_mode,
            failure_mode: b.failure_mode,
            principal_entity_type: b.principal_entity_type,
            resource_entity_type: b.resource_entity_type,
            validate_schema_per_request: b.validate_schema_per_request,
        };
        Any {
            type_url: "type.googleapis.com/arion.extensions.filters.http.cedar.cedar_policy.v3.CedarPolicy".to_owned(),
            value: proto.encode_to_vec(),
        }
    }
}
