// Copyright 2026 The arion-gateway Authors
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

//! Tool entries, their public projections, and the config-to-runtime builders.

use std::{borrow::Cow, sync::Arc as StdArc, time::Duration};

use arion_configuration::config::network_filters::{
    http_connection_manager::http_filters::mcp_gateway::{
        DisclosureMode, McpRbacPermission, McpTool, McpToolRbac, UpstreamBackend, UpstreamLimits,
    },
    network_rbac::Action,
};
use arion_interner::InternedStr;
use jsonschema::Validator;
use opentelemetry::StringValue;
use rmcp::model::{self, Tool};
use serde::Serialize;
use serde_json::{Map, Value};
use smol_str::SmolStr;

use super::{ToolId, errors::CallToolError, errors::ToolBuilderError, rest::RestBackend};
use crate::listeners::http_connection_manager::mcp_gateway::{
    embeddings::{self, Bm25Document},
    rbac::{
        Action as RbacAction, JwtClaimField, JwtHeaderField, JwtHeaderMatcher, JwtPayloadMatcher,
        Permission as RbacPermission, ToolRbac,
    },
};

#[derive(Debug, Clone)]
pub enum ToolSource {
    Direct,
    Dynamic { upstream_tool_name: SmolStr },
    OpenApi,
}

#[derive(Debug)]
pub(super) enum ToolBackend {
    Rest(Box<RestBackend>),
    McpServer(McpServerBackend),
}

#[derive(Debug)]
pub(super) struct McpServerBackend {
    pub path: SmolStr,
    pub cluster: InternedStr,
    pub upstream_attr: StringValue,
    pub upstream_tool_name: Cow<'static, str>,
    pub timeout: Duration,
    pub max_response_bytes: usize,
}

#[derive(Debug)]
pub struct ToolEntry {
    pub id: ToolId,
    pub description: SmolStr,
    pub input_schema: Map<String, Value>,
    pub output_schema: Map<String, Value>,
    pub source: ToolSource,
    pub(super) backend: ToolBackend,
    pub input_schema_validator: Option<Validator>,
    pub output_schema_validator: Option<Validator>,
    pub rbac: Option<ToolRbac>,
    pub disclosure_mode: DisclosureMode,
    pub bm25_doc: Bm25Document,
    pub embedding: arc_swap::ArcSwapOption<Vec<f32>>,
    pub tool: Tool,
    pub(super) tool_name_attr: StringValue,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CompactToolDefinition {
    pub namespace: SmolStr,
    pub name: SmolStr,
    pub qualified_name: String,
    pub description: SmolStr,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolDefinition {
    pub namespace: SmolStr,
    pub name: SmolStr,
    pub qualified_name: String,
    pub description: SmolStr,
    pub input_schema: Map<String, Value>,
    pub output_schema: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NamespaceSummary {
    pub namespace: SmolStr,
    pub tool_count: usize,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CatalogPage<T> {
    pub items: Vec<T>,
    pub total: usize,
    pub limit: usize,
    pub offset: usize,
    pub has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_offset: Option<usize>,
}

impl ToolEntry {
    #[inline]
    pub fn is_permitted(&self, req_ext: &http::Extensions) -> bool {
        self.rbac.as_ref().is_none_or(|rbac| rbac.is_permitted(req_ext))
    }

    pub fn compact(&self) -> CompactToolDefinition {
        CompactToolDefinition {
            namespace: self.id.namespace.clone(),
            name: self.id.name.clone(),
            qualified_name: self.id.qualified_name(),
            description: self.description.clone(),
        }
    }

    pub fn definition(&self) -> ToolDefinition {
        ToolDefinition {
            namespace: self.id.namespace.clone(),
            name: self.id.name.clone(),
            qualified_name: self.id.qualified_name(),
            description: self.description.clone(),
            input_schema: self.input_schema.clone(),
            output_schema: self.output_schema.clone(),
        }
    }

    fn validate_json_against_schema(
        validator: Option<&jsonschema::Validator>,
        arguments: &Value,
    ) -> Result<(), CallToolError> {
        if let Some(validator) = validator
            && let Some(err) = validator.iter_errors(arguments).next()
        {
            return Err(CallToolError::ValidationError(err.to_string()));
        }
        Ok(())
    }

    #[inline]
    pub fn validate_against_input_schema(&self, arguments: &Value) -> Result<(), CallToolError> {
        Self::validate_json_against_schema(self.input_schema_validator.as_ref(), arguments)
    }

    #[inline]
    pub fn validate_against_output_schema(&self, arguments: &Value) -> Result<(), CallToolError> {
        Self::validate_json_against_schema(self.output_schema_validator.as_ref(), arguments)
    }
}

pub(super) fn build_tool_entry(
    tool_conf: McpTool,
    source: ToolSource,
    upstream_limits: &UpstreamLimits,
) -> Result<ToolEntry, ToolBuilderError> {
    let id = ToolId::from_tool(&tool_conf)?;
    let McpTool { description, input_schema, output_schema, backend, rbac, embedding, disclosure_mode, .. } = tool_conf;
    let rbac = rbac.as_ref().map(convert_config_rbac_to_runtime);
    let input_schema_validator = if input_schema.is_empty() {
        None
    } else {
        Some(
            Validator::new(&Value::Object(input_schema.clone()))
                .map_err(|e| ToolBuilderError::InvalidInputSchema(e.to_string()))?,
        )
    };
    let output_schema_validator = if output_schema.is_empty() {
        None
    } else {
        Some(
            Validator::new(&Value::Object(output_schema.clone()))
                .map_err(|e| ToolBuilderError::InvalidOutputSchema(e.to_string()))?,
        )
    };
    let backend = match backend {
        UpstreamBackend::Rest { method, path, query_params, cluster, upstream_policy, body_template, body_json } => {
            ToolBackend::Rest(Box::new(RestBackend::compile(
                method,
                path,
                query_params,
                cluster,
                *upstream_policy,
                body_template,
                body_json,
                upstream_limits,
            )?))
        },
        UpstreamBackend::McpServer { path, cluster, .. } => {
            let upstream_tool_name: Cow<'static, str> = match &source {
                ToolSource::Direct | ToolSource::OpenApi => Cow::Owned(id.name.to_string()),
                ToolSource::Dynamic { upstream_tool_name } => Cow::Owned(upstream_tool_name.to_string()),
            };
            let upstream_attr = StringValue::from(StdArc::<str>::from(path.as_str()));
            ToolBackend::McpServer(McpServerBackend {
                path,
                cluster,
                upstream_attr,
                upstream_tool_name,
                timeout: upstream_limits.timeout,
                max_response_bytes: upstream_limits.max_response_bytes,
            })
        },
    };
    let embedding = {
        let supplied = embedding.as_slice();
        if supplied.is_empty() {
            arc_swap::ArcSwapOption::default()
        } else {
            let mut v: Vec<f32> = supplied.to_vec();
            embeddings::normalise_in_place(&mut v);
            arc_swap::ArcSwapOption::from(Some(StdArc::new(v)))
        }
    };
    let qualified_name = id.qualified_name();
    let tool_name_attr = StringValue::from(StdArc::<str>::from(qualified_name.as_str()));
    let bm25_doc = Bm25Document::from_tool_parts(&qualified_name, &description, &input_schema);
    let tool = {
        let mut tool = Tool::new(qualified_name, description.to_string(), StdArc::new(input_schema.clone()));
        tool.meta = Some(model::MetaObject(Map::from_iter([(
            "arion/namespace".to_owned(),
            Value::String(id.namespace.to_string()),
        )])));
        if !output_schema.is_empty() {
            tool = tool.with_raw_output_schema(StdArc::new(output_schema.clone()));
        }
        tool
    };
    Ok(ToolEntry {
        id,
        description,
        input_schema,
        output_schema,
        source,
        backend,
        rbac,
        disclosure_mode,
        input_schema_validator,
        output_schema_validator,
        bm25_doc,
        embedding,
        tool,
        tool_name_attr,
    })
}

/// Rejects an explicit zero limit; shared by search and catalog pagination.
pub(super) fn validate_positive_limit(requested_limit: Option<usize>) -> Result<(), CallToolError> {
    if requested_limit == Some(0) {
        return Err(CallToolError::ValidationError("limit must be greater than zero".to_owned()));
    }
    Ok(())
}

pub(crate) fn catalog_page<T>(
    items: Vec<T>,
    requested_limit: Option<usize>,
    requested_offset: Option<usize>,
) -> Result<CatalogPage<T>, CallToolError> {
    const DEFAULT_LIMIT: usize = 20;
    const MAX_LIMIT: usize = 100;

    validate_positive_limit(requested_limit)?;
    let limit = requested_limit.unwrap_or(DEFAULT_LIMIT).min(MAX_LIMIT);
    let offset = requested_offset.unwrap_or(0);
    let total = items.len();
    let page = items.into_iter().skip(offset).take(limit).collect::<Vec<_>>();
    let next = offset.saturating_add(page.len());
    let has_more = next < total;
    Ok(CatalogPage { items: page, total, limit, offset, has_more, next_offset: has_more.then_some(next) })
}

/// Convert configuration RBAC to runtime RBAC
pub(super) fn convert_config_rbac_to_runtime(config_rbac: &McpToolRbac) -> ToolRbac {
    let action = match config_rbac.action {
        Action::Allow => RbacAction::Allow,
        Action::Deny => RbacAction::Deny,
    };

    let permissions = config_rbac
        .permissions
        .iter()
        .map(|p| match p {
            McpRbacPermission::JwtHeader { field, value } => {
                #[allow(clippy::wildcard_in_or_patterns)]
                let header_field = match field.as_str() {
                    "alg" | "algorithm" => JwtHeaderField::Algorithm,
                    "typ" | "type" => JwtHeaderField::Type,
                    "cty" | "content_type" => JwtHeaderField::ContentType,
                    "jku" | "json_key_url" => JwtHeaderField::JsonKeyURL,
                    "jwk" | "json_web_key" => JwtHeaderField::JsonWebKey,
                    "x5u" | "x509_url" => JwtHeaderField::X509URL,
                    "x5c" | "x509_certificate_chain" => JwtHeaderField::X509CertificateChain,
                    "x5t" | "x509_certificate_sha1_thumbprint" => JwtHeaderField::X509CertificateSHA1Thumbprint,
                    "x5t#S256" | "x509_certificate_sha256_thumbprint" => {
                        JwtHeaderField::X509CertificateSHA256Thumbprint
                    },
                    "crit" | "critical" => JwtHeaderField::Critical,
                    "enc" | "encryption" => JwtHeaderField::Encryption,
                    "zip" => JwtHeaderField::Zip,
                    "url" => JwtHeaderField::Url,
                    "nonce" => JwtHeaderField::Nonce,
                    "kid" | "key_id" | _ => JwtHeaderField::KeyID,
                };
                RbacPermission::JwtHeader(JwtHeaderMatcher { field: header_field, value: value.clone() })
            },
            McpRbacPermission::JwtClaim { field, value } => {
                let claim_field = match field.as_str() {
                    "iss" | "issuer" => JwtClaimField::Issuer,
                    "sub" | "subject" => JwtClaimField::Subject,
                    "aud" | "audience" => JwtClaimField::Audience,
                    "exp" | "expiration" => JwtClaimField::Expiration,
                    "iat" | "issued_at" => JwtClaimField::IssuedAt,
                    "nbf" | "not_before" => JwtClaimField::NotBefore,
                    "jti" | "jwt_id" => JwtClaimField::JwtID,
                    custom => JwtClaimField::Extra(custom.into()),
                };
                RbacPermission::JwtClaim(JwtPayloadMatcher { field: claim_field, value: value.clone() })
            },
        })
        .collect();

    ToolRbac { action, permissions }
}
