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

//! Error types for tool invocation, listing, and registry construction.

use http::StatusCode;
use rmcp::{ServiceError, model, service::ClientInitializeError};
use serde_json::{Value, json};
use smol_str::SmolStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ToolInvocationErrorCode {
    InvalidOutput,
    UpstreamError,
    UpstreamTimeout,
    Overflow,
    ResponseTooLarge,
}

impl ToolInvocationErrorCode {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidOutput => "invalid_output",
            Self::UpstreamError => "upstream_error",
            Self::UpstreamTimeout => "upstream_timeout",
            Self::Overflow => "overflow",
            Self::ResponseTooLarge => "response_too_large",
        }
    }
}

/// A backend failure normalized into the gateway's model-facing error shape.
#[derive(Debug)]
pub(super) struct ToolInvocationFailure {
    pub(super) code: ToolInvocationErrorCode,
    pub(super) message: String,
    pub(super) status: Option<StatusCode>,
    pub(super) retryable: bool,
}

impl ToolInvocationFailure {
    pub(super) fn upstream_error() -> Self {
        Self {
            code: ToolInvocationErrorCode::UpstreamError,
            message: "The upstream tool is unavailable".to_owned(),
            status: Some(StatusCode::BAD_GATEWAY),
            retryable: true,
        }
    }

    pub(super) fn upstream_timeout() -> Self {
        Self {
            code: ToolInvocationErrorCode::UpstreamTimeout,
            message: "The upstream tool call timed out".to_owned(),
            status: Some(StatusCode::GATEWAY_TIMEOUT),
            retryable: true,
        }
    }

    pub(super) fn response_too_large(limit: usize) -> Self {
        Self {
            code: ToolInvocationErrorCode::ResponseTooLarge,
            message: format!("The upstream tool result exceeded the configured {limit}-byte limit"),
            status: Some(StatusCode::PAYLOAD_TOO_LARGE),
            retryable: false,
        }
    }

    pub(super) fn into_call_tool_result(self) -> model::CallToolResult {
        let mut error = serde_json::Map::from_iter([
            ("code".to_owned(), Value::String(self.code.as_str().to_owned())),
            ("message".to_owned(), Value::String(self.message)),
            ("retryable".to_owned(), Value::Bool(self.retryable)),
        ]);
        if let Some(status) = self.status {
            error.insert("status".to_owned(), Value::from(status.as_u16()));
        }
        model::CallToolResult::structured_error(json!({ "error": error }))
    }
}

pub(super) type ToolInvocationOutcome = Result<model::CallToolResult, ToolInvocationFailure>;

#[derive(Debug, thiserror::Error)]
pub enum CallToolError {
    #[error("'name' parameter is missing or not a string")]
    NameNotString,
    #[error("tool '{0}' not found in registry")]
    ToolNotFound(String),
    #[error("access to tool '{0}' denied by RBAC policy")]
    RbacDenied(String),
    #[error("Failed to encode request for tool '{tool}': {reason}")]
    RequestEncoding { tool: String, reason: String },
    #[error("Client initialization error: {0}")]
    ClientInitializeError(#[from] Box<ClientInitializeError>),
    #[error("ServiceError: {0}")]
    ServiceError(#[from] Box<ServiceError>),
    #[error("SerdeError: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("Validation error: {0}")]
    ValidationError(String),
    #[cfg(feature = "codemode")]
    #[error("too many Code Mode executions are active")]
    CodeModeConcurrencyLimit,
}

impl From<ClientInitializeError> for CallToolError {
    fn from(error: ClientInitializeError) -> Self {
        Self::ClientInitializeError(Box::new(error))
    }
}

impl From<ServiceError> for CallToolError {
    fn from(error: ServiceError) -> Self {
        Self::ServiceError(Box::new(error))
    }
}

impl From<CallToolError> for model::ErrorData {
    fn from(err: CallToolError) -> Self {
        match err {
            CallToolError::NameNotString => {
                model::ErrorData::invalid_params("'name' parameter is missing or not a valid string", None)
            },
            CallToolError::ToolNotFound(name) => {
                model::ErrorData::resource_not_found(format!("Tool '{name}' not found"), None)
            },
            CallToolError::RbacDenied(tool) => {
                model::ErrorData::invalid_request(format!("Access denied by RBAC policy for tool '{tool}'"), None)
            },
            CallToolError::RequestEncoding { tool, reason } => {
                model::ErrorData::internal_error(format!("Failed to encode request for tool '{tool}': {reason}"), None)
            },
            CallToolError::ClientInitializeError(init_err) => match *init_err {
                ClientInitializeError::JsonRpcError(error_data) => error_data,
                ClientInitializeError::ConnectionClosed(msg) => {
                    model::ErrorData::internal_error(format!("Upstream connection closed: {msg}"), None)
                },
                ClientInitializeError::TransportError { error, context } => {
                    model::ErrorData::internal_error(format!("Upstream transport error ({context}): {error}"), None)
                },
                ClientInitializeError::Cancelled => {
                    model::ErrorData::internal_error("Upstream client initialization was cancelled", None)
                },
                other => {
                    model::ErrorData::internal_error(format!("Upstream client initialization error: {other}"), None)
                },
            },
            CallToolError::SerdeError(e) => {
                model::ErrorData::parse_error(format!("Serialization error: {e}"), None)
            },
            CallToolError::ServiceError(svc_err) => match *svc_err {
                ServiceError::McpError(error_data) => error_data,
                ServiceError::TransportSend(e) => {
                    model::ErrorData::internal_error(format!("Upstream transport send error: {e}"), None)
                },
                ServiceError::TransportClosed => model::ErrorData::internal_error("Upstream transport closed", None),
                ServiceError::UnexpectedResponse => {
                    model::ErrorData::internal_error("Unexpected response from upstream", None)
                },
                ServiceError::Cancelled { reason } => model::ErrorData::internal_error(
                    format!("Upstream request cancelled: {}", reason.as_deref().unwrap_or("<unknown>")),
                    None,
                ),
                ServiceError::Timeout { timeout } => {
                    model::ErrorData::internal_error(format!("Upstream request timed out after {timeout:?}"), None)
                },
                other => model::ErrorData::internal_error(format!("Upstream service error: {other}"), None),
            },
            CallToolError::ValidationError(e) => {
                model::ErrorData::internal_error(format!("Json schema validation error: {e}"), None)
            },
            #[cfg(feature = "codemode")]
            CallToolError::CodeModeConcurrencyLimit => {
                model::ErrorData::invalid_request("Too many Code Mode executions are active", None)
            },
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ListToolsError {
    #[error("Unsupported transport")]
    UnsupportedTransport,
    #[error("Client: {0}")]
    ClientError(#[from] Box<ClientInitializeError>),
    #[error("ServiceError: {0}")]
    ServiceError(#[from] Box<ServiceError>),
    #[error("Invalid dynamic tool snapshot: {0}")]
    InvalidSnapshot(String),
}

impl From<ClientInitializeError> for ListToolsError {
    fn from(error: ClientInitializeError) -> Self {
        Self::ClientError(Box::new(error))
    }
}

impl From<ServiceError> for ListToolsError {
    fn from(error: ServiceError) -> Self {
        Self::ServiceError(Box::new(error))
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ToolBuilderError {
    #[error("Invalid input schema")]
    InvalidInputSchema(String),
    #[error("Invalid output schema")]
    InvalidOutputSchema(String),
    #[error("Failed compiling template: {0}")]
    FailedCompilingTemplate(#[from] Box<upon::Error>),
    #[error("body_template and body_json are mutually exclusive")]
    ConflictingBodyModes,
    #[error("Invalid REST upstream authority: {0}")]
    InvalidUpstreamAuthority(#[source] http::header::InvalidHeaderValue),
    #[error("Duplicate tool name: {0}")]
    DuplicateTool(SmolStr),
    #[error("Invalid tool namespace: {0}")]
    InvalidNamespace(SmolStr),
    #[error("Invalid local tool name: {0}")]
    InvalidToolName(SmolStr),
    #[error("Invalid qualified tool name: {0}")]
    InvalidQualifiedToolName(SmolStr),
    #[error("Duplicate dynamic MCP server namespace: {0}")]
    DuplicateDynamicServer(SmolStr),
    #[error("Duplicate OpenAPI tool source namespace: {0}")]
    DuplicateOpenApiSource(SmolStr),
    #[error("Namespace '{0}' is already owned by another tool source")]
    SharedNamespace(SmolStr),
    #[error("Dynamic MCP server refresh failed: {0}")]
    DynamicRefresh(String),
    #[error("MCP server name '{0}' already registered on this runtime")]
    DuplicateServerName(SmolStr),
    #[error(
        "Tool '{tool}' has supplied embedding of dimension {got}, but embeddings service produces dimension {expected}"
    )]
    EmbeddingDimensionMismatch { tool: SmolStr, expected: usize, got: usize },
    #[error("Tool '{0}' has a supplied embedding with non-finite components")]
    EmbeddingNotFinite(SmolStr),
    #[error("Invalid embeddings config: {0}")]
    InvalidEmbeddingsConfig(String),
    #[error("Code Mode requires building Arion with the 'codemode' feature")]
    CodeModeFeatureDisabled,
    #[cfg(feature = "codemode")]
    #[error("Invalid Code Mode configuration: {0}")]
    InvalidCodeMode(#[from] super::super::codemode::CodeModeError),
    #[cfg(feature = "codemode")]
    #[error("Invalid Code Mode toolkit configuration: {0}")]
    InvalidToolkit(String),
}

impl From<upon::Error> for ToolBuilderError {
    fn from(error: upon::Error) -> Self {
        Self::FailedCompilingTemplate(Box::new(error))
    }
}
