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

use serde_json::Value;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuestErrorKind {
    JavaScript,
    Fatal,
}

#[derive(Debug, Error)]
pub enum CodeModeError {
    #[error("failed to initialize the codemode engine: {0}")]
    Engine(#[source] wasmtime::Error),
    #[error("the codemode guest artifact is invalid: {0}")]
    Artifact(#[source] wasmtime::Error),
    #[error("failed to read the codemode guest artifact: {0}")]
    ArtifactRead(#[source] std::io::Error),
    #[error("failed to link the codemode guest: {0}")]
    Link(#[source] wasmtime::Error),
    #[error("invalid codemode limits: {0}")]
    InvalidLimits(&'static str),
    #[error("the codemode execute input schema is invalid: {0}")]
    ExecuteSchema(String),
    #[error("codemode execution failed")]
    Execution,
    #[error("codemode execution timed out")]
    Timeout,
    #[error("codemode CPU budget was exhausted")]
    CpuLimit,
    #[error("codemode host-call budget was exhausted")]
    HostCallLimit,
    #[error("too many codemode executions are active")]
    ConcurrencyLimit,
    #[error("the codemode guest returned invalid JSON")]
    InvalidResult,
    #[error("the codemode result exceeded its size limit")]
    ResultTooLarge,
    #[error("the codemode script exceeded its size limit")]
    ScriptTooLarge,
    #[error("TypeScript syntax error at {line}:{col}: {message}")]
    Syntax { message: String, line: usize, col: usize },
    #[error("unsupported TypeScript syntax at {line}:{col}: {message}")]
    UnsupportedSyntax { message: String, line: usize, col: usize },
    #[error("failed to strip TypeScript types")]
    TypeStrip,
    #[error("JavaScript execution failed: {message}")]
    Guest { kind: GuestErrorKind, message: String },
}

impl CodeModeError {
    /// The client-facing diagnostic reported in an execute result.
    pub(super) fn diagnostic(&self) -> Value {
        let (kind, message, location) = match self {
            Self::Syntax { message, line, col } | Self::UnsupportedSyntax { message, line, col } => {
                ("ParseError", message.as_str(), Some((*line, *col)))
            },
            Self::Timeout => ("TimeoutExceeded", "Code Mode execution timed out", None),
            Self::CpuLimit => ("ResourceLimitExceeded", "Code Mode CPU budget was exhausted", None),
            Self::HostCallLimit => ("ResourceLimitExceeded", "Code Mode host-call budget was exhausted", None),
            Self::ResultTooLarge => {
                ("ResourceLimitExceeded", "Code Mode result exceeded its configured size limit", None)
            },
            Self::ScriptTooLarge => {
                ("ResourceLimitExceeded", "Code Mode script exceeded its configured size limit", None)
            },
            Self::Guest { kind: GuestErrorKind::JavaScript, message } => ("ExecutionFailure", message.as_str(), None),
            Self::Guest { kind: GuestErrorKind::Fatal, .. }
            | Self::Engine(_)
            | Self::Artifact(_)
            | Self::ArtifactRead(_)
            | Self::Link(_)
            | Self::InvalidLimits(_)
            | Self::ExecuteSchema(_)
            | Self::Execution
            | Self::InvalidResult
            | Self::TypeStrip
            | Self::ConcurrencyLimit => ("InternalError", "Code Mode execution failed", None),
        };
        let mut diagnostic = serde_json::Map::from_iter([
            ("kind".to_owned(), Value::from(kind)),
            ("message".to_owned(), Value::from(message)),
        ]);
        if let Some((line, column)) = location {
            diagnostic.insert("line".to_owned(), Value::from(line));
            diagnostic.insert("column".to_owned(), Value::from(column));
        }
        Value::Object(diagnostic)
    }
}
