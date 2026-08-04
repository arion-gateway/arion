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

pub mod context;
pub mod grammar;
pub mod header_formatter;
pub mod operator;
pub mod types;

use crate::grammar::{AccessLogGrammar, ENVOY_OPERATORS};
use arrayvec::ArrayString;
use context::Context;
use operator::{Category, Operator};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use smol_str::{format_smolstr, SmolStr};
use std::{
    collections::HashSet,
    fmt::{self, Display, Formatter},
    io::IoSlice,
    sync::{Arc, OnceLock},
};

static CUSTOM_OPERATORS: OnceLock<HashSet<SmolStr>> = OnceLock::new();
use thiserror::Error;

pub const DEFAULT_ACCESS_LOG_FORMAT: &str = r#"[%START_TIME%] "%REQ(:METHOD)% %REQ(X-ENVOY-ORIGINAL-PATH?:PATH)% %PROTOCOL%" %RESPONSE_CODE% %RESPONSE_FLAGS% %BYTES_RECEIVED% %BYTES_SENT% %DURATION% %RESP(X-ENVOY-UPSTREAM-SERVICE-TIME)% "%REQ(X-FORWARDED-FOR)%" "%REQ(USER-AGENT)%" "%REQ(X-REQUEST-ID)%" "%REQ(:AUTHORITY)%" "%UPSTREAM_HOST%"
"#;

pub const DEFAULT_ISTIO_ACCESS_LOG_FORMAT: &str = r#"[%START_TIME%] "%REQ(:METHOD)% %REQ(X-ENVOY-ORIGINAL-PATH?:PATH)% %PROTOCOL%" %RESPONSE_CODE% %RESPONSE_FLAGS% %RESPONSE_CODE_DETAILS% %CONNECTION_TERMINATION_DETAILS% "%UPSTREAM_TRANSPORT_FAILURE_REASON%" %BYTES_RECEIVED% %BYTES_SENT% %DURATION% %RESP(X-ENVOY-UPSTREAM-SERVICE-TIME)% "%REQ(X-FORWARDED-FOR)%" "%REQ(USER-AGENT)%" "%REQ(X-REQUEST-ID)%" "%REQ(:AUTHORITY)%" "%UPSTREAM_HOST%" %UPSTREAM_CLUSTER_RAW% %UPSTREAM_LOCAL_ADDRESS%  %DOWNSTREAM_LOCAL_ADDRESS% %DOWNSTREAM_REMOTE_ADDRESS% %REQUESTED_SERVER_NAME% %ROUTE_NAME%"
"#;

#[allow(clippy::implicit_hasher)]
pub fn set_custom_operators(custom_ops: HashSet<SmolStr>) -> Result<(), FormatError> {
    for op in &custom_ops {
        if op.contains('%') || op.contains(' ') {
            return Err(FormatError::InvalidCustomOperator("cannot contain '%' or whitespace".to_owned()));
        }

        if ENVOY_OPERATORS.get(op.as_str().bytes()).is_some() {
            return Err(FormatError::InvalidCustomOperator(format!("{op} conflicts with built-in operator")));
        }
    }

    _ = CUSTOM_OPERATORS.set(custom_ops);
    Ok(())
}

#[derive(Error, Debug, Eq, PartialEq)]
pub enum FormatError {
    #[error("invalid operator `{0}`")]
    InvalidOperator(String),
    #[error("unsupported operator `{0}`")]
    UnsupportedOperator(String),
    #[error("missing argument `{0}`")]
    MissingArgument(String),
    #[error("missing bracket `{0}`")]
    MissingBracket(String),
    #[error("missing delimiter `{0}`")]
    MissingDelimiter(String),
    #[error("empty argument `{0}`")]
    EmptyArgument(String),
    #[error("invalid request argument `{0}`")]
    InvalidRequestArg(String),
    #[error("invalid response argument `{0}`")]
    InvalidResponseArg(String),
    #[error("invalid custom operator `{0}`")]
    InvalidCustomOperator(String),
}

#[derive(Hash, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub enum Template {
    Char(char),
    Literal(SmolStr),
    Placeholder(Operator, Category), // eg. ("DURATION", Pattern::Duration, None), (Pattern::Req, Some(":METHOD"))
    Custom(SmolStr),
}

impl Template {
    pub fn is_placeholder(&self) -> bool {
        matches!(self, Template::Placeholder(_, _) | Template::Custom(_))
    }

    pub fn is_unsupported(&self) -> bool {
        match self {
            Template::Placeholder(_, cat) => cat.contains(Category::UNSUPPORTED),
            _ => false,
        }
    }
}

impl Display for Template {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Template::Char(c) => write!(f, "{c}"),
            Template::Placeholder(op, _) => write!(f, "{op:?}"),
            Template::Literal(s) | Template::Custom(s) => write!(f, "{s}"),
        }
    }
}

#[derive(Hash, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub enum StringType {
    Smol(SmolStr),
    Bytes(Box<[u8]>),
    Array(ArrayString<64>),
    None,
}

#[derive(Hash, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
struct LogFormatterConf {
    templates: Vec<Template>,
    omit_empty_values: bool,
}

#[derive(Debug, Serialize, Deserialize, Hash)]
#[allow(clippy::unsafe_derive_deserialize)]
#[allow(clippy::derived_hash_with_manual_eq)]
pub struct LogFormatter {
    conf: Arc<LogFormatterConf>,
    format: Vec<StringType>,
}

impl PartialEq for LogFormatter {
    fn eq(&self, other: &Self) -> bool {
        self.conf == other.conf && self.format == other.format
    }
}

impl Eq for LogFormatter {}

impl Clone for LogFormatter {
    fn clone(&self) -> Self {
        LogFormatter { conf: Arc::clone(&self.conf), format: self.format.clone() }
    }
}

impl LogFormatter {
    pub fn try_new(input: &str, omit_empty_values: bool) -> Result<LogFormatter, FormatError> {
        let templates = AccessLogGrammar::parse(input)?;

        for template in &templates {
            if template.is_unsupported() {
                return Err(FormatError::UnsupportedOperator(template.to_string()));
            }
        }

        let mut format: Vec<StringType> = Vec::with_capacity(templates.len());

        for t in &templates {
            match t {
                Template::Char(c) => {
                    // Pre-encode once at construction time; eliminates encode_utf8 from the write hot path.
                    let mut buf = [0u8; 4];
                    let s = c.encode_utf8(&mut buf);
                    format.push(StringType::Smol(SmolStr::new(s)));
                },
                Template::Literal(smol_str) => format.push(StringType::Smol(smol_str.clone())),
                Template::Placeholder(_, _) | Template::Custom(_) => format.push(StringType::None),
            }
        }

        Ok(LogFormatter { conf: Arc::new(LogFormatterConf { templates, omit_empty_values }), format })
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.format.is_empty()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.format.len()
    }

    pub fn with_context<C: Context>(&mut self, ctx: &C) -> &mut Self {
        for (idx, template) in self.conf.templates.iter().enumerate() {
            if let Template::Placeholder(op, _) = template {
                // SAFETY: `idx` is guaranteed to be valid for `format` vector by construction.
                if matches!(unsafe { self.format.get_unchecked(idx) }, StringType::None) {
                    let res = ctx.eval_op(op);
                    if !matches!(res, StringType::None) {
                        // SAFETY: `idx` is guaranteed to be valid for `format` vector, by construction.
                        // SAFETY: ptr::write without dropping the old value, since it does not require destruction
                        // (it is guaranteed to be StringType::None).
                        #[allow(clippy::multiple_unsafe_ops_per_block)]
                        unsafe {
                            std::ptr::write(self.format.get_unchecked_mut(idx), res)
                        };
                    }
                }
            }
        }
        self
    }

    pub fn with_value(&mut self, value: &serde_json::Value) -> &mut Self {
        let Some(obj) = value.as_object() else {
            return self;
        };

        // Try to match against custom placeholders (Template::Custom)
        for (idx, template) in self.conf.templates.iter().enumerate() {
            if let Template::Custom(name) = template {
                if let Some(val) = obj.get(name.as_str()) {
                    // SAFETY: `idx` is guaranteed to be valid for `format` vector by construction.
                    if matches!(unsafe { self.format.get_unchecked(idx) }, StringType::None) {
                        let res = json_value_to_string_type(val);
                        if !matches!(res, StringType::None) {
                            // SAFETY: `idx` is guaranteed to be valid for `format` vector, by construction.
                            // SAFETY: ptr::write without dropping the old value, since it does not require destruction
                            // (it is guaranteed to be StringType::None).
                            #[allow(clippy::multiple_unsafe_ops_per_block)]
                            unsafe {
                                std::ptr::write(self.format.get_unchecked_mut(idx), res)
                            };
                        }
                    }
                }
            }
        }

        self
    }

    pub fn with_custom_value(&mut self, key: &str, value: &str) -> &mut Self {
        for (idx, template) in self.conf.templates.iter().enumerate() {
            if let Template::Custom(name) = template {
                if name.as_str() == key {
                    // SAFETY: `idx` is guaranteed to be valid for `format` vector by construction.
                    if matches!(unsafe { self.format.get_unchecked(idx) }, StringType::None) && value != "null" {
                        let res = StringType::Smol(SmolStr::new(value));
                        // SAFETY: `idx` is guaranteed to be valid for `format` vector, by construction.
                        // SAFETY: ptr::write without dropping the old value, since it does not require destruction
                        // (it is guaranteed to be StringType::None).
                        #[allow(clippy::multiple_unsafe_ops_per_block)]
                        unsafe {
                            std::ptr::write(self.format.get_unchecked_mut(idx), res)
                        };
                    }
                }
            }
        }
        self
    }

    #[inline]
    pub fn into_message(self) -> FormattedMessage {
        FormattedMessage { format: self.format, omit_empty_values: self.conf.omit_empty_values }
    }
}

fn json_value_to_string_type(val: &serde_json::Value) -> StringType {
    match val {
        serde_json::Value::Null => StringType::None,
        serde_json::Value::Bool(b) => StringType::Smol(SmolStr::new_static(if *b { "true" } else { "false" })),
        serde_json::Value::Number(n) => {
            if let Some(u) = n.as_u64() {
                StringType::Smol(format_smolstr!("{u}"))
            } else if let Some(i) = n.as_i64() {
                StringType::Smol(format_smolstr!("{i}"))
            } else if let Some(f) = n.as_f64() {
                StringType::Smol(format_smolstr!("{f}"))
            } else {
                StringType::None
            }
        },
        serde_json::Value::String(s) => StringType::Smol(SmolStr::new(s)),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => StringType::Smol(format_smolstr!("{val}")),
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct FormattedMessage {
    omit_empty_values: bool,
    format: Vec<StringType>,
}

impl FormattedMessage {
    pub fn write_to<W: std::io::Write>(&self, w: &mut W) -> std::io::Result<usize> {
        let none_bytes: &[u8] = b"-";

        let mut slices: SmallVec<[IoSlice<'_>; 64]> = SmallVec::new();

        for out in &self.format {
            match out {
                StringType::Smol(s) => slices.push(IoSlice::new(s.as_bytes())),
                StringType::Bytes(v) => slices.push(IoSlice::new(v.as_ref())),
                StringType::Array(v) => slices.push(IoSlice::new(v.as_bytes())),
                StringType::None if !self.omit_empty_values => slices.push(IoSlice::new(none_bytes)),
                StringType::None => {},
            }
        }

        let total: usize = slices.iter().map(|s| s.len()).sum();

        // Single write_vectored call instead of one write_all per element.
        //
        let mut remaining = slices.as_mut_slice();
        while !remaining.is_empty() {
            let n = w.write_vectored(remaining)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WriteZero,
                    "write_to: write_vectored wrote 0 bytes",
                ));
            }
            IoSlice::advance_slices(&mut remaining, n);
        }

        Ok(total)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.format.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.format.is_empty()
    }
}

impl Display for FormattedMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for out in &self.format {
            match out {
                StringType::Smol(s) => f.write_str(s.as_ref())?,
                StringType::Bytes(v) => f.write_str(&String::from_utf8_lossy(v))?,
                StringType::Array(v) => f.write_str(v)?,
                StringType::None => {
                    if !self.omit_empty_values {
                        f.write_str("-")?
                    }
                },
            }
        }
        Ok(())
    }
}

pub trait Grammar {
    fn parse(input: &str) -> Result<Vec<Template>, FormatError>;
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use http::{HeaderValue, Request, Response, StatusCode};

    use crate::{
        context::{
            DownstreamContext, DownstreamResponseContext, FinishContext, InitContext, SocketAddrContext,
            UpstreamContext, UpstreamRequestContext,
        },
        types::ResponseFlags,
    };

    use super::*;

    fn build_request() -> Request<()> {
        Request::builder().uri("https://www.rust-lang.org/").header("User-Agent", "awesome/1.0").body(()).unwrap()
    }

    fn build_response() -> Response<()> {
        let builder = Response::builder().status(StatusCode::OK);
        builder.body(()).unwrap()
    }

    #[test]
    fn test_request_path() {
        let mut req = build_request();
        req.headers_mut().append("X-ENVOY-ORIGINAL-PATH", HeaderValue::from_static("/original"));

        let source = LogFormatter::try_new("%REQ(:PATH)%", false).unwrap();
        let mut formatter = source.clone();
        let expected = "/";

        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });
        let actual = format!("{}", &formatter.into_message());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_request_original_path() {
        let mut req = build_request();
        req.headers_mut().append("X-ENVOY-ORIGINAL-PATH", HeaderValue::from_static("/original"));

        let source = LogFormatter::try_new("%REQ(X-ENVOY-ORIGINAL-PATH?:PATH)%", false).unwrap();
        let mut formatter = source.clone();
        let expected = "/original";

        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });
        let actual = format!("{}", &formatter.into_message());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_request_method() {
        let req = build_request();
        let source = LogFormatter::try_new("%REQ(:METHOD)%", false).unwrap();
        let mut formatter = source.clone();
        println!("FORMATTER: {formatter:?}");
        let expected = "GET";
        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });
        let actual = format!("{}", &formatter.into_message());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_request_protocol() {
        let req = build_request();
        let source = LogFormatter::try_new("%PROTOCOL%", false).unwrap();
        let mut formatter = source.clone();
        println!("FORMATTER: {formatter:?}");
        let expected = "HTTP/1.1";
        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });
        let actual = format!("{}", &formatter.into_message());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_request_upstream_protocol() {
        let req = build_request();
        let source = LogFormatter::try_new("%UPSTREAM_PROTOCOL%", false).unwrap();
        let mut formatter = source.clone();
        println!("FORMATTER: {formatter:?}");
        let expected = "HTTP/1.1";
        formatter.with_context(&UpstreamRequestContext(&req));
        let actual = format!("{}", &formatter.into_message());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_request_scheme() {
        let req = build_request();
        let source = LogFormatter::try_new("%REQ(:SCHEME)%", false).unwrap();
        let mut formatter = source.clone();
        let expected = "https";
        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });
        let actual = format!("{}", &formatter.into_message());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_request_authority() {
        let req = build_request();
        let source = LogFormatter::try_new("%REQ(:AUTHORITY)%", false).unwrap();
        let mut formatter = source.clone();
        let expected = "www.rust-lang.org";
        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });
        let actual = format!("{}", &formatter.into_message());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_request_user_agent() {
        let req = build_request();
        let source = LogFormatter::try_new("%REQ(USER-AGENT)%", false).unwrap();
        let mut formatter = source.clone();
        let expected = "awesome/1.0";
        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });
        let actual = format!("{}", &formatter.into_message());
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_unevaluated_operator() {
        let source = LogFormatter::try_new("%REQ(USER-AGENT)%", false).unwrap();
        let formatter = source.clone();
        let actual = format!("{}", &formatter.into_message());
        println!("{actual}");
    }

    #[test]
    fn test_raw_string() {
        let source = LogFormatter::try_new("raw string", false).unwrap();
        let formatter = source.clone();
        let actual = format!("{}", &formatter.into_message());
        assert_eq!(actual, "raw string");
    }

    #[test]
    fn default_format_string() {
        let req = build_request();
        let resp = build_response();
        let source = LogFormatter::try_new(DEFAULT_ACCESS_LOG_FORMAT, false).unwrap();
        let mut formatter = source.clone();
        formatter.with_context(&InitContext { start_time: std::time::SystemTime::now() });
        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });
        formatter.with_context(&UpstreamContext {
            authority: Some(req.uri().authority().unwrap()),
            cluster_name: Some("test_cluster"),
            route_name: "test_route",
        });
        formatter.with_context(&DownstreamResponseContext { response: &resp, response_head_size: 0 });
        formatter.with_context(&FinishContext {
            duration: Duration::from_millis(100),
            bytes_received: 128,
            bytes_sent: 256,
            response_flags: ResponseFlags::NO_HEALTHY_UPSTREAM,
            upstream_transport_failure_reason: None,
            response_code_details: None,
            connection_termination_details: None,
        });
        println!("{}", &formatter.into_message());
    }

    #[test]
    fn default_istio_format_string() {
        let req = build_request();
        let resp = build_response();
        let source = LogFormatter::try_new(DEFAULT_ISTIO_ACCESS_LOG_FORMAT, false).unwrap();
        let mut formatter = source.clone();
        formatter.with_context(&InitContext { start_time: std::time::SystemTime::now() });
        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });
        formatter.with_context(&UpstreamContext {
            authority: Some(req.uri().authority().unwrap()),
            cluster_name: Some("test_cluster"),
            route_name: "test_route",
        });
        formatter.with_context(&DownstreamResponseContext { response: &resp, response_head_size: 0 });
        formatter.with_context(&FinishContext {
            duration: Duration::from_millis(100),
            bytes_received: 128,
            bytes_sent: 256,
            response_flags: ResponseFlags::NO_HEALTHY_UPSTREAM,
            upstream_transport_failure_reason: None,
            response_code_details: None,
            connection_termination_details: None,
        });
        println!("{}", &formatter.into_message());
    }

    #[test]
    fn test_sizes() {
        println!("Vec:       {}", std::mem::size_of::<Vec<u8>>());
        println!("SmolStr:   {}", std::mem::size_of::<SmolStr>());
        println!("Box<[u8]>: {}", std::mem::size_of::<Box<[u8]>>());
    }

    fn init_custom_operators() {
        let mut ops = std::collections::HashSet::new();
        ops.insert(SmolStr::new("MY_CUSTOM_KEY"));
        ops.insert(SmolStr::new("KEY1"));
        ops.insert(SmolStr::new("KEY2"));
        ops.insert(SmolStr::new("KEY3"));
        ops.insert(SmolStr::new("KEY4"));
        _ = CUSTOM_OPERATORS.set(ops);
    }

    #[test]
    fn test_with_value() {
        init_custom_operators();

        let source = LogFormatter::try_new("[%KEY1%] %KEY2% %KEY3% %KEY4%", false).unwrap();
        let mut formatter = source.clone();

        let value = serde_json::json!({
            "KEY1": "2026-06-08T12:00:00Z",
            "KEY2": 200,
            "KEY3": "Mozilla/5.0",
            "KEY4": "rust-lang.org"
        });

        formatter.with_value(&value);
        let msg = formatter.into_message();
        let mut buf = Vec::new();
        msg.write_to(&mut buf).unwrap();
        let result = String::from_utf8(buf).unwrap();
        assert_eq!(result, "[2026-06-08T12:00:00Z] 200 Mozilla/5.0 rust-lang.org");
    }

    #[test]
    fn test_with_value_free_placeholder() {
        init_custom_operators();

        let source = LogFormatter::try_new("[%START_TIME%] %MY_CUSTOM_KEY%", false).unwrap();
        let mut formatter = source.clone();

        let value = serde_json::json!({
            "START_TIME": "2026-06-08T12:00:00Z",
            "MY_CUSTOM_KEY": "hello_world"
        });

        formatter.with_value(&value);
        let msg = formatter.into_message();
        let mut buf = Vec::new();
        msg.write_to(&mut buf).unwrap();
        let result = String::from_utf8(buf).unwrap();
        assert_eq!(result, "[-] hello_world");
    }

    #[test]
    fn test_with_value_free_placeholder_omit_empty() {
        init_custom_operators();

        // Test with omit_empty_values = false (should print "-")
        let mut formatter_keep = LogFormatter::try_new("%KEY1% %KEY2%", false).unwrap();
        let value = serde_json::json!({
            "KEY1": "val1"
        });
        formatter_keep.with_value(&value);
        let mut buf = Vec::new();
        formatter_keep.into_message().write_to(&mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "val1 -");

        // Test with omit_empty_values = true (should omit KEY2)
        let mut formatter_omit = LogFormatter::try_new("%KEY1% %KEY2%", true).unwrap();
        formatter_omit.with_value(&value);
        let mut buf = Vec::new();
        formatter_omit.into_message().write_to(&mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "val1 ");
    }

    #[test]
    fn test_with_value_mixed_placeholders() {
        init_custom_operators();

        let mut formatter = LogFormatter::try_new("%PROTOCOL% %KEY1% %RESPONSE_CODE% %KEY2%", false).unwrap();
        let value = serde_json::json!({
            "PROTOCOL": "HTTP/2",
            "KEY1": "custom1",
            "RESPONSE_CODE": 404,
            "KEY2": "custom2"
        });
        formatter.with_value(&value);
        let mut buf = Vec::new();
        formatter.into_message().write_to(&mut buf).unwrap();
        assert_eq!(String::from_utf8(buf).unwrap(), "- custom1 - custom2");
    }

    #[test]
    fn test_with_value_no_overwrite_builtin() {
        init_custom_operators();

        // Create a formatter with a mix of standard and custom operators
        let mut formatter = LogFormatter::try_new("%PROTOCOL% %KEY1% %RESPONSE_CODE%", false).unwrap();

        // 1. First, populate standard operators using with_context
        let req = build_request();
        formatter.with_context(&DownstreamContext {
            request: &req,
            request_head_size: 0,
            trace_id: None,
            server_name: None,
            socket_address: SocketAddrContext::default(),
        });

        let resp = build_response();
        formatter.with_context(&DownstreamResponseContext { response: &resp, response_head_size: 0 });

        // 2. Now call with_value with a JSON payload that tries to:
        //    - Overwrite the already-populated standard operators (PROTOCOL, RESPONSE_CODE)
        //    - Populate the custom operator (KEY1)
        let value = serde_json::json!({
            "PROTOCOL": "HTTP/2",         // Attempted overwrite of populated standard op
            "RESPONSE_CODE": 500,         // Attempted overwrite of populated standard op
            "KEY1": "custom-value",       // Custom operator
        });

        formatter.with_value(&value);

        let mut buf = Vec::new();
        formatter.into_message().write_to(&mut buf).unwrap();
        let result = String::from_utf8(buf).unwrap();

        // The standard operators must NOT be overwritten!
        // - PROTOCOL should remain "HTTP/1.1" (from context)
        // - RESPONSE_CODE should remain "200" (from context)
        // - KEY1 should be populated with "custom-value"
        assert_eq!(result, "HTTP/1.1 custom-value 200");
    }
}
