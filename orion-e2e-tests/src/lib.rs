// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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

#![allow(clippy::expect_used, reason = "test infrastructure — panicking on setup failure is intentional")]

pub mod config_builder;
mod embeddings_service;
mod error;
pub mod ext_proc_test_server;
mod grpc_test_backend;
mod grpc_test_client;
pub mod mcp_gateway;
pub mod orion_instance;
pub(crate) mod port_allocator;
pub mod pp_test_client;
pub mod raw_http;
pub mod rls_test_server;
mod tcp_test_backend;
mod tcp_test_client;
mod test_backend;
mod test_certs;
mod test_client;
mod tls_test_backend;
mod tls_test_client;
mod xds_harness;
pub mod xds_server;

pub use embeddings_service::{CapturedEmbeddingsTestRequest, EmbeddingsTestService};
pub use error::{Error, Result};
pub use ext_proc_test_server::{
    ext_proc_responses, CapturedProcessingRequest, ExtProcTestServer, ExtProcTestServerBuilder,
};
pub use grpc_test_backend::test_proto::EchoResponse;
pub use grpc_test_backend::{GrpcTestBackend, GrpcTestBackendBuilder};
pub use grpc_test_client::GrpcTestClient;
pub use mcp_gateway::{
    generate_jwt_token, CallToolParams, CallToolResult, JwtKeyPair, ListToolsResult, McpJsonRpcError,
    McpJsonRpcRequest, McpJsonRpcResponse, McpResultExt, McpTestClient, McpTool, MockMcpServer, TestJwtClaims,
    ToolContent,
};
pub use orion_instance::{OrionInstance, SpawnOptions};
pub use port_allocator::PortBlock;
pub use pp_test_client::ProxyProtocolTcpClient;
pub use raw_http::{assert_rejected, PartialSendClient, RawHttpRequestBuilder, RawHttpResponse};
pub use rls_test_server::{rls_responses, RlsTestServer, RlsTestServerBuilder};
pub use tcp_test_backend::{CapturedTcpConnection, TcpTestBackend};
pub use tcp_test_client::TcpTestClient;
pub use test_backend::{CapturedRequest, PreConfiguredResponse, TestBackend};
pub use test_certs::TestCerts;
pub use test_client::{RequestBuilder, TestClient, TestResponse};
pub use tls_test_backend::{TlsBackendConfig, TlsTestBackend};
pub use tls_test_client::{TlsClientConfig, TlsTestClient, TlsTestClientBuilder};
pub use xds_harness::{HarnessError, HarnessTimeouts, XdsEnabledHarness, XdsHarnessOptions};
pub use xds_server::ServerEvent;

/// Fixed admin port used by test Orion instances. Hardcoded so that tests do not need
/// dynamic port allocation for the admin interface. E2e tests must run single-threaded
/// (--test-threads=1) to avoid port conflicts across concurrent test binaries.
pub const TEST_ADMIN_PORT: u16 = 9901;

pub fn cleanup_config_file(path: &std::path::Path) {
    if let Err(e) = std::fs::remove_file(path) {
        tracing::warn!(?e, ?path, "Failed to remove config file");
    }
}

pub fn parse_metric_value(prometheus_output: &str, metric_name: &str) -> Option<u64> {
    for line in prometheus_output.lines() {
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with(metric_name) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if let Some(val_str) = parts.last() {
                if let Ok(val) = val_str.parse::<u64>() {
                    return Some(val);
                }
            }
        }
    }
    None
}
