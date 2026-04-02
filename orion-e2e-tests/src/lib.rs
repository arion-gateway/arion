// Copyright 2025 The kmesh Authors
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

pub mod config_builder;
mod error;
pub mod ext_proc_test_server;
mod grpc_test_backend;
mod grpc_test_client;
pub mod mcp_gateway;
pub mod orion_instance;
pub(crate) mod port_allocator;
pub mod raw_http;
mod tcp_test_backend;
mod tcp_test_client;
mod test_backend;
mod test_certs;
mod test_client;
mod tls_test_backend;
mod tls_test_client;
mod xds_harness;
pub mod xds_server;

pub use error::{Error, Result};
pub use ext_proc_test_server::{
    ext_proc_responses, CapturedProcessingRequest, ExtProcTestServer, ExtProcTestServerBuilder,
};
pub use grpc_test_backend::test_proto::EchoResponse;
pub use grpc_test_backend::{GrpcTestBackend, GrpcTestBackendBuilder};
pub use grpc_test_client::GrpcTestClient;
pub use mcp_gateway::{
    generate_jwt_token, mcp_gateway_config, mcp_gateway_with_direct_semantic_search_config,
    mcp_gateway_with_jwt_and_semantic_search_config, mcp_gateway_with_jwt_config, mcp_server_tool_config, rbac_config,
    rest_tool_config, CallToolParams, CallToolResult, JwtKeyPair, ListToolsResult, McpJsonRpcError, McpJsonRpcRequest,
    McpJsonRpcResponse, McpResultExt, McpTestClient, McpTool, MockMcpServer, TestJwtClaims, ToolContent,
};
pub use orion_instance::{OrionInstance, SpawnOptions};
pub use port_allocator::PortBlock;
pub use raw_http::{assert_rejected, PartialSendClient, RawHttpRequestBuilder, RawHttpResponse};
pub use tcp_test_backend::{CapturedTcpConnection, TcpTestBackend};
pub use tcp_test_client::TcpTestClient;
pub use test_backend::{CapturedRequest, PreConfiguredResponse, TestBackend};
pub use test_certs::TestCerts;
pub use test_client::{RequestBuilder, TestClient, TestResponse};
pub use tls_test_backend::{TlsBackendConfig, TlsTestBackend};
pub use tls_test_client::{TlsClientConfig, TlsTestClient, TlsTestClientBuilder};
pub use xds_harness::{HarnessError, HarnessTimeouts, XdsEnabledHarness, XdsHarnessOptions};
