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

#![allow(clippy::indexing_slicing, clippy::assertions_on_result_states)]

use std::time::Duration;

use arion_configuration::config::network_filters::{
    http_connection_manager::http_filters::mcp_gateway::{
        EmbeddingVector, McpBackendTransportUpstream, McpRbacPermission, McpToolRbac, UpstreamBackend,
    },
    network_rbac::Action,
};
use serde_json::{json, Value};

use super::entry::ToolBackend;
use super::*;

fn create_test_tool_with_schemas(
    input_schema: serde_json::Map<String, Value>,
    output_schema: serde_json::Map<String, Value>,
) -> McpTool {
    McpTool {
        namespace: "test".into(),
        name: "test_tool".into(),
        description: "A test tool".into(),
        input_schema,
        output_schema,
        backend: UpstreamBackend::Rest {
            method: http::Method::GET,
            path: "/test".into(),
            query_params: vec![],
            cluster: "test_cluster".into(),
            upstream_policy: Box::default(),
            body_template: None,
        },
        rbac: None,
        embedding: EmbeddingVector::default(),
        disclosure_mode: DisclosureMode::Progressive,
    }
}

fn registry_with_tool(tool: McpTool) -> ToolsRegistry {
    ToolsRegistry::with_config(vec![tool], Vec::new(), McpToolSearch::default(), UpstreamLimits::default(), None)
        .unwrap()
}

fn dynamic_server_entry(namespace: &str) -> DynamicMcpServerEntry {
    DynamicMcpServerEntry::new(DynamicMcpServer {
        namespace: namespace.into(),
        description: "".into(),
        transport: McpBackendTransportUpstream::StreamableHttp,
        url: "http://localhost/mcp".into(),
        cache_duration: None,
        rbac: None,
        tool_disclosure_mode: DisclosureMode::Progressive,
    })
}

#[test]
fn tool_id_uses_the_first_dot_as_the_namespace_separator() {
    let id = ToolId::parse_qualified("crm.customer.find").unwrap();
    assert_eq!(id.namespace, "crm");
    assert_eq!(id.name, "customer.find");
    assert_eq!(id.qualified_name(), "crm.customer.find");

    assert!(ToolId::new("crm.v2", "find").is_err());
    assert!(ToolId::new("crm", "bad/name").is_err());
    assert!(ToolId::new("crm", "x".repeat(125)).is_err());
}

#[test]
fn duplicate_local_names_coexist_across_namespaces() {
    let mut crm = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    crm.namespace = "crm".into();
    crm.name = "search".into();
    crm.description = "Search customer records".into();
    crm.disclosure_mode = DisclosureMode::Eager;
    let mut github = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    github.namespace = "github".into();
    github.name = "search".into();
    github.description = "Search code repositories".into();
    github.disclosure_mode = DisclosureMode::Eager;
    let registry = ToolsRegistry::with_config(
        vec![github, crm],
        Vec::new(),
        McpToolSearch::default(),
        UpstreamLimits::default(),
        None,
    )
    .unwrap();

    let (listed, _) = registry.list_disclosed_tools(&http::Extensions::new());
    assert_eq!(listed.iter().map(|tool| tool.name.as_ref()).collect::<Vec<_>>(), ["crm.search", "github.search"]);
}

#[test]
fn catalog_page_respects_explicit_window_and_rejects_zero_limit() {
    let items: Vec<usize> = (0..25).collect();

    let last = catalog_page(items.clone(), Some(10), Some(20)).unwrap();
    assert_eq!((last.total, last.limit, last.offset), (25, 10, 20));
    assert_eq!(last.items, (20..25).collect::<Vec<_>>());
    assert!(!last.has_more);
    assert_eq!(last.next_offset, None);

    let past_end = catalog_page(items.clone(), Some(5), Some(100)).unwrap();
    assert!(past_end.items.is_empty());
    assert!(!past_end.has_more);
    assert_eq!(past_end.total, 25);

    assert!(catalog_page(items, Some(0), None).is_err());
}

#[test]
fn namespace_and_list_totals_are_computed_after_rbac() {
    let mut allowed = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    allowed.name = "allowed".into();
    let mut denied = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    denied.name = "denied".into();
    denied.rbac = Some(McpToolRbac {
        action: Action::Allow,
        permissions: vec![McpRbacPermission::JwtHeader { field: "kid".into(), value: "required".into() }],
    });
    let mut hidden_namespace = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    hidden_namespace.namespace = "secret".into();
    hidden_namespace.rbac = Some(McpToolRbac {
        action: Action::Allow,
        permissions: vec![McpRbacPermission::JwtHeader { field: "kid".into(), value: "required".into() }],
    });
    let registry = ToolsRegistry::with_config(
        vec![allowed, denied, hidden_namespace],
        Vec::new(),
        McpToolSearch::default(),
        UpstreamLimits::default(),
        None,
    )
    .unwrap();
    let req_ext = http::Extensions::new();

    let namespaces = registry.catalog_namespaces(&req_ext, None, None).unwrap();
    assert_eq!(namespaces.total, 1, "a namespace with only denied tools must not appear");
    assert_eq!(namespaces.items[0].namespace, "test");
    assert_eq!(namespaces.items[0].tool_count, 1, "toolCount counts only permitted tools");

    let listed = registry.catalog_list_tools(&req_ext, "test", None, None).unwrap();
    assert_eq!(listed.total, 1);
    assert_eq!(listed.items[0].name, "allowed");
}

#[tokio::test]
async fn failed_dynamic_refresh_retains_previous_tools() {
    let registry =
        ToolsRegistry::with_config(Vec::new(), Vec::new(), McpToolSearch::default(), UpstreamLimits::default(), None)
            .unwrap();

    // A server whose upstream is unreachable (closed port), with a
    // previously materialised snapshot.
    let server = StdArc::new(DynamicMcpServerEntry::new(DynamicMcpServer {
        namespace: "srv".into(),
        description: "".into(),
        transport: McpBackendTransportUpstream::StreamableHttp,
        url: "http://127.0.0.1:1/mcp".into(),
        cache_duration: None,
        rbac: None,
        tool_disclosure_mode: DisclosureMode::Progressive,
    }));
    registry.dynamic_mcp_servers.pin().insert("srv".into(), StdArc::clone(&server));
    let conf = McpTool {
        namespace: "srv".into(),
        name: "alpha".into(),
        description: "".into(),
        input_schema: serde_json::Map::new(),
        output_schema: serde_json::Map::new(),
        backend: UpstreamBackend::McpServer {
            transport: McpBackendTransportUpstream::StreamableHttp,
            url: "http://127.0.0.1:1/mcp".into(),
        },
        rbac: None,
        embedding: EmbeddingVector::default(),
        disclosure_mode: DisclosureMode::Progressive,
    };
    let entry =
        build_tool_entry(conf, ToolSource::Dynamic { upstream_tool_name: "alpha".into() }, &UpstreamLimits::default())
            .unwrap();
    registry.tools.pin().insert(entry.id.clone(), StdArc::new(entry));

    registry.fetch_and_materialise(&server).await;

    assert!(registry.get_tool_by_name("srv.alpha").is_some(), "a failed refresh must retain the previous snapshot");
}

#[test]
fn eager_tool_is_listed_without_tool_search() {
    let mut tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool.disclosure_mode = DisclosureMode::Eager;
    let registry = registry_with_tool(tool);

    let (listed, progressive) = registry.list_disclosed_tools(&http::Extensions::new());

    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].name.as_ref(), "test.test_tool");
    assert!(progressive.is_empty());
}

#[tokio::test]
async fn unlisted_progressive_tool_remains_callable() {
    let tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    let registry =
        ToolsRegistry::with_config(vec![tool], Vec::new(), McpToolSearch::default(), UpstreamLimits::default(), None)
            .unwrap();
    let cx = TestCallContext::default();

    let result = registry.invoke_tool(cx.context(), &ToolId::new("test", "test_tool").unwrap(), json!({})).await;

    assert!(matches!(result, Ok(ref call_result) if call_result.is_error == Some(true)), "{result:?}");
}

#[tokio::test]
async fn rbac_controls_tool_search_visibility_and_description() {
    let mut allowed = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    allowed.name = "allowed".into();
    let mut denied = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    denied.name = "denied".into();
    denied.rbac = Some(McpToolRbac {
        action: Action::Allow,
        permissions: vec![McpRbacPermission::JwtHeader { field: "kid".into(), value: "required".into() }],
    });
    let registry = ToolsRegistry::with_config(
        vec![denied, allowed],
        Vec::new(),
        McpToolSearch::default(),
        UpstreamLimits::default(),
        None,
    )
    .unwrap();

    let (listed, progressive) = registry.list_disclosed_tools(&http::Extensions::new());
    assert!(listed.is_empty());
    assert_eq!(progressive, ["test.allowed"]);

    let searched = registry
        .search_tools(
            &http::Extensions::new(),
            &RequestCtx::default(),
            ToolSearchRequest { query: "tool".to_owned(), limit: None, namespace: None },
            ToolSearchScope::Progressive,
        )
        .await
        .unwrap();
    assert_eq!(searched.iter().map(|tool| tool.name.as_ref()).collect::<Vec<_>>(), ["test.allowed"]);

    assert!(registry.remove_tool(&ToolId::new("test", "allowed").unwrap()));
    let (_, progressive) = registry.list_disclosed_tools(&http::Extensions::new());
    assert!(progressive.is_empty());
}

#[tokio::test]
async fn all_search_scope_includes_eager_tools() {
    let mut eager = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    eager.disclosure_mode = DisclosureMode::Eager;
    let registry = registry_with_tool(eager);
    let request = ToolSearchRequest { query: "test tool".to_owned(), limit: None, namespace: None };

    let progressive = registry
        .search_tools(&http::Extensions::new(), &RequestCtx::default(), request.clone(), ToolSearchScope::Progressive)
        .await
        .unwrap();
    let all = registry
        .search_tools(&http::Extensions::new(), &RequestCtx::default(), request, ToolSearchScope::All)
        .await
        .unwrap();

    assert!(progressive.is_empty());
    assert_eq!(all.iter().map(|tool| tool.name.as_ref()).collect::<Vec<_>>(), ["test.test_tool"]);
}

#[test]
fn describe_applies_tool_rbac() {
    let mut denied = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    denied.rbac = Some(McpToolRbac {
        action: Action::Allow,
        permissions: vec![McpRbacPermission::JwtHeader { field: "kid".into(), value: "required".into() }],
    });
    let registry = registry_with_tool(denied);

    let described =
        registry.catalog_describe_tool(&http::Extensions::new(), &ToolId::new("test", "test_tool").unwrap());
    assert!(described.is_none(), "unauthorized tools must be indistinguishable from missing ones");
}

#[test]
fn test_input_schema_validation_passes_with_valid_arguments() {
    let input_schema = serde_json::from_value(json!({
        "type": "object",
        "properties": {
            "username": { "type": "string" },
            "age": { "type": "integer" }
        },
        "required": ["username"]
    }))
    .unwrap();

    let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
    let registry = registry_with_tool(tool);
    let tool_entry = registry.get_tool_by_name("test.test_tool").unwrap();

    let args = json!({
        "username": "john_doe",
        "age": 25
    });

    assert!(tool_entry.validate_against_input_schema(&args).is_ok());
}

#[test]
fn test_input_schema_validation_fails_with_missing_required_field() {
    let input_schema = serde_json::from_value(json!({
        "type": "object",
        "properties": {
            "username": { "type": "string" }
        },
        "required": ["username"]
    }))
    .unwrap();

    let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
    let registry = registry_with_tool(tool);
    let tool_entry = registry.get_tool_by_name("test.test_tool").unwrap();

    let args = json!({});

    let result = tool_entry.validate_against_input_schema(&args);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("username"), "Error should mention missing field: {err_msg}");
}

#[test]
fn test_input_schema_validation_fails_with_wrong_type() {
    let input_schema = serde_json::from_value(json!({
        "type": "object",
        "properties": {
            "age": { "type": "integer" }
        }
    }))
    .unwrap();

    let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
    let registry = registry_with_tool(tool);
    let tool_entry = registry.get_tool_by_name("test.test_tool").unwrap();

    let args = json!({
        "age": "not a number"
    });

    let result = tool_entry.validate_against_input_schema(&args);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("number") || err_msg.contains("integer"), "Error should mention type mismatch: {err_msg}");
}

#[test]
fn test_input_schema_validation_skips_when_empty() {
    let tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    let registry = registry_with_tool(tool);
    let tool_entry = registry.get_tool_by_name("test.test_tool").unwrap();

    let args = json!({
        "anything": "goes",
        "count": 123
    });

    assert!(tool_entry.validate_against_input_schema(&args).is_ok());
}

#[test]
fn test_output_schema_validation_passes_with_valid_response() {
    let output_schema = serde_json::from_value(json!({
        "type": "object",
        "properties": {
            "temperature": { "type": "number" },
            "unit": { "type": "string" }
        },
        "required": ["temperature"]
    }))
    .unwrap();

    let tool = create_test_tool_with_schemas(serde_json::Map::new(), output_schema);
    let registry = registry_with_tool(tool);
    let tool_entry = registry.get_tool_by_name("test.test_tool").unwrap();

    let response = json!({
        "temperature": 72.5,
        "unit": "F"
    });

    assert!(tool_entry.validate_against_output_schema(&response).is_ok());
}

#[test]
fn test_output_schema_validation_fails_with_invalid_response() {
    let output_schema = serde_json::from_value(json!({
        "type": "object",
        "properties": {
            "temperature": { "type": "number" }
        },
        "required": ["temperature"]
    }))
    .unwrap();

    let tool = create_test_tool_with_schemas(serde_json::Map::new(), output_schema);
    let registry = registry_with_tool(tool);
    let tool_entry = registry.get_tool_by_name("test.test_tool").unwrap();

    let response = json!({
        "temperature": "hot" // Should be a number
    });

    let result = tool_entry.validate_against_output_schema(&response);
    assert!(result.is_err());
}

#[test]
fn test_output_schema_validation_skips_when_empty() {
    let tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    let registry = registry_with_tool(tool);
    let tool_entry = registry.get_tool_by_name("test.test_tool").unwrap();

    let response = json!({
        "arbitrary": "data",
        "nested": {
            "value": 123
        }
    });

    assert!(tool_entry.validate_against_output_schema(&response).is_ok());
}

#[test]
fn test_with_config_fails_with_invalid_input_schema() {
    let input_schema = serde_json::from_value(json!({ "type": "invalid_type" })).unwrap();
    let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
    let result =
        ToolsRegistry::with_config(vec![tool], Vec::new(), McpToolSearch::default(), UpstreamLimits::default(), None);
    assert!(matches!(result, Err(ToolBuilderError::InvalidInputSchema(_))));
}

#[test]
fn test_with_config_fails_with_invalid_output_schema() {
    let output_schema = serde_json::from_value(json!({ "type": "invalid_type" })).unwrap();
    let tool = create_test_tool_with_schemas(serde_json::Map::new(), output_schema);
    let result =
        ToolsRegistry::with_config(vec![tool], Vec::new(), McpToolSearch::default(), UpstreamLimits::default(), None);
    assert!(matches!(result, Err(ToolBuilderError::InvalidOutputSchema(_))));
}

#[test]
fn test_nested_object_validation() {
    let input_schema = serde_json::from_value(json!({
        "type": "object",
        "properties": {
            "user": {
                "type": "object",
                "properties": {
                    "name": { "type": "string" },
                    "age": { "type": "integer" }
                },
                "required": ["name"]
            }
        }
    }))
    .unwrap();

    let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
    let registry = registry_with_tool(tool);
    let tool_entry = registry.get_tool_by_name("test.test_tool").unwrap();

    let valid_args = json!({
        "user": {
            "name": "Alice",
            "age": 30
        }
    });
    tool_entry.validate_against_input_schema(&valid_args).unwrap();

    let invalid_args = json!({
        "user": {
            "age": 30
        }
    });
    assert!(tool_entry.validate_against_input_schema(&invalid_args).is_err());
}

#[test]
fn test_array_validation() {
    let input_schema = serde_json::from_value(json!({
        "type": "object",
        "properties": {
            "tags": {
                "type": "array",
                "items": { "type": "string" }
            }
        }
    }))
    .unwrap();

    let tool = create_test_tool_with_schemas(input_schema, serde_json::Map::new());
    let registry = registry_with_tool(tool);
    let tool_entry = registry.get_tool_by_name("test.test_tool").unwrap();

    let valid_args = json!({ "tags": ["rust", "mcp", "api"] });
    assert!(tool_entry.validate_against_input_schema(&valid_args).is_ok());

    let invalid_args = json!({ "tags": [1, 2, 3] });
    assert!(tool_entry.validate_against_input_schema(&invalid_args).is_err());
}

#[tokio::test]
async fn test_add_and_remove_provided_tool() {
    let registry =
        ToolsRegistry::with_config(Vec::new(), Vec::new(), McpToolSearch::default(), UpstreamLimits::default(), None)
            .unwrap();
    let tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());

    registry.add_tool(tool).await.unwrap();
    assert!(registry.get_tool_by_name("test.test_tool").is_some());

    assert!(registry.remove_tool(&ToolId::new("test", "test_tool").unwrap()));
    assert!(registry.get_tool_by_name("test.test_tool").is_none());
    assert!(!registry.remove_tool(&ToolId::new("test", "test_tool").unwrap()));
}

#[tokio::test]
async fn tds_added_tools_inherit_registry_limits() {
    let limits = UpstreamLimits { timeout: Duration::from_secs(8), max_response_bytes: 321 };
    let registry =
        ToolsRegistry::with_config(Vec::new(), Vec::new(), McpToolSearch::default(), limits.clone(), None).unwrap();
    let mut tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool.backend = UpstreamBackend::McpServer {
        transport: McpBackendTransportUpstream::StreamableHttp,
        url: "http://localhost/mcp".into(),
    };

    registry.add_tool(tool).await.unwrap();
    let entry = registry.get_tool_by_name("test.test_tool").unwrap();
    let ToolBackend::McpServer(backend) = &entry.backend else {
        panic!("expected compiled MCP backend");
    };
    assert_eq!(backend.timeout, limits.timeout);
    assert_eq!(backend.max_response_bytes, limits.max_response_bytes);
}

#[test]
fn test_duplicate_provided_tool_names_rejected() {
    let tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    let tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    let result = ToolsRegistry::with_config(
        vec![tool_a, tool_b],
        Vec::new(),
        McpToolSearch::default(),
        UpstreamLimits::default(),
        None,
    );
    assert!(matches!(result, Err(ToolBuilderError::DuplicateTool(_))));
}

#[test]
fn dynamic_materialization_preserves_output_schema() {
    let server = dynamic_server_entry("upstream");
    let input_schema = StdArc::new(serde_json::Map::new());
    let mut output_schema = serde_json::Map::new();
    output_schema.insert("type".to_owned(), Value::String("object".to_owned()));
    let mut tool = Tool::new("with_output", "Output", input_schema);
    tool.output_schema = Some(StdArc::new(output_schema.clone()));
    let registry =
        ToolsRegistry::with_config(Vec::new(), Vec::new(), McpToolSearch::default(), UpstreamLimits::default(), None)
            .unwrap();
    let materialized = registry.materialise_dynamic_tools(&server, vec![tool]).unwrap();
    assert_eq!(materialized.len(), 1);
    assert_eq!(materialized[0].output_schema, output_schema);
}

#[test]
fn dynamic_tools_inherit_registry_limits_and_original_name() {
    let limits = UpstreamLimits { timeout: Duration::from_secs(11), max_response_bytes: 654 };
    let registry =
        ToolsRegistry::with_config(Vec::new(), Vec::new(), McpToolSearch::default(), limits.clone(), None).unwrap();
    let server = dynamic_server_entry("upstream");
    let tool = Tool::new("original_name", "Output", StdArc::new(serde_json::Map::new()));

    let materialized = registry.materialise_dynamic_tools(&server, vec![tool]).unwrap();
    let ToolBackend::McpServer(backend) = &materialized[0].backend else {
        panic!("expected compiled MCP backend");
    };
    assert_eq!(backend.upstream_tool_name, "original_name");
    assert_eq!(backend.timeout, limits.timeout);
    assert_eq!(backend.max_response_bytes, limits.max_response_bytes);
}

#[tokio::test]
async fn test_remove_dynamic_server_evicts_its_tools() {
    let registry =
        ToolsRegistry::with_config(Vec::new(), Vec::new(), McpToolSearch::default(), UpstreamLimits::default(), None)
            .unwrap();

    // Pretend a server previously materialised two tools
    let server_conf = DynamicMcpServer {
        namespace: "srv".into(),
        description: "".into(),
        transport: McpBackendTransportUpstream::StreamableHttp,
        url: "http://localhost/mcp".into(),
        cache_duration: None,
        rbac: None,
        tool_disclosure_mode: DisclosureMode::Progressive,
    };
    let server = StdArc::new(DynamicMcpServerEntry::new(server_conf));
    registry.dynamic_mcp_servers.pin().insert("srv".into(), StdArc::clone(&server));

    for upstream in ["alpha", "beta"] {
        let conf = McpTool {
            namespace: "srv".into(),
            name: upstream.into(),
            description: "".into(),
            input_schema: serde_json::Map::new(),
            output_schema: serde_json::Map::new(),
            backend: UpstreamBackend::McpServer {
                transport: McpBackendTransportUpstream::StreamableHttp,
                url: "http://localhost/mcp".into(),
            },
            rbac: None,
            embedding: EmbeddingVector::default(),
            disclosure_mode: DisclosureMode::Progressive,
        };
        let entry = build_tool_entry(
            conf,
            ToolSource::Dynamic { upstream_tool_name: upstream.into() },
            &UpstreamLimits::default(),
        )
        .unwrap();
        registry.tools.pin().insert(entry.id.clone(), StdArc::new(entry));
    }

    // And a provided tool that must be left alone
    let provided = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    registry.add_tool(provided).await.unwrap();

    assert!(registry.remove_dynamic_server("srv"));
    assert!(registry.get_tool_by_name("srv.alpha").is_none());
    assert!(registry.get_tool_by_name("srv.beta").is_none());
    assert!(registry.get_tool_by_name("test.test_tool").is_some());
}

#[tokio::test]
async fn tool_search_ranks_with_cosine() {
    let client = embeddings::EmbeddingsClient::test_one_hot();
    let search = McpToolSearch { embeddings: None, max_allowable_results: 2 };

    let mut tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_a.name = "weather_get_forecast".into();
    tool_a.description = "Get the weather forecast".into();
    let mut tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_b.name = "user_profile_get".into();
    tool_b.description = "Get user profile".into();
    let mut tool_c = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_c.name = "admin_delete".into();
    tool_c.description = "Admin delete operation".into();

    let registry = ToolsRegistry::with_config(
        vec![tool_a, tool_b, tool_c],
        Vec::new(),
        search,
        UpstreamLimits::default(),
        Some(StdArc::clone(&client)),
    )
    .unwrap();

    let req_ctx = RequestCtx::default();
    registry.ensure_tools_current(&req_ctx).await.unwrap();

    for name in ["test.weather_get_forecast", "test.user_profile_get", "test.admin_delete"] {
        let entry = registry.get_tool_by_name(name).unwrap();
        assert!(entry.embedding.load_full().is_some(), "tool '{name}' was not embedded");
    }

    let req_ext = http::Extensions::new();
    let ranked = registry
        .search_tools(
            &req_ext,
            &req_ctx,
            ToolSearchRequest { query: "what is the weather today".to_owned(), limit: None, namespace: None },
            ToolSearchScope::Progressive,
        )
        .await
        .expect("semantic search ranking should succeed");
    assert_eq!(ranked.len(), 2);
    assert_eq!(ranked[0].name.as_ref(), "test.weather_get_forecast", "weather tool should rank first");
}

#[tokio::test]
async fn tool_search_ranks_with_bm25_without_remote_embeddings() {
    let search = McpToolSearch { embeddings: None, max_allowable_results: 1 };

    let mut tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_a.name = "weather_get_forecast".into();
    tool_a.description = "Get the weather forecast".into();
    let mut tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_b.name = "user_profile_get".into();
    tool_b.description = "Get user profile".into();

    let registry =
        ToolsRegistry::with_config(vec![tool_a, tool_b], Vec::new(), search, UpstreamLimits::default(), None).unwrap();

    let req_ext = http::Extensions::new();
    let req_ctx = RequestCtx::default();
    let ranked = registry
        .search_tools(
            &req_ext,
            &req_ctx,
            ToolSearchRequest { query: "weather forecast".to_owned(), limit: None, namespace: None },
            ToolSearchScope::Progressive,
        )
        .await
        .expect("BM25 ranking should succeed");
    assert_eq!(ranked.len(), 1);
    assert_eq!(ranked[0].name.as_ref(), "test.weather_get_forecast", "BM25 should rank weather first");
}

#[tokio::test]
async fn add_tool_replacement_refreshes_bm25_ranking() {
    let search = McpToolSearch { embeddings: None, max_allowable_results: 1 };

    let mut tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_a.name = "alpha".into();
    tool_a.description = "Weather forecasts and temperature reports".into();
    let mut tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_b.name = "beta".into();
    tool_b.description = "Billing invoices and payment records".into();

    let registry =
        ToolsRegistry::with_config(vec![tool_a], Vec::new(), search, UpstreamLimits::default(), None).unwrap();
    registry.add_tool(tool_b).await.unwrap();

    let req_ext = http::Extensions::new();
    let req_ctx = RequestCtx::default();
    let ranked = registry
        .search_tools(
            &req_ext,
            &req_ctx,
            ToolSearchRequest { query: "billing invoices".to_owned(), limit: None, namespace: None },
            ToolSearchScope::Progressive,
        )
        .await
        .expect("BM25 ranking should succeed");
    assert_eq!(ranked[0].name.as_ref(), "test.beta");

    let mut replacement = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    replacement.name = "alpha".into();
    replacement.description = "Billing billing billing invoices invoices ledger reconciliation".into();
    registry.add_tool(replacement).await.unwrap();

    let ranked = registry
        .search_tools(
            &req_ext,
            &req_ctx,
            ToolSearchRequest { query: "billing invoices".to_owned(), limit: None, namespace: None },
            ToolSearchScope::Progressive,
        )
        .await
        .expect("BM25 ranking should succeed after replacement");
    assert_eq!(ranked[0].name.as_ref(), "test.alpha");
}

#[tokio::test]
async fn remote_embedding_failure_degrades_to_bm25() {
    use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
        McpToolSearch, RemoteEmbeddings,
    };

    let client = embeddings::EmbeddingsClient::test_failing();
    let search = McpToolSearch {
        embeddings: Some(RemoteEmbeddings {
            cluster: "embeddings".into(),
            model_id: "test-model".into(),
            path: RemoteEmbeddings::default_path(),
            timeout: None,
            dimensions: 3,
            max_batch_size: None,
        }),
        max_allowable_results: 1,
    };

    let mut tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_a.name = "weather_get_forecast".into();
    tool_a.description = "Get the weather forecast".into();
    let mut tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_b.name = "billing_lookup".into();
    tool_b.description = "Find billing invoices and payment records".into();

    let registry =
        ToolsRegistry::with_config(vec![tool_a, tool_b], Vec::new(), search, UpstreamLimits::default(), Some(client))
            .unwrap();

    let req_ext = http::Extensions::new();
    let req_ctx = RequestCtx::default();
    let ranked = registry
        .search_tools(
            &req_ext,
            &req_ctx,
            ToolSearchRequest { query: "billing invoices".to_owned(), limit: None, namespace: None },
            ToolSearchScope::Progressive,
        )
        .await
        .expect("BM25 fallback should rank candidates when embeddings fail");
    assert_eq!(ranked[0].name.as_ref(), "test.billing_lookup");
}

#[tokio::test]
async fn add_tool_inserts_even_when_embeddings_client_fails() {
    let client = embeddings::EmbeddingsClient::test_failing();
    let search = McpToolSearch::default();
    let registry =
        ToolsRegistry::with_config(Vec::new(), Vec::new(), search, UpstreamLimits::default(), Some(client)).unwrap();
    let mut tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool.name = "tds_tool".into();

    let res = registry.add_tool(tool).await;
    assert!(res.is_ok(), "embeddings failure should not block insertion: {res:?}");
    assert!(registry.get_tool_by_name("test.tds_tool").is_some());
    assert!(registry.get_tool_by_name("test.tds_tool").unwrap().embedding.load_full().is_none());
}

#[tokio::test]
async fn refresh_embeds_unembedded_tools_in_both_disclosure_modes() {
    use arion_configuration::config::network_filters::http_connection_manager::http_filters::mcp_gateway::{
        McpToolSearch, RemoteEmbeddings,
    };

    let client = embeddings::EmbeddingsClient::test_one_hot();
    let search = McpToolSearch {
        embeddings: Some(RemoteEmbeddings {
            cluster: "embeddings".into(),
            model_id: "test-model".into(),
            path: RemoteEmbeddings::default_path(),
            timeout: None,
            dimensions: 3,
            max_batch_size: None,
        }),
        max_allowable_results: 5,
    };

    let mut tool_a = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_a.name = "weather_get_forecast".into();
    tool_a.description = "Get the weather forecast".into();
    tool_a.embedding = EmbeddingVector(vec![1.0, 0.0, 0.0]);

    let mut tool_b = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool_b.name = "billing_lookup".into();
    tool_b.description = "Find billing invoices".into();
    tool_b.disclosure_mode = DisclosureMode::Eager;

    let registry =
        ToolsRegistry::with_config(vec![tool_a, tool_b], Vec::new(), search, UpstreamLimits::default(), Some(client))
            .unwrap();

    assert!(registry.get_tool_by_name("test.weather_get_forecast").unwrap().embedding.load_full().is_some());
    assert!(registry.get_tool_by_name("test.billing_lookup").unwrap().embedding.load_full().is_none());

    registry.ensure_tools_current(&RequestCtx::default()).await.expect("refresh should succeed");
    assert!(registry.get_tool_by_name("test.weather_get_forecast").unwrap().embedding.load_full().is_some());
    assert!(registry.get_tool_by_name("test.billing_lookup").unwrap().embedding.load_full().is_some());
}

fn numbered_tools(count: usize) -> Vec<McpTool> {
    (0..count)
        .map(|index| {
            let mut tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
            tool.name = format!("tool_{index:02}").into();
            tool
        })
        .collect()
}

fn embedded_count(registry: &ToolsRegistry, count: usize) -> usize {
    (0..count)
        .filter(|index| {
            registry.get_tool_by_name(&format!("test.tool_{index:02}")).unwrap().embedding.load_full().is_some()
        })
        .count()
}

#[tokio::test]
async fn embedding_chunks_are_stored_incrementally_across_a_failing_chunk() {
    let client = embeddings::EmbeddingsClient::test_flaky();
    let registry = ToolsRegistry::with_config(
        numbered_tools(65),
        Vec::new(),
        McpToolSearch::default(),
        UpstreamLimits::default(),
        Some(client),
    )
    .unwrap();

    // First refresh: the 64-input chunk fails, the trailing chunk succeeds.
    registry.ensure_tools_current(&RequestCtx::default()).await.unwrap();
    assert_eq!(embedded_count(&registry, 65), 1, "a failing chunk must not discard the successful chunk's vectors");

    // The retry embeds only the tools the failed chunk left behind.
    registry.ensure_tools_current(&RequestCtx::default()).await.unwrap();
    assert_eq!(embedded_count(&registry, 65), 65);
}

#[tokio::test]
async fn catalog_embedding_requests_are_chunked_to_the_batch_limit() {
    let client = embeddings::EmbeddingsClient::test_counting();
    let registry = ToolsRegistry::with_config(
        numbered_tools(65),
        Vec::new(),
        McpToolSearch::default(),
        UpstreamLimits::default(),
        Some(StdArc::clone(&client)),
    )
    .unwrap();

    registry.ensure_tools_current(&RequestCtx::default()).await.unwrap();
    assert_eq!(client.recorded_batch_sizes(), [64, 1]);
    assert_eq!(embedded_count(&registry, 65), 65);

    // Everything is embedded, so another refresh sends no requests.
    registry.ensure_tools_current(&RequestCtx::default()).await.unwrap();
    assert_eq!(client.recorded_batch_sizes(), [64, 1]);
}

#[tokio::test]
async fn embedding_refresh_retries_after_failure_without_latching() {
    let client = embeddings::EmbeddingsClient::test_flaky();
    let search = McpToolSearch::default();
    let mut tool = create_test_tool_with_schemas(serde_json::Map::new(), serde_json::Map::new());
    tool.name = "flaky_tool".into();

    let registry = ToolsRegistry::with_config(
        vec![tool],
        Vec::new(),
        search,
        UpstreamLimits::default(),
        Some(StdArc::clone(&client)),
    )
    .unwrap();

    // First call: embeddings fail, bootstrap still succeeds because BM25 can
    // serve as the fallback ranking path.
    let req_ctx = RequestCtx::default();
    registry.ensure_tools_current(&req_ctx).await.expect("embeddings failure should not fail refresh");
    assert!(
        registry.get_tool_by_name("test.flaky_tool").unwrap().embedding.load_full().is_none(),
        "tool must remain unembedded after a failed refresh"
    );

    // Second call: remote embeddings recover, bootstrap succeeds and stores the embedding.
    registry.ensure_tools_current(&req_ctx).await.expect("retry must succeed once embeddings recover");
    assert!(
        registry.get_tool_by_name("test.flaky_tool").unwrap().embedding.load_full().is_some(),
        "embedding should be stored after a successful retry"
    );
}
