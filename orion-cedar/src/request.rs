use cedar_policy::{Context, EntityUid};
use serde_json::{Map, Value};
use smol_str::SmolStr;

use crate::entity::{build_context, entity_uid};
use crate::error::Error;

/// Extract a Cedar principal [`EntityUid`] from JWT claims.
///
/// Uses the `sub` claim as the entity ID. Returns [`Error::Entity`] if `sub`
/// is absent or not a string.
pub fn principal_from_jwt(claims: &Value, entity_type: &str) -> Result<EntityUid, Error> {
    let sub = claims
        .get("sub")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Entity(SmolStr::from("JWT claims missing string 'sub' field")))?;
    entity_uid(entity_type, sub)
}

/// Build a Cedar [`Context`] from per-request Orion data.
///
/// The resulting record contains up to three namespaces — each is omitted
/// entirely when all its inputs are `None`:
///
/// - `jwt`:  the JWT claims object passed verbatim
/// - `http`: `{ method?, path?, query? }` HTTP request metadata
/// - `tool`: `{ name?, arguments_json? }` MCP tool call data
///
/// Tool arguments are JSON-serialised to a `String` (`arguments_json`) so
/// that Cedar can declare them as a typed scalar in the schema rather than
/// requiring a fully-typed record for arbitrary argument shapes.
///
/// The Cedar schema must declare exactly the context fields that policies
/// reference. Only pass the namespaces relevant to the filter:
/// - MCP tool filter: `jwt_claims` + `tool_name`/`tool_args`
/// - HTTP RBAC filter: `jwt_claims` + `http_method`/`http_path`/`http_query`
#[allow(clippy::too_many_arguments)]
pub fn build_authz_context(
    jwt_claims: Option<&Value>,
    http_method: Option<&str>,
    http_path: Option<&str>,
    http_query: Option<&str>,
    tool_name: Option<&str>,
    tool_args: Option<&Value>,
) -> Result<Context, Error> {
    let mut ctx = Map::new();

    if let Some(claims) = jwt_claims {
        ctx.insert("jwt".to_owned(), claims.clone());
    }

    if http_method.is_some() || http_path.is_some() || http_query.is_some() {
        let mut http = Map::new();
        if let Some(m) = http_method {
            http.insert("method".to_owned(), Value::String(m.to_owned()));
        }
        if let Some(p) = http_path {
            http.insert("path".to_owned(), Value::String(p.to_owned()));
        }
        if let Some(q) = http_query {
            http.insert("query".to_owned(), Value::String(q.to_owned()));
        }
        ctx.insert("http".to_owned(), Value::Object(http));
    }

    if tool_name.is_some() || tool_args.is_some() {
        let mut tool = Map::new();
        if let Some(n) = tool_name {
            tool.insert("name".to_owned(), Value::String(n.to_owned()));
        }
        if let Some(a) = tool_args {
            let json_str = serde_json::to_string(a)
                .map_err(|e| Error::Context(SmolStr::from(format!("failed to serialize tool arguments: {e}"))))?;
            tool.insert("arguments_json".to_owned(), Value::String(json_str));
        }
        ctx.insert("tool".to_owned(), Value::Object(tool));
    }

    build_context(&Value::Object(ctx))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::{AuthzRequest, PolicyStore};
    use serde_json::json;

    // --- principal_from_jwt ---

    #[test]
    fn principal_from_jwt_extracts_sub() {
        let claims = json!({ "sub": "svc-alice", "iss": "https://auth.example.com" });
        let uid = principal_from_jwt(&claims, "User").unwrap();
        assert_eq!(uid.to_string(), r#"User::"svc-alice""#);
    }

    #[test]
    fn principal_from_jwt_with_namespace() {
        let claims = json!({ "sub": "urn:example:svc-alice" });
        let uid = principal_from_jwt(&claims, "AgentIdentity::IamEntity").unwrap();
        assert_eq!(uid.to_string(), r#"AgentIdentity::IamEntity::"urn:example:svc-alice""#);
    }

    #[test]
    fn principal_from_jwt_missing_sub_errors() {
        let claims = json!({ "iss": "https://auth.example.com" });
        assert!(principal_from_jwt(&claims, "User").is_err());
    }

    #[test]
    fn principal_from_jwt_non_string_sub_errors() {
        let claims = json!({ "sub": 42 });
        assert!(principal_from_jwt(&claims, "User").is_err());
    }

    // --- build_authz_context ---

    #[test]
    fn context_all_none_is_empty_record() {
        assert!(build_authz_context(None, None, None, None, None, None).is_ok());
    }

    #[test]
    fn context_jwt_only() {
        let claims = json!({ "sub": "svc-alice", "iss": "https://auth.example.com" });
        assert!(build_authz_context(Some(&claims), None, None, None, None, None).is_ok());
    }

    #[test]
    fn context_http_only() {
        assert!(build_authz_context(None, Some("POST"), Some("/mcp"), Some("sessionId=abc"), None, None).is_ok());
    }

    #[test]
    fn context_jwt_and_http() {
        let claims = json!({ "sub": "svc-alice" });
        assert!(build_authz_context(Some(&claims), Some("POST"), Some("/mcp"), None, None, None).is_ok());
    }

    #[test]
    fn context_tool_args_serialised_to_string() {
        let args = json!({ "url": "https://example.com" });
        assert!(build_authz_context(None, None, None, None, Some("fetch"), Some(&args)).is_ok());
    }

    // --- integration: full AuthzRequest → PolicyStore round-trips ---

    // Identity-based: policy permits when context.jwt.sub matches.
    const JWT_SCHEMA: &str = r#"
        entity User;
        entity Document;
        action "read" appliesTo {
            principal: [User],
            resource: [Document],
            context: { jwt: { sub: String } }
        };
    "#;

    const JWT_POLICY: &str = r#"
        permit (
            principal,
            action == Action::"read",
            resource == Document::"doc-1"
        ) when { context.jwt.sub == "svc-alice" };
    "#;

    #[test]
    fn jwt_context_permits_matching_sub() {
        let store = PolicyStore::new(JWT_POLICY, JWT_SCHEMA, "").unwrap();
        let claims = json!({ "sub": "svc-alice" });
        let response = store
            .is_authorized(AuthzRequest {
                principal: principal_from_jwt(&claims, "User").unwrap(),
                action: entity_uid("Action", "read").unwrap(),
                resource: entity_uid("Document", "doc-1").unwrap(),
                context: build_authz_context(Some(&claims), None, None, None, None, None).unwrap(),
            })
            .unwrap();
        assert!(response.is_allowed());
    }

    #[test]
    fn jwt_context_denies_wrong_sub() {
        let store = PolicyStore::new(JWT_POLICY, JWT_SCHEMA, "").unwrap();
        let claims = json!({ "sub": "svc-bob" });
        let response = store
            .is_authorized(AuthzRequest {
                principal: principal_from_jwt(&claims, "User").unwrap(),
                action: entity_uid("Action", "read").unwrap(),
                resource: entity_uid("Document", "doc-1").unwrap(),
                context: build_authz_context(Some(&claims), None, None, None, None, None).unwrap(),
            })
            .unwrap();
        assert!(!response.is_allowed());
    }

    // Route-based: policy permits when context.http.method and path match.
    const HTTP_SCHEMA: &str = r#"
        entity User;
        entity Api;
        action "call" appliesTo {
            principal: [User],
            resource: [Api],
            context: { http: { method: String, path: String } }
        };
    "#;

    const HTTP_POLICY: &str = r#"
        permit (
            principal == User::"alice",
            action == Action::"call",
            resource == Api::"gateway"
        ) when { context.http.method == "POST" && context.http.path == "/mcp" };
    "#;

    #[test]
    fn http_context_permits_matching_route() {
        let store = PolicyStore::new(HTTP_POLICY, HTTP_SCHEMA, "").unwrap();
        let response = store
            .is_authorized(AuthzRequest {
                principal: entity_uid("User", "alice").unwrap(),
                action: entity_uid("Action", "call").unwrap(),
                resource: entity_uid("Api", "gateway").unwrap(),
                context: build_authz_context(None, Some("POST"), Some("/mcp"), None, None, None).unwrap(),
            })
            .unwrap();
        assert!(response.is_allowed());
    }

    #[test]
    fn http_context_denies_wrong_method() {
        let store = PolicyStore::new(HTTP_POLICY, HTTP_SCHEMA, "").unwrap();
        let response = store
            .is_authorized(AuthzRequest {
                principal: entity_uid("User", "alice").unwrap(),
                action: entity_uid("Action", "call").unwrap(),
                resource: entity_uid("Api", "gateway").unwrap(),
                context: build_authz_context(None, Some("GET"), Some("/mcp"), None, None, None).unwrap(),
            })
            .unwrap();
        assert!(!response.is_allowed());
    }

    // Tool-based: policy permits when context.tool.name matches.
    const TOOL_SCHEMA: &str = r#"
        entity ServiceAccount;
        entity McpServer;
        action "invoke" appliesTo {
            principal: [ServiceAccount],
            resource: [McpServer],
            context: { tool: { name: String } }
        };
    "#;

    const TOOL_POLICY: &str = r#"
        permit (
            principal == ServiceAccount::"backend",
            action == Action::"invoke",
            resource == McpServer::"gateway"
        ) when { context.tool.name == "fetch" };
    "#;

    #[test]
    fn tool_context_permits_allowed_tool() {
        let store = PolicyStore::new(TOOL_POLICY, TOOL_SCHEMA, "").unwrap();
        let response = store
            .is_authorized(AuthzRequest {
                principal: entity_uid("ServiceAccount", "backend").unwrap(),
                action: entity_uid("Action", "invoke").unwrap(),
                resource: entity_uid("McpServer", "gateway").unwrap(),
                context: build_authz_context(None, None, None, None, Some("fetch"), None).unwrap(),
            })
            .unwrap();
        assert!(response.is_allowed());
    }

    #[test]
    fn tool_context_denies_blocked_tool() {
        let store = PolicyStore::new(TOOL_POLICY, TOOL_SCHEMA, "").unwrap();
        let response = store
            .is_authorized(AuthzRequest {
                principal: entity_uid("ServiceAccount", "backend").unwrap(),
                action: entity_uid("Action", "invoke").unwrap(),
                resource: entity_uid("McpServer", "gateway").unwrap(),
                context: build_authz_context(None, None, None, None, Some("execute_code"), None).unwrap(),
            })
            .unwrap();
        assert!(!response.is_allowed());
    }
}
