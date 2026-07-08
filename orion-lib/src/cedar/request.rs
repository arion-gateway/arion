use cedar_policy::{Context, EntityId, EntityTypeName, EntityUid, RestrictedExpression};
use serde_json::Value;
use smol_str::SmolStr;
use std::str::FromStr;

use super::error::Error;

pub(crate) fn entity_uid(type_name: &str, id: &str) -> Result<EntityUid, Error> {
    let type_name = EntityTypeName::from_str(type_name)
        .map_err(|e| Error::Entity(SmolStr::from(format!("invalid entity type '{type_name}': {e}"))))?;
    let id =
        EntityId::from_str(id).map_err(|e| Error::Entity(SmolStr::from(format!("invalid entity id '{id}': {e}"))))?;
    Ok(EntityUid::from_type_name_and_id(type_name, id))
}

pub fn principal_from_jwt(claims: &Value, entity_type: &str) -> Result<EntityUid, Error> {
    let sub = claims
        .get("sub")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::Entity(SmolStr::from("JWT claims missing string 'sub' field")))?;
    entity_uid(entity_type, sub)
}

fn json_value_to_restricted_expr(value: &Value) -> Result<RestrictedExpression, Error> {
    match value {
        Value::String(s) => Ok(RestrictedExpression::new_string(s.clone())),
        Value::Number(n) => n
            .as_i64()
            .map(RestrictedExpression::new_long)
            .ok_or_else(|| Error::Context(SmolStr::from(format!("unsupported number in Cedar context: {n}")))),
        Value::Bool(b) => Ok(RestrictedExpression::new_bool(*b)),
        Value::Array(arr) => {
            let items: Result<Vec<_>, _> = arr.iter().map(json_value_to_restricted_expr).collect();
            Ok(RestrictedExpression::new_set(items?))
        },
        Value::Object(map) => {
            let fields: Result<Vec<(String, RestrictedExpression)>, _> =
                map.iter().map(|(k, v)| json_value_to_restricted_expr(v).map(|e| (k.clone(), e))).collect();
            RestrictedExpression::new_record(fields?).map_err(|e| Error::Context(SmolStr::from(e.to_string())))
        },
        Value::Null => Err(Error::Context(SmolStr::new_static("null values are not supported in Cedar context"))),
    }
}

pub fn build_authz_context(
    jwt_claims: Option<&Value>,
    http_method: Option<&str>,
    http_path: Option<&str>,
    http_query: Option<&str>,
) -> Result<Context, Error> {
    let mut pairs: Vec<(String, RestrictedExpression)> = Vec::new();

    if let Some(claims) = jwt_claims {
        pairs.push(("jwt".to_string(), json_value_to_restricted_expr(claims)?));
    }

    if http_method.is_some() || http_path.is_some() || http_query.is_some() {
        let mut http_fields: Vec<(String, RestrictedExpression)> = Vec::new();
        if let Some(m) = http_method {
            http_fields.push(("method".to_string(), RestrictedExpression::new_string(m.to_string())));
        }
        if let Some(p) = http_path {
            http_fields.push(("path".to_string(), RestrictedExpression::new_string(p.to_string())));
        }
        if let Some(q) = http_query {
            http_fields.push(("query".to_string(), RestrictedExpression::new_string(q.to_string())));
        }
        let http_record =
            RestrictedExpression::new_record(http_fields).map_err(|e| Error::Context(SmolStr::from(e.to_string())))?;
        pairs.push(("http".to_string(), http_record));
    }

    Context::from_pairs(pairs).map_err(|e| Error::Context(SmolStr::from(e.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cedar::store::{AuthzRequest, PolicyStore};
    use serde_json::json;

    #[test]
    fn entity_uid_valid() {
        let uid = entity_uid("User", "alice").unwrap();
        assert_eq!(uid.to_string(), "User::\"alice\"");
    }

    #[test]
    fn entity_uid_with_namespace() {
        let uid = entity_uid("AgentIdentity::IamEntity", "urn:example").unwrap();
        assert_eq!(uid.to_string(), "AgentIdentity::IamEntity::\"urn:example\"");
    }

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
        principal_from_jwt(&claims, "User").unwrap_err();
    }

    #[test]
    fn principal_from_jwt_non_string_sub_errors() {
        let claims = json!({ "sub": 42 });
        principal_from_jwt(&claims, "User").unwrap_err();
    }

    #[test]
    fn context_all_none_is_empty_record() {
        build_authz_context(None, None, None, None).unwrap();
    }

    #[test]
    fn context_jwt_only() {
        let claims = json!({ "sub": "svc-alice", "iss": "https://auth.example.com" });
        build_authz_context(Some(&claims), None, None, None).unwrap();
    }

    #[test]
    fn context_http_only() {
        build_authz_context(None, Some("POST"), Some("/mcp"), Some("sessionId=abc")).unwrap();
    }

    #[test]
    fn context_jwt_and_http() {
        let claims = json!({ "sub": "svc-alice" });
        build_authz_context(Some(&claims), Some("POST"), Some("/mcp"), None).unwrap();
    }

    #[test]
    fn jwt_complex_types_deny_is_ok_not_err() {
        const SCHEMA: &str = r#"
            entity User;
            entity HttpPath;
            action "GET" appliesTo {
                principal: [User],
                resource: [HttpPath],
                context: {
                    jwt: { sub?: String, iss?: String, aud?: Set<String>, exp?: Long, iat?: Long },
                    http: { method: String, path: String, query?: String }
                }
            };
            action "POST" appliesTo {
                principal: [User],
                resource: [HttpPath],
                context: {
                    jwt: { sub?: String, iss?: String, aud?: Set<String>, exp?: Long, iat?: Long },
                    http: { method: String, path: String, query?: String }
                }
            };
        "#;
        const POLICY: &str = r#"
            permit(principal == User::"svc-frontend", action == Action::"GET", resource);
            permit(principal == User::"svc-admin",    action,                  resource);
        "#;
        let store = PolicyStore::new(POLICY, SCHEMA, "").unwrap();
        let claims = json!({
            "sub": "svc-frontend",
            "iss": "test-issuer",
            "aud": ["mcp-gateway"],
            "exp": 9_999_999_999u64,
            "iat": 0u64
        });
        let result = store.is_authorized(AuthzRequest {
            principal: entity_uid("User", "svc-frontend").unwrap(),
            action: entity_uid("Action", "POST").unwrap(),
            resource: entity_uid("HttpPath", "/api").unwrap(),
            context: build_authz_context(Some(&claims), Some("POST"), Some("/api"), None).unwrap(),
        });
        assert!(result.is_ok(), "Cedar should return Ok(Deny), not Err: {:?}", result.err());
        assert!(!result.unwrap().is_allowed(), "svc-frontend POST should be denied");
    }

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
                context: build_authz_context(Some(&claims), None, None, None).unwrap(),
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
                context: build_authz_context(Some(&claims), None, None, None).unwrap(),
            })
            .unwrap();
        assert!(!response.is_allowed());
    }

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
                context: build_authz_context(None, Some("POST"), Some("/mcp"), None).unwrap(),
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
                context: build_authz_context(None, Some("GET"), Some("/mcp"), None).unwrap(),
            })
            .unwrap();
        assert!(!response.is_allowed());
    }
}
