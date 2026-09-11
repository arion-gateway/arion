use cedar_policy::{Context, EntityId, EntityTypeName, EntityUid};
use cedar_policy_core::ast::{Context as CoreContext, RestrictedExpr};
use cedar_policy_core::extensions::Extensions;
use serde_json::Value;
use smallvec::SmallVec;
use smol_str::SmolStr;
use std::str::FromStr;

use super::error::Error;
use crate::listeners::http_connection_manager::jwt_authn::claims::JwtClaims;

#[inline]
pub(crate) fn parse_entity_type(type_name: &str) -> Result<EntityTypeName, Error> {
    EntityTypeName::from_str(type_name).map_err(|e| Error::Entity(format!("invalid entity type '{type_name}': {e}")))
}

pub(crate) fn entity_uid_from_type(entity_type: &EntityTypeName, id: &str) -> EntityUid {
    EntityUid::from_type_name_and_id(entity_type.clone(), EntityId::new(id))
}

#[cfg(test)]
pub(crate) fn entity_uid(type_name: &str, id: &str) -> Result<EntityUid, Error> {
    Ok(entity_uid_from_type(&parse_entity_type(type_name)?, id))
}

#[inline]
pub fn principal_from_jwt(claims: &JwtClaims, entity_type: &EntityTypeName) -> Result<EntityUid, Error> {
    let sub = claims.sub.as_deref().ok_or_else(|| Error::Entity("JWT claims missing string 'sub' field".to_owned()))?;
    Ok(entity_uid_from_type(entity_type, sub))
}

fn json_value_to_restricted_expr(value: &Value) -> Result<RestrictedExpr, Error> {
    match value {
        // `val(&str)` copies once into `SmolStr` (often inline, no heap).
        // The old `cedar-policy` path did `s.clone()` (String) + `SmolStr::new` (second copy).
        Value::String(s) => Ok(RestrictedExpr::val(s.as_str())),
        Value::Number(n) => n
            .as_i64()
            .map(RestrictedExpr::val)
            .ok_or_else(|| Error::Context(format!("unsupported number in Cedar context: {n}"))),
        Value::Bool(b) => Ok(RestrictedExpr::val(*b)),
        Value::Array(arr) => {
            let items: Result<Vec<_>, _> = arr.iter().map(json_value_to_restricted_expr).collect();
            Ok(RestrictedExpr::set(items?))
        },
        Value::Object(map) => {
            let fields: Result<Vec<(SmolStr, RestrictedExpr)>, _> =
                map.iter().map(|(k, v)| json_value_to_restricted_expr(v).map(|e| (SmolStr::new(k), e))).collect();
            RestrictedExpr::record(fields?).map_err(|e| Error::Context(e.to_string()))
        },
        Value::Null => Err(Error::Context("null values are not supported in Cedar context".to_owned())),
    }
}

#[inline]
fn optional_long(key: &'static str, value: Option<u64>) -> Result<Option<(SmolStr, RestrictedExpr)>, Error> {
    value
        .map(|n| {
            i64::try_from(n)
                .map(|n| (SmolStr::new_static(key), RestrictedExpr::val(n)))
                .map_err(|_err| Error::Context(format!("unsupported number in Cedar context: {n}")))
        })
        .transpose()
}

fn jwt_claims_to_restricted_expr(claims: &JwtClaims) -> Result<RestrictedExpr, Error> {
    let mut fields: SmallVec<[(SmolStr, RestrictedExpr); 16]> = SmallVec::with_capacity(7 + claims.extra.len());

    if let Some(sub) = &claims.sub {
        // `From<SmolStr> for Literal` moves the inline/heap string once;
        // the old code did `sub.to_string()` (String alloc) + `SmolStr::new` (second copy).
        fields.push((SmolStr::new_static("sub"), RestrictedExpr::val(sub.clone())));
    }
    if let Some(iss) = &claims.iss {
        fields.push((SmolStr::new_static("iss"), RestrictedExpr::val(iss.clone())));
    }
    if let Some(aud) = &claims.aud {
        fields.push((
            SmolStr::new_static("aud"),
            RestrictedExpr::set(aud.iter().map(|a| RestrictedExpr::val(a.clone()))),
        ));
    }
    if let Some(field) = optional_long("exp", claims.exp)? {
        fields.push(field);
    }
    if let Some(field) = optional_long("iat", claims.iat)? {
        fields.push(field);
    }
    if let Some(field) = optional_long("nbf", claims.nbf)? {
        fields.push(field);
    }
    if let Some(jti) = &claims.jti {
        fields.push((SmolStr::new_static("jti"), RestrictedExpr::val(jti.clone())));
    }
    for (k, v) in &claims.extra {
        fields.push((SmolStr::new(k), json_value_to_restricted_expr(v)?));
    }

    RestrictedExpr::record(fields).map_err(|e| Error::Context(e.to_string()))
}

pub fn build_authz_context(
    jwt_claims: Option<&JwtClaims>,
    http_method: Option<&str>,
    http_path: Option<&str>,
    http_query: Option<&str>,
) -> Result<Context, Error> {
    let http = {
        let mut fields: SmallVec<[(SmolStr, RestrictedExpr); 3]> = SmallVec::new();
        if let Some(m) = http_method {
            fields.push((SmolStr::new_static("method"), RestrictedExpr::val(m)));
        }
        if let Some(p) = http_path {
            fields.push((SmolStr::new_static("path"), RestrictedExpr::val(p)));
        }
        if let Some(q) = http_query {
            fields.push((SmolStr::new_static("query"), RestrictedExpr::val(q)));
        }
        if fields.is_empty() {
            None
        } else {
            Some(RestrictedExpr::record(fields).map_err(|e| Error::Context(e.to_string()))?)
        }
    };

    // `CoreContext::from_pairs` takes `SmolStr` keys directly; the
    // `cedar-policy` wrapper took `String` and did `SmolStr::from` internally
    // (one extra heap alloc per key). `Context::from` below is zero-cost
    // (`repr(transparent)` wrapper over `cedar_policy_core::ast::Context`).
    let core = match (jwt_claims, http) {
        (Some(claims), Some(http_rec)) => CoreContext::from_pairs(
            [
                (SmolStr::new_static("jwt"), jwt_claims_to_restricted_expr(claims)?),
                (SmolStr::new_static("http"), http_rec),
            ],
            Extensions::all_available(),
        ),
        (Some(claims), None) => CoreContext::from_pairs(
            [(SmolStr::new_static("jwt"), jwt_claims_to_restricted_expr(claims)?)],
            Extensions::all_available(),
        ),
        (None, Some(http_rec)) => {
            CoreContext::from_pairs([(SmolStr::new_static("http"), http_rec)], Extensions::all_available())
        },
        (None, None) => return Ok(Context::empty()),
    }
    .map_err(|e| Error::Context(e.to_string()))?;

    Ok(Context::from(core))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cedar::store::{AuthzRequest, PolicyStore};
    use ahash::HashMap;

    fn jwt_claims(sub: Option<&str>) -> JwtClaims {
        JwtClaims {
            sub: sub.map(SmolStr::from),
            iss: None,
            aud: None,
            exp: None,
            iat: None,
            nbf: None,
            jti: None,
            extra: HashMap::default(),
        }
    }

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
        let uid = principal_from_jwt(&jwt_claims(Some("svc-alice")), &parse_entity_type("User").unwrap()).unwrap();
        assert_eq!(uid.to_string(), r#"User::"svc-alice""#);
    }

    #[test]
    fn principal_from_jwt_with_namespace() {
        let uid = principal_from_jwt(
            &jwt_claims(Some("urn:example:svc-alice")),
            &parse_entity_type("AgentIdentity::IamEntity").unwrap(),
        )
        .unwrap();
        assert_eq!(uid.to_string(), r#"AgentIdentity::IamEntity::"urn:example:svc-alice""#);
    }

    #[test]
    fn principal_from_jwt_missing_sub_errors() {
        principal_from_jwt(&jwt_claims(None), &parse_entity_type("User").unwrap()).unwrap_err();
    }

    #[test]
    fn context_all_none_is_empty_record() {
        build_authz_context(None, None, None, None).unwrap();
    }

    #[test]
    fn context_jwt_only() {
        let mut claims = jwt_claims(Some("svc-alice"));
        claims.iss = Some(SmolStr::from("https://auth.example.com"));
        build_authz_context(Some(&claims), None, None, None).unwrap();
    }

    #[test]
    fn context_http_only() {
        build_authz_context(None, Some("POST"), Some("/mcp"), Some("sessionId=abc")).unwrap();
    }

    #[test]
    fn context_jwt_and_http() {
        let claims = jwt_claims(Some("svc-alice"));
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
        let store = PolicyStore::new(POLICY, SCHEMA, "", false).unwrap();
        let claims = JwtClaims {
            sub: Some(SmolStr::from("svc-frontend")),
            iss: Some(SmolStr::from("test-issuer")),
            aud: Some(vec![SmolStr::from("mcp-gateway")]),
            exp: Some(9_999_999_999),
            iat: Some(0),
            nbf: None,
            jti: None,
            extra: HashMap::default(),
        };
        let result = store.authorize(AuthzRequest {
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
        let store = PolicyStore::new(JWT_POLICY, JWT_SCHEMA, "", false).unwrap();
        let claims = jwt_claims(Some("svc-alice"));
        let response = store
            .authorize(AuthzRequest {
                principal: principal_from_jwt(&claims, &parse_entity_type("User").unwrap()).unwrap(),
                action: entity_uid("Action", "read").unwrap(),
                resource: entity_uid("Document", "doc-1").unwrap(),
                context: build_authz_context(Some(&claims), None, None, None).unwrap(),
            })
            .unwrap();
        assert!(response.is_allowed());
    }

    #[test]
    fn jwt_context_denies_wrong_sub() {
        let store = PolicyStore::new(JWT_POLICY, JWT_SCHEMA, "", false).unwrap();
        let claims = jwt_claims(Some("svc-bob"));
        let response = store
            .authorize(AuthzRequest {
                principal: principal_from_jwt(&claims, &parse_entity_type("User").unwrap()).unwrap(),
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
        let store = PolicyStore::new(HTTP_POLICY, HTTP_SCHEMA, "", false).unwrap();
        let response = store
            .authorize(AuthzRequest {
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
        let store = PolicyStore::new(HTTP_POLICY, HTTP_SCHEMA, "", false).unwrap();
        let response = store
            .authorize(AuthzRequest {
                principal: entity_uid("User", "alice").unwrap(),
                action: entity_uid("Action", "call").unwrap(),
                resource: entity_uid("Api", "gateway").unwrap(),
                context: build_authz_context(None, Some("GET"), Some("/mcp"), None).unwrap(),
            })
            .unwrap();
        assert!(!response.is_allowed());
    }
}
