use serde_json::Value;
use smol_str::SmolStr;
use std::str::FromStr;
use tracing::debug;

use crate::listeners::http_connection_manager::jwt_authn::claims::JwtClaims;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwtHeaderField {
    Algorithm,
    Type,
    ContentType,
    JsonKeyURL,
    JsonWebKey,
    KeyID,
    X509URL,
    X509CertificateChain,
    X509CertificateSHA1Thumbprint,
    X509CertificateSHA256Thumbprint,
    Critical,
    Encryption,
    Zip,
    Url,
    Nonce,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwtClaimField {
    Issuer,
    Subject,
    Audience,
    Expiration,
    IssuedAt,
    NotBefore,
    JWTID,
    Extra(SmolStr),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwtHeaderMatcher {
    pub field: JwtHeaderField,
    pub value: SmolStr,
}

impl JwtHeaderMatcher {
    pub fn matches(&self, ext: &http::Extensions) -> bool {
        let header = ext.get::<jsonwebtoken::Header>();

        if let Some(header) = header {
            match &self.field {
                JwtHeaderField::Algorithm => {
                    jsonwebtoken::Algorithm::from_str(self.value.as_str()) == Ok(header.alg)
                },
                JwtHeaderField::Type => header.typ.as_ref().is_some_and(|t| t.as_str() == self.value.as_str()),
                JwtHeaderField::ContentType => header.cty.as_ref().is_some_and(|c| c.as_str() == self.value.as_str()),
                JwtHeaderField::KeyID => header.kid.as_ref().is_some_and(|k| k.as_str() == self.value.as_str()),
                JwtHeaderField::JsonKeyURL => header.jku.as_ref().is_some_and(|j| j.as_str() == self.value.as_str()),
                JwtHeaderField::X509URL => header.x5u.as_ref().is_some_and(|x| x.as_str() == self.value.as_str()),
                _ => {
                    // Other fields not commonly used or available in jsonwebtoken::Header
                    debug!(target: "mcp_rbac", "JWT header field {:?} not supported for matching", self.field);
                    false
                },
            }
        } else {
            debug!(target: "mcp_rbac", "No JWT header found in request extensions");
            false
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwtPayloadMatcher {
    pub field: JwtClaimField,
    pub value: SmolStr,
}

impl JwtPayloadMatcher {
    pub fn matches(&self, ext: &http::Extensions) -> bool {
        let claims = ext.get::<JwtClaims>();

        if let Some(claims) = claims {
            match &self.field {
                JwtClaimField::Issuer => claims.iss.as_ref().is_some_and(|iss| iss.as_str() == self.value.as_str()),
                JwtClaimField::Subject => claims.sub.as_ref().is_some_and(|sub| sub.as_str() == self.value.as_str()),
                JwtClaimField::Audience => {
                    claims.aud.as_ref().is_some_and(|aud| aud.iter().any(|a| a.as_str() == self.value.as_str()))
                },
                JwtClaimField::JWTID => claims.jti.as_ref().is_some_and(|jti| jti.as_str() == self.value.as_str()),
                JwtClaimField::Expiration => claims.exp.is_some_and(|exp| exp.to_string() == self.value.as_str()),
                JwtClaimField::IssuedAt => claims.iat.is_some_and(|iat| iat.to_string() == self.value.as_str()),
                JwtClaimField::NotBefore => claims.nbf.is_some_and(|nbf| nbf.to_string() == self.value.as_str()),
                JwtClaimField::Extra(claim_name) => {
                    // Check custom claims
                    claims.extra.get(claim_name.as_str()).is_some_and(|v| match v {
                        Value::String(s) => s.as_str() == self.value.as_str(),
                        Value::Number(n) => n.to_string() == self.value.as_str(),
                        Value::Bool(b) => b.to_string() == self.value.as_str(),
                        _ => false,
                    })
                },
            }
        } else {
            debug!(target: "mcp_rbac", "No JWT claims found in request extensions");
            false
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Permission {
    JwtHeader(JwtHeaderMatcher),
    JwtClaim(JwtPayloadMatcher),
}

impl Permission {
    pub fn is_applicable(&self, ext: &http::Extensions) -> bool {
        match self {
            Permission::JwtHeader(matcher) => matcher.matches(ext),
            Permission::JwtClaim(matcher) => matcher.matches(ext),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolRbac {
    pub action: Action,
    /// List of permissions (OR logic - any permission can match)
    pub permissions: Vec<Permission>,
}

impl ToolRbac {
    pub fn new() -> Self {
        Self { action: Action::Allow, permissions: Vec::new() }
    }

    pub fn is_permitted(&self, ext: &http::Extensions) -> bool {
        let any_permission_matched = self.permissions.iter().any(|p| p.is_applicable(ext));

        let permitted = match self.action {
            Action::Allow => any_permission_matched,
            Action::Deny => !any_permission_matched,
        };

        debug!(
            target: "mcp_gateway",
            "Tool RBAC: action={:?}, matched={}, permitted={}",
            self.action, any_permission_matched, permitted
        );

        permitted
    }
}

impl Default for ToolRbac {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tool_rbac_tests {
    use super::*;
    use http::Request;
    use serde_json::json;
    use std::collections::HashMap;

    fn create_test_claims(subject: &str, role: Option<&str>) -> JwtClaims {
        let mut extra = HashMap::default();
        if let Some(r) = role {
            extra.insert("role".to_owned(), json!(r));
        }

        JwtClaims {
            iss: Some("test-issuer".into()),
            sub: Some(subject.into()),
            aud: Some(vec!["test-audience".into()]),
            exp: Some(1234567890),
            iat: Some(1234567800),
            nbf: Some(1234567800),
            jti: Some("test-jti".into()),
            extra,
        }
    }

    fn create_test_header(kid: Option<&str>, alg: jsonwebtoken::Algorithm) -> jsonwebtoken::Header {
        jsonwebtoken::Header { alg, kid: kid.map(String::from), ..Default::default() }
    }

    fn create_request_with_claims(claims: JwtClaims, header: jsonwebtoken::Header) -> Request<()> {
        let mut req = Request::builder().uri("/test").body(()).unwrap();
        req.extensions_mut().insert(claims);
        req.extensions_mut().insert(header);
        req
    }

    #[test]
    fn test_allow_with_matching_role() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![Permission::JwtClaim(JwtPayloadMatcher {
                field: JwtClaimField::Extra("role".into()),
                value: "admin".into(),
            })],
        };

        let claims = create_test_claims("user@example.com", Some("admin"));
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);

        let permitted = rbac.is_permitted(req.extensions());
        assert!(permitted);
    }

    #[test]
    fn test_allow_without_matching_role() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![Permission::JwtClaim(JwtPayloadMatcher {
                field: JwtClaimField::Extra("role".into()),
                value: "admin".into(),
            })],
        };

        let claims = create_test_claims("user@example.com", Some("user"));
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);

        let permitted = rbac.is_permitted(req.extensions());
        assert!(!permitted);
    }

    #[test]
    fn test_deny_with_matching_role() {
        let rbac = ToolRbac {
            action: Action::Deny,
            permissions: vec![Permission::JwtClaim(JwtPayloadMatcher {
                field: JwtClaimField::Extra("role".into()),
                value: "guest".into(),
            })],
        };

        let claims = create_test_claims("user@example.com", Some("guest"));
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);

        let permitted = rbac.is_permitted(req.extensions());
        assert!(!permitted);
    }

    #[test]
    fn test_deny_without_matching_role() {
        let rbac = ToolRbac {
            action: Action::Deny,
            permissions: vec![Permission::JwtClaim(JwtPayloadMatcher {
                field: JwtClaimField::Extra("role".into()),
                value: "guest".into(),
            })],
        };

        let claims = create_test_claims("user@example.com", Some("admin"));
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);

        let permitted = rbac.is_permitted(req.extensions());
        assert!(permitted);
    }

    #[test]
    fn test_multiple_permissions_or_logic() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![
                Permission::JwtClaim(JwtPayloadMatcher {
                    field: JwtClaimField::Extra("role".into()),
                    value: "admin".into(),
                }),
                Permission::JwtClaim(JwtPayloadMatcher {
                    field: JwtClaimField::Extra("role".into()),
                    value: "moderator".into(),
                }),
            ],
        };

        // Test with admin role
        let claims = create_test_claims("user@example.com", Some("admin"));
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        let permitted = rbac.is_permitted(req.extensions());
        assert!(permitted);

        // Test with moderator role
        let claims = create_test_claims("user@example.com", Some("moderator"));
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        let permitted = rbac.is_permitted(req.extensions());
        assert!(permitted);

        // Test with user role (should be denied)
        let claims = create_test_claims("user@example.com", Some("user"));
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        let permitted = rbac.is_permitted(req.extensions());
        assert!(!permitted);
    }

    #[test]
    fn test_subject_matching() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![Permission::JwtClaim(JwtPayloadMatcher {
                field: JwtClaimField::Subject,
                value: "alice@example.com".into(),
            })],
        };

        // Alice should be allowed
        let claims = create_test_claims("alice@example.com", None);
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        let permitted = rbac.is_permitted(req.extensions());
        assert!(permitted);

        // Bob should be denied
        let claims = create_test_claims("bob@example.com", None);
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        let permitted = rbac.is_permitted(req.extensions());
        assert!(!permitted);
    }

    #[test]
    fn test_key_id_matching() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![Permission::JwtHeader(JwtHeaderMatcher {
                field: JwtHeaderField::KeyID,
                value: "trusted-key-123".into(),
            })],
        };

        let claims = create_test_claims("user@example.com", None);

        // Request with trusted key
        let header = create_test_header(Some("trusted-key-123"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims.clone(), header);
        let permitted = rbac.is_permitted(req.extensions());
        assert!(permitted);

        // Request with untrusted key
        let header = create_test_header(Some("untrusted-key"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        let permitted = rbac.is_permitted(req.extensions());
        assert!(!permitted);
    }

    #[test]
    fn test_algorithm_matching() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![Permission::JwtHeader(JwtHeaderMatcher {
                field: JwtHeaderField::Algorithm,
                value: "RS256".into(),
            })],
        };

        let claims = create_test_claims("user@example.com", None);

        // Request with RS256 algorithm
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::RS256);
        let req = create_request_with_claims(claims.clone(), header);
        let permitted = rbac.is_permitted(req.extensions());
        assert!(permitted);

        // Request with HS256 algorithm
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        let permitted = rbac.is_permitted(req.extensions());
        assert!(!permitted);
    }

    #[test]
    fn test_no_jwt_in_request() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![Permission::JwtClaim(JwtPayloadMatcher {
                field: JwtClaimField::Subject,
                value: "user@example.com".into(),
            })],
        };

        // Request without JWT claims
        let req = Request::builder().uri("/test").body(()).unwrap();
        let permitted = rbac.is_permitted(req.extensions());
        assert!(!permitted);
    }

    #[test]
    fn test_empty_permissions_denies_all() {
        let rbac = ToolRbac { action: Action::Allow, permissions: vec![] };

        let claims = create_test_claims("user@example.com", Some("admin"));
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);

        let permitted = rbac.is_permitted(req.extensions());
        assert!(!permitted);
    }
}
