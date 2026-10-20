// Copyright 2025-2026 The arion-gateway Authors
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
    JwtID,
    Extra(SmolStr),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwtHeaderMatcher {
    pub field: JwtHeaderField,
    pub value: SmolStr,
}

impl JwtHeaderMatcher {
    #[inline]
    pub fn matches(&self, ext: &http::Extensions) -> bool {
        self.matches_header(ext.get::<jsonwebtoken::Header>())
    }

    pub(crate) fn matches_header(&self, header: Option<&jsonwebtoken::Header>) -> bool {
        if let Some(header) = header {
            match &self.field {
                JwtHeaderField::Algorithm => jsonwebtoken::Algorithm::from_str(self.value.as_str()) == Ok(header.alg),
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
    #[inline]
    pub fn matches(&self, ext: &http::Extensions) -> bool {
        self.matches_claims(ext.get::<JwtClaims>())
    }

    pub(crate) fn matches_claims(&self, claims: Option<&JwtClaims>) -> bool {
        if let Some(claims) = claims {
            match &self.field {
                JwtClaimField::Issuer => claims.iss.as_ref().is_some_and(|iss| iss.as_str() == self.value.as_str()),
                JwtClaimField::Subject => claims.sub.as_ref().is_some_and(|sub| sub.as_str() == self.value.as_str()),
                JwtClaimField::Audience => {
                    claims.aud.as_ref().is_some_and(|aud| aud.iter().any(|a| a.as_str() == self.value.as_str()))
                },
                JwtClaimField::JwtID => claims.jti.as_ref().is_some_and(|jti| jti.as_str() == self.value.as_str()),
                JwtClaimField::Expiration => claims.exp.is_some_and(|exp| u64_eq_str(exp, self.value.as_str())),
                JwtClaimField::IssuedAt => claims.iat.is_some_and(|iat| u64_eq_str(iat, self.value.as_str())),
                JwtClaimField::NotBefore => claims.nbf.is_some_and(|nbf| u64_eq_str(nbf, self.value.as_str())),
                JwtClaimField::Extra(claim_name) => {
                    // Check custom claims
                    claims.extra.get(claim_name.as_str()).is_some_and(|v| match v {
                        Value::String(s) => s.as_str() == self.value.as_str(),
                        Value::Number(n) => json_number_eq_str(n, self.value.as_str()),
                        Value::Bool(true) => self.value.as_str() == "true",
                        Value::Bool(false) => self.value.as_str() == "false",
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
        let mut cached_header: Option<Option<&jsonwebtoken::Header>> = None;
        let mut cached_claims: Option<Option<&JwtClaims>> = None;

        let any_permission_matched = self.permissions.iter().any(|p| match p {
            Permission::JwtHeader(matcher) => {
                let header = *cached_header.get_or_insert_with(|| ext.get::<jsonwebtoken::Header>());
                matcher.matches_header(header)
            },
            Permission::JwtClaim(matcher) => {
                let claims = *cached_claims.get_or_insert_with(|| ext.get::<JwtClaims>());
                matcher.matches_claims(claims)
            },
        });

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

#[inline]
fn i64_eq_str(n: i64, expected: &str) -> bool {
    let mut buf = itoa::Buffer::new();
    buf.format(n) == expected
}

#[inline]
fn u64_eq_str(n: u64, expected: &str) -> bool {
    let mut buf = itoa::Buffer::new();
    buf.format(n) == expected
}

#[inline]
fn json_number_eq_str(n: &serde_json::Number, expected: &str) -> bool {
    if let Some(i) = n.as_i64() {
        i64_eq_str(i, expected)
    } else if let Some(u) = n.as_u64() {
        u64_eq_str(u, expected)
    } else {
        n.to_string() == expected
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
            exp: Some(1_234_567_890),
            iat: Some(1_234_567_800),
            nbf: Some(1_234_567_800),
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
                value: "admin@example.com".into(),
            })],
        };

        let claims = create_test_claims("admin@example.com", None);
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        assert!(rbac.is_permitted(req.extensions()));

        let claims = create_test_claims("user@example.com", None);
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        assert!(!rbac.is_permitted(req.extensions()));
    }

    #[test]
    fn test_header_kid_matching() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![Permission::JwtHeader(JwtHeaderMatcher {
                field: JwtHeaderField::KeyID,
                value: "trusted-key-1".into(),
            })],
        };

        let claims = create_test_claims("user@example.com", None);
        let header = create_test_header(Some("trusted-key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        assert!(rbac.is_permitted(req.extensions()));

        let claims = create_test_claims("user@example.com", None);
        let header = create_test_header(Some("untrusted-key"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        assert!(!rbac.is_permitted(req.extensions()));
    }

    #[test]
    fn test_header_algorithm_matching() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![Permission::JwtHeader(JwtHeaderMatcher {
                field: JwtHeaderField::Algorithm,
                value: "RS256".into(),
            })],
        };

        let claims = create_test_claims("user@example.com", None);
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::RS256);
        let req = create_request_with_claims(claims, header);
        assert!(rbac.is_permitted(req.extensions()));

        let claims = create_test_claims("user@example.com", None);
        let header = create_test_header(Some("key-1"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        assert!(!rbac.is_permitted(req.extensions()));
    }

    #[test]
    fn test_no_jwt_in_extensions() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![Permission::JwtClaim(JwtPayloadMatcher {
                field: JwtClaimField::Subject,
                value: "admin@example.com".into(),
            })],
        };

        let req = Request::builder().uri("/test").body(()).unwrap();
        // Should deny when no JWT is present and action is Allow
        assert!(!rbac.is_permitted(req.extensions()));

        let rbac_deny = ToolRbac {
            action: Action::Deny,
            permissions: vec![Permission::JwtClaim(JwtPayloadMatcher {
                field: JwtClaimField::Subject,
                value: "blocked@example.com".into(),
            })],
        };

        // Should allow when no JWT is present and action is Deny (no match means not denied)
        assert!(rbac_deny.is_permitted(req.extensions()));
    }

    #[test]
    fn test_combined_header_and_claim_permissions() {
        let rbac = ToolRbac {
            action: Action::Allow,
            permissions: vec![
                Permission::JwtHeader(JwtHeaderMatcher { field: JwtHeaderField::KeyID, value: "admin-key".into() }),
                Permission::JwtClaim(JwtPayloadMatcher {
                    field: JwtClaimField::Extra("role".into()),
                    value: "admin".into(),
                }),
            ],
        };

        // Match via header
        let claims = create_test_claims("user@example.com", Some("user"));
        let header = create_test_header(Some("admin-key"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        assert!(rbac.is_permitted(req.extensions()));

        // Match via claim
        let claims = create_test_claims("user@example.com", Some("admin"));
        let header = create_test_header(Some("regular-key"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        assert!(rbac.is_permitted(req.extensions()));

        // Match neither
        let claims = create_test_claims("user@example.com", Some("user"));
        let header = create_test_header(Some("regular-key"), jsonwebtoken::Algorithm::HS256);
        let req = create_request_with_claims(claims, header);
        assert!(!rbac.is_permitted(req.extensions()));
    }
}
