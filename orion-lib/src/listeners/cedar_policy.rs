use http::Request;
use orion_cedar::{
    entity::entity_uid,
    error::Error as CedarError,
    request::{build_authz_context, principal_from_jwt},
    store::{AuthzRequest, AuthzResponse, SharedPolicyStore},
};
use orion_configuration::config::network_filters::http_connection_manager::http_filters::cedar_policy::{
    CedarPolicy as CedarPolicyConfig, EnforcementMode, FailureMode,
};
use serde_json::Value;
use smol_str::SmolStr;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::{
    event_error::{EventFailure, EventKind},
    listeners::{
        http_connection_manager::jwt_authn::claims::JwtClaims, http_filters::FilterDecision,
        synthetic_http_response::SyntheticHttpResponse,
    },
};

#[derive(Debug, Clone)]
pub(crate) struct CedarHttpFilter {
    store: SharedPolicyStore,
    enforcement_mode: EnforcementMode,
    failure_mode: FailureMode,
    principal_entity_type: SmolStr,
    resource_entity_type: SmolStr,
}

impl CedarHttpFilter {
    pub(crate) fn try_from_config(conf: CedarPolicyConfig) -> crate::Result<Self> {
        let store = orion_cedar::store::PolicyStore::new(&conf.policies, &conf.schema, &conf.entities)?;
        Ok(Self {
            store: Arc::new(store),
            enforcement_mode: conf.enforcement_mode,
            failure_mode: conf.failure_mode,
            principal_entity_type: conf.principal_entity_type,
            resource_entity_type: conf.resource_entity_type,
        })
    }

    pub(crate) fn apply_request<B>(&self, req: &Request<B>) -> FilterDecision {
        let result = self.evaluate(req);
        self.make_decision(result, req.version())
    }

    pub(crate) fn evaluate<B>(&self, req: &Request<B>) -> Result<AuthzResponse, CedarError> {
        let claims_value: Option<Value> =
            req.extensions().get::<JwtClaims>().and_then(|c| serde_json::to_value(c).ok());

        let principal = claims_value
            .as_ref()
            .map(|v| principal_from_jwt(v, &self.principal_entity_type))
            .unwrap_or_else(|| entity_uid(&self.principal_entity_type, "anonymous"))?;

        let action = entity_uid("Action", req.method().as_str())?;
        let resource = entity_uid(&self.resource_entity_type, req.uri().path())?;
        let context = build_authz_context(
            claims_value.as_ref(),
            Some(req.method().as_str()),
            Some(req.uri().path()),
            req.uri().query(),
            None,
            None,
            None,
        )?;

        self.store.is_authorized(AuthzRequest { principal, action, resource, context })
    }

    fn make_decision(&self, result: Result<AuthzResponse, CedarError>, ver: http::Version) -> FilterDecision {
        match result {
            Ok(response) => {
                let policy_id = response.diagnostics.reason.first().cloned().unwrap_or(SmolStr::new_static("cedar"));

                debug!(
                    decision = ?response.decision,
                    policy_id = %policy_id,
                    enforcement = ?self.enforcement_mode,
                    "Cedar authorization decision"
                );

                match self.enforcement_mode {
                    EnforcementMode::Enforce => {
                        if response.is_allowed() {
                            FilterDecision::Continue
                        } else {
                            FilterDecision::DirectResponse(Box::new(
                                SyntheticHttpResponse::forbidden(
                                    EventKind::Failure(EventFailure::CedarAccessDenied(policy_id)),
                                    "Cedar: access denied",
                                )
                                .into_response(ver),
                            ))
                        }
                    },
                    EnforcementMode::LogOnly => FilterDecision::Continue,
                }
            },
            Err(err) => {
                warn!(%err, "Cedar policy evaluation error");

                match self.failure_mode {
                    FailureMode::FailClosed => FilterDecision::DirectResponse(Box::new(
                        SyntheticHttpResponse::forbidden(
                            EventKind::Failure(EventFailure::CedarAccessDenied(SmolStr::new_static("error"))),
                            "Cedar: policy evaluation error",
                        )
                        .into_response(ver),
                    )),
                    FailureMode::FailOpen => FilterDecision::Continue,
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::listeners::http_connection_manager::jwt_authn::claims::JwtClaims;
    use ahash::RandomState;
    use orion_configuration::config::network_filters::http_connection_manager::http_filters::cedar_policy::{
        CedarPolicy as CedarPolicyConfig, EnforcementMode, FailureMode,
    };
    use smol_str::SmolStr;
    use std::collections::HashMap;

    const JWT_SCHEMA: &str = r#"
        entity User;
        entity HttpPath;
        action "GET" appliesTo {
            principal: [User], resource: [HttpPath],
            context: { jwt: { sub?: String, iss?: String, aud?: Set<String>, exp?: Long, iat?: Long }, http: { method: String, path: String, query?: String } }
        };
        action "POST" appliesTo {
            principal: [User], resource: [HttpPath],
            context: { jwt: { sub?: String, iss?: String, aud?: Set<String>, exp?: Long, iat?: Long }, http: { method: String, path: String, query?: String } }
        };
    "#;

    const JWT_POLICIES: &str = r#"
        permit(principal == User::"svc-frontend", action == Action::"GET", resource);
        permit(principal == User::"svc-admin",    action,                  resource);
    "#;

    fn make_cedar_filter() -> CedarHttpFilter {
        CedarHttpFilter::try_from_config(CedarPolicyConfig {
            schema: JWT_SCHEMA.into(),
            policies: JWT_POLICIES.into(),
            entities: SmolStr::default(),
            enforcement_mode: EnforcementMode::Enforce,
            failure_mode: FailureMode::FailOpen,
            principal_entity_type: "User".into(),
            resource_entity_type: "HttpPath".into(),
        })
        .unwrap()
    }

    fn jwt_claims(sub: &str) -> JwtClaims {
        JwtClaims {
            sub: Some(SmolStr::from(sub)),
            iss: Some(SmolStr::from("test-issuer")),
            aud: Some(vec![SmolStr::from("mcp-gateway")]),
            exp: Some(9_999_999_999),
            iat: Some(0),
            nbf: None,
            jti: None,
            extra: HashMap::with_hasher(RandomState::new()),
        }
    }

    fn make_request(method: &str, path: &str, claims: JwtClaims) -> Request<()> {
        let mut req = Request::builder().method(method).uri(format!("http://example.com{path}")).body(()).unwrap();
        req.extensions_mut().insert(claims);
        req
    }

    #[test]
    fn svc_frontend_get_is_allowed() {
        let filter = make_cedar_filter();
        let req = make_request("GET", "/api", jwt_claims("svc-frontend"));
        let result = filter.evaluate(&req);
        assert!(result.is_ok(), "Cedar should not error: {:?}", result.err());
        assert!(result.unwrap().is_allowed(), "svc-frontend GET should be allowed");
    }

    #[test]
    fn svc_frontend_post_is_denied() {
        let filter = make_cedar_filter();
        let req = make_request("POST", "/api", jwt_claims("svc-frontend"));
        let result = filter.evaluate(&req);
        assert!(result.is_ok(), "Cedar should not error: {:?}", result.err());
        assert!(!result.unwrap().is_allowed(), "svc-frontend POST should be denied");
    }

    #[test]
    fn svc_admin_post_is_allowed() {
        let filter = make_cedar_filter();
        let req = make_request("POST", "/api", jwt_claims("svc-admin"));
        let result = filter.evaluate(&req);
        assert!(result.is_ok(), "Cedar should not error: {:?}", result.err());
        assert!(result.unwrap().is_allowed(), "svc-admin POST should be allowed");
    }
}
