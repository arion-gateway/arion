use http::Request;
use orion_configuration::config::network_filters::http_connection_manager::http_filters::cedar_policy::{
    CedarPolicy as CedarPolicyConfig, EnforcementMode, FailureMode,
};
use smol_str::SmolStr;
use std::sync::Arc;
use tracing::{debug, info};

use cedar_policy::{EntityTypeName, EntityUid};

use crate::cedar::{
    error::Error as CedarError,
    request::{build_authz_context, entity_uid_from_type, parse_entity_type, principal_from_jwt},
    store::{AuthzRequest, AuthzResponse, SharedPolicyStore},
};

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
    principal_type: EntityTypeName,
    resource_type: EntityTypeName,
    action_type: EntityTypeName,
    anonymous_principal: EntityUid,
}

impl CedarHttpFilter {
    pub(crate) fn try_from_config(conf: CedarPolicyConfig) -> crate::Result<Self> {
        let store = crate::cedar::store::PolicyStore::new(&conf.policies, &conf.schema, &conf.entities)?;
        let principal_type = parse_entity_type(&conf.principal_entity_type)?;
        let resource_type = parse_entity_type(&conf.resource_entity_type)?;
        let action_type = parse_entity_type("Action")?;
        let anonymous_principal = entity_uid_from_type(&principal_type, "anonymous");
        Ok(Self {
            store: Arc::new(store),
            enforcement_mode: conf.enforcement_mode,
            failure_mode: conf.failure_mode,
            principal_type,
            resource_type,
            action_type,
            anonymous_principal,
        })
    }

    fn evaluate_policy<B>(&self, req: &Request<B>) -> Result<AuthzResponse, CedarError> {
        let claims = req.extensions().get::<JwtClaims>();

        let principal = match claims {
            Some(claims) => principal_from_jwt(claims, &self.principal_type)?,
            None => self.anonymous_principal.clone(),
        };
        let action = entity_uid_from_type(&self.action_type, req.method().as_str());
        let resource = entity_uid_from_type(&self.resource_type, req.uri().path());
        let context =
            build_authz_context(claims, Some(req.method().as_str()), Some(req.uri().path()), req.uri().query())?;
        self.store.is_authorized(AuthzRequest { principal, action, resource, context })
    }

    pub(crate) fn apply_request<B>(&self, req: &Request<B>) -> FilterDecision {
        match self.evaluate_policy(req) {
            Ok(response) => {
                debug!(
                    target: "cedar_policy",
                    decision = ?response.decision,
                    policy_id = ?response.reason.as_ref().and_then(|r| r.first()),
                    enforcement = ?self.enforcement_mode,
                    "Cedar authorization decision"
                );

                match self.enforcement_mode {
                    EnforcementMode::Enforce => {
                        if response.is_allowed() {
                            FilterDecision::Continue
                        } else {
                            let policy_id =
                                response.reason.and_then(|mut r| r.pop()).unwrap_or(SmolStr::new_static("cedar"));
                            FilterDecision::DirectResponse(Box::new(
                                SyntheticHttpResponse::forbidden(EventKind::Failure(EventFailure::CedarAccessDenied(
                                    policy_id,
                                )))
                                .with_body("Access denied by policy")
                                .into_response(req.version()),
                            ))
                        }
                    },
                    EnforcementMode::LogOnly => {
                        info!(target: "cedar_policy", "Request is denied by Cedar policy");
                        debug!(target: "cedar_policy", "Denied request: {:?}", req.uri());
                        FilterDecision::Continue
                    },
                }
            },
            Err(err) => {
                debug!(target: "cedar_policy", %err, "Cedar policy evaluation error");

                match self.failure_mode {
                    FailureMode::FailClosed => FilterDecision::DirectResponse(Box::new(
                        SyntheticHttpResponse::forbidden(EventKind::Failure(EventFailure::CedarAccessDenied(
                            SmolStr::new_static("error"),
                        )))
                        .with_body("Access denied due to policy evaluation error")
                        .into_response(req.version()),
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
    use ahash::RandomState;
    use orion_configuration::config::network_filters::http_connection_manager::http_filters::cedar_policy::CedarPolicy as CedarPolicyConfig;
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
        assert!(matches!(filter.apply_request(&req), FilterDecision::Continue), "svc-frontend GET should be allowed");
    }

    #[test]
    fn svc_frontend_post_is_denied() {
        let filter = make_cedar_filter();
        let req = make_request("POST", "/api", jwt_claims("svc-frontend"));
        assert!(
            matches!(filter.apply_request(&req), FilterDecision::DirectResponse(_)),
            "svc-frontend POST should be denied"
        );
    }

    #[test]
    fn svc_admin_post_is_allowed() {
        let filter = make_cedar_filter();
        let req = make_request("POST", "/api", jwt_claims("svc-admin"));
        assert!(matches!(filter.apply_request(&req), FilterDecision::Continue), "svc-admin POST should be allowed");
    }
}
