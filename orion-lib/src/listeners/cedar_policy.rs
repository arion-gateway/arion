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

    fn evaluate<B>(&self, req: &Request<B>) -> Result<AuthzResponse, CedarError> {
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
