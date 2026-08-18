use cedar_policy::{
    Authorizer, Context, Decision, Entities, EntityUid, PolicySet, Request, Schema, ValidationMode, Validator,
};
use smol_str::{SmolStr, ToSmolStr};
use std::sync::Arc;
use tracing::{debug, warn};

use super::error::{Error, ValidationError};

#[derive(Debug)]
pub struct PolicyStore {
    policy_set: PolicySet,
    schema: Schema,
    entities: Entities,
    authorizer: Authorizer,
    validate_schema_per_request: bool,
}

pub struct AuthzResponse {
    pub decision: Decision,
    pub reason: Option<Vec<SmolStr>>,
}

pub struct AuthzRequest {
    pub principal: EntityUid,
    pub action: EntityUid,
    pub resource: EntityUid,
    pub context: Context,
}

impl PolicyStore {
    pub fn new(
        policy_src: &str,
        schema_src: &str,
        entities_json: &str,
        validate_schema_per_request: bool,
    ) -> Result<Self, Error> {
        let schema: Schema = schema_src.parse()?;
        let policy_set: PolicySet = policy_src.parse()?;

        let validator = Validator::new(schema.clone());
        let result = validator.validate(&policy_set, ValidationMode::default());

        if let Some(err) = ValidationError::from_result(&result) {
            return Err(Error::Validation(err));
        }

        for w in result.validation_warnings() {
            warn!(warning = %w, "Cedar policy validation warning");
        }

        let entities = if entities_json.is_empty() {
            Entities::empty()
        } else {
            Entities::from_json_str(entities_json, Some(&schema))?
        };

        debug!(
            policies = policy_set.policies().count(),
            entities = entities.iter().count(),
            "Cedar policy store initialized"
        );

        Ok(Self { policy_set, schema, entities, authorizer: Authorizer::new(), validate_schema_per_request })
    }

    pub fn authorize(&self, authz_req: AuthzRequest) -> Result<AuthzResponse, Error> {
        let AuthzRequest { principal, action, resource, context } = authz_req;

        let request = Request::new(
            principal,
            action,
            resource,
            context,
            self.validate_schema_per_request.then_some(&self.schema),
        )
        .map_err(|e| Error::Context(e.to_string()))?;

        let response = self.authorizer.is_authorized(&request, &self.policy_set, &self.entities);
        let decision = response.decision();

        let reason =
            (decision == Decision::Deny).then(|| response.diagnostics().reason().map(ToSmolStr::to_smolstr).collect());

        debug!(
            principal = ?request.principal(),
            action = ?request.action(),
            resource = ?request.resource(),
            ?decision,
            ?reason,
            "Cedar authorization decision"
        );

        Ok(AuthzResponse { decision, reason })
    }

    #[cfg(test)]
    pub fn entities(&self) -> &Entities {
        &self.entities
    }
}

impl AuthzResponse {
    pub fn is_allowed(&self) -> bool {
        self.decision == Decision::Allow
    }
}

pub type SharedPolicyStore = Arc<PolicyStore>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cedar::request::entity_uid;
    use serde_json::json;

    const TEST_SCHEMA: &str = r#"
        entity User;
        entity Document;
        action "read" appliesTo {
            principal: [User],
            resource: [Document],
        };
        action "write" appliesTo {
            principal: [User],
            resource: [Document],
        };
    "#;

    const TEST_POLICY: &str = r#"
        permit (
            principal == User::"alice",
            action == Action::"read",
            resource == Document::"doc-1"
        );
    "#;

    #[test]
    fn store_loads_valid_policy() {
        let store = PolicyStore::new(TEST_POLICY, TEST_SCHEMA, "", false);
        store.unwrap();
    }

    #[test]
    fn store_rejects_invalid_policy_syntax() {
        let result = PolicyStore::new("not a policy", TEST_SCHEMA, "", false);
        result.unwrap_err();
    }

    #[test]
    fn store_rejects_schema_violation() {
        let bad_policy = r#"
            permit (
                principal == User::"alice",
                action == Action::"read",
                resource == Document::"doc-1"
            ) when { principal.nonexistent_attr == "x" };
        "#;
        let result = PolicyStore::new(bad_policy, TEST_SCHEMA, "", false);
        result.unwrap_err();
    }

    fn make_request(principal_id: &str, action_id: &str, resource_id: &str) -> AuthzRequest {
        AuthzRequest {
            principal: entity_uid("User", principal_id).unwrap(),
            action: entity_uid("Action", action_id).unwrap(),
            resource: entity_uid("Document", resource_id).unwrap(),
            context: Context::from_json_value(json!({}), None).unwrap(),
        }
    }

    #[test]
    fn authorize_permits_matching_request() {
        let store = PolicyStore::new(TEST_POLICY, TEST_SCHEMA, "", false).unwrap();
        let response = store.authorize(make_request("alice", "read", "doc-1")).unwrap();
        assert!(response.is_allowed());
    }

    #[test]
    fn authorize_denies_wrong_principal() {
        let store = PolicyStore::new(TEST_POLICY, TEST_SCHEMA, "", false).unwrap();
        let response = store.authorize(make_request("bob", "read", "doc-1")).unwrap();
        assert!(!response.is_allowed());
    }

    #[test]
    fn authorize_denies_wrong_action() {
        let store = PolicyStore::new(TEST_POLICY, TEST_SCHEMA, "", false).unwrap();
        let response = store.authorize(make_request("alice", "write", "doc-1")).unwrap();
        assert!(!response.is_allowed());
    }

    #[test]
    fn store_loads_entities_from_json() {
        let entities_json = r#"[
            {
                "uid": { "type": "User", "id": "alice" },
                "attrs": {},
                "parents": []
            }
        ]"#;
        let store = PolicyStore::new(TEST_POLICY, TEST_SCHEMA, entities_json, false).unwrap();
        let user_uid = entity_uid("User", "alice").unwrap();
        assert!(store.entities().get(&user_uid).is_some());
    }

    #[test]
    fn store_loads_empty_entities() {
        let store = PolicyStore::new(TEST_POLICY, TEST_SCHEMA, "", false).unwrap();
        assert!(store.entities().iter().next().is_none());
    }

    #[test]
    fn store_rejects_invalid_entities_json() {
        let result = PolicyStore::new(TEST_POLICY, TEST_SCHEMA, "not valid json", false);
        result.unwrap_err();
    }
}
