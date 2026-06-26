use cedar_policy::{Context, Entities, Entity, EntityId, EntityTypeName, EntityUid, RestrictedExpression};
use smol_str::SmolStr;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use crate::error::Error;

pub fn entity_uid(type_name: &str, id: &str) -> Result<EntityUid, Error> {
    let type_name = EntityTypeName::from_str(type_name)
        .map_err(|e| Error::Entity(SmolStr::from(format!("invalid entity type '{type_name}': {e}"))))?;
    let id =
        EntityId::from_str(id).map_err(|e| Error::Entity(SmolStr::from(format!("invalid entity id '{id}': {e}"))))?;
    Ok(EntityUid::from_type_name_and_id(type_name, id))
}

pub fn build_context(values: &serde_json::Value) -> Result<Context, Error> {
    Context::from_json_value(values.clone(), None).map_err(|e| Error::Context(SmolStr::from(e.to_string())))
}

pub fn build_entities(entities: Vec<Entity>) -> Result<Entities, Error> {
    Entities::from_entities(entities, None).map_err(|e| Error::Entity(SmolStr::from(e.to_string())))
}

pub fn empty_entities() -> Entities {
    Entities::empty()
}

#[allow(clippy::implicit_hasher)]
pub fn build_entity(
    type_name: &str,
    id: &str,
    attrs: HashMap<String, RestrictedExpression>,
    parents: HashSet<EntityUid>,
) -> Result<Entity, Error> {
    let uid = entity_uid(type_name, id)?;
    Entity::new(uid, attrs, parents).map_err(|e| Error::Entity(SmolStr::from(e.to_string())))
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn build_context_from_json() {
        let ctx = build_context(&json!({
            "method": "GET",
            "path": "/api/v1/users",
        }));
        assert!(ctx.is_ok());
    }

    #[test]
    fn build_entity_with_parents() {
        let parent_uid = entity_uid("AgentIdentity::TokenVault", "default").unwrap();
        let entity = build_entity(
            "AgentIdentity::ApiKeyCredentialProvider",
            "provider-1",
            HashMap::new(),
            HashSet::from([parent_uid]),
        );
        assert!(entity.is_ok());
    }

    #[test]
    fn empty_entities_is_valid() {
        let entities = empty_entities();
        assert!(entities.iter().next().is_none());
    }
}
