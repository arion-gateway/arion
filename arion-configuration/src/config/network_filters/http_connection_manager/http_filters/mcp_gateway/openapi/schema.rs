// Copyright 2026 The arion-gateway Authors
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

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Map, Value};

const COMPONENT_SCHEMA_PREFIX: &str = "#/components/schemas/";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ParameterKind {
    Scalar,
    Array,
}

pub(super) fn parameter_kind(schema: &Value, components: &BTreeMap<String, Value>) -> Option<ParameterKind> {
    // Objects and nested arrays are unsupported until the REST backend can
    // serialize their OpenAPI styles.
    let schema = resolve(schema, components)?;
    if is_scalar(schema) {
        Some(ParameterKind::Scalar)
    } else if schema.get("type").and_then(Value::as_str) == Some("array") {
        is_scalar(resolve(schema.get("items")?, components)?).then_some(ParameterKind::Array)
    } else {
        None
    }
}

fn is_scalar(schema: &Value) -> bool {
    const SCALAR_TYPES: &[&str] = &["string", "number", "integer", "boolean", "null"];
    match schema.get("type") {
        Some(Value::String(ty)) => SCALAR_TYPES.contains(&ty.as_str()),
        Some(Value::Array(types)) => types.iter().all(|ty| ty.as_str().is_some_and(|ty| SCALAR_TYPES.contains(&ty))),
        Some(_) => false,
        None => schema
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().all(|value| !value.is_array() && !value.is_object())),
    }
}

pub(super) fn resolves_to_object(schema: &Value, components: &BTreeMap<String, Value>) -> bool {
    resolve(schema, components).and_then(|schema| schema.get("type")?.as_str()) == Some("object")
}

fn resolve<'a>(mut schema: &'a Value, components: &'a BTreeMap<String, Value>) -> Option<&'a Value> {
    let mut visited = BTreeSet::new();
    while let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        let name = reference.strip_prefix(COMPONENT_SCHEMA_PREFIX)?;
        if !visited.insert(name) {
            return None;
        }
        schema = components.get(name)?;
    }
    Some(schema)
}

pub(super) fn validate(schema: &Map<String, Value>, kind: &str) -> Result<(), String> {
    jsonschema::Validator::new(&Value::Object(schema.clone()))
        .map(|_| ())
        .map_err(|error| format!("generated {kind} schema is invalid: {error}"))
}

pub(super) fn rewrite(value: &mut Value, used: &mut BTreeSet<String>) -> Result<(), String> {
    // Keywords whose value is a schema or a list of schemas.
    const SCHEMA_VALUE_KEYWORDS: &[&str] = &[
        "items",
        "additionalProperties",
        "additionalItems",
        "unevaluatedItems",
        "unevaluatedProperties",
        "propertyNames",
        "contains",
        "not",
        "if",
        "then",
        "else",
        "allOf",
        "anyOf",
        "oneOf",
        "prefixItems",
    ];
    // Keywords whose value maps names to schemas.
    const SCHEMA_MAP_KEYWORDS: &[&str] =
        &["properties", "patternProperties", "dependentSchemas", "$defs", "definitions"];

    let Value::Object(map) = value else {
        return Ok(()); // Boolean and other non-object schemas need no rewriting.
    };
    if let Some(Value::String(reference)) = map.get_mut("$ref") {
        let name = reference
            .strip_prefix(COMPONENT_SCHEMA_PREFIX)
            .ok_or_else(|| format!("unsupported $ref '{reference}'"))?
            .to_owned();
        *reference = format!("#/$defs/{name}");
        used.insert(name);
    }
    // OpenAPI 3.0 `nullable` becomes a JSON Schema type union.
    if map.get("nullable") == Some(&Value::Bool(true)) {
        map.remove("nullable");
        if let Some(ty @ Value::String(_)) = map.get_mut("type") {
            *ty = json!([ty, "null"]);
        }
    }
    // Recurse only into schema positions: data-carrying keywords like
    // `default`, `example`, `const`, and `enum` must stay untouched.
    for (key, child) in map.iter_mut() {
        if SCHEMA_VALUE_KEYWORDS.contains(&key.as_str()) {
            match child {
                Value::Array(items) => items.iter_mut().try_for_each(|item| rewrite(item, used))?,
                other => rewrite(other, used)?,
            }
        } else if SCHEMA_MAP_KEYWORDS.contains(&key.as_str()) {
            if let Value::Object(children) = child {
                children.values_mut().try_for_each(|child| rewrite(child, used))?;
            }
        }
    }
    Ok(())
}

pub(super) fn attach_defs(
    schema: &mut Map<String, Value>,
    mut used: BTreeSet<String>,
    components: &BTreeMap<String, Value>,
) -> Result<(), String> {
    let mut defs = Map::new();
    // rewrite() adds the names each component references to `used`, so nested
    // and cyclic references are attached too.
    while let Some(name) = used.pop_first() {
        if defs.contains_key(&name) {
            continue;
        }
        let mut component =
            components.get(&name).cloned().ok_or_else(|| format!("unresolved schema reference '{name}'"))?;
        rewrite(&mut component, &mut used)?;
        defs.insert(name, component);
    }
    if !defs.is_empty() {
        schema.insert("$defs".to_owned(), Value::Object(defs));
    }
    Ok(())
}
