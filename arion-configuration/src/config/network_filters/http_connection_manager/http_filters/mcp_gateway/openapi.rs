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

mod model;
mod schema;

use std::collections::{BTreeMap, BTreeSet};

use arion_interner::InternedStr;
use serde_json::{Map, Value, json};
use smol_str::SmolStr;
use tracing::warn;

use self::model::{MediaType, Operation, Parameter, PathItem, RefOr, Spec};
use super::{
    DisclosureMode, EmbeddingVector, HttpUpstreamPolicy, McpRestQueryParams, McpTool, McpToolRbac, UpstreamBackend,
};
use crate::config::GenericError;

#[derive(Debug, Clone)]
pub struct OpenApiExpansionParams {
    pub namespace: InternedStr,
    pub cluster: InternedStr,
    pub upstream_policy: HttpUpstreamPolicy,
    pub rbac: Option<McpToolRbac>,
    pub tool_disclosure_mode: DisclosureMode,
    pub path_prefix: Option<String>,
    pub skip_unsupported_operations: bool,
    pub include_operations: Vec<String>,
    pub exclude_operations: Vec<String>,
}

pub fn openapi_to_tools(spec: &[u8], params: &OpenApiExpansionParams) -> Result<Vec<McpTool>, GenericError> {
    let spec: Spec = serde_yaml::from_slice(spec)
        .map_err(|error| GenericError::from_msg(format!("Failed to parse OpenAPI document: {error}")))?;
    if spec.swagger.is_some() {
        return Err(GenericError::from_msg("Swagger 2.0 documents are not supported; provide an OpenAPI 3.x document"));
    }
    match &spec.openapi {
        Some(version) if !version.starts_with("3.") => {
            return Err(GenericError::from_msg(format!("Unsupported OpenAPI version '{version}'")));
        },
        None if spec.paths.is_empty() => {
            return Err(GenericError::from_msg("Not an OpenAPI 3.x document: no 'openapi' version and no paths"));
        },
        _ => {},
    }
    let base_path = base_path(params, &spec);
    let unsupported = |what: String| {
        if params.skip_unsupported_operations {
            warn!("OpenAPI source '{}': skipping {what}", params.namespace);
            Ok(())
        } else {
            Err(GenericError::from_msg(format!("{what} (set skip_unsupported_operations to skip it)")))
        }
    };

    let mut tools = Vec::new();
    let mut names = BTreeSet::new();
    for (raw_path, path_item) in &spec.paths {
        if path_item.reference.is_some() {
            unsupported(format!("path '{raw_path}': path item $ref is unsupported"))?;
            continue;
        }
        for (method, operation) in path_item.operations() {
            let method_path = format!("{method} {raw_path}");
            if !is_selected(params, operation.id.as_deref(), &method_path) {
                continue;
            }
            match build_tool(params, &spec, &base_path, raw_path, path_item, &method, operation, &names) {
                Ok(tool) => {
                    names.insert(tool.name.clone());
                    tools.push(tool);
                },
                Err(reason) => unsupported(format!("operation '{method_path}': {reason}"))?,
            }
        }
    }
    Ok(tools)
}

fn base_path(params: &OpenApiExpansionParams, spec: &Spec) -> String {
    let base = match &params.path_prefix {
        Some(prefix) => prefix.clone(),
        None => match spec.servers.first().map(|server| server.url.as_str()) {
            None | Some("") => String::new(),
            Some(url) if url.starts_with('/') => url.to_owned(),
            Some(url) => {
                if let Ok(parsed) = url::Url::parse(url) {
                    parsed.path().to_owned()
                } else {
                    warn!("OpenAPI source '{}': ignoring unparsable server URL '{url}'", params.namespace);
                    String::new()
                }
            },
        },
    };
    let base = base.trim_end_matches('/');
    if base.is_empty() || base.starts_with('/') { base.to_owned() } else { format!("/{base}") }
}

fn is_selected(params: &OpenApiExpansionParams, operation_id: Option<&str>, method_path: &str) -> bool {
    let matches = |patterns: &[String]| {
        patterns
            .iter()
            .any(|pattern| operation_id.is_some_and(|id| glob_match(pattern, id)) || glob_match(pattern, method_path))
    };
    let included = params.include_operations.is_empty() || matches(&params.include_operations);
    included && !matches(&params.exclude_operations)
}

fn glob_match(pattern: &str, text: &str) -> bool {
    let (pattern, text) = (pattern.as_bytes(), text.as_bytes());
    let (mut p, mut t) = (0, 0);
    let mut backtrack = None;
    while let Some(byte) = text.get(t) {
        if pattern.get(p) == Some(&b'*') {
            backtrack = Some((p, t));
            p += 1;
        } else if pattern.get(p).is_some_and(|c| *c == b'?' || c == byte) {
            p += 1;
            t += 1;
        } else if let Some((star, matched)) = backtrack {
            backtrack = Some((star, matched + 1));
            p = star + 1;
            t = matched + 1;
        } else {
            return false;
        }
    }
    pattern.iter().skip(p).all(|&c| c == b'*')
}

const BODY_ARGUMENT: &str = "body";
const MAX_MCP_TOOL_NAME_BYTES: usize = 128;

#[allow(clippy::too_many_arguments)]
fn build_tool(
    params: &OpenApiExpansionParams,
    spec: &Spec,
    base_path: &str,
    raw_path: &str,
    path_item: &PathItem,
    method: &http::Method,
    operation: &Operation,
    names: &BTreeSet<SmolStr>,
) -> Result<McpTool, String> {
    if operation.deprecated {
        return Err("operation is deprecated".to_owned());
    }
    let name = match &operation.id {
        Some(operation_id) => sanitize_tool_name(operation_id),
        None => derived_name(method, raw_path),
    };
    if params.namespace.len() + 1 + name.len() > MAX_MCP_TOOL_NAME_BYTES {
        return Err(format!(
            "qualified tool name '{}.{name}' exceeds {MAX_MCP_TOOL_NAME_BYTES} bytes",
            params.namespace
        ));
    }
    if names.contains(name.as_str()) {
        return Err(format!("tool name '{name}' collides with an earlier operation"));
    }
    let mut properties = Map::new();
    let mut required = Vec::new();
    let mut query_params = Vec::new();
    let mut used_schemas = BTreeSet::new();

    let (path_template, mut undeclared_path_params) = convert_path_template(raw_path)?;
    for parameter in merged_parameters(spec, &operation.parameters, &path_item.parameters)? {
        if matches!(parameter.location.as_str(), "header" | "cookie") && !parameter.required {
            warn!(
                "OpenAPI source '{}': dropping optional {} parameter '{}' of '{method} {raw_path}'",
                params.namespace, parameter.location, parameter.name
            );
            continue;
        }
        if !matches!(parameter.location.as_str(), "path" | "query") {
            return Err(format!("unsupported required '{}' parameter '{}'", parameter.location, parameter.name));
        }

        let ident = sanitize_ident(&parameter.name);
        let mut schema = parameter.schema.clone().unwrap_or_else(|| json!({"type": "string"}));
        validate_parameter_serialization(parameter, &schema, &spec.components.schemas)?;
        schema::rewrite(&mut schema, &mut used_schemas)?;
        if let (Some(description), Some(object)) = (&parameter.description, schema.as_object_mut()) {
            object.entry("description").or_insert_with(|| Value::String(description.clone()));
        }
        insert_property(&mut properties, ident.clone(), schema)?;
        if parameter.location == "path" {
            undeclared_path_params.remove(&parameter.name);
            required.push(ident);
        } else {
            if parameter.required {
                required.push(ident.clone());
            }
            query_params.push(McpRestQueryParams { name: parameter.name.as_str().into(), source: ident.into() });
        }
    }
    // Path parameters used by the template but not declared default to strings.
    for parameter in undeclared_path_params {
        let ident = sanitize_ident(&parameter);
        insert_property(&mut properties, ident.clone(), json!({"type": "string"}))?;
        required.push(ident);
    }

    let body_json = match &operation.request_body {
        Some(request_body) => {
            let request_body = request_body.resolve("requestBodies", &spec.components.request_bodies)?;
            let media_type = json_media_type(&request_body.content)
                .ok_or_else(|| "request body has no application/json content".to_owned())?;
            let mut schema = media_type.schema.clone().unwrap_or_else(|| json!({}));
            schema::rewrite(&mut schema, &mut used_schemas)?;
            insert_property(&mut properties, BODY_ARGUMENT.to_owned(), schema)?;
            if request_body.required {
                required.push(BODY_ARGUMENT.to_owned());
            }
            Some(SmolStr::new(BODY_ARGUMENT))
        },
        None => None,
    };

    let mut input_schema = Map::from_iter([("type".to_owned(), json!("object"))]);
    if !properties.is_empty() {
        input_schema.insert("properties".to_owned(), Value::Object(properties));
    }
    if !required.is_empty() {
        input_schema.insert("required".to_owned(), json!(required));
    }
    schema::attach_defs(&mut input_schema, used_schemas, &spec.components.schemas)?;
    schema::validate(&input_schema, "input")?;

    Ok(McpTool {
        namespace: params.namespace,
        name: name.as_str().into(),
        description: operation.summary.as_deref().or(operation.description.as_deref()).unwrap_or(&name).into(),
        input_schema,
        output_schema: output_schema(params, spec, method, raw_path, operation)?,
        backend: UpstreamBackend::Rest {
            method: method.clone(),
            path: format!("{base_path}{path_template}").into(),
            query_params,
            cluster: params.cluster,
            upstream_policy: Box::new(params.upstream_policy.clone()),
            body_template: None,
            body_json,
        },
        rbac: params.rbac.clone(),
        embedding: EmbeddingVector::default(),
        disclosure_mode: params.tool_disclosure_mode,
    })
}

fn validate_parameter_serialization(
    parameter: &Parameter,
    schema: &Value,
    components: &BTreeMap<String, Value>,
) -> Result<(), String> {
    let kind = schema::parameter_kind(schema, components);
    let scalar = kind == Some(schema::ParameterKind::Scalar);
    let style = parameter.style.as_deref();
    let supported = if parameter.location == "path" {
        scalar && style.is_none_or(|style| style == "simple")
    } else {
        style.is_none_or(|style| style == "form") && (scalar || (kind.is_some() && parameter.explode != Some(false)))
    };
    if supported {
        Ok(())
    } else {
        Err(format!(
            "unsupported {} serialization for parameter '{}': expected a scalar path value, or a form/exploded scalar or array query value",
            parameter.location, parameter.name
        ))
    }
}

fn derived_name(method: &http::Method, raw_path: &str) -> String {
    let mut name = method.as_str().to_ascii_lowercase();
    for segment in raw_path.split('/').filter(|segment| !segment.is_empty()) {
        name.push('_');
        name.push_str(&sanitize_tool_name(segment.trim_start_matches('{').trim_end_matches('}')));
    }
    name
}

fn sanitize_tool_name(name: &str) -> String {
    let sanitized: String =
        name.chars().map(|c| if c.is_ascii_alphanumeric() || matches!(c, '_' | '-') { c } else { '_' }).collect();
    if sanitized.is_empty() { "_".to_owned() } else { sanitized }
}

fn convert_path_template(raw_path: &str) -> Result<(String, BTreeSet<String>), String> {
    let mut template = String::new();
    let mut parameters = BTreeSet::new();
    let mut rest = raw_path;
    while let Some((prefix, remainder)) = rest.split_once('{') {
        let (name, tail) = remainder.split_once('}').ok_or_else(|| format!("unbalanced '{{' in path '{raw_path}'"))?;
        if name.is_empty() {
            return Err(format!("empty path parameter in '{raw_path}'"));
        }
        template.push_str(prefix);
        template.push_str("{{");
        template.push_str(&sanitize_ident(name));
        template.push_str(" | encode_path_segment}}");
        parameters.insert(name.to_owned());
        rest = tail;
    }
    if rest.contains('}') {
        return Err(format!("unbalanced '}}' in path '{raw_path}'"));
    }
    template.push_str(rest);
    Ok((template, parameters))
}

fn sanitize_ident(name: &str) -> String {
    let mut sanitized: String =
        name.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect();
    // A leading digit would not lex as a template identifier.
    if sanitized.chars().next().is_none_or(|c| c.is_ascii_digit()) {
        sanitized.insert(0, '_');
    }
    sanitized
}

fn merged_parameters<'a>(
    spec: &'a Spec,
    operation: &'a [RefOr<Parameter>],
    path_item: &'a [RefOr<Parameter>],
) -> Result<Vec<&'a Parameter>, String> {
    let mut merged = BTreeMap::new();
    // Operation parameters override path-item parameters with the same name and location.
    for parameter in path_item.iter().chain(operation) {
        let parameter = parameter.resolve("parameters", &spec.components.parameters)?;
        merged.insert((parameter.name.clone(), parameter.location.clone()), parameter);
    }
    Ok(merged.into_values().collect())
}

fn insert_property(properties: &mut Map<String, Value>, name: String, schema: Value) -> Result<(), String> {
    if properties.contains_key(&name) {
        return Err(format!("argument name '{name}' collides with another parameter"));
    }
    properties.insert(name, schema);
    Ok(())
}

fn output_schema(
    params: &OpenApiExpansionParams,
    spec: &Spec,
    method: &http::Method,
    raw_path: &str,
    operation: &Operation,
) -> Result<Map<String, Value>, String> {
    let mut selected = None;
    for status in ["200", "201", "2XX", "default"] {
        let Some(response) = operation.responses.get(status) else { continue };
        let response = response.resolve("responses", &spec.components.responses)?;
        if let Some(schema) = json_media_type(&response.content).and_then(|media_type| media_type.schema.as_ref()) {
            selected = Some(schema);
            break;
        }
    }
    let Some(schema) = selected else {
        return Ok(Map::new());
    };
    if !schema::resolves_to_object(schema, &spec.components.schemas) {
        warn!(
            "OpenAPI source '{}': response schema of '{method} {raw_path}' is not an object; returning text",
            params.namespace
        );
        return Ok(Map::new());
    }
    let mut schema = schema.clone();
    let mut used_schemas = BTreeSet::new();
    schema::rewrite(&mut schema, &mut used_schemas)?;
    let Value::Object(mut schema) = schema else {
        return Ok(Map::new());
    };
    schema::attach_defs(&mut schema, used_schemas, &spec.components.schemas)?;
    schema::validate(&schema, "output")?;
    Ok(schema)
}

fn json_media_type(content: &BTreeMap<String, MediaType>) -> Option<&MediaType> {
    content
        .iter()
        .find(|(key, _)| *key == "application/json" || key.starts_with("application/json;"))
        .map(|(_, media_type)| media_type)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::indexing_slicing)]
    use super::*;

    const PETSTORE: &str = r##"
openapi: 3.0.3
servers:
  - url: https://petstore.example.com/api/v3
paths:
  /pets:
    get:
      operationId: listPets
      summary: List all pets
      parameters:
        - name: limit
          in: query
          required: false
          schema: { type: integer }
        - name: X-Trace
          in: header
          required: false
          schema: { type: string }
      responses:
        "200":
          content:
            application/json:
              schema: { $ref: "#/components/schemas/PetPage" }
    post:
      operationId: createPet
      description: Create a pet
      requestBody:
        required: true
        content:
          application/json:
            schema: { $ref: "#/components/schemas/Pet" }
      responses:
        "201":
          content:
            application/json:
              schema: { $ref: "#/components/schemas/Pet" }
  /pets/{pet-id}:
    parameters:
      - name: pet-id
        in: path
        required: true
        schema: { type: string }
    get:
      summary: Get one pet
      responses:
        "200":
          content:
            application/json:
              schema: { type: array, items: { type: string } }
components:
  schemas:
    Pet:
      type: object
      nullable: true
      properties:
        name: { type: string }
        friend: { $ref: "#/components/schemas/Pet" }
        tag: { $ref: "#/components/schemas/Tag" }
    Tag:
      type: object
      properties:
        label: { type: string }
    PetPage:
      type: object
      properties:
        items:
          type: array
          items: { $ref: "#/components/schemas/Pet" }
"##;

    fn params() -> OpenApiExpansionParams {
        OpenApiExpansionParams {
            namespace: "petstore".into(),
            cluster: "backend".into(),
            upstream_policy: HttpUpstreamPolicy::default(),
            rbac: None,
            tool_disclosure_mode: DisclosureMode::Eager,
            path_prefix: None,
            skip_unsupported_operations: false,
            include_operations: Vec::new(),
            exclude_operations: Vec::new(),
        }
    }

    fn expand(params: &OpenApiExpansionParams) -> Vec<McpTool> {
        openapi_to_tools(PETSTORE.as_bytes(), params).unwrap()
    }

    fn tool<'a>(tools: &'a [McpTool], name: &str) -> &'a McpTool {
        tools.iter().find(|tool| tool.name == name).unwrap_or_else(|| panic!("missing tool '{name}'"))
    }

    fn rest_backend(tool: &McpTool) -> (&http::Method, &str, &[McpRestQueryParams], Option<&str>) {
        match &tool.backend {
            UpstreamBackend::Rest { method, path, query_params, body_json, .. } => {
                (method, path, query_params, body_json.as_deref())
            },
            other => panic!("expected REST backend, got {other:?}"),
        }
    }

    #[test]
    fn expands_paths_query_params_and_base_path() {
        let tools = expand(&params());
        assert_eq!(tools.len(), 3);

        let list = tool(&tools, "listPets");
        let (method, path, query_params, body_json) = rest_backend(list);
        assert_eq!(method, http::Method::GET);
        assert_eq!(path, "/api/v3/pets");
        assert_eq!(query_params, [McpRestQueryParams { name: "limit".into(), source: "limit".into() }]);
        assert_eq!(body_json, None);
        assert_eq!(list.description, "List all pets");
        assert_eq!(list.disclosure_mode, DisclosureMode::Eager);
        let properties = list.input_schema["properties"].as_object().unwrap();
        assert_eq!(properties["limit"]["type"], "integer");
        assert!(!properties.contains_key("X-Trace"), "optional header parameters are dropped");
        assert!(!list.input_schema.contains_key("required"));
    }

    #[test]
    fn path_parameters_are_sanitized_required_strings() {
        let tools = expand(&params());
        let get = tool(&tools, "get_pets_pet-id");
        let (_, path, _, _) = rest_backend(get);
        assert_eq!(path, "/api/v3/pets/{{pet_id | encode_path_segment}}");
        assert_eq!(get.input_schema["required"], json!(["pet_id"]));
        assert_eq!(get.input_schema["properties"]["pet_id"]["type"], "string");
    }

    #[test]
    fn query_arrays_are_supported_but_objects_and_custom_styles_are_not() {
        const ARRAY: &str = r#"
paths:
  /pets:
    get:
      operationId: findPets
      parameters:
        - { name: tag, in: query, schema: { type: array, items: { type: string } } }
"#;
        let tools = openapi_to_tools(ARRAY.as_bytes(), &params()).unwrap();
        assert_eq!(tools[0].input_schema["properties"]["tag"]["type"], "array");

        for unsupported in [
            "schema: { type: object, properties: { name: { type: string } } }",
            "style: spaceDelimited, schema: { type: array, items: { type: string } }",
            "explode: false, schema: { type: array, items: { type: string } }",
        ] {
            let spec = ARRAY.replace("schema: { type: array, items: { type: string } }", unsupported);
            let error = openapi_to_tools(spec.as_bytes(), &params()).unwrap_err();
            assert!(error.to_string().contains("unsupported query serialization"), "{error}");
        }
    }

    #[test]
    fn request_bodies_map_to_a_body_json_argument() {
        let tools = expand(&params());
        let create = tool(&tools, "createPet");
        let (method, _, _, body_json) = rest_backend(create);
        assert_eq!(method, http::Method::POST);
        assert_eq!(body_json, Some("body"));
        assert_eq!(create.input_schema["required"], json!(["body"]));
        assert_eq!(create.input_schema["properties"]["body"]["$ref"], "#/$defs/Pet");
    }

    #[test]
    fn referenced_schemas_become_a_self_contained_defs_closure() {
        let tools = expand(&params());
        let create = tool(&tools, "createPet");
        let defs = create.input_schema["$defs"].as_object().unwrap();
        assert!(defs.contains_key("Pet"), "directly referenced schema");
        assert!(defs.contains_key("Tag"), "transitively referenced schema");
        assert_eq!(defs["Pet"]["properties"]["friend"]["$ref"], "#/$defs/Pet", "cyclic references are rewritten");
        assert_eq!(defs["Pet"]["type"], json!(["object", "null"]), "nullable becomes a type union");
        assert!(!defs["Pet"].as_object().unwrap().contains_key("nullable"));
    }

    #[test]
    fn object_responses_become_output_schemas_and_others_fall_back_to_text() {
        let tools = expand(&params());
        let list = tool(&tools, "listPets");
        assert_eq!(list.output_schema["$ref"], "#/$defs/PetPage", "ref to an object schema is structured output");
        assert!(list.output_schema["$defs"].as_object().unwrap().contains_key("Pet"));
        let get = tool(&tools, "get_pets_pet-id");
        assert!(get.output_schema.is_empty(), "array responses fall back to text passthrough");
    }

    #[test]
    fn include_and_exclude_filters_select_operations() {
        let mut selected = params();
        selected.include_operations = vec!["*Pet*".to_owned()];
        let names: Vec<_> = expand(&selected).into_iter().map(|tool| tool.name).collect();
        assert_eq!(names, ["listPets", "createPet"]);

        let mut excluded = params();
        excluded.exclude_operations = vec!["GET /pets*".to_owned()];
        let names: Vec<_> = expand(&excluded).into_iter().map(|tool| tool.name).collect();
        assert_eq!(names, ["createPet"], "exclude matches the METHOD /path form");
    }

    #[test]
    fn unsupported_operations_fail_strict_and_skip_without_reserving_their_name() {
        const SPEC: &str = r#"
paths:
  /bad:
    get:
      operationId: shared
      parameters:
        - { name: X-Key, in: header, required: true, schema: { type: string } }
  /good:
    get: { operationId: shared }
"#;
        let strict = openapi_to_tools(SPEC.as_bytes(), &params()).unwrap_err();
        assert!(strict.to_string().contains("skip_unsupported_operations"), "{strict}");

        let mut skipping = params();
        skipping.skip_unsupported_operations = true;
        let tools = openapi_to_tools(SPEC.as_bytes(), &skipping).unwrap();
        assert_eq!(tools.len(), 1, "a skipped operation must not reserve its tool name");
        assert_eq!(tools[0].name, "shared");
    }

    #[test]
    fn ignored_optional_parameters_do_not_process_their_schema() {
        const SPEC: &str = r#"
paths:
  /pets:
    get:
      operationId: listPets
      parameters:
        - { name: X-Trace, in: header, schema: { $ref: "external.yaml#/Trace" } }
"#;
        let tools = openapi_to_tools(SPEC.as_bytes(), &params()).unwrap();
        assert_eq!(tools.len(), 1);
        assert!(!tools[0].input_schema.contains_key("properties"));
    }

    #[test]
    fn deprecated_operations_and_name_collisions_are_unsupported() {
        const SPEC: &str = r#"
paths:
  /old:
    get: { operationId: "get pets", deprecated: true }
  /pets:
    get: { operationId: "get pets" }
  /pets2:
    get: { operationId: "get.pets" }
"#;
        let mut skipping = params();
        skipping.skip_unsupported_operations = true;
        let tools = openapi_to_tools(SPEC.as_bytes(), &skipping).unwrap();
        assert_eq!(tools.len(), 1, "the sanitized duplicate 'get_pets' is skipped");
        assert_eq!(tools[0].name, "get_pets");

        openapi_to_tools(SPEC.as_bytes(), &params()).unwrap_err();
    }

    #[test]
    fn colliding_parameter_idents_are_rejected() {
        const SPEC: &str = r#"
paths:
  /a:
    get:
      operationId: clash
      parameters:
        - { name: "user id", in: query, schema: { type: string } }
        - { name: "user-id", in: query, schema: { type: string } }
"#;
        let error = openapi_to_tools(SPEC.as_bytes(), &params()).unwrap_err();
        assert!(error.to_string().contains("collides"), "{error}");
    }

    #[test]
    fn path_prefix_overrides_the_server_url() {
        let mut params = params();
        params.path_prefix = Some("/proxy/".to_owned());
        let tools = expand(&params);
        let (_, path, _, _) = rest_backend(tool(&tools, "listPets"));
        assert_eq!(path, "/proxy/pets");
    }

    #[test]
    fn external_refs_are_unsupported() {
        const SPEC: &str = r#"
paths:
  /a:
    post:
      operationId: external
      requestBody:
        content:
          application/json:
            schema: { $ref: "other.yaml#/Pet" }
"#;
        let error = openapi_to_tools(SPEC.as_bytes(), &params()).unwrap_err();
        assert!(error.to_string().contains("unsupported $ref"), "{error}");
    }

    #[test]
    fn component_parameter_and_request_body_reference_chains_are_resolved() {
        const SPEC: &str = r##"
paths:
  /pets:
    post:
      operationId: createPet
      parameters:
        - { $ref: "#/components/parameters/LimitAlias" }
      requestBody: { $ref: "#/components/requestBodies/PetBodyAlias" }
components:
  parameters:
    LimitAlias: { $ref: "#/components/parameters/Limit" }
    Limit: { name: limit, in: query, schema: { type: integer } }
  requestBodies:
    PetBodyAlias: { $ref: "#/components/requestBodies/PetBody" }
    PetBody:
      required: true
      content:
        application/json:
          schema: { type: object, properties: { name: { type: string } } }
"##;
        let tools = openapi_to_tools(SPEC.as_bytes(), &params()).unwrap();
        let schema = &tools[0].input_schema;
        assert_eq!(schema["properties"]["limit"]["type"], "integer");
        assert_eq!(schema["properties"]["body"]["type"], "object");
        assert_eq!(schema["required"], json!(["body"]));
    }

    #[test]
    fn cyclic_component_references_are_rejected() {
        const SPEC: &str = r##"
paths:
  /pets:
    get:
      operationId: listPets
      parameters:
        - { $ref: "#/components/parameters/A" }
components:
  parameters:
    A: { $ref: "#/components/parameters/B" }
    B: { $ref: "#/components/parameters/A" }
"##;
        let error = openapi_to_tools(SPEC.as_bytes(), &params()).unwrap_err();
        assert!(error.to_string().contains("cyclic $ref"), "{error}");
    }

    #[test]
    fn glob_match_supports_star_and_question_mark() {
        assert!(glob_match("get_*", "get_users"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("GET /users/*", "GET /users/{id}"));
        assert!(glob_match("get_user?", "get_users"));
        assert!(!glob_match("get_*", "post_users"));
        assert!(!glob_match("get_user?", "get_user"));
        // A '*' wildcard still matches text containing a literal '*'.
        assert!(glob_match("a*b", "a*xb"));
        assert!(glob_match("GET /a*", "GET /a*b"));
    }

    #[test]
    fn data_positions_are_not_treated_as_schemas() {
        const SPEC: &str = r##"
paths:
  /a:
    post:
      operationId: create
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                nullable: true
                mode:
                  type: string
                  enum: [{ "$ref": "#/components/schemas/Pet" }]
                  default: { "$ref": "not a reference" }
components:
  schemas:
    Pet: { type: object }
"##;
        let tools = openapi_to_tools(SPEC.as_bytes(), &params()).unwrap();
        let body = &tools[0].input_schema["properties"]["body"];
        let properties = body["properties"].as_object().unwrap();
        assert_eq!(properties["nullable"], Value::Bool(true), "a property named 'nullable' must survive");
        assert_eq!(properties["mode"]["enum"][0]["$ref"], "#/components/schemas/Pet", "enum data is not rewritten");
        assert_eq!(properties["mode"]["default"]["$ref"], "not a reference", "default data is not a reference");
        assert!(!body.as_object().unwrap().contains_key("$defs"), "data refs must not pull schemas into $defs");
    }

    #[test]
    fn non_openapi_documents_are_rejected() {
        for (document, expected) in [
            ("{not yaml: [", "Failed to parse OpenAPI document"),
            ("swagger: '2.0'", "Swagger 2.0"),
            ("openapi: 2.0.0", "Unsupported OpenAPI version"),
            ("some_key: value", "Not an OpenAPI 3.x document"),
        ] {
            let error = openapi_to_tools(document.as_bytes(), &params()).unwrap_err();
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn output_schema_falls_through_to_the_first_json_response() {
        const SPEC: &str = r#"
paths:
  /a:
    get:
      operationId: get_a
      responses:
        "200":
          content:
            text/plain:
              schema: { type: string }
        default:
          content:
            application/json:
              schema: { type: object, properties: { ok: { type: boolean } } }
"#;
        let tools = openapi_to_tools(SPEC.as_bytes(), &params()).unwrap();
        assert_eq!(tools[0].output_schema["properties"]["ok"]["type"], "boolean");
    }

    #[test]
    fn colliding_path_parameter_names_are_rejected() {
        const SPEC: &str = r#"
paths:
  /x/{a-b}/{a_b}:
    get: { operationId: clash }
"#;
        let error = openapi_to_tools(SPEC.as_bytes(), &params()).unwrap_err();
        assert!(error.to_string().contains("collides"), "{error}");
    }

    #[test]
    fn path_item_refs_are_unsupported_and_options_operations_are_expanded() {
        const SPEC: &str = r#"
paths:
  /o:
    options: { operationId: opt_o }
  /r:
    $ref: "other.yaml#/paths/foo"
"#;
        let strict = openapi_to_tools(SPEC.as_bytes(), &params()).unwrap_err();
        assert!(strict.to_string().contains("path item $ref"), "{strict}");

        let mut skipping = params();
        skipping.skip_unsupported_operations = true;
        let tools = openapi_to_tools(SPEC.as_bytes(), &skipping).unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0].name, "opt_o");
        assert_eq!(rest_backend(&tools[0]).0, http::Method::OPTIONS);
    }
}
