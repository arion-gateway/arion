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

use serde::Deserialize;
use serde_json::Value;

// Only the subset needed for expansion is modelled. Unknown fields are
// ignored and schemas stay as raw JSON, so 3.0 and 3.1 documents both parse.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct Spec {
    pub(super) openapi: Option<String>,
    pub(super) swagger: Option<String>,
    pub(super) servers: Vec<Server>,
    pub(super) paths: BTreeMap<String, PathItem>,
    pub(super) components: Components,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct Server {
    pub(super) url: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct PathItem {
    #[serde(rename = "$ref")]
    pub(super) reference: Option<String>,
    pub(super) parameters: Vec<RefOr<Parameter>>,
    get: Option<Operation>,
    put: Option<Operation>,
    post: Option<Operation>,
    delete: Option<Operation>,
    patch: Option<Operation>,
    head: Option<Operation>,
    options: Option<Operation>,
    trace: Option<Operation>,
}

impl PathItem {
    pub(super) fn operations(&self) -> impl Iterator<Item = (http::Method, &Operation)> {
        [
            (http::Method::GET, &self.get),
            (http::Method::PUT, &self.put),
            (http::Method::POST, &self.post),
            (http::Method::DELETE, &self.delete),
            (http::Method::PATCH, &self.patch),
            (http::Method::HEAD, &self.head),
            (http::Method::OPTIONS, &self.options),
            (http::Method::TRACE, &self.trace),
        ]
        .into_iter()
        .filter_map(|(method, operation)| Some((method, operation.as_ref()?)))
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(super) struct Operation {
    #[serde(rename = "operationId")]
    pub(super) id: Option<String>,
    pub(super) summary: Option<String>,
    pub(super) description: Option<String>,
    pub(super) deprecated: bool,
    pub(super) parameters: Vec<RefOr<Parameter>>,
    pub(super) request_body: Option<RefOr<RequestBody>>,
    pub(super) responses: BTreeMap<String, RefOr<Response>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct Parameter {
    pub(super) name: String,
    #[serde(rename = "in")]
    pub(super) location: String,
    pub(super) required: bool,
    pub(super) style: Option<String>,
    pub(super) explode: Option<bool>,
    pub(super) description: Option<String>,
    pub(super) schema: Option<Value>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct RequestBody {
    pub(super) required: bool,
    pub(super) content: BTreeMap<String, MediaType>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct MediaType {
    pub(super) schema: Option<Value>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
pub(super) struct Response {
    pub(super) content: BTreeMap<String, MediaType>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub(super) struct Components {
    pub(super) schemas: BTreeMap<String, Value>,
    pub(super) parameters: BTreeMap<String, RefOr<Parameter>>,
    pub(super) request_bodies: BTreeMap<String, RefOr<RequestBody>>,
    pub(super) responses: BTreeMap<String, RefOr<Response>>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(super) enum RefOr<T> {
    Ref {
        #[serde(rename = "$ref")]
        reference: String,
    },
    Item(T),
}

impl<T> RefOr<T> {
    pub(super) fn resolve<'a>(
        &'a self,
        section: &str,
        components: &'a BTreeMap<String, RefOr<T>>,
    ) -> Result<&'a T, String> {
        let mut current = self;
        let mut visited = BTreeSet::new();
        loop {
            match current {
                Self::Item(item) => return Ok(item),
                Self::Ref { reference } => {
                    let name = reference
                        .strip_prefix("#/components/")
                        .and_then(|rest| rest.strip_prefix(section))
                        .and_then(|rest| rest.strip_prefix('/'))
                        .ok_or_else(|| format!("unsupported $ref '{reference}'"))?;
                    if !visited.insert(name) {
                        return Err(format!("cyclic $ref '{reference}'"));
                    }
                    current = components.get(name).ok_or_else(|| format!("unresolved $ref '{reference}'"))?;
                },
            }
        }
    }
}
