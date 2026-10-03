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

use std::cell::OnceCell;

use rquickjs::{Context, Exception, Function, Promise, Runtime};

wit_bindgen::generate!({
    path: "wit",
    world: "sandbox",
});

use exports::arion::codemode::runner::{ExecutionError, Guest};
use arion::codemode::host;

const QUICKJS_HEAP_BYTES: usize = 32 * 1024 * 1024;
const QUICKJS_STACK_BYTES: usize = 1024 * 1024;

struct QuickJs {
    _runtime: Runtime,
    context: Context,
}

thread_local! {
    static QUICKJS: OnceCell<QuickJs> = const { OnceCell::new() };
}

/// Create the QuickJS runtime before Wizer snapshots the module.
#[export_name = "wizer.initialize"]
pub extern "C" fn initialize() {
    let runtime = Runtime::new().expect("create QuickJS runtime");
    runtime.set_memory_limit(QUICKJS_HEAP_BYTES);
    runtime.set_max_stack_size(QUICKJS_STACK_BYTES);
    let context = Context::full(&runtime).expect("create QuickJS context");

    QUICKJS.with(|slot| {
        assert!(slot.set(QuickJs { _runtime: runtime, context }).is_ok(), "QuickJS is already initialized");
    });
}

struct Sandbox;

impl Guest for Sandbox {
    fn execute(script: String) -> Result<String, ExecutionError> {
        QUICKJS.with(|slot| {
            let quickjs = slot.get().ok_or_else(|| ExecutionError::Fatal("QuickJS is not initialized".to_owned()))?;

            let source = format!(
                "(async () => {{\n{script}\n}})().then(\
                    value => JSON.stringify(value === undefined ? null : value)\
                );"
            );

            quickjs.context.with(|ctx| {
                let evaluated = (|| -> rquickjs::Result<Result<String, ExecutionError>> {
                    let request = Function::new(ctx.clone(), |kind: String, payload: String| {
                        host::request(&kind, &payload)
                            .map_err(|message| rquickjs::Error::new_from_js_message("host", "request", message))
                    })?;
                    ctx.globals().set("__arion_host_request", request)?;
                    let submit = Function::new(ctx.clone(), |kind: String, payload: String| {
                        host::submit(&kind, &payload)
                            .map_err(|message| rquickjs::Error::new_from_js_message("host", "submit", message))
                    })?;
                    ctx.globals().set("__arion_host_submit", submit)?;

                    // The SDK evaluates to its completion-delivery function,
                    // which exists only on this Rust stack and can never be
                    // reached or forged by user code.
                    let deliver = ctx.eval::<Function<'_>, _>(SDK_SOURCE)?;
                    let promise = ctx.eval::<Promise<'_>, _>(source)?;

                    loop {
                        while ctx.execute_pending_job() {}
                        if let Some(settled) = promise.result::<String>() {
                            // The host cancels submissions still in flight.
                            return settled.map(Ok);
                        }
                        match host::next_completion() {
                            Ok(Some((id, outcome))) => {
                                let (ok, payload) = match outcome {
                                    Ok(payload) => (true, payload),
                                    Err(message) => (false, message),
                                };
                                deliver.call::<_, ()>((id, ok, payload))?;
                            },
                            Ok(None) => {
                                return Ok(Err(ExecutionError::Javascript(
                                    "execution stalled: the returned promise never settled and no host calls are pending"
                                        .to_owned(),
                                )));
                            },
                            Err(message) => {
                                return Ok(Err(ExecutionError::Fatal(format!("host completion failed: {message}"))));
                            },
                        }
                    }
                })();

                match evaluated {
                    Ok(done) => done,
                    Err(error) if error.is_exception() => {
                        let caught = ctx.catch();
                        let message = caught
                            .as_object()
                            .and_then(|object| Exception::from_object(object.clone()))
                            .and_then(|exception| match (exception.message(), exception.stack()) {
                                (Some(message), Some(stack)) => Some(format!("{message}\n{stack}")),
                                (Some(message), None) => Some(message),
                                (None, Some(stack)) => Some(stack),
                                (None, None) => None,
                            })
                            .unwrap_or_else(|| "JavaScript execution failed".to_owned());
                        Err(ExecutionError::Javascript(message))
                    },
                    Err(_) => Err(ExecutionError::Javascript("JavaScript execution failed".to_owned())),
                }
            })
        })
    }
}

const SDK_SOURCE: &str = r#"
(() => {
  "use strict";
  const pending = new Map();
  const submit = (kind, payload) => {
    const id = __arion_host_submit(kind, JSON.stringify(payload));
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
    });
  };
  const enumerationError = () => {
    throw new Error(
      "tools cannot be enumerated; use catalog.tools.search, catalog.tools.list, or catalog.toolkits.list"
    );
  };

  const callable = (path) => {
    const target = function () {};
    return new Proxy(target, {
      get(_target, property) {
        if (typeof property === "symbol") return undefined;
        if (property === "then") return undefined;
        return callable(path.concat(String(property)));
      },
      apply(_target, _this, args) {
        const qualifiedName = path.join(".");
        return submit("tool_call", {
          qualifiedName,
          arguments: args.length === 0 ? {} : args[0]
        });
      },
      ownKeys: enumerationError,
      set() { throw new Error("tools is read-only"); },
      deleteProperty() { throw new Error("tools is read-only"); }
    });
  };

  const method = (operation) => Object.freeze((parameters = {}) =>
    submit("catalog", { ...parameters, operation })
  );
  const catalog = Object.freeze({
    namespaces: Object.freeze({ list: method("namespaces_list") }),
    tools: Object.freeze({
      search: method("tools_search"),
      list: method("tools_list"),
      describe: method("tools_describe")
    }),
    toolkits: Object.freeze({
      list: method("toolkits_list"),
      get: method("toolkits_get")
    })
  });

  const consoleText = (value) => {
    if (typeof value === "string") return value;
    try {
      const json = JSON.stringify(value);
      return json === undefined ? String(value) : json;
    } catch {
      return String(value);
    }
  };
  const log = Object.freeze((...args) => {
    __arion_host_request("log", args.map(consoleText).join(" "));
  });
  const console = Object.freeze({ log, info: log, warn: log, error: log, debug: log });

  Object.defineProperties(globalThis, {
    tools: { value: callable([]), writable: false, configurable: false, enumerable: true },
    catalog: { value: catalog, writable: false, configurable: false, enumerable: true },
    console: { value: console, writable: false, configurable: false, enumerable: true }
  });

  return (id, ok, payload) => {
    const entry = pending.get(id);
    if (entry === undefined) return;
    pending.delete(id);
    if (!ok) {
      entry.reject(new Error(payload));
      return;
    }
    let value;
    try {
      value = JSON.parse(payload);
    } catch (error) {
      entry.reject(error);
      return;
    }
    entry.resolve(value);
  };
})();
"#;

export!(Sandbox);
