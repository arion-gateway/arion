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

use rquickjs::{Context, Exception, Runtime};

wit_bindgen::generate!({
    path: "wit",
    world: "sandbox",
});

use exports::arion::codemode::runner::{ExecutionError, Guest};

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
            let quickjs = slot
                .get()
                .ok_or_else(|| ExecutionError::Fatal("QuickJS is not initialized".to_owned()))?;

            let source = format!(
                "const __arion_value = (\n{script}\n);\nJSON.stringify(__arion_value === undefined ? null : __arion_value);"
            );

            quickjs.context.with(|ctx| match ctx.eval::<String, _>(source) {
                Ok(json) => Ok(json),
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
            })
        })
    }
}

export!(Sandbox);
