// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
//
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use opentelemetry::{global::BoxedSpan, trace::Span};
use parking_lot::Mutex;

#[derive(Debug)]
pub struct SpanState {
    pub server_span: Mutex<Option<BoxedSpan>>, // SERVER span
    pub client_span: Mutex<Option<BoxedSpan>>, // CLIENT span
}

impl SpanState {
    #[inline]
    pub fn new(server_span: Option<BoxedSpan>) -> Self {
        SpanState { server_span: Mutex::new(server_span), client_span: Mutex::new(None) }
    }

    pub fn end(&self) {
        // emit the server span if created...
        let mut guard = self.server_span.lock();
        if let Some(ref mut span) = *guard {
            span.end();
        }

        // emit the client span if created...
        let mut guard = self.client_span.lock();
        if let Some(ref mut span) = *guard {
            span.end();
        }
    }
}
