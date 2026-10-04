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

use std::sync::{Arc as StdArc, Mutex};

use swc_common::{
    errors::{DiagnosticBuilder, Emitter, Handler, HANDLER},
    sync::Lrc,
    Globals, SourceMap, Span, GLOBALS,
};
use swc_fast_ts_strip::{operate, ErrorCode, Mode, Options, TsError};

use super::error::CodeModeError;

pub(super) fn strip_types(source: String) -> Result<String, CodeModeError> {
    let source_map: Lrc<SourceMap> = Lrc::default();
    let captured = StdArc::new(Mutex::new(None));
    let handler =
        Handler::with_emitter(true, false, Box::new(DiagnosticEmitter { captured: StdArc::clone(&captured) }));
    let options = Options { mode: Mode::StripOnly, ..Default::default() };

    let result =
        GLOBALS.set(&Globals::new(), || HANDLER.set(&handler, || operate(&source_map, &handler, source, options)));

    result.map(|output| output.code).map_err(|error| {
        let diagnostic = captured.lock().unwrap_or_else(std::sync::PoisonError::into_inner).take();
        map_error(error, diagnostic, &source_map)
    })
}

pub(super) fn strip_function_body(source: &str) -> Result<String, CodeModeError> {
    const PREFIX: &str = "async function __arion_user() {\n";
    const SUFFIX: &str = "\n}";

    let wrapped = format!("{PREFIX}{source}{SUFFIX}");
    match strip_types(wrapped) {
        Ok(stripped) => stripped
            .get(PREFIX.len()..stripped.len().saturating_sub(SUFFIX.len()))
            .map(str::to_owned)
            .ok_or(CodeModeError::TypeStrip),
        Err(CodeModeError::Syntax { message, line, col }) => {
            Err(CodeModeError::Syntax { message, line: line.saturating_sub(1).max(1), col })
        },
        Err(CodeModeError::UnsupportedSyntax { message, line, col }) => {
            Err(CodeModeError::UnsupportedSyntax { message, line: line.saturating_sub(1).max(1), col })
        },
        Err(error) => Err(error),
    }
}

#[derive(Clone)]
struct CapturedDiagnostic {
    message: String,
    span: Option<Span>,
}

struct DiagnosticEmitter {
    captured: StdArc<Mutex<Option<CapturedDiagnostic>>>,
}

impl Emitter for DiagnosticEmitter {
    fn emit(&mut self, db: &mut DiagnosticBuilder<'_>) {
        let mut captured = self.captured.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        if captured.is_none() && db.is_error() {
            *captured = Some(CapturedDiagnostic { message: db.message(), span: db.span.primary_span() });
        }
    }
}

fn map_error(error: TsError, diagnostic: Option<CapturedDiagnostic>, source_map: &SourceMap) -> CodeModeError {
    let message = diagnostic.as_ref().map_or(error.message, |diagnostic| diagnostic.message.clone());
    let location = diagnostic.and_then(|diagnostic| diagnostic.span).and_then(|span| {
        source_map.try_lookup_char_pos(span.lo).ok().map(|location| {
            let is_decorator =
                location.file.src.lines().nth(location.line - 1).and_then(|line| line.chars().nth(location.col.0))
                    == Some('@');
            (location.line, location.col_display + 1, is_decorator)
        })
    });
    let (line, col, is_decorator) = location.unwrap_or((1, 1, false));

    match error.code {
        ErrorCode::InvalidSyntax if is_decorator => CodeModeError::UnsupportedSyntax { message, line, col },
        ErrorCode::InvalidSyntax => CodeModeError::Syntax { message, line, col },
        ErrorCode::UnsupportedSyntax => CodeModeError::UnsupportedSyntax { message, line, col },
        _ => CodeModeError::TypeStrip,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_erasable_types_without_moving_source_positions() {
        let source = "const greeting: string = 'hello';\nconst answer = (value: number): number => value + 1;";

        let stripped = strip_types(source.to_owned()).expect("erasable TypeScript");

        assert_eq!(stripped.len(), source.len());
        assert_eq!(
            stripped.match_indices('\n').map(|(index, _)| index).collect::<Vec<_>>(),
            source.match_indices('\n').map(|(index, _)| index).collect::<Vec<_>>()
        );
        assert!(stripped.contains("const greeting         = 'hello';"));
        assert!(stripped.contains("=> value + 1"));
        assert!(!stripped.contains(": number"));
    }

    #[test]
    fn reports_invalid_syntax_with_its_source_location() {
        let error = strip_types("const valid = 1;\nconst broken: = 2;".to_owned()).expect_err("invalid TypeScript");

        assert!(
            matches!(error, CodeModeError::Syntax { line: 2, col, ref message } if col > 1 && !message.is_empty()),
            "{error:?}"
        );
    }

    #[test]
    fn reports_unsupported_syntax_with_its_source_location() {
        for (source, expected_line, expected_message) in [
            (
                "const valid = 1;\nenum Direction { Up, Down }",
                2,
                Some("TypeScript enum is not supported in strip-only mode"),
            ),
            (
                "const valid = 1;\nnamespace Values { export const answer = 42; }",
                2,
                Some("TypeScript namespace declaration is not supported in strip-only mode"),
            ),
            ("@sealed\nclass Example {}", 1, None),
        ] {
            let error = strip_types(source.to_owned()).expect_err("unsupported TypeScript");
            let CodeModeError::UnsupportedSyntax { message, line, col } = error else {
                panic!("{error:?}");
            };

            assert_eq!((line, col), (expected_line, 1));
            if let Some(expected_message) = expected_message {
                assert_eq!(message, expected_message);
            }
        }
    }
}
