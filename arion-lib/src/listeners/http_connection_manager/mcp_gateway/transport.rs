// Copyright 2025-2026 The arion-gateway Authors
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

use std::borrow::Borrow;

use arion_http_header::MCP_SESSION_ID;
use smol_str::SmolStr;

pub const MIME_TEXT_EVENT_STREAM: &str = "text/event-stream";
pub const MIME_APPLICATION_JSON: &str = "application/json";

#[derive(Debug, Clone, Default, Eq, PartialEq, Hash)]
pub struct SessionId(pub SmolStr);

impl SessionId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Borrow<str> for SessionId {
    #[inline]
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

pub trait RequestExt {
    fn get_mcp_session_id(&self) -> Option<&str>;
    fn get_mcp_accepted_mime(&self) -> Option<AcceptedMime>;
}

pub enum AcceptedMime {
    EventStream,
    ApplicationJson,
    EventStreamAndJson,
}

impl AcceptedMime {
    #[allow(dead_code)]
    pub fn is_app_json(&self) -> bool {
        match self {
            AcceptedMime::EventStream => false,
            AcceptedMime::ApplicationJson | AcceptedMime::EventStreamAndJson => true,
        }
    }
    #[allow(dead_code)]
    pub fn is_event_stream(&self) -> bool {
        match self {
            AcceptedMime::ApplicationJson => false,
            AcceptedMime::EventStreamAndJson | AcceptedMime::EventStream => true,
        }
    }
}

impl TryFrom<&str> for AcceptedMime {
    type Error = ();
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        let has_event_stream = s.contains(MIME_TEXT_EVENT_STREAM);
        let has_json = s.contains(MIME_APPLICATION_JSON);
        if has_event_stream && has_json {
            Ok(AcceptedMime::EventStreamAndJson)
        } else if has_event_stream {
            Ok(AcceptedMime::EventStream)
        } else if has_json {
            Ok(AcceptedMime::ApplicationJson)
        } else {
            Err(())
        }
    }
}

impl<B> RequestExt for http::Request<B> {
    #[inline]
    fn get_mcp_session_id(&self) -> Option<&str> {
        self.headers().get(MCP_SESSION_ID)?.to_str().ok()
    }

    fn get_mcp_accepted_mime(&self) -> Option<AcceptedMime> {
        if let Some(accept_value) = self.headers().get(http::header::ACCEPT) {
            let accept = accept_value.to_str().ok()?;
            return AcceptedMime::try_from(accept).ok();
        }
        None
    }
}
