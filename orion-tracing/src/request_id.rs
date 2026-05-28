// Copyright 2025 The kmesh Authors
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
//

use http::{HeaderValue, Request, Response};
use orion_http_header::X_REQUEST_ID;
use tracing::info;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct RequestIdManager {
    generate_request_id: bool,
    preserve_external_request_id: bool,
    always_set_request_id_in_response: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestId {
    Propagate(HeaderValue),
    Internal(HeaderValue),
}

impl AsRef<HeaderValue> for RequestId {
    fn as_ref(&self) -> &HeaderValue {
        match self {
            RequestId::Propagate(id) | RequestId::Internal(id) => id,
        }
    }
}

impl RequestId {
    pub fn from_request<B>(request: &Request<B>) -> Option<Self> {
        let value = request.headers().get(X_REQUEST_ID)?;
        if value.is_empty() {
            return None;
        }
        if let Ok(s) = value.to_str() {
            if Uuid::parse_str(s).is_err() {
                info!("Non-UUID value in X-Request-ID header: {}", s);
            }
        }
        Some(RequestId::Propagate(value.clone()))
    }

    pub fn to_value(&self) -> HeaderValue {
        match self {
            RequestId::Propagate(id) => (*id).clone(),
            RequestId::Internal(id) => (*id).clone(),
        }
    }

    pub fn propagate_ref(&self) -> Option<&HeaderValue> {
        match self {
            RequestId::Propagate(id) => Some(id),
            RequestId::Internal(_) => None,
        }
    }

    pub fn internal_ref(&self) -> Option<&HeaderValue> {
        match self {
            RequestId::Internal(id) => Some(id),
            RequestId::Propagate(_) => None,
        }
    }
}

impl RequestIdManager {
    pub fn new(
        generate_request_id: bool,
        preserve_external_request_id: bool,
        always_set_request_id_in_response: bool,
    ) -> Self {
        Self { generate_request_id, preserve_external_request_id, always_set_request_id_in_response }
    }

    pub fn apply_policy<B>(
        &self,
        mut req: Request<B>,
        _access_log_enabled: bool,
        incoming_request_id: Option<&RequestId>,
        is_internal: bool,
    ) -> (Request<B>, Option<RequestId>) {
        let (authoritative_id, is_generated) = match incoming_request_id.as_ref() {
            Some(id) if is_internal || self.preserve_external_request_id => (Some(id.to_value()), false),
            _ if self.generate_request_id => (Some(Self::generate_new_id()), true),
            #[cfg(feature = "tracing")]
            _ => (Some(Self::generate_new_id()), true),
            #[cfg(not(feature = "tracing"))]
            _ if _access_log_enabled => (Some(Self::generate_new_id()), false),
            #[cfg(not(feature = "tracing"))]
            _ => (None, false),
        };

        // 2. Determine if the ID must be propagated...
        let should_propagate_header =
            (incoming_request_id.is_some() && (is_internal || self.preserve_external_request_id)) || self.generate_request_id;

        // 3. Apply the changes to the request...
        if should_propagate_header {
            if is_generated {
                if let Some(authoritative_id) = authoritative_id.as_ref() {
                    //info!("Generated new X-Request-ID: {}", authoritative_id.to_str().unwrap_or("invalid"));
                    req.headers_mut().insert(X_REQUEST_ID, authoritative_id.clone());
                }
            }
        } else if incoming_request_id.is_some() {
            req.headers_mut().remove(X_REQUEST_ID);
        }

        // 4. Create the RequestId...
        let req_id = if should_propagate_header {
            authoritative_id.map(RequestId::Propagate)
        } else {
            authoritative_id.map(RequestId::Internal)
        };

        (req, req_id)
    }

    #[inline]
    fn generate_new_id() -> HeaderValue {
        let mut buffer = [0u8; 36];
        let new_id_str = uuid::Uuid::new_v4().hyphenated().encode_lower(&mut buffer);
        HeaderValue::from_str(new_id_str).unwrap_or_else(|e| {
            info!("UUID string should be valid HeaderValue: {e}");
            // Fallback in case of an error, though this should not happen with valid UUIDs
            HeaderValue::from_static("unknown-request-id")
        })
    }

    pub fn apply_to<B>(&self, resp: &mut Response<B>, req_id: Option<&HeaderValue>) {
        if self.always_set_request_id_in_response {
            req_id.inspect(|id| {
                resp.headers_mut().insert(X_REQUEST_ID, (*id).clone());
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;

    #[test]
    fn test_request_id_from_request() {
        let request = Request::builder().header(X_REQUEST_ID, "123e4567-e89b-12d3-a456-426614174000").body(()).unwrap();
        let request_id = RequestId::from_request(&request);
        assert!(request_id.is_some());
        if let Some(RequestId::Propagate(id)) = request_id {
            assert_eq!(id.to_str().unwrap(), "123e4567-e89b-12d3-a456-426614174000");
        } else {
            panic!("Expected RequestId::Propagate, got {request_id:?}");
        }
    }

    #[test]
    fn test_broken_request_id_from_request() {
        let request = Request::builder().header(X_REQUEST_ID, "123e4567-invalid-614174").body(()).unwrap();
        let request_id = RequestId::from_request(&request);
        assert!(request_id.is_some());
        if let Some(RequestId::Propagate(id)) = request_id {
            assert_eq!(id.to_str().unwrap(), "123e4567-invalid-614174");
        } else {
            panic!("Expected RequestId::Propagate, got {request_id:?}");
        }
    }

    #[test]
    fn test_not_avail_request_id_from_request() {
        let request = Request::builder().body(()).unwrap();
        let request_id = RequestId::from_request(&request);
        assert!(request_id.is_none());
    }

    #[test]
    fn test_req_id_manager_apply_policy() {
        let access_log_enabled = false;

        // generate = false, preserve = false, always_set = false, is_internal = false
        let manager = RequestIdManager::new(false, false, false);
        let request = Request::builder().body(()).unwrap();
        let (modified_request, req_id) = manager.apply_policy(request, access_log_enabled, None, false);
        assert!(!modified_request.headers().contains_key(X_REQUEST_ID));
        #[cfg(feature = "tracing")]
        assert!(matches!(req_id, Some(RequestId::Internal(_))));
        #[cfg(not(feature = "tracing"))]
        assert!(req_id.is_none());

        // generate = false, preserve = false, always_set = false, is_internal = false
        let manager = RequestIdManager::new(false, false, false);
        let request = Request::builder().header(X_REQUEST_ID, "123e4567-e89b-12d3-a456-426614174000").body(()).unwrap();
        let request_id = RequestId::from_request(&request);
        let (modified_request, req_id) = manager.apply_policy(request, access_log_enabled, request_id.as_ref(), false);
        assert!(!modified_request.headers().contains_key(X_REQUEST_ID));
        #[cfg(feature = "tracing")]
        assert!(matches!(req_id, Some(RequestId::Internal(_))));
        #[cfg(not(feature = "tracing"))]
        assert!(req_id.is_none());

        // generate = true, preserve = false, always_set = false, is_internal = false
        let manager = RequestIdManager::new(true, false, false);
        let request = Request::builder().header(X_REQUEST_ID, "123e4567-e89b-12d3-a456-426614174000").body(()).unwrap();
        let request_id = RequestId::from_request(&request);
        let (modified_request, req_id) = manager.apply_policy(request, access_log_enabled, request_id.as_ref(), false);
        assert!(modified_request.headers().contains_key(X_REQUEST_ID));
        assert!(matches!(req_id, Some(RequestId::Propagate(_))));
        assert_ne!(
            modified_request.headers().get(X_REQUEST_ID),
            Some(&HeaderValue::from_static("123e4567-e89b-12d3-a456-426614174000"))
        );

        // generate = true, preserve = true, always_set = false, is_internal = false
        let manager = RequestIdManager::new(true, true, false);
        let request = Request::builder().body(()).unwrap();
        let request_id = RequestId::from_request(&request);
        let (modified_request, req_id) = manager.apply_policy(request, access_log_enabled, request_id.as_ref(), false);
        assert!(modified_request.headers().contains_key(X_REQUEST_ID));
        assert!(matches!(req_id, Some(RequestId::Propagate(_))));

        // generate = true, preserve = true, always_set = false (with request already having X-Request-ID), is_internal = false
        let manager = RequestIdManager::new(true, true, false);
        let request = Request::builder().header(X_REQUEST_ID, "123e4567-e89b-12d3-a456-426614174000").body(()).unwrap();
        let request_id = RequestId::from_request(&request);
        let (modified_request, req_id) = manager.apply_policy(request, access_log_enabled, request_id.as_ref(), false);
        assert!(modified_request.headers().contains_key(X_REQUEST_ID));
        assert!(matches!(req_id, Some(RequestId::Propagate(_))));
        assert_eq!(
            modified_request.headers().get(X_REQUEST_ID),
            Some(&HeaderValue::from_static("123e4567-e89b-12d3-a456-426614174000"))
        );

        // generate = true, preserve = false, always_set = false, is_internal = true
        let manager = RequestIdManager::new(true, false, false);
        let request = Request::builder().header(X_REQUEST_ID, "123e4567-e89b-12d3-a456-426614174000").body(()).unwrap();
        let request_id = RequestId::from_request(&request);
        let (modified_request, req_id) = manager.apply_policy(request, access_log_enabled, request_id.as_ref(), true);
        assert!(modified_request.headers().contains_key(X_REQUEST_ID));
        assert!(matches!(req_id, Some(RequestId::Propagate(_))));
        assert_eq!(
            modified_request.headers().get(X_REQUEST_ID),
            Some(&HeaderValue::from_static("123e4567-e89b-12d3-a456-426614174000"))
        );
    }
}
