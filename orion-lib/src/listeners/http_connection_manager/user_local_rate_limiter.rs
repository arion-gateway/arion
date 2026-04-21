use std::sync::Arc;

use orion_configuration::config::{GenericError, network_filters::http_connection_manager::http_filters::user_local_rate_limit::UserLocalRateLimit};
use tracing::debug;

use crate::{OrionRequestBody, listeners::http_filters::FilterDecision};


#[derive(Debug, Clone)]
pub struct UserLocalRateLimiter {
    inner: Arc<UserLocalRateLimit>
}

impl TryFrom<UserLocalRateLimit> for UserLocalRateLimiter {
    type Error = GenericError;

    fn try_from(value: UserLocalRateLimit) -> Result<Self, Self::Error> {
        Ok(Self {
            inner: Arc::new(value),
        })
    }
}

impl UserLocalRateLimiter {
    pub async fn apply_request(&mut self, request: &mut http::Request<OrionRequestBody>) -> FilterDecision {
        debug!(target: "user_local_rate_limiter", "apply_request: processing request: {:?}", request);
        debug!(target: "user_local_rate_limiter", "{:#?}", self.inner);
        FilterDecision::Continue
    }
}
