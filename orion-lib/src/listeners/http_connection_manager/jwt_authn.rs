use crate::{PolyBody, body::{instrumented_body::InstrumentedBody, timeout_body::TimeoutBody}, listeners::http_connection_manager::FilterDecision};
use http::Request;
use tracing::debug;

use orion_configuration::config::{
    network_filters::http_connection_manager::http_filters::{
        jwt::{JwtAuthentication as JwtAuthenticationConfig
        },
    },
};

#[derive(Debug, Clone)]
pub struct JwtAuthentication {
    config: JwtAuthenticationConfig,
}

impl JwtAuthentication {
    pub fn new(config: JwtAuthenticationConfig) -> Self {
        debug!(target: "jwt", "Creating new JWT authentication filter");
        Self{ config }
    }
    #[allow(clippy::too_many_lines)]
    pub async fn apply_request(
        &mut self,
        _request: &mut Request<InstrumentedBody<TimeoutBody<PolyBody>>>,
    ) -> FilterDecision {
        debug!(target: "jwt", "Applying JWT authentication filter");
        debug!(target: "jwt", "{:#?}", self.config);
        FilterDecision::Continue
    }
}
