use crate::{
    body::response_flags::ResponseFlags,
    event_error::EventFailure,
    listeners::{http_connection_manager::FilterDecision, synthetic_http_response::SyntheticHttpResponse},
};

use orion_format::types::ResponseFlags as FmtResponseFlags;

#[inline]
pub fn internal_server_error(msg: &str, ver: http::Version) -> FilterDecision {
    FilterDecision::DirectResponse(
        SyntheticHttpResponse::internal_server_error(
            EventFailure::DirectResponse.into(),
            ResponseFlags::default(),
            msg,
        )
        .into_response(ver),
    )
}

#[inline]
pub fn bad_request(ver: http::Version) -> FilterDecision {
    FilterDecision::DirectResponse(
        SyntheticHttpResponse::bad_request(EventFailure::DirectResponse.into()).into_response(ver),
    )
}

#[inline]
pub fn not_found(ver: http::Version) -> FilterDecision {
    FilterDecision::DirectResponse(
        SyntheticHttpResponse::not_found(EventFailure::DirectResponse.into(), ResponseFlags::default())
            .into_response(ver),
    )
}

#[inline]
pub fn rate_limited(ver: http::Version) -> FilterDecision {
    FilterDecision::DirectResponse(
        SyntheticHttpResponse::custom_error(
            http::StatusCode::TOO_MANY_REQUESTS,
            None,
            EventFailure::RateLimited.into(),
            ResponseFlags(FmtResponseFlags::RATE_LIMITED),
        )
        .into_response(ver),
    )
}
