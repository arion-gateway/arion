use crate::{body::timeout_body::TimeoutBody, OrionResponseBody, PolyBody};

use http::Response;
use http_body_util::Empty;

#[inline]
pub fn okay_response(ver: http::Version) -> Response<OrionResponseBody> {
    let mut okay = Response::new(TimeoutBody::new(None, PolyBody::from(Empty::new())));
    *okay.status_mut() = http::StatusCode::OK;
    *okay.version_mut() = ver;
    okay
}
