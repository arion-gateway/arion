use bytes::Bytes;
use http_body_util::{combinators::WithTrailers, BodyExt, Collected, Full};
use std::convert::Infallible;
use std::future::{ready, Ready};

pub type TrailersType = Option<Result<http::HeaderMap, Infallible>>;

pub fn dup_collected(
    body: Collected<Bytes>,
) -> (WithTrailers<Full<Bytes>, Ready<TrailersType>>, WithTrailers<Full<Bytes>, Ready<TrailersType>>) {
    let trailers = body.trailers().cloned();
    let trailers2 = body.trailers().cloned();
    let bytes = body.to_bytes();
    let bytes2 = bytes.clone();

    (
        Full::new(bytes).with_trailers(ready(trailers.map(Ok::<_, Infallible>))),
        Full::new(bytes2).with_trailers(ready(trailers2.map(Ok::<_, Infallible>))),
    )
}
