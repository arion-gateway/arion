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
//

pub use crate::body::error::BodyError;
pub type PolyBodyError = BodyError;

use crate::body::channel_body::ChannelBody;
use arion_xds::grpc_deps::GrpcBody;
use bytes::Bytes;
use http_body::SizeHint;
use http_body_util::combinators::WithTrailers;
use http_body_util::{BodyExt, Collected, Empty, Full};
use hyper::body::{Body, Incoming};
use pin_project::pin_project;
use std::convert::Infallible;
use std::future::Ready;

pub type TrailersType = Option<Result<http::HeaderMap, Infallible>>;

#[pin_project(project = PolyBodyProj)]
pub enum PolyBody {
    Empty(#[pin] Empty<Bytes>),
    Full(#[pin] Full<Bytes>),
    Incoming(#[pin] Incoming),
    Grpc(#[pin] GrpcBody),
    ChannelBody(#[pin] Box<ChannelBody>),
    Collected(#[pin] Box<Collected<Bytes>>),
    FullWithTrailers(#[pin] Box<WithTrailers<Full<Bytes>, Ready<TrailersType>>>),
    CollectedWithTrailers(#[pin] Box<WithTrailers<Collected<Bytes>, Ready<TrailersType>>>),
}

impl PolyBody {
    pub fn with_trailers(self, trailers: http::HeaderMap) -> Result<Self, BodyError> {
        match self {
            PolyBody::Empty(_) => {
                let ready = std::future::ready(Some(Ok::<_, Infallible>(trailers)));
                Ok(PolyBody::FullWithTrailers(Box::new(Full::new(Bytes::new()).with_trailers(ready))))
            },
            PolyBody::Full(f) => {
                let ready = std::future::ready(Some(Ok::<_, Infallible>(trailers)));
                Ok(PolyBody::FullWithTrailers(Box::new(f.with_trailers(ready))))
            },
            PolyBody::Collected(c) => {
                let ready = std::future::ready(Some(Ok::<_, Infallible>(trailers)));
                Ok(PolyBody::CollectedWithTrailers(Box::new((*c).with_trailers(ready))))
            },
            PolyBody::Incoming(_) => Err(BodyError::TrailersNotSupported("Incoming")),
            PolyBody::Grpc(_) => Err(BodyError::TrailersNotSupported("Grpc")),
            PolyBody::ChannelBody(_) => Err(BodyError::TrailersNotSupported("ChannelBody")),
            PolyBody::FullWithTrailers(_) => Err(BodyError::TrailersNotSupported("FullWithTrailers")),
            PolyBody::CollectedWithTrailers(_) => Err(BodyError::TrailersNotSupported("CollectedWithTrailers")),
        }
    }

    pub async fn prefetch_frames(&mut self) {
        if let PolyBody::ChannelBody(m) = self {
            m.prefetch_frames().await;
        } else {
            // No-op for other body types
        }
    }
}

impl Default for PolyBody {
    #[inline]
    fn default() -> Self {
        PolyBody::Empty(Empty::<Bytes>::default())
    }
}

impl std::fmt::Debug for PolyBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PolyBody::Empty(b) => f.write_fmt(format_args!("PolyBody::Empty<Bytes>: {b:?}")),
            PolyBody::Full(b) => f.write_fmt(format_args!("PolyBody::Full<Bytes>: {b:?}")),
            PolyBody::Incoming(b) => f.write_fmt(format_args!("PolyBody::Incoming: {b:?}")),
            PolyBody::Grpc(b) => f.write_fmt(format_args!("PolyBody::Grpc: {b:?}")),
            PolyBody::ChannelBody(b) => f.write_fmt(format_args!("PolyBody::ChannelBody: {b:?}")),
            PolyBody::Collected(b) => f.write_fmt(format_args!("PolyBody::Collected: {b:?}")),
            PolyBody::FullWithTrailers(_) => f.write_str("PolyBody::WithTrailers<Full<Bytes>, Ready<TrailersType>>"),
            PolyBody::CollectedWithTrailers(_) => {
                f.write_str("PolyBody::CollectedWithTrailers<Collected<Bytes>, Ready<TrailersType>>")
            },
        }
    }
}

impl Body for PolyBody {
    type Data = Bytes;
    type Error = BodyError;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.project() {
            PolyBodyProj::Empty(e) => e.poll_frame(cx).map_err(|inf| match inf {}),
            PolyBodyProj::Full(f) => f.poll_frame(cx).map_err(|inf| match inf {}),
            PolyBodyProj::Incoming(i) => i.poll_frame(cx).map_err(Into::into),
            PolyBodyProj::Grpc(g) => g.poll_frame(cx).map_err(Into::into),
            PolyBodyProj::ChannelBody(m) => m.poll_frame(cx),
            PolyBodyProj::Collected(s) => match s.poll_frame(cx) {
                std::task::Poll::Ready(Some(Ok(f))) => std::task::Poll::Ready(Some(Ok(f))),
                std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
                std::task::Poll::Pending => std::task::Poll::Pending,
                std::task::Poll::Ready(Some(Err(inf))) => match inf {},
            },
            PolyBodyProj::FullWithTrailers(w) => w.poll_frame(cx).map_err(|inf| match inf {}),
            PolyBodyProj::CollectedWithTrailers(w) => w.poll_frame(cx).map_err(|inf| match inf {}),
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            PolyBody::Empty(e) => e.is_end_stream(),
            PolyBody::Full(f) => f.is_end_stream(),
            PolyBody::Incoming(i) => i.is_end_stream(),
            PolyBody::Grpc(g) => g.is_end_stream(),
            PolyBody::ChannelBody(m) => m.is_end_stream(),
            PolyBody::Collected(s) => s.is_end_stream(),
            PolyBody::FullWithTrailers(w) => w.is_end_stream(),
            PolyBody::CollectedWithTrailers(w) => w.is_end_stream(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self {
            PolyBody::Empty(e) => e.size_hint(),
            PolyBody::Full(f) => f.size_hint(),
            PolyBody::Incoming(i) => i.size_hint(),
            PolyBody::Grpc(g) => g.size_hint(),
            PolyBody::ChannelBody(m) => m.size_hint(),
            PolyBody::Collected(s) => s.size_hint(),
            PolyBody::FullWithTrailers(w) => w.size_hint(),
            PolyBody::CollectedWithTrailers(w) => w.size_hint(),
        }
    }
}

impl From<Empty<Bytes>> for PolyBody {
    #[inline]
    fn from(body: Empty<Bytes>) -> Self {
        PolyBody::Empty(body)
    }
}

impl From<Full<Bytes>> for PolyBody {
    #[inline]
    fn from(body: Full<Bytes>) -> Self {
        PolyBody::Full(body)
    }
}

impl From<Incoming> for PolyBody {
    #[inline]
    fn from(body: Incoming) -> Self {
        PolyBody::Incoming(body)
    }
}

impl From<GrpcBody> for PolyBody {
    #[inline]
    fn from(body: GrpcBody) -> Self {
        PolyBody::Grpc(body)
    }
}

impl From<Collected<Bytes>> for PolyBody {
    #[inline]
    fn from(body: Collected<Bytes>) -> Self {
        PolyBody::Collected(Box::new(body))
    }
}

impl From<ChannelBody> for PolyBody {
    #[inline]
    fn from(body: ChannelBody) -> Self {
        PolyBody::ChannelBody(Box::new(body))
    }
}

impl From<WithTrailers<Full<Bytes>, Ready<TrailersType>>> for PolyBody {
    #[inline]
    fn from(body: WithTrailers<Full<Bytes>, Ready<TrailersType>>) -> Self {
        PolyBody::FullWithTrailers(Box::new(body))
    }
}

impl From<WithTrailers<Collected<Bytes>, Ready<TrailersType>>> for PolyBody {
    #[inline]
    fn from(body: WithTrailers<Collected<Bytes>, Ready<TrailersType>>) -> Self {
        PolyBody::CollectedWithTrailers(Box::new(body))
    }
}
