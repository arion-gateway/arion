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

use super::body_with_timeout::{BodyWithTimeout, TimeoutBodyError};
use crate::Error;
use bytes::Bytes;
use http_body::Frame;
use http_body_util::{Empty, Full, StreamBody};
use hyper::body::{Body, Incoming};
use orion_xds::grpc_deps::{GrpcBody, Status as GrpcError};
use pin_project::pin_project;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

#[pin_project(project = PolyBodyProj)]
pub enum PolyBody {
    Empty(#[pin] Empty<Bytes>),
    Full(#[pin] Full<Bytes>),
    Incoming(#[pin] Incoming),
    Timeout(#[pin] BodyWithTimeout<Incoming>),
    Grpc(#[pin] GrpcBody),
    Stream(#[pin] StreamBody<ReceiverStream<Result<Frame<Bytes>, Error>>>),
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
            PolyBody::Empty(_) => f.write_str("PolyBody::Empty<Bytes>"),
            PolyBody::Full(_) => f.write_str("PolyBody::Full<Bytes>"),
            PolyBody::Incoming(_) => f.write_str("PolyBody::Incoming"),
            PolyBody::Timeout(_) => f.write_str("PolyBody::Timeout<Incoming>"),
            PolyBody::Grpc(_) => f.write_str("PolyBody::Grpc"),
            PolyBody::Stream(_) => f.write_str("PolyBody::Stream"),
        }
    }
}

#[derive(thiserror::Error, Debug)]
pub enum PolyBodyError {
    #[error(transparent)]
    Hyper(#[from] hyper::Error),
    #[error(transparent)]
    Infallible(#[from] std::convert::Infallible),
    #[error(transparent)]
    Grpc(#[from] GrpcError),
    #[error(transparent)]
    Boxed(#[from] Box<dyn std::error::Error + std::marker::Send + std::marker::Sync>),
    #[error("data was not received within the designated timeout")]
    TimedOut,
}

//hyper::Error is the error type returned by incoming
impl From<TimeoutBodyError<hyper::Error>> for PolyBodyError {
    fn from(value: TimeoutBodyError<hyper::Error>) -> Self {
        match value {
            TimeoutBodyError::TimedOut => Self::TimedOut,
            TimeoutBodyError::BodyError(e) => Self::Hyper(e),
        }
    }
}

impl Body for PolyBody {
    type Data = Bytes;
    type Error = PolyBodyError;

    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        match self.project() {
            PolyBodyProj::Empty(e) => e.poll_frame(cx).map_err(Into::into),
            PolyBodyProj::Full(f) => f.poll_frame(cx).map_err(Into::into),
            PolyBodyProj::Incoming(i) => i.poll_frame(cx).map_err(Into::into),
            PolyBodyProj::Timeout(t) => t.poll_frame(cx).map_err(Into::into),
            PolyBodyProj::Grpc(g) => g.poll_frame(cx).map_err(Into::into),
            PolyBodyProj::Stream(s) => {
                s.poll_frame(cx).map_err(|e| PolyBodyError::Boxed(Box::new(std::io::Error::other(e.to_string()))))
            },
        }
    }

    fn is_end_stream(&self) -> bool {
        match self {
            PolyBody::Empty(e) => e.is_end_stream(),
            PolyBody::Full(f) => f.is_end_stream(),
            PolyBody::Incoming(i) => i.is_end_stream(),
            PolyBody::Timeout(t) => t.is_end_stream(),
            PolyBody::Grpc(g) => g.is_end_stream(),
            PolyBody::Stream(s) => s.is_end_stream(),
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

impl From<BodyWithTimeout<Incoming>> for PolyBody {
    #[inline]
    fn from(body: BodyWithTimeout<Incoming>) -> Self {
        PolyBody::Timeout(body)
    }
}

impl From<GrpcBody> for PolyBody {
    #[inline]
    fn from(body: GrpcBody) -> Self {
        PolyBody::Grpc(body)
    }
}

impl From<StreamBody<ReceiverStream<Result<Frame<Bytes>, Error>>>> for PolyBody {
    #[inline]
    fn from(body: StreamBody<ReceiverStream<Result<Frame<Bytes>, Error>>>) -> Self {
        PolyBody::Stream(body)
    }
}

impl PolyBody {
    pub fn new_stream_body(buffer_size: usize) -> (Self, mpsc::Sender<Result<Frame<Bytes>, Error>>) {
        let (tx, rx) = mpsc::channel(buffer_size);
        let stream = ReceiverStream::new(rx);
        let body = StreamBody::new(stream);
        (PolyBody::Stream(body), tx)
    }
}

pub struct BodySender {
    sender: mpsc::Sender<Result<Frame<Bytes>, Error>>,
}

impl BodySender {
    pub fn new(sender: mpsc::Sender<Result<Frame<Bytes>, Error>>) -> Self {
        Self { sender }
    }
    pub async fn send_data(&self, chunk: Bytes) -> Result<(), mpsc::error::SendError<Result<Frame<Bytes>, Error>>> {
        self.sender.send(Ok(Frame::data(chunk))).await
    }
    pub async fn send_trailers(
        &self,
        trailers: http::HeaderMap,
    ) -> Result<(), mpsc::error::SendError<Result<Frame<Bytes>, Error>>> {
        self.sender.send(Ok(Frame::trailers(trailers))).await
    }
}
