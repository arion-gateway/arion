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

use http_body::{Body, Frame, SizeHint};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::body::response_flags::{BodyKind, ResponseFlags};

#[cfg(any(feature = "access-log", feature = "metrics"))]
mod metrics_enabled {
    #[allow(clippy::wildcard_imports)]
    use super::*;
    use crate::{
        event_error::{DownstreamError, EventKind, TryInferFrom, UpstreamError},
        utils::instrumented_stream::StreamMetrics,
    };
    use atomicoption::AtomicOption;
    use bytes::Buf;
    use pin_project::{pin_project, pinned_drop};
    use std::sync::{Arc, atomic::Ordering};

    type MetricsClosure = Box<dyn FnOnce(u64, &StreamMetrics, Option<EventKind>, ResponseFlags) + Send + 'static>;

    #[pin_project(PinnedDrop)]
    pub struct InstrumentedBody<B> {
        #[pin]
        pub inner: B,
        pub body_kind: BodyKind,
        pub body_bytes: u64,
        pub stream_metrics: Option<Arc<StreamMetrics>>,
        pub on_complete: Arc<AtomicOption<MetricsClosure>>,
    }

    #[pinned_drop]
    impl<B> PinnedDrop for InstrumentedBody<B> {
        fn drop(self: std::pin::Pin<&mut Self>) {
            let this = self.project();
            if let Some(closure) = this.on_complete.take(Ordering::Acquire) {
                if let Some(metrics) = this.stream_metrics.as_ref() {
                    closure(*this.body_bytes, metrics.as_ref(), None, ResponseFlags::default());
                }
            }
        }
    }

    impl<B> std::fmt::Debug for InstrumentedBody<B>
    where
        B: std::fmt::Debug,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("InstrumentedBody<{:?}>", self.inner))
        }
    }

    impl<B: Default> InstrumentedBody<B> {
        pub fn new<F>(kind: BodyKind, inner: B, metrics: Option<Arc<StreamMetrics>>, on_complete: F) -> Self
        where
            F: FnOnce(u64, &StreamMetrics, Option<EventKind>, ResponseFlags) + Send + 'static,
        {
            Self {
                inner,
                body_kind: kind,
                body_bytes: 0,
                stream_metrics: metrics,
                on_complete: Arc::new(AtomicOption::some(Box::new(on_complete))),
            }
        }

        #[inline]
        pub fn map_into<B2>(mut self) -> InstrumentedBody<B2>
        where
            B: Into<B2>,
        {
            let free_body = std::mem::replace(&mut self.inner, B::default());
            InstrumentedBody {
                inner: free_body.into(),
                body_kind: self.body_kind,
                body_bytes: self.body_bytes,
                stream_metrics: self.stream_metrics.clone(),
                on_complete: Arc::clone(&self.on_complete),
            }
        }

        #[inline]
        pub fn map_inner<B2, F2>(mut self, f: F2) -> InstrumentedBody<B2>
        where
            F2: FnOnce(B) -> B2,
        {
            let free_body = std::mem::replace(&mut self.inner, B::default());
            InstrumentedBody {
                inner: f(free_body),
                body_kind: self.body_kind,
                body_bytes: self.body_bytes,
                stream_metrics: self.stream_metrics.clone(),
                on_complete: Arc::clone(&self.on_complete),
            }
        }

        #[inline]
        pub fn into_inner(mut self) -> B {
            let inner = std::mem::replace(&mut self.inner, B::default());
            inner
        }
    }

    impl<B> Body for InstrumentedBody<B>
    where
        B: Body,
        <B as http_body::Body>::Error: std::error::Error + Send + Sync + 'static,
        ResponseFlags: for<'a> From<(&'a <B as Body>::Error, BodyKind)>,
    {
        type Data = B::Data;
        type Error = B::Error;

        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            let this = self.project();
            let poll = this.inner.poll_frame(cx);
            match &poll {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Some(data) = frame.data_ref() {
                        *this.body_bytes += data.remaining() as u64;
                    }
                },
                Poll::Ready(None) => {
                    if let Some(closure) = this.on_complete.take(Ordering::Acquire) {
                        if let Some(metrics) = this.stream_metrics.as_ref() {
                            closure(*this.body_bytes, metrics.as_ref(), None, ResponseFlags::default());
                        }
                    }
                },
                Poll::Ready(Some(Err(err))) => {
                    if let Some(closure) = this.on_complete.take(Ordering::Acquire) {
                        if let Some(metrics) = this.stream_metrics.as_ref() {
                            let event_error: Option<EventKind> = match *this.body_kind {
                                BodyKind::Request => DownstreamError::try_infer_from(err).map(Into::into),
                                BodyKind::Response => UpstreamError::try_infer_from(err).map(Into::into),
                            };

                            let flags = ResponseFlags::from((err, *this.body_kind));
                            closure(*this.body_bytes, metrics.as_ref(), event_error, flags);
                        }
                    }
                },
                Poll::Pending => {},
            }
            poll
        }

        #[inline]
        fn is_end_stream(&self) -> bool {
            self.inner.is_end_stream()
        }

        #[inline]
        fn size_hint(&self) -> SizeHint {
            self.inner.size_hint()
        }
    }
}

#[cfg(not(any(feature = "access-log", feature = "metrics")))]
mod metrics_disabled {
    use std::{marker::PhantomData, sync::Arc};

    use crate::{event_error::EventKind, utils::instrumented_stream::StreamMetrics};

    #[allow(clippy::wildcard_imports)]
    use super::*;
    use pin_project::pin_project;

    #[pin_project]
    #[derive(Clone, Copy)]
    pub struct InstrumentedBody<B> {
        #[pin]
        pub inner: B,
        pub body_kind: BodyKind,
        pub body_bytes: u64,
        pub stream_metrics: PhantomData<()>,
        pub on_complete: PhantomData<()>,
    }

    impl<B> std::fmt::Debug for InstrumentedBody<B>
    where
        B: std::fmt::Debug,
    {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_fmt(format_args!("InstrumentedBody<{:?}>", self.inner))
        }
    }

    impl<B> InstrumentedBody<B> {
        pub fn new<F>(kind: BodyKind, inner: B, _metrics: Option<Arc<StreamMetrics>>, _on_complete: F) -> Self
        where
            F: FnOnce(u64, &StreamMetrics, Option<EventKind>, ResponseFlags) + Send + 'static,
        {
            Self { inner, body_kind: kind, body_bytes: 0, stream_metrics: PhantomData, on_complete: PhantomData }
        }

        pub fn map_into<B2>(self) -> InstrumentedBody<B2>
        where
            B: Into<B2>,
        {
            InstrumentedBody {
                inner: self.inner.into(),
                body_kind: self.body_kind,
                body_bytes: self.body_bytes,
                stream_metrics: PhantomData,
                on_complete: PhantomData,
            }
        }

        pub fn map_inner<B2, F>(self, f: F) -> InstrumentedBody<B2>
        where
            F: FnOnce(B) -> B2,
        {
            InstrumentedBody {
                inner: f(self.inner),
                body_kind: self.body_kind,
                body_bytes: self.body_bytes,
                stream_metrics: PhantomData,
                on_complete: PhantomData,
            }
        }

        #[inline]
        pub fn into_inner(self) -> B {
            self.inner
        }
    }

    impl<B: Body> Body for InstrumentedBody<B> {
        type Data = B::Data;
        type Error = B::Error;

        #[inline]
        fn poll_frame(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
            self.project().inner.poll_frame(cx)
        }

        #[inline]
        fn is_end_stream(&self) -> bool {
            self.inner.is_end_stream()
        }

        #[inline]
        fn size_hint(&self) -> SizeHint {
            self.inner.size_hint()
        }
    }
}

#[cfg(any(feature = "access-log", feature = "metrics"))]
pub use metrics_enabled::InstrumentedBody;

#[cfg(not(any(feature = "access-log", feature = "metrics")))]
pub use metrics_disabled::InstrumentedBody;
