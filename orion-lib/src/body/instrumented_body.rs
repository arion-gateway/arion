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

use crate::{
    body::response_flags::{BodyKind, ResponseFlags},
    event_error::EventError,
};

#[cfg(any(feature = "access-log", feature = "metrics"))]
mod metrics_enabled {
    #[allow(clippy::wildcard_imports)]
    use super::*;
    use crate::event_error::TryInferFrom;
    use bytes::Buf;
    use parking_lot::Mutex;
    use pin_project::pin_project;
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };

    type MetricsClosure = Box<dyn FnOnce(u64, Option<EventError>, ResponseFlags) + Send + 'static>;

    pub struct MetricsState {
        body_kind: BodyKind,
        bytes_counter: AtomicU64,
        on_complete: Mutex<Option<MetricsClosure>>,
    }

    /// Pin-project prevents the struct to implement `Drop`.
    /// This workaround allows us to use `Drop` and invoke the closure, if not already executed.
    #[derive(Clone)]
    pub struct DropGuard {
        state: Arc<MetricsState>,
    }

    impl Drop for DropGuard {
        fn drop(&mut self) {
            trigger_on_complete(&self.state, None, ResponseFlags::default());
        }
    }

    fn trigger_on_complete(state: &Arc<MetricsState>, event_error: Option<EventError>, flags: ResponseFlags) {
        let mut guard = state.on_complete.lock();
        if let Some(closure) = guard.take() {
            let bytes = state.bytes_counter.load(Ordering::Relaxed);
            closure(bytes, event_error, flags);
        }
    }

    #[pin_project]
    pub struct InstrumentedBody<B> {
        #[pin]
        pub inner: B,
        pub state: Arc<MetricsState>,
        pub guard: DropGuard,
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
        pub fn new<F>(kind: BodyKind, inner: B, on_complete: F) -> Self
        where
            F: FnOnce(u64, Option<EventError>, ResponseFlags) + Send + 'static,
        {
            let state = Arc::new(MetricsState {
                body_kind: kind,
                bytes_counter: AtomicU64::new(0),
                on_complete: Mutex::new(Some(Box::new(on_complete))),
            });

            Self { inner, guard: DropGuard { state: state.clone() }, state }
        }

        #[inline]
        pub fn map_into<B2>(self) -> InstrumentedBody<B2>
        where
            B: Into<B2>,
        {
            InstrumentedBody { inner: self.inner.into(), state: self.state, guard: self.guard }
        }

        #[inline]
        pub fn map_inner<B2, F>(self, f: F) -> InstrumentedBody<B2>
        where
            F: FnOnce(B) -> B2,
        {
            InstrumentedBody { inner: f(self.inner), state: self.state, guard: self.guard }
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
                        let size = data.remaining() as u64;
                        this.state.bytes_counter.fetch_add(size, std::sync::atomic::Ordering::Relaxed);
                    }
                },
                Poll::Ready(None) => {
                    trigger_on_complete(this.state, None, ResponseFlags::default());
                },
                Poll::Ready(Some(Err(err))) => {
                    let event_error = EventError::try_infer_from(err);
                    let flags = ResponseFlags::from((err, this.state.body_kind));
                    trigger_on_complete(this.state, event_error, flags);
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
    use std::marker::PhantomData;

    #[allow(clippy::wildcard_imports)]
    use super::*;
    use pin_project::pin_project;

    #[pin_project]
    #[derive(Clone, Copy)]
    pub struct InstrumentedBody<B> {
        #[pin]
        pub inner: B,
        pub guard: PhantomData<()>,
        pub state: PhantomData<()>,
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
        pub fn new<F>(_kind: BodyKind, inner: B, _on_complete: F) -> Self
        where
            F: FnOnce(u64, Option<EventError>, ResponseFlags) + Send + 'static,
        {
            Self { inner, guard: PhantomData, state: PhantomData }
        }

        pub fn map_into<B2>(self) -> InstrumentedBody<B2>
        where
            B: Into<B2>,
        {
            InstrumentedBody { inner: self.inner.into(), guard: PhantomData, state: PhantomData }
        }

        pub fn map_inner<B2, F>(self, f: F) -> InstrumentedBody<B2>
        where
            F: FnOnce(B) -> B2,
        {
            InstrumentedBody { inner: f(self.inner), state: self.state, guard: self.guard }
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
