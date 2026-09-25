// Copyright 2025-2026 The arion-gateway Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use hyper::rt::{Sleep, Timer};
use pin_project::pin_project;

/// A wrapper around the future returned by `pingora_timeout::sleep`
#[pin_project]
#[derive(Debug)]
pub struct PingoraSleep<F: Future<Output = ()>> {
    #[pin]
    inner: F,
}

// PingoraSleep implements Future
impl<F> Future for PingoraSleep<F>
where
    F: Future<Output = ()>,
{
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

/// `PingoraSleep` implements hyper Sleep
impl<F> Sleep for PingoraSleep<F> where F: Future<Output = ()> + Send + Sync {}

/// A `hyper::rt::Timer` implementation using pingora's sleep function
#[derive(Clone, Debug, Default)]
pub struct PingoraTimer;

impl Timer for PingoraTimer {
    fn sleep(&self, duration: std::time::Duration) -> Pin<Box<dyn Sleep>> {
        let sleep_future = pingora_timeout::sleep(duration);
        Box::pin(PingoraSleep { inner: sleep_future })
    }

    fn sleep_until(&self, deadline: std::time::Instant) -> Pin<Box<dyn Sleep>> {
        let duration = deadline.saturating_duration_since(std::time::Instant::now());
        self.sleep(duration)
    }
}
