// Copyright 2025 The kmesh Authors
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

use super::connector::UnifiedConnector;
use crate::{
    body::{
        on_end_body::{BodyEndPermit, OnEndBody},
        poly_body::PolyBody,
        timeout_body::TimeoutBody,
    },
    thread_local::{LocalBuilder, ThreadLocalObject},
    Error, OrionRequestBody, OrionResponseBody, Result,
};
use http::{uri::Authority, Request, Response, StatusCode, Uri};
use hyper::{
    body::Incoming,
    client::conn::http1::{Builder as Http1Builder, SendRequest},
    rt::{Read, Write},
};
use hyper_rustls::HttpsConnector;
use parking_lot::Mutex;
use std::{sync::Arc as StdArc, time::Duration};
use tower::Service;
use tracing::debug;

const IDLE_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

struct IdleConn {
    tx: SendRequest<OrionRequestBody>,
    idle_at: u64,
}

pub struct Http1PoolInner {
    idle: Mutex<Vec<IdleConn>>,
    idle_timeout: Duration,
    clock: quanta::Clock,
}

impl Http1PoolInner {
    pub fn new(idle_timeout: Duration) -> StdArc<Self> {
        let this = StdArc::new(Self { idle: Mutex::new(Vec::new()), idle_timeout, clock: quanta::Clock::new() });
        let weak = StdArc::downgrade(&this);
        tokio::spawn(async move {
            loop {
                pingora_timeout::sleep(IDLE_CLEANUP_INTERVAL).await;
                let Some(inner) = weak.upgrade() else {
                    break;
                };
                inner.cleanup_expired();
            }
        });
        this
    }

    pub fn cleanup_expired(&self) {
        let mut idle = self.idle.lock();
        if idle.is_empty() {
            return;
        }
        let now = self.clock.raw();
        idle.retain(|conn| {
            if conn.tx.is_closed() {
                return false;
            }
            self.clock.delta(conn.idle_at, now) <= self.idle_timeout
        });
    }

    #[inline]
    pub fn pop(&self) -> Option<SendRequest<OrionRequestBody>> {
        self.idle.lock().pop().map(|conn| conn.tx)
    }

    #[inline]
    pub fn release(&self, tx: SendRequest<OrionRequestBody>) {
        if tx.is_closed() {
            return;
        }
        self.idle.lock().push(IdleConn { tx, idle_at: self.clock.raw() });
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.idle.lock().len()
    }
}

/// Concrete handle that returns an HTTP/1 sender to its pool when the body ends.
pub struct Http1Permit {
    inner: StdArc<Http1PoolInner>,
    tx: SendRequest<OrionRequestBody>,
}

impl Http1Permit {
    pub fn new(inner: StdArc<Http1PoolInner>, tx: SendRequest<OrionRequestBody>) -> Self {
        Self { inner, tx }
    }

    pub fn on_body_end(self, completed: bool) {
        if self.tx.is_closed() {
            return;
        }
        // Drop without a final `poll` (`None`) is common once `is_end_stream` is true.
        // Recycle if the sender can still take another request.
        if completed || self.tx.is_ready() {
            self.inner.release(self.tx);
        }
    }
}

#[derive(Clone)]
enum Http1Connect {
    Plain(UnifiedConnector),
    Tls(HttpsConnector<UnifiedConnector>),
}

#[derive(Clone)]
struct Http1PoolArg {
    connect: Http1Connect,
    dst: Uri,
    idle_timeout: Duration,
    is_tls: bool,
}

#[derive(Clone, Debug, Default)]
struct Http1PoolBuilder;

impl LocalBuilder<Http1PoolArg, StdArc<Http1Pool>> for Http1PoolBuilder {
    fn build(&self, arg: Http1PoolArg) -> StdArc<Http1Pool> {
        StdArc::new(Http1Pool {
            inner: Http1PoolInner::new(arg.idle_timeout),
            connect: arg.connect,
            dst: arg.dst,
            is_tls: arg.is_tls,
        })
    }
}

/// One `Http1Pool` per worker thread, same shape as the legacy `Client`.
#[derive(Clone)]
pub struct Http1ClientExt {
    is_tls: bool,
    pools: StdArc<ThreadLocalObject<StdArc<Http1Pool>, Http1PoolBuilder, Http1PoolArg>>,
}

impl std::fmt::Debug for Http1ClientExt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http1ClientExt").field("is_tls", &self.is_tls).finish()
    }
}

impl Http1ClientExt {
    pub fn plain(connector: UnifiedConnector, authority: &Authority, idle_timeout: Duration) -> Result<Self> {
        Ok(Self {
            is_tls: false,
            pools: StdArc::new(ThreadLocalObject::new(
                Http1PoolBuilder,
                Http1PoolArg {
                    connect: Http1Connect::Plain(connector),
                    dst: dst_uri(authority, false)?,
                    idle_timeout,
                    is_tls: false,
                },
            )),
        })
    }

    pub fn tls(
        connector: HttpsConnector<UnifiedConnector>,
        authority: &Authority,
        idle_timeout: Duration,
    ) -> Result<Self> {
        Ok(Self {
            is_tls: true,
            pools: StdArc::new(ThreadLocalObject::new(
                Http1PoolBuilder,
                Http1PoolArg {
                    connect: Http1Connect::Tls(connector),
                    dst: dst_uri(authority, true)?,
                    idle_timeout,
                    is_tls: true,
                },
            )),
        })
    }

    #[inline]
    pub fn is_tls(&self) -> bool {
        self.is_tls
    }

    #[inline]
    pub fn local_pool(&self) -> StdArc<Http1Pool> {
        StdArc::clone(self.pools.get_local())
    }

    #[inline]
    pub fn local_pool_ref(&self) -> &Http1Pool {
        self.pools.get_local()
    }

    #[inline]
    pub fn strong_count(&self) -> usize {
        StdArc::strong_count(&self.pools)
    }
}

/// Per-endpoint HTTP/1 pool of `hyper::client::conn` senders.
///
/// A sender is checked out for a request and returned only after the response
/// body is fully streamed (or dropped at end-of-stream).
pub struct Http1Pool {
    inner: StdArc<Http1PoolInner>,
    connect: Http1Connect,
    dst: Uri,
    is_tls: bool,
}

impl std::fmt::Debug for Http1Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http1Pool")
            .field("idle", &self.inner.len())
            .field("is_tls", &self.is_tls)
            .finish_non_exhaustive()
    }
}

impl Http1Pool {
    pub async fn send(&self, req: Request<OrionRequestBody>) -> Result<Response<OrionResponseBody>> {
        let mut tx = self.checkout().await?;
        let response = tx.send_request(req).await.map_err(Error::from)?;
        Ok(attach_permit(&self.inner, tx, response))
    }

    async fn checkout(&self) -> Result<SendRequest<OrionRequestBody>> {
        loop {
            let idle = self.pop_idle();
            if let Some(mut tx) = idle {
                if tx.is_closed() {
                    continue;
                }
                if tx.is_ready() {
                    return Ok(tx);
                }
                match tx.ready().await {
                    Ok(()) => return Ok(tx),
                    Err(_) => continue,
                }
            }
            return self.connect_one().await;
        }
    }

    #[inline]
    fn pop_idle(&self) -> Option<SendRequest<OrionRequestBody>> {
        self.inner.pop()
    }

    async fn connect_one(&self) -> Result<SendRequest<OrionRequestBody>> {
        match &self.connect {
            Http1Connect::Plain(connector) => {
                let mut connector = connector.clone();
                let io = connector.call(self.dst.clone()).await.map_err(Error::from)?;
                handshake(io).await
            },
            Http1Connect::Tls(connector) => {
                let mut connector = connector.clone();
                let io = connector.call(self.dst.clone()).await.map_err(Error::from)?;
                handshake(io).await
            },
        }
    }
}

fn attach_permit(
    inner: &StdArc<Http1PoolInner>,
    tx: SendRequest<OrionRequestBody>,
    response: Response<Incoming>,
) -> Response<OrionResponseBody> {
    let (parts, body) = response.into_parts();
    if parts.status == StatusCode::SWITCHING_PROTOCOLS {
        return Response::from_parts(parts, TimeoutBody::new(None, PolyBody::from(body)).into());
    }
    // Empty bodies are often never polled (`is_end_stream` already true). Recycle now.
    if http_body::Body::is_end_stream(&body) {
        if !tx.is_closed() {
            inner.release(tx);
        }
        return Response::from_parts(parts, TimeoutBody::new(None, PolyBody::from(body)).into());
    }
    Response::from_parts(
        parts,
        OnEndBody::new(TimeoutBody::new(None, PolyBody::from(body)))
            .with_on_end(BodyEndPermit::Http1(Http1Permit::new(StdArc::clone(inner), tx))),
    )
}

async fn handshake<T>(io: T) -> Result<SendRequest<OrionRequestBody>>
where
    T: Read + Write + Unpin + Send + 'static,
{
    let (mut sender, conn) = Http1Builder::new().writev(false).handshake(io).await.map_err(Error::from)?;
    tokio::spawn(async move {
        if let Err(err) = conn.with_upgrades().await {
            debug!("upstream http1 connection closed: {err}");
        }
    });
    sender.ready().await.map_err(Error::from)?;
    Ok(sender)
}

#[inline]
fn dst_uri(authority: &Authority, tls: bool) -> Result<Uri> {
    Uri::builder()
        .scheme(if tls { http::uri::Scheme::HTTPS } else { http::uri::Scheme::HTTP })
        .authority(authority.clone())
        .path_and_query(http::uri::PathAndQuery::from_static("/"))
        .build()
        .map_err(Error::from)
}
