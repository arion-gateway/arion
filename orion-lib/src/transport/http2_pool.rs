// Copyright 2025 The kmesh Authors
// Copyright 2026 The arion-gateway Authors
//
// Modified by arion-gateway Authors.
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
use super::timer::PingoraTimer;
use crate::{
    body::{
        on_end_body::{BodyEndPermit, OnEndBody},
        poly_body::PolyBody,
        timeout_body::TimeoutBody,
    },
    thread_local::{LocalBuilder, ThreadLocalObject},
    Error, OrionRequestBody, OrionResponseBody, Result,
};
use http::{uri::Authority, Request, Response, Uri};
use hyper::{
    body::Incoming,
    client::conn::http2::{Builder as Http2Builder, SendRequest},
    rt::{Read, Write},
};
use hyper_rustls::HttpsConnector;
use hyper_util::rt::TokioExecutor;
use orion_configuration::config::cluster::http_protocol_options::Http2ProtocolOptions;
use parking_lot::Mutex;
use std::{
    sync::{
        atomic::{AtomicU32, AtomicU64, Ordering},
        Arc as StdArc,
    },
    time::Duration,
};
use tower::Service;
use tracing::debug;

const IDLE_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

struct H2Conn {
    tx: SendRequest<OrionRequestBody>,
    in_flight: StdArc<AtomicU32>,
    last_used: AtomicU64,
}

pub struct Http2PoolInner {
    conns: Mutex<Vec<H2Conn>>,
    idle_timeout: Duration,
    max_concurrent_streams: Option<u32>,
    clock: quanta::Clock,
}

impl Http2PoolInner {
    pub fn new(idle_timeout: Duration, max_concurrent_streams: Option<u32>) -> StdArc<Self> {
        let this = StdArc::new(Self {
            conns: Mutex::new(Vec::new()),
            idle_timeout,
            max_concurrent_streams,
            clock: quanta::Clock::new(),
        });
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

    fn cleanup_expired(&self) {
        let mut conns = self.conns.lock();
        if conns.is_empty() {
            return;
        }
        let now = self.clock.raw();
        conns.retain(|conn| {
            if conn.tx.is_closed() {
                return false;
            }
            if conn.in_flight.load(Ordering::Relaxed) > 0 {
                return true;
            }
            self.clock.delta(conn.last_used.load(Ordering::Relaxed), now) <= self.idle_timeout
        });
    }

    fn checkout(&self) -> Option<(SendRequest<OrionRequestBody>, StdArc<AtomicU32>)> {
        let conns = self.conns.lock();
        for conn in conns.iter() {
            if conn.tx.is_closed() {
                continue;
            }
            let in_flight = conn.in_flight.load(Ordering::Relaxed);
            let has_capacity = self.max_concurrent_streams.map(|max| in_flight < max).unwrap_or(true);
            if has_capacity {
                conn.in_flight.fetch_add(1, Ordering::Relaxed);
                conn.last_used.store(self.clock.raw(), Ordering::Relaxed);
                return Some((conn.tx.clone(), StdArc::clone(&conn.in_flight)));
            }
        }
        None
    }

    fn insert(&self, tx: SendRequest<OrionRequestBody>, in_flight: StdArc<AtomicU32>) {
        self.conns.lock().push(H2Conn { tx, in_flight, last_used: AtomicU64::new(self.clock.raw()) });
    }

    fn len(&self) -> usize {
        self.conns.lock().len()
    }
}

pub struct Http2StreamPermit {
    in_flight: StdArc<AtomicU32>,
}

impl Http2StreamPermit {
    pub fn on_body_end(self, _completed: bool) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Clone)]
enum Http2Connect {
    Plain(UnifiedConnector),
    Tls(HttpsConnector<UnifiedConnector>),
}

#[derive(Clone)]
struct Http2PoolArg {
    connect: Http2Connect,
    dst: Uri,
    idle_timeout: Duration,
    http2_options: Http2ProtocolOptions,
}

#[derive(Clone, Debug, Default)]
struct Http2PoolBuilder;

impl LocalBuilder<Http2PoolArg, StdArc<Http2Pool>> for Http2PoolBuilder {
    fn build(&self, arg: Http2PoolArg) -> StdArc<Http2Pool> {
        let max_concurrent_streams = arg.http2_options.max_concurrent_streams().and_then(|max| u32::try_from(max).ok());
        StdArc::new(Http2Pool {
            inner: Http2PoolInner::new(arg.idle_timeout, max_concurrent_streams),
            connect: arg.connect,
            dst: arg.dst,
            http2_options: arg.http2_options,
        })
    }
}

#[derive(Clone)]
pub struct Http2ClientExt {
    pools: StdArc<ThreadLocalObject<StdArc<Http2Pool>, Http2PoolBuilder, Http2PoolArg>>,
}

impl std::fmt::Debug for Http2ClientExt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http2ClientExt").finish()
    }
}

impl Http2ClientExt {
    pub fn plain(
        connector: UnifiedConnector,
        authority: &Authority,
        idle_timeout: Duration,
        http2_options: Http2ProtocolOptions,
    ) -> Result<Self> {
        Ok(Self {
            pools: StdArc::new(ThreadLocalObject::new(
                Http2PoolBuilder,
                Http2PoolArg {
                    connect: Http2Connect::Plain(connector),
                    dst: dst_uri(authority, false)?,
                    idle_timeout,
                    http2_options,
                },
            )),
        })
    }

    pub fn tls(
        connector: HttpsConnector<UnifiedConnector>,
        authority: &Authority,
        idle_timeout: Duration,
        http2_options: Http2ProtocolOptions,
    ) -> Result<Self> {
        Ok(Self {
            pools: StdArc::new(ThreadLocalObject::new(
                Http2PoolBuilder,
                Http2PoolArg {
                    connect: Http2Connect::Tls(connector),
                    dst: dst_uri(authority, true)?,
                    idle_timeout,
                    http2_options,
                },
            )),
        })
    }

    #[inline]
    pub fn local_pool(&self) -> StdArc<Http2Pool> {
        StdArc::clone(self.pools.get_local())
    }

    #[inline]
    pub fn local_pool_ref(&self) -> &Http2Pool {
        self.pools.get_local()
    }

    #[inline]
    pub fn strong_count(&self) -> usize {
        StdArc::strong_count(&self.pools)
    }
}

pub struct Http2Pool {
    inner: StdArc<Http2PoolInner>,
    connect: Http2Connect,
    dst: Uri,
    http2_options: Http2ProtocolOptions,
}

impl std::fmt::Debug for Http2Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http2Pool").field("conns", &self.inner.len()).finish_non_exhaustive()
    }
}

impl Http2Pool {
    pub async fn send(&self, req: Request<OrionRequestBody>) -> Result<Response<OrionResponseBody>> {
        let (mut tx, in_flight) = self.checkout().await?;
        match tx.send_request(req).await {
            Ok(response) => Ok(attach_stream_permit(in_flight, response)),
            Err(err) => {
                in_flight.fetch_sub(1, Ordering::Relaxed);
                Err(Error::from(err))
            },
        }
    }

    async fn checkout(&self) -> Result<(SendRequest<OrionRequestBody>, StdArc<AtomicU32>)> {
        if let Some(ready) = self.inner.checkout() {
            return Ok(ready);
        }
        let tx = self.connect_one().await?;
        let in_flight = StdArc::new(AtomicU32::new(1));
        self.inner.insert(tx.clone(), StdArc::clone(&in_flight));
        Ok((tx, in_flight))
    }

    async fn connect_one(&self) -> Result<SendRequest<OrionRequestBody>> {
        match &self.connect {
            Http2Connect::Plain(connector) => {
                let mut connector = connector.clone();
                let io = connector.call(self.dst.clone()).await.map_err(Error::from)?;
                handshake(io, &self.http2_options).await
            },
            Http2Connect::Tls(connector) => {
                let mut connector = connector.clone();
                let io = connector.call(self.dst.clone()).await.map_err(Error::from)?;
                handshake(io, &self.http2_options).await
            },
        }
    }
}

fn attach_stream_permit(in_flight: StdArc<AtomicU32>, response: Response<Incoming>) -> Response<OrionResponseBody> {
    let (parts, body) = response.into_parts();
    if http_body::Body::is_end_stream(&body) {
        in_flight.fetch_sub(1, Ordering::Relaxed);
        return Response::from_parts(parts, TimeoutBody::new(None, PolyBody::from(body)).into());
    }
    Response::from_parts(
        parts,
        OnEndBody::new(TimeoutBody::new(None, PolyBody::from(body)))
            .with_on_end(BodyEndPermit::Http2(Http2StreamPermit { in_flight })),
    )
}

async fn handshake<T>(io: T, opts: &Http2ProtocolOptions) -> Result<SendRequest<OrionRequestBody>>
where
    T: Read + Write + Unpin + Send + 'static,
{
    let mut builder = Http2Builder::new(TokioExecutor::new());
    builder.timer(PingoraTimer);

    if let Some(settings) = &opts.keep_alive_settings {
        builder.keep_alive_interval(settings.keep_alive_interval);
        if let Some(timeout) = settings.keep_alive_timeout {
            builder.keep_alive_timeout(timeout);
        }
        builder.keep_alive_while_idle(true);
    }

    let stream_window = opts.initial_stream_window_size();
    let conn_window = opts.initial_connection_window_size();
    if stream_window.is_none() && conn_window.is_none() {
        builder.adaptive_window(true);
    } else {
        builder.initial_stream_window_size(stream_window);
        builder.initial_connection_window_size(conn_window);
    }

    if let Some(max) = opts.max_concurrent_streams() {
        builder.initial_max_send_streams(max);
        builder.max_concurrent_reset_streams(max);
        if let Ok(max) = u32::try_from(max) {
            builder.max_concurrent_streams(max);
        }
    }

    let (mut sender, conn) = builder.handshake(io).await.map_err(Error::from)?;
    tokio::spawn(async move {
        if let Err(err) = conn.await {
            debug!("upstream http2 connection closed: {err}");
        }
    });
    sender.ready().await.map_err(Error::from)?;
    Ok(sender)
}

fn dst_uri(authority: &Authority, tls: bool) -> Result<Uri> {
    Uri::builder()
        .scheme(if tls { http::uri::Scheme::HTTPS } else { http::uri::Scheme::HTTP })
        .authority(authority.clone())
        .path_and_query(http::uri::PathAndQuery::from_static("/"))
        .build()
        .map_err(Error::from)
}
