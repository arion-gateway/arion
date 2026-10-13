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

use super::connector::{UnifiedConnector, map_tls_connect_error};
use super::pool_permit::{BodyEndPermit, pooled_response};
use super::timer::PingoraTimer;
use crate::{
    ArionRequestBody, ArionResponseBody, Error, Result,
    event_error::UpstreamError,
    thread_local::{LocalBuilder, ThreadLocalObject},
};
use arion_configuration::config::cluster::http_protocol_options::Http2ProtocolOptions;
use http::{Request, Response, Uri, uri::Authority};
use hyper::{
    client::conn::http2::{Builder as Http2Builder, SendRequest},
    rt::{Read, Write},
};
use hyper_rustls::HttpsConnector;
use hyper_util::rt::TokioExecutor;
use parking_lot::Mutex;
use std::{
    sync::{
        Arc as StdArc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};
use tower::Service;
use tracing::debug;

const IDLE_CLEANUP_INTERVAL: Duration = Duration::from_secs(1);

struct H2Conn {
    tx: SendRequest<ArionRequestBody>,
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

    fn checkout(&self) -> Option<(SendRequest<ArionRequestBody>, Http2StreamPermit)> {
        let conns = self.conns.lock();
        for conn in conns.iter() {
            if conn.tx.is_closed() {
                continue;
            }
            let in_flight = conn.in_flight.load(Ordering::Relaxed);
            let has_capacity = self.max_concurrent_streams.map(|max| in_flight < max).unwrap_or(true);
            if has_capacity {
                conn.last_used.store(self.clock.raw(), Ordering::Relaxed);
                return Some((conn.tx.clone(), Http2StreamPermit::acquire(&conn.in_flight)));
            }
        }
        None
    }

    fn insert(&self, tx: SendRequest<ArionRequestBody>) -> Http2StreamPermit {
        let in_flight = StdArc::new(AtomicU32::new(0));
        let permit = Http2StreamPermit::acquire(&in_flight);
        self.conns.lock().push(H2Conn { tx, in_flight, last_used: AtomicU64::new(self.clock.raw()) });
        permit
    }

    fn len(&self) -> usize {
        self.conns.lock().len()
    }
}

/// One in-flight stream on an HTTP/2 connection. Taken at checkout and released on drop, so the
/// slot is freed even when the request future is cancelled before the response arrives.
pub struct Http2StreamPermit {
    in_flight: StdArc<AtomicU32>,
}

impl Http2StreamPermit {
    #[inline]
    fn acquire(in_flight: &StdArc<AtomicU32>) -> Self {
        in_flight.fetch_add(1, Ordering::Relaxed);
        Self { in_flight: StdArc::clone(in_flight) }
    }
}

impl Drop for Http2StreamPermit {
    #[inline]
    fn drop(&mut self) {
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
    pub async fn send(&self, req: Request<ArionRequestBody>) -> Result<Response<ArionResponseBody>> {
        // The permit is dropped (stream slot released) on error and on cancellation.
        let (mut tx, permit) = self.checkout().await?;
        let response = tx.send_request(req).await.map_err(|err| Error::from(UpstreamError::from(err)))?;
        Ok(pooled_response(response, BodyEndPermit::Http2(permit)))
    }

    async fn checkout(&self) -> Result<(SendRequest<ArionRequestBody>, Http2StreamPermit)> {
        if let Some(ready) = self.inner.checkout() {
            return Ok(ready);
        }
        let tx = self.connect_one().await?;
        let permit = self.inner.insert(tx.clone());
        Ok((tx, permit))
    }

    async fn connect_one(&self) -> Result<SendRequest<ArionRequestBody>> {
        match &self.connect {
            Http2Connect::Plain(connector) => {
                let mut connector = connector.clone();
                let io = connector.call(self.dst.clone()).await?;
                handshake(io, &self.http2_options).await
            },
            Http2Connect::Tls(connector) => {
                let mut connector = connector.clone();
                let io = connector.call(self.dst.clone()).await.map_err(map_tls_connect_error)?;
                handshake(io, &self.http2_options).await
            },
        }
    }
}

async fn handshake<T>(io: T, opts: &Http2ProtocolOptions) -> Result<SendRequest<ArionRequestBody>>
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

    let (mut sender, conn) = builder.handshake(io).await.map_err(|e| Error::upstream(UpstreamError::from(e)))?;
    tokio::spawn(async move {
        if let Err(err) = conn.await {
            debug!("upstream http2 connection closed: {err}");
        }
    });
    sender.ready().await.map_err(|e| Error::upstream(UpstreamError::from(e)))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::connector::ConnectUsing;
    use arion_interner::StringInterner;
    use bytes::Bytes;
    use http_body_util::{BodyExt, Full};
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use std::{convert::Infallible, net::SocketAddr};

    /// h2c server: `/slow` answers after a long delay, `/stream` sends a body in two chunks.
    async fn spawn_h2_server() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let service = service_fn(|req: Request<hyper::body::Incoming>| async move {
                        if req.uri().path() == "/slow" {
                            pingora_timeout::sleep(Duration::from_secs(30)).await;
                        }
                        Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"hello"))))
                    });
                    if let Err(err) = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                    {
                        debug!("test h2 server connection closed: {err}");
                    }
                });
            }
        });
        addr
    }

    fn pool_for(addr: SocketAddr) -> StdArc<Http2Pool> {
        let authority = Authority::try_from(addr.to_string()).unwrap();
        let connect_using = ConnectUsing::Socket {
            authority: authority.clone(),
            bind_device: None,
            connect_timeout: None,
            idle_timeout: None,
        };
        let connector = UnifiedConnector::from((&connect_using, "h2-pool-test".to_interned_str(), true));
        Http2PoolBuilder.build(Http2PoolArg {
            connect: Http2Connect::Plain(connector),
            dst: dst_uri(&authority, false).unwrap(),
            idle_timeout: Duration::from_secs(60),
            http2_options: Http2ProtocolOptions::default(),
        })
    }

    fn request(addr: SocketAddr, path: &str) -> Request<ArionRequestBody> {
        Request::builder()
            .version(http::Version::HTTP_2)
            .uri(format!("http://{addr}{path}"))
            .body(ArionRequestBody::default())
            .unwrap()
    }

    fn in_flight(pool: &Http2Pool) -> u32 {
        pool.inner.conns.lock().iter().map(|c| c.in_flight.load(Ordering::Relaxed)).sum()
    }

    #[tokio::test]
    async fn stream_slot_is_released_when_send_is_cancelled() {
        let addr = spawn_h2_server().await;
        let pool = pool_for(addr);

        let cancelled =
            pingora_timeout::fast_timeout::fast_timeout(Duration::from_millis(100), pool.send(request(addr, "/slow")))
                .await;
        assert!(cancelled.is_err(), "the slow request must time out");
        assert_eq!(in_flight(&pool), 0, "a cancelled send must not leak an in-flight stream");
    }

    #[tokio::test]
    async fn stream_slot_is_released_at_end_of_body_and_on_drop() {
        let addr = spawn_h2_server().await;
        let pool = pool_for(addr);

        let response = pool.send(request(addr, "/")).await.unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&*body, b"hello");
        assert_eq!(in_flight(&pool), 0, "completed body must release the stream");

        let response = pool.send(request(addr, "/")).await.unwrap();
        assert!(in_flight(&pool) <= 1);
        drop(response);
        assert_eq!(in_flight(&pool), 0, "dropped body must release the stream");
        assert_eq!(pool.inner.len(), 1, "both requests must share one connection");
    }
}
