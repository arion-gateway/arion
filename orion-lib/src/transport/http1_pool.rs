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
        h1_permit::{Http1Idle, Http1Permit},
        poly_body::PolyBody,
        timeout_body::TimeoutBody,
    },
    thread_local::{LocalBuilder, ThreadLocalObject},
    Error, OrionRequestBody, OrionResponseBody, Result,
};
use http::{uri::Authority, Request, Response, StatusCode, Uri};
use hyper::{
    body::Incoming,
    client::conn::http1::Builder as Http1Builder,
    client::conn::http1::SendRequest,
    rt::{Read, Write},
};
use hyper_rustls::HttpsConnector;
use std::{sync::Arc, time::Duration};
use tower::Service;
use tracing::debug;

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

impl LocalBuilder<Http1PoolArg, Arc<Http1Pool>> for Http1PoolBuilder {
    fn build(&self, arg: Http1PoolArg) -> Arc<Http1Pool> {
        Arc::new(Http1Pool {
            idle: Http1Idle::new(arg.idle_timeout),
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
    pools: Arc<ThreadLocalObject<Arc<Http1Pool>, Http1PoolBuilder, Http1PoolArg>>,
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
            pools: Arc::new(ThreadLocalObject::new(
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
            pools: Arc::new(ThreadLocalObject::new(
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

    pub fn is_tls(&self) -> bool {
        self.is_tls
    }

    pub fn local_pool(&self) -> Arc<Http1Pool> {
        Arc::clone(self.pools.get_local())
    }

    pub fn strong_count(&self) -> usize {
        Arc::strong_count(&self.pools)
    }
}

/// Per-endpoint HTTP/1 pool of `hyper::client::conn` senders.
///
/// A sender is checked out for a request and returned only after the response
/// body is fully streamed (or dropped at end-of-stream).
pub struct Http1Pool {
    idle: Arc<Http1Idle>,
    connect: Http1Connect,
    dst: Uri,
    is_tls: bool,
}

impl std::fmt::Debug for Http1Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Http1Pool")
            .field("idle", &self.idle.len())
            .field("is_tls", &self.is_tls)
            .finish_non_exhaustive()
    }
}

impl Http1Pool {
    pub async fn send(self: &Arc<Self>, mut req: Request<OrionRequestBody>) -> Result<Response<OrionResponseBody>> {
        origin_form(req.uri_mut());
        let mut tx = self.checkout().await?;
        let response = tx.send_request(req).await.map_err(Error::from)?;
        Ok(attach_permit(Arc::clone(self), tx, response))
    }

    async fn checkout(&self) -> Result<SendRequest<OrionRequestBody>> {
        loop {
            let idle = self.pop_idle();
            if let Some(mut tx) = idle {
                if tx.is_closed() {
                    continue;
                }
                match tx.ready().await {
                    Ok(()) => return Ok(tx),
                    Err(_) => continue,
                }
            }
            return self.connect_one().await;
        }
    }

    fn pop_idle(&self) -> Option<SendRequest<OrionRequestBody>> {
        self.idle.pop()
    }

    fn release(&self, tx: SendRequest<OrionRequestBody>) {
        self.idle.release(tx);
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
    pool: Arc<Http1Pool>,
    tx: SendRequest<OrionRequestBody>,
    response: Response<Incoming>,
) -> Response<OrionResponseBody> {
    let (parts, body) = response.into_parts();
    if parts.status == StatusCode::SWITCHING_PROTOCOLS {
        return Response::from_parts(parts, TimeoutBody::new(None, PolyBody::from(body)));
    }
    // Empty bodies are often never polled (`is_end_stream` already true). Recycle now.
    if http_body::Body::is_end_stream(&body) {
        if !tx.is_closed() {
            pool.release(tx);
        }
        return Response::from_parts(parts, TimeoutBody::new(None, PolyBody::from(body)));
    }
    Response::from_parts(
        parts,
        TimeoutBody::new(None, PolyBody::from(body)).with_on_end(Http1Permit::new(Arc::clone(&pool.idle), tx)),
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

fn dst_uri(authority: &Authority, tls: bool) -> Result<Uri> {
    Uri::builder()
        .scheme(if tls { http::uri::Scheme::HTTPS } else { http::uri::Scheme::HTTP })
        .authority(authority.clone())
        .path_and_query(http::uri::PathAndQuery::from_static("/"))
        .build()
        .map_err(Error::from)
}

fn origin_form(uri: &mut Uri) {
    let path = match uri.path_and_query() {
        Some(path) if path.as_str() != "/" => {
            let mut parts = http::uri::Parts::default();
            parts.path_and_query = Some(path.clone());
            Uri::from_parts(parts).expect("path is valid uri")
        },
        _ => Uri::default(),
    };
    *uri = path;
}
