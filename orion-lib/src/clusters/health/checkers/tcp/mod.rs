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

#[cfg(test)]
mod tests;

use std::sync::Arc;

use futures::{future::BoxFuture, FutureExt, TryFutureExt};
use orion_configuration::config::cluster::health_check::{ClusterHealthCheck, TcpHealthCheck};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, Notify},
    task::JoinHandle,
};

use crate::{
    clusters::health::{checkers::checker::HealthCheckerLoop, counter::HealthStatusCounter, EndpointId},
    transport::TcpChannelConnector,
    EndpointHealthUpdate, Error,
};

use super::checker::{IntervalWaiter, ProtocolChecker, WaitInterval};

const DEFAULT_MAX_PAYLOAD_BUFFER_SIZE: usize = 0x10_0000; // 1 MB

#[allow(clippy::too_many_arguments)]
pub fn spawn_tcp_health_checker(
    endpoint: EndpointId,
    cluster_config: ClusterHealthCheck,
    protocol_config: TcpHealthCheck,
    channel: TcpChannelConnector,
    sender: mpsc::Sender<EndpointHealthUpdate>,
    stop_signal: Arc<Notify>,
) -> JoinHandle<Result<(), Error>> {
    let interval_waiter = IntervalWaiter;
    spawn_tcp_health_checker_impl::<_, _, DEFAULT_MAX_PAYLOAD_BUFFER_SIZE>(
        endpoint,
        cluster_config,
        protocol_config,
        sender,
        stop_signal,
        (channel, interval_waiter),
    )
}

trait TcpClient
where
    Self::Stream: AsyncRead + AsyncWrite,
{
    type Stream;
    fn connect(&self) -> BoxFuture<'static, std::result::Result<Self::Stream, Error>>;
}

impl TcpClient for TcpChannelConnector {
    type Stream = crate::transport::AsyncInstrumentedStream;
    fn connect(&self) -> BoxFuture<'static, std::result::Result<Self::Stream, Error>> {
        self.connect(None).map(|result| result.map(|channel| channel.stream)).map_err(Error::from).boxed()
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_tcp_health_checker_impl<T, W, const MAX_PAYLOAD_BUFFER_SIZE: usize>(
    endpoint: EndpointId,
    cluster_config: ClusterHealthCheck,
    protocol_config: TcpHealthCheck,
    sender: mpsc::Sender<EndpointHealthUpdate>,
    stop_signal: Arc<Notify>,
    dependencies: (T, W),
) -> JoinHandle<Result<(), Error>>
where
    W: WaitInterval + Send + 'static,
    T: TcpClient + Send + Sync + 'static,
    T::Stream: Unpin + Send + 'static,
{
    tracing::debug!(
        "Starting HTTP health checks of endpoint {:?} in cluster {:?}",
        endpoint.endpoint,
        endpoint.cluster
    );

    let (tcp_client, interval_waiter) = dependencies;

    let tcp_checker = TcpChecker::<_, MAX_PAYLOAD_BUFFER_SIZE> { tcp_client, config: protocol_config };
    let check_loop =
        HealthCheckerLoop::new(endpoint, cluster_config, sender, stop_signal, interval_waiter, tcp_checker);

    check_loop.spawn()
}

struct TcpChecker<T, const MAX_PAYLOAD_BUFFER_SIZE: usize> {
    tcp_client: T,
    config: TcpHealthCheck,
}

impl<T, const MAX_PAYLOAD_BUFFER_SIZE: usize> ProtocolChecker for TcpChecker<T, MAX_PAYLOAD_BUFFER_SIZE>
where
    T: TcpClient + Send,
    T::Stream: AsyncRead + AsyncWrite + Unpin + Send,
{
    type Response = ();

    async fn check(&mut self) -> Result<Self::Response, Error> {
        let mut stream = self.tcp_client.connect().await?;

        if let Some(send_payload) = &self.config.send {
            stream.write_all(send_payload).await?;
        }

        if !self.config.receive.is_empty() {
            let mut matcher = PayloadMatcher::<_, MAX_PAYLOAD_BUFFER_SIZE>::new(&mut stream, &self.config.receive);
            return matcher.try_match().await;
        }

        Ok(())
    }

    fn process_response(
        &self,
        _endpoint: &EndpointId,
        counter: &mut HealthStatusCounter,
        _response: &Self::Response,
    ) -> Option<orion_configuration::config::cluster::HealthStatus> {
        counter.add_success()
    }
}

// See the description of the pattern matcher:
// https://www.envoyproxy.io/docs/envoy/latest/api-v3/config/core/v3/health_check.proto#envoy-v3-api-msg-config-core-v3-healthcheck-tcphealthcheck
struct PayloadMatcher<'a, T, const MAX_PAYLOAD_BUFFER_SIZE: usize>
where
    T: AsyncRead + Unpin,
{
    buffer: Vec<u8>,
    stream: &'a mut T,
    payloads: &'a [Vec<u8>],
    payload_size: usize,
}

impl<'a, T, const MAX_PAYLOAD_BUFFER_SIZE: usize> PayloadMatcher<'a, T, MAX_PAYLOAD_BUFFER_SIZE>
where
    T: AsyncRead + Unpin,
{
    fn new(stream: &'a mut T, payloads: &'a [Vec<u8>]) -> Self {
        let payload_size = payloads.iter().map(Vec::len).sum();
        Self { buffer: Vec::new(), stream, payloads, payload_size }
    }

    async fn try_match(&'a mut self) -> Result<(), Error> {
        let mut more_bytes = 1024_usize;

        if self.payload_size == 0 {
            return Ok(());
        }

        // This algorithm just keeps reading data until the payload matches
        // or there is a timeout. It could be improved by discarding the
        // buffer if the head of the payload is not found, and only reading
        // the remaining bytes if the payload partially matches.
        // However, the complexity of that code doesn't seem justified given
        // the small benefits, and would require extensive unit tests.

        self.recv_exact(self.payload_size).await?;

        loop {
            if self.matches() {
                return Ok(());
            }
            self.recv_at_most(more_bytes).await?;

            // Let's be increasingly hungry for more data until we reach the maximum
            more_bytes = more_bytes.saturating_mul(2);
        }
    }

    fn matches(&self) -> bool {
        let mut index = 0;

        for payload in self.payloads {
            if payload.is_empty() {
                continue;
            }

            let Some(buffer) = &self.buffer.get(index..) else {
                tracing::error!("Unexpected out-of-bounds error when verifying the TCP payload in health checker");
                return false;
            };

            let Some(payload_index) = buffer.windows(payload.len()).position(|window| window == payload) else {
                return false;
            };

            index += payload_index;
        }

        true
    }

    async fn recv_at_most(&mut self, bytes_to_read: usize) -> Result<(), Error> {
        let prev_size = self.buffer.len();

        if prev_size >= MAX_PAYLOAD_BUFFER_SIZE || bytes_to_read == 0 {
            return Err("payload buffer too big".into());
        }

        let available_space = MAX_PAYLOAD_BUFFER_SIZE - prev_size;
        let to_read = bytes_to_read.min(available_space);

        // 1. Ensure the Vec has enough capacity without changing its 'length'.
        // reserve() might reallocate if needed, but doesn't write any bytes.
        self.buffer.reserve(to_read);

        // 2. UNSAFE: Create a slice that points to the uninitialized memory
        // area after the current end of the buffer.
        // SAFETY: the buffer has been reserved, so it's safe to write up to 'to_read' bytes starting from 'prev_size'.
        #[allow(clippy::multiple_unsafe_ops_per_block)]
        let received = unsafe {
            // Get a pointer to the start of the uninitialized area
            let ptr = self.buffer.as_mut_ptr().add(prev_size);

            // Build a mutable slice of raw memory.
            // WARNING: We must NOT read from this slice before writing to it,
            // as reading uninitialized memory is Undefined Behavior.
            let slice = std::slice::from_raw_parts_mut(ptr, to_read);

            // Read directly into that raw memory
            self.stream.read(slice).await?
        };

        if received == 0 {
            return Err("end of stream".into());
        }

        // SAFETY: 3. Now that we know 'received' bytes are valid data,
        // we can safely update the Vec's length.
        unsafe { self.buffer.set_len(prev_size + received) }

        Ok(())
    }

    async fn recv_exact(&mut self, bytes_to_read: usize) -> Result<(), Error> {
        if bytes_to_read == 0 {
            return Ok(());
        }

        let prev_size = self.buffer.len();

        // 1. Ensure capacity without zeroing the memory.
        // This is O(1) if capacity is already sufficient.
        self.buffer.reserve(bytes_to_read);

        // 2. UNSAFE: Create a mutable slice from uninitialized memory.
        // We must ensure the read_exact succeeds before we 'trust' these bytes.
        // SAFETY: The buffer has been reserved, so it's safe to write up to 'bytes_to_read' bytes starting from 'prev_size'.
        #[allow(clippy::multiple_unsafe_ops_per_block)]
        let read_result = unsafe {
            let ptr = self.buffer.as_mut_ptr().add(prev_size);
            let slice = std::slice::from_raw_parts_mut(ptr, bytes_to_read);
            self.stream.read_exact(slice).await
        };

        match read_result {
            Ok(_) => {
                // 3. UNSAFE: Successfully read exact bytes, update the length.
                // SAFETY: We have just read 'bytes_to_read' bytes into the uninitialized area, so it's now valid data.
                unsafe { self.buffer.set_len(prev_size + bytes_to_read) }
                Ok(())
            },
            Err(e) => {
                // If read_exact fails, the buffer length remains prev_size,
                // naturally discarding the uninitialized memory area.
                Err(e.into())
            },
        }
    }
}
