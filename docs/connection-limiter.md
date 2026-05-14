# Connection Limiter

The `connection_limit` network filter caps the number of active TCP connections accepted by a filter chain. When the limit is reached, new connections are either closed immediately or held open for a configurable delay before being closed, slowing down malicious clients and reducing the effectiveness of connection-flood attacks. Use this filter to protect upstream services from connection exhaustion and to enforce resource bounds at the proxy layer.

## Configuration

The filter is identified by the type URL `type.googleapis.com/envoy.extensions.filters.network.connection_limit.v3.ConnectionLimit`.

| Field | Type | Required | Description |
|---|---|---|---|
| `stat_prefix` | string | yes | Prefix for the filter's statistics. |
| `max_connections` | uint64 | yes | Maximum number of active connections allowed on this filter chain. |
| `delay` | duration | no | If set, connections that exceed the limit are held open for this duration before being closed. If unset, excess connections are closed immediately. |

## Example

The filter must be placed before the terminal network filter (`tcp_proxy` or `http_connection_manager`) in the filter chain.

```yaml
filterChains:
  - name: my_filter_chain
    filters:
      - name: envoy.filters.network.connection_limit
        typedConfig:
          "@type": type.googleapis.com/envoy.extensions.filters.network.connection_limit.v3.ConnectionLimit
          stat_prefix: cx_limit
          max_connections: 100
          delay: 1s
      - name: envoy.filters.network.tcp_proxy
        typedConfig:
          "@type": type.googleapis.com/envoy.extensions.filters.network.tcp_proxy.v3.TcpProxy
          stat_prefix: tcp
          cluster: my_cluster
```

## Behavior

The filter maintains a single connection counter per filter chain, shared across all worker runtimes. This ensures the cap is enforced exactly at the process level regardless of how many Tokio runtimes are handling connections concurrently.

When a new connection arrives:

- If the active connection count is below `max_connections`, the connection is accepted and the counter is incremented.
- If the count has reached `max_connections` and no `delay` is configured, the connection is closed immediately.
- If the count has reached `max_connections` and a `delay` is configured, the connection is held open for the specified duration before being closed.

The counter is decremented as soon as a connection closes, making the slot available to the next incoming connection.

## Observability

The filter emits debug logs under the `connection_limit` target. To enable them, set the `RUST_LOG` environment variable before starting Orion:

```sh
RUST_LOG=connection_limit=debug ./orion
```

Three log events are emitted:

| Event | When |
|---|---|
| `connection accepted` | A connection was admitted; logs the new active count and the configured limit. |
| `connection rejected: limit reached` | A connection was denied; logs the active count, the limit, and the configured delay. |
| `connection closed` | A connection closed and the counter was decremented; logs the new active count. |
