### Overview

The network global rate limit filter enforces connection-level rate limiting before the HTTP stack is started. It fires on every new TCP connection, alongside RBAC and connection limit filters, making it the earliest point at which a connection can be rejected based on external policy.

The filter calls a [Rate Limit Service (RLS)](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/other_features/global_rate_limiting) over gRPC on each new connection. If the RLS returns an `OVER_LIMIT` response, the TCP connection is dropped immediately. When the RLS response includes a quota, Arion caches it process-wide for the duration of the quota window, so subsequent connections consume from the local bucket without an RLS round-trip. Once the quota is exhausted or expired, one connection refreshes it while others wait.

The filter is configured per filter chain. Because Arion selects a filter chain based on TLS SNI before invoking any filter, the rate limiter is already scoped to the correct tenant or domain by the time it runs.

### Differences from Envoy

**`domain` is optional on TLS listeners.**
In Envoy the `domain` field is always required. In Arion it may be omitted on listeners that have a TLS inspector configured. When omitted, the TLS SNI read from the connection is used as the RLS domain, enabling per-SNI rate limiting without static configuration. On non-TLS listeners `domain` remains mandatory — there is no SNI to fall back on, and Arion will reject the configuration at startup with a clear error.

When a static `domain` is configured it always takes priority over the SNI, regardless of what the client presents.

**Only static descriptors are supported.**
Envoy's HTTP-level rate limit filter supports dynamic descriptor entries populated from request attributes (headers, remote address, destination cluster, etc.) using access log format strings. The network-level filter — both in Envoy and in Arion — only supports static key/value descriptor entries declared in the configuration. Dynamic descriptor population from connection or request metadata is not yet supported.

### Configuration

The filter is declared as `envoy.filters.network.ratelimit` in a filter chain, before the terminal filter (HCM or TCP proxy).

| Field | Type | Required | Description |
| :--- | :--- | :--- | :--- |
| `stat_prefix` | string | yes | Prefix for statistics emitted by this filter instance. |
| `domain` | string | no\* | RLS namespace sent in every `ShouldRateLimit` request. \*Mandatory on non-TLS listeners. |
| `failure_mode_deny` | bool | no (default `false`) | Drop the connection when the RLS is unreachable. When `false`, connections are allowed through on RLS failure. |
| `descriptors` | list | yes | One or more static descriptor entries sent with every RLS request. Each descriptor is a list of `{key, value}` pairs. |
| `rate_limit_service.grpc_service` | GrpcService | yes | Address of the RLS. Supports both `envoy_grpc` (cluster reference) and `google_grpc` (direct URI). |

**TLS listener — static domain (all subdomains share one RLS namespace)**
```yaml
- name: envoy.filters.network.ratelimit
  typedConfig:
    "@type": type.googleapis.com/envoy.extensions.filters.network.ratelimit.v3.RateLimit
    stat_prefix: ratelimit_myservice
    domain: myservice.example
    failure_mode_deny: true
    descriptors:
      - entries:
          - key: destination_cluster
            value: myservice
    rate_limit_service:
      grpc_service:
        envoy_grpc:
          cluster_name: rls_cluster
```

**TLS listener — SNI-driven domain (each subdomain is its own RLS namespace)**
```yaml
- name: envoy.filters.network.ratelimit
  typedConfig:
    "@type": type.googleapis.com/envoy.extensions.filters.network.ratelimit.v3.RateLimit
    stat_prefix: ratelimit_myservice
    # domain omitted: TLS SNI is used as the RLS domain at connection time
    failure_mode_deny: false
    descriptors:
      - entries:
          - key: destination_cluster
            value: myservice
    rate_limit_service:
      grpc_service:
        envoy_grpc:
          cluster_name: rls_cluster
```

**Non-TLS listener — domain is mandatory**
```yaml
- name: envoy.filters.network.ratelimit
  typedConfig:
    "@type": type.googleapis.com/envoy.extensions.filters.network.ratelimit.v3.RateLimit
    stat_prefix: ratelimit_http
    domain: http.myservice.example   # required: no TLS inspector, no SNI available
    failure_mode_deny: true
    descriptors:
      - entries:
          - key: destination_cluster
            value: myservice
    rate_limit_service:
      grpc_service:
        envoy_grpc:
          cluster_name: rls_cluster
```

### Testing with the Envoy ratelimit reference implementation

The [envoy/ratelimit](https://github.com/envoyproxy/ratelimit) project is a Go gRPC service compatible with Arion's network global rate limit filter. Refer to its README for installation options; the quickest path for local testing is Docker.

**RLS configuration**

The ratelimit service loads YAML files from a directory. Create one file per domain under a config directory.

`/tmp/ratelimit/config/myservice.yaml` — limits `myservice.example` to 5 connections per minute:
```yaml
domain: myservice.example
descriptors:
  - key: destination_cluster
    value: myservice
    rate_limit:
      unit: MINUTE
      requests_per_unit: 5
```

`/tmp/ratelimit/config/tenant-a.yaml` — SNI-driven example, limits `tenant-a.myservice.example` independently:
```yaml
domain: tenant-a.myservice.example
descriptors:
  - key: destination_cluster
    value: myservice
    rate_limit:
      unit: MINUTE
      requests_per_unit: 2
```

**Running the RLS**

The config directory is mounted into the container as a volume — no rebuild is needed when editing files.

```bash
docker run --rm \
  -p 8081:8081 \
  -v /tmp/ratelimit:/tmp/ratelimit \
  -e RUNTIME_ROOT=/tmp/ratelimit \
  -e RUNTIME_SUBDIRECTORY=config \
  -e RUNTIME_IGNOREDOTFILES=true \
  -e USE_STATSD=false \
  -e DISABLE_STATS=true \
  -e LOG_LEVEL=debug \
  envoyproxy/ratelimit:master /bin/ratelimit
```

**Running Arion**

Use the `arion-runtime-global-rate-limit.yaml` example from `arion-proxy/conf/` as a base. It already includes the `rls_cluster` pointing to `127.0.0.1:8081`.

```bash
cargo run -p arion-proxy -- arion-proxy/conf/arion-runtime-global-rate-limit.yaml
```

**curl examples**

`--resolve` sets the SNI and the target address without requiring a real DNS entry. `-k` skips certificate verification for the self-signed test certs.

Allowed connection (within quota):
```bash
curl -sk --resolve cnpp1.example:8443:127.0.0.1 https://cnpp1.example:8443/
```
```
[upstream response]
```

After the quota is exhausted the TCP connection is dropped before the HTTP stack starts, so curl reports a connection-level error rather than an HTTP status code:
```bash
curl -sk --resolve cnpp1.example:8443:127.0.0.1 https://cnpp1.example:8443/
```
```
curl: (XX) [connection reset error]
```

SNI-driven domain — each subdomain is rate-limited independently against its own RLS namespace:
```bash
curl -sk --resolve tenant-a.myservice.example:8443:127.0.0.1 \
  https://tenant-a.myservice.example:8443/
```
```
[upstream response]
```

Testing `failure_mode_deny` — stop the RLS and observe what happens depending on the setting. With `failure_mode_deny: true` the connection is dropped; with `false` it is allowed through:
```bash
# Stop the RLS, then:
curl -sk --resolve cnpp1.example:8443:127.0.0.1 https://cnpp1.example:8443/
```
```
curl: (XX) [connection reset error]   # failure_mode_deny: true
[upstream response]                   # failure_mode_deny: false
```
