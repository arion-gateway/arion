# Arion Proxy

**The fastest programmable gateway for services, models and tools.**

Arion Proxy is a high-performance, memory-safe proxy written in Rust. It
natively supports the [Envoy](https://www.envoyproxy.io/) configuration
model and the commonly used Envoy features, and extends them with
Arion-native capabilities for AI workloads.

## Why Arion

- **Memory safe.** Implemented in Rust, avoiding entire classes of
  memory-management and data-race bugs and making Arion a robust and secure
  foundation for your traffic.
- **Envoy-native configuration.** Arion's configuration is generated from
  Envoy's xDS protobuf definitions and is consumed the same way Envoy
  consumes it — as static bootstrap config or dynamically from an existing
  xDS control plane. The commonly used Envoy features are supported:
  routing, load balancing, TLS, JWT authentication, global rate limiting,
  circuit breaking, connection limits, external processing, health checks,
  and more — see the example configurations in
  [arion-proxy/conf/](arion-proxy/conf/).
- **Native AI extensions.** Alongside Envoy feature parity, Arion adds its
  own extensions for AI traffic, such as an
  [MCP gateway](docs/mcp/mcp-gateway.md) with [RBAC](docs/mcp/RBAC.md) for
  routing model and tool traffic.
- **Programmable.** HTTP filters can be written in WebAssembly and loaded at
  runtime. The Wasm SDK lives in this repo
  ([arion-wasm-sdk](arion-wasm-sdk/)).
- **Observable.** Prometheus metrics, distributed tracing, and access logging.

## Quick start

### Build from source

```console
git clone https://github.com/arion-gateway/arion
cd arion
git submodule update --init --force
cargo build
```

### Run

```console
cargo run --bin arion -- --config arion-proxy/conf/arion-runtime.yaml
```

[arion-proxy/conf/](arion-proxy/conf/) contains further example
configurations (JWT, rate limiting, circuit breaking, health checks, and
more).

### Docker

```bash
# Build
docker build -t arion-proxy -f docker/Dockerfile .

# Run
docker run -p 8000:8000 --name arion-proxy arion-proxy

# Verify
curl -v http://localhost:8000/direct-response   # HTTP 200 with "meow! 🐱"
```

See [docker/README.md](docker/README.md) for detailed Docker options,
including testing load balancing against real backends.

## Cargo features

Optional features of the `arion-proxy` crate (enable with
`--features <name>`):

| Feature | Description |
|---------|-------------|
| `wasm` | HTTP Wasm filter host (Wasmtime). Required to load Wasm HTTP filters at runtime. |
| `metrics` | Metrics collection |
| `prometheus` | Prometheus metrics export (implies `metrics`) |
| `access-log` | Access logging |
| `tracing` | Distributed tracing |
| `config-dump` | Admin config dump support |
| `jemalloc` | Use jemalloc as the allocator |
| `instrumentation` | Internal instrumentation |
| `dhat-heap` | Heap profiling with dhat |

Example:

```console
cargo build -p arion-proxy --features wasm
```

## Runtime configuration

To control how many CPU cores/threads Arion uses, set the `ARION_CPU_LIMIT`
environment variable. This is especially useful in containerized
environments where access to `/sys/fs` is restricted; Arion uses the value
to size its worker thread pool.

## Documentation

See [docs/](docs/) for guides, including [metrics](docs/metrics.md),
[tracing](docs/tracing.md), [access logging](docs/access-log.md),
[global rate limiting](docs/global-rate-limiter.md),
[the MCP gateway](docs/mcp/mcp-gateway.md), and [demos](docs/demos.md).

## License

Arion Proxy is licensed under the
[Apache License, Version 2.0](./LICENSE).
