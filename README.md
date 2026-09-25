# Arion Proxy

<!--
[![LICENSE](https://img.shields.io/github/license/arion-gateway/arion)](/LICENSE) [![codecov](https://codecov.io/gh/kmesh-net/kmesh/graph/badge.svg?token=0EGQ84FGDU)](https://img.shields.io/github/license/arion-gateway/arion) 
-->

## Introduction

Arion Proxy is a high performance and memory safe implementation of popular [Envoy Proxy](https://www.envoyproxy.io/). Arion Proxy is implemented in Rust using high-quality open source components. 

### Key features

**Memory Safety**

Rust programming language allows to avoid a whole lot of bugs related to memory management and data races making Arion Proxy a very robust and secure application.  


**Compatibility**

Arion Proxy configuration is generated from Envoy's xDS protobuf definitions. Arion Proxy aims to be a drop in replacement for Envoy.



## Quick Start

**Note:** To control how many CPU cores/threads Arion uses (especially in containers), set the `ARION_CPU_LIMIT` environment variable. In Kubernetes, use the Downward API:

```yaml
env:
    - name: ARION_CPU_LIMIT
        valueFrom:
            resourceFieldRef:
                resource: limits.cpu
                divisor: "1"
```

## CPU/Thread Limit Configuration

Arion can be configured to use a specific number of CPU cores/threads by setting the `ARION_CPU_LIMIT` environment variable. This is especially useful in containerized environments where access to `/sys/fs` may be restricted.

### Kubernetes Example (Downward API)

Add the following to your container spec to set `ARION_CPU_LIMIT` to the container's CPU limit:

```yaml
env:
    - name: ARION_CPU_LIMIT
        valueFrom:
            resourceFieldRef:
                resource: limits.cpu
                divisor: "1"
```

Arion will automatically use this value to determine the number of threads/cores.


### Building
```console
git clone https://github.com/arion-gateway/arion
cd arion
git submodule init
git submodule update --force
cargo build
```

Optional Cargo features (enable with `--features <name>` on `arion-proxy`):

| Feature | Description |
|---------|-------------|
| `wasm` | HTTP Wasm filter host (Wasmtime). Required to load Wasm HTTP filters at runtime. |
| `metrics` / `prometheus` | Metrics export |
| `access-log` | Access logging |
| `tracing` | Distributed tracing |

Example with Wasm support:
```console
cargo build -p arion-proxy --features wasm
```

### Running
```console
cargo run --bin arion -- --config arion-proxy/conf/arion-runtime.yaml
```

### Docker

Build and run with Docker:

```bash
# Build
docker build -t arion-proxy -f docker/Dockerfile .

# Run
docker run -p 8000:8000 --name arion-proxy arion-proxy

# Verify service
curl -v http://localhost:8000/direct-response # Should return HTTP 200 with "meow! 🐱"
```

### Testing with Backend Servers

For testing load balancing with real backend servers:

```bash
# Start two nginx containers
docker run -d -p 4001:80 --name backend1 nginx:alpine
docker run -d -p 4002:80 --name backend2 nginx:alpine

# Start Arion Proxy (uses host networking to access localhost:4001/4002)
docker run -d --network host --name arion-proxy arion-proxy

# Test load balancing
curl http://localhost:8000/  # Proxies to nginx backends!

# Cleanup
docker rm -f backend1 backend2 arion-proxy
```

For detailed Docker configuration options, see [docker/README.md](docker/README.md).


<!-- ## Contributing -->
<!-- If you're interested in being a contributor and want to get involved in developing Arion Proxy, please see [CONTRIBUTING](CONTRIBUTING.md) for more details on submitting patches and the contribution workflow. -->

## License

Arion Proxy is licensed under the
[Apache License, Version 2.0](./LICENSE).
