# Base configuration

An Arion YAML file has process settings at the top level and proxy resources
under `envoy_bootstrap`. Start with this dialect when writing a new config.

## A small HTTP proxy

Save this complete configuration as `arion.yaml`. It listens on port 8080,
answers `/health` itself and forwards other paths to an HTTP service on port
5678.

```yaml
runtime:
  num_cpus: 2
  num_runtimes: 1

logging:
  log_level: info

envoy_bootstrap:
  static_resources:
    listeners:
      - name: ingress
        address:
          socket_address: { address: 127.0.0.1, port_value: 8080 }
        filter_chains:
          - name: http
            filters:
              - name: http
                typed_config:
                  "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                  stat_prefix: ingress_http
                  codec_type: HTTP1
                  http_filters:
                    - name: envoy.filters.http.router
                      typed_config:
                        "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
                  route_config:
                    name: routes
                    virtual_hosts:
                      - name: api
                        domains: ["*"]
                        routes:
                          - match: { path: /health }
                            direct_response:
                              status: 200
                              body: { inline_string: "ok\n" }
                          - match: { prefix: / }
                            route: { cluster: api }
    clusters:
      - name: api
        type: STATIC
        connect_timeout: 0.25s
        lb_policy: ROUND_ROBIN
        load_assignment:
          cluster_name: api
          endpoints:
            - lb_endpoints:
                - endpoint:
                    address:
                      socket_address: { address: 127.0.0.1, port_value: 5678 }
```

Run from the repository root:

```bash
cargo run --bin arion -- --config arion.yaml
```

In another terminal:

```bash
curl -i http://127.0.0.1:8080/health  # 200, body: ok
```

To exercise forwarding, start a local backend in another terminal, then call
the proxy:

```bash
python3 -m http.server 5678 --bind 127.0.0.1
# From a separate terminal:
curl -i http://127.0.0.1:8080/
```

The listener accepts connections; its HTTP connection manager (HCM) runs
`http_filters` and matches `route_config`; the selected route names an upstream
cluster. Routes are tested in order, so put specific paths before the `/`
fallback. The router must be the last HTTP filter. Add authentication,
rate limits or [ext_proc](ext-proc.md) before it; see [HTTP filters](http-filters.md).

## Process settings

All these sections are optional:

| Key | Purpose |
| --- | --- |
| `runtime` | CPU count, worker runtimes and service threads. Defaults to detected CPUs, one worker runtime and one service thread. |
| `logging` | Process log filter (`log_level`, default `error`); optional `log_file` and `log_directory`. `RUST_LOG` overrides the filter. |
| `access_logging` | Access-log queues and file rotation. Destinations and formats belong to the listener/HCM; see [access logs](access-log.md). |
| `metrics` | Partition keys, custom metrics and renames; see [metrics](metrics.md). |
| `timezone` | Log timestamp timezone, for example `local: Europe/Dublin`; defaults to UTC. |

Runtime precedence is **environment > command-line flags > YAML > defaults**.
For example, `ARION_CPU_LIMIT` overrides both `--num-cpus` and
`runtime.num_cpus`. Common runtime variables are `ARION_GATEWAY_RUNTIMES` and
`ARION_SERVICE_THREADS`. CPU detection honours container cgroup quotas.
Start with runtime defaults and tune for your workload; scheduler knobs are not
required for a base configuration.

```bash
ARION_CPU_LIMIT=4 RUST_LOG=info cargo run --bin arion -- --config arion.yaml
```

## Bootstrap sources

- `--config arion.yaml` reads the Arion wrapper shown above.
- `--with-envoy-bootstrap envoy.yaml` reads a bare Envoy Bootstrap: its root
  has `static_resources`, `dynamic_resources`, `node` or `admin`, without the
  `envoy_bootstrap` wrapper.
- With both flags, process settings come from `--config`, while
  `--with-envoy-bootstrap` replaces its proxy resources.

The wrapper also accepts `bootstrap`, Arion's native serialization. A file
cannot set both `bootstrap` and `envoy_bootstrap`. Prefer `envoy_bootstrap` for
hand-written configuration. Static resources can coexist with resources
delivered by an xDS control plane; see the
[xDS example](../arion-proxy/conf/arion-runtime-xds.yaml).

Inside `envoy_bootstrap`, unknown fields fail parsing and unsupported Envoy
features set to non-default values fail conversion. Both snake_case and
camelCase protobuf field names are accepted. Top-level wrapper keys are less
strict: unknown keys are ignored, so check their spelling carefully.

For full fields and flags, see the website's
[file format](https://arion-gateway.dev/reference/configuration/file-format/),
[CLI](https://arion-gateway.dev/reference/configuration/cli/),
[environment variables](https://arion-gateway.dev/reference/configuration/environment-variables/)
and [Envoy compatibility matrix](https://arion-gateway.dev/reference/envoy-compatibility/supported-configuration/).
Adaptation and license details are in the [docs index](README.md#keeping-these-guides-current).
