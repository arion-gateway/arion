# External processing (`ext_proc`)

The `ext_proc` HTTP filter sends selected request and response phases to an
external gRPC processor. The processor can inspect traffic, change headers or
bodies, or return an immediate response. Use it for logic running outside the
proxy, including inference endpoint selection.

## Configure the filter and processor cluster

Starting with the [base configuration](configuration.md), insert this entry in
the HCM's `http_filters` before the router. This example sends only request
headers:

```yaml
- name: envoy.filters.http.ext_proc
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExternalProcessor
    grpc_service:
      envoy_grpc:
        cluster_name: ext_proc_server
    message_timeout: 5s
    failure_mode_allow: false
    processing_mode:
      request_header_mode: SEND
      response_header_mode: SKIP
      request_body_mode: NONE
      response_body_mode: NONE
```

Add this cluster alongside the application cluster in
`envoy_bootstrap.static_resources.clusters`. The processor speaks gRPC and
requires HTTP/2:

```yaml
- name: ext_proc_server
  type: STATIC
  connect_timeout: 0.25s
  typed_extension_protocol_options:
    envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
      "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
      explicit_http_config:
        http2_protocol_options: {}
  load_assignment:
    cluster_name: ext_proc_server
    endpoints:
      - lb_endpoints:
          - endpoint:
              address:
                socket_address: { address: 127.0.0.1, port_value: 18080 }
```

Run a processor implementing Envoy's bidirectional `ExternalProcessor.Process`
gRPC service at that address. Place ext_proc after authentication when it needs
verified identity. The repository also has a
[complete example](../arion-proxy/conf/arion-runtime-ext_proc.yaml).

## Select the phases

| Phase fields under `processing_mode` | Modes | Default |
| --- | --- | --- |
| `request_header_mode`, `response_header_mode` | `DEFAULT`, `SEND`, `SKIP` | Send |
| `request_body_mode`, `response_body_mode` | `NONE`, `STREAMED`, `BUFFERED`, `BUFFERED_PARTIAL`, `FULL_DUPLEX_STREAMED` | None |
| `request_trailer_mode`, `response_trailer_mode` | `DEFAULT`, `SEND`, `SKIP` | Skip |

With no `processing_mode`, both header phases are sent. Buffered bodies have a
default cap of 100 MiB; `grpc_service.envoy_grpc.max_receive_message_length`
can change the cap. Oversized buffered bodies return 413.

If a processor must read the body before answering request headers, use
`FULL_DUPLEX_STREAMED` and set
`send_body_without_waiting_for_header_response: true` on the filter. Otherwise,
waiting for the header reply can prevent the body from reaching the processor.

## Replies, timeouts and failures

A processor can mutate headers, replace or clear a body, or send an immediate
response. Omitting `mutation_rules` adds no mutation-rule restrictions;
`mutation_rules: {}` applies default protections for routing and `x-envoy-`
headers. Header mutations can trigger route re-matching;
`route_cache_action: CLEAR` or `RETAIN` makes that choice explicit.

`message_timeout` is the wait for each reply (default 200ms). With
`failure_mode_allow: false`, a processor message timeout returns 504, and a
processor/stream failure returns 500. With `true`, processing continues after
that failure. Choose this according to whether the processor enforces required
policy or supplies optional enrichment.

`allow_mode_override: true` lets processor replies change later phases;
`allowed_override_modes` can restrict those changes. `max_message_timeout`
bounds processor-requested timeout overrides. In `observability_mode: true`,
Arion mirrors phases without waiting for replies, and replies cannot mutate
traffic.

## Per-route settings

Put `typed_per_filter_config` on a route or virtual host and key it by the
filter's exact name in `http_filters`:

```yaml
typed_per_filter_config:
  envoy.filters.http.ext_proc:
    "@type": type.googleapis.com/envoy.extensions.filters.http.ext_proc.v3.ExtProcPerRoute
    overrides:
      processing_mode:
        request_body_mode: BUFFERED
      failure_mode_allow: true
```

Use `disabled: true` instead of `overrides` to bypass the processor. Overrides
can also select another `grpc_service`; they work with inline routes and RDS.

With the processor stopped, `curl -i http://127.0.0.1:8080/health` against the
base config plus this filter fails closed. Start the processor and return a
normal header response; the request then reaches the route and returns `ok`.

The supported transport is gRPC; `http_service` and attribute forwarding are
unsupported. Metadata forwarding is limited to `envoy.lb` served-endpoint
feedback for `OverrideHost` clusters, enabled with
`metadata_options.forwarding_namespaces.untyped: [envoy.lb]`; it requires
response-header processing and cannot be overridden per route.

See the website's [external processing guide](https://arion-gateway.dev/docs/extend/ext-proc/)
and [filter reference](https://arion-gateway.dev/reference/filters/http/ext-proc/)
for mutation fields and endpoint feedback details. Adaptation and license
details are in the [docs index](README.md#keeping-these-guides-current).
