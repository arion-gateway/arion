# Arion documentation

These repository guides cover the configuration and filters most useful when
working with Arion locally. The [website](https://arion-gateway.dev/docs/)
has the full guides and field reference; this folder keeps a smaller set of
practical entry points alongside the code.

## Start here

| Guide | Covers |
| --- | --- |
| [Base configuration](configuration.md) | A working HTTP proxy, process settings, bootstrap forms and overrides |
| [HTTP filters](http-filters.md) | Filter order, CORS, JWT, RBAC, Cedar, request limits and Wasm |
| [External processing (`ext_proc`)](ext-proc.md) | gRPC processors, HTTP/2 clusters, processing modes and per-route settings |
| [MCP gateway](mcp/mcp-gateway.md) | Tools/code modes, REST tools, OpenAPI, federation, search and TDS |
| [MCP tool authorization](mcp/RBAC.md) | JWT metadata and per-tool policy |

The YAML examples use `envoy_bootstrap`, Arion's canonical configuration
dialect. Fragments say where they belong; commands assume the repository root.
Working configurations also live in [arion-proxy/conf](../arion-proxy/conf/).

## Existing focused guides

- Observability: [access logs](access-log.md), [metrics](metrics.md),
  [tracing](tracing.md) and [timezone](timezone.md).
- Connection protection: [connection limiter](connection-limiter.md) and
  [global network rate limiter](global-rate-limiter.md). HTTP request limits
  are covered in [HTTP filters](http-filters.md#request-rate-limits).
- Security: [OAuth2](oauth2.md), [certificate troubleshooting](certificates-troubleshooting.md)
  and a [JWT configuration](jwt/arion.yaml).
- Development: [demos](demos.md), [access-log formatter](arion-format.md),
  [string interner](arion-interner.md) and the [Wasm SDK](../arion-wasm-sdk/README.md).

