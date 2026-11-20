# Common HTTP filters

HTTP filters live in the HTTP connection manager's `http_filters` list. They
run in declared order on requests; response processing unwinds the filters that
ran in reverse order. A filter can answer immediately and stop the request.
The router is mandatory and must be last.

Start with the [base configuration](configuration.md) and insert the following
filter entries before its router as needed. A typical order is CORS, JWT,
authorization, request limits, ext_proc or Wasm, MCP, then router. Put a limiter
earlier when it should also count rejected traffic.

| Need | Filter | Result when blocked |
| --- | --- | --- |
| Browser cross-origin access | CORS | Browser enforces the returned CORS headers |
| Verify bearer tokens | JWT authentication | 401 |
| Browser OAuth2 login and session cookies | [OAuth2](oauth2.md) | Redirect to IdP (or 401) |
| Match HTTP headers/method/path | HTTP RBAC | 403 |
| Evaluate policy over JWT claims and request context | Cedar | 403 |
| Bound aggregate or per-caller request rate | Local/user rate limit | 429 by default |
| Run logic in an external service | [ext_proc](ext-proc.md) | Configurable failure handling |
| Run a custom module in the proxy | Wasm | Plugin-defined |
| Expose tools to agents | [MCP gateway](mcp/mcp-gateway.md) | Per-tool policy and MCP errors |

## CORS

Place CORS before authentication so it can answer browser preflights without a
token. This policy allows one web origin:

```yaml
- name: envoy.filters.http.cors
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.cors.v3.CorsPolicy
    allow_origin_string_match:
      - exact: https://app.example.com
    allow_methods: "GET, POST, OPTIONS"
    allow_headers: "content-type, authorization"
    allow_credentials: true
    max_age: "600"
```

Allowed preflights get 204; requests without `Origin` pass through. A disallowed
origin, method or header gets no CORS permission headers and is forwarded;
the browser enforces the restriction. CORS does not authorize callers.
`envoy.extensions.filters.http.cors.v3.Cors` is also accepted for permissive
defaults. Per-route CORS overrides are supported via `typed_per_filter_config`
with `CorsPolicy`, while per-virtual-host CORS policies are unsupported.
`mcp-session-id` is automatically allowed and exposed.

## JWT authentication

Configure a token source explicitly; Arion has no implicit bearer-header
default. Replace the issuer, audience and JWKS file below with your identity
provider's values:

```yaml
- name: envoy.filters.http.jwt_authn
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.jwt_authn.v3.JwtAuthentication
    providers:
      idp:
        issuer: https://idp.example.com
        audiences: [api.example.com]
        from_headers:
          - name: Authorization
            value_prefix: "Bearer "
        local_jwks:
          filename: jwks.json
        payload_in_metadata: jwt_payload
        claim_to_headers:
          - claim_name: sub
            header_name: x-user-id
    rules:
      - match: { prefix: / }
        requires: { provider_name: idp }
```

The first matching rule chooses the provider. A request resolving to no
provider gets 401; `allow_missing` and composite requirements are unsupported.
This checkout expects the token's `aud` claim as an array, for example
`["api.example.com"]`; a string-valued audience fails token decoding.
`payload_in_metadata` publishes validated claims to Cedar and MCP tool RBAC.
`header_in_metadata` also publishes the token header for MCP `jwt_header`
rules. `claim_to_headers` supplies trusted identity headers to downstream
filters or backends. Tokens are stripped after validation unless
`forward: true` is set.

For rotating keys, use `remote_jwks` with an `http_uri` naming a configured
cluster. See the [JWT reference](https://arion-gateway.dev/reference/filters/http/jwt-authn/)
for its fields and supported requirements.

## HTTP RBAC and Cedar

HTTP RBAC supports header policies, including pseudo-headers such as `:method`
and `:path`. This entry blocks a legacy user agent:

```yaml
- name: envoy.filters.http.rbac
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.rbac.v3.RBAC
    rules:
      action: DENY
      policies:
        block_legacy:
          permissions:
            - any: true
          principals:
            - header:
                name: user-agent
                string_match: { prefix: "legacy-app/" }
```

Use `ALLOW` to admit only matching policies, or `DENY` to block matches.
This HTTP filter supports `any` and header matchers, with no policy condition
expressions or shadow rules. Use network RBAC for connection IP/SNI rules.

For expressions over verified JWT claims, use the Arion Cedar filter
(`type.googleapis.com/arion.extensions.filters.http.cedar.cedar_policy.v3.CedarPolicy`).
It requires inline `schema` and `policies` strings and denies when no policy
permits the request. JWT must run first with `payload_in_metadata`; otherwise
the principal is anonymous. Start from the
[Cedar example](../arion-proxy/conf/arion-runtime-cedar.yaml) and
[website guide](https://arion-gateway.dev/docs/security/cedar-policies/).

These filters authorize HTTP requests. Individual MCP tools have their own
[JWT RBAC](mcp/RBAC.md), including calls made inside code-mode scripts.

## Request rate limits

A local token bucket limits aggregate HTTP requests for a filter instance:

```yaml
- name: local_rate
  typed_config:
    "@type": type.googleapis.com/envoy.extensions.filters.http.local_ratelimit.v3.LocalRateLimit
    stat_prefix: http_rl
    token_bucket:
      max_tokens: 100
      tokens_per_fill: 10
      fill_interval: 1s
```

This permits a burst of 100, then replenishes 10 tokens per second. Exhaustion
returns 429. For separate budgets per identified caller, use the Arion user
limiter after JWT, using the `x-user-id` claim header configured above:

```yaml
- name: arion.filters.http.user_rate_limit
  typed_config:
    "@type": type.googleapis.com/arion.extensions.filters.http.user_rate_limit.v3.UserRateLimiter
    user_id_header_name: x-user-id
    user_rate_limits:
      - user_id: premium-user
        simple_rate_limit: { max_tokens: 100, rate: 50 }
      - simple_rate_limit: { max_tokens: 10, rate: 5 }
```

`rate` is tokens per second. An entry without `user_id` is the default for
other identified callers. A request missing the identifying header is not
limited; use authentication to establish that identity. These budgets are
local to one process. Connection admission and fleet-wide connection quotas
are separate: see [connection limits](connection-limiter.md),
[global network rate limits](global-rate-limiter.md) and the website's
[limiter comparison](https://arion-gateway.dev/docs/traffic/rate-limiting/).

## Per-route settings and custom logic

Routes use `typed_per_filter_config`, keyed by the filter's declared name.
For example, disable the `local_rate` filter on a health-check route:

```yaml
typed_per_filter_config:
  local_rate:
    "@type": type.googleapis.com/envoy.config.route.v3.FilterConfig
    disabled: true
```

CORS (`CorsPolicy`), local rate limit, RBAC and ext_proc also support replacing
settings with their own per-route types. Other filters vary; consult their
reference before using
an override. An override naming an undeclared filter is rejected.

For custom behavior, choose [ext_proc](ext-proc.md) for a gRPC service or
[Wasm](../arion-wasm-sdk/README.md) for a module loaded in the proxy. Wasm needs
`cargo build -p arion-proxy --features wasm`; its filter type is
`type.googleapis.com/envoy.extensions.filters.http.wasm.v3.Wasm`, and
`config.vm_config.code.local.filename` points to the module. The website's
[Wasm guide](https://arion-gateway.dev/docs/extend/wasm-filters/) covers loading
and plugin configuration.

See the website's [filter-chain model](https://arion-gateway.dev/docs/concepts/filter-chain/)
and [HTTP filter reference](https://arion-gateway.dev/reference/filters/http/)
for the complete supported fields. Adaptation and license details are in the
[docs index](README.md#keeping-these-guides-current).
