# Arion MCP gateway

The MCP gateway is an HTTP filter that serves a Model Context Protocol endpoint
at `/mcp`. One registry combines static REST tools, OpenAPI-derived tools and
tools from upstream MCP servers. Agents use that registry through conventional
tool calls or sandboxed TypeScript in code mode.

## Add a REST tool

Start with the [base configuration](../configuration.md). Replace its HCM
`http_filters` list with the following. Keep the `api` cluster and route config;
the backend at `127.0.0.1:5678` must implement `GET /v1/weather/<city>`:

```yaml
http_filters:
  - name: arion.filters.http.mcp
    typed_config:
      "@type": type.googleapis.com/arion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway
      server_info:
        name: weather-tools
        version: "1.0.0"
      tools_mode: {}
      upstream_timeout: 15s
      max_upstream_response_bytes: 1048576
      tools:
        - namespace: weather
          name: get_forecast
          description: Get the weather forecast for a city.
          disclosure_mode: EAGER
          input_schema:
            inline_string: |
              {
                "type": "object",
                "properties": {
                  "city": { "type": "string" },
                  "days": { "type": "integer", "minimum": 1 }
                },
                "required": ["city"]
              }
          rest_backend:
            cluster: api
            method: GET
            path: "/v1/weather/{{city | encode_path_segment}}"
            query_params:
              - name: days
                source: days
  - name: envoy.filters.http.router
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
```

Every gateway must select exactly one of `tools_mode` and `code_mode`. Every
tool needs a namespace, local name, description and exactly one backend. Clients
address this tool as **`weather.get_forecast`**. Namespaces contain letters,
digits, `_` and `-`; local names may also contain dots. A qualified name must
be unique and at most 128 bytes. One namespace belongs to static/TDS tools,
one OpenAPI source, or one dynamic server.

REST calls resolve their configured cluster directly, without a
`cluster_header` or route-table lookup. Retain the HCM route configuration and
terminal router. The MCP filter returns 404 for unrecognized paths, including
the base example's `/health`; use a separate listener for ordinary HTTP routes.
Place [CORS and JWT](../http-filters.md) before MCP, and the router last.

## Connect a client

Start Arion with the edited configuration, then initialize a session:

```bash
MCP_ENDPOINT=http://127.0.0.1:8080/mcp

curl -is "$MCP_ENDPOINT" \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"curl","version":"1.0.0"}}}'
```

Copy the `mcp-session-id` response header:

```bash
SESSION_ID='paste-the-returned-session-id'

curl -s "$MCP_ENDPOINT" \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'

curl -s "$MCP_ENDPOINT" \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"weather.get_forecast","arguments":{"city":"Dublin","days":3}}}'
```

The list includes `weather.get_forecast`; its call sends
`GET /v1/weather/Dublin?days=3` to the `api` cluster. Without an output schema,
the upstream body is returned as text content. Initialization and listing work
without a running REST backend; a successful call requires one.

The endpoint supports `initialize`, `ping`, `tools/list` and `tools/call`.
Follow-up MCP requests carry the session header. Responses are JSON, although
clients must accept both JSON and event-stream media types. `DELETE /mcp`
ends the session; idle sessions expire after five minutes. `GET /mcp` returns
405, and there is no legacy `/sse` endpoint.

## REST mapping and bounds

- `path` and `body_template` render double-brace argument placeholders. Use
  `encode_path_segment` for caller-supplied path values and mark template
  arguments as required in the input schema.
- `query_params` maps a parameter `name` to a dot-path `source` in the arguments.
  Values are URL-encoded, arrays become repeated parameters and absent values
  are omitted.
- `body_json: user` serializes the `user` argument as the request body. It is
  mutually exclusive with `body_template`; both use JSON content type.
- `input_schema` validates REST arguments. `output_schema` parses and validates
  successful JSON responses and returns structured content. In code mode,
  those results become parsed JSON values. Empty schemas do not validate.
- `rest_backend.upstream_policy` can set `authority`, `timeout` and
  `retry_policy`. `authority` is required for `ORIGINAL_DST` clusters.

The gateway-wide defaults are a 15-second upstream timeout and a 1 MiB response
cap, including serialized MCP results. Results are buffered and returned whole.
Tool errors distinguish upstream failure, timeout, oversized response, invalid
output and overflow.

## Discovery and search

In tools mode, `disclosure_mode: EAGER` includes a tool in `tools/list`.
The default, `PROGRESSIVE`, leaves it out of that list and makes it discoverable
through `tool_search`. The gateway advertises `tool_search` automatically when
the caller has permitted progressive tools. Its arguments are `query`, optional
`namespace` and optional `limit`; results contain full tool definitions.

Search is stateless: it does not reveal tools in later `tools/list` results.
A permitted tool can be called by qualified name whether or not it was listed.
Tools-mode clients can also search without an MCP session:

```bash
curl -s http://127.0.0.1:8080/mcp/search \
  -H 'content-type: application/json' \
  -d '{"query":"weather forecast","namespace":"weather","limit":5}'
```

HTTP search covers every permitted tool; `tool_search` returns progressive
tools. Both enforce authorization. Authenticated clients must send their token
on search and subsequent MCP requests too.

Ranking defaults to local BM25 with five results. Under the gateway's
`typed_config`, `search.max_allowable_results` changes the cap, and an optional
`search.embeddings` block enables an OpenAI-compatible embeddings service using
`cluster`, `model_id` and `dimensions` (plus optional `path`, `timeout` and
`max_batch_size`). See the website's
[tool search guide](https://arion-gateway.dev/docs/mcp-gateway/tool-search/).

## OpenAPI and upstream MCP servers

These fragments belong under the gateway's `typed_config`. An OpenAPI 3.x
document supplies one REST-backed tool per selected operation, named
`namespace.operationId`:

```yaml
open_api_tool_sources:
  - namespace: petstore
    spec:
      filename: /etc/arion/petstore.yaml
    cluster: api
    disclosure_mode: EAGER
    include_operations: ["GET *"]
```

Operations can also be excluded. Unsupported operations fail loading by
default; `skip_unsupported_operations: true` skips them with a warning. The
document's first server URL supplies its base path, while the configured
cluster supplies the destination. See
[OpenAPI tools](https://arion-gateway.dev/docs/mcp-gateway/openapi-tools/)
for mapping limits.

To discover an upstream MCP server's whole catalog, define a cluster for that
server and add:

```yaml
dynamic_mcp_servers:
  - namespace: repo
    description: Repository tools.
    transport: StreamableHttp
    cluster: repo_mcp
    path: /mcp
    cache_duration: 5m
    disclosure_mode: PROGRESSIVE
```

An upstream `get_pull_request` becomes `repo.get_pull_request`; calls forward
the original local name. Discovery starts when the registry is used. After
the cache interval, a later list/search triggers refresh. Failed refreshes
retain the last good catalog. Without a cache duration, a successful fetch is
cached indefinitely; failed fetches use a 30-second retry backoff.

To pin one tool instead, use a static tool with this backend:

```yaml
mcp_server_backend:
  transport: StreamableHttp
  cluster: repo_mcp
  path: /mcp
```

The static tool's local `name` must match the upstream name. For both forms,
the cluster controls endpoints, TLS, HTTP version and upstream authority.
Set `StreamableHttp` explicitly; upstream SSE is unsupported. The upstream
server owns schemas and result shapes; configured static MCP input/output
schemas are ignored. This checkout uses `cluster`/`path` and
`disclosure_mode`, where some website examples still use URL-based fields.

## Code mode

Build with `cargo build -p arion-proxy --features codemode`, then replace
`tools_mode: {}` with `code_mode: {}`. The registry and authorization stay the
same, but `tools/list` returns only `execute`. Its `code` argument is an async
TypeScript function body:

```typescript
const forecast = await tools["weather.get_forecast"]({ city: "Dublin", days: 3 });
if (!forecast.ok) return { error: forecast.error.code };
return forecast.data;
```

Scripts discover tools through `catalog.namespaces`, `catalog.tools` and
`catalog.toolkits`, and call them through `tools`. Calls return an
`{ ok: true, data }` or `{ ok: false, error }` envelope. The sandbox has no
direct filesystem, environment or network access. `tool_search` and HTTP
`/mcp/search` are absent in code mode; use `catalog.tools.search` instead.

Default per-gateway limits include 30 seconds per execution, 100 host calls,
32 concurrent executions, eight concurrent tool calls per execution, 1 MiB
output and 64 MiB memory. Process-wide `runtime.codemode` settings cap the pool
and per-sandbox memory. Optional `code_mode.toolkits` bundle at least two tools
with composition instructions; a caller sees a toolkit only when every member
exists and is permitted. See the
[code-mode guide](https://arion-gateway.dev/docs/mcp-gateway/code-mode/) and
[SDK reference](https://arion-gateway.dev/reference/sdk/code-mode-sdk-and-limits/).

## Authorization and dynamic updates

[Tool RBAC](RBAC.md) hides unauthorized tools from lists, search and the
code-mode catalog, and rejects calls. JWT must run before MCP and publish its
validated claims through `payload_in_metadata`. Policies can be attached to a
static tool or applied to every tool from a dynamic server or OpenAPI source.

With an xDS control plane, add `tds: { config_name: main }` under the gateway's
`typed_config`. Delta TDS delivers `Tool`, `OpenApiToolSource`,
`DynamicMcpServer` and code-mode `Toolkit` resources. Resource IDs are
`<server_info.name>/<config_name>/<resource_name>`; for a tool, the resource
name is its qualified name. Neither scope component may contain `/`, and
TDS-enabled gateway server names must be unique per process. See
[TDS](https://arion-gateway.dev/docs/mcp-gateway/tool-discovery-tds/) for update
and removal rules.

For full field tables, see the
[website MCP reference](https://arion-gateway.dev/reference/filters/http/mcp-gateway/).
For the schema shipped here, see
[mcp_gateway.proto](../../envoy-data-plane-api/arion/extensions/filters/http/mcp/mcp_gateway.proto).
Adaptation and license details are in the [docs index](../README.md#keeping-these-guides-current).
