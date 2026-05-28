# Orion MCP Gateway

The MCP gateway is an Orion HTTP filter that exposes a Model Context Protocol endpoint and maps MCP tool operations onto Orion-managed upstreams. A single gateway owns a tool registry. Tools can be configured statically, discovered from upstream MCP servers, or delivered incrementally through xDS.


## Key Concepts

```
MCP client
  |
  | POST /mcp                         GET /sse for legacy downstream SSE
  v
Orion listener -> HTTP connection manager -> MCP gateway filter
  |
  | tools/list and tools/call
  v
MCP tool registry
  |
  +-- REST backend: transcode tool call to HTTP and route through an Orion cluster
  +-- MCP server backend: forward tool call to an upstream MCP server
  +-- Dynamic MCP server: discover upstream tools with tools/list and materialise them
```

The default REST pattern is:

1. The MCP gateway receives `tools/call`.
2. The selected tool's REST backend renders an HTTP request.
3. The gateway adds a configured cluster-selection header, usually `x-mcp-target-cluster`.
4. The HCM route uses `cluster_header` to send the request to the tool's cluster.
5. The upstream HTTP response is converted back into an MCP `CallToolResult`.

The streamable HTTP MCP endpoint is `/mcp`. Orion also supports a legacy downstream SSE handshake on `/sse`; that is separate from upstream MCP server communication, which is Streamable HTTP by design.

## Quick Start

This minimal bootstrap exposes one MCP tool, `get_weather`, backed by a REST service.

```yaml
runtime:
  num_cpus: 2
  num_runtimes: 2

logging:
  log_level: info

envoy_bootstrap:
  static_resources:
    listeners:
      - name: mcp_listener
        address:
          socket_address:
            address: 0.0.0.0
            port_value: 8000
        filterChains:
          - name: mcp_filter_chain
            filters:
              - name: http_gateway
                typedConfig:
                  "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                  statPrefix: ingress_mcp
                  codecType: HTTP1
                  httpFilters:
                    - name: orion.filters.http.mcp
                      typed_config:
                        "@type": type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway
                        server_info:
                          name: weather-mcp-gateway
                          version: "1.0.0"
                        cluster_header: x-mcp-target-cluster
                        tools:
                          - name: get_weather
                            description: Get current weather and forecast information by city.
                            input_schema:
                              inline_string: |
                                {
                                  "type": "object",
                                  "properties": {
                                    "city": { "type": "string", "description": "City name" },
                                    "days": { "type": "integer", "description": "Forecast days" }
                                  },
                                  "required": ["city"]
                                }
                            rest_backend:
                              cluster: weather_api
                              method: GET
                              path: /v1/weather/{{city}}
                              query_params:
                                - name: days
                                  source: days
                    - name: envoy.filters.http.router
                      typedConfig:
                        "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
                  routeConfig:
                    name: mcp_routes
                    virtual_hosts:
                      - name: mcp
                        domains: ["*"]
                        routes:
                          - match:
                              prefix: /
                            route:
                              cluster_header: x-mcp-target-cluster

    clusters:
      - name: weather_api
        connect_timeout: 0.25s
        type: STATIC
        lb_policy: ROUND_ROBIN
        load_assignment:
          endpoints:
            - lb_endpoints:
                - endpoint:
                    address:
                      socket_address:
                        address: 127.0.0.1
                        port_value: 4001
```

Initialize a streamable HTTP session:

```bash
MCP_ENDPOINT=http://127.0.0.1:8000/mcp

curl -i -s "$MCP_ENDPOINT" \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"curl","version":"1.0.0"}}}'
```

Copy the returned `mcp-session-id` response header and reuse it:

```bash
SESSION_ID=<session-id-from-initialize>

curl -s "$MCP_ENDPOINT" \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'

curl -s "$MCP_ENDPOINT" \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"get_weather","arguments":{"city":"Dublin","days":3}}}'
```

`tools/call` requires a valid session. Initialize first, then send the returned `mcp-session-id` on every follow-up request.

## Filter Configuration

The MCP gateway filter type URL is:

```text
type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway
```

The filter can be used anywhere an HTTP filter can be configured before the router filter.

| Field | Required | Description |
|-------|----------|-------------|
| `server_info` | yes | Name and version reported in MCP `initialize`. `name` should be unique for each gateway when TDS is enabled. |
| `cluster_header` | no | Header the gateway writes on REST backend requests to select the upstream cluster. |
| `tools` | no | Static tool definitions. |
| `dynamic_mcp_servers` | no | Upstream MCP servers whose tools are discovered at runtime. |
| `tds` | no | xDS Tool Discovery Service binding for incremental `Tool` and `DynamicMcpServer` resources. |
| `semantic_search_tool` | no | Enables the synthetic `semantic_search` tool. |

### Server Info

```yaml
server_info:
  name: tenant-a-mcp
  version: "1.0.0"
```

`server_info.name` is the logical MCP server identity. Do not include `/` in the name. When `tds` is configured, Orion uses this name as part of the xDS resource scope.

### Cluster Header

For REST-backed tools, set `cluster_header` and configure the HCM route with the same header:

```yaml
cluster_header: x-mcp-target-cluster

routeConfig:
  name: mcp_routes
  virtual_hosts:
    - name: mcp
      domains: ["*"]
      routes:
        - match:
            prefix: /
          route:
            cluster_header: x-mcp-target-cluster
```

Each REST tool's `rest_backend.cluster` value is written into that header before routing.

## Static Tools

Every static tool has:

| Field | Description |
|-------|-------------|
| `name` | MCP tool name exposed to clients. |
| `description` | Human-readable description. Also used by semantic search. |
| `input_schema` | JSON Schema for tool call arguments. Empty means no validation. |
| `output_schema` | JSON Schema for REST response validation. Empty means raw response text is returned. |
| `rest_backend`, `mcp_server_backend`, or `function_graph_backend` | Exactly one backend. |
| `rbac` | Optional per-tool JWT RBAC policy. |
| `embedding` | Optional precomputed semantic-search vector. |

### REST Backend

REST tools transcode MCP `tools/call` into HTTP requests routed through Orion clusters.

```yaml
tools:
  - name: create_user
    description: Create a user profile.
    input_schema:
      inline_string: |
        {
          "type": "object",
          "properties": {
            "username": { "type": "string" },
            "email": { "type": "string" },
            "plan": { "type": "string" }
          },
          "required": ["username", "email", "plan"]
        }
    output_schema:
      inline_string: |
        {
          "type": "object",
          "properties": {
            "id": { "type": "string" },
            "status": { "type": "string" }
          },
          "required": ["id", "status"]
        }
    rest_backend:
      cluster: users_api
      method: POST
      path: /v1/users
      body_template:
        inline_string: |
          {
            "username": "{{username}}",
            "email": "{{email}}",
            "plan": "{{plan}}"
          }
```

REST backend fields:

| Field | Description |
|-------|-------------|
| `cluster` | Orion cluster name used for routing. |
| `method` | HTTP method, for example `GET`, `POST`, `PUT`, or `DELETE`. |
| `path` | Path template rendered with `{{argument}}` values. A leading `/` is added if missing. |
| `query_params` | Mappings from URL query parameter names to argument paths. |
| `body_template` | Optional template for the HTTP request body. Orion sets `content-type: application/json` when present. |
| `async` | For streamable HTTP clients, return the REST result on an event-stream response. Legacy downstream SSE already uses a separate response stream. |

Path and body templates use `{{name}}` syntax. Query parameter `source` supports dot notation:

```yaml
rest_backend:
  cluster: search_api
  method: GET
  path: /v1/search/{{tenant.id}}
  query_params:
    - name: q
      source: query.text
    - name: limit
      source: pagination.limit
```

Missing query parameter sources are omitted. Input schema validation is the best way to make required template variables explicit.

If `output_schema` is configured, Orion parses successful REST responses as JSON and validates them before returning structured MCP content. Without `output_schema`, Orion returns the upstream response body as text.

### MCP Server Backend

Static MCP-server tools forward `tools/call` to an upstream MCP server. Upstream MCP server communication is Streamable HTTP by design, so configure `transport: StreamableHttp`; upstream SSE is not supported. This is distinct from Orion's legacy downstream `/sse` support for clients.

```yaml
tools:
  - name: echo
    description: Forward an echo call to the upstream MCP server.
    mcp_server_backend:
      transport: StreamableHttp
      url: http://127.0.0.1:3001/mcp
```

For a static MCP-server backend, the exposed tool name is also the upstream tool name. If the client calls `echo`, Orion calls `echo` on the upstream MCP server.

The upstream MCP server is authoritative for its tool schemas and result shape. Orion does not apply REST-style input or output schema validation to MCP-server backend calls.

### FunctionGraph Backend

`function_graph_backend` is present in the proto but is not fully implemented. Ultimately, it will work very similarly to the REST backend, but optionally may have a simpler configuration to support the CloudEvent based API of FunctionGraph.

## Dynamic MCP Servers

`dynamic_mcp_servers` let Orion discover tools from upstream MCP servers at runtime. Orion calls `tools/list` on the upstream server, materialises each returned tool into the local registry, and exposes namespaced tool names.

```yaml
dynamic_mcp_servers:
  - name: github
    description: GitHub repository and pull request tools.
    transport: StreamableHttp
    url: http://127.0.0.1:3001/mcp
    cache_duration: 30s
```

If upstream `github` returns a tool named `get_pull_request`, Orion exposes it as:

```text
github__get_pull_request
```

When the client calls `github__get_pull_request`, Orion forwards the call upstream as `get_pull_request`.

Dynamic server fields:

| Field | Description |
|-------|-------------|
| `name` | Namespace prefix for materialised tools. |
| `description` | Human-readable server description. |
| `transport` | Configure `StreamableHttp` for upstream MCP communication. Dynamic discovery does not support upstream SSE. |
| `url` | Upstream MCP endpoint. |
| `cache_duration` | Optional TTL for the discovered tool list. If unset, a successful first fetch is cached indefinitely. |
| `rbac` | Optional RBAC policy applied to every materialised tool. |

Discovery and refresh behavior:

- Orion fetches dynamic tools when the registry is bootstrapped, usually by the first `tools/list` or semantic-search request.
- When `cache_duration` expires, Orion refreshes the upstream tool list.
- A successful refresh evicts the old tools for that dynamic server and inserts the latest list.
- A failed refresh logs a warning and leaves the previous materialised tools in place.

## Tool RBAC

Per-tool RBAC filters both `tools/list` and `tools/call`. Unauthorized tools are hidden from the tool list and denied if called directly.

RBAC depends on JWT data already being present in request extensions, so configure JWT authentication before the MCP gateway filter.

```yaml
httpFilters:
  - name: envoy.filters.http.jwt_authn
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.filters.http.jwt_authn.v3.JwtAuthentication
      providers:
        tenant_jwks:
          issuer: https://issuer.example.com/
          audiences: ["mcp-gateway"]
          local_jwks:
            inline_string: '{"keys":[]}'
      rules:
        - match:
            prefix: /
          requires:
            provider_name: tenant_jwks
  - name: orion.filters.http.mcp
    typed_config:
      "@type": type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway
      server_info:
        name: secure-mcp
        version: "1.0.0"
      cluster_header: x-mcp-target-cluster
      tools:
        - name: admin_delete_user
          description: Delete a user account as an administrator.
          input_schema:
            inline_string: '{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}'
          rbac:
            action: ALLOW
            permissions:
              - jwt_claim:
                  field: role
                  value: admin
          rest_backend:
            cluster: admin_api
            method: DELETE
            path: /v1/admin/users/{{id}}
```

RBAC permissions use OR logic:

- `ALLOW` permits access when any permission matches. No match means deny.
- `DENY` denies access when any permission matches. No match means allow.

See [Tool RBAC](RBAC.md) for the full RBAC reference.

## Semantic Search

### Motivation

Semantic search is an important feature when providing a large catalog of tools to agents. Semantic search provides a way to filter available MCP tools based on the type of work the agent wants to perform. To do this, Orion injects a small `semantic_search` discovery tool, ranks the available tools against the agents query, and then returns or reveals the most relevant tool definitions. Assisted discovery is the mode that avoids sending the full catalog in the initial `tools/list`; direct mode keeps the full list available for simpler clients.

This is really important when a tenant or platform team has hundreds of tools, but any single task for an agent only needs a small subset.

### Build And Bootstrap Requirements

Semantic search requires Orion to be built with the `mcp-semantic-search` feature:

```bash
cargo build -p orion-proxy --features mcp-semantic-search
```

It also requires a named top-level `embeddings_services` entry. The MCP gateway references that service by name:

```yaml
embeddings_services:
  - name: mcp-tools
    local:
      model_id: BAAI/bge-small-en-v1.5

envoy_bootstrap:
  static_resources:
    listeners:
      - name: mcp_listener
        # listener omitted
        filterChains:
          - name: main
            filters:
              - name: http_gateway
                typedConfig:
                  "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                  httpFilters:
                    - name: orion.filters.http.mcp
                      typed_config:
                        "@type": type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway
                        server_info:
                          name: searchable-mcp
                          version: "1.0.0"
                        cluster_header: x-mcp-target-cluster
                        semantic_search_tool:
                          embeddings_service: mcp-tools
                          enable_assisted_discovery: false
                          similarity:
                            top_k: 5
```

If `semantic_search_tool` is configured without the feature or without a resolvable embeddings provider, Orion rejects the configuration. If top-level `embeddings_services` are configured without the feature, Orion also rejects startup.

### Runnable Demo

The repository includes a semantic-search demo at `orion-e2e-tests/examples/mcp_semantic_search.rs`. It starts Orion, four local REST backends, a local FastEmbed embeddings provider, and a generated MCP gateway config.

Build the Orion binary with semantic search enabled:

```bash
cargo build -p orion-proxy --features mcp-semantic-search
```

Run the demo:

```bash
cargo run -p orion-e2e-tests --example mcp_semantic_search
```

The demo prints the generated config path, the MCP endpoint, backend addresses, a sample semantic-search result, and a copy-pasteable `curl` flow that captures the real `mcp-session-id` from `initialize`.

Useful options:

```bash
# Use assisted discovery and reveal only the best match.
cargo run -p orion-e2e-tests --example mcp_semantic_search -- \
  --assisted-discovery --top-k 1

# Keep the generated YAML so you can inspect or reuse the exact config.
cargo run -p orion-e2e-tests --example mcp_semantic_search -- --keep-config

# Load a pre-staged FastEmbed model snapshot instead of allowing a first-run download.
cargo run -p orion-e2e-tests --example mcp_semantic_search -- \
  --model-dir /path/to/fastembed/model/snapshot
```

Without `--model-dir`, FastEmbed may download model files before Orion starts listening. In offline environments, pre-stage the model files and pass `--model-dir`, or set `ORION_MCP_EMBEDDINGS_MODEL_DIR`.

### Local Embeddings

Local embeddings use FastEmbed in the Orion process.

```yaml
embeddings_services:
  - name: mcp-local
    local:
      model_id: BAAI/bge-small-en-v1.5
      model_dir: /var/lib/orion/models/bge-small-en-v1.5
      dimensions: 384
```

Supported local model IDs:

| Canonical ID | Short alias |
|--------------|-------------|
| `BAAI/bge-small-en-v1.5` | `bge-small-en-v1.5` |
| `sentence-transformers/all-MiniLM-L6-v2` | `all-MiniLM-L6-v2` |
| `BAAI/bge-small-zh-v1.5` | `bge-small-zh-v1.5` |

`model_dir` is optional. If it is omitted, FastEmbed may download model files on first use. If it is set, Orion loads the model files from that directory instead.

#### Offline FastEmbed Deployments

For environments where Orion does not have direct internet access, configure local embeddings with `model_dir` and pre-stage the model files before Orion starts. Without `model_dir`, FastEmbed may attempt a runtime model download and fail in offline deployments.

```yaml
embeddings_services:
  - name: mcp-offline
    local:
      model_id: BAAI/bge-small-en-v1.5
      model_dir: /opt/orion/models/bge-small-en-v1.5
      dimensions: 384
```

Operationally:

- Download or build the FastEmbed model snapshot on a machine with network access.
- Bake the snapshot into the Orion image or mount it into the Orion host/container before startup.
- Point `model_dir` at the snapshot directory for the selected model, not at the parent cache directory.
- Ensure the Orion process can read the directory and files.
- Keep `model_id` set to the supported local model ID that matches the staged snapshot. `dimensions` is optional; if set, it should match the model output dimensions unless you intentionally need an override.

The directory must contain the files FastEmbed needs for the selected model, including `tokenizer.json`, `config.json`, `special_tokens_map.json`, `tokenizer_config.json`, and the model's ONNX file.

### Remote Embeddings

Remote embeddings call an Orion cluster with an OpenAI-compatible shape:

Request:

```json
{ "model": "text-embedding-model", "input": ["text one", "text two"] }
```

Response:

```json
{
  "data": [
    { "index": 0, "embedding": [0.1, 0.2, 0.3] },
    { "index": 1, "embedding": [0.3, 0.2, 0.1] }
  ]
}
```

Config:

```yaml
embeddings_services:
  - name: mcp-remote
    remote:
      cluster: embeddings_api
      model_id: text-embedding-model
      path: /v1/embeddings
      timeout: 5s
      dimensions: 384

envoy_bootstrap:
  static_resources:
    clusters:
      - name: embeddings_api
        connect_timeout: 1s
        type: STATIC
        lb_policy: ROUND_ROBIN
        load_assignment:
          endpoints:
            - lb_endpoints:
                - endpoint:
                    address:
                      socket_address:
                        address: 127.0.0.1
                        port_value: 8081
```

### Search Text And Embeddings

For each tool, Orion builds semantic-search text from:

- tool name
- tool description
- input schema property names
- input schema property descriptions

If `embedding` is supplied on a tool, Orion uses that vector instead of generating one. The vector must match the configured provider dimensions.

```yaml
tools:
  - name: get_weather
    description: Get forecast, temperature, and sky condition information.
    embedding: [0.123, -0.456, 0.789]
    input_schema:
      inline_string: '{"type":"object","properties":{"city":{"type":"string","description":"City name"}}}'
    rest_backend:
      cluster: weather_api
      method: GET
      path: /weather/{{city}}
```

Ranking uses cosine similarity when query and tool embeddings are available. If query embedding fails, or a tool has no embedding available, Orion falls back to keyword overlap scoring.

### Direct Mode

Direct mode is the default when `enable_assisted_discovery` is `false`.

Flow:

1. Before a search, `tools/list` returns the normal tools plus `semantic_search`.
2. The client calls `semantic_search` with `user_query`.
3. Orion returns the top matching full tool definitions directly in the `semantic_search` result.
4. Orion records those matches as the session's active tools, so a later `tools/list` in the same session is narrowed to `semantic_search` plus the ranked tools. In direct mode, `tools/call` is still allowed for any RBAC-permitted tool in the registry.

The direct-mode result is an MCP `CallToolResult` whose first text content item contains a JSON array of MCP tool definitions.

```yaml
semantic_search_tool:
  embeddings_service: mcp-tools
  enable_assisted_discovery: false
  similarity:
    top_k: 3
```

Example call:

```bash
curl -s "$MCP_ENDPOINT" \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"semantic_search","arguments":{"user_query":"I need to forecast weather for a city"}}}'
```

### Assisted Discovery Mode

Assisted discovery minimizes the initial tool list more aggressively.

Flow:

1. Initial `tools/list` returns only `semantic_search`.
2. The client calls `semantic_search` with `user_query`.
3. Orion stores the matched tool names in the MCP session and emits a tool-list-changed notification.
4. A follow-up `tools/list` returns `semantic_search` plus the matched tools.
5. `tools/call` is restricted to the active tools for that session.

```yaml
semantic_search_tool:
  embeddings_service: mcp-tools
  enable_assisted_discovery: true
  similarity:
    top_k: 5
```

`similarity.top_k` controls the maximum number of ranked tools. If `similarity` is omitted, the default is 10. If `top_k` is `0`, results are unlimited.

## TDS And xDS Updates

### Motivation

TDS/xDS lets MCP gateway tool configuration evolve over time without replacing the full listener stack. Control planes can add, update, or remove MCP tools and dynamic MCP servers while leaving listener sockets, filter chains, routes, TLS, JWT, and unrelated HTTP filter configuration undisturbed.

This is useful for long-lived gateways where tool catalogs change more often than network topology.

### Enable TDS On A Gateway

Configure `tds.config_name` on the MCP gateway:

```yaml
typed_config:
  "@type": type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway
  server_info:
    name: tenant-a-mcp
    version: "1.0.0"
  cluster_header: x-mcp-target-cluster
  tds:
    config_name: main
  tools:
    - name: always_available
      description: Static tool that coexists with xDS tools.
      input_schema:
        inline_string: '{"type":"object","properties":{}}'
      rest_backend:
        cluster: tenant_a_api
        method: GET
        path: /health
```

TDS scopes are formed as:

```text
{server_info.name}/{tds.config_name}
```

For the example above, the scope is:

```text
tenant-a-mcp/main
```

`server_info.name` and `tds.config_name` must not contain `/`.

### xDS Resource Types

TDS uses xDS extension resources with these type URLs:

```text
type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.Tool
type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.DynamicMcpServer
```

Resource IDs must use this shape:

```text
{server_info.name}/{tds.config_name}/{resource_name}
```

Examples:

```text
tenant-a-mcp/main/get_weather
tenant-a-mcp/main/github
```

For `Tool` resources, Orion treats `resource_name` as canonical. If the payload's `Tool.name` differs, Orion logs a warning and exposes the tool under `resource_name` so later removes target the same name. For `DynamicMcpServer` resources, make `resource_name` match the `DynamicMcpServer.name` carried in the resource payload.

### Update And Remove Behavior

`Tool` update:

- Decodes an MCP `Tool` resource.
- Converts it through the same validation path as static config.
- Adds or replaces a provided tool in the scoped registry.
- Can overwrite a static tool with the same tool name.
- If semantic search is enabled and no embedding is supplied, Orion embeds the tool after the update.

`Tool` remove:

- Removes a provided tool whose name matches the resource name.
- Does not remove tools materialised from a dynamic MCP server.

`DynamicMcpServer` update:

- Decodes a dynamic server resource.
- Adds or replaces the server in the scoped registry.
- Fetches the upstream tools and materialises them with `{server_name}__{upstream_tool}` names.
- Replacing a dynamic server evicts that server's previously materialised tools.

`DynamicMcpServer` remove:

- Removes the dynamic server.
- Evicts all tools materialised from that server.

Malformed resource IDs and invalid payloads are NACKed. Updates for unknown scopes are ACKed as no-ops, so a control plane can safely publish resources before a matching listener exists.

### Scope Isolation

Two listeners can use the same `tds.config_name` as long as their `server_info.name` differs:

```yaml
# Listener A
server_info:
  name: tenant-a-mcp
  version: "1.0.0"
tds:
  config_name: main

# Listener B
server_info:
  name: tenant-b-mcp
  version: "1.0.0"
tds:
  config_name: main
```

A resource named `tenant-a-mcp/main/search` updates only tenant A. Tenant B will not see it.

For TDS-enabled gateways, Orion rejects duplicate `server_info.name` values within the same runtime. This prevents accidental xDS fan-out between unrelated listener scopes.

## Common Examples

### Public REST Tool

```yaml
tools:
  - name: get_status
    description: Return public service status.
    input_schema:
      inline_string: '{"type":"object","properties":{}}'
    rest_backend:
      cluster: status_api
      method: GET
      path: /status
```

### Admin-Only Tool

```yaml
tools:
  - name: delete_user
    description: Delete a user account.
    input_schema:
      inline_string: '{"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}'
    rbac:
      action: ALLOW
      permissions:
        - jwt_claim:
            field: role
            value: admin
    rest_backend:
      cluster: admin_api
      method: DELETE
      path: /users/{{id}}
```

### Dynamic MCP Server With Cache

```yaml
dynamic_mcp_servers:
  - name: repo
    description: Repository management tools.
    transport: StreamableHttp
    url: http://repo-mcp.internal:3001/mcp
    cache_duration: 5m
    rbac:
      action: ALLOW
      permissions:
        - jwt_claim:
            field: tenant
            value: tenant-a
```

### Semantic Search Direct Mode

```yaml
embeddings_services:
  - name: tenant-a-tools
    local:
      model_id: BAAI/bge-small-en-v1.5

envoy_bootstrap:
  static_resources:
    listeners:
      - name: tenant_a_mcp
        # listener and HCM omitted
        filterChains:
          - name: main
            filters:
              - name: http_gateway
                typedConfig:
                  "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                  httpFilters:
                    - name: orion.filters.http.mcp
                      typed_config:
                        "@type": type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway
                        server_info:
                          name: tenant-a-mcp
                          version: "1.0.0"
                        cluster_header: x-mcp-target-cluster
                        semantic_search_tool:
                          embeddings_service: tenant-a-tools
                          enable_assisted_discovery: false
                          similarity:
                            top_k: 5
```

### Semantic Search Assisted Discovery

```yaml
semantic_search_tool:
  embeddings_service: tenant-a-tools
  enable_assisted_discovery: true
  similarity:
    top_k: 5
```

### TDS Incremental Updates

Gateway binding:

```yaml
server_info:
  name: tenant-a-mcp
  version: "1.0.0"
tds:
  config_name: main
```

Control plane resource names:

```text
# Add or update a Tool payload named get_weather
tenant-a-mcp/main/get_weather

# Remove that Tool
tenant-a-mcp/main/get_weather

# Add or update a DynamicMcpServer payload named github
tenant-a-mcp/main/github

# Remove that DynamicMcpServer
tenant-a-mcp/main/github
```

### Multi-Tenant Gateway With Bind Device

This pattern runs multiple tenants in one Orion instance while keeping each tenant's listener and upstream traffic on tenant-specific network interfaces. Orion supports Linux `SO_BINDTODEVICE` via Envoy `socket_options`.

Important details:

- Listener bind-device socket options apply to the downstream listener socket.
- Cluster `upstream_bind_config.socket_options` apply to upstream connections.
- Orion supports at most one bind-device socket option per listener or upstream bind config.
- `SO_BINDTODEVICE` is Linux-only.
- `level: 1` and `name: 25` are the Linux socket option identifiers for `SOL_SOCKET` and `SO_BINDTODEVICE`.
- `buf_value` is base64 for the interface name. A trailing null byte is optional; Orion appends it if missing.

Two-tenant sketch:

```yaml
envoy_bootstrap:
  static_resources:
    listeners:
      - name: tenant_a_mcp
        address:
          socket_address:
            address: 10.10.1.10
            port_value: 8000
        socket_options:
          - description: bind tenant A listener to tenant-a-in
            level: 1
            name: 25
            buf_value: dGVuYW50LWEtaW4=
        filterChains:
          - name: tenant_a_chain
            filters:
              - name: http_gateway
                typedConfig:
                  "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                  statPrefix: tenant_a_mcp
                  codecType: HTTP1
                  httpFilters:
                    - name: orion.filters.http.mcp
                      typed_config:
                        "@type": type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway
                        server_info:
                          name: tenant-a-mcp
                          version: "1.0.0"
                        cluster_header: x-mcp-target-cluster
                        tds:
                          config_name: main
                    - name: envoy.filters.http.router
                      typedConfig:
                        "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
                  routeConfig:
                    name: tenant_a_mcp_routes
                    virtual_hosts:
                      - name: tenant_a
                        domains: ["*"]
                        routes:
                          - match:
                              prefix: /
                            route:
                              cluster_header: x-mcp-target-cluster

      - name: tenant_b_mcp
        address:
          socket_address:
            address: 10.10.2.10
            port_value: 8000
        socket_options:
          - description: bind tenant B listener to tenant-b-in
            level: 1
            name: 25
            buf_value: dGVuYW50LWItaW4=
        filterChains:
          - name: tenant_b_chain
            filters:
              - name: http_gateway
                typedConfig:
                  "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
                  statPrefix: tenant_b_mcp
                  codecType: HTTP1
                  httpFilters:
                    - name: orion.filters.http.mcp
                      typed_config:
                        "@type": type.googleapis.com/orion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway
                        server_info:
                          name: tenant-b-mcp
                          version: "1.0.0"
                        cluster_header: x-mcp-target-cluster
                        tds:
                          config_name: main
                    - name: envoy.filters.http.router
                      typedConfig:
                        "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
                  routeConfig:
                    name: tenant_b_mcp_routes
                    virtual_hosts:
                      - name: tenant_b
                        domains: ["*"]
                        routes:
                          - match:
                              prefix: /
                            route:
                              cluster_header: x-mcp-target-cluster

    clusters:
      - name: tenant_a_tools_api
        connect_timeout: 0.25s
        type: STATIC
        lb_policy: ROUND_ROBIN
        upstream_bind_config:
          socket_options:
            - description: bind tenant A upstreams to tenant-a-out
              level: 1
              name: 25
              buf_value: dGVuYW50LWEtb3V0
        load_assignment:
          endpoints:
            - lb_endpoints:
                - endpoint:
                    address:
                      socket_address:
                        address: 10.20.1.10
                        port_value: 8080

      - name: tenant_b_tools_api
        connect_timeout: 0.25s
        type: STATIC
        lb_policy: ROUND_ROBIN
        upstream_bind_config:
          socket_options:
            - description: bind tenant B upstreams to tenant-b-out
              level: 1
              name: 25
              buf_value: dGVuYW50LWItb3V0
        load_assignment:
          endpoints:
            - lb_endpoints:
                - endpoint:
                    address:
                      socket_address:
                        address: 10.20.2.10
                        port_value: 8080
```

Tenant-specific TDS resources then target different scopes:

```text
tenant-a-mcp/main/search_customer
tenant-b-mcp/main/search_customer
```

Both resources can carry a `Tool` named `search_customer`, but they update different gateway registries.

## Operational Notes

- `tools/call` requires a valid MCP session.
- `GET /mcp` returns method not allowed. Use `POST /mcp` for streamable HTTP.
- `DELETE /mcp` with `mcp-session-id` deletes a streamable HTTP session.
- Legacy downstream SSE starts with `GET /sse` and then posts messages to the endpoint sent in the SSE handshake.
- Tool names must be unique within a gateway registry after namespacing.
- Static tools and xDS-provided tools share the same provided-tool namespace; xDS can replace a static tool with the same name.
- Dynamic tools are owned by their dynamic server and are removed by removing or replacing that server.
- Configure upstream MCP server communication as Streamable HTTP by design.
- Do not configure `function_graph_backend`; it is not implemented.
- If semantic search is enabled, build Orion with `mcp-semantic-search` and configure a matching `embeddings_services` entry.
- For offline semantic search with local FastEmbed, pre-stage the model snapshot and set `model_dir`; otherwise FastEmbed may attempt a runtime model download.
