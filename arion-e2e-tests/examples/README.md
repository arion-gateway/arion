# MCP Gateway Semantic Search Demo

This directory contains runnable examples for Arion MCP Gateway semantic search.

- `mcp_semantic_search.rs` starts Arion, four local REST backends, and a generated MCP Gateway config using built-in BM25 ranking.
- `fastembed_embedding_service.rs` is a standalone OpenAI-compatible `/v1/embeddings` sidecar for testing remote embeddings.

## How Semantic Search Works

The MCP gateway exposes RBAC-permitted progressive tools through a synthetic tool named `tool_search`. When a client calls it with a `query`, Arion ranks progressive tools using each tool's name, description, and input schema metadata.

By default, ranking uses BM25 locally and does not require an embeddings service. If `search.embeddings` is configured, Arion can use remote vector ranking and falls back to BM25 if query embedding fails or any candidate lacks a vector.

## Discovery Flow

Semantic search uses one stateless flow:

1. `tools/list` returns eager tools directly plus `tool_search` when progressive tools are available.
2. The client calls `tool_search`.
3. Arion returns the matching full tool definitions directly in the `tool_search` result.

Discovered tools remain callable by their exact qualified `namespace.name` even though they are omitted from `tools/list`.

## Run

Build Arion first:

```bash
cargo build -p arion-proxy
```

Run the BM25 demo:

```bash
cargo run -p arion-e2e-tests --example mcp_semantic_search
```

Useful options:

```bash
cargo run -p arion-e2e-tests --example mcp_semantic_search -- --max-allowable-results 1

cargo run -p arion-e2e-tests --example mcp_semantic_search -- --keep-config

cargo run -p arion-e2e-tests --example mcp_semantic_search -- --verbose-arion
```

The example prints the generated config path, backend addresses, the MCP endpoint, a copy-pasteable `curl` smoke test, and the result of a sample `tool_search` call. The manual `curl` flow must reuse the real `mcp-session-id` returned by `initialize`.

## CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--max-allowable-results` | `2` | Maximum number of tool definitions returned by one search. |
| `--verbose-arion` | `false` | Print Arion subprocess logs. |
| `--keep-config` | `false` | Leave the generated YAML config in `/tmp` after exit. |

## Remote FastEmbed Sidecar

To test vector search with FastEmbed, run the sidecar example and configure `search.embeddings` to point at its cluster:

```bash
cargo run -p arion-e2e-tests --features fastembed-service --example fastembed_embedding_service
```

## Troubleshooting

`Could not find arion binary`

Build Arion first:

```bash
cargo build -p arion-proxy
```

Or set `ARION_BIN`:

```bash
ARION_BIN=target/debug/arion cargo run -p arion-e2e-tests --example mcp_semantic_search
```

`Session not available` or `No such Session exists`

The `tools/call` request is missing a valid `mcp-session-id`, is using a stale session id, or is still using the literal `<session-id>` placeholder. Run `initialize`, copy the `mcp-session-id` response header exactly, and send it on every follow-up `tools/list` and `tools/call` request.
