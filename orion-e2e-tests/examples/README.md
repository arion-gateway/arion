# MCP Gateway Semantic Search Demo

This directory contains a runnable example for experimenting with Orion MCP Gateway semantic search using a local FastEmbed model.

The example lives in `orion-e2e-tests` because it reuses utilities and code from the integration-test harness:

- `McpGatewayBuilder`, `McpToolBuilder`, and related config builders create the Orion bootstrap.
- `LocalEmbeddingsServiceConfig` configures the local embeddings service without string templates.
- `TestBackend` starts local REST backends for the demo tools.
- `OrionInstance::spawn_auto_port` starts Orion and discovers the listener port from Orion logs.
- `McpTestClient` performs an initialize, `tools/list`, and sample `semantic_search` call.

## How Local Semantic Search Works

The MCP gateway can inject a synthetic tool named `semantic_search`. When a client (typically an agent) calls it with a `user_query`, Orion performs an advanced semantic search for relevant tools using vector search. To do this, Orion embeds the query provided by the user and compares it with embeddings for available MCP tool descriptions in the current MCP Gateway. The tool embeddings are built from each tool's name, description, and input schema.

In this local demo, the embeddings provider is FastEmbed running in the Orion process:

```yaml
embeddings_services:
  - name: mcp-local-demo
    local:
      model_id: BAAI/bge-small-en-v1.5
```

If `--model-dir` is not supplied, FastEmbed loads the model by `model_id` and may download model files on first run. If `--model-dir` is supplied, Orion loads model files from that directory instead.

## Direct Mode And Assisted Discovery

The demo defaults to direct mode:

1. `tools/list`, if called, returns the normal tools plus the `semantic_search` tool.
2. The client calls `semantic_search` (can be called directly without calling `tools\list` or after)
3. Orion returns the top matching tool definitions directly in the `semantic_search` result.

With `--assisted-discovery`:

1. Initial `tools/list` returns only `semantic_search`.
2. The client calls `semantic_search`.
3. Orion stores the matched tools in the MCP session and emits a tool-list-changed notification.
4. A follow-up `tools/list` returns filtered tools based on the semantic search.

## Build And Run

Build the Orion binary with semantic search enabled:

```bash
cargo build -p orion-proxy --features mcp-semantic-search
```

Run the demo:

```bash
cargo run -p orion-e2e-tests --example mcp_semantic_search
```

The example prints the generated config path, backend addresses, the MCP endpoint, a copy-pasteable `curl` smoke test, and the result of a sample semantic-search call. It keeps Orion and the demo backends running until Ctrl-C.

The manual `curl` flow must reuse the real `mcp-session-id` returned by `initialize`. `tools/call` requires a valid session; the literal placeholder `<session-id>` is not a valid value.

Run with a specific model:

```bash
cargo run -p orion-e2e-tests --example mcp_semantic_search -- \
  --model-id sentence-transformers/all-MiniLM-L6-v2
```

Run with local model files:

```bash
cargo run -p orion-e2e-tests --example mcp_semantic_search -- \
  --model-dir /path/to/fastembed/model/snapshot
```

You can also set:

```bash
ORION_MCP_EMBEDDINGS_MODEL_DIR=/path/to/fastembed/model/snapshot \
cargo run -p orion-e2e-tests --example mcp_semantic_search
```

Run assisted discovery mode:

```bash
cargo run -p orion-e2e-tests --example mcp_semantic_search -- \
  --assisted-discovery --top-k 1
```

Keep the generated config after exit:

```bash
cargo run -p orion-e2e-tests --example mcp_semantic_search -- --keep-config
```

Show Orion subprocess logs:

```bash
cargo run -p orion-e2e-tests --example mcp_semantic_search -- --verbose-orion
```

## CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--model-id` | `BAAI/bge-small-en-v1.5` | FastEmbed model id to load. |
| `--model-dir` | unset | Directory containing local model files. Overrides model download. |
| `--dimensions` | model default | Optional dimension override. Usually leave unset. |
| `--top-k` | `2` | Number of tools returned by semantic ranking. |
| `--assisted-discovery` | `false` | Use session-filtered discovery instead of returning tools directly. |
| `--verbose-orion` | `false` | Print Orion subprocess logs. |
| `--keep-config` | `false` | Leave the generated YAML config in `/tmp` after exit. |

## Supported Local Model IDs

Orion currently accepts these local FastEmbed model ids:

| Canonical id | Short alias |
|--------------|-------------|
| `BAAI/bge-small-en-v1.5` | `bge-small-en-v1.5` |
| `sentence-transformers/all-MiniLM-L6-v2` | `all-MiniLM-L6-v2` |
| `BAAI/bge-small-zh-v1.5` | `bge-small-zh-v1.5` |

When using `--model-dir`, pass the snapshot directory that contains the files FastEmbed expects for the chosen model, such as `tokenizer.json`, `config.json`, `special_tokens_map.json`, `tokenizer_config.json`, and the ONNX model file.

## Modifying The Demo

The demo is intentionally small and easy to edit:

- Add or change tools in `demo_tools()`.
- Tune search behavior with tool names, descriptions, and input schemas. Orion embeds all three.
- Change REST responses in `DemoBackends::start()`.
- Add another backend by adding a cluster constant, `TestBackend`, `ClusterBuilder`, and `McpToolBuilder`.
- Try direct mode and assisted discovery with the same tool set to see the different client flows.
- Change `--top-k` to see how many ranked tools are exposed.

## Troubleshooting

`Could not find orion binary`

Build Orion first:

```bash
cargo build -p orion-proxy --features mcp-semantic-search
```

Or set `ORION_BIN`:

```bash
ORION_BIN=target/debug/orion cargo run -p orion-e2e-tests --example mcp_semantic_search
```

`embeddings_services configured but Orion was built without the mcp-semantic-search feature`

Rebuild the spawned Orion binary with:

```bash
cargo build -p orion-proxy --features mcp-semantic-search
```

`unknown local embeddings model`

Use one of the supported model ids listed above.

`failed to read local embeddings model file`

The `--model-dir` directory does not contain the files expected for that FastEmbed model. Pass the model snapshot directory, not the parent cache directory.

`Session not available` or `No such Session exists`

The `tools/call` request is missing a valid `mcp-session-id`, is using a stale session id, or is still using the literal `<session-id>` placeholder. Run `initialize`, copy the `mcp-session-id` response header exactly, and send it on every follow-up `tools/list` and `tools/call` request. `tools/list` may appear to work without a valid session, but `tools/call` requires one.

First run takes a long time

Without `--model-dir`, FastEmbed may download model files before Orion starts listening. Use `--verbose-orion` for more startup logs or pre-download the model and pass `--model-dir`.
