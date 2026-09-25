# Access Log

The access log is a record of requests processed by Arion, useful for monitoring and debugging. Messages are formatted using `arion-format`, which implements the Envoy command-operator syntax (e.g. `%REQ(:METHOD)%`).

Arion extends the standard Envoy format with **custom operators**: user-defined placeholders whose values are extracted at runtime from base64-encoded JSON payloads carried by configurable HTTP headers.

---

## Architecture

```

                                                                                                    ┌──────────────┐       ┌────┐   ┌────┐    ┌────┐
                                                                                             ┌─────▶│  Listener1   │──────▶│Log1├──▶│Log2├───▶│LogN│
                                                                                             │      └──────────────┘       └────┘   └────┘    └────┘
                                                                                             │
                                                                                             │
                                                                       ┌───────────────┐     │      ┌──────────────┐       ┌────┐   ┌────┐    ┌────┐
                                                                       │               │     ├─────▶│  Listener2   │──────▶│Log1├──▶│Log2├───▶│LogN│
                                          ┌───────────────────┐        │               │     │      └──────────────┘       └────┘   └────┘    └────┘
                                ┌ ─ ─ ─ ─▶│                   ├───────▶│AccessLogger#0 │─────┤
                                          └───────────────────┘        │               │     │
                                │                                      │               │     │      ┌──────────────┐       ┌────┐   ┌────┐
                                                                       └───────────────┘     └─────▶│  ListenerN   │──────▶│Log1├──▶│Log2│
                                │       tokio::sync::mpsc                                           └──────────────┘       └────┘   └────┘

                                │

                                │
                                                                                                    ┌──────────────┐       ┌────┐   ┌────┐    ┌────┐
┌─────────────────────┐         │                                                            ┌─────▶│  Listener1   │──────▶│Log1├──▶│Log2├───▶│LogN│
│log_access_balanced! │─ ─ ─ ─ ─                                                             │      └──────────────┘       └────┘   └────┘    └────┘
└─────────────────────┘         │                                                            │
                                                                                             │
                                │                                      ┌───────────────┐     │      ┌──────────────┐       ┌────┐
                                                                       │               │     ├─────▶│  Listener2   │──────▶│Log1│
                                │         ┌───────────────────┐        │               │     │      └──────────────┘       └────┘
                                 ─ ─ ─ ─ ▶│                   ├───────▶│AccessLogger#1 │─────┤
                                          └───────────────────┘        │               │     │
                                                                       │               │     │      ┌──────────────┐       ┌────┐   ┌────┐
                                        tokio::sync::mpsc              └───────────────┘     └─────▶│  ListenerN   │──────▶│Log1├──▶│Log2│
                                                                                                    └──────────────┘       └────┘   └────┘
```

---

## Features

### Logging infrastructure

- **Asynchronous logging.** Once the message is formatted, it is sent to the logging system asynchronously via `tokio::sync::mpsc` channels. The access_log module provides convenience macros (`log_access_balanced!`, `log_access_single!`, `with_access_log_enabled!`) so callers never touch the sender channel directly.
- **Multiple logger instances.** Multiple loggers can be enabled at the same time (configured via `num_instances`), each with its own channel, reducing contention on the sender side and improving I/O throughput.
- **Balanced routing.** The macro `log_access_balanced!` selects a logger based on the hash of the calling thread ID. This means the same thread — and typically requests coming from the same connection — always hit the same logger.
- **Lazy file creation.** Files are opened only when the first message is logged, avoiding empty log files when a logger is configured but never used.
- **Log rotation** is supported and fully configurable in the Arion configuration (`access_log` section). The available options are `minutely`, `hourly`, and `daily` (default).
- **Per-listener configuration.** Each Listener can have its own access log configuration, including a different format string and a different sink type (file, stdout, stderr). A single listener can also write to multiple destinations.
- **xDS support.** The system supports dynamic xDS configuration for access logs, allowing log configuration changes without restarting Arion.

### Formatting & operators

- **Envoy-compatible format strings.** The format string uses the standard Envoy command-operator syntax: `%OPERATOR%` for general operators and `%REQ(HEADER)%` / `%RESP(HEADER)%` for request/response headers. All unquoted `%`-delimited placeholders that don't match a known operator will produce a **startup error**.
- **Default format.** When no `text_format` is specified in the filter-chain configuration, Arion falls back to `DEFAULT_ACCESS_LOG_FORMAT`, which matches Envoy's default HTTP access log format.
- **Custom operators.** Arion extends the Envoy syntax with user-defined operators whose values are supplied at runtime via base64-encoded JSON payloads in HTTP headers. See the [Custom Operators](#custom-operators) section below for details.

---

## Configuration

### Global access log settings (`access_logging` section)

The following options are set at the top level of the Arion configuration file, under the `access_logging` key. All fields are optional and have sensible defaults:

```yaml
access_logging:
  num_instances: 2        # number of parallel logger tasks (default: 1)
  queue_length: 1024      # channel buffer size per logger (default: 1024)
  log_rotation: daily     # rotation frequency: minutely | hourly | daily (default: daily)
  log_max_size: 104857600 # max file size in bytes before rotation (optional)
  max_log_files: 10       # max rotated files to keep (default: 10)
  blocking: false         # true = sender blocks when buffer is full; false = drops oldest
```

### Hook header configuration

The `access_logging` section also accepts six optional header-name fields. Each field names an HTTP header that Arion will inspect at a specific point in the request/response lifecycle. When the named header is present, its value is base64-decoded into a JSON object which is then used to populate the custom operators.

| Field                        | Hook point                                        |
| ---------------------------- | -------------------------------------------------- |
| `incoming_request_header`    | After the downstream request headers are received  |
| `ext_proc_request_header`    | After ext_proc request processing completes        |
| `upstream_request_header`    | Before the request is sent to the upstream         |
| `incoming_response_header`   | After the upstream response headers are received   |
| `ext_proc_response_header`   | After ext_proc response processing completes       |
| `downstream_response_header` | Before the response is sent to the downstream      |

Each field expects an HTTP header name (e.g. `x-arion-metadata`).

### Custom operators registration

To use custom operators in your format strings, you must explicitly register them in the `custom_operators` list under `access_logging`. This list registers the valid custom operators in `arion-format`'s parser at startup.

Example:

```yaml
access_logging:
  blocking: true
  incoming_request_header: "x-arion-metadata"
  custom_operators:
    - "user_id"
    - "session_id"
    - "app_version"
```

### Filter-chain configuration

Access log sinks are configured inside the filter-chain (HCM or TcpProxy), following the standard Envoy `access_log` syntax:

```yaml
filter_chains:
  - name: main
    filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: ingress_http
          access_log:
            - name: envoy.access_loggers.file
              typed_config:
                "@type": type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog
                path: /var/log/arion/access.log
                log_format:
                  text_format: |
                    [%START_TIME%] %REQ(:METHOD)% %REQ(:PATH)% %PROTOCOL%
                    %RESPONSE_CODE% %DURATION% %BYTES_SENT%
                    user=%user_id% session=%session_id% version=%app_version%
```

Any operator used in `text_format` must be either:
- a **standard operator** (listed in [Supported Operators](#supported-operators) below), or
- a **custom operator** registered in the `custom_operators` list under `access_logging`.

Unknown placeholders cause a startup error.

---

## Custom Operators

Custom operators let you inject dynamic key-value data into access log entries without modifying Arion's code. The mechanism decouples the **transport header** from the **logged fields**:

### Lifecycle

1. **Configuration.** In `access_logging` you specify:
   - Which HTTP header to inspect at each of the six hook points (e.g. `incoming_request_header: "x-arion-metadata"`).
   - The list of custom operators you intend to use in your format strings (e.g. `custom_operators: ["user_id", "session_id", "app_version"]`).

2. **Format string.** You use the registered custom operators as placeholders in the access log format: `%user_id%`, `%session_id%`, `%app_version%`. At configuration parse time, Arion registers these in an internal allowlist (`CUSTOM_OPERATORS`).

3. **Request processing.** At each hook point, Arion checks whether the configured header is present in the current request/response headers. If it is:
   - the header value is **base64-decoded**,
   - the decoded bytes are parsed as a **JSON object**,
   - each key in the JSON object is matched against custom operator names in the format string,
   - matching placeholders are filled with the corresponding JSON values.

   > ⚠️ **Security & Log Integrity Note:** The JSON payload can **only** populate custom operators registered in `custom_operators`. Standard built-in operators (like `%RESPONSE_CODE%`, `%BYTES_SENT%`, `%DURATION%`, etc.) are protected and can **never** be populated or overwritten by the JSON payload, preventing any external tampering with standard audit fields.

4. **Log output.** When the log entry is written, custom operators that received a value produce that value; operators that were never populated produce `"-"` (the Envoy convention for absent/missing values).

### JSON payload format

The header value must be a **base64-encoded UTF-8 JSON string** representing a JSON object. The keys in the object must match the custom operator names.

Example — setting `x-arion-metadata` to inject `user_id = "user-123"`, `session_id = "sess-456"`, and `app_version = "2.1.0"`:

```
# Raw JSON:
{"user_id": "user-123", "session_id": "sess-456", "app_version": "2.1.0"}

# Base64-encoded:
eyJ1c2VyX2lkIjogInVzZXItMTIzIiwgInNlc3Npb25faWQiOiAic2Vzcy00NTYiLCAiYXBwX3ZlcnNpb24iOiAiMi4xLjAifQ==
```

The header sent on the wire:

```
x-arion-metadata: eyJ1c2VyX2lkIjogInVzZXItMTIzIiwgInNlc3Npb25faWQiOiAic2Vzcy00NTYiLCAiYXBwX3ZlcnNpb24iOiAiMi4xLjAifQ==
```

### Supported JSON value types

All JSON scalar types are supported and rendered as their string representation:

| JSON type    | Example value         | Log output       |
| ------------ | --------------------- | ---------------- |
| String       | `"hello"`             | `hello`          |
| Number (int) | `42`                  | `42`             |
| Number (float)| `3.14`               | `3.14`           |
| Boolean      | `true`                | `true`           |
| Null         | `null`                | `-` (empty value)|
| Array        | `[1,2,3]`             | `[1,2,3]`        |

Nested objects are **not** expanded recursively — only the top-level keys are matched against format string placeholders.

### Hook reference

Arion defines six hooks, mirroring the `AccessLogHook` enum:

| Hook                    | Enum variant           | Description                                           |
| ----------------------- | ---------------------- | ----------------------------------------------------- |
| `IncomingRequest`       | `IncomingRequest`      | Fires after the downstream request is fully received  |
| `ExtProcRequest`        | `ExtProcRequest`       | Fires after external processing of the request        |
| `UpstreamRequest`       | `UpstreamRequest`      | Fires before the request is forwarded upstream        |
| `IncomingResponse`      | `IncomingResponse`     | Fires after the upstream response is received         |
| `ExtProcResponse`       | `ExtProcResponse`      | Fires after external processing of the response       |
| `DownstreamResponse`    | `DownstreamResponse`   | Fires before the response is sent to the downstream   |

Each hook attempts to extract its configured header from the header map available at that point:
- **IncomingRequest** and **DownstreamResponse** inspect the **downstream** (client-side) headers.
- **UpstreamRequest** and **IncomingResponse** inspect the **upstream** (backend-side) headers.
- **ExtProcRequest** and **ExtProcResponse** inspect the headers produced by the external processing filter.

### Error handling

The `evaluate_access_log_hook` function is designed to be **graceful**:

- If no `access_logging` headers are configured at all → silent no-op.
- If a specific hook header is not configured → silent no-op.
- If the configured header is not present in the request/response → silent no-op.
- If the header value is present but malformed (invalid UTF-8, bad base64, invalid JSON) → a warning is logged internally, but the request processing continues and the operator renders as `"-"`.

This means missing or malformed custom-operator data never breaks request processing or log emission.

---

## Limitations

- If multiple loggers are enabled, the message is formatted separately for each logger, even when the format is the same. This could be optimised but has not been implemented yet.
- JSON values for custom operators are not cached across hooks — if the same header appears at multiple hook points, it is decoded and parsed each time.
- Nested JSON objects in custom-operator payloads are not traversed. Only top-level keys are matched against format string placeholders.

The following log writers are currently **not** supported:
- `fluentd`
- `http_grpc`
- `tcp_grpc`
- `opentelemetry`
- `wasm`

---

## Supported Operators

The tables below show all operators currently implemented. The **HCM** (HttpConnectionManager) and **TcpProxy** columns indicate their status: a check mark (✅) means the operator is supported, a dash (–) means it is not applicable for that connection type, and a cross mark (❌) means it is not yet implemented. Operators shown in bold are those used in Envoy's default HTTP format.

| Operator                               | Listener: tcp/http/websocket | HCM | TCPProxy |
| :------------------------------------- | :--------------------------: | :-: | :------: |
| **BYTES_RECEIVED**                     | ✅                           | ✅  | ✅      |
| **BYTES_SENT**                         | ✅                           | ✅  | ✅      |
| **DOWNSTREAM_WIRE_BYTES_RECEIVED**     | ✅                           | ✅  | ✅      |
| **DOWNSTREAM_WIRE_BYTES_SENT**         | ✅                           | ✅  | ✅      |
| **PROTOCOL**                           | –                            | ✅  | –       |
| UPSTREAM_PROTOCOL                      | –                            | ✅  | –       |
| **RESPONSE_CODE**                      | –                            | ✅  | –       |
| RESPONSE_CODE_DETAILS                  | –                            | ✅  | ✅      |
| REQUEST_HEADERS_BYTES                  | –                            | ✅  | –       |
| RESPONSE_HEADERS_BYTES                 | –                            | ✅  | –       |
| **DURATION**                           | ✅                           | ✅  | ✅      |
| REQUEST_DURATION                       | –                            | ✅  | –       |
| REQUEST_TX_DURATION                    | –                            | ✅  | –       |
| RESPONSE_DURATION                      | –                            | ✅  | –       |
| RESPONSE_TX_DURATION                   | –                            | ✅  | –       |
| **RESPONSE_FLAGS**                     | –                            | ✅  | ✅      |
| **RESPONSE_FLAGS_LONG**                | –                            | ✅  | ✅      |
| CONNECTION_TERMINATION_DETAILS         | ✅                           | ✅  | ✅      |
| **UPSTREAM_HOST**                      | –                            | ✅  | ✅      |
| UPSTREAM_HOST_NAME                     | –                            | ✅  | –       |
| UPSTREAM_HOST_NAME_WITHOUT_PORT        | –                            | ✅  | –       |
| UPSTREAM_LOCAL_ADDRESS                 | –                            | ❌   | ✅      |
| UPSTREAM_LOCAL_ADDRESS_WITHOUT_PORT    | –                            | ❌   | ✅      |
| UPSTREAM_LOCAL_PORT                    | –                            | ❌   | ✅      |
| UPSTREAM_REMOTE_ADDRESS                | –                            | ❌   | ✅      |
| UPSTREAM_REMOTE_ADDRESS_WITHOUT_PORT   | –                            | ❌   | ✅      |
| UPSTREAM_REMOTE_PORT                   | –                            | ❌   | ✅      |
| DOWNSTREAM_LOCAL_ADDRESS               | ✅                           | ✅  | ✅      |
| DOWNSTREAM_LOCAL_ADDRESS_WITHOUT_PORT  | ✅                           | ✅  | ✅      |
| DOWNSTREAM_LOCAL_PORT                  | ✅                           | ✅  | ✅      |
| DOWNSTREAM_REMOTE_ADDRESS              | ✅                           | ✅  | ✅      |
| DOWNSTREAM_REMOTE_ADDRESS_WITHOUT_PORT | ✅                           | ✅  | ✅      |
| DOWNSTREAM_REMOTE_PORT                 | ✅                           | ✅  | ✅      |
| UPSTREAM_CLUSTER                       | –                            | ✅  | ✅      |
| UPSTREAM_CLUSTER_RAW                   | –                            | ✅  | ✅      |
| UPSTREAM_TRANSPORT_FAILURE_REASON      | –                            | ✅  | ✅      |
| CONNECTION_ID                          | ✅                           | ✅  | ✅      |
| UPSTREAM_CONNECTION_ID                 | –                            | –   | ✅      |
| UNIQUE_ID                              | –                            | ✅  | –       |
| TRACE_ID                               | –                            | ✅  | –       |
| **START_TIME**                         | ✅                           | ✅  | ✅      |
| REQ(:SCHEME)                           | –                            | ✅  | –       |
| **REQ(:METHOD)**                       | –                            | ✅  | –       |
| REQ(:PATH)                             | –                            | ✅  | –       |
| **REQ(:AUTHORITY)**                    | –                            | ✅  | –       |
| **REQ(X-ENVOY-ORIGINAL-PATH?:PATH)**   | –                            | ✅  | –       |
| RESP(:STATUS)                          | –                            | ✅  | –       |
| **REQ(HEADER)**                        | –                            | ✅  | –       |
| **RESP(HEADER)**                       | –                            | ✅  | –       |
| REQUESTED_SERVER_NAME                  | –                            | ✅  | –       |
| ROUTE_NAME                             | –                            | ✅  | –       |

**Notes:**

1. `%UPSTREAM_CLUSTER%` and `%UPSTREAM_CLUSTER_RAW%` are identical — Arion does not currently support `alt_stat_name` for clusters.
2. The `UNIQUE_ID` follows Envoy's convention: if the `x-request-id` header is present and valid, it is reused; otherwise a new unique ID is generated.
3. `%UPSTREAM_HOST_NAME%` and `%UPSTREAM_HOST%` are currently identical — the distinction (hostname vs host+port) is not yet implemented.
4. `%REQUESTED_SERVER_NAME%` is the TLS SNI value. It is populated only on TLS connections; on plaintext connections it renders as `"-"`.
5. **Listener-level** metrics (`BYTES_RECEIVED`, `BYTES_SENT`, `DURATION`, etc.) represent cumulative totals for the entire connection, not for a single request/transaction.
6. At the **Listener** and **TcpProxy** level, `%BYTES_RECEIVED%` / `%DOWNSTREAM_WIRE_BYTES_RECEIVED%` are identical (both measure raw TCP bytes). The same applies to the `_SENT` counterparts.
7. At the **HCM** level, `%BYTES_RECEIVED%` and `%BYTES_SENT%` count only HTTP body bytes. `%DOWNSTREAM_WIRE_BYTES_RECEIVED%` / `%DOWNSTREAM_WIRE_BYTES_SENT%` are measured at the TCP layer and therefore also include HTTP headers; on TLS connections they additionally include TLS handshake bytes.
