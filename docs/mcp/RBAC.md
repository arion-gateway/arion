# MCP tool authorization

Each MCP tool can carry JWT-based `rbac`. Unauthorized tools are hidden from
listing, search and the code-mode catalog, and calls are denied. Tools without
`rbac` are available to every request that passes the earlier HTTP filters.

## Publish validated JWT data

Place `envoy.filters.http.jwt_authn` before the MCP gateway. Configure a token
source, a real JWKS and rules selecting the provider, as in the
[JWT guide](../http-filters.md#jwt-authentication). Inside that provider, set:

```yaml
payload_in_metadata: jwt_payload
header_in_metadata: jwt_header
```

`payload_in_metadata` is required for `jwt_claim` permissions;
`header_in_metadata` is needed only for `jwt_header` permissions. Without the
corresponding metadata, no permission of that kind can match, even when the
token is valid. Send the token on subsequent MCP and HTTP search requests.

## Restrict a tool

This entry belongs in the MCP gateway's `tools` list. The `api` cluster must
exist, and the gateway must select `tools_mode` or `code_mode`:

```yaml
- namespace: admin
  name: delete_user
  description: Delete a user account.
  disclosure_mode: EAGER
  input_schema:
    inline_string: |
      {"type":"object","properties":{"id":{"type":"string"}},"required":["id"]}
  rbac:
    action: ALLOW
    permissions:
      - jwt_claim:
          field: role
          value: admin
  rest_backend:
    cluster: api
    method: DELETE
    path: "/users/{{id | encode_path_segment}}"
```

A caller with `role: admin` can list and call `admin.delete_user`. Other callers
cannot discover it and are denied if they call the name directly. On a dynamic
MCP server or OpenAPI source, the same `rbac` block applies to all contributed
tools.

| Action | Any permission matches | No permission matches |
| --- | --- | --- |
| `ALLOW` (default) | Allow | Deny |
| `DENY` | Deny | Allow |

Permissions are **OR**, with at least one entry required. Two entries checking
tenant and role do not require both. A caller without JWT metadata matches
nothing, so `ALLOW` denies and `DENY` allows. Use `ALLOW` for a restricted tool.

## Supported matches

`jwt_claim` compares a payload field using exact string equality. Standard
claims include `iss`, `sub`, `aud`, `exp`, `iat`, `nbf` and `jti`; issuer,
subject and audience also accept their long aliases. Audience matches any
audience entry. Custom numbers and booleans compare by their string form.
Custom array-valued claims, wildcards and regexes are unsupported.

`jwt_header` matches token header fields such as `kid`, `alg`, `typ`, `cty`,
`jku` and `x5u`. For example, use `{ field: kid, value: service-key-1 }` to
restrict access to a validated token signed by that key. This refers to the
JWT header, not arbitrary HTTP request headers.

In code mode, each script uses the identity of its `execute` request. Every
tool call is checked, catalog operations expose only permitted tools, and a
toolkit is visible only if every member is permitted. A denied script call
returns an error envelope with `error.code: access_denied`.

## Verify the policy

Initialize a session as in the [MCP guide](mcp-gateway.md#connect-a-client),
including `Authorization: Bearer $TOKEN` on initialization and later calls.
Compare a non-admin and admin token: `tools/list` should expose the eager
`admin.delete_user` only to the admin. To check denial without performing a
delete, call it with the non-admin token:

```bash
curl -s http://127.0.0.1:8080/mcp \
  -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' \
  -H "Authorization: Bearer $TOKEN" \
  -H "mcp-session-id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"admin.delete_user","arguments":{"id":"42"}}}'
```

The denial is a JSON-RPC error over HTTP 200, with code `-32600` and a message
identifying the tool. Check the JSON-RPC result as well as the HTTP status.

HTTP RBAC and Cedar authorize the outer HTTP request; they do not evaluate
individual tool calls inside MCP or code-mode scripts. Use this per-tool
policy for that boundary. The website's
[tool authorization guide](https://arion-gateway.dev/docs/mcp-gateway/tool-authorization/)
and [filter reference](https://arion-gateway.dev/reference/filters/http/mcp-gateway/)
have further details. Adaptation and license details are in the
[docs index](../README.md#keeping-these-guides-current).
