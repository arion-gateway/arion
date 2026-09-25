# MCP Gateway Tool RBAC (Role-Based Access Control)

## Overview

The MCP Gateway RBAC feature provides per-tool, JWT-based access control for Model Context Protocol (MCP) tools. Unlike traditional gateway-level RBAC, each tool can define its own access control rules based on JWT claims and headers, allowing fine-grained control over who can access which tools.

## Key Concepts

### Per-Tool RBAC
- **Each tool has its own RBAC configuration** - Access control is defined at the tool level, not globally
- **Optional configuration** - Tools without RBAC are accessible to all authenticated users
- **JWT-based** - Access decisions are based on JWT token claims and headers
- **No principals, only permissions** - Simplified model focusing on JWT attribute matching

### How It Works

```
┌─────────────┐      ┌──────────────┐      ┌──────────────┐      ┌──────────┐
│   Client    │─────▶│ JWT Auth     │─────▶│  MCP Gateway │─────▶│  Tool    │
│  (AI Agent) │      │   Filter     │      │              │      │ Upstream │
└─────────────┘      └──────────────┘      └──────────────┘      └──────────┘
                           │                      │
                           │ Store JWT            │ Check tool's
                           │ in extensions        │ RBAC rules
                           ▼                      ▼
                     ┌──────────────┐      ┌──────────────┐
                     │ JWT Claims   │      │ Tool RBAC    │
                     │ JWT Header   │      │ Config       │
                     └──────────────┘      └──────────────┘
```

1. **JWT Authentication Filter** validates the token and stores claims/headers in request extensions
2. **MCP Gateway** receives a tool call request
3. **Tool Registry** checks if the requested tool has RBAC configured
4. **RBAC Evaluation** checks if JWT claims/headers match the tool's permission requirements
5. **Decision** - Allow or deny access to the tool

## Configuration

### Basic Structure

Each tool can have an optional `rbac` configuration:

```yaml
tools:
  - name: "my_tool"
    description: "Tool description"
    backend:
      cluster: my_cluster
      rest_transcoding:
        method: GET
        path: "/api/endpoint"
    input_schema:
      inline_string: '{"type": "object", "properties": {}}'
    rbac:
      action: ALLOW  # or DENY
      permissions:
        - jwt_claim:
            field: "role"
            value: "admin"
```

### Action Types

- **`ALLOW`**: Deny by default. Only allow if **any** permission matches.
- **`DENY`**: Allow by default. Only deny if **any** permission matches.

### Permission Types

Permissions define JWT requirements that must be met. They use **OR logic** - if any permission matches, the condition is satisfied.

#### 1. JWT Claim Permission

Match based on JWT payload claims (standard or custom):

```yaml
permissions:
  - jwt_claim:
      field: "role"      # Claim name
      value: "admin"     # Required value
```

**Standard JWT claim fields:**
- `iss` or `issuer` - Token issuer
- `sub` or `subject` - Token subject (usually user ID/email)
- `aud` or `audience` - Token audience
- `exp` or `expiration` - Expiration time (as string)
- `iat` or `issued_at` - Issued at time (as string)
- `nbf` or `not_before` - Not before time (as string)
- `jti` or `jwt_id` - JWT ID

**Custom claims:**
Use any custom claim name directly (e.g., `role`, `department`, `permissions`, `organization`)

#### 2. JWT Header Permission

Match based on JWT header fields:

```yaml
permissions:
  - jwt_header:
      field: "kid"           # Header field name
      value: "service-key-1" # Required value
```

**Supported JWT header fields:**
- `alg` or `algorithm` - Signing algorithm (e.g., "RS256", "HS256")
- `typ` or `type` - Token type
- `kid` or `key_id` - Key ID
- `cty` or `content_type` - Content type
- Other standard JWT header fields

## Examples

### Example 1: Public Tool (No RBAC)

Tool accessible to all authenticated users:

```yaml
tools:
  - name: "weather_forecast"
    description: "Get weather forecast"
    backend:
      cluster: weather_cluster
      rest_transcoding:
        method: GET
        path: "/forecast"
    input_schema:
      inline_string: '{"type": "object"}'
    # No rbac = accessible to all authenticated users
```

### Example 2: Role-Based Access (Single Role)

Tool accessible only to admins:

```yaml
tools:
  - name: "delete_user"
    description: "Delete a user account"
    backend:
      cluster: user_api
      rest_transcoding:
        method: DELETE
        path: "/users"
    input_schema:
      inline_string: '{"type": "object", "properties": {"user_id": {"type": "string"}}}'
    rbac:
      action: ALLOW
      permissions:
        - jwt_claim:
            field: "role"
            value: "admin"
```

### Example 3: Multiple Roles (OR Logic)

Tool accessible to users with either "admin" OR "moderator" role:

```yaml
tools:
  - name: "moderate_content"
    description: "Moderate user content"
    backend:
      cluster: moderation_api
      rest_transcoding:
        method: POST
        path: "/moderate"
    input_schema:
      inline_string: '{"type": "object"}'
    rbac:
      action: ALLOW
      permissions:
        - jwt_claim:
            field: "role"
            value: "admin"
        - jwt_claim:
            field: "role"
            value: "moderator"
```

**Result:** Access granted if role is "admin" **OR** "moderator"

### Example 4: Department-Based Access

Tool accessible only to engineering department:

```yaml
tools:
  - name: "deploy_service"
    description: "Deploy a service to production"
    backend:
      cluster: deployment_api
      rest_transcoding:
        method: POST
        path: "/deploy"
    input_schema:
      inline_string: '{"type": "object"}'
    rbac:
      action: ALLOW
      permissions:
        - jwt_claim:
            field: "department"
            value: "engineering"
```

### Example 5: Deny Guests (Inverse Logic)

Tool accessible to everyone EXCEPT guests:

```yaml
tools:
  - name: "premium_feature"
    description: "Premium feature"
    backend:
      cluster: premium_api
      rest_transcoding:
        method: GET
        path: "/premium"
    input_schema:
      inline_string: '{"type": "object"}'
    rbac:
      action: DENY
      permissions:
        - jwt_claim:
            field: "role"
            value: "guest"
```

**Result:** Deny if role is "guest", allow everyone else

### Example 6: Service Account (Key-Based)

Tool accessible only to specific service accounts identified by key ID:

```yaml
tools:
  - name: "service_api"
    description: "Service-to-service API"
    backend:
      cluster: internal_api
      rest_transcoding:
        method: POST
        path: "/service/call"
    input_schema:
      inline_string: '{"type": "object"}'
    rbac:
      action: ALLOW
      permissions:
        - jwt_header:
            field: "kid"
            value: "service-account-1"
        - jwt_header:
            field: "kid"
            value: "service-account-2"
```

**Result:** Access granted if token signed with key "service-account-1" OR "service-account-2"

### Example 7: Specific Users Only

Tool accessible only to specific user accounts:

```yaml
tools:
  - name: "admin_dashboard"
    description: "Admin dashboard API"
    backend:
      cluster: admin_api
      rest_transcoding:
        method: GET
        path: "/dashboard"
    input_schema:
      inline_string: '{"type": "object"}'
    rbac:
      action: ALLOW
      permissions:
        - jwt_claim:
            field: "sub"
            value: "admin@example.com"
        - jwt_claim:
            field: "sub"
            value: "superadmin@example.com"
```

### Example 8: Algorithm-Based Access

Tool accessible only to tokens signed with specific algorithm:

```yaml
tools:
  - name: "high_security_api"
    description: "High security API requiring RS256"
    backend:
      cluster: secure_api
      rest_transcoding:
        method: POST
        path: "/secure"
    input_schema:
      inline_string: '{"type": "object"}'
    rbac:
      action: ALLOW
      permissions:
        - jwt_header:
            field: "algorithm"
            value: "RS256"
```

## Integration with JWT Authentication

### Required Setup

1. **JWT Authentication Filter** must be configured before the MCP Gateway filter
2. JWT filter must store claims in request extensions using:
   - `payload_in_metadata: "jwt_payload"` - Stores JWT claims
   - `header_in_metadata: "jwt_header"` - Stores JWT header (optional, for header-based RBAC)

### Example JWT + MCP Gateway Configuration

```yaml
http_filters:
  # 1. JWT Authentication Filter (MUST come first)
  - name: envoy.filters.http.jwt_authn
    typed_config:
      "@type": type.googleapis.com/envoy.extensions.filters.http.jwt_authn.v3.JwtAuthentication
      providers:
        auth_provider:
          issuer: "https://auth.example.com"
          audiences: ["mcp-api"]
          payload_in_metadata: "jwt_payload"  # Required for RBAC
          header_in_metadata: "jwt_header"    # Optional
          remote_jwks:
            http_uri:
              uri: "https://auth.example.com/.well-known/jwks.json"
              cluster: auth_cluster
              timeout: 5s
      rules:
        - match:
            prefix: "/"
          requires:
            provider_name: "auth_provider"
  
  # 2. MCP Gateway Filter with per-tool RBAC
  - name: arion.filters.http.mcp
    typed_config:
      "@type": type.googleapis.com/arion.extensions.filters.http.mcp.mcp_gateway.v3.McpGateway
      server_info:
        name: "mcp-gateway"
        version: "1.0.0"
      tools:
        - name: "my_tool"
          # ... tool configuration ...
          rbac:
            action: ALLOW
            permissions:
              - jwt_claim:
                  field: "role"
                  value: "admin"
```

## Permission Evaluation Logic

### OR Logic for Permissions

Permissions within a tool's RBAC use **OR logic**:

```yaml
rbac:
  action: ALLOW
  permissions:
    - jwt_claim: {field: "role", value: "admin"}
    - jwt_claim: {field: "role", value: "superuser"}
    - jwt_claim: {field: "department", value: "security"}
```

**Evaluation:** Access is granted if **ANY** of these conditions is true:
- role == "admin" **OR**
- role == "superuser" **OR**
- department == "security"

### Action Evaluation

**ALLOW Action:**
- No permissions match → **Deny**
- Any permission matches → **Allow**

**DENY Action:**
- No permissions match → **Allow**
- Any permission matches → **Deny**

### Missing JWT

If JWT claims or headers are not present in the request:
- All permission checks fail
- ALLOW action → Access denied
- DENY action → Access allowed (nothing to deny)

## Error Handling

When RBAC denies access:

**JSON-RPC Error Response:**
```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {
    "code": -32600,
    "message": "Access denied by RBAC policy"
  }
}
```

**HTTP Status:** 200 OK (JSON-RPC error, not HTTP error)

**Logs:**
```
DEBUG mcp_gateway: handle_rpc_json_request: tools/call failed to build request
Tool RBAC: action=Allow, matched=false, permitted=false
```

## Best Practices

1. **Default Deny**: Use `action: ALLOW` for sensitive tools (explicit allow-list)
2. **Default Allow**: Use `action: DENY` only for exclusion lists (e.g., block guests)
3. **Minimal Permissions**: Grant only necessary access
4. **Use Custom Claims**: Leverage custom JWT claims like `role`, `department`, `permissions`
5. **Document Requirements**: Clearly document what JWT claims each tool requires
6. **Test Thoroughly**: Test with different JWT tokens to verify access control
7. **Monitor Denials**: Log and monitor RBAC denials to detect issues
8. **Regular Audits**: Review tool RBAC configurations regularly

## Security Considerations

1. **JWT Validation**: RBAC assumes JWT is already validated by the JWT authentication filter
2. **No Bypass**: Tools without RBAC are accessible to **all authenticated users**
3. **Claim Tampering**: RBAC security depends on proper JWT signature verification
4. **Token Expiration**: RBAC doesn't check token expiration (JWT filter does this)
5. **Audience Validation**: Ensure JWT filter validates audience claims appropriately

## Performance

- **In-Memory**: All RBAC checks are performed in-memory
- **Minimal Overhead**: ~1-2 microseconds per tool call
- **No Network Calls**: RBAC decisions are local, no external services
- **Scalable**: Handles thousands of requests per second

## Debugging

### Enable Debug Logging

```yaml
logging:
  log_level: "debug"
```

### Log Output

```
DEBUG mcp_gateway: handle_rpc_json_request: tools/call
DEBUG mcp_rbac: Tool RBAC: action=Allow, matched=true, permitted=true
DEBUG mcp_gateway: handle_rpc_json_request: UPSTREAM
```

Or when denied:

```
DEBUG mcp_gateway: handle_rpc_json_request: tools/call failed to build request
Tool RBAC: action=Allow, matched=false, permitted=false
```

### Common Issues

**Issue:** Tool always denied even with correct JWT
- Check JWT filter is configured with `payload_in_metadata`
- Verify JWT claims actually contain expected values
- Check claim field names match exactly (case-sensitive)

**Issue:** Tool accessible when it should be denied
- Verify RBAC is configured on the tool
- Check action type (ALLOW vs DENY)
- Ensure permissions list is not empty

**Issue:** All tools denied
- JWT authentication filter may not be storing claims in extensions
- Check JWT filter configuration for `payload_in_metadata`
- Verify JWT token is valid and not expired

## Protobuf Definition

```protobuf
message Tool {
  string name = 1;
  string description = 2;
  DataSource input_schema = 3;
  Backend backend = 4;
  optional ToolRbac rbac = 5;
}

message ToolRbac {
  enum Action {
    ALLOW = 0;
    DENY = 1;
  }
  
  Action action = 1;
  repeated Permission permissions = 2;
}

message Permission {
  oneof permission_type {
    JwtHeaderMatcher jwt_header = 1;
    JwtClaimMatcher jwt_claim = 2;
  }
}

message JwtHeaderMatcher {
  string field = 1;
  string value = 2;
}

message JwtClaimMatcher {
  string field = 1;
  string value = 2;
}
```

## See Also

- [JWT Authentication Filter Documentation](https://www.envoyproxy.io/docs/envoy/latest/api-v3/extensions/filters/http/jwt_authn/v3/config.proto)
- [Example Configuration](../../arion-proxy/conf/arion-runtime-mcp.yaml)
