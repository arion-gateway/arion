# Architectural Guide and Configuration Reference: OAuth2 Filter in Arion Gateway

This document describes the request flow, session HMAC verification, and configuration of the **OAuth2 HTTP Filter** in Arion Gateway.

**Scope and limitations:** The session HMAC protects cookie integrity; it does not provide complete OAuth2/OIDC security or full Envoy compatibility. CSRF nonce validation, token encryption, and the actual refresh-token grant flow remain unimplemented. Full cookie interoperability with Envoy encrypted tokens is not included.

---

## 1. End-to-End Architectural Flow

The filter coordinates the authorization-code flow with the Identity Provider (IdP) and verifies session cookies before forwarding protected requests upstream:

1. **Initial request:** An unauthenticated client is redirected to `authorization_endpoint`, using the configured `redirect_uri`. An `OauthNonce` cookie is emitted, but nonce validation is not implemented.
2. **IdP login:** The IdP authenticates the user and redirects the browser to the callback with an authorization code.
3. **Callback and code exchange:** Arion exchanges the code at `token_endpoint`, computes the session expiry, signs the emitted token values as described below, sets session cookies, and redirects the client back.
4. **Protected resource access:** Every protected request verifies the session HMAC and checks expiry before any optional bearer-token forwarding. There is no session cache or background session cleaner.
5. **Signout:** Arion clears session cookies using `Max-Age=0` and redirects to `end_session_endpoint` or `/`.

The Python mock IdP needs no change for this HMAC fix: the IdP issues tokens; the gateway signs and verifies the session cookies.

---

## 2. Request Decision Pipeline

Requests are evaluated in order: `pass_through_matcher` bypass, signout, callback/code exchange, then session verification for protected resources. A valid, unexpired session continues upstream, optionally with `Authorization: Bearer <token>`. Unauthenticated requests receive a **401** if they match `deny_redirect_matcher`; otherwise they are redirected to the IdP.

### 2.1. Session Signature

New session signatures use standard Base64 encoding of the raw HMAC-SHA256 digest:

```text
Base64(HMAC-SHA256(secret, domain + '\n' + expires + '\n' + access_token + '\n' + id_token + '\n' + refresh_token))
```

- `secret` is the configured `hmac_secret`.
- `domain` is the configured `cookie_domain` when nonempty; otherwise it is the request Host/authority, **including any port**.
- Fields are separated by a single newline byte, with no extra trailing separator.
- When issuing cookies, missing tokens and tokens whose cookie emission is disabled are represented by empty fields.
- Verification uses the **literal expiry cookie value** (not a parsed-and-reformatted timestamp) and **all received token cookie values**, including tokens whose emission is disabled in the current configuration. Absent token cookies contribute empty fields. Expiry is also checked separately.

Every protected request recomputes and verifies this signature. Changing the domain, expiry, access token, ID token, or refresh token invalidates the signature.

The verifier also accepts the legacy Envoy representation: standard Base64 of the lowercase hexadecimal digest's ASCII bytes, computed over the same newline-separated fields. This is signature-format compatibility only, not full cookie interoperability with Envoy encrypted tokens.

**Migration:** Old Arion signatures over `host:expires` are invalid under this scheme. Existing users must log in again.

### 2.2. Verification Implementation

The filter prebuilds the HMAC key once per filter configuration. Verification borrows cookie values and feeds the fields incrementally into HMAC rather than allocating a concatenated payload. Signature decoding uses a stack buffer, and digest comparison is constant-time. When forwarding a bearer token, the filter builds one owned authorization-header buffer.

---

## 3. Configuration Reference

The configuration is organized under two primary data structures: `OAuth2Config` and `OAuth2Credentials`.

### 3.1. Main Filter Configuration (`OAuth2Config`)

| Field | Type | Default | Lifecycle Role | Description & Examples |
| :--- | :--- | :--- | :--- | :--- |
| `token_endpoint` | `HttpUri` | *Required* | Token Exchange (Phase 3) | The IdP's token endpoint (`uri`, `cluster`, `timeout`) called out-of-band by Arion to exchange the authorization code for tokens. E.g., `http://idp:8080/oauth/token`. |
| `authorization_endpoint` | `SmolStr` | *Required* | Login Redirect (Phase 1) | The browser-facing URL where Arion redirects unauthenticated users for authentication. E.g., `https://auth.example.com/oauth/authorize`. |
| `credentials` | `OAuth2Credentials` | *Required* | All Phases | Client ID, secret keys, HMAC signing key, and cookie naming settings (see Section 3.2). |
| `redirect_uri` | `SmolStr` | *Required* | Phases 1 & 3 | The redirect URI registered with the IdP. **Supports dynamic access log operators**: `%REQ(x-forwarded-proto)%://%REQ(:authority)%/callback`. Formatted on-demand with zero hot-path overhead. |
| `redirect_path_matcher` | `PathMatcher` | *Required* | Callback (Phase 3) | The exact path matcher (e.g., `exact: "/callback"`) intercepted by Arion to complete code exchange and session establishment. |
| `signout_path` | `PathMatcher` | *Required* | Logout (Phase 5) | The path (e.g., `exact: "/signout"`) that terminates user sessions by emitting `Max-Age=0` cookies and redirecting. |
| `end_session_endpoint` | `Option<SmolStr>` | `None` | Logout (Phase 5) | Optional federated OIDC logout URL. When configured, users are redirected here upon signout instead of `/`. |
| `forward_bearer_token` | `bool` | `false` | Protected Requests (Phase 4) | When `true`, extracts the access token from the session cookie and injects `Authorization: Bearer <token>` toward upstream services. |
| `preserve_authorization_header` | `bool` | `false` | Protected Requests (Phase 4) | When `true`, preserves any pre-existing `Authorization` header already sent by the client, avoiding overwrite. |
| `pass_through_matcher` | `Vec<HeaderMatcher>` | `[]` | Initial Inspection | List of header matchers to **completely bypass authentication** (e.g., public health checks, webhooks, or `x-internal-service: true`). |
| `deny_redirect_matcher` | `Vec<HeaderMatcher>` | `[]` | Unauthenticated | If an unauthenticated request matches these headers (e.g., `accept: application/json` or `x-requested-with: XMLHttpRequest`), Arion responds with **401 Unauthorized** instead of a 302 redirect. |
| `auth_scopes` | `Vec<SmolStr>` | `["user"]` | Login Redirect (Phase 1) | OAuth2/OIDC scopes requested from the IdP (e.g., `["openid", "profile", "email"]`). Space-delimited in query parameters. |
| `resources` | `Vec<SmolStr>` | `[]` | Login Redirect (Phase 1) | Optional RFC 8707 resource indicators (`resource=urn:...`) to include in authorization requests. |
| `auth_type` | `AuthType` | `url_encoded_body` | Token Exchange (Phase 3) | Authentication scheme for `token_endpoint`: `url_encoded_body` (`client_id` & `client_secret` in form body) or `basic_auth` (`Authorization: Basic base64(id:secret)`). |
| `use_refresh_token` | `bool` | `true` | Session Lifecycle | Refresh-token configuration flag; the actual refresh-token grant flow is not implemented, so this does not enable session renewal. |
| `default_expires_in` | `Duration` | `0s` | Token Exchange (Phase 3) | Fallback session validity duration if the IdP does not provide an `expires_in` field in its token response. |
| `default_refresh_token_expires_in` | `Duration` | `7 days` | Token Exchange (Phase 3) | Maximum lifetime for the `RefreshToken` cookie (default: 604,800 seconds). |
| `csrf_token_expires_in` | `Duration` | `10 minutes` | Login Redirect (Phase 1) | Lifetime of the temporary `OauthNonce` cookie (default: 600 seconds). CSRF nonce validation is not implemented. |
| `disable_access_token_set_cookie` | `bool` | `false` | Cookie Emission | If `true`, avoids setting the access token cookie in the browser (e.g., for IdP-only flows). |
| `disable_id_token_set_cookie` | `bool` | `false` | Cookie Emission | If `true`, avoids persisting the OIDC `IdToken` cookie on the client. |
| `disable_refresh_token_set_cookie` | `bool` | `false` | Cookie Emission | If `true`, avoids persisting the `RefreshToken` cookie on the client. |
| `cookie_configs` | `CookieConfigs` | Default | Cookie Emission | Granular per-cookie attributes (`SameSite`, `Path`, `Partitioned`). |

---

### 3.2. Credentials & Cookies (`OAuth2Credentials` and `CookieNames`)

| Field | Type | Default | Description & Usage |
| :--- | :--- | :--- | :--- |
| `client_id` | `SmolStr` | *Required* | Application ID registered with the OAuth2 Identity Provider. |
| `token_secret` | `SmolStr` | *Required* | Client secret used to authenticate against the `token_endpoint`. |
| `hmac_secret` | `SmolStr` | *Required* | **Arion Gateway private cryptographic key** used to sign and verify the `OauthHMAC` cookie via HMAC-SHA256 (`aws-lc-rs`), protecting sessions from tampering. |
| `cookie_domain` | `Option<SmolStr>` | `None` | Domain attribute for emitted cookies (e.g., `.example.com`). A nonempty value is also the HMAC domain; otherwise signing and verification use the request Host/authority including any port. |
| `cookie_names.bearer_token` | `SmolStr` | `BearerToken` | Cookie storing the OAuth2 access token. |
| `cookie_names.oauth_hmac` | `SmolStr` | `OauthHMAC` | Cookie storing the HMAC-SHA256 cryptographic session signature. |
| `cookie_names.oauth_expires` | `SmolStr` | `OauthExpires` | Cookie storing the Unix epoch expiration timestamp. |
| `cookie_names.id_token` | `SmolStr` | `IdToken` | Cookie storing the OpenID Connect ID token. |
| `cookie_names.refresh_token` | `SmolStr` | `RefreshToken` | Cookie storing the OAuth2 refresh token. |
| `cookie_names.oauth_nonce` | `SmolStr` | `OauthNonce` | Temporary nonce cookie; CSRF nonce validation is not implemented. |

---

### 3.3. Granular Cookie Attributes (`CookieConfig`)

Every cookie emitted by the filter can be fine-tuned via `cookie_configs`:

- **`same_site`**: `lax` (default), `strict`, `none`, or `disabled`.
- **`path`**: Defaults to `"/"`. Defines cookie scope in client browsers.
- **`partitioned`**: `bool` (default: `false`). Attaches the modern `Partitioned` attribute (CHIPS) required when cookies operate across third-party/cross-site contexts.
