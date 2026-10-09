#!/usr/bin/env python3
"""
Lightweight Mock OAuth2 Server & Upstream Backend for testing Arion Gateway.
Zero external dependencies (uses standard library http.server).

Endpoints:
- OAuth2 Authorize endpoint: GET  /oauth/authorize (default port 8089)
- OAuth2 Token endpoint:     POST /oauth/token     (default port 8089)
- Upstream Mock API:         GET  /api/hello       (optional, when --upstream-port is specified)

Usage:
  python3 tools/oauth2_backend.py
  python3 tools/oauth2_backend.py --upstream-port 8090
  python3 tools/oauth2_backend.py --port 8089 --upstream-port 8090 --auto-approve
"""

import argparse
import base64
import http.server
import json
import secrets
import sys
import threading
import urllib.parse
from datetime import datetime

# ANSI colors for clear console output
CYAN = "\033[96m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
MAGENTA = "\033[95m"
BOLD = "\033[1m"
RESET = "\033[0m"

# In-memory store for generated authorization codes
ACTIVE_CODES = set()

HTML_LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Mock OAuth2 Authorization Server</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #f8fafc; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }
        .card { background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 32px; width: 460px; box-shadow: 0 10px 25px -5px rgba(0,0,0,0.5); }
        h2 { margin-top: 0; color: #38bdf8; font-size: 22px; }
        .info { background: #0f172a; border-radius: 8px; padding: 14px; margin: 18px 0; font-size: 13px; line-height: 1.6; }
        .badge { display: inline-block; background: #0284c7; color: white; border-radius: 4px; padding: 2px 8px; font-weight: bold; }
        .badge-pkce { background: #10b981; }
        .btn-group { display: flex; gap: 12px; margin-top: 24px; }
        button { flex: 1; padding: 12px; border-radius: 8px; font-weight: bold; cursor: pointer; border: none; font-size: 14px; }
        .btn-primary { background: #38bdf8; color: #0f172a; }
        .btn-primary:hover { background: #7dd3fc; }
        .btn-secondary { background: #475569; color: white; }
        .btn-secondary:hover { background: #64748b; }
    </style>
</head>
<body>
    <div class="card">
        <h2>🔐 Arion OAuth2 Mock Server</h2>
        <p style="color: #94a3b8; font-size: 14px;">An application is requesting authorization to access your account.</p>
        
        <div class="info">
            <div><strong>Client ID:</strong> <code>{client_id}</code></div>
            <div><strong>Scopes:</strong> <span class="badge">{scope}</span></div>
            <div><strong>Redirect URI:</strong> <span style="word-break: break-all; color: #cbd5e1;">{redirect_uri}</span></div>
            <div><strong>State:</strong> <code style="color: #a78bfa; word-break: break-all;">{state}</code></div>
            {pkce_row}
        </div>

        <form method="POST" action="/oauth/authorize/confirm">
            <input type="hidden" name="redirect_uri" value="{redirect_uri}">
            <input type="hidden" name="state" value="{state}">
            <div class="btn-group">
                <button type="submit" name="action" value="approve" class="btn-primary">✓ Authorize (Login as Alice)</button>
                <button type="submit" name="action" value="deny" class="btn-secondary">✕ Deny</button>
            </div>
        </form>
    </div>
</body>
</html>
"""

def log(tag: str, color: str, msg: str):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"{color}[{now}] [{tag}]{RESET} {msg}")

class OAuthHandler(http.server.BaseHTTPRequestHandler):
    auto_approve: bool = False

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if parsed.path == "/oauth/authorize":
            client_id = params.get("client_id", ["unknown"])[0]
            redirect_uri = params.get("redirect_uri", ["http://localhost:8080/callback"])[0]
            state = params.get("state", ["/"])[0]
            scope = params.get("scope", ["openid profile"])[0]
            code_challenge = params.get("code_challenge", [""])[0]
            code_challenge_method = params.get("code_challenge_method", [""])[0]

            log("AUTHORIZE", CYAN, f"Received authorization request from client '{client_id}'")
            log("AUTHORIZE", CYAN, f"  ↳ redirect_uri         : {redirect_uri}")
            log("AUTHORIZE", CYAN, f"  ↳ state                : {state}")
            log("AUTHORIZE", CYAN, f"  ↳ scopes               : {scope}")
            if code_challenge:
                log("AUTHORIZE", CYAN, f"  ↳ PKCE challenge       : {code_challenge}")
                log("AUTHORIZE", CYAN, f"  ↳ PKCE method          : {code_challenge_method or 'plain'}")

            code = f"auth_code_{secrets.token_hex(16)}"
            ACTIVE_CODES.add(code)

            if self.auto_approve or "auto" in params:
                log("AUTHORIZE", GREEN, f"Auto-approving authorization, redirecting with code '{code}'")
                sep = "&" if "?" in redirect_uri else "?"
                target = f"{redirect_uri}{sep}code={code}&state={urllib.parse.quote(state)}"
                self.send_response(302)
                self.send_header("Location", target)
                self.end_headers()
                return

            if code_challenge:
                pkce_row = (
                    f"<div><strong>PKCE:</strong> <span class=\"badge badge-pkce\">{code_challenge_method or 'plain'}</span> "
                    f"<code style=\"font-size: 11px; color: #94a3b8; word-break: break-all;\">{code_challenge}</code></div>"
                )
            else:
                pkce_row = ""

            # Display mock HTML approval screen
            body = (HTML_LOGIN_PAGE
                .replace("{client_id}", client_id)
                .replace("{redirect_uri}", redirect_uri)
                .replace("{state}", state)
                .replace("{scope}", scope)
                .replace("{pkce_row}", pkce_row)
            ).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"Not Found")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        length = int(self.headers.get("Content-Length", 0))
        post_body = self.rfile.read(length).decode("utf-8")
        params = urllib.parse.parse_qs(post_body)

        if parsed.path == "/oauth/authorize/confirm":
            action = params.get("action", ["deny"])[0]
            redirect_uri = params.get("redirect_uri", ["http://localhost:8080/callback"])[0]
            state = params.get("state", ["/"])[0]

            if action == "approve":
                code = f"auth_code_{secrets.token_hex(16)}"
                ACTIVE_CODES.add(code)
                log("AUTHORIZE", GREEN, f"User approved request! Issuing code '{code}'")
                sep = "&" if "?" in redirect_uri else "?"
                target = f"{redirect_uri}{sep}code={code}&state={urllib.parse.quote(state)}"
                self.send_response(302)
                self.send_header("Location", target)
                self.end_headers()
            else:
                log("AUTHORIZE", YELLOW, f"User denied authorization request.")
                sep = "&" if "?" in redirect_uri else "?"
                target = f"{redirect_uri}{sep}error=access_denied&state={urllib.parse.quote(state)}"
                self.send_response(302)
                self.send_header("Location", target)
                self.end_headers()
            return

        if parsed.path == "/oauth/token":
            grant_type = params.get("grant_type", [""])[0]
            code = params.get("code", [""])[0]
            redirect_uri = params.get("redirect_uri", [""])[0]
            code_verifier = params.get("code_verifier", [""])[0]

            client_id = ""
            client_secret = ""
            auth_source = ""

            auth_header = self.headers.get("Authorization", "")
            if auth_header.startswith("Basic "):
                try:
                    raw_b64 = auth_header.split(" ", 1)[1].strip()
                    decoded = base64.b64decode(raw_b64).decode("utf-8")
                    if ":" in decoded:
                        client_id, client_secret = decoded.split(":", 1)
                        auth_source = " [via Basic Auth header]"
                except Exception:
                    pass

            if not client_id:
                client_id = params.get("client_id", [""])[0]
                client_secret = params.get("client_secret", [""])[0]
                if client_id:
                    auth_source = " [via URL-encoded body]"

            log("TOKEN", MAGENTA, f"Received token exchange request:")
            log("TOKEN", MAGENTA, f"  ↳ grant_type    : {grant_type}")
            log("TOKEN", MAGENTA, f"  ↳ client_id     : {client_id or '[NONE]'}{auth_source}")
            log("TOKEN", MAGENTA, f"  ↳ client_secret : {'***' if client_secret else '[NONE]'}")
            log("TOKEN", MAGENTA, f"  ↳ code          : {code}")
            if redirect_uri:
                log("TOKEN", MAGENTA, f"  ↳ redirect_uri  : {redirect_uri}")
            if code_verifier:
                log("TOKEN", MAGENTA, f"  ↳ code_verifier : {code_verifier}")

            if grant_type != "authorization_code":
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "unsupported_grant_type"}).encode("utf-8"))
                return

            if code not in ACTIVE_CODES:
                log("TOKEN", YELLOW, f"Invalid or expired code '{code}'")
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "invalid_grant", "error_description": "Code expired or invalid"}).encode("utf-8"))
                return

            # Consume the authorization code (single-use)
            ACTIVE_CODES.remove(code)

            token_response = {
                "access_token": f"mock_bearer_token_{secrets.token_hex(20)}",
                "token_type": "Bearer",
                "expires_in": 3600,
                "refresh_token": f"mock_refresh_token_{secrets.token_hex(20)}",
                "scope": "user email profile",
            }
            log("TOKEN", GREEN, f"Successfully exchanged code for Bearer token: {token_response['access_token']}")

            body = json.dumps(token_response).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"Not Found")

    def log_message(self, format, *args):
        # Suppress default stdlib http log formatting for clean output
        pass


class UpstreamHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        auth_header = self.headers.get("Authorization", "")
        log("UPSTREAM", YELLOW, f"Received upstream request {self.command} {self.path}")
        log("UPSTREAM", YELLOW, f"  ↳ Authorization header: {auth_header if auth_header else '[NONE]'}")

        if not auth_header.startswith("Bearer "):
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"error": "Unauthorized: missing or invalid Bearer token"}')
            return

        token = auth_header.split(" ", 1)[1]
        response_data = {
            "status": "success",
            "message": "Welcome to the protected API behind Arion Gateway!",
            "user": "alice@example.com",
            "received_bearer_token": token,
            "timestamp": datetime.now().isoformat(),
        }
        body = json.dumps(response_data, indent=2).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


def main():
    parser = argparse.ArgumentParser(description="Mock OAuth2 & Upstream Server for Arion Gateway")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8089, help="OAuth2 server port (default: 8089)")
    parser.add_argument("--upstream-port", type=int, default=None, help="Optional upstream mock backend port (not bound if omitted)")
    parser.add_argument("--auto-approve", action="store_true", help="Automatically approve authorization requests without user interaction")
    args = parser.parse_args()

    OAuthHandler.auto_approve = args.auto_approve

    oauth_server = http.server.HTTPServer((args.host, args.port), OAuthHandler)
    t1 = threading.Thread(target=oauth_server.serve_forever, daemon=True)
    t1.start()

    upstream_server = None
    t2 = None
    if args.upstream_port is not None:
        upstream_server = http.server.HTTPServer((args.host, args.upstream_port), UpstreamHandler)
        t2 = threading.Thread(target=upstream_server.serve_forever, daemon=True)
        t2.start()

    print(f"\n{BOLD}{GREEN}===================================================================={RESET}")
    title = "Arion OAuth2 Mock Server" + (" & Upstream Backend" if upstream_server else "") + " running!"
    print(f"{BOLD}{GREEN}  {title}{RESET}")
    print(f"{BOLD}{GREEN}===================================================================={RESET}")
    print(f"  • {BOLD}OAuth2 Server:{RESET}      http://{args.host}:{args.port}")
    print(f"    - Authorize endpoint:   http://{args.host}:{args.port}/oauth/authorize")
    print(f"    - Token endpoint:       http://{args.host}:{args.port}/oauth/token")
    if upstream_server:
        print(f"  • {BOLD}Upstream Backend:{RESET}   http://{args.host}:{args.upstream_port}")
        print(f"    - Protected API:        http://{args.host}:{args.upstream_port}/api/hello")
    print(f"  • Mode:                   {'Auto-approve (headless curl)' if args.auto_approve else 'Interactive HTML consent screen'}")
    print(f"{BOLD}{GREEN}===================================================================={RESET}\n")

    try:
        t1.join()
        if t2:
            t2.join()
    except KeyboardInterrupt:
        server_str = "servers" if upstream_server else "server"
        print(f"\n{YELLOW}Shutting down mock {server_str}...{RESET}")
        oauth_server.shutdown()
        if upstream_server:
            upstream_server.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    main()
