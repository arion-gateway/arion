# Certificate Configuration Troubleshooting Guide

## Common Error: "secret doesn't contain server name"

### Root Cause

This error occurs when the TLS certificate configured in your `arion-runtime-secrets-*.yaml` file does not contain a valid DNS name in its Subject Alternative Name (SAN) extension that can be parsed as a server name.

The Arion proxy extracts the server name from the certificate's SAN extension to match against SNI (Server Name Indication) requests. If no valid DNS name is found in the SAN, the certificate cannot be used.

### What the Code Does

1. Parses the certificate from the `certificate_chain` field
2. Looks for the Subject Alternative Name (SAN) extension
3. Iterates through SAN entries looking for DNS names
4. Validates each DNS name can be parsed as a valid `ServerName`
5. Uses the first valid DNS name found as the server name
6. If no valid DNS name is found, sets `name: None`
7. Later, when creating a `ServerCert`, fails with "secret doesn't contain server name"

### How to Fix

#### Option 1: Generate a New Certificate with SAN

Use OpenSSL to generate a self-signed certificate with the correct SAN:

```bash
# Create a config file for the certificate
cat > cert.conf <<EOF
[req]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
x509_extensions = v3_req

[dn]
CN = cnpp1.example

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = cnpp1.example
DNS.2 = *.cnpp1.example
EOF

# Generate the certificate and key
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout server-key.pem \
  -out server-cert.pem \
  -days 365 \
  -config cert.conf

# View the certificate to verify SAN
openssl x509 -in server-cert.pem -text -noout | grep -A 1 "Subject Alternative Name"
```

#### Option 2: Add SAN to Existing Certificate

If you have an existing certificate without SAN, you'll need to regenerate it. Certificates cannot be modified after signing.

### Verify Your Certificate

Before using a certificate in Arion, verify it has the required SAN:

```bash
# Check for Subject Alternative Name
openssl x509 -in your-cert.pem -text -noout | grep -A 2 "Subject Alternative Name"

# Should show something like:
#     X509v3 Subject Alternative Name:
#         DNS:cnpp1.example, DNS:*.cnpp1.example
```

### Configuration Format

Your `arion-runtime-secrets-*.yaml` should look like:

```yaml
envoy_bootstrap:
  static_resources:
    secrets:
      - name: cnpp1_tls_server
        tls_certificate:
          certificate_chain:
            inline_string: |
              -----BEGIN CERTIFICATE-----
              [Your certificate with SAN here]
              -----END CERTIFICATE-----
          private_key:
            inline_string: |
              -----BEGIN PRIVATE KEY-----
              [Your PKCS8 private key here]
              -----END PRIVATE KEY-----
```

### Common Mistakes

1. **Certificate chain has PUBLIC KEY instead of CERTIFICATE**: Use `-----BEGIN CERTIFICATE-----`, not `-----BEGIN PUBLIC KEY-----`
2. **Private key has PUBLIC KEY instead of PRIVATE KEY**: Use `-----BEGIN PRIVATE KEY-----` or `-----BEGIN RSA PRIVATE KEY-----`
3. **Swapped certificate and key**: Make sure the certificate is in `certificate_chain` and the key is in `private_key`
4. **Certificate without SAN**: Older certificates might only have CN (Common Name) without SAN
5. **PKCS1 vs PKCS8 key format**: Arion expects PKCS8 format (`-----BEGIN PRIVATE KEY-----`). Convert PKCS1 if needed:

```bash
# Convert PKCS1 to PKCS8
openssl pkcs8 -topk8 -nocrypt -in pkcs1-key.pem -out pkcs8-key.pem
```

## Debugging Steps

1. **Enable debug logging**:
   ```bash
   RUST_LOG=debug ./target/debug/arion --config conf/your-config.yaml 2>&1 | tee debug.log
   ```

2. **Look for these debug messages**:
   - `Certificate SAN name` - Shows what SAN entries were found
   - `Certificate Subject's common name` - Shows the CN from the certificate
   - `added secret: CertificateSecret { name: None` - Indicates no valid server name was found
   - `added secret: CertificateSecret { name: Some("...")` - Indicates success

3. **Check the error chain**:
   The full error output now shows:
   ```
   Failed to launch runtimes: Failed to get listeners and clusters
   
   caused by:
       secret doesn't contain server name
   ```

## Related Configuration

The server name extracted from the certificate must match the `server_names` in your listener's `filter_chain_match`:

```yaml
filter_chains:
  - name: filter_chain_https1
    filter_chain_match:
      server_names: [cnpp1.example]  # Must match SAN in certificate
```

## Testing Your Certificate

```bash
# Start the proxy
RUST_LOG=debug ./arion --config conf/arion-runtime-secrets-pkcs1.yaml

# Test with curl (in another terminal)
curl -v --resolve cnpp1.example:8443:127.0.0.1 \
     --cacert server-cert.pem \
     https://cnpp1.example:8443/

# Or test with openssl
openssl s_client -connect localhost:8443 -servername cnpp1.example
```

## See Also

- [OpenSSL Certificate Generation](https://www.openssl.org/docs/man1.1.1/man1/req.html)
- [X.509 Certificate SAN Extension](https://datatracker.ietf.org/doc/html/rfc5280#section-4.2.1.6)
- [Server Name Indication (SNI)](https://datatracker.ietf.org/doc/html/rfc6066#section-3)
