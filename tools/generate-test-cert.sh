#!/bin/bash
# generate-test-cert.sh
# Helper script to generate test TLS certificates with proper Subject Alternative Name (SAN)
# for use with Orion proxy

set -e

# Default values
DOMAIN="${1:-cnpp1.example}"
DAYS="${2:-365}"
OUTPUT_DIR="$(dirname "$0")"
KEY_FILE="${OUTPUT_DIR}/${DOMAIN}-key.pem"
CERT_FILE="${OUTPUT_DIR}/${DOMAIN}-cert.pem"
CONFIG_FILE="${OUTPUT_DIR}/${DOMAIN}-cert.conf"

echo "Generating test certificate for: ${DOMAIN}"
echo "Valid for: ${DAYS} days"
echo "Output directory: ${OUTPUT_DIR}"

# Create OpenSSL config file with SAN
cat > "${CONFIG_FILE}" <<EOF
[req]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
x509_extensions = v3_req

[dn]
CN = ${DOMAIN}
O = Orion Test Certificate
OU = Testing
C = US

[v3_req]
subjectAltName = @alt_names
basicConstraints = CA:TRUE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth

[alt_names]
DNS.1 = ${DOMAIN}
DNS.2 = *.${DOMAIN}
EOF

echo "Generated OpenSSL config: ${CONFIG_FILE}"

# Generate the certificate and private key
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "${KEY_FILE}" \
  -out "${CERT_FILE}" \
  -days "${DAYS}" \
  -config "${CONFIG_FILE}"

echo ""
echo "Certificate generated successfully!"
echo "  Private key: ${KEY_FILE}"
echo "  Certificate: ${CERT_FILE}"
echo ""

# Verify the certificate
echo "=== Certificate Details ==="
openssl x509 -in "${CERT_FILE}" -text -noout | grep -A 2 "Subject Alternative Name"

echo ""
echo "=== Subject ==="
openssl x509 -in "${CERT_FILE}" -noout -subject

echo ""
echo "=== Validity ==="
openssl x509 -in "${CERT_FILE}" -noout -dates

echo ""
echo "=== Usage Instructions ==="
echo "To use this certificate in your Orion configuration:"
echo ""
echo "1. Copy the certificate content:"
echo "   cat ${CERT_FILE}"
echo ""
echo "2. Copy the private key content:"
echo "   cat ${KEY_FILE}"
echo ""
echo "3. Paste them into your orion-runtime-secrets-*.yaml file:"
echo ""
cat <<'YAML'
envoy_bootstrap:
  static_resources:
    secrets:
      - name: your_tls_server
        tls_certificate:
          certificate_chain:
            inline_string: |
              # Paste certificate content here (from cert file)
          private_key:
            inline_string: |
              # Paste private key content here (from key file)
YAML

echo ""
echo "4. Make sure your listener's server_names matches the certificate domain:"
echo "   server_names: [${DOMAIN}]"
echo ""
echo "=== Test the certificate ==="
echo "Start orion with your config, then test with:"
echo "  curl -v --resolve ${DOMAIN}:8443:127.0.0.1 --cacert ${CERT_FILE} https://${DOMAIN}:8443/"
echo ""

# Clean up config file
rm "${CONFIG_FILE}"