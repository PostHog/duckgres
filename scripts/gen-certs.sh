#!/bin/bash
# Generate self-signed certificates for Duckgres development
# Usage: ./scripts/gen-certs.sh

set -e

CERT_DIR="./certs"
DAYS=365
KEY_SIZE=2048

# Create certs directory
mkdir -p "$CERT_DIR"

# Generate private key
openssl genrsa -out "$CERT_DIR/server.key" $KEY_SIZE

# Generate self-signed certificate
openssl req -new -x509 \
    -key "$CERT_DIR/server.key" \
    -out "$CERT_DIR/server.crt" \
    -days $DAYS \
    -subj "/CN=localhost/O=Duckgres/C=US" \
    -addext "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1"

# Set permissions
chmod 600 "$CERT_DIR/server.key"
chmod 644 "$CERT_DIR/server.crt"

echo "Generated certificates in $CERT_DIR/"
echo "  - server.key (private key)"
echo "  - server.crt (certificate)"
echo ""
echo "Certificate details:"
openssl x509 -in "$CERT_DIR/server.crt" -noout -subject -dates
