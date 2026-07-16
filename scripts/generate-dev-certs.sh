#!/bin/sh
set -eu

output="${1:-deploy/dev-certs}"
mkdir -p "$output"

openssl rand -hex 32 > "$output/discovery.token"
chmod 600 "$output/discovery.token"

openssl ecparam -name prime256v1 -genkey -noout -out "$output/ca.key"
openssl req -x509 -new -sha256 -days 3650 \
  -key "$output/ca.key" -out "$output/ca.pem" \
  -subj "/CN=RustQueue Development CA"

key="$output/node-1.key"
csr="$output/node-1.csr"
certificate="$output/node-1.pem"
extensions="$output/extensions.cnf"
openssl ecparam -name prime256v1 -genkey -noout -out "$key"
openssl req -new -key "$key" -out "$csr" -subj "/CN=rustqueue-1"
printf '%s\n' \
  'subjectAltName=DNS:rustqueue-1,DNS:localhost,IP:127.0.0.1' \
  'extendedKeyUsage=serverAuth,clientAuth' > "$extensions"
openssl x509 -req -sha256 -days 825 \
  -in "$csr" -CA "$output/ca.pem" -CAkey "$output/ca.key" \
  -CAcreateserial -out "$certificate" -extfile "$extensions"
rm -f "$csr" "$extensions" "$output/ca.srl"
chmod 600 "$key"

chmod 600 "$output/ca.key"
printf 'development certificates written to %s\n' "$output"
