#!/usr/bin/env bash



set -euo pipefail

OUT="${1:-certs}"
mkdir -p "$OUT"


openssl genrsa -out "$OUT/ca.key" 4096 2>/dev/null
openssl req -new -x509 -days 3650 -key "$OUT/ca.key" \
  -subj "/CN=ReclaimNet-CA/O=ReclaimNet" \
  -out "$OUT/ca.crt" 2>/dev/null

gen_cert() {
  local name="$1"
  local cn="$2"
  openssl genrsa -out "$OUT/$name.key" 2048 2>/dev/null
  openssl req -new -key "$OUT/$name.key" \
    -subj "/CN=$cn/O=ReclaimNet" \
    -out "$OUT/$name.csr" 2>/dev/null
  openssl x509 -req -days 365 \
    -in "$OUT/$name.csr" \
    -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial \
    -out "$OUT/$name.crt" 2>/dev/null
  rm -f "$OUT/$name.csr"
}

gen_cert "coordinator" "coordinator"
gen_cert "agent"       "agent"

echo "Certificates written to $OUT/"
ls -1 "$OUT/"
