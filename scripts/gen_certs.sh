#!/usr/bin/env bash
set -euo pipefail
OUT="${1:-certs}"
NODE="${2:-node-001}"
mkdir -p "$OUT"
openssl genrsa -out "$OUT/ca.key" 4096 2>/dev/null
openssl req -new -x509 -days 3650 -key "$OUT/ca.key" -subj "/CN=ReclaimNet-CA/O=ReclaimNet" -out "$OUT/ca.crt" 2>/dev/null
gen_cert() {
  local name="$1" cn="$2"
  openssl genrsa -out "$OUT/$name.key" 2048 2>/dev/null
  openssl req -new -key "$OUT/$name.key" -subj "/CN=$cn/O=ReclaimNet" -out "$OUT/$name.csr" 2>/dev/null
  cat > "$OUT/$name.ext" <<EXT
subjectAltName=DNS:$cn,DNS:localhost,IP:127.0.0.1
extendedKeyUsage=serverAuth,clientAuth
keyUsage=digitalSignature,keyEncipherment
EXT
  openssl x509 -req -days 365 -in "$OUT/$name.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" -CAcreateserial -extfile "$OUT/$name.ext" -out "$OUT/$name.crt" 2>/dev/null
  rm -f "$OUT/$name.csr" "$OUT/$name.ext"
}
gen_cert coordinator coordinator
gen_cert agent "$NODE"
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out "$OUT/jwt.key" 2>/dev/null
openssl pkey -in "$OUT/jwt.key" -pubout -out "$OUT/jwt.pub" 2>/dev/null
chmod 600 "$OUT/ca.key" "$OUT/coordinator.key" "$OUT/agent.key" "$OUT/jwt.key"
