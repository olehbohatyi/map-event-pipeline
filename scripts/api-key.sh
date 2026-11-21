#!/usr/bin/env bash
# Makes an API key for the gate's writes (POST events, resolve).
#   scripts/api-key.sh <client-name>
# Prints the key, for the client only, and the name:hash pair to add to the gate's GATE_API_KEYS (comma-separated).
# The gate keeps only the hash; a lost key cannot be recovered, only replaced.
set -euo pipefail

name="${1:?usage: scripts/api-key.sh <client-name>}"
[[ "$name" =~ ^[A-Za-z0-9_.-]{1,64}$ ]] || { echo "name: 1-64 letters, digits, '_', '.' or '-'" >&2; exit 1; }

key=$(head -c 32 /dev/urandom | base64 | tr -d '/+=' | head -c 40)
if command -v sha256sum >/dev/null; then hash=$(printf '%s' "$key" | sha256sum | cut -d' ' -f1)
else hash=$(printf '%s' "$key" | shasum -a 256 | cut -d' ' -f1); fi

echo "key (give it to the client):  $key"
echo "GATE_API_KEYS entry:          $name:$hash"
