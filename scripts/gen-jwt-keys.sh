#!/usr/bin/env bash
# Prints a single-line JWT_PRIVATE_KEY (PEM with literal \n) for an .env file.
# Usage: ./scripts/gen-jwt-keys.sh >> auth-service/.env
set -euo pipefail
KEY="$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 2>/dev/null)"
printf 'JWT_PRIVATE_KEY="%s"\n' "$(printf '%s\n' "$KEY" | awk '{printf "%s\\n", $0}')"
