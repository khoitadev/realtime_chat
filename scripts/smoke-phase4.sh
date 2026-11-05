#!/usr/bin/env bash
# Boots auth-service (3010) and chat-service (3000) as real processes against the test
# databases and walks the whole flow. Requires: ./scripts/test-infra.sh, both projects built.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

set -a; . ./.env.test; set +a
CHAT_DB="$DATABASE_URL"
unset DATABASE_URL
(cd auth-service && set -a && . ./.env.test && set +a && npx prisma migrate deploy >/dev/null)

AUTH_DB="$(grep '^DATABASE_URL=' auth-service/.env.test | cut -d'"' -f2)"
KEY="$(./scripts/gen-jwt-keys.sh | cut -d'"' -f2)"
FAIL=0
pids=()
cleanup() { for p in "${pids[@]:-}"; do kill -TERM "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

PORT=3010 DATABASE_URL="$AUTH_DB" JWT_PRIVATE_KEY="$KEY" node auth-service/dist/main.js >/tmp/smoke-auth.log 2>&1 &
pids+=($!)
PORT=3000 DATABASE_URL="$CHAT_DB" AUTH_JWKS_URL="http://localhost:3010/.well-known/jwks.json" node dist/main.js >/tmp/smoke-chat.log 2>&1 &
pids+=($!)
sleep 6

check() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1 (expected $2, got $3)"; FAIL=1; fi; }
EMAIL="smoke-$(date +%s)@example.com"

code=$(curl -s -o /dev/null -w '%{http_code}' localhost:3000/rooms)
check "chat rejects anonymous" 401 "$code"

TOKENS=$(curl -s -X POST localhost:3010/auth/register -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"password123\",\"displayName\":\"Smoke\"}")
ACCESS=$(printf '%s' "$TOKENS" | node -pe 'JSON.parse(require("fs").readFileSync(0)).accessToken')
check "register returns access token" true "$([ -n "$ACCESS" ] && echo true || echo false)"

code=$(curl -s -o /dev/null -w '%{http_code}' localhost:3000/rooms -H "Authorization: Bearer $ACCESS")
check "chat accepts auth-service token (JWKS)" 200 "$code"

ROOM=$(curl -s -X POST localhost:3000/rooms -H "Authorization: Bearer $ACCESS" \
  -H 'Content-Type: application/json' -d '{"name":"smoke"}')
ROOM_ID=$(printf '%s' "$ROOM" | node -pe 'JSON.parse(require("fs").readFileSync(0)).id')
check "create room" true "$([ -n "$ROOM_ID" ] && echo true || echo false)"

TICKET=$(curl -s -X POST localhost:3000/ws-ticket -H "Authorization: Bearer $ACCESS" \
  | node -pe 'JSON.parse(require("fs").readFileSync(0)).ticket')
RESULT=$(node -e '
const { io } = require("socket.io-client");
const s = io("http://localhost:3000", { transports: ["websocket"], auth: { ticket: process.argv[1] }, reconnection: false });
s.on("connect", () => { console.log("connected"); s.disconnect(); });
s.on("connect_error", (e) => { console.log("error:" + e.message); process.exit(0); });
' "$TICKET")
check "websocket connects with ticket" connected "$RESULT"

code=$(curl -s -o /dev/null -w '%{http_code}' localhost:3010/auth/me -H "Authorization: Bearer $ACCESS")
check "auth-service /auth/me" 200 "$code"

exit $FAIL
