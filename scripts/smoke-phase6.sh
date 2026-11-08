#!/usr/bin/env bash
# Boots auth-service(3010), chat-service(3000), gateway(3020), notification-service and a webhook
# sink against the test infra; walks REST + WS sends, correlation ids and broker-side retries.
# Requires: ./scripts/test-infra.sh and all projects built (npm run build in each).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
TMP="$(mktemp -d)"
SINK_OUT="$TMP/sink.jsonl"
touch "$SINK_OUT"

set -a; . ./.env.test; set +a
CHAT_DB="$DATABASE_URL"; unset DATABASE_URL
AUTH_DB="$(grep '^DATABASE_URL=' auth-service/.env.test | cut -d'"' -f2)"
KEY="$(./scripts/gen-jwt-keys.sh | cut -d'"' -f2)"
JWKS="http://localhost:3010/.well-known/jwks.json"
export INTERNAL_SECRET=smoke-internal-secret
FAIL=0; pids=()
cleanup() { for p in "${pids[@]:-}"; do kill -TERM "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; rabbitmqctl purge_queue -p chat_test notifications.work >/dev/null 2>&1 || true; }
trap cleanup EXIT

rabbitmqctl delete_queue -p chat_test notifications.work >/dev/null 2>&1 || true
for q in 1 2 3; do rabbitmqctl delete_queue -p chat_test notifications.retry.$q >/dev/null 2>&1 || true; done
rabbitmqctl delete_queue -p chat_test notifications.dlq >/dev/null 2>&1 || true
(cd auth-service && set -a && . ./.env.test && set +a && npx prisma migrate deploy >/dev/null)

node scripts/smoke-sink.js "$SINK_OUT" 3099 & pids+=($!)
PORT=3010 DATABASE_URL="$AUTH_DB" JWT_PRIVATE_KEY="$KEY" node auth-service/dist/main.js >"$TMP/auth.log" 2>&1 & pids+=($!)
PORT=3000 DATABASE_URL="$CHAT_DB" AUTH_JWKS_URL="$JWKS" AMQP_URL="$AMQP_URL" node dist/main.js >"$TMP/chat.log" 2>&1 & pids+=($!)
(cd gateway && PORT=3020 AUTH_JWKS_URL="$JWKS" AUTH_SERVICE_URL=http://localhost:3010 CHAT_SERVICE_URL=http://localhost:3000 exec node dist/main.js >"$TMP/gw.log" 2>&1) & pids+=($!)
(cd notification-service && AMQP_URL="$AMQP_URL" REDIS_URL=redis://localhost:6379/2 PUSH_WEBHOOK_URL=http://127.0.0.1:3099/hook RETRY_TTL_MS=500,1000,2000 exec node dist/main.js >"$TMP/notif.log" 2>&1) & pids+=($!)

for _ in $(seq 1 30); do curl -sf localhost:3010/.well-known/jwks.json >/dev/null && curl -s -o /dev/null localhost:3020/rooms && break; sleep 1; done
sleep 3

check() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1 (expected $2, got $3)"; FAIL=1; fi; }
count() { wc -l < "$SINK_OUT" | tr -d ' '; }
wait_count() { for _ in $(seq 1 40); do [ "$(count)" -ge "$1" ] && return 0; sleep 0.5; done; return 1; }
json() { node -pe "JSON.parse(require('fs').readFileSync(0)).$1"; }

STAMP=$(date +%s)
reg() { curl -s -X POST localhost:3020/auth/register -H 'Content-Type: application/json' -d "{\"email\":\"$1$STAMP@x.com\",\"password\":\"password123\",\"displayName\":\"$1\"}"; }
A=$(reg a); B=$(reg b)
TA=$(printf '%s' "$A" | json accessToken); TB=$(printf '%s' "$B" | json accessToken)
ROOM=$(curl -s -X POST localhost:3020/rooms -H "Authorization: Bearer $TA" -H 'Content-Type: application/json' -d '{"name":"smoke"}' | json id)
curl -s -o /dev/null -X POST "localhost:3020/rooms/$ROOM/join" -H "Authorization: Bearer $TB"
B_ID=$(curl -s localhost:3020/auth/me -H "Authorization: Bearer $TB" | json id)

# 1. REST send with a client correlation id -> exactly one webhook for B, same correlation id
curl -s -o /dev/null -X POST "localhost:3020/rooms/$ROOM/messages" -H "Authorization: Bearer $TA" \
  -H 'Content-Type: application/json' -H 'X-Correlation-Id: smoke-corr-1' -d '{"content":"hello via REST"}'
wait_count 1 || true
check "REST send -> 1 webhook" 1 "$(count)"
check "webhook recipient is B" "$B_ID" "$(sed -n 1p "$SINK_OUT" | json body.recipientId)"
check "correlation id preserved gateway->chat->event->webhook" smoke-corr-1 "$(sed -n 1p "$SINK_OUT" | json "headers['x-correlation-id']")"

# 2. WS send through the gateway -> a second webhook with a gateway-generated correlation id
TICKET=$(curl -s -X POST localhost:3020/ws-ticket -H "Authorization: Bearer $TA" | json ticket)
node -e '
const { io } = require("./web/node_modules/socket.io-client");
const s = io("http://localhost:3020", { transports: ["websocket"], auth: { ticket: process.argv[1] }, reconnection: false });
s.on("connect", () => s.emit("room:subscribe", { roomId: process.argv[2] }, () =>
  s.emit("message:send", { roomId: process.argv[2], content: "hello via WS" }, () => setTimeout(() => process.exit(0), 300))));
s.on("connect_error", (e) => { console.error(e.message); process.exit(1); });
' "$TICKET" "$ROOM"
wait_count 2 || true
check "WS send -> second webhook" 2 "$(count)"
CORR2=$(sed -n 2p "$SINK_OUT" | json "headers['x-correlation-id']")
check "WS send carries a generated correlation id" true "$([[ "$CORR2" =~ ^[0-9a-f-]{36}$ ]] && echo true || echo false)"

# 3. Provider failing twice -> broker-side retries (500ms, 1s tiers) -> eventually delivered once
curl -s "http://127.0.0.1:3099/__fail?n=2" >/dev/null
BEFORE=$(count)
curl -s -o /dev/null -X POST "localhost:3020/rooms/$ROOM/messages" -H "Authorization: Bearer $TA" \
  -H 'Content-Type: application/json' -d '{"content":"retry me"}'
wait_count $((BEFORE + 3)) || true
check "2 failures + 1 success recorded by the sink" 3 "$(( $(count) - BEFORE ))"
check "last attempt succeeded" 200 "$(tail -n 1 "$SINK_OUT" | json status)"
sleep 2
check "no extra deliveries afterwards" 3 "$(( $(count) - BEFORE ))"

# 4. Broker restart safety: RabbitMQ drops all connections, new messages still flow
rabbitmqctl close_all_connections --vhost chat_test smoke >/dev/null
sleep 4
BEFORE=$(count)
curl -s -o /dev/null -X POST "localhost:3020/rooms/$ROOM/messages" -H "Authorization: Bearer $TA" \
  -H 'Content-Type: application/json' -d '{"content":"after reconnect"}'
wait_count $((BEFORE + 1)) || true
check "delivery works after the broker dropped all connections" 1 "$(( $(count) - BEFORE ))"

grep -ciE "error|exception" "$TMP"/*.log | grep -v ":0" || echo "ok   no errors in service logs (see $TMP)"
exit $FAIL
