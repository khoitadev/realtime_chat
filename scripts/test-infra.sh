#!/usr/bin/env bash
# Start native Postgres + Redis for tests when Docker is unavailable.
# Idempotent. Creates .env.test if missing. Usage: ./scripts/test-infra.sh
set -euo pipefail

PG_PORT="${PG_PORT:-5432}"
REDIS_PORT="${REDIS_PORT:-6379}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if ! pg_isready -q -p "$PG_PORT"; then
  service postgresql start >/dev/null 2>&1 || pg_ctlcluster "$(ls /usr/lib/postgresql | tail -1)" main start
  for _ in $(seq 1 20); do pg_isready -q -p "$PG_PORT" && break; sleep 0.5; done
fi

su postgres -c "psql -p $PG_PORT -qtc \"ALTER USER postgres PASSWORD 'postgres'\"" >/dev/null
for db in chat_dev chat_test auth_dev auth_test; do
  su postgres -c "psql -p $PG_PORT -tAc \"SELECT 1 FROM pg_database WHERE datname='$db'\"" | grep -q 1 \
    || su postgres -c "createdb -p $PG_PORT $db"
done

if ! redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1; then
  redis-server --port "$REDIS_PORT" --daemonize yes --save "" --dir /tmp >/dev/null
  for _ in $(seq 1 20); do redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1 && break; sleep 0.25; done
fi

if ! command -v rabbitmq-server >/dev/null 2>&1; then
  DEBIAN_FRONTEND=noninteractive apt-get install -y rabbitmq-server >/dev/null
fi
if ! rabbitmqctl status >/dev/null 2>&1; then
  rabbitmq-server -detached >/dev/null 2>&1 || true
  for _ in $(seq 1 30); do rabbitmqctl await_startup --timeout 5 >/dev/null 2>&1 && break; sleep 2; done
fi
rabbitmqctl list_vhosts -q | grep -qx chat_test || rabbitmqctl add_vhost chat_test >/dev/null
rabbitmqctl list_users -q | awk '{print $1}' | grep -qx chat || rabbitmqctl add_user chat chat >/dev/null
rabbitmqctl set_permissions -p chat_test chat ".*" ".*" ".*" >/dev/null

if [ ! -f "$ROOT/.env.test" ]; then
  cat > "$ROOT/.env.test" <<ENV
DATABASE_URL="postgresql://postgres:postgres@localhost:${PG_PORT}/chat_test"
REDIS_URL="redis://localhost:${REDIS_PORT}/1"
ENV
fi

grep -q '^AMQP_URL=' "$ROOT/.env.test" || echo 'AMQP_URL="amqp://chat:chat@localhost:5672/chat_test"' >> "$ROOT/.env.test"

mkdir -p "$ROOT/notification-service"
if [ ! -f "$ROOT/notification-service/.env.test" ]; then
  cat > "$ROOT/notification-service/.env.test" <<ENV
AMQP_URL="amqp://chat:chat@localhost:5672/chat_test"
REDIS_URL="redis://localhost:${REDIS_PORT}/2"
ENV
fi

if [ ! -f "$ROOT/auth-service/.env.test" ]; then
  cat > "$ROOT/auth-service/.env.test" <<ENV
DATABASE_URL="postgresql://postgres:postgres@localhost:${PG_PORT}/auth_test"
ENV
fi

if [ ! -f "$ROOT/auth-service/.env" ]; then
  {
    echo "DATABASE_URL=\"postgresql://postgres:postgres@localhost:${PG_PORT}/auth_dev\""
    "$ROOT/scripts/gen-jwt-keys.sh"
  } > "$ROOT/auth-service/.env"
fi

echo "postgres:${PG_PORT} redis:${REDIS_PORT} rabbitmq:5672 ready"
