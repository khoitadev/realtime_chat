# Phase 6 — RabbitMQ + notification-service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce RabbitMQ and a first consumer. chat-service publishes a `message.created` event (best-effort, with publisher confirms) after saving a message; a new `notification-service` consumes it and calls a webhook for every recipient with correct idempotency (claim → side-effect → mark done, release on failure), `prefetch=1`, durable broker-side retry (3 TTL tiers + DLQ) and a `correlationId` that flows gateway → chat-service → event → webhook.

**Architecture:** `notification-service/` is an independent NestJS application context (no HTTP). `BrokerService` owns the AMQP connection/confirm-channel with automatic reconnect and re-asserts the topology on every (re)connect. `NotificationConsumer` processes one message at a time: validate → per recipient `IdempotencyService.claim` → `WebhookClient.deliver` → `complete`/`release` → ack, or publish to the next retry queue (confirmed) then ack, or DLQ. Retry queues dead-letter through the **default exchange** straight back into `notifications.work` (so other consumers bound to `chat.events` never see retried duplicates). In chat-service an `EventPublisher` (confirm channel, persistent messages, fire-and-forget with swallowed errors) is called from `RoomsService.sendMessage`. The gateway forwards/generates `X-Correlation-Id`.

**Tech Stack:** `amqplib@^0.10` (+ `@types/amqplib`), RabbitMQ 3.12 (real broker in tests), `ioredis`, NestJS 10, Jest, existing Postgres/Redis stack.

**Spec:** `docs/superpowers/specs/2026-11-08-phase6-rabbitmq-notification-design.md`
**Ledger (Rulings):** `docs/superpowers/plans/2026-11-08-phase6-rabbitmq-notification-ledger.md`

## Global Constraints

- Work on branch `phase-6-rabbitmq-notification` (from `master`). Never push to `master`.
- TDD with real I/O: real Postgres, real Redis, **real RabbitMQ**, real HTTP. The webhook sink is a real local HTTP server (network-level test infrastructure, programmable failures) — never mock the broker, Redis, Prisma or the consumer logic.
- **Never add a `Co-Authored-By` line (or any Claude/AI attribution) to a git commit message.**
- Event contract `message.created` v1 (see spec): `{ eventId, type, version: 1, occurredAt, correlationId, data: { messageId, roomId, senderId, preview (≤100 chars), recipientIds } }`. `recipientIds` = room members except the sender; empty → no publish.
- Publishing is **best-effort**: a failed/slow broker must never fail or delay the user's message send (publish is not awaited by `sendMessage`; errors are logged and counted). Known limitation: crash between DB commit and publish loses the event.
- Topology (asserted idempotently by notification-service on every connect): exchange `chat.events` (topic, durable); queues `notifications.work` (bound `message.created`), `notifications.retry.1/2/3` (durable, fixed `x-message-ttl`, `x-dead-letter-exchange=""`, `x-dead-letter-routing-key=notifications.work`), `notifications.dlq`. Retry TTLs from env `RETRY_TTL_MS` (default `5000,30000,300000`).
- Consumer: `prefetch(1)`, strictly sequential. Idempotency key `notif:{eventId}:{recipientId}`: claim `SET … processing NX PX 60000`; mark `done` (EX 7d) only **after** webhook success; `DEL` on failure; 4xx (≠429) → `rejected` (EX 7d), never retried. Retry = publish to `retry.{attempt+1}` with `x-attempt`, wait for broker confirm, **then** ack; `attempt ≥ 3` → DLQ (`x-reason=retries-exhausted`); unparseable/unsupported events → DLQ (`x-reason`) immediately.
- `correlationId`: accepted only as `[A-Za-z0-9._-]{1,128}`, otherwise replaced by a fresh UUID.
- Tests run serially (`maxWorkers: 1`); RabbitMQ vhost `chat_test`; Redis db 1 (chat-service, gateway) and **db 2** (notification-service). Tests that need `rabbitmqctl` skip themselves when it is not installed.
- Run commands from the repo root unless a step says `cd <project>`.
- Out of scope: outbox, presence-based recipient filtering, real push provider, DLQ metrics (Phase 9), full correlation-id propagation (Phase 9), analytics-service (Phase 7).

---

## File Structure

```
notification-service/                      # NEW independent project (application context, no HTTP)
├── package.json  package-lock.json  tsconfig.json  tsconfig.build.json  nest-cli.json  jest.config.js
├── src/
│   ├── main.ts  app.module.ts
│   ├── config.ts            config.spec.ts             # CONFIG token, loadConfig()
│   ├── redis/redis.module.ts
│   ├── broker/{event.schema.ts, event.schema.spec.ts, topology.ts, topology.spec.ts,
│   │           broker.service.ts, broker.service.spec.ts}
│   ├── idempotency/{idempotency.service.ts, idempotency.service.spec.ts}
│   ├── delivery/{webhook.client.ts, webhook.client.spec.ts}
│   ├── consumer/{notification.consumer.ts, notification.consumer.spec.ts, notification.lifecycle.spec.ts}
└── test/{jest.env.ts, redis-cleanup.ts, helpers.ts, broker.ts, webhook-sink.ts, harness.ts}

src/messaging/                             # chat-service (NEW)
├── events.ts   events.spec.ts             # MessageCreatedEvent + builder
├── correlation.ts  correlation.spec.ts    # sanitizeCorrelationId
├── event.publisher.ts  event.publisher.spec.ts
└── messaging.module.ts                    # @Global
src/rooms/rooms.service.ts  rooms.controller.ts  (+ specs)   # MODIFY
src/app.module.ts                                            # MODIFY
test/amqp.ts  test/jest.env.ts                               # chat-service test helpers

gateway/src/common/correlation.ts (+ spec)  gateway/src/proxy/proxy.controller.ts
gateway/src/realtime/internal-chat.client.ts  events.gateway.ts   # MODIFY (+ specs)

scripts/test-infra.sh  scripts/smoke-phase6.sh  scripts/smoke-sink.js
docker-compose.yml  tsconfig.json  tsconfig.build.json
```

---

### Task 1: Infrastructure (RabbitMQ), dependencies, scaffold `notification-service`

**Files:**
- Modify: `scripts/test-infra.sh`, `docker-compose.yml`, `tsconfig.json`, `tsconfig.build.json`, `test/jest.env.ts`, `package.json`, `package-lock.json`
- Create: `notification-service/{package.json, tsconfig.json, tsconfig.build.json, nest-cli.json, jest.config.js, test/jest.env.ts, test/redis-cleanup.ts, src/main.ts, src/app.module.ts}` (minimal bootable skeleton), `test/amqp.ts`

**Interfaces:**
- Produces: a running RabbitMQ with vhost `chat_test` + user `chat`/`chat` (`AMQP_URL=amqp://chat:chat@localhost:5672/chat_test`); chat-service deps `amqplib` + `@types/amqplib`; compiling `notification-service` project; `test/amqp.ts` helpers for chat-service tests: `resetExchange()`, `listenForEvents()`.

- [ ] **Step 1: Extend `scripts/test-infra.sh` with RabbitMQ**

Insert before the `.env.test` creation block (after the Redis section):

```bash
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
```

and after the existing `.env.test` creation block add:

```bash
grep -q '^AMQP_URL=' "$ROOT/.env.test" || echo 'AMQP_URL="amqp://chat:chat@localhost:5672/chat_test"' >> "$ROOT/.env.test"

mkdir -p "$ROOT/notification-service"
if [ ! -f "$ROOT/notification-service/.env.test" ]; then
  cat > "$ROOT/notification-service/.env.test" <<ENV
AMQP_URL="amqp://chat:chat@localhost:5672/chat_test"
REDIS_URL="redis://localhost:${REDIS_PORT}/2"
ENV
fi
```

Finally change the last line to `echo "postgres:${PG_PORT} redis:${REDIS_PORT} rabbitmq:5672 ready"`.

Run: `./scripts/test-infra.sh && rabbitmqctl list_vhosts -q && rabbitmqctl list_user_permissions chat`
Expected: prints the ready line; `chat_test` listed; user `chat` has `.*` on `chat_test`. Run it twice to confirm it is idempotent.

- [ ] **Step 2: `docker-compose.yml` (Docker users)**

Add under `services:`:

```yaml
  rabbitmq:
    image: rabbitmq:3-management
    environment:
      RABBITMQ_DEFAULT_USER: chat
      RABBITMQ_DEFAULT_PASS: chat
      RABBITMQ_DEFAULT_VHOST: chat_test
    ports:
      - "5672:5672"
      - "15672:15672"
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 10s
      timeout: 5s
      retries: 10
```

- [ ] **Step 3: Exclude the new project from the chat-service TypeScript configs**

Root `tsconfig.json` `exclude` must also contain `"notification-service"`; root `tsconfig.build.json` likewise (keep its existing entries, including `gateway`, `auth-service`, `web`).

- [ ] **Step 4: chat-service dependencies and test env**

```bash
npm install amqplib
npm install --save-dev @types/amqplib
```

Append to `test/jest.env.ts`:

```typescript
// Real RabbitMQ (vhost chat_test) unless configured otherwise.
process.env.AMQP_URL ??= 'amqp://chat:chat@localhost:5672/chat_test';
```

- [ ] **Step 5: Scaffold `notification-service/`**

`notification-service/package.json`:

```json
{
  "name": "realtime-chat-notification-service",
  "version": "0.1.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch",
    "test": "jest",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "@nestjs/common": "^10.4.0",
    "@nestjs/core": "^10.4.0",
    "amqplib": "^0.10.9",
    "ioredis": "^6.0.0",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  },
  "devDependencies": {
    "@nestjs/cli": "^10.4.5",
    "@nestjs/testing": "^10.4.0",
    "@types/amqplib": "^0.10.8",
    "@types/jest": "^29.5.13",
    "@types/node": "^20.16.0",
    "dotenv": "^16.4.5",
    "jest": "^29.7.0",
    "ts-jest": "^29.2.5",
    "ts-node": "^10.9.2",
    "typescript": "^5.6.2"
  }
}
```

`notification-service/tsconfig.json` and `tsconfig.build.json` and `nest-cli.json`: copy the three files from `auth-service/` unchanged (`cp auth-service/tsconfig.json auth-service/tsconfig.build.json auth-service/nest-cli.json notification-service/`) and in `tsconfig.build.json` keep `"exclude": ["node_modules", "dist", "**/*spec.ts", "test", "scripts"]`.

`notification-service/jest.config.js`:

```javascript
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  rootDir: '.',
  roots: ['<rootDir>/src'],
  testRegex: '.*\\.spec\\.ts$',
  setupFiles: ['<rootDir>/test/jest.env.ts'],
  testTimeout: 20000,
  // Specs share one RabbitMQ vhost and one Redis database; run files serially.
  maxWorkers: 1,
};
```

`notification-service/test/jest.env.ts`:

```typescript
import { config } from 'dotenv';
import { resolve } from 'path';

config({ path: resolve(__dirname, '../.env.test') });

process.env.AMQP_URL ??= 'amqp://chat:chat@localhost:5672/chat_test';
// db 2: notification-service tests never touch the db 1 used by chat-service/gateway specs.
process.env.REDIS_URL ??= 'redis://localhost:6379/2';
```

`notification-service/test/redis-cleanup.ts`:

```typescript
import Redis from 'ioredis';

/** Flushes the test Redis database. Refuses to run against db 0. */
export async function cleanRedis(redis: Redis): Promise<void> {
  if ((redis.options.db ?? 0) === 0) {
    throw new Error('Refusing to flush Redis db 0; tests must use REDIS_URL with /2');
  }
  await redis.flushdb();
}
```

`notification-service/src/app.module.ts` (placeholder, replaced in Task 6):

```typescript
import { Module } from '@nestjs/common';

@Module({})
export class AppModule {}
```

`notification-service/src/main.ts` (placeholder, replaced in Task 7):

```typescript
import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  app.enableShutdownHooks();
}
bootstrap();
```

- [ ] **Step 6: chat-service AMQP test helper `test/amqp.ts`**

```typescript
import * as amqp from 'amqplib';

export const EXCHANGE = 'chat.events';

export function amqpUrl(): string {
  return process.env.AMQP_URL!;
}

/** Deletes the exchange so each spec starts from a clean broker state. */
export async function resetExchange(): Promise<void> {
  const connection = await amqp.connect(amqpUrl());
  const channel = await connection.createChannel();
  await channel.deleteExchange(EXCHANGE).catch(() => undefined);
  await connection.close().catch(() => undefined);
}

export interface EventListener {
  messages: amqp.ConsumeMessage[];
  close(): Promise<void>;
}

/**
 * Binds an exclusive auto-delete queue to chat.events for `message.created` and records
 * everything published (a stand-in for "some consumer").
 */
export async function listenForEvents(): Promise<EventListener> {
  const connection = await amqp.connect(amqpUrl());
  const channel = await connection.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  const { queue } = await channel.assertQueue('', { exclusive: true, autoDelete: true });
  await channel.bindQueue(queue, EXCHANGE, 'message.created');
  const messages: amqp.ConsumeMessage[] = [];
  await channel.consume(
    queue,
    (msg) => {
      if (msg) {
        messages.push(msg);
        channel.ack(msg);
      }
    },
    { noAck: false },
  );
  return {
    messages,
    close: async () => {
      await connection.close().catch(() => undefined);
    },
  };
}
```

- [ ] **Step 7: Install, verify**

```bash
cd notification-service && npm install && npx tsc --noEmit && npx jest --passWithNoTests; cd ..
npx tsc --noEmit && npm test
```

Expected: both `tsc` clean; notification-service jest exits 0 (no tests); chat-service suite unchanged (68 tests) and green; `rabbitmqctl list_connections` shows none left open afterwards.

- [ ] **Step 8: Commit**

```bash
git add scripts docker-compose.yml tsconfig.json tsconfig.build.json test package.json package-lock.json notification-service
git commit -m "chore: add RabbitMQ test infra and scaffold notification-service"
```

(Do not stage `notification-service/node_modules` or any `.env*`.)

---

### Task 2: `loadConfig` and event schema validation (TDD)

**Files:**
- Create: `notification-service/src/config.ts`, `notification-service/src/config.spec.ts`, `notification-service/src/broker/event.schema.ts`, `notification-service/src/broker/event.schema.spec.ts`

**Interfaces:**
- Produces: `CONFIG` (Symbol); `NotificationConfig { amqpUrl; redisUrl; pushWebhookUrl; retryTtlMs: [number, number, number]; webhookTimeoutMs; processingTtlMs; doneTtlSec; reconnectDelayMs }`; `loadConfig(env?): NotificationConfig` (throws if `AMQP_URL`/`PUSH_WEBHOOK_URL` missing or `RETRY_TTL_MS` is not exactly three positive integers). `MessageCreatedEvent`; `parseEvent(content: Buffer): { ok: true; event } | { ok: false; reason: 'invalid-json' | 'invalid-schema' | 'unsupported-version' }`.

- [ ] **Step 1: Write the failing tests**

`notification-service/src/config.spec.ts`:

```typescript
import { loadConfig } from './config';

const base = { AMQP_URL: 'amqp://x', PUSH_WEBHOOK_URL: 'http://hook' };

describe('loadConfig', () => {
  it('applies defaults', () => {
    const c = loadConfig(base);

    expect(c).toMatchObject({
      amqpUrl: 'amqp://x',
      pushWebhookUrl: 'http://hook',
      redisUrl: 'redis://localhost:6379',
      retryTtlMs: [5000, 30000, 300000],
      webhookTimeoutMs: 3000,
      processingTtlMs: 60000,
      doneTtlSec: 604800,
      reconnectDelayMs: 1000,
    });
  });

  it('reads overrides from env', () => {
    const c = loadConfig({
      ...base,
      REDIS_URL: 'redis://r:1/2',
      RETRY_TTL_MS: '100, 200,300',
      WEBHOOK_TIMEOUT_MS: '500',
    });

    expect(c.redisUrl).toBe('redis://r:1/2');
    expect(c.retryTtlMs).toEqual([100, 200, 300]);
    expect(c.webhookTimeoutMs).toBe(500);
  });

  it.each([
    ['AMQP_URL', { PUSH_WEBHOOK_URL: 'http://hook' }],
    ['PUSH_WEBHOOK_URL', { AMQP_URL: 'amqp://x' }],
  ])('throws when %s is missing', (name, env) => {
    expect(() => loadConfig(env)).toThrow(new RegExp(name));
  });

  it.each(['1,2', '1,2,3,4', 'a,b,c', '1,0,3', '1,-2,3'])('rejects RETRY_TTL_MS=%s', (value) => {
    expect(() => loadConfig({ ...base, RETRY_TTL_MS: value })).toThrow(/RETRY_TTL_MS/);
  });
});
```

`notification-service/src/broker/event.schema.spec.ts`:

```typescript
import { parseEvent } from './event.schema';

const valid = {
  eventId: 'e1',
  type: 'message.created',
  version: 1,
  occurredAt: '2026-11-08T10:00:00.000Z',
  correlationId: 'c1',
  data: { messageId: 'm1', roomId: 'r1', senderId: 's1', preview: 'hi', recipientIds: ['u1', 'u2'] },
};

const buf = (o: unknown) => Buffer.from(JSON.stringify(o));

describe('parseEvent', () => {
  it('accepts a valid v1 event', () => {
    const r = parseEvent(buf(valid));
    expect(r).toEqual({ ok: true, event: valid });
  });

  it('accepts an empty recipient list', () => {
    const r = parseEvent(buf({ ...valid, data: { ...valid.data, recipientIds: [] } }));
    expect(r.ok).toBe(true);
  });

  it('rejects non-JSON', () => {
    expect(parseEvent(Buffer.from('not json'))).toEqual({ ok: false, reason: 'invalid-json' });
  });

  it('rejects an unsupported version', () => {
    expect(parseEvent(buf({ ...valid, version: 2 }))).toEqual({
      ok: false,
      reason: 'unsupported-version',
    });
  });

  it.each([
    ['wrong type', { ...valid, type: 'message.deleted' }],
    ['missing eventId', { ...valid, eventId: undefined }],
    ['empty eventId', { ...valid, eventId: '' }],
    ['missing correlationId', { ...valid, correlationId: undefined }],
    ['missing data', { ...valid, data: undefined }],
    ['missing recipientIds', { ...valid, data: { ...valid.data, recipientIds: undefined } }],
    ['non-string recipient', { ...valid, data: { ...valid.data, recipientIds: [1] } }],
    ['missing roomId', { ...valid, data: { ...valid.data, roomId: undefined } }],
    ['non-object payload', 'hello'],
    ['null payload', null],
  ])('rejects %s as invalid-schema', (_name, payload) => {
    expect(parseEvent(buf(payload))).toEqual({ ok: false, reason: 'invalid-schema' });
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `cd notification-service && npx jest src/config src/broker/event.schema`
Expected: FAIL — `Cannot find module './config'` / `'./event.schema'`.

- [ ] **Step 3: Implement**

`notification-service/src/config.ts`:

```typescript
export const CONFIG = Symbol('NOTIFICATION_CONFIG');

export interface NotificationConfig {
  amqpUrl: string;
  redisUrl: string;
  pushWebhookUrl: string;
  /** Fixed TTL of retry queues 1..3 (ms). */
  retryTtlMs: [number, number, number];
  webhookTimeoutMs: number;
  /** How long a `processing` idempotency claim survives a crashed worker (ms). */
  processingTtlMs: number;
  /** How long `done`/`rejected` idempotency marks are kept (s). */
  doneTtlSec: number;
  reconnectDelayMs: number;
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function positiveInt(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return n;
}

function parseRetryTtls(raw: string | undefined): [number, number, number] {
  if (!raw) return [5000, 30000, 300000];
  const parts = raw.split(',').map((p) => Number(p.trim()));
  if (parts.length !== 3 || parts.some((n) => !Number.isInteger(n) || n <= 0)) {
    throw new Error('RETRY_TTL_MS must be three comma-separated positive integers');
  }
  return parts as [number, number, number];
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): NotificationConfig {
  return {
    amqpUrl: required(env, 'AMQP_URL'),
    redisUrl: env.REDIS_URL ?? 'redis://localhost:6379',
    pushWebhookUrl: required(env, 'PUSH_WEBHOOK_URL'),
    retryTtlMs: parseRetryTtls(env.RETRY_TTL_MS),
    webhookTimeoutMs: positiveInt(env, 'WEBHOOK_TIMEOUT_MS', 3000),
    processingTtlMs: positiveInt(env, 'PROCESSING_TTL_MS', 60000),
    doneTtlSec: positiveInt(env, 'DONE_TTL_SEC', 604800),
    reconnectDelayMs: positiveInt(env, 'RECONNECT_DELAY_MS', 1000),
  };
}
```

`notification-service/src/broker/event.schema.ts`:

```typescript
export interface MessageCreatedEvent {
  eventId: string;
  type: 'message.created';
  version: 1;
  occurredAt: string;
  correlationId: string;
  data: {
    messageId: string;
    roomId: string;
    senderId: string;
    preview: string;
    recipientIds: string[];
  };
}

export type ParseResult =
  | { ok: true; event: MessageCreatedEvent }
  | { ok: false; reason: 'invalid-json' | 'invalid-schema' | 'unsupported-version' };

const isNonEmptyString = (v: unknown): v is string => typeof v === 'string' && v.length > 0;

export function parseEvent(content: Buffer): ParseResult {
  let raw: unknown;
  try {
    raw = JSON.parse(content.toString('utf8'));
  } catch {
    return { ok: false, reason: 'invalid-json' };
  }

  if (typeof raw !== 'object' || raw === null) {
    return { ok: false, reason: 'invalid-schema' };
  }
  const e = raw as Record<string, unknown>;

  if (e.type !== 'message.created') {
    return { ok: false, reason: 'invalid-schema' };
  }
  if (e.version !== 1) {
    return { ok: false, reason: typeof e.version === 'number' ? 'unsupported-version' : 'invalid-schema' };
  }

  const data = e.data as Record<string, unknown> | undefined;
  const valid =
    isNonEmptyString(e.eventId) &&
    typeof e.occurredAt === 'string' &&
    isNonEmptyString(e.correlationId) &&
    typeof data === 'object' &&
    data !== null &&
    isNonEmptyString(data.messageId) &&
    isNonEmptyString(data.roomId) &&
    isNonEmptyString(data.senderId) &&
    typeof data.preview === 'string' &&
    Array.isArray(data.recipientIds) &&
    data.recipientIds.every(isNonEmptyString);

  if (!valid) {
    return { ok: false, reason: 'invalid-schema' };
  }
  return { ok: true, event: raw as MessageCreatedEvent };
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cd notification-service && npx jest src/config src/broker/event.schema && npx tsc --noEmit`
Expected: PASS (config 11 tests, schema 13 tests); tsc clean.

- [ ] **Step 5: Commit**

```bash
git add notification-service/src
git commit -m "feat(notification-service): add config loading and message.created schema validation"
```

---

### Task 3: Topology and `BrokerService` (TDD, real RabbitMQ)

**Files:**
- Create: `notification-service/src/broker/topology.ts`, `topology.spec.ts`, `broker.service.ts`, `broker.service.spec.ts`, `notification-service/test/broker.ts`

**Interfaces:**
- Produces: constants `EXCHANGE='chat.events'`, `ROUTING_KEY='message.created'`, `WORK_QUEUE`, `DLQ`, `RETRY_QUEUES` (3 names); `assertTopology(ch, retryTtlMs)`. `BrokerService(config)` with `onReady(handler: (ch: ConfirmChannel) => Promise<void>)` (runs for the current and every future channel), `isReady(): boolean`, `restart(): Promise<void>` (drop the connection and reconnect), `onModuleInit()` (awaits the first connect attempt, never throws), `onApplicationShutdown()`. Test helpers (`test/broker.ts`): `resetBroker(url)`, `withChannel(url, fn)`, `queueCount(url, name)`.
- Reconnect: any connection/channel close or error while not shutting down reschedules `connect()` after `config.reconnectDelayMs`; topology is re-asserted and handlers re-run on every connect.

- [ ] **Step 1: Test helpers `notification-service/test/broker.ts`**

```typescript
import * as amqp from 'amqplib';

export const QUEUES = [
  'notifications.work',
  'notifications.retry.1',
  'notifications.retry.2',
  'notifications.retry.3',
  'notifications.dlq',
];

export async function withChannel<T>(
  url: string,
  fn: (ch: amqp.ConfirmChannel) => Promise<T>,
): Promise<T> {
  const connection = await amqp.connect(url);
  try {
    const ch = await connection.createConfirmChannel();
    return await fn(ch);
  } finally {
    await connection.close().catch(() => undefined);
  }
}

/** Deletes all queues and the exchange so topology is re-created from scratch. */
export async function resetBroker(url: string): Promise<void> {
  await withChannel(url, async (ch) => {
    for (const q of QUEUES) {
      await ch.deleteQueue(q).catch(() => undefined);
    }
  });
  // A failed deleteQueue closes its channel, so use a fresh one for the exchange.
  await withChannel(url, async (ch) => {
    await ch.deleteExchange('chat.events').catch(() => undefined);
  });
}

export async function queueCount(url: string, name: string): Promise<number> {
  return withChannel(url, async (ch) => (await ch.checkQueue(name)).messageCount);
}
```

(`deleteQueue` on a missing queue is a no-op in RabbitMQ 3.12, and the `.catch` covers brokers that error.)

- [ ] **Step 2: Write the failing tests**

`notification-service/src/broker/topology.spec.ts`:

```typescript
import * as amqp from 'amqplib';
import { assertTopology, DLQ, EXCHANGE, RETRY_QUEUES, ROUTING_KEY, WORK_QUEUE } from './topology';
import { resetBroker, withChannel } from '../../test/broker';

const url = process.env.AMQP_URL!;

describe('topology', () => {
  beforeEach(async () => {
    await resetBroker(url);
  });

  afterAll(async () => {
    await resetBroker(url);
  });

  it('declares every queue and the exchange, and is idempotent', async () => {
    await withChannel(url, async (ch) => {
      await assertTopology(ch, [200, 400, 600]);
      await assertTopology(ch, [200, 400, 600]);

      for (const q of [WORK_QUEUE, DLQ, ...RETRY_QUEUES]) {
        expect((await ch.checkQueue(q)).queue).toBe(q);
      }
      await ch.checkExchange(EXCHANGE);
    });
  });

  it('routes message.created into the work queue only', async () => {
    await withChannel(url, async (ch) => {
      await assertTopology(ch, [200, 400, 600]);
      ch.publish(EXCHANGE, ROUTING_KEY, Buffer.from('x'), { persistent: true });
      await ch.waitForConfirms();

      expect((await ch.checkQueue(WORK_QUEUE)).messageCount).toBe(1);
      expect((await ch.checkQueue(DLQ)).messageCount).toBe(0);
    });
  });

  it('dead-letters an expired retry message straight back into the work queue (no fan-out to other consumers)', async () => {
    await withChannel(url, async (ch) => {
      await assertTopology(ch, [200, 400, 600]);
      // A second consumer's queue bound to the same routing key (e.g. a future analytics-service).
      const { queue: other } = await ch.assertQueue('', { exclusive: true, autoDelete: true });
      await ch.bindQueue(other, EXCHANGE, ROUTING_KEY);

      ch.sendToQueue(RETRY_QUEUES[0], Buffer.from('retried'), {
        persistent: true,
        headers: { 'x-attempt': 1 },
      });
      await ch.waitForConfirms();
      await new Promise((r) => setTimeout(r, 450));

      const msg = await ch.get(WORK_QUEUE, { noAck: true });
      expect(msg).not.toBe(false);
      expect((msg as amqp.GetMessage).properties.headers?.['x-attempt']).toBe(1);
      expect((await ch.checkQueue(other)).messageCount).toBe(0);
      expect((await ch.checkQueue(RETRY_QUEUES[0])).messageCount).toBe(0);
    });
  });

  it('uses the configured TTL per retry tier', async () => {
    await withChannel(url, async (ch) => {
      await assertTopology(ch, [150, 1200, 1200]);
      ch.sendToQueue(RETRY_QUEUES[0], Buffer.from('fast'), { persistent: true });
      ch.sendToQueue(RETRY_QUEUES[1], Buffer.from('slow'), { persistent: true });
      await ch.waitForConfirms();
      await new Promise((r) => setTimeout(r, 400));

      expect((await ch.checkQueue(WORK_QUEUE)).messageCount).toBe(1); // only tier 1 expired
      expect((await ch.checkQueue(RETRY_QUEUES[1])).messageCount).toBe(1);
    });
  });
});
```

`notification-service/src/broker/broker.service.spec.ts`:

```typescript
import { execFileSync, spawnSync } from 'child_process';
import { loadConfig, NotificationConfig } from '../config';
import { BrokerService } from './broker.service';
import { WORK_QUEUE } from './topology';
import { queueCount, resetBroker } from '../../test/broker';

const url = process.env.AMQP_URL!;
const hasRabbitmqctl = spawnSync('rabbitmqctl', ['--version']).status === 0;

function config(overrides: Partial<NotificationConfig> = {}): NotificationConfig {
  return {
    ...loadConfig({ AMQP_URL: url, PUSH_WEBHOOK_URL: 'http://unused' }),
    retryTtlMs: [200, 400, 600],
    reconnectDelayMs: 200,
    ...overrides,
  };
}

async function waitFor(fn: () => boolean | Promise<boolean>, timeoutMs = 8000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error('waitFor timed out');
}

describe('BrokerService', () => {
  let broker: BrokerService | undefined;

  beforeEach(async () => {
    await resetBroker(url);
  });

  afterEach(async () => {
    await broker?.onApplicationShutdown();
    broker = undefined;
  });

  afterAll(async () => {
    await resetBroker(url);
  });

  it('connects, asserts the topology and runs ready handlers', async () => {
    broker = new BrokerService(config());
    let handled = 0;
    broker.onReady(async () => {
      handled++;
    });

    await broker.onModuleInit();

    expect(broker.isReady()).toBe(true);
    expect(handled).toBe(1);
    expect(await queueCount(url, WORK_QUEUE)).toBe(0); // queue exists
  });

  it('runs a handler registered after the connection is up', async () => {
    broker = new BrokerService(config());
    await broker.onModuleInit();
    let handled = 0;

    broker.onReady(async () => {
      handled++;
    });

    await waitFor(() => handled === 1);
  });

  it('does not throw when the broker is unreachable and reports not ready', async () => {
    broker = new BrokerService(config({ amqpUrl: 'amqp://chat:chat@localhost:5999/chat_test' }));

    await expect(broker.onModuleInit()).resolves.toBeUndefined();

    expect(broker.isReady()).toBe(false);
  });

  it('restart() drops the connection and reconnects, re-running handlers', async () => {
    broker = new BrokerService(config());
    let handled = 0;
    broker.onReady(async () => {
      handled++;
    });
    await broker.onModuleInit();

    await broker.restart();

    await waitFor(() => handled === 2 && broker!.isReady());
  });

  (hasRabbitmqctl ? it : it.skip)(
    'reconnects by itself after the broker closes all connections',
    async () => {
      broker = new BrokerService(config());
      let handled = 0;
      broker.onReady(async () => {
        handled++;
      });
      await broker.onModuleInit();

      execFileSync('rabbitmqctl', ['close_all_connections', 'test-reconnect']);

      await waitFor(() => handled >= 2 && broker!.isReady());
    },
  );

  it('stops reconnecting after shutdown', async () => {
    broker = new BrokerService(config());
    await broker.onModuleInit();

    await broker.onApplicationShutdown();

    expect(broker.isReady()).toBe(false);
    await new Promise((r) => setTimeout(r, 500));
    expect(broker.isReady()).toBe(false);
  });
});
```

- [ ] **Step 3: Run to verify failure**

Run: `cd notification-service && npx jest src/broker/topology src/broker/broker.service`
Expected: FAIL — `Cannot find module './topology'` / `'./broker.service'`.

- [ ] **Step 4: Implement**

`notification-service/src/broker/topology.ts`:

```typescript
import { Channel } from 'amqplib';

export const EXCHANGE = 'chat.events';
export const ROUTING_KEY = 'message.created';
export const WORK_QUEUE = 'notifications.work';
export const DLQ = 'notifications.dlq';
export const RETRY_QUEUES = [
  'notifications.retry.1',
  'notifications.retry.2',
  'notifications.retry.3',
] as const;

/**
 * Idempotent topology declaration. Retry queues have a fixed per-queue TTL and dead-letter
 * through the DEFAULT exchange straight back into the work queue, so other consumers bound
 * to chat.events never receive a retried duplicate.
 */
export async function assertTopology(ch: Channel, retryTtlMs: readonly number[]): Promise<void> {
  await ch.assertExchange(EXCHANGE, 'topic', { durable: true });
  await ch.assertQueue(WORK_QUEUE, { durable: true });
  await ch.bindQueue(WORK_QUEUE, EXCHANGE, ROUTING_KEY);
  await ch.assertQueue(DLQ, { durable: true });
  for (let i = 0; i < RETRY_QUEUES.length; i++) {
    await ch.assertQueue(RETRY_QUEUES[i], {
      durable: true,
      arguments: {
        'x-message-ttl': retryTtlMs[i],
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': WORK_QUEUE,
      },
    });
  }
}
```

`notification-service/src/broker/broker.service.ts`:

```typescript
import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { CONFIG, NotificationConfig } from '../config';
import { assertTopology } from './topology';

export type ReadyHandler = (channel: amqp.ConfirmChannel) => Promise<void>;

/**
 * Owns the AMQP connection and one confirm channel. Reconnects automatically and, on every
 * (re)connect, re-asserts the topology and re-runs every registered ready handler.
 */
@Injectable()
export class BrokerService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(BrokerService.name);
  private connection?: amqp.ChannelModel;
  private channel?: amqp.ConfirmChannel;
  private readonly handlers: ReadyHandler[] = [];
  private closing = false;
  private reconnectTimer?: NodeJS.Timeout;

  constructor(@Inject(CONFIG) private readonly config: NotificationConfig) {}

  onReady(handler: ReadyHandler): void {
    this.handlers.push(handler);
    if (this.channel) {
      const channel = this.channel;
      handler(channel).catch((err) => this.logger.error(`ready handler failed: ${err.message}`));
    }
  }

  isReady(): boolean {
    return this.channel !== undefined;
  }

  /** The first attempt is awaited; a failure schedules a retry instead of throwing. */
  async onModuleInit(): Promise<void> {
    await this.connect();
  }

  /** Drops the current connection; the close handler reconnects. */
  async restart(): Promise<void> {
    await this.connection?.close().catch(() => undefined);
  }

  async onApplicationShutdown(): Promise<void> {
    this.closing = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    const { channel, connection } = this;
    this.channel = undefined;
    this.connection = undefined;
    await channel?.close().catch(() => undefined);
    await connection?.close().catch(() => undefined);
  }

  private async connect(): Promise<void> {
    if (this.closing) return;
    let connection: amqp.ChannelModel | undefined;
    try {
      connection = await amqp.connect(this.config.amqpUrl);
      const conn = connection;
      conn.on('error', (err: Error) => this.logger.warn(`AMQP connection error: ${err.message}`));
      conn.on('close', () => this.onClosed(conn));

      const channel = await conn.createConfirmChannel();
      channel.on('error', (err: Error) => this.logger.warn(`AMQP channel error: ${err.message}`));
      channel.on('close', () => {
        // A closed channel (e.g. PRECONDITION_FAILED) is unrecoverable: rebuild everything.
        conn.close().catch(() => undefined);
      });

      await assertTopology(channel, this.config.retryTtlMs);
      this.connection = conn;
      this.channel = channel;
      for (const handler of this.handlers) {
        await handler(channel);
      }
      this.logger.log('AMQP ready');
    } catch (err) {
      this.logger.warn(`AMQP connect failed: ${(err as Error).message}`);
      if (connection) await connection.close().catch(() => undefined);
      this.channel = undefined;
      this.connection = undefined;
      this.scheduleReconnect();
    }
  }

  private onClosed(connection: amqp.ChannelModel): void {
    if (this.connection === connection) {
      this.connection = undefined;
      this.channel = undefined;
    }
    if (!this.closing) {
      this.logger.warn('AMQP connection closed; reconnecting');
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect(): void {
    if (this.closing || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connect();
    }, this.config.reconnectDelayMs);
  }
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cd notification-service && npx jest src/broker && npx tsc --noEmit`
Expected: PASS (topology 4, broker 6, schema 13); tsc clean. If `connection.close()` inside the channel `close` handler races with the connection `close` handler and produces an unhandled rejection or double reconnect, fix minimally and report it as a Ruling. Verify no leaked connections: `rabbitmqctl list_connections` is empty after the run.

- [ ] **Step 6: Commit**

```bash
git add notification-service/src/broker notification-service/test/broker.ts
git commit -m "feat(notification-service): add broker topology (TTL retry tiers + DLQ) and reconnecting BrokerService"
```

---

### Task 4: `IdempotencyService` (TDD, real Redis)

**Files:**
- Create: `notification-service/src/redis/redis.module.ts`, `notification-service/src/idempotency/idempotency.service.ts`, `notification-service/src/idempotency/idempotency.service.spec.ts`

**Interfaces:**
- Produces: `REDIS` token + `RedisModule` (global; builds the client from `CONFIG.redisUrl`; on shutdown `quit()` and waits for `end`). `IdempotencyService(redis, config)` with `claim(eventId, recipientId): Promise<'claimed' | 'done' | 'busy'>`, `complete(eventId, recipientId, status: 'done' | 'rejected'): Promise<void>`, `release(eventId, recipientId): Promise<void>`, `keyFor(eventId, recipientId)` (`notif:{eventId}:{recipientId}`), `stateOf(...)` for tests. `onModuleInit()` defines the Lua commands.

- [ ] **Step 1: Write the failing test**

`notification-service/src/idempotency/idempotency.service.spec.ts`:

```typescript
import Redis from 'ioredis';
import { loadConfig } from '../config';
import { IdempotencyService } from './idempotency.service';
import { cleanRedis } from '../../test/redis-cleanup';

describe('IdempotencyService', () => {
  let redis: Redis;
  let service: IdempotencyService;

  beforeAll(() => {
    redis = new Redis(process.env.REDIS_URL!);
    const config = {
      ...loadConfig({ AMQP_URL: 'x', PUSH_WEBHOOK_URL: 'y' }),
      processingTtlMs: 400,
    };
    service = new IdempotencyService(redis, config);
    service.onModuleInit();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await cleanRedis(redis);
  });

  const key = (e: string, r: string) => `notif:${e}:${r}`;

  it('the first claim wins and marks the key as processing', async () => {
    expect(await service.claim('e1', 'u1')).toBe('claimed');
    expect(await redis.get(key('e1', 'u1'))).toBe('processing');
  });

  it('a second claim while processing is busy', async () => {
    await service.claim('e1', 'u1');
    expect(await service.claim('e1', 'u1')).toBe('busy');
  });

  it('claims are independent per recipient and per event', async () => {
    expect(await service.claim('e1', 'u1')).toBe('claimed');
    expect(await service.claim('e1', 'u2')).toBe('claimed');
    expect(await service.claim('e2', 'u1')).toBe('claimed');
  });

  it('complete marks done (with a long TTL) and later claims report done', async () => {
    await service.claim('e1', 'u1');

    await service.complete('e1', 'u1', 'done');

    expect(await redis.get(key('e1', 'u1'))).toBe('done');
    expect(await redis.ttl(key('e1', 'u1'))).toBeGreaterThan(600000); // ~7 days (604800s)
    expect(await service.claim('e1', 'u1')).toBe('done');
  });

  it('a rejected mark also counts as done for later claims', async () => {
    await service.claim('e1', 'u1');
    await service.complete('e1', 'u1', 'rejected');

    expect(await redis.get(key('e1', 'u1'))).toBe('rejected');
    expect(await service.claim('e1', 'u1')).toBe('done');
  });

  it('release frees the key so the next attempt can claim it again (key is not burned)', async () => {
    await service.claim('e1', 'u1');

    await service.release('e1', 'u1');

    expect(await redis.get(key('e1', 'u1'))).toBeNull();
    expect(await service.claim('e1', 'u1')).toBe('claimed');
  });

  it('release never erases a completed mark', async () => {
    await service.claim('e1', 'u1');
    await service.complete('e1', 'u1', 'done');

    await service.release('e1', 'u1');

    expect(await redis.get(key('e1', 'u1'))).toBe('done');
  });

  it('complete does not downgrade an existing done/rejected mark', async () => {
    await service.claim('e1', 'u1');
    await service.complete('e1', 'u1', 'rejected');

    await service.complete('e1', 'u1', 'done');

    expect(await redis.get(key('e1', 'u1'))).toBe('rejected');
  });

  it('a crashed worker claim expires by itself, so a later claim succeeds', async () => {
    await service.claim('e1', 'u1'); // worker "crashes": never completes or releases
    expect(await service.claim('e1', 'u1')).toBe('busy');

    await new Promise((r) => setTimeout(r, 500));

    expect(await service.claim('e1', 'u1')).toBe('claimed');
  });

  it('complete still marks done if the processing claim expired mid-flight', async () => {
    await service.claim('e1', 'u1');
    await new Promise((r) => setTimeout(r, 500)); // claim expired while the webhook was running

    await service.complete('e1', 'u1', 'done');

    expect(await redis.get(key('e1', 'u1'))).toBe('done');
  });
});
```

- [ ] **Step 2: Run to verify failure**

Run: `cd notification-service && npx jest src/idempotency`
Expected: FAIL — `Cannot find module './idempotency.service'`.

- [ ] **Step 3: Implement**

`notification-service/src/redis/redis.module.ts`:

```typescript
import { Global, Inject, Module, OnApplicationShutdown } from '@nestjs/common';
import Redis from 'ioredis';
import { CONFIG, NotificationConfig } from '../config';

export const REDIS = Symbol('REDIS');

@Global()
@Module({
  providers: [
    {
      provide: REDIS,
      inject: [CONFIG],
      useFactory: (config: NotificationConfig): Redis =>
        new Redis(config.redisUrl, { maxRetriesPerRequest: 3 }),
    },
  ],
  exports: [REDIS],
})
export class RedisModule implements OnApplicationShutdown {
  constructor(@Inject(REDIS) private readonly redis: Redis) {}

  async onApplicationShutdown(): Promise<void> {
    if (this.redis.status === 'end') return;
    const ended = new Promise<void>((resolve) => this.redis.once('end', () => resolve()));
    await this.redis.quit();
    // quit() resolves on +OK, before the socket is actually closed.
    await ended;
  }
}
```

`notification-service/src/idempotency/idempotency.service.ts`:

```typescript
import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { CONFIG, NotificationConfig } from '../config';
import { REDIS } from '../redis/redis.module';

export type ClaimResult = 'claimed' | 'done' | 'busy';

interface IdempotencyRedis extends Redis {
  idemComplete(key: string, status: string, ttlSec: number): Promise<number>;
  idemRelease(key: string): Promise<number>;
}

/**
 * Idempotency for the webhook side-effect, per (eventId, recipient):
 *   claim -> side-effect -> complete          (success)
 *   claim -> side-effect fails -> release     (so the retry really re-sends)
 * `complete` is only ever called AFTER the side-effect succeeded.
 */
@Injectable()
export class IdempotencyService implements OnModuleInit {
  private readonly client: IdempotencyRedis;

  constructor(
    @Inject(REDIS) redis: Redis,
    @Inject(CONFIG) private readonly config: NotificationConfig,
  ) {
    this.client = redis as IdempotencyRedis;
  }

  onModuleInit(): void {
    // Never downgrade a final mark; a missing key (expired claim) is still completed.
    this.client.defineCommand('idemComplete', {
      numberOfKeys: 1,
      lua: `local v = redis.call('GET', KEYS[1])
if v == 'done' or v == 'rejected' then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1`,
    });
    // Only a still-processing claim may be released.
    this.client.defineCommand('idemRelease', {
      numberOfKeys: 1,
      lua: `if redis.call('GET', KEYS[1]) == 'processing' then redis.call('DEL', KEYS[1]) return 1 end
return 0`,
    });
  }

  keyFor(eventId: string, recipientId: string): string {
    return `notif:${eventId}:${recipientId}`;
  }

  async claim(eventId: string, recipientId: string): Promise<ClaimResult> {
    const key = this.keyFor(eventId, recipientId);
    const ok = await this.client.set(key, 'processing', 'PX', this.config.processingTtlMs, 'NX');
    if (ok === 'OK') return 'claimed';
    const state = await this.client.get(key);
    if (state === 'done' || state === 'rejected') return 'done';
    return 'busy';
  }

  async complete(eventId: string, recipientId: string, status: 'done' | 'rejected'): Promise<void> {
    await this.client.idemComplete(this.keyFor(eventId, recipientId), status, this.config.doneTtlSec);
  }

  async release(eventId: string, recipientId: string): Promise<void> {
    await this.client.idemRelease(this.keyFor(eventId, recipientId));
  }
}
```

- [ ] **Step 4: Run to verify pass**

Run: `cd notification-service && npx jest src/idempotency && npx tsc --noEmit`
Expected: PASS, 10 tests; tsc clean.

- [ ] **Step 5: Commit**

```bash
git add notification-service/src/redis notification-service/src/idempotency
git commit -m "feat(notification-service): add Redis idempotency (claim, complete-after-success, release)"
```

---

### Task 5: `WebhookClient` and the webhook sink (TDD)

**Files:**
- Create: `notification-service/src/delivery/webhook.client.ts`, `notification-service/src/delivery/webhook.client.spec.ts`, `notification-service/test/webhook-sink.ts`

**Interfaces:**
- Produces: `WebhookSink` test server — `startWebhookSink(): Promise<WebhookSink>` with `url`, `requests: RecordedRequest[]` (`{ at, headers, body, n }`), `setResponder(fn)` where `fn(req) => { status?: number; delayMs?: number; destroy?: boolean }`, `maxConcurrent()`, `close()`. `WebhookClient(config)` with `deliver(input: { event: MessageCreatedEvent; recipientId: string }): Promise<'ok' | 'retry' | 'rejected'>`; sends `POST config.pushWebhookUrl` JSON `{ eventId, type, recipientId, correlationId, messageId, roomId, senderId, preview }` with headers `Idempotency-Key: {eventId}:{recipientId}`, `X-Correlation-Id`, timeout `config.webhookTimeoutMs`. Outcomes: 2xx → `ok`; 429, ≥500, network error, timeout → `retry`; other non-2xx (4xx) → `rejected`.

- [ ] **Step 1: Create the sink `notification-service/test/webhook-sink.ts`**

```typescript
import { createServer, IncomingMessage, ServerResponse } from 'http';
import { AddressInfo } from 'net';

export interface RecordedRequest {
  n: number;
  at: number;
  headers: IncomingMessage['headers'];
  body: any;
}

export interface SinkResponse {
  status?: number;
  delayMs?: number;
  destroy?: boolean;
}

export interface WebhookSink {
  url: string;
  requests: RecordedRequest[];
  setResponder(fn: (req: RecordedRequest) => SinkResponse): void;
  maxConcurrent(): number;
  close(): Promise<void>;
}

/** A real HTTP server standing in for the external push provider. */
export async function startWebhookSink(): Promise<WebhookSink> {
  const requests: RecordedRequest[] = [];
  let responder: (req: RecordedRequest) => SinkResponse = () => ({ status: 200 });
  let active = 0;
  let max = 0;

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', async () => {
      active++;
      max = Math.max(max, active);
      let body: unknown = raw;
      try {
        body = JSON.parse(raw);
      } catch {
        /* keep raw */
      }
      const recorded: RecordedRequest = { n: requests.length + 1, at: Date.now(), headers: req.headers, body };
      requests.push(recorded);
      const response = responder(recorded);
      if (response.delayMs) await new Promise((r) => setTimeout(r, response.delayMs));
      active--;
      if (response.destroy) {
        req.socket.destroy();
        return;
      }
      res.statusCode = response.status ?? 200;
      res.end('{}');
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    url: `http://127.0.0.1:${port}/hook`,
    requests,
    setResponder: (fn) => {
      responder = fn;
    },
    maxConcurrent: () => max,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}
```

- [ ] **Step 2: Write the failing test**

`notification-service/src/delivery/webhook.client.spec.ts`:

```typescript
import { loadConfig } from '../config';
import { MessageCreatedEvent } from '../broker/event.schema';
import { WebhookClient } from './webhook.client';
import { startWebhookSink, WebhookSink } from '../../test/webhook-sink';

const event: MessageCreatedEvent = {
  eventId: 'e1',
  type: 'message.created',
  version: 1,
  occurredAt: '2026-11-08T10:00:00.000Z',
  correlationId: 'corr-1',
  data: { messageId: 'm1', roomId: 'r1', senderId: 's1', preview: 'hello', recipientIds: ['u1'] },
};

describe('WebhookClient', () => {
  let sink: WebhookSink;
  let client: WebhookClient;

  beforeEach(async () => {
    sink = await startWebhookSink();
    client = new WebhookClient({
      ...loadConfig({ AMQP_URL: 'x', PUSH_WEBHOOK_URL: sink.url }),
      webhookTimeoutMs: 300,
    });
  });

  afterEach(async () => {
    await sink.close();
  });

  it('POSTs the notification with idempotency and correlation headers', async () => {
    const result = await client.deliver({ event, recipientId: 'u1' });

    expect(result).toBe('ok');
    expect(sink.requests).toHaveLength(1);
    const req = sink.requests[0];
    expect(req.headers['idempotency-key']).toBe('e1:u1');
    expect(req.headers['x-correlation-id']).toBe('corr-1');
    expect(req.headers['content-type']).toContain('application/json');
    expect(req.body).toEqual({
      eventId: 'e1',
      type: 'message.created',
      recipientId: 'u1',
      correlationId: 'corr-1',
      messageId: 'm1',
      roomId: 'r1',
      senderId: 's1',
      preview: 'hello',
    });
  });

  it.each([
    [200, 'ok'],
    [204, 'ok'],
    [429, 'retry'],
    [500, 'retry'],
    [503, 'retry'],
    [400, 'rejected'],
    [404, 'rejected'],
    [422, 'rejected'],
  ])('maps HTTP %i to %s', async (status, expected) => {
    sink.setResponder(() => ({ status }));
    expect(await client.deliver({ event, recipientId: 'u1' })).toBe(expected);
  });

  it('treats a timeout as retry', async () => {
    sink.setResponder(() => ({ status: 200, delayMs: 800 }));
    expect(await client.deliver({ event, recipientId: 'u1' })).toBe('retry');
  });

  it('treats a dropped connection as retry', async () => {
    sink.setResponder(() => ({ destroy: true }));
    expect(await client.deliver({ event, recipientId: 'u1' })).toBe('retry');
  });

  it('treats an unreachable endpoint as retry', async () => {
    await sink.close();
    expect(await client.deliver({ event, recipientId: 'u1' })).toBe('retry');
    sink = await startWebhookSink(); // keep afterEach's close() valid
  });
});
```

- [ ] **Step 3: Run to verify failure**

Run: `cd notification-service && npx jest src/delivery`
Expected: FAIL — `Cannot find module './webhook.client'`.

- [ ] **Step 4: Implement**

`notification-service/src/delivery/webhook.client.ts`:

```typescript
import { Inject, Injectable, Logger } from '@nestjs/common';
import { CONFIG, NotificationConfig } from '../config';
import { MessageCreatedEvent } from '../broker/event.schema';

export type DeliveryOutcome = 'ok' | 'retry' | 'rejected';

@Injectable()
export class WebhookClient {
  private readonly logger = new Logger(WebhookClient.name);

  constructor(@Inject(CONFIG) private readonly config: NotificationConfig) {}

  async deliver(input: { event: MessageCreatedEvent; recipientId: string }): Promise<DeliveryOutcome> {
    const { event, recipientId } = input;
    try {
      const res = await fetch(this.config.pushWebhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `${event.eventId}:${recipientId}`,
          'X-Correlation-Id': event.correlationId,
        },
        body: JSON.stringify({
          eventId: event.eventId,
          type: event.type,
          recipientId,
          correlationId: event.correlationId,
          messageId: event.data.messageId,
          roomId: event.data.roomId,
          senderId: event.data.senderId,
          preview: event.data.preview,
        }),
        signal: AbortSignal.timeout(this.config.webhookTimeoutMs),
      });

      if (res.ok) return 'ok';
      if (res.status === 429 || res.status >= 500) return 'retry';
      return 'rejected';
    } catch (err) {
      this.logger.warn(
        `[${event.correlationId}] webhook call failed for ${recipientId}: ${(err as Error).message}`,
      );
      return 'retry';
    }
  }
}
```

- [ ] **Step 5: Run to verify pass**

Run: `cd notification-service && npx jest src/delivery && npx tsc --noEmit`
Expected: PASS, 13 tests; tsc clean.

- [ ] **Step 6: Commit**

```bash
git add notification-service/src/delivery notification-service/test/webhook-sink.ts
git commit -m "feat(notification-service): add webhook client and a real HTTP webhook sink for tests"
```

---

### Task 6: `NotificationConsumer`, app wiring and test harness (TDD)

**Files:**
- Create: `notification-service/src/consumer/notification.consumer.ts`, `notification-service/src/consumer/notification.consumer.spec.ts`, `notification-service/test/harness.ts`
- Modify: `notification-service/src/app.module.ts`

**Interfaces:**
- Produces: `NotificationConsumer(broker, idempotency, webhook, config)` — on every channel: `prefetch(1)`, consume `notifications.work`; per message: parse → per recipient claim/deliver/complete|release → retry (confirmed publish to `notifications.retry.{attempt+1}` with `x-attempt`, then ack) or DLQ (`x-reason`), `beforeApplicationShutdown()` cancels the consumer and awaits the in-flight message. `AppModule` wiring: `CONFIG` (from `loadConfig()`), `RedisModule`, `BrokerService`, `IdempotencyService`, `WebhookClient`, `NotificationConsumer`. `Harness` (`test/harness.ts`): `createHarness(overrides?)` → `{ sink, redis, config, publish(event), publishRaw(buf), counts(), dlqMessages(), closeApp(), close() }`; `makeEvent(overrides?)`.

- [ ] **Step 1: Wire `AppModule` with a global config module**

Create `notification-service/src/config.module.ts`:

```typescript
import { Global, Module } from '@nestjs/common';
import { CONFIG, loadConfig } from './config';

@Global()
@Module({
  providers: [{ provide: CONFIG, useFactory: () => loadConfig() }],
  exports: [CONFIG],
})
export class ConfigModule {}
```

(`RedisModule` from Task 4 injects `CONFIG` and needs no change; it resolves it from this global module. The harness overrides the single `CONFIG` provider.)

Replace `notification-service/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from './config.module';
import { RedisModule } from './redis/redis.module';
import { BrokerService } from './broker/broker.service';
import { IdempotencyService } from './idempotency/idempotency.service';
import { WebhookClient } from './delivery/webhook.client';
import { NotificationConsumer } from './consumer/notification.consumer';

@Module({
  imports: [ConfigModule, RedisModule],
  providers: [BrokerService, IdempotencyService, WebhookClient, NotificationConsumer],
})
export class AppModule {}
```

- [ ] **Step 2: Create the harness `notification-service/test/harness.ts`**

```typescript
import * as amqp from 'amqplib';
import { randomUUID } from 'crypto';
import Redis from 'ioredis';
import { Test, TestingModule } from '@nestjs/testing';
import { AppModule } from '../src/app.module';
import { CONFIG, loadConfig, NotificationConfig } from '../src/config';
import { MessageCreatedEvent } from '../src/broker/event.schema';
import { BrokerService } from '../src/broker/broker.service';
import { REDIS } from '../src/redis/redis.module';
import { cleanRedis } from './redis-cleanup';
import { resetBroker, withChannel } from './broker';
import { startWebhookSink, WebhookSink } from './webhook-sink';

export function makeEvent(overrides: Partial<MessageCreatedEvent['data']> & { eventId?: string; correlationId?: string } = {}): MessageCreatedEvent {
  return {
    eventId: overrides.eventId ?? randomUUID(),
    type: 'message.created',
    version: 1,
    occurredAt: new Date().toISOString(),
    correlationId: overrides.correlationId ?? 'corr-test',
    data: {
      messageId: overrides.messageId ?? randomUUID(),
      roomId: overrides.roomId ?? 'room-1',
      senderId: overrides.senderId ?? 'sender-1',
      preview: overrides.preview ?? 'hello',
      recipientIds: overrides.recipientIds ?? ['u1'],
    },
  };
}

export async function waitFor(fn: () => boolean | Promise<boolean>, timeoutMs = 8000, intervalMs = 25): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

export interface Harness {
  sink: WebhookSink;
  redis: Redis;
  config: NotificationConfig;
  moduleRef: TestingModule;
  publish(event: MessageCreatedEvent): Promise<void>;
  publishRaw(content: Buffer): Promise<void>;
  /** messageCount of every queue. */
  counts(): Promise<Record<string, number>>;
  dlqMessages(): Promise<amqp.GetMessage[]>;
  /** Shuts the app down (graceful) but keeps sink/redis for assertions. */
  closeApp(): Promise<void>;
  close(): Promise<void>;
}

export async function createHarness(overrides: Partial<NotificationConfig> = {}): Promise<Harness> {
  const url = process.env.AMQP_URL!;
  await resetBroker(url);
  const sink = await startWebhookSink();
  const config: NotificationConfig = {
    ...loadConfig({ AMQP_URL: url, REDIS_URL: process.env.REDIS_URL!, PUSH_WEBHOOK_URL: sink.url }),
    retryTtlMs: [200, 400, 600],
    webhookTimeoutMs: 1000,
    reconnectDelayMs: 200,
    ...overrides,
  };

  const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
    .overrideProvider(CONFIG)
    .useValue(config)
    .compile();
  const redis = moduleRef.get<Redis>(REDIS);
  await cleanRedis(redis);
  await moduleRef.init();

  const broker = moduleRef.get(BrokerService);
  if (config.amqpUrl === url) {
    await waitFor(() => broker.isReady());
  }

  let appClosed = false;
  const closeApp = async () => {
    if (appClosed) return;
    appClosed = true;
    await moduleRef.close();
  };

  return {
    sink,
    redis,
    config,
    moduleRef,
    publish: (event) => harnessPublish(url, Buffer.from(JSON.stringify(event))),
    publishRaw: (content) => harnessPublish(url, content),
    counts: async () =>
      withChannel(url, async (ch) => {
        const out: Record<string, number> = {};
        for (const q of ['notifications.work', 'notifications.retry.1', 'notifications.retry.2', 'notifications.retry.3', 'notifications.dlq']) {
          out[q] = (await ch.checkQueue(q)).messageCount;
        }
        return out;
      }),
    dlqMessages: async () =>
      withChannel(url, async (ch) => {
        const out: amqp.GetMessage[] = [];
        for (;;) {
          const m = await ch.get('notifications.dlq', { noAck: true });
          if (!m) break;
          out.push(m);
        }
        return out;
      }),
    closeApp,
    close: async () => {
      await closeApp();
      await sink.close();
      await resetBroker(url);
    },
  };
}

/** Publishes through a short-lived connection (robust against broker restarts in tests). */
async function harnessPublish(url: string, content: Buffer): Promise<void> {
  await withChannel(url, async (ch) => {
    ch.publish('chat.events', 'message.created', content, { persistent: true, contentType: 'application/json' });
    await ch.waitForConfirms();
  });
}
```

(The harness imports `Test` from `@nestjs/testing`, already in devDependencies.)

- [ ] **Step 3: Write the failing consumer spec**

`notification-service/src/consumer/notification.consumer.spec.ts`:

```typescript
import { createHarness, Harness, makeEvent, waitFor } from '../../test/harness';
import { SinkResponse } from '../../test/webhook-sink';

describe('NotificationConsumer', () => {
  let h: Harness;

  afterEach(async () => {
    await h.close();
  });

  const key = (e: string, r: string) => `notif:${e}:${r}`;
  const requestsFor = (recipient: string) =>
    h.sink.requests.filter((r) => r.body.recipientId === recipient);

  it('delivers exactly once to every recipient with idempotency and correlation headers', async () => {
    h = await createHarness();
    const event = makeEvent({ recipientIds: ['u1', 'u2'], correlationId: 'corr-42' });

    await h.publish(event);

    await waitFor(() => h.sink.requests.length === 2);
    expect(h.sink.requests.map((r) => r.body.recipientId).sort()).toEqual(['u1', 'u2']);
    expect(h.sink.requests[0].headers['idempotency-key']).toBe(`${event.eventId}:u1`);
    expect(h.sink.requests[0].headers['x-correlation-id']).toBe('corr-42');
    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u2'))) === 'done');
    expect(await h.redis.get(key(event.eventId, 'u1'))).toBe('done');
    const counts = await h.counts();
    expect(Object.values(counts).every((n) => n === 0)).toBe(true);
  });

  it('does not call the webhook again when the same event is redelivered', async () => {
    h = await createHarness();
    const event = makeEvent({ recipientIds: ['u1'] });
    await h.publish(event);
    await waitFor(() => h.sink.requests.length === 1);
    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u1'))) === 'done');

    await h.publish(event); // broker redelivery / duplicate publish with the same eventId
    await new Promise((r) => setTimeout(r, 500));

    expect(h.sink.requests).toHaveLength(1);
    expect(Object.values(await h.counts()).every((n) => n === 0)).toBe(true);
  });

  it('retries through the TTL tiers until the webhook recovers, then stops', async () => {
    h = await createHarness();
    h.sink.setResponder((req) => ({ status: req.n <= 2 ? 500 : 200 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(() => h.sink.requests.length === 3);
    const [a, b, c] = h.sink.requests;
    expect(b.at - a.at).toBeGreaterThanOrEqual(150); // tier 1 TTL = 200ms
    expect(c.at - b.at).toBeGreaterThanOrEqual(330); // tier 2 TTL = 400ms
    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u1'))) === 'done');
    await new Promise((r) => setTimeout(r, 900));
    expect(h.sink.requests).toHaveLength(3);
    expect(Object.values(await h.counts()).every((n) => n === 0)).toBe(true);
  });

  it('parks the event in the DLQ after the retry tiers are exhausted and releases the key', async () => {
    h = await createHarness();
    h.sink.setResponder(() => ({ status: 500 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(async () => (await h.counts())['notifications.dlq'] === 1, 10000);
    expect(h.sink.requests).toHaveLength(4); // first delivery + 3 retries
    const [parked] = await h.dlqMessages();
    expect(parked.properties.headers?.['x-reason']).toBe('retries-exhausted');
    expect(JSON.parse(parked.content.toString()).eventId).toBe(event.eventId);
    expect(await h.redis.get(key(event.eventId, 'u1'))).toBeNull(); // not burned
  });

  it('does not burn the idempotency key while a failed delivery waits in a retry queue', async () => {
    h = await createHarness({ retryTtlMs: [5000, 5000, 5000] });
    h.sink.setResponder(() => ({ status: 500 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(async () => (await h.counts())['notifications.retry.1'] === 1);
    expect(await h.redis.get(key(event.eventId, 'u1'))).toBeNull();
  });

  it('after a partial failure only the failed recipient is re-sent', async () => {
    h = await createHarness();
    let u2Calls = 0;
    h.sink.setResponder((req) => {
      if (req.body.recipientId === 'u2') {
        u2Calls++;
        return { status: u2Calls <= 2 ? 500 : 200 };
      }
      return { status: 200 };
    });
    const event = makeEvent({ recipientIds: ['u1', 'u2', 'u3'] });

    await h.publish(event);

    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u2'))) === 'done', 10000);
    expect(requestsFor('u1')).toHaveLength(1);
    expect(requestsFor('u3')).toHaveLength(1);
    expect(requestsFor('u2')).toHaveLength(3);
  });

  it('a busy claim (another worker in flight) goes to retry instead of calling the webhook', async () => {
    h = await createHarness();
    const event = makeEvent({ recipientIds: ['u1'] });
    await h.redis.set(key(event.eventId, 'u1'), 'processing', 'PX', 500);

    await h.publish(event);

    await new Promise((r) => setTimeout(r, 150));
    expect(h.sink.requests).toHaveLength(0);
    await waitFor(() => h.sink.requests.length === 1, 5000); // claim expired, retry delivers once
    expect(requestsFor('u1')).toHaveLength(1);
  });

  it('a permanent 4xx is rejected without retrying', async () => {
    h = await createHarness();
    h.sink.setResponder(() => ({ status: 400 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u1'))) === 'rejected');
    await new Promise((r) => setTimeout(r, 900));
    expect(h.sink.requests).toHaveLength(1);
    expect(Object.values(await h.counts()).every((n) => n === 0)).toBe(true);
  });

  it('429 is retried', async () => {
    h = await createHarness();
    h.sink.setResponder((req) => ({ status: req.n === 1 ? 429 : 200 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(() => h.sink.requests.length === 2);
    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u1'))) === 'done');
  });

  it('a hanging webhook is treated as a failure and retried', async () => {
    h = await createHarness({ webhookTimeoutMs: 150 });
    h.sink.setResponder((req) => (req.n === 1 ? { status: 200, delayMs: 700 } : { status: 200 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u1'))) === 'done', 8000);
    expect(h.sink.requests.length).toBeGreaterThanOrEqual(2);
  });

  it('sends malformed events straight to the DLQ without retrying or calling the webhook', async () => {
    h = await createHarness();

    await h.publishRaw(Buffer.from('not json'));
    await h.publishRaw(Buffer.from(JSON.stringify({ ...makeEvent(), version: 2 })));
    await h.publishRaw(Buffer.from(JSON.stringify({ type: 'message.created', version: 1 })));

    await waitFor(async () => (await h.counts())['notifications.dlq'] === 3);
    const reasons = (await h.dlqMessages()).map((m) => m.properties.headers?.['x-reason']).sort();
    expect(reasons).toEqual(['invalid-json', 'invalid-schema', 'unsupported-version']);
    expect(h.sink.requests).toHaveLength(0);
    const counts = await h.counts();
    expect(counts['notifications.retry.1']).toBe(0);
  });

  it('acknowledges an event with no recipients without calling the webhook', async () => {
    h = await createHarness();

    await h.publish(makeEvent({ recipientIds: [] }));

    await new Promise((r) => setTimeout(r, 400));
    expect(h.sink.requests).toHaveLength(0);
    expect(Object.values(await h.counts()).every((n) => n === 0)).toBe(true);
  });

  it('prefetch=1: handles events one at a time, in publish order', async () => {
    h = await createHarness();
    h.sink.setResponder(() => ({ status: 200, delayMs: 100 } as SinkResponse));
    const events = Array.from({ length: 5 }, (_, i) => makeEvent({ recipientIds: ['u1'], preview: `m${i}` }));

    for (const e of events) await h.publish(e);

    await waitFor(() => h.sink.requests.length === 5, 10000);
    expect(h.sink.maxConcurrent()).toBe(1);
    expect(h.sink.requests.map((r) => r.body.eventId)).toEqual(events.map((e) => e.eventId));
  });
});
```

- [ ] **Step 4: Run to verify failure**

Run: `cd notification-service && npx jest src/consumer/notification.consumer`
Expected: FAIL — `Cannot find module './notification.consumer'`.

- [ ] **Step 5: Implement the consumer**

`notification-service/src/consumer/notification.consumer.ts`:

```typescript
import { BeforeApplicationShutdown, Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { CONFIG, NotificationConfig } from '../config';
import { BrokerService } from '../broker/broker.service';
import { MessageCreatedEvent, parseEvent } from '../broker/event.schema';
import { DLQ, RETRY_QUEUES, WORK_QUEUE } from '../broker/topology';
import { IdempotencyService } from '../idempotency/idempotency.service';
import { WebhookClient } from '../delivery/webhook.client';

type RecipientOutcome = 'done' | 'retry';

@Injectable()
export class NotificationConsumer implements OnModuleInit, BeforeApplicationShutdown {
  private readonly logger = new Logger(NotificationConsumer.name);
  private current?: { channel: amqp.ConfirmChannel; tag: string };
  private inflight?: Promise<void>;

  constructor(
    private readonly broker: BrokerService,
    private readonly idempotency: IdempotencyService,
    private readonly webhook: WebhookClient,
    @Inject(CONFIG) private readonly config: NotificationConfig,
  ) {}

  onModuleInit(): void {
    this.broker.onReady((channel) => this.start(channel));
  }

  async beforeApplicationShutdown(): Promise<void> {
    if (this.current) {
      await this.current.channel.cancel(this.current.tag).catch(() => undefined);
      this.current = undefined;
    }
    await this.inflight?.catch(() => undefined);
  }

  private async start(channel: amqp.ConfirmChannel): Promise<void> {
    // One message at a time: deterministic order for first deliveries, no concurrent webhook calls.
    await channel.prefetch(1);
    const { consumerTag } = await channel.consume(WORK_QUEUE, (msg) => {
      if (!msg) {
        // The broker cancelled this consumer (e.g. the queue was deleted): rebuild everything.
        this.logger.warn('consumer cancelled by the broker; restarting the connection');
        void this.broker.restart();
        return;
      }
      this.inflight = this.handle(channel, msg).finally(() => {
        this.inflight = undefined;
      });
    });
    this.current = { channel, tag: consumerTag };
  }

  private async handle(channel: amqp.ConfirmChannel, msg: amqp.ConsumeMessage): Promise<void> {
    try {
      const parsed = parseEvent(msg.content);
      if (!parsed.ok) {
        this.logger.warn(`dropping unprocessable event to DLQ: ${parsed.reason}`);
        await this.publishConfirmed(channel, DLQ, msg, { 'x-reason': parsed.reason });
        channel.ack(msg);
        return;
      }

      const { event } = parsed;
      let needsRetry = false;
      for (const recipientId of event.data.recipientIds) {
        if ((await this.deliverTo(event, recipientId)) === 'retry') needsRetry = true;
      }

      if (needsRetry) {
        await this.retryOrPark(channel, msg, event);
      }
      channel.ack(msg);
    } catch (err) {
      // Unexpected failure (Redis/broker hiccup): give the broker the message back after a pause.
      this.logger.error(`unexpected failure, requeueing: ${(err as Error).message}`);
      await new Promise((r) => setTimeout(r, this.config.reconnectDelayMs));
      try {
        channel.nack(msg, false, true);
      } catch {
        /* channel already gone: the broker redelivers on its own */
      }
    }
  }

  private async deliverTo(event: MessageCreatedEvent, recipientId: string): Promise<RecipientOutcome> {
    const claim = await this.idempotency.claim(event.eventId, recipientId);
    if (claim === 'done') return 'done';
    if (claim === 'busy') {
      this.logger.log(`[${event.correlationId}] ${recipientId} is busy elsewhere; will retry`);
      return 'retry';
    }

    let outcome: Awaited<ReturnType<WebhookClient['deliver']>>;
    try {
      outcome = await this.webhook.deliver({ event, recipientId });
    } catch (err) {
      await this.idempotency.release(event.eventId, recipientId);
      throw err;
    }

    if (outcome === 'ok') {
      // Mark done only now that the side-effect really happened.
      await this.idempotency.complete(event.eventId, recipientId, 'done');
      this.logger.log(`[${event.correlationId}] notified ${recipientId}`);
      return 'done';
    }
    if (outcome === 'rejected') {
      await this.idempotency.complete(event.eventId, recipientId, 'rejected');
      this.logger.warn(`[${event.correlationId}] provider rejected ${recipientId}; not retrying`);
      return 'done';
    }

    await this.idempotency.release(event.eventId, recipientId); // never burn the key on failure
    return 'retry';
  }

  private async retryOrPark(
    channel: amqp.ConfirmChannel,
    msg: amqp.ConsumeMessage,
    event: MessageCreatedEvent,
  ): Promise<void> {
    const attempt = Number(msg.properties.headers?.['x-attempt'] ?? 0);
    if (attempt < RETRY_QUEUES.length) {
      this.logger.log(`[${event.correlationId}] retry tier ${attempt + 1} for ${event.eventId}`);
      await this.publishConfirmed(channel, RETRY_QUEUES[attempt], msg, { 'x-attempt': attempt + 1 });
    } else {
      this.logger.error(`[${event.correlationId}] parking ${event.eventId} in the DLQ`);
      await this.publishConfirmed(channel, DLQ, msg, { 'x-reason': 'retries-exhausted' });
    }
  }

  /** Publishes a copy of the message and waits for the broker confirm (before the caller acks). */
  private publishConfirmed(
    channel: amqp.ConfirmChannel,
    queue: string,
    msg: amqp.ConsumeMessage,
    extraHeaders: Record<string, unknown>,
  ): Promise<void> {
    const { 'x-death': _death, ...headers } = (msg.properties.headers ?? {}) as Record<string, unknown>;
    return new Promise<void>((resolve, reject) => {
      channel.sendToQueue(
        queue,
        msg.content,
        {
          persistent: true,
          contentType: 'application/json',
          headers: { ...headers, ...extraHeaders },
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });
  }
}
```

- [ ] **Step 6: Run to verify pass**

Run: `cd notification-service && npx jest src/consumer && npx tsc --noEmit`
Expected: PASS, 13 tests; tsc clean. Run the consumer spec 3 times to check timing flakiness (TTLs 200/400/600 ms; widen thresholds only if a real flake appears and record a Ruling). `rabbitmqctl list_connections` must be empty afterwards.

- [ ] **Step 7: Commit**

```bash
git add notification-service/src notification-service/test/harness.ts
git commit -m "feat(notification-service): add NotificationConsumer with idempotent delivery, TTL retry tiers and DLQ"
```

---

### Task 7: Bootstrap, graceful shutdown, reconnection and startup without a broker (TDD)

**Files:**
- Create: `notification-service/src/consumer/notification.lifecycle.spec.ts`
- Modify: `notification-service/src/main.ts`

**Interfaces:**
- Produces: production `main.ts` (application context, shutdown hooks, fatal-config errors exit non-zero); verified behaviours: graceful shutdown finishes the in-flight message before closing, consumer recovers after the broker drops all connections, the app starts (and keeps retrying) when the broker is down, a deleted work queue triggers a rebuild.
- Consumes: harness (Task 6), `BrokerService` (Task 3).

- [ ] **Step 1: Write the failing test**

`notification-service/src/consumer/notification.lifecycle.spec.ts`:

```typescript
import { execFileSync, spawnSync } from 'child_process';
import { createHarness, Harness, makeEvent, waitFor } from '../../test/harness';
import { BrokerService } from '../broker/broker.service';
import { withChannel } from '../../test/broker';

const hasRabbitmqctl = spawnSync('rabbitmqctl', ['--version']).status === 0;
const key = (e: string, r: string) => `notif:${e}:${r}`;

describe('notification-service lifecycle', () => {
  let h: Harness;

  afterEach(async () => {
    await h.close();
  });

  it('graceful shutdown finishes the in-flight message, acknowledges it and does not redeliver', async () => {
    h = await createHarness();
    h.sink.setResponder(() => ({ status: 200, delayMs: 500 }));
    const event = makeEvent({ recipientIds: ['u1'] });
    await h.publish(event);
    await waitFor(() => h.sink.requests.length === 1); // the handler is now blocked inside the webhook call

    await h.closeApp();

    expect(await h.redis.get(key(event.eventId, 'u1'))).toBe('done'); // finished before closing
    await new Promise((r) => setTimeout(r, 400));
    expect(h.sink.requests).toHaveLength(1); // not redelivered after shutdown
    const counts = await h.counts();
    expect(counts['notifications.work']).toBe(0);
  });

  (hasRabbitmqctl ? it : it.skip)(
    'recovers after the broker drops every connection and keeps delivering',
    async () => {
      h = await createHarness();
      const first = makeEvent({ recipientIds: ['u1'] });
      await h.publish(first);
      await waitFor(() => h.sink.requests.length === 1);

      execFileSync('rabbitmqctl', ['close_all_connections', 'lifecycle-test']);
      const broker = h.moduleRef.get(BrokerService);
      await waitFor(() => broker.isReady(), 10000);

      const second = makeEvent({ recipientIds: ['u2'] });
      await h.publish(second);
      await waitFor(() => h.sink.requests.length === 2, 10000);
      expect(h.sink.requests[1].body.recipientId).toBe('u2');
    },
  );

  it('rebuilds itself when the work queue is deleted under the consumer', async () => {
    h = await createHarness();
    await withChannel(process.env.AMQP_URL!, async (ch) => {
      await ch.deleteQueue('notifications.work');
    });
    const broker = h.moduleRef.get(BrokerService);

    await waitFor(() => broker.isReady(), 10000);
    const event = makeEvent({ recipientIds: ['u1'] });
    await h.publish(event);

    await waitFor(() => h.sink.requests.length === 1, 10000);
  });

  it('starts and shuts down cleanly while the broker is unreachable', async () => {
    h = await createHarness({ amqpUrl: 'amqp://chat:chat@localhost:5999/chat_test' });
    const broker = h.moduleRef.get(BrokerService);

    expect(broker.isReady()).toBe(false);
    await new Promise((r) => setTimeout(r, 500)); // a few reconnect attempts
    expect(broker.isReady()).toBe(false);

    await h.closeApp(); // must not hang or throw
  });
});
```

(The harness only waits for readiness when `amqpUrl` equals the real one, so the last test's app starts without a broker.)

- [ ] **Step 2: Run to verify behaviour**

Run: `cd notification-service && npx jest src/consumer/notification.lifecycle`
Expected: all four tests PASS if Tasks 3 and 6 are correct. If the graceful-shutdown test fails (key not `done`, or the message redelivered), the cause is the shutdown hook order — fix `NotificationConsumer.beforeApplicationShutdown` / `BrokerService.onApplicationShutdown` minimally and record a Ruling. If the queue-deleted test fails because the `msg === null` branch never fires, investigate amqplib's cancel notification handling and record the finding.

(There is no separate RED step here: these behaviours were designed into Tasks 3 and 6; this task proves them. If anything fails it is a real defect to fix in those files.)

- [ ] **Step 3: Finalise `main.ts`**

Replace `notification-service/src/main.ts`:

```typescript
import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  // SIGTERM/SIGINT: finish the in-flight message, then close the channel and Redis.
  app.enableShutdownHooks();
  new Logger('Bootstrap').log('notification-service started');
}

bootstrap().catch((err) => {
  new Logger('Bootstrap').error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
```

- [ ] **Step 4: Verify build and a real process**

```bash
cd notification-service && npx tsc --noEmit && npm run build
AMQP_URL=amqp://chat:chat@localhost:5672/chat_test PUSH_WEBHOOK_URL=http://127.0.0.1:9/hook REDIS_URL=redis://localhost:6379/2 node dist/main.js & PID=$!
sleep 4; rabbitmqctl list_consumers -p chat_test | grep notifications.work; kill -TERM $PID; wait $PID; echo "exit=$?"
rabbitmqctl delete_queue -p chat_test notifications.work >/dev/null 2>&1 || true
cd ..
```

Expected: tsc/build clean; `list_consumers` shows one consumer on `notifications.work`; after SIGTERM the process exits (code 143 is Nest's re-raised SIGTERM) with no stack trace. Also run `AMQP_URL=` unset: `node dist/main.js` exits non-zero with `AMQP_URL is required`.

- [ ] **Step 5: Run the whole notification-service suite twice**

Run: `cd notification-service && npx jest && npx jest`
Expected: all suites pass both times (config 11, schema 13, topology 4, broker 6, idempotency 10, webhook 13, consumer 13, lifecycle 4).

- [ ] **Step 6: Commit**

```bash
git add notification-service/src
git commit -m "feat(notification-service): graceful shutdown, reconnect and no-broker startup verified"
```

---

### Task 8: chat-service `EventPublisher` and event builder (TDD, real RabbitMQ)

**Files:**
- Create: `src/messaging/events.ts`, `src/messaging/events.spec.ts`, `src/messaging/correlation.ts`, `src/messaging/correlation.spec.ts`, `src/messaging/event.publisher.ts`, `src/messaging/event.publisher.spec.ts`, `src/messaging/messaging.module.ts`
- Modify: `src/app.module.ts`

**Interfaces:**
- Produces: `MessageCreatedEvent` + `buildMessageCreatedEvent({ message: {id, roomId, senderId, content}, recipientIds, correlationId })` (preview = first 100 code points, `eventId` = new UUID, `occurredAt` = now); `sanitizeCorrelationId(value: unknown): string | undefined` (accepts `[A-Za-z0-9._-]{1,128}`); `EventPublisher(options: MessagingOptions)` with `onModuleInit()` (awaits one connect attempt, never throws; no `amqpUrl` → no-op), `isConnected()`, `publishMessageCreated(event): Promise<void>` (**never rejects**; waits for the broker confirm up to `confirmTimeoutMs`; counts into `stats: { published, failed, skipped }`), `onApplicationShutdown()` (drains in-flight publishes then closes); `MESSAGING_OPTIONS` token; `MessagingModule` (`@Global`, options from env `AMQP_URL`, defaults `confirmTimeoutMs=2000`, `reconnectDelayMs=1000`). Exchange `chat.events` (topic, durable) asserted on every connect; messages `persistent`, `contentType application/json`, header `x-correlation-id`, `messageId = eventId`.

- [ ] **Step 1: Write the failing tests**

`src/messaging/events.spec.ts`:

```typescript
import { buildMessageCreatedEvent } from './events';

const message = { id: 'm1', roomId: 'r1', senderId: 's1', content: 'hello' };

describe('buildMessageCreatedEvent', () => {
  it('builds a v1 event', () => {
    const e = buildMessageCreatedEvent({ message, recipientIds: ['u1', 'u2'], correlationId: 'c1' });

    expect(e).toMatchObject({
      type: 'message.created',
      version: 1,
      correlationId: 'c1',
      data: { messageId: 'm1', roomId: 'r1', senderId: 's1', preview: 'hello', recipientIds: ['u1', 'u2'] },
    });
    expect(e.eventId).toMatch(/^[0-9a-f-]{36}$/);
    expect(new Date(e.occurredAt).toString()).not.toBe('Invalid Date');
  });

  it('generates a new eventId every time', () => {
    const a = buildMessageCreatedEvent({ message, recipientIds: ['u1'], correlationId: 'c' });
    const b = buildMessageCreatedEvent({ message, recipientIds: ['u1'], correlationId: 'c' });
    expect(a.eventId).not.toBe(b.eventId);
  });

  it('cuts the preview at 100 characters without splitting surrogate pairs', () => {
    const long = '😀'.repeat(150);
    const e = buildMessageCreatedEvent({
      message: { ...message, content: long },
      recipientIds: ['u1'],
      correlationId: 'c',
    });

    expect(Array.from(e.data.preview)).toHaveLength(100);
    expect(e.data.preview).toBe('😀'.repeat(100));
  });
});
```

`src/messaging/correlation.spec.ts`:

```typescript
import { sanitizeCorrelationId } from './correlation';

describe('sanitizeCorrelationId', () => {
  it.each(['abc', 'req-123.x_y', 'A'.repeat(128), '550e8400-e29b-41d4-a716-446655440000'])(
    'accepts %s',
    (v) => expect(sanitizeCorrelationId(v)).toBe(v),
  );

  it.each([undefined, null, 42, '', 'has space', 'semi;colon', 'new\nline', 'A'.repeat(129), ['a']])(
    'rejects %p',
    (v) => expect(sanitizeCorrelationId(v)).toBeUndefined(),
  );
});
```

`src/messaging/event.publisher.spec.ts`:

```typescript
import { execFileSync, spawnSync } from 'child_process';
import { EventPublisher } from './event.publisher';
import { buildMessageCreatedEvent } from './events';
import { amqpUrl, listenForEvents, resetExchange } from '../../test/amqp';
import { waitFor } from '../../test/helpers';

const hasRabbitmqctl = spawnSync('rabbitmqctl', ['--version']).status === 0;

function makeEvent(correlationId = 'corr-1') {
  return buildMessageCreatedEvent({
    message: { id: 'm1', roomId: 'r1', senderId: 's1', content: 'hello' },
    recipientIds: ['u1'],
    correlationId,
  });
}

describe('EventPublisher', () => {
  let publisher: EventPublisher | undefined;

  beforeEach(async () => {
    await resetExchange();
  });

  afterEach(async () => {
    await publisher?.onApplicationShutdown();
    publisher = undefined;
  });

  async function start(options: Partial<ConstructorParameters<typeof EventPublisher>[0]> = {}) {
    publisher = new EventPublisher({
      amqpUrl: amqpUrl(),
      confirmTimeoutMs: 1000,
      reconnectDelayMs: 200,
      ...options,
    });
    await publisher.onModuleInit();
    return publisher;
  }

  it('publishes a persistent message.created with correlation and message ids', async () => {
    const listener = await listenForEvents();
    const p = await start();
    const event = makeEvent('corr-77');

    await p.publishMessageCreated(event);

    await waitFor(() => listener.messages.length === 1);
    const msg = listener.messages[0];
    expect(JSON.parse(msg.content.toString())).toEqual(event);
    expect(msg.properties.deliveryMode).toBe(2); // persistent
    expect(msg.properties.contentType).toBe('application/json');
    expect(msg.properties.messageId).toBe(event.eventId);
    expect(msg.properties.headers?.['x-correlation-id']).toBe('corr-77');
    expect(msg.fields.routingKey).toBe('message.created');
    expect(p.stats).toMatchObject({ published: 1, failed: 0, skipped: 0 });
    await listener.close();
  });

  it('never rejects and counts a skip when the broker is unreachable', async () => {
    const p = await start({ amqpUrl: 'amqp://chat:chat@localhost:5999/chat_test' });

    await expect(p.publishMessageCreated(makeEvent())).resolves.toBeUndefined();

    expect(p.isConnected()).toBe(false);
    expect(p.stats.skipped).toBe(1);
    expect(p.stats.published).toBe(0);
  });

  it('is a silent no-op when AMQP_URL is not configured', async () => {
    const p = await start({ amqpUrl: undefined });

    await expect(p.publishMessageCreated(makeEvent())).resolves.toBeUndefined();

    expect(p.isConnected()).toBe(false);
    expect(p.stats.skipped).toBe(1);
  });

  (hasRabbitmqctl ? it : it.skip)('reconnects after the broker closes all connections', async () => {
    const listener = await listenForEvents();
    const p = await start();
    await p.publishMessageCreated(makeEvent('before'));
    await waitFor(() => listener.messages.length === 1);

    execFileSync('rabbitmqctl', ['close_all_connections', 'publisher-test']);
    await waitFor(() => !p.isConnected(), 5000).catch(() => undefined);
    await waitFor(() => p.isConnected(), 10000);
    const listener2 = await listenForEvents(); // the old listener's connection was closed too
    await p.publishMessageCreated(makeEvent('after'));

    await waitFor(() => listener2.messages.length === 1);
    expect(listener2.messages[0].properties.headers?.['x-correlation-id']).toBe('after');
    await listener2.close();
  });

  it('waits for in-flight publishes on shutdown', async () => {
    const listener = await listenForEvents();
    const p = await start();
    void p.publishMessageCreated(makeEvent('inflight')); // not awaited

    await p.onApplicationShutdown();

    await waitFor(() => listener.messages.length === 1);
    await listener.close();
  });
});
```

(`test/helpers.ts` already exports `waitFor` from Phase 3; make sure the amqp listener helper is created with the topology the publisher asserts: `listenForEvents()` asserts the exchange itself.)

- [ ] **Step 2: Run to verify failure**

Run: `npm test -- src/messaging`
Expected: FAIL — `Cannot find module './events'` / `'./correlation'` / `'./event.publisher'`.

- [ ] **Step 3: Implement**

`src/messaging/events.ts`:

```typescript
import { randomUUID } from 'crypto';

export interface MessageCreatedEvent {
  eventId: string;
  type: 'message.created';
  version: 1;
  occurredAt: string;
  correlationId: string;
  data: {
    messageId: string;
    roomId: string;
    senderId: string;
    preview: string;
    recipientIds: string[];
  };
}

const PREVIEW_CHARS = 100;

export function buildMessageCreatedEvent(input: {
  message: { id: string; roomId: string; senderId: string; content: string };
  recipientIds: string[];
  correlationId: string;
}): MessageCreatedEvent {
  const { message, recipientIds, correlationId } = input;
  return {
    eventId: randomUUID(),
    type: 'message.created',
    version: 1,
    occurredAt: new Date().toISOString(),
    correlationId,
    data: {
      messageId: message.id,
      roomId: message.roomId,
      senderId: message.senderId,
      // Code points, not UTF-16 units: never cut a surrogate pair in half.
      preview: Array.from(message.content).slice(0, PREVIEW_CHARS).join(''),
      recipientIds,
    },
  };
}
```

`src/messaging/correlation.ts`:

```typescript
const VALID = /^[A-Za-z0-9._-]{1,128}$/;

/** Returns the value if it is a safe correlation id, otherwise undefined. */
export function sanitizeCorrelationId(value: unknown): string | undefined {
  return typeof value === 'string' && VALID.test(value) ? value : undefined;
}
```

`src/messaging/event.publisher.ts`:

```typescript
import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { MessageCreatedEvent } from './events';

export const MESSAGING_OPTIONS = Symbol('MESSAGING_OPTIONS');

export interface MessagingOptions {
  /** Unset => the publisher is a no-op (e.g. local dev without RabbitMQ). */
  amqpUrl?: string;
  confirmTimeoutMs: number;
  reconnectDelayMs: number;
}

export const EXCHANGE = 'chat.events';
const ROUTING_KEY = 'message.created';

/**
 * Best-effort publisher: confirm channel, persistent messages, errors swallowed (logged and
 * counted) so a broker problem can never fail or delay a user's message send.
 * Known limitation: a crash between the DB commit and the publish loses the event.
 */
@Injectable()
export class EventPublisher implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(EventPublisher.name);
  readonly stats = { published: 0, failed: 0, skipped: 0 };
  private connection?: amqp.ChannelModel;
  private channel?: amqp.ConfirmChannel;
  private closing = false;
  private reconnectTimer?: NodeJS.Timeout;
  private readonly inflight = new Set<Promise<void>>();

  constructor(@Inject(MESSAGING_OPTIONS) private readonly options: MessagingOptions) {}

  async onModuleInit(): Promise<void> {
    if (!this.options.amqpUrl) {
      this.logger.warn('AMQP_URL is not set: event publishing is disabled');
      return;
    }
    await this.connect();
  }

  isConnected(): boolean {
    return this.channel !== undefined;
  }

  /** Never rejects. */
  publishMessageCreated(event: MessageCreatedEvent): Promise<void> {
    const promise = this.doPublish(event).finally(() => this.inflight.delete(promise));
    this.inflight.add(promise);
    return promise;
  }

  async onApplicationShutdown(): Promise<void> {
    this.closing = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    await Promise.allSettled([...this.inflight]);
    const { channel, connection } = this;
    this.channel = undefined;
    this.connection = undefined;
    await channel?.close().catch(() => undefined);
    await connection?.close().catch(() => undefined);
  }

  private async doPublish(event: MessageCreatedEvent): Promise<void> {
    const channel = this.channel;
    if (!channel) {
      this.stats.skipped++;
      if (this.options.amqpUrl) {
        this.logger.warn(`[${event.correlationId}] broker unavailable; skipped event ${event.eventId}`);
      }
      return;
    }

    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error(`no broker confirm within ${this.options.confirmTimeoutMs}ms`)),
          this.options.confirmTimeoutMs,
        );
        channel.publish(
          EXCHANGE,
          ROUTING_KEY,
          Buffer.from(JSON.stringify(event)),
          {
            persistent: true,
            contentType: 'application/json',
            messageId: event.eventId,
            headers: { 'x-correlation-id': event.correlationId },
          },
          (err) => {
            clearTimeout(timer);
            if (err) reject(err);
            else resolve();
          },
        );
      });
      this.stats.published++;
    } catch (err) {
      this.stats.failed++;
      this.logger.warn(
        `[${event.correlationId}] publish of ${event.eventId} failed: ${(err as Error).message}`,
      );
    }
  }

  private async connect(): Promise<void> {
    if (this.closing || !this.options.amqpUrl) return;
    let connection: amqp.ChannelModel | undefined;
    try {
      connection = await amqp.connect(this.options.amqpUrl);
      const conn = connection;
      conn.on('error', (err: Error) => this.logger.warn(`AMQP connection error: ${err.message}`));
      conn.on('close', () => this.onClosed(conn));
      const channel = await conn.createConfirmChannel();
      channel.on('error', (err: Error) => this.logger.warn(`AMQP channel error: ${err.message}`));
      channel.on('close', () => {
        conn.close().catch(() => undefined);
      });
      await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
      this.connection = conn;
      this.channel = channel;
      this.logger.log('AMQP publisher ready');
    } catch (err) {
      this.logger.warn(`AMQP connect failed: ${(err as Error).message}`);
      if (connection) await connection.close().catch(() => undefined);
      this.channel = undefined;
      this.connection = undefined;
      this.scheduleReconnect();
    }
  }

  private onClosed(connection: amqp.ChannelModel): void {
    if (this.connection === connection) {
      this.connection = undefined;
      this.channel = undefined;
    }
    if (!this.closing) this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.closing || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connect();
    }, this.options.reconnectDelayMs);
  }
}
```

`src/messaging/messaging.module.ts`:

```typescript
import { Global, Module } from '@nestjs/common';
import { EventPublisher, MESSAGING_OPTIONS, MessagingOptions } from './event.publisher';

@Global()
@Module({
  providers: [
    {
      provide: MESSAGING_OPTIONS,
      useFactory: (): MessagingOptions => ({
        amqpUrl: process.env.AMQP_URL || undefined,
        confirmTimeoutMs: 2000,
        reconnectDelayMs: 1000,
      }),
    },
    EventPublisher,
  ],
  exports: [EventPublisher],
})
export class MessagingModule {}
```

In `src/app.module.ts` add `MessagingModule` to `imports` (`import { MessagingModule } from './messaging/messaging.module';`).

- [ ] **Step 4: Run to verify pass**

Run: `npm test -- src/messaging && npx tsc --noEmit`
Expected: PASS (events 3, correlation 16, publisher 5 — the reconnect test runs only when `rabbitmqctl` exists); tsc clean; `rabbitmqctl list_connections` empty afterwards. Then the full suite: `npm test` (all green; `RoomsService` is not changed yet).

- [ ] **Step 5: Commit**

```bash
git add src/messaging src/app.module.ts
git commit -m "feat: add best-effort EventPublisher (confirm channel, persistent) and message.created builder"
```

---

### Task 9: `sendMessage` publishes `message.created`; `correlationId` through the controller (TDD)

**Files:**
- Modify: `src/rooms/rooms.service.ts`, `src/rooms/rooms.controller.ts`, `src/rooms/rooms.service.spec.ts`, `src/rooms/rooms.controller.spec.ts`, `src/rooms/internal.controller.spec.ts` (only if it constructs `RoomsService`), any other spec that builds `RoomsService` by hand (the compiler/test run reveals them)

**Interfaces:**
- Produces: `RoomsService.sendMessage(userId, roomId, dto, correlationId?: string): Promise<Message>` — after saving the message and the existing Redis publish, builds `message.created` (recipients = other members; none → no event; correlation id = argument or fresh UUID) and calls `EventPublisher.publishMessageCreated` **without awaiting** it; any failure while preparing the event is logged and never fails the send. `RoomsController.sendMessage` reads `X-Correlation-Id` via `sanitizeCorrelationId`. `RoomsService` now injects `EventPublisher` (provided globally by `MessagingModule`).
- Specs that construct `RoomsService` manually must import `MessagingModule` and call `await moduleRef.init()` (so the publisher connects) and `await moduleRef.close()` in `afterAll`.

- [ ] **Step 1: Write the failing tests**

Add to `src/rooms/rooms.service.spec.ts` (inside the existing top-level `describe`, using its existing `createUser`, `roomsService`, `prisma` helpers). First change the module setup to:

```typescript
import { MessagingModule } from '../messaging/messaging.module';
import { EventPublisher } from '../messaging/event.publisher';
import { listenForEvents, resetExchange, EventListener } from '../../test/amqp';
import { waitFor } from '../../test/helpers';
...
    const moduleRef = await Test.createTestingModule({
      imports: [MessagingModule],
      providers: [
        RoomsService,
        PrismaService,
        { provide: REDIS, useFactory: () => new Redis(process.env.REDIS_URL!) },
      ],
    }).compile();
    await moduleRef.init();
    publisher = moduleRef.get(EventPublisher);
```

(declare `let publisher: EventPublisher; let moduleRef: TestingModule;`, keep a reference to `moduleRef`, and in `afterAll` add `await moduleRef.close();` — closing the module also closes Prisma and the publisher.) Then add this block:

```typescript
  describe('message.created event', () => {
    let listener: EventListener;

    beforeEach(async () => {
      await resetExchange();
      await new Promise((r) => setTimeout(r, 50));
      listener = await listenForEvents();
    });

    afterEach(async () => {
      await listener.close();
    });

    it('publishes one event with the other members as recipients and the given correlation id', async () => {
      const sender = await createUser('ev-sender@example.com');
      const a = await createUser('ev-a@example.com');
      const b = await createUser('ev-b@example.com');
      const room = await roomsService.createRoom(sender.id, { name: 'Ev' });
      await roomsService.joinRoom(a.id, room.id);
      await roomsService.joinRoom(b.id, room.id);

      const message = await roomsService.sendMessage(
        sender.id,
        room.id,
        { content: 'hi everyone' },
        'corr-abc',
      );

      await waitFor(() => listener.messages.length === 1);
      const event = JSON.parse(listener.messages[0].content.toString());
      expect(event).toMatchObject({
        type: 'message.created',
        version: 1,
        correlationId: 'corr-abc',
        data: { messageId: message.id, roomId: room.id, senderId: sender.id, preview: 'hi everyone' },
      });
      expect(event.data.recipientIds.sort()).toEqual([a.id, b.id].sort());
      expect(event.data.recipientIds).not.toContain(sender.id);
    });

    it('generates a correlation id when none is given', async () => {
      const sender = await createUser('ev-gen@example.com');
      const other = await createUser('ev-gen2@example.com');
      const room = await roomsService.createRoom(sender.id, { name: 'Gen' });
      await roomsService.joinRoom(other.id, room.id);

      await roomsService.sendMessage(sender.id, room.id, { content: 'x' });

      await waitFor(() => listener.messages.length === 1);
      const event = JSON.parse(listener.messages[0].content.toString());
      expect(event.correlationId).toMatch(/^[0-9a-f-]{36}$/);
    });

    it('cuts a long message preview at 100 characters', async () => {
      const sender = await createUser('ev-long@example.com');
      const other = await createUser('ev-long2@example.com');
      const room = await roomsService.createRoom(sender.id, { name: 'Long' });
      await roomsService.joinRoom(other.id, room.id);

      await roomsService.sendMessage(sender.id, room.id, { content: 'a'.repeat(300) });

      await waitFor(() => listener.messages.length === 1);
      const event = JSON.parse(listener.messages[0].content.toString());
      expect(event.data.preview).toBe('a'.repeat(100));
    });

    it('publishes nothing when the sender is the only member', async () => {
      const sender = await createUser('ev-solo@example.com');
      const room = await roomsService.createRoom(sender.id, { name: 'Solo' });

      await roomsService.sendMessage(sender.id, room.id, { content: 'alone' });

      await new Promise((r) => setTimeout(r, 400));
      expect(listener.messages).toHaveLength(0);
    });

    it('does not publish when the sender is not a member (send is rejected)', async () => {
      const owner = await createUser('ev-owner@example.com');
      const outsider = await createUser('ev-out@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Closed' });

      await expect(
        roomsService.sendMessage(outsider.id, room.id, { content: 'nope' }),
      ).rejects.toThrow(ForbiddenException);

      await new Promise((r) => setTimeout(r, 300));
      expect(listener.messages).toHaveLength(0);
    });

    it('still saves and returns the message when the broker is unavailable', async () => {
      const sender = await createUser('ev-down@example.com');
      const other = await createUser('ev-down2@example.com');
      const room = await roomsService.createRoom(sender.id, { name: 'Down' });
      await roomsService.joinRoom(other.id, room.id);
      const before = { ...publisher.stats };
      // Simulate an unavailable broker: the publisher has no channel.
      (publisher as unknown as { channel: undefined }).channel = undefined;

      const message = await roomsService.sendMessage(sender.id, room.id, { content: 'offline' });

      expect(message.content).toBe('offline');
      expect(await prisma.message.findUnique({ where: { id: message.id } })).not.toBeNull();
      await waitFor(() => publisher.stats.skipped === before.skipped + 1);
    });
  });
```

(The last test deliberately detaches the publisher's channel — a real publisher whose broker is down has exactly this state — instead of mocking the publisher.) Add after it a test for resilience to a failed membership lookup is not needed: membership read is inside the same try/catch.

Add to `src/rooms/rooms.controller.spec.ts` (follow the file's existing request helpers and `createInstance`/module setup; import `listenForEvents`, `resetExchange`): one test that `POST /rooms/:roomId/messages` with header `X-Correlation-Id: ctrl-corr-1` (valid JWT of a member, another member in the room) results in an event whose `correlationId` is `ctrl-corr-1`, and one test that an invalid header (`bad value with spaces`) is replaced by a generated UUID.

Add a test to `src/rooms/internal.controller.spec.ts` only if it already exercises `POST /rooms/:id/messages` with the internal secret; otherwise rely on the controller test (the route is shared).

- [ ] **Step 2: Run to verify failure**

Run: `npm test -- rooms.service rooms.controller`
Expected: FAIL — `sendMessage` does not accept a 4th argument / no event published (TypeScript error or `waitFor timed out`).

- [ ] **Step 3: Implement**

In `src/rooms/rooms.service.ts`:

```typescript
import { randomUUID } from 'crypto';
import { Logger } from '@nestjs/common';
import { EventPublisher } from '../messaging/event.publisher';
import { buildMessageCreatedEvent } from '../messaging/events';
```

add a logger and the new dependency:

```typescript
export class RoomsService {
  private readonly logger = new Logger(RoomsService.name);

  constructor(
    private readonly prisma: PrismaService,
    @Inject(REDIS) private readonly redis: Redis,
    private readonly events: EventPublisher,
  ) {}
```

and replace `sendMessage`:

```typescript
  async sendMessage(
    userId: string,
    roomId: string,
    dto: SendMessageDto,
    correlationId?: string,
  ): Promise<Message> {
    await this.assertMember(userId, roomId);
    const message = await this.prisma.message.create({
      data: { roomId, senderId: userId, content: dto.content },
    });
    await this.redis.publish('chat:message:new', JSON.stringify({ roomId, message }));
    await this.announceMessageCreated(message, correlationId);
    return message;
  }

  /**
   * Best-effort `message.created` event for notification-service. Never fails the send and is
   * not awaited by the caller beyond preparing the event (the publish itself runs in the
   * background and swallows its own errors).
   */
  private async announceMessageCreated(message: Message, correlationId?: string): Promise<void> {
    try {
      const memberIds = await this.listMemberIds(message.roomId);
      const recipientIds = memberIds.filter((id) => id !== message.senderId);
      if (recipientIds.length === 0) return;

      const event = buildMessageCreatedEvent({
        message,
        recipientIds,
        correlationId: correlationId ?? randomUUID(),
      });
      void this.events.publishMessageCreated(event);
    } catch (err) {
      this.logger.warn(`could not prepare message.created for ${message.id}: ${(err as Error).message}`);
    }
  }
```

In `src/rooms/rooms.controller.ts`:

```typescript
import { Headers } from '@nestjs/common';
import { sanitizeCorrelationId } from '../messaging/correlation';
...
  @Post(':roomId/messages')
  @UseGuards(JwtOrInternalAuthGuard)
  sendMessage(
    @Req() req: { user: { id: string } },
    @Param('roomId') roomId: string,
    @Body() dto: SendMessageDto,
    @Headers('x-correlation-id') correlationId?: string,
  ) {
    return this.roomsService.sendMessage(req.user.id, roomId, dto, sanitizeCorrelationId(correlationId));
  }
```

- [ ] **Step 4: Fix every spec that builds `RoomsService` by hand**

Run: `npx tsc --noEmit` and `npm test`. For each spec that fails because `EventPublisher` cannot be resolved, add `imports: [MessagingModule]` to its testing module, `await moduleRef.init()` after `compile()` if it needs the publisher connected (otherwise the publisher is simply not connected and skips silently), and `await moduleRef.close()` in `afterAll` where it previously closed pieces individually. Do not change assertions.

- [ ] **Step 5: Run everything**

Run: `npx tsc --noEmit && npm test && npm test`
Expected: tsc clean; all suites pass twice (existing + Task 8's + this task's new tests; `rabbitmqctl list_connections` empty at the end). Check no `Connection is closed` unhandled rejections appear; if one does, close apps in the spec's `afterAll` before the module and record a Ruling.

- [ ] **Step 6: Commit**

```bash
git add src
git commit -m "feat: publish message.created from sendMessage with correlationId from X-Correlation-Id"
```

---

### Task 10: gateway forwards and generates `X-Correlation-Id` (TDD)

**Files:**
- Create: `gateway/src/common/correlation.ts`, `gateway/src/common/correlation.spec.ts`, `gateway/src/proxy/proxy.correlation.spec.ts`
- Modify: `gateway/src/proxy/proxy.controller.ts`, `gateway/src/realtime/internal-chat.client.ts`, `gateway/src/realtime/internal-chat.client.spec.ts`, `gateway/src/realtime/events.gateway.ts`

**Interfaces:**
- Produces: `resolveCorrelationId(value: unknown): string` (valid `[A-Za-z0-9._-]{1,128}` kept, otherwise a fresh UUID). `ProxyController` forwards `x-correlation-id` to the upstream service (auth-service or chat-service) and echoes it back in the response header `X-Correlation-Id`. `InternalChatClient.sendMessage(roomId, userId, content, correlationId?)` sends `x-correlation-id` when given. `EventsGateway` `message:send` generates one correlation id per send and passes it to `sendMessage`.

- [ ] **Step 1: Write the failing tests**

`gateway/src/common/correlation.spec.ts`:

```typescript
import { resolveCorrelationId } from './correlation';

describe('resolveCorrelationId', () => {
  it('keeps a valid id', () => {
    expect(resolveCorrelationId('req-1.a_b')).toBe('req-1.a_b');
  });

  it.each([undefined, null, '', 'has space', 'bad;char', 'A'.repeat(129), 42, ['x']])(
    'replaces %p with a fresh uuid',
    (v) => {
      expect(resolveCorrelationId(v)).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    },
  );

  it('generates a different id each time', () => {
    expect(resolveCorrelationId(undefined)).not.toBe(resolveCorrelationId(undefined));
  });
});
```

`gateway/src/proxy/proxy.correlation.spec.ts`:

```typescript
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { AddressInfo } from 'net';
import { createServer, Server as HttpServer } from 'http';
import { ProxyController } from './proxy.controller';

function startBackend(): Promise<{ url: string; server: HttpServer; seen: string[] }> {
  const seen: string[] = [];
  return new Promise((resolve) => {
    const server = createServer((req, res) => {
      seen.push(String(req.headers['x-correlation-id']));
      res.setHeader('Content-Type', 'application/json');
      res.end('{}');
    });
    server.listen(0, '127.0.0.1', () => {
      resolve({ url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, server, seen });
    });
  });
}

describe('ProxyController correlation id', () => {
  let app: INestApplication;
  let baseUrl: string;
  let auth: Awaited<ReturnType<typeof startBackend>>;
  let chat: Awaited<ReturnType<typeof startBackend>>;

  beforeAll(async () => {
    auth = await startBackend();
    chat = await startBackend();
    process.env.AUTH_SERVICE_URL = auth.url;
    process.env.CHAT_SERVICE_URL = chat.url;
    const moduleRef = await Test.createTestingModule({
      imports: [HttpModule],
      controllers: [ProxyController],
    }).compile();
    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
  });

  afterAll(async () => {
    await app.close();
    await new Promise((r) => auth.server.close(r));
    await new Promise((r) => chat.server.close(r));
  });

  it('forwards a client-supplied correlation id to chat-service and echoes it back', async () => {
    const res = await fetch(`${baseUrl}/rooms`, { headers: { 'X-Correlation-Id': 'client-corr-1' } });

    expect(chat.seen.at(-1)).toBe('client-corr-1');
    expect(res.headers.get('x-correlation-id')).toBe('client-corr-1');
  });

  it('forwards it to auth-service too', async () => {
    await fetch(`${baseUrl}/auth/me`, { headers: { 'X-Correlation-Id': 'client-corr-2' } });

    expect(auth.seen.at(-1)).toBe('client-corr-2');
  });

  it('generates one when the client sends none', async () => {
    const res = await fetch(`${baseUrl}/rooms`);

    const echoed = res.headers.get('x-correlation-id');
    expect(echoed).toMatch(/^[0-9a-f-]{36}$/);
    expect(chat.seen.at(-1)).toBe(echoed);
  });

  it('replaces an unsafe value with a generated one', async () => {
    const res = await fetch(`${baseUrl}/rooms`, { headers: { 'X-Correlation-Id': 'bad value; drop table' } });

    const echoed = res.headers.get('x-correlation-id');
    expect(echoed).toMatch(/^[0-9a-f-]{36}$/);
    expect(chat.seen.at(-1)).toBe(echoed);
  });
});
```

Add to `gateway/src/realtime/internal-chat.client.spec.ts` (alongside the existing capturing-server helpers): a test that `sendMessage(roomId, userId, 'hi', 'corr-9')` sends header `x-correlation-id: corr-9` (extend the capturing server to record `req.headers['x-correlation-id']`, or add a small dedicated capture server), and that omitting the argument sends no such header.

Add to `gateway/src/realtime/events.gateway.spec.ts` (using the existing chat stub): extend `test/chat-stub.ts` so the stub records the `x-correlation-id` header of `POST /rooms/:roomId/messages` (`lastCorrelationId()`), and add a test that a WS `message:send` reaches chat-service with a valid UUID correlation id, and that two sends carry different ids.

- [ ] **Step 2: Run to verify failure**

Run: `cd gateway && npx jest src/common src/proxy/proxy.correlation src/realtime/internal-chat.client src/realtime/events.gateway`
Expected: FAIL — `Cannot find module './correlation'`; header assertions fail.

- [ ] **Step 3: Implement**

`gateway/src/common/correlation.ts`:

```typescript
import { randomUUID } from 'crypto';

const VALID = /^[A-Za-z0-9._-]{1,128}$/;

/** Keeps a client-supplied correlation id if it is safe, otherwise generates a fresh one. */
export function resolveCorrelationId(value: unknown): string {
  return typeof value === 'string' && VALID.test(value) ? value : randomUUID();
}
```

In `gateway/src/proxy/proxy.controller.ts` update `forward`:

```typescript
import { resolveCorrelationId } from '../common/correlation';
...
  private async forward(req: Request, res: Response, target: string): Promise<void> {
    const correlationId = resolveCorrelationId(req.headers['x-correlation-id']);
    const headers: Record<string, string> = { 'x-correlation-id': correlationId };
    if (req.headers.authorization) {
      headers.authorization = req.headers.authorization;
    }
    const config: AxiosRequestConfig = {
      method: req.method,
      url: `${target}${req.originalUrl}`,
      data: req.body,
      headers,
      validateStatus: () => true,
    };
    const response = await firstValueFrom(this.http.request(config));
    res.setHeader('X-Correlation-Id', correlationId);
    res.status(response.status).json(response.data);
  }
```

(keep any existing error handling of `forward` unchanged; only the header handling is added.)

In `gateway/src/realtime/internal-chat.client.ts` extend `sendMessage`:

```typescript
  async sendMessage(
    roomId: string,
    userId: string,
    content: string,
    correlationId?: string,
  ): Promise<{ success: true } | { success: false; error: string }> {
    const res = await fetch(`${this.baseUrl}/rooms/${encodeURIComponent(roomId)}/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-internal-secret': this.secret,
        'x-internal-user-id': userId,
        ...(correlationId ? { 'x-correlation-id': correlationId } : {}),
      },
      ...
```

In `gateway/src/realtime/events.gateway.ts`: `import { randomUUID } from 'crypto';` and change the call at the `message:send` handler to `this.internalChatClient.sendMessage(body.roomId, client.data.userId, body.content, randomUUID())`.

- [ ] **Step 4: Run to verify pass**

Run: `cd gateway && npx jest && npx tsc --noEmit`
Expected: all gateway suites pass (previous 68 + new); tsc clean.

- [ ] **Step 5: Commit**

```bash
git add gateway
git commit -m "feat(gateway): forward and generate X-Correlation-Id (proxy and internal chat client)"
```

---

### Task 11: Smoke test and final verification

**Files:**
- Create: `scripts/smoke-sink.js`, `scripts/smoke-phase6.sh`
- Modify: `docs/superpowers/specs/2026-11-08-phase6-rabbitmq-notification-design.md` (tick proven criteria; note deviations), `docs/superpowers/plans/2026-11-08-phase6-rabbitmq-notification-ledger.md`

- [ ] **Step 1: Create `scripts/smoke-sink.js`**

```javascript
// Tiny webhook sink for the smoke test: records requests to a file, can be told to fail N times.
const http = require('http');
const fs = require('fs');

const out = process.argv[2];
const port = Number(process.argv[3]);
let failNext = 0;

http
  .createServer((req, res) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', () => {
      if (req.url.startsWith('/__fail')) {
        failNext = Number(new URL(req.url, 'http://x').searchParams.get('n') || 0);
        res.end('ok');
        return;
      }
      const status = failNext > 0 ? 500 : 200;
      if (failNext > 0) failNext--;
      fs.appendFileSync(
        out,
        JSON.stringify({ status, at: Date.now(), headers: req.headers, body: JSON.parse(raw || '{}') }) + '\n',
      );
      res.statusCode = status;
      res.end('{}');
    });
  })
  .listen(port, '127.0.0.1');
```

- [ ] **Step 2: Create `scripts/smoke-phase6.sh`**

```bash
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
rabbitmqctl close_all_connections smoke >/dev/null
sleep 4
BEFORE=$(count)
curl -s -o /dev/null -X POST "localhost:3020/rooms/$ROOM/messages" -H "Authorization: Bearer $TA" \
  -H 'Content-Type: application/json' -d '{"content":"after reconnect"}'
wait_count $((BEFORE + 1)) || true
check "delivery works after the broker dropped all connections" 1 "$(( $(count) - BEFORE ))"

grep -ciE "error|exception" "$TMP"/*.log | grep -v ":0" || echo "ok   no errors in service logs (see $TMP)"
exit $FAIL
```

- [ ] **Step 3: Full automated verification**

```bash
chmod +x scripts/smoke-phase6.sh
npx tsc --noEmit && (cd gateway && npx tsc --noEmit) && (cd auth-service && npx tsc --noEmit) && (cd notification-service && npx tsc --noEmit) && (cd web && npx tsc --noEmit)
npm run build && (cd gateway && npm run build) && (cd auth-service && npm run build) && (cd notification-service && npm run build)
npm test && npm test
(cd gateway && npx jest && npx jest)
(cd auth-service && npx jest)
(cd notification-service && npx jest && npx jest)
rabbitmqctl list_connections -q | wc -l
```

Expected: all type checks/builds clean; every suite passes twice where run twice; no RabbitMQ connections left open (`0`).

- [ ] **Step 4: Run the smoke test**

Run: `./scripts/smoke-phase6.sh`
Expected: every check prints `ok` and the exit code is 0. The retry step takes ~2 s (500 ms + 1 s tiers). If a check fails, fix the underlying defect (not the script) and record a Ruling; if the script itself is wrong, fix it minimally and record that.

- [ ] **Step 5: Update the spec and ledger**

In the spec: tick every completion criterion the tests and smoke run prove; for criteria not proven (if any), leave unticked and annotate why. In the ledger: mark all tasks `done`, and append Rulings recorded by the task reports.

- [ ] **Step 6: Commit**

```bash
git add scripts docs
git commit -m "docs: add Phase 6 smoke test and mark completion criteria"
```
