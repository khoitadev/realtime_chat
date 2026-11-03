# Phase 3 — Redis Adapter + Redis-Set Presence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Phase 2 Socket.IO monolith safe to run as multiple instances: Redis adapter for cross-instance broadcast, WS tickets stored in Redis, and presence tracked as a Redis **Set** of socketIds per user (multi-tab / multi-device correct, crash-safe via heartbeat keys).

**Architecture:** A global `RedisModule` provides one `ioredis` client (`REDIS`). `RedisIoAdapter` wires `@socket.io/redis-adapter` into Nest's Socket.IO server. `WsTicketService` moves to Redis (`SET EX` + atomic `GETDEL`). `PresenceService` keeps `presence:user:{userId}` (Set of socketIds) and `presence:socket:{socketId}` (heartbeat string with TTL) manipulated only through atomic Lua scripts; dead socketIds are pruned lazily on every read/write. `EventsGateway` calls `PresenceService` on connect/disconnect, refreshes heartbeats of its own sockets on a timer, broadcasts `presence:update` only on offline↔online transitions, and cleans its sockets on shutdown. A `PresenceController` exposes `GET /rooms/:roomId/presence`.

**Tech Stack:** `ioredis@^6`, `@socket.io/redis-adapter@^8.3`, existing NestJS 10 / Socket.IO 4.8 / Prisma / Jest stack. Real Postgres + real Redis in tests (no mocks).

**Spec:** `docs/superpowers/specs/2026-11-03-phase3-redis-scale-readiness-design.md`
**Ledger (Rulings):** `docs/superpowers/plans/2026-11-03-phase3-redis-scale-readiness-ledger.md`

## Global Constraints

- TDD with real I/O: real Postgres test DB, real Redis (database **1**), real `socket.io-client`. Never mock Prisma, Redis or sockets.
- **Never add a `Co-Authored-By` line (or any Claude attribution) to a git commit message.**
- Presence uses a **Set per user** — never a single string flag. Closing one of several tabs must not mark the user offline.
- `presence:update` is emitted only on offline→online and online→offline transitions, never per tab.
- Transport is `websocket`-only on both server (`@WebSocketGateway({ transports: ['websocket'] })`) and clients → no sticky sessions needed.
- WS ticket stays one-time, 30-second TTL; `GETDEL` makes consumption atomic across instances.
- All presence Redis mutations go through Lua scripts (atomic); keys are `presence:user:{userId}` and `presence:socket:{socketId}`. (Scripts build the second key name inside Lua — fine for a single Redis node; not Redis-Cluster-safe, which is out of scope.)
- Test runs are serial (`maxWorkers: 1`, already configured) because all specs share one Postgres DB and one Redis DB.
- Run all commands from the repo root. Infra for tests: `./scripts/test-infra.sh` (native) or `docker compose up -d postgres-test redis` (Docker).
- Out of scope: typing indicators, last-seen, Redis Cluster/Sentinel, streams adapter, broadcasting `online:false` for a crashed instance (reads are still correct after TTL).

---

## File Structure

```
src/redis/                              # NEW
├── redis.constants.ts                  # REDIS injection token
├── redis.module.ts                     # @Global, ioredis from REDIS_URL, quit on shutdown
├── redis.module.spec.ts
├── redis-io.adapter.ts                 # IoAdapter + @socket.io/redis-adapter
└── redis-io.adapter.spec.ts

src/realtime/
├── ws-ticket.service.ts                # MODIFY: Redis, async
├── ws-ticket.service.spec.ts           # MODIFY: Redis-backed tests
├── presence.service.ts                 # NEW
├── presence.service.spec.ts            # NEW
├── presence.controller.ts              # NEW: GET /rooms/:roomId/presence
├── presence.controller.spec.ts         # NEW
├── events.gateway.ts                   # MODIFY: auth async, presence hooks, heartbeat, shutdown
├── events.gateway.spec.ts              # MODIFY: async tickets, RedisModule, websocket transport
├── multi-instance.spec.ts              # NEW: 2 real app instances sharing Redis
├── presence.lifecycle.spec.ts          # NEW: heartbeat / crash / shutdown
└── realtime.module.ts                  # MODIFY

src/rooms/rooms.service.ts              # MODIFY: + listMemberIds, listRoomIdsOf
src/rooms/rooms.service.spec.ts         # MODIFY
src/auth/auth.controller.ts             # MODIFY: await issue()
src/auth/auth.controller.spec.ts        # MODIFY: import RedisModule
src/app.module.ts                       # MODIFY: import RedisModule
src/main.ts                             # MODIFY: RedisIoAdapter + shutdown hooks

test/
├── jest.env.ts                         # MODIFY: default REDIS_URL
├── redis-cleanup.ts                    # NEW
├── helpers.ts                          # NEW: connectSocket, emitWithAck, waitFor, sleep
└── instance.ts                         # NEW: boot a full AppModule instance on a random port

scripts/test-infra.sh                   # NEW: native Postgres + Redis (no Docker)
docker-compose.yml                      # MODIFY: + redis service
web/app/api/rooms/[roomId]/presence/route.ts   # NEW
web/app/chat/chat-client.tsx            # MODIFY: websocket-only, online list
package.json / package-lock.json        # MODIFY: ioredis, @socket.io/redis-adapter
```

---

### Task 1: Dependencies and test infrastructure

**Files:**
- Modify: `package.json`, `package-lock.json`, `docker-compose.yml`, `test/jest.env.ts`
- Create: `scripts/test-infra.sh`, `test/redis-cleanup.ts`, `test/helpers.ts`

**Interfaces:**
- Produces: `ioredis` + `@socket.io/redis-adapter` deps; `cleanRedis(redis: Redis): Promise<void>`; test helpers `connectSocket(url, ticket)`, `emitWithAck<T>(socket, event, payload)`, `waitFor(fn, timeoutMs?, intervalMs?)`, `sleep(ms)`; `./scripts/test-infra.sh`; `REDIS_URL` default `redis://localhost:6379/1` in tests.

- [ ] **Step 1: Install dependencies**

Run: `npm install ioredis @socket.io/redis-adapter`
Expected: both appear in `dependencies` of `package.json` (`ioredis ^6.0.0`, `@socket.io/redis-adapter ^8.3.0`).

- [ ] **Step 2: Create `scripts/test-infra.sh`**

```bash
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
for db in chat_dev chat_test; do
  su postgres -c "psql -p $PG_PORT -tAc \"SELECT 1 FROM pg_database WHERE datname='$db'\"" | grep -q 1 \
    || su postgres -c "createdb -p $PG_PORT $db"
done

if ! redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1; then
  redis-server --port "$REDIS_PORT" --daemonize yes >/dev/null
  for _ in $(seq 1 20); do redis-cli -p "$REDIS_PORT" ping >/dev/null 2>&1 && break; sleep 0.25; done
fi

if [ ! -f "$ROOT/.env.test" ]; then
  cat > "$ROOT/.env.test" <<ENV
DATABASE_URL="postgresql://postgres:postgres@localhost:${PG_PORT}/chat_test"
JWT_SECRET="test-secret"
REDIS_URL="redis://localhost:${REDIS_PORT}/1"
ENV
fi

echo "postgres:${PG_PORT} redis:${REDIS_PORT} ready"
```

Run: `chmod +x scripts/test-infra.sh && ./scripts/test-infra.sh && npm run prisma:migrate:test`
Expected: prints `postgres:5432 redis:6379 ready`; migrations applied. (With Docker instead: `docker compose up -d postgres-test redis`, `.env.test` with port 5433.)

- [ ] **Step 3: Add `redis` to `docker-compose.yml`**

Add under `services:` (same indentation as `postgres-test`):

```yaml
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

(Redis ≥ 6.2 is required for `GETDEL`.)

- [ ] **Step 4: Default `REDIS_URL` for tests**

Replace `test/jest.env.ts` with:

```typescript
import { config } from 'dotenv';
import { resolve } from 'path';

config({ path: resolve(__dirname, '../.env.test') });

// Tests always use Redis database 1 so they can never touch dev data in db 0.
process.env.REDIS_URL ??= 'redis://localhost:6379/1';
```

- [ ] **Step 5: Create `test/redis-cleanup.ts`**

```typescript
import Redis from 'ioredis';

/** Flushes the test Redis database. Refuses to run against db 0. */
export async function cleanRedis(redis: Redis): Promise<void> {
  if ((redis.options.db ?? 0) === 0) {
    throw new Error('Refusing to flush Redis db 0; tests must use REDIS_URL with /1');
  }
  await redis.flushdb();
}
```

- [ ] **Step 6: Create `test/helpers.ts`**

```typescript
import { io, Socket as ClientSocket } from 'socket.io-client';

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export function connectSocket(url: string, ticket: string): Promise<ClientSocket> {
  return new Promise((resolve, reject) => {
    const socket = io(url, {
      auth: { ticket },
      transports: ['websocket'],
      reconnection: false,
      forceNew: true,
    });
    socket.on('connect', () => resolve(socket));
    socket.on('connect_error', (err) => reject(err));
  });
}

export function emitWithAck<T>(socket: ClientSocket, event: string, payload?: unknown): Promise<T> {
  return new Promise((resolve) => socket.emit(event, payload, resolve));
}

/** Polls until fn() is truthy; throws on timeout. */
export async function waitFor(
  fn: () => boolean | Promise<boolean>,
  timeoutMs = 3000,
  intervalMs = 25,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return;
    await sleep(intervalMs);
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}
```

- [ ] **Step 7: Verify nothing regressed**

Run: `npx tsc --noEmit && npm test`
Expected: tsc clean; existing 39 tests pass.

- [ ] **Step 8: Commit**

```bash
git add package.json package-lock.json docker-compose.yml test scripts
git commit -m "chore: add ioredis + redis adapter deps and Redis/native test infra"
```

---

### Task 2: RedisModule and RedisIoAdapter (TDD)

**Files:**
- Create: `src/redis/redis.constants.ts`, `src/redis/redis.module.ts`, `src/redis/redis.module.spec.ts`, `src/redis/redis-io.adapter.ts`, `src/redis/redis-io.adapter.spec.ts`

**Interfaces:**
- Produces: `REDIS` (Symbol injection token for an `ioredis` client); `RedisModule` (`@Global`, reads `process.env.REDIS_URL`, default `redis://localhost:6379`, `quit()` on application shutdown); `new RedisIoAdapter(app: INestApplicationContext, redis: Redis)` with `createIOServer` + idempotent `dispose()` (Nest's `SocketModule.close()` calls `dispose()` automatically on `app.close()`).

- [ ] **Step 1: Write the failing tests**

Create `src/redis/redis.module.spec.ts`:

```typescript
import { Test } from '@nestjs/testing';
import Redis from 'ioredis';
import { RedisModule } from './redis.module';
import { REDIS } from './redis.constants';

describe('RedisModule', () => {
  it('provides a connected ioredis client', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [RedisModule] }).compile();
    const redis = moduleRef.get<Redis>(REDIS);

    expect(await redis.ping()).toBe('PONG');

    await moduleRef.close();
  });

  it('closes the connection on application shutdown', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [RedisModule] }).compile();
    const redis = moduleRef.get<Redis>(REDIS);
    await redis.ping();

    await moduleRef.close();

    expect(redis.status).toBe('end');
  });
});
```

Create `src/redis/redis-io.adapter.spec.ts`:

```typescript
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import {
  ConnectedSocket,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { Server, Socket } from 'socket.io';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { RedisModule } from './redis.module';
import { REDIS } from './redis.constants';
import { RedisIoAdapter } from './redis-io.adapter';
import { sleep } from '../../test/helpers';

@WebSocketGateway({ transports: ['websocket'] })
class EchoGateway {
  @WebSocketServer()
  server!: Server;

  @SubscribeMessage('join')
  async join(@ConnectedSocket() client: Socket) {
    await client.join('room');
    return { ok: true };
  }

  @SubscribeMessage('say')
  say() {
    this.server.to('room').emit('said', 'hello');
    return { ok: true };
  }
}

async function boot(useAdapter: boolean) {
  const moduleRef = await Test.createTestingModule({
    imports: [RedisModule],
    providers: [EchoGateway],
  }).compile();
  const app = moduleRef.createNestApplication();
  if (useAdapter) {
    app.useWebSocketAdapter(new RedisIoAdapter(app, moduleRef.get<Redis>(REDIS)));
  }
  await app.listen(0);
  const port = (app.getHttpServer().address() as AddressInfo).port;
  return { app, url: `http://localhost:${port}` };
}

function connect(url: string): Promise<ClientSocket> {
  return new Promise((resolve, reject) => {
    const socket = io(url, { transports: ['websocket'], forceNew: true, reconnection: false });
    socket.on('connect', () => resolve(socket));
    socket.on('connect_error', reject);
  });
}

describe('RedisIoAdapter', () => {
  const apps: INestApplication[] = [];
  const clients: ClientSocket[] = [];

  afterEach(async () => {
    clients.splice(0).forEach((c) => c.disconnect());
    await Promise.all(apps.splice(0).map((a) => a.close()));
  });

  it('delivers a room broadcast emitted on instance A to a client on instance B', async () => {
    const a = await boot(true);
    const b = await boot(true);
    apps.push(a.app, b.app);
    const ca = await connect(a.url);
    const cb = await connect(b.url);
    clients.push(ca, cb);
    await ca.emitWithAck('join');
    await cb.emitWithAck('join');

    const received = new Promise<string>((resolve) => cb.once('said', resolve));
    await ca.emitWithAck('say');

    expect(await received).toBe('hello');
  });

  it('control: without the adapter a broadcast does NOT cross instances', async () => {
    const a = await boot(false);
    const b = await boot(false);
    apps.push(a.app, b.app);
    const ca = await connect(a.url);
    const cb = await connect(b.url);
    clients.push(ca, cb);
    await ca.emitWithAck('join');
    await cb.emitWithAck('join');

    let receivedOnB = false;
    cb.on('said', () => {
      receivedOnB = true;
    });
    await ca.emitWithAck('say');
    await sleep(300);

    expect(receivedOnB).toBe(false);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test -- src/redis`
Expected: FAIL — `Cannot find module './redis.module'` / `'./redis-io.adapter'`.

- [ ] **Step 3: Implement**

Create `src/redis/redis.constants.ts`:

```typescript
export const REDIS = Symbol('REDIS');
```

Create `src/redis/redis.module.ts`:

```typescript
import { Global, Inject, Module, OnApplicationShutdown } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS } from './redis.constants';

@Global()
@Module({
  providers: [
    {
      provide: REDIS,
      useFactory: (): Redis =>
        new Redis(process.env.REDIS_URL ?? 'redis://localhost:6379', {
          maxRetriesPerRequest: 3,
        }),
    },
  ],
  exports: [REDIS],
})
export class RedisModule implements OnApplicationShutdown {
  constructor(@Inject(REDIS) private readonly redis: Redis) {}

  async onApplicationShutdown(): Promise<void> {
    await this.redis.quit();
  }
}
```

Create `src/redis/redis-io.adapter.ts`:

```typescript
import { INestApplicationContext } from '@nestjs/common';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { createAdapter } from '@socket.io/redis-adapter';
import Redis from 'ioredis';
import { Server, ServerOptions } from 'socket.io';

/**
 * Socket.IO adapter backed by Redis pub/sub so that `server.to(room).emit()`
 * reaches sockets connected to any instance. pub/sub need their own
 * connections (a subscriber-mode connection cannot run normal commands).
 */
export class RedisIoAdapter extends IoAdapter {
  private readonly clients: Redis[] = [];

  constructor(
    app: INestApplicationContext,
    private readonly redis: Redis,
  ) {
    super(app);
  }

  createIOServer(port: number, options?: ServerOptions): Server {
    const server = super.createIOServer(port, options) as Server;
    const pub = this.redis.duplicate();
    const sub = this.redis.duplicate();
    this.clients.push(pub, sub);
    server.adapter(createAdapter(pub, sub));
    return server;
  }

  /** Called by Nest's SocketModule.close() after the servers are closed. */
  async dispose(): Promise<void> {
    await Promise.all(this.clients.splice(0).map((c) => c.quit().catch(() => undefined)));
  }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test -- src/redis`
Expected: PASS, 4 tests (including the control test proving the adapter is what makes broadcasts cross instances).

- [ ] **Step 5: Commit**

```bash
git add src/redis
git commit -m "feat: add RedisModule and RedisIoAdapter (multi-instance Socket.IO)"
```

---

### Task 3: WsTicketService on Redis (TDD) + wiring

**Files:**
- Modify: `src/realtime/ws-ticket.service.ts`, `src/realtime/ws-ticket.service.spec.ts`, `src/realtime/events.gateway.ts`, `src/realtime/events.gateway.spec.ts`, `src/auth/auth.controller.ts`, `src/auth/auth.controller.spec.ts`, `src/app.module.ts`

**Interfaces:**
- Produces: `new WsTicketService(redis)`; `issue(userId: string, ttlSec = 30): Promise<string>` (`SET ws:ticket:{ticket} userId EX ttl`); `consume(ticket: string): Promise<string | null>` (`GETDEL`, atomic and one-time across instances). `@WebSocketGateway({ cors: true, transports: ['websocket'] })`. `AppModule` imports `RedisModule`.
- Consumes: `REDIS`, `RedisModule` (Task 2); `cleanRedis`, `sleep` (Task 1).

- [ ] **Step 1: Rewrite the failing test**

Replace `src/realtime/ws-ticket.service.spec.ts` with:

```typescript
import Redis from 'ioredis';
import { WsTicketService } from './ws-ticket.service';
import { cleanRedis } from '../../test/redis-cleanup';
import { sleep } from '../../test/helpers';

describe('WsTicketService (Redis)', () => {
  let redis: Redis;
  let service: WsTicketService;

  beforeAll(() => {
    redis = new Redis(process.env.REDIS_URL!);
    service = new WsTicketService(redis);
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await cleanRedis(redis);
  });

  it('issues a different ticket each time', async () => {
    const a = await service.issue('user-1');
    const b = await service.issue('user-1');
    expect(a).not.toBe(b);
  });

  it('consumes a freshly issued ticket and returns the userId', async () => {
    const ticket = await service.issue('user-1');
    expect(await service.consume(ticket)).toBe('user-1');
  });

  it('returns null when consuming the same ticket twice (one-time use)', async () => {
    const ticket = await service.issue('user-1');
    await service.consume(ticket);
    expect(await service.consume(ticket)).toBeNull();
  });

  it('returns null for an unknown ticket', async () => {
    expect(await service.consume('does-not-exist')).toBeNull();
  });

  it('returns null for an expired ticket (Redis TTL)', async () => {
    const ticket = await service.issue('user-1', 1);
    await sleep(1200);
    expect(await service.consume(ticket)).toBeNull();
  });

  it('sets a 30 second TTL by default', async () => {
    const ticket = await service.issue('user-1');
    const ttl = await redis.ttl(`ws:ticket:${ticket}`);
    expect(ttl).toBeGreaterThan(25);
    expect(ttl).toBeLessThanOrEqual(30);
  });

  it('lets exactly one of two concurrent consumers win', async () => {
    const ticket = await service.issue('user-1');
    const results = await Promise.all([service.consume(ticket), service.consume(ticket)]);
    expect(results.filter((r) => r === 'user-1')).toHaveLength(1);
    expect(results.filter((r) => r === null)).toHaveLength(1);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- ws-ticket.service`
Expected: FAIL — TypeScript error: `Expected 0 arguments, but got 1` (constructor takes no Redis yet).

- [ ] **Step 3: Implement the service**

Replace `src/realtime/ws-ticket.service.ts` with:

```typescript
import { Inject, Injectable } from '@nestjs/common';
import { randomBytes } from 'crypto';
import Redis from 'ioredis';
import { REDIS } from '../redis/redis.constants';

export const WS_TICKET_TTL_SEC = 30;

@Injectable()
export class WsTicketService {
  constructor(@Inject(REDIS) private readonly redis: Redis) {}

  async issue(userId: string, ttlSec = WS_TICKET_TTL_SEC): Promise<string> {
    const ticket = randomBytes(24).toString('hex');
    await this.redis.set(`ws:ticket:${ticket}`, userId, 'EX', ttlSec);
    return ticket;
  }

  /** One-time: GETDEL reads and deletes atomically, so only one caller can win. */
  async consume(ticket: string): Promise<string | null> {
    return this.redis.getdel(`ws:ticket:${ticket}`);
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- ws-ticket.service`
Expected: PASS, 7 tests.

- [ ] **Step 5: Update the callers (the type checker will list them)**

Run: `npx tsc --noEmit`
Expected: errors in `events.gateway.ts`, `events.gateway.spec.ts`, `auth.controller.ts`. Fix as follows.

`src/auth/auth.controller.ts` — make the handler async:

```typescript
  @Post('ws-ticket')
  @UseGuards(JwtAuthGuard)
  async wsTicket(@Req() req: { user: { id: string } }) {
    return { ticket: await this.wsTicketService.issue(req.user.id), expiresIn: 30 };
  }
```

`src/realtime/events.gateway.ts` — set websocket-only transport and make the auth middleware async. Replace the decorator line and `afterInit`:

```typescript
@Injectable()
@WebSocketGateway({ cors: true, transports: ['websocket'] })
export class EventsGateway implements OnGatewayInit {
  @WebSocketServer()
  server!: Server;

  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly roomsService: RoomsService,
  ) {}

  afterInit(server: Server): void {
    server.use(async (socket: Socket, next: (err?: Error) => void) => {
      try {
        const ticket = socket.handshake.auth?.ticket as string | undefined;
        const userId = ticket ? await this.wsTicketService.consume(ticket) : null;

        if (!userId) {
          next(new Error('Unauthorized'));
          return;
        }

        socket.data.userId = userId;
        next();
      } catch {
        next(new Error('Unauthorized'));
      }
    });
  }
```

(leave the three `@SubscribeMessage` handlers unchanged).

`src/app.module.ts` — import the Redis module:

```typescript
import { RedisModule } from './redis/redis.module';
...
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    RedisModule,
    PrismaModule,
    AuthModule,
    RoomsModule,
    RealtimeModule,
  ],
```

`src/auth/auth.controller.spec.ts` — add `RedisModule` to the testing module `imports` array (`import { RedisModule } from '../redis/redis.module';`).

`src/realtime/events.gateway.spec.ts`:
1. Add `import { RedisModule } from '../redis/redis.module';` and change the testing module to `imports: [RedisModule], providers: [EventsGateway, WsTicketService, RoomsService, PrismaService]`.
2. Make every ticket issue awaited: run `sed -i 's/wsTicketService\.issue(/await wsTicketService.issue(/g' src/realtime/events.gateway.spec.ts`.
3. In `connectWithTicket`, add `transports: ['websocket']` to the `io(...)` options (server is websocket-only now):

```typescript
      const socket = io(baseUrl, {
        auth: { ticket },
        transports: ['websocket'],
        reconnection: false,
        forceNew: true,
      });
```

- [ ] **Step 6: Run the full suite**

Run: `npx tsc --noEmit && npm test`
Expected: tsc clean; all suites pass (Phase 1/2 behaviour unchanged, now over Redis tickets).

- [ ] **Step 7: Commit**

```bash
git add src
git commit -m "feat: store WS tickets in Redis (atomic GETDEL) and make WS transport websocket-only"
```

---

### Task 4: PresenceService (TDD)

**Files:**
- Create: `src/realtime/presence.service.ts`, `src/realtime/presence.service.spec.ts`

**Interfaces:**
- Produces: `PRESENCE_OPTIONS` (Symbol), `PresenceOptions { heartbeatTtlSec: number; refreshIntervalMs: number }`, `DEFAULT_PRESENCE_OPTIONS = { heartbeatTtlSec: 60, refreshIntervalMs: 20_000 }`; `PresenceService` with `onModuleInit()` (registers Lua commands), `connect(userId, socketId): Promise<boolean>` (true = offline→online), `disconnect(userId, socketId): Promise<boolean>` (true = online→offline; false if the socket was not in the Set), `touch(userId, socketId): Promise<void>`, `isOnline(userId): Promise<boolean>`, `onlineAmong(userIds: string[]): Promise<string[]>`. Consumed by `EventsGateway` (Task 6/7) and `PresenceController` (Task 8).
- Consumes: `REDIS` (Task 2), `cleanRedis`, `sleep` (Task 1).

- [ ] **Step 1: Write the failing test**

Create `src/realtime/presence.service.spec.ts`:

```typescript
import Redis from 'ioredis';
import { PresenceService } from './presence.service';
import { cleanRedis } from '../../test/redis-cleanup';
import { sleep } from '../../test/helpers';

describe('PresenceService (Redis Set)', () => {
  let redis: Redis;
  let service: PresenceService;

  beforeAll(() => {
    redis = new Redis(process.env.REDIS_URL!);
    service = new PresenceService(redis, { heartbeatTtlSec: 1, refreshIntervalMs: 1000 });
    service.onModuleInit();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await cleanRedis(redis);
  });

  it('reports offline for a user nobody connected', async () => {
    expect(await service.isOnline('u1')).toBe(false);
  });

  it('first connect is an offline->online transition, second connect is not', async () => {
    expect(await service.connect('u1', 'sock-a')).toBe(true);
    expect(await service.connect('u1', 'sock-b')).toBe(false);
    expect(await redis.scard('presence:user:u1')).toBe(2);
  });

  it('keeps the user online after closing one of two tabs (the single-string bug)', async () => {
    await service.connect('u1', 'sock-a');
    await service.connect('u1', 'sock-b');

    expect(await service.disconnect('u1', 'sock-a')).toBe(false);

    expect(await service.isOnline('u1')).toBe(true);
  });

  it('goes offline only when the last socket disconnects', async () => {
    await service.connect('u1', 'sock-a');
    await service.connect('u1', 'sock-b');
    await service.disconnect('u1', 'sock-a');

    expect(await service.disconnect('u1', 'sock-b')).toBe(true);

    expect(await service.isOnline('u1')).toBe(false);
    expect(await redis.exists('presence:socket:sock-b')).toBe(0);
  });

  it('disconnecting an unknown or already-removed socket is not a transition', async () => {
    await service.connect('u1', 'sock-a');
    await service.disconnect('u1', 'sock-a');

    expect(await service.disconnect('u1', 'sock-a')).toBe(false);
    expect(await service.disconnect('never-seen', 'sock-x')).toBe(false);
  });

  it('tracks users independently', async () => {
    await service.connect('u1', 'sock-a');
    await service.connect('u2', 'sock-b');
    await service.disconnect('u1', 'sock-a');

    expect(await service.isOnline('u1')).toBe(false);
    expect(await service.isOnline('u2')).toBe(true);
  });

  it('ignores an orphaned socketId (no heartbeat key) so the user is not "online" forever', async () => {
    await redis.sadd('presence:user:u1', 'ghost-socket'); // instance crashed, key never removed

    expect(await service.isOnline('u1')).toBe(false);
    expect(await redis.scard('presence:user:u1')).toBe(0); // lazily pruned
  });

  it('treats a user with only orphaned sockets as offline when a new socket connects', async () => {
    await redis.sadd('presence:user:u1', 'ghost-socket');

    expect(await service.connect('u1', 'sock-a')).toBe(true);
    expect(await redis.smembers('presence:user:u1')).toEqual(['sock-a']);
  });

  it('expires the heartbeat so a silent socket stops counting as online', async () => {
    await service.connect('u1', 'sock-a');
    await sleep(1200); // heartbeat TTL is 1s and nobody refreshes it

    expect(await service.isOnline('u1')).toBe(false);
  });

  it('touch keeps a socket alive past the original TTL', async () => {
    await service.connect('u1', 'sock-a');
    await sleep(700);
    await service.touch('u1', 'sock-a');
    await sleep(700); // 1.4s since connect, but only 0.7s since touch

    expect(await service.isOnline('u1')).toBe(true);
  });

  it('touch re-registers a socket whose heartbeat expired but is still connected', async () => {
    await service.connect('u1', 'sock-a');
    await sleep(1200);
    await service.touch('u1', 'sock-a');

    expect(await service.isOnline('u1')).toBe(true);
  });

  it('onlineAmong returns only online users and handles an empty list', async () => {
    await service.connect('u1', 'sock-a');
    await service.connect('u3', 'sock-c');

    expect((await service.onlineAmong(['u1', 'u2', 'u3'])).sort()).toEqual(['u1', 'u3']);
    expect(await service.onlineAmong([])).toEqual([]);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- presence.service`
Expected: FAIL — `Cannot find module './presence.service'`.

- [ ] **Step 3: Implement**

Create `src/realtime/presence.service.ts`:

```typescript
import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS } from '../redis/redis.constants';

export const PRESENCE_OPTIONS = Symbol('PRESENCE_OPTIONS');

export interface PresenceOptions {
  /** TTL of the per-socket heartbeat key, in seconds. */
  heartbeatTtlSec: number;
  /** How often an instance refreshes heartbeats of its own sockets. */
  refreshIntervalMs: number;
}

export const DEFAULT_PRESENCE_OPTIONS: PresenceOptions = {
  heartbeatTtlSec: 60,
  refreshIntervalMs: 20_000,
};

// Shared Lua helper: drop socketIds whose heartbeat key is gone (orphans left
// by a crashed instance), then return the remaining set size.
const PRUNE_LUA = `
local function prune(userKey)
  local members = redis.call('SMEMBERS', userKey)
  for _, id in ipairs(members) do
    if redis.call('EXISTS', 'presence:socket:' .. id) == 0 then
      redis.call('SREM', userKey, id)
    end
  end
  return redis.call('SCARD', userKey)
end
`;

interface PresenceRedis extends Redis {
  presenceConnect(userKey: string, socketId: string, userId: string, ttl: number): Promise<number>;
  presenceDisconnect(userKey: string, socketId: string): Promise<number>;
  presenceTouch(userKey: string, socketId: string, userId: string, ttl: number): Promise<number>;
  presenceIsOnline(userKey: string): Promise<number>;
}

const userKey = (userId: string) => `presence:user:${userId}`;

@Injectable()
export class PresenceService implements OnModuleInit {
  private readonly client: PresenceRedis;

  constructor(
    @Inject(REDIS) redis: Redis,
    @Inject(PRESENCE_OPTIONS) private readonly options: PresenceOptions,
  ) {
    this.client = redis as PresenceRedis;
  }

  onModuleInit(): void {
    // Re-defining the same command on the shared client is idempotent.
    this.client.defineCommand('presenceConnect', {
      numberOfKeys: 1,
      lua: `${PRUNE_LUA}
local before = prune(KEYS[1])
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('SET', 'presence:socket:' .. ARGV[1], ARGV[2], 'EX', ARGV[3])
if before == 0 then return 1 else return 0 end`,
    });
    this.client.defineCommand('presenceDisconnect', {
      numberOfKeys: 1,
      lua: `${PRUNE_LUA}
local removed = redis.call('SREM', KEYS[1], ARGV[1])
redis.call('DEL', 'presence:socket:' .. ARGV[1])
local left = prune(KEYS[1])
if removed == 1 and left == 0 then return 1 else return 0 end`,
    });
    this.client.defineCommand('presenceTouch', {
      numberOfKeys: 1,
      lua: `redis.call('SADD', KEYS[1], ARGV[1])
redis.call('SET', 'presence:socket:' .. ARGV[1], ARGV[2], 'EX', ARGV[3])
return 1`,
    });
    this.client.defineCommand('presenceIsOnline', {
      numberOfKeys: 1,
      lua: `${PRUNE_LUA}
if prune(KEYS[1]) > 0 then return 1 else return 0 end`,
    });
  }

  /** Returns true when this is the user's first live socket (offline -> online). */
  async connect(userId: string, socketId: string): Promise<boolean> {
    const r = await this.client.presenceConnect(
      userKey(userId),
      socketId,
      userId,
      this.options.heartbeatTtlSec,
    );
    return r === 1;
  }

  /** Returns true when the user has no live socket left (online -> offline). */
  async disconnect(userId: string, socketId: string): Promise<boolean> {
    const r = await this.client.presenceDisconnect(userKey(userId), socketId);
    return r === 1;
  }

  /** Heartbeat: keeps the socket alive and self-heals an expired heartbeat. */
  async touch(userId: string, socketId: string): Promise<void> {
    await this.client.presenceTouch(
      userKey(userId),
      socketId,
      userId,
      this.options.heartbeatTtlSec,
    );
  }

  async isOnline(userId: string): Promise<boolean> {
    return (await this.client.presenceIsOnline(userKey(userId))) === 1;
  }

  /** Returns the subset of userIds that are currently online. */
  async onlineAmong(userIds: string[]): Promise<string[]> {
    if (userIds.length === 0) return [];
    const pipeline = this.client.pipeline();
    for (const id of userIds) {
      (pipeline as unknown as PresenceRedis).presenceIsOnline(userKey(id));
    }
    const results = (await pipeline.exec()) ?? [];
    return userIds.filter((_, i) => results[i]?.[1] === 1);
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- presence.service`
Expected: PASS, 12 tests. (Spec takes ~5s because of the TTL sleeps.)

- [ ] **Step 5: Commit**

```bash
git add src/realtime/presence.service.ts src/realtime/presence.service.spec.ts
git commit -m "feat: add PresenceService backed by a Redis Set per user with heartbeat keys"
```

---

### Task 5: RoomsService member lookups (TDD)

**Files:**
- Modify: `src/rooms/rooms.service.ts`, `src/rooms/rooms.service.spec.ts`

**Interfaces:**
- Produces: `RoomsService.listMemberIds(roomId: string): Promise<string[]>`, `RoomsService.listRoomIdsOf(userId: string): Promise<string[]>` — consumed by `EventsGateway` (Task 6) and `PresenceController` (Task 8).

- [ ] **Step 1: Write the failing test**

In `src/rooms/rooms.service.spec.ts`, add this block inside the outer `describe('RoomsService', ...)`, before its final `});`:

```typescript
  describe('member lookups', () => {
    it('listMemberIds returns the userIds of every member of the room', async () => {
      const a = await createUser('lm-a@example.com');
      const b = await createUser('lm-b@example.com');
      const c = await createUser('lm-c@example.com');
      const room = await roomsService.createRoom(a.id, { name: 'LM' });
      await roomsService.joinRoom(b.id, room.id);

      const ids = await roomsService.listMemberIds(room.id);

      expect(ids.sort()).toEqual([a.id, b.id].sort());
      expect(ids).not.toContain(c.id);
    });

    it('listRoomIdsOf returns the ids of every room the user belongs to', async () => {
      const a = await createUser('lr-a@example.com');
      const b = await createUser('lr-b@example.com');
      const r1 = await roomsService.createRoom(a.id, { name: 'R1' });
      const r2 = await roomsService.createRoom(a.id, { name: 'R2' });
      await roomsService.createRoom(b.id, { name: 'Other' });

      const ids = await roomsService.listRoomIdsOf(a.id);

      expect(ids.sort()).toEqual([r1.id, r2.id].sort());
    });

    it('returns empty arrays for unknown ids', async () => {
      expect(await roomsService.listMemberIds('00000000-0000-0000-0000-000000000000')).toEqual([]);
      expect(await roomsService.listRoomIdsOf('00000000-0000-0000-0000-000000000000')).toEqual([]);
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- rooms.service`
Expected: FAIL — `Property 'listMemberIds' does not exist on type 'RoomsService'`.

- [ ] **Step 3: Implement**

In `src/rooms/rooms.service.ts`, add after `listRooms`:

```typescript
  async listMemberIds(roomId: string): Promise<string[]> {
    const rows = await this.prisma.roomMember.findMany({
      where: { roomId },
      select: { userId: true },
    });
    return rows.map((r) => r.userId);
  }

  async listRoomIdsOf(userId: string): Promise<string[]> {
    const rows = await this.prisma.roomMember.findMany({
      where: { userId },
      select: { roomId: true },
    });
    return rows.map((r) => r.roomId);
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- rooms.service`
Expected: PASS (existing + 3 new tests).

- [ ] **Step 5: Commit**

```bash
git add src/rooms
git commit -m "feat: add RoomsService.listMemberIds and listRoomIdsOf"
```

---

### Task 6: Gateway presence broadcast + multi-instance harness (TDD)

**Files:**
- Create: `test/instance.ts`, `src/realtime/multi-instance.spec.ts`
- Modify: `src/realtime/events.gateway.ts`, `src/realtime/realtime.module.ts`, `src/realtime/events.gateway.spec.ts`

**Interfaces:**
- Produces: `createInstance(presence?: Partial<PresenceOptions>): Promise<TestInstance>` where `TestInstance { app: INestApplication; url: string; redis: Redis; get<T>(token): T; close(): Promise<void> }` — boots the real `AppModule` with `RedisIoAdapter` on a random port (each instance has its own Redis/Prisma connections, all sharing the same Postgres and Redis DB). `EventsGateway` now implements `OnGatewayConnection`/`OnGatewayDisconnect`, emits `presence:update { userId, online }` to every room the user is a member of, **only on transitions**. `RealtimeModule` provides `PresenceService` and `PRESENCE_OPTIONS`.
- Consumes: `PresenceService`, `PRESENCE_OPTIONS`, `DEFAULT_PRESENCE_OPTIONS` (Task 4); `RoomsService.listRoomIdsOf` (Task 5); `RedisIoAdapter`, `REDIS` (Task 2); helpers (Task 1).

- [ ] **Step 1: Create the harness `test/instance.ts`**

```typescript
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { AppModule } from '../src/app.module';
import { REDIS } from '../src/redis/redis.constants';
import { RedisIoAdapter } from '../src/redis/redis-io.adapter';
import {
  DEFAULT_PRESENCE_OPTIONS,
  PRESENCE_OPTIONS,
  PresenceOptions,
} from '../src/realtime/presence.service';

export interface TestInstance {
  app: INestApplication;
  url: string;
  redis: Redis;
  get<T>(token: unknown): T;
  close(): Promise<void>;
}

/** Boots a full app instance (real Postgres + real Redis + Redis adapter) on a random port. */
export async function createInstance(presence: Partial<PresenceOptions> = {}): Promise<TestInstance> {
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
    .overrideProvider(PRESENCE_OPTIONS)
    .useValue({ ...DEFAULT_PRESENCE_OPTIONS, ...presence })
    .compile();

  const app = moduleRef.createNestApplication();
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  const redis = app.get<Redis>(REDIS);
  app.useWebSocketAdapter(new RedisIoAdapter(app, redis));
  await app.listen(0);

  const port = (app.getHttpServer().address() as AddressInfo).port;
  return {
    app,
    url: `http://localhost:${port}`,
    redis,
    get: <T>(token: unknown) => app.get<T>(token as never),
    close: () => app.close(),
  };
}
```

(`app.close()` also runs Nest's shutdown hooks: gateway cleanup, `RedisIoAdapter.dispose()` via `SocketModule.close()`, and `RedisModule.onApplicationShutdown`.)

- [ ] **Step 2: Write the failing test**

Create `src/realtime/multi-instance.spec.ts`:

```typescript
import { Socket as ClientSocket } from 'socket.io-client';
import { createInstance, TestInstance } from '../../test/instance';
import { cleanDatabase } from '../../test/db-cleanup';
import { cleanRedis } from '../../test/redis-cleanup';
import { connectSocket, emitWithAck, sleep, waitFor } from '../../test/helpers';
import { PrismaService } from '../prisma/prisma.service';
import { RoomsService } from '../rooms/rooms.service';
import { WsTicketService } from './ws-ticket.service';
import { PresenceService } from './presence.service';

interface PresenceEvent {
  userId: string;
  online: boolean;
}

describe('multi-instance realtime (2 app instances, 1 Redis, 1 Postgres)', () => {
  let a: TestInstance;
  let b: TestInstance;
  let prisma: PrismaService;
  let rooms: RoomsService;
  let sockets: ClientSocket[] = [];

  beforeAll(async () => {
    a = await createInstance();
    b = await createInstance();
    prisma = a.get(PrismaService);
    rooms = a.get(RoomsService);
  });

  afterAll(async () => {
    await a.close();
    await b.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
    await cleanRedis(a.redis);
    sockets = [];
  });

  afterEach(() => {
    sockets.forEach((s) => s.disconnect());
  });

  async function createUser(email: string) {
    return prisma.user.create({ data: { email, passwordHash: 'x', displayName: email } });
  }

  async function connectTo(instance: TestInstance, userId: string): Promise<ClientSocket> {
    const ticket = await instance.get<WsTicketService>(WsTicketService).issue(userId);
    const socket = await connectSocket(instance.url, ticket);
    sockets.push(socket);
    return socket;
  }

  function presenceEvents(socket: ClientSocket, userId: string): PresenceEvent[] {
    const events: PresenceEvent[] = [];
    socket.on('presence:update', (e: PresenceEvent) => {
      if (e.userId === userId) events.push(e);
    });
    return events;
  }

  describe('messaging and tickets', () => {
    it('delivers message:new sent via instance A to a subscriber connected to instance B', async () => {
      const sender = await createUser('x-sender@example.com');
      const receiver = await createUser('x-receiver@example.com');
      const room = await rooms.createRoom(sender.id, { name: 'Cross' });
      await rooms.joinRoom(receiver.id, room.id);

      const onA = await connectTo(a, sender.id);
      const onB = await connectTo(b, receiver.id);
      await emitWithAck(onA, 'room:subscribe', { roomId: room.id });
      await emitWithAck(onB, 'room:subscribe', { roomId: room.id });

      const received = new Promise<{ content: string }>((resolve) => onB.once('message:new', resolve));
      const ack = await emitWithAck<{ success: boolean }>(onA, 'message:send', {
        roomId: room.id,
        content: 'hello across instances',
      });

      expect(ack.success).toBe(true);
      expect((await received).content).toBe('hello across instances');
    });

    it('accepts on instance B a ticket that was issued by instance A, once only', async () => {
      const user = await createUser('x-ticket@example.com');
      const ticket = await a.get<WsTicketService>(WsTicketService).issue(user.id);

      const first = await connectSocket(b.url, ticket);
      sockets.push(first);
      expect(first.connected).toBe(true);

      await expect(connectSocket(a.url, ticket)).rejects.toBeDefined();
    });
  });

  describe('presence', () => {
    async function setup() {
      const subject = await createUser('p-subject@example.com');
      const observer = await createUser('p-observer@example.com');
      const room = await rooms.createRoom(subject.id, { name: 'Presence' });
      await rooms.joinRoom(observer.id, room.id);

      const observerSocket = await connectTo(b, observer.id);
      await emitWithAck(observerSocket, 'room:subscribe', { roomId: room.id });
      const events = presenceEvents(observerSocket, subject.id);
      return { subject, observer, room, observerSocket, events };
    }

    it('broadcasts online:true across instances when a user connects', async () => {
      const { subject, events } = await setup();

      await connectTo(a, subject.id);

      await waitFor(() => events.some((e) => e.online));
      expect(events).toEqual([{ userId: subject.id, online: true }]);
    });

    it('does not announce extra tabs, and only announces offline when the last tab closes', async () => {
      const { subject, events } = await setup();
      const redisKey = `presence:user:${subject.id}`;

      const tab1 = await connectTo(a, subject.id);
      await waitFor(() => events.length === 1);
      const tab2 = await connectTo(b, subject.id); // second device, other instance
      await waitFor(async () => (await a.redis.scard(redisKey)) === 2);
      await sleep(250);
      expect(events).toEqual([{ userId: subject.id, online: true }]); // no second online event

      tab1.disconnect();
      await waitFor(async () => (await a.redis.scard(redisKey)) === 1);
      await sleep(250);
      expect(events).toHaveLength(1); // still online: no offline event yet
      expect(await a.get<PresenceService>(PresenceService).isOnline(subject.id)).toBe(true);

      tab2.disconnect();
      await waitFor(() => events.length === 2);
      expect(events[1]).toEqual({ userId: subject.id, online: false });
    });

    it('does not send presence updates for rooms the user is not a member of', async () => {
      const subject = await createUser('p-subj2@example.com');
      const stranger = await createUser('p-stranger@example.com');
      const strangerRoom = await rooms.createRoom(stranger.id, { name: 'Elsewhere' });
      const strangerSocket = await connectTo(b, stranger.id);
      await emitWithAck(strangerSocket, 'room:subscribe', { roomId: strangerRoom.id });
      const events = presenceEvents(strangerSocket, subject.id);

      await connectTo(a, subject.id);
      await sleep(400);

      expect(events).toEqual([]);
    });
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npm test -- multi-instance`
Expected: the two messaging/ticket tests PASS (they exercise adapter + Redis tickets from Tasks 2-3); the three presence tests FAIL (`waitFor timed out` — nothing emits `presence:update` yet; `PresenceService` is not provided in the module either, so the `a.get(PresenceService)` lookup would throw). If the messaging tests fail, stop and fix Tasks 2-3 first.

- [ ] **Step 4: Implement — module wiring**

Replace `src/realtime/realtime.module.ts` with:

```typescript
import { Module } from '@nestjs/common';
import { RoomsModule } from '../rooms/rooms.module';
import { WsTicketService } from './ws-ticket.service';
import { EventsGateway } from './events.gateway';
import {
  DEFAULT_PRESENCE_OPTIONS,
  PRESENCE_OPTIONS,
  PresenceService,
} from './presence.service';

@Module({
  imports: [RoomsModule],
  providers: [
    WsTicketService,
    EventsGateway,
    PresenceService,
    { provide: PRESENCE_OPTIONS, useValue: DEFAULT_PRESENCE_OPTIONS },
  ],
  exports: [WsTicketService, PresenceService],
})
export class RealtimeModule {}
```

- [ ] **Step 5: Implement — gateway presence hooks**

In `src/realtime/events.gateway.ts` update imports and the class header/constructor, and add the two lifecycle handlers plus `broadcastPresence`:

```typescript
import { HttpException, Injectable, Logger } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { WsTicketService } from './ws-ticket.service';
import { PresenceService } from './presence.service';
import { RoomsService } from '../rooms/rooms.service';

interface AckResponse {
  success: boolean;
  error?: string;
}

@Injectable()
@WebSocketGateway({ cors: true, transports: ['websocket'] })
export class EventsGateway implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect {
  private readonly logger = new Logger(EventsGateway.name);

  @WebSocketServer()
  server!: Server;

  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly roomsService: RoomsService,
    private readonly presenceService: PresenceService,
  ) {}
```

Keep `afterInit` and the three `@SubscribeMessage` handlers exactly as they are after Task 3, and add:

```typescript
  async handleConnection(client: Socket): Promise<void> {
    try {
      const userId = client.data.userId as string;
      const cameOnline = await this.presenceService.connect(userId, client.id);
      if (cameOnline) await this.broadcastPresence(userId, true);
    } catch (err) {
      this.logger.error(`presence connect failed: ${(err as Error).message}`);
    }
  }

  async handleDisconnect(client: Socket): Promise<void> {
    const userId = client.data.userId as string | undefined;
    if (!userId) return;
    try {
      const wentOffline = await this.presenceService.disconnect(userId, client.id);
      if (wentOffline) await this.broadcastPresence(userId, false);
    } catch (err) {
      this.logger.error(`presence disconnect failed: ${(err as Error).message}`);
    }
  }

  /** Tells every room the user belongs to (on any instance, via the adapter). */
  private async broadcastPresence(userId: string, online: boolean): Promise<void> {
    const roomIds = await this.roomsService.listRoomIdsOf(userId);
    if (roomIds.length === 0) return;
    this.server.to(roomIds).emit('presence:update', { userId, online });
  }
```

- [ ] **Step 6: Keep the single-instance gateway spec compiling**

In `src/realtime/events.gateway.spec.ts`, add `PresenceService`, `PRESENCE_OPTIONS`, `DEFAULT_PRESENCE_OPTIONS` to the testing module:

```typescript
import {
  DEFAULT_PRESENCE_OPTIONS,
  PRESENCE_OPTIONS,
  PresenceService,
} from './presence.service';
...
    const moduleRef = await Test.createTestingModule({
      imports: [RedisModule],
      providers: [
        EventsGateway,
        WsTicketService,
        RoomsService,
        PrismaService,
        PresenceService,
        { provide: PRESENCE_OPTIONS, useValue: DEFAULT_PRESENCE_OPTIONS },
      ],
    }).compile();
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `npx tsc --noEmit && npm test -- events.gateway multi-instance`
Expected: PASS — all `events.gateway` tests and all 5 `multi-instance` tests.

- [ ] **Step 8: Run the full suite and commit**

Run: `npm test`
Expected: all suites pass.

```bash
git add src test
git commit -m "feat: broadcast presence:update on online/offline transitions across instances"
```

---

### Task 7: Heartbeat refresh, crash safety, graceful shutdown (TDD)

**Files:**
- Create: `src/realtime/presence.lifecycle.spec.ts`
- Modify: `src/realtime/events.gateway.ts`

**Interfaces:**
- Produces: `EventsGateway` starts a heartbeat timer in `afterInit` (every `options.refreshIntervalMs`, `touch`es all **local** sockets), and `onModuleDestroy` stops it and runs the disconnect path for every local socket (broadcasting `online:false` where the user's last socket was on this instance).
- Consumes: `PRESENCE_OPTIONS` (Task 4), `createInstance` (Task 6).

- [ ] **Step 1: Write the failing test**

Create `src/realtime/presence.lifecycle.spec.ts`:

```typescript
import { Socket as ClientSocket } from 'socket.io-client';
import { createInstance, TestInstance } from '../../test/instance';
import { cleanDatabase } from '../../test/db-cleanup';
import { cleanRedis } from '../../test/redis-cleanup';
import { connectSocket, emitWithAck, sleep, waitFor } from '../../test/helpers';
import { PrismaService } from '../prisma/prisma.service';
import { RoomsService } from '../rooms/rooms.service';
import { WsTicketService } from './ws-ticket.service';
import { PresenceService } from './presence.service';

describe('presence lifecycle (heartbeat, crash, shutdown)', () => {
  const instances: TestInstance[] = [];
  let sockets: ClientSocket[] = [];
  let observerInstance: TestInstance;
  let prisma: PrismaService;
  let rooms: RoomsService;

  async function boot(presence: { heartbeatTtlSec: number; refreshIntervalMs: number }) {
    const instance = await createInstance(presence);
    instances.push(instance);
    return instance;
  }

  async function connectTo(instance: TestInstance, userId: string) {
    const ticket = await instance.get<WsTicketService>(WsTicketService).issue(userId);
    const socket = await connectSocket(instance.url, ticket);
    sockets.push(socket);
    return socket;
  }

  async function createUser(email: string) {
    return prisma.user.create({ data: { email, passwordHash: 'x', displayName: email } });
  }

  beforeAll(async () => {
    observerInstance = await boot({ heartbeatTtlSec: 60, refreshIntervalMs: 20_000 });
    prisma = observerInstance.get(PrismaService);
    rooms = observerInstance.get(RoomsService);
  });

  afterAll(async () => {
    await Promise.all(instances.map((i) => i.close().catch(() => undefined)));
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
    await cleanRedis(observerInstance.redis);
    sockets = [];
  });

  afterEach(() => {
    sockets.forEach((s) => s.disconnect());
  });

  it('keeps a live socket online past the heartbeat TTL because the gateway refreshes it', async () => {
    const instance = await boot({ heartbeatTtlSec: 1, refreshIntervalMs: 200 });
    const user = await createUser('hb-live@example.com');
    await connectTo(instance, user.id);

    await sleep(1600); // > 1.5x TTL; would be offline without the refresh timer

    expect(await observerInstance.get<PresenceService>(PresenceService).isOnline(user.id)).toBe(true);
  });

  it('drops a user whose instance stopped heartbeating (simulated crash)', async () => {
    // refresh interval far longer than the TTL => this instance never refreshes in time,
    // exactly what other instances observe when it is killed with SIGKILL.
    const dying = await boot({ heartbeatTtlSec: 1, refreshIntervalMs: 600_000 });
    const user = await createUser('hb-dead@example.com');
    await connectTo(dying, user.id);
    expect(await observerInstance.get<PresenceService>(PresenceService).isOnline(user.id)).toBe(true);

    await sleep(1300);

    expect(await observerInstance.get<PresenceService>(PresenceService).isOnline(user.id)).toBe(false);
    expect(await observerInstance.redis.scard(`presence:user:${user.id}`)).toBe(0);
  });

  it('on graceful shutdown removes the instance sockets and announces online:false', async () => {
    const leaving = await boot({ heartbeatTtlSec: 60, refreshIntervalMs: 20_000 });
    const subject = await createUser('sd-subject@example.com');
    const observer = await createUser('sd-observer@example.com');
    const room = await rooms.createRoom(subject.id, { name: 'Shutdown' });
    await rooms.joinRoom(observer.id, room.id);

    const observerSocket = await connectTo(observerInstance, observer.id);
    await emitWithAck(observerSocket, 'room:subscribe', { roomId: room.id });
    const events: Array<{ userId: string; online: boolean }> = [];
    observerSocket.on('presence:update', (e) => {
      if (e.userId === subject.id) events.push(e);
    });

    await connectTo(leaving, subject.id);
    await waitFor(() => events.some((e) => e.online));

    await leaving.close();

    await waitFor(() => events.some((e) => !e.online));
    expect(await observerInstance.redis.scard(`presence:user:${subject.id}`)).toBe(0);
    expect(events.filter((e) => !e.online)).toHaveLength(1); // not duplicated by socket close handlers
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- presence.lifecycle`
Expected: "keeps a live socket online…" FAILS (no refresh timer → offline after 1s) and "graceful shutdown…" FAILS (`waitFor timed out` — no `online:false`; or Set not empty). "drops a user whose instance stopped heartbeating" already PASSES (pruning from Task 4).

- [ ] **Step 3: Implement**

In `src/realtime/events.gateway.ts`:

1. Import `Inject`, `OnModuleDestroy` and the options:

```typescript
import { HttpException, Inject, Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
...
import { PRESENCE_OPTIONS, PresenceOptions, PresenceService } from './presence.service';
```

2. Class header and constructor:

```typescript
export class EventsGateway
  implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect, OnModuleDestroy
{
  private readonly logger = new Logger(EventsGateway.name);
  private heartbeatTimer?: NodeJS.Timeout;

  @WebSocketServer()
  server!: Server;

  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly roomsService: RoomsService,
    private readonly presenceService: PresenceService,
    @Inject(PRESENCE_OPTIONS) private readonly presenceOptions: PresenceOptions,
  ) {}
```

3. At the end of `afterInit(server)`, after `server.use(...)`, start the timer:

```typescript
    this.heartbeatTimer = setInterval(
      () => void this.refreshHeartbeats(),
      this.presenceOptions.refreshIntervalMs,
    );
    this.heartbeatTimer.unref();
```

4. Add the two methods:

```typescript
  /** Refreshes heartbeats of the sockets connected to THIS instance only. */
  private async refreshHeartbeats(): Promise<void> {
    const local = [...this.server.sockets.sockets.values()].filter((s) => s.data.userId);
    const results = await Promise.allSettled(
      local.map((s) => this.presenceService.touch(s.data.userId as string, s.id)),
    );
    for (const r of results) {
      if (r.status === 'rejected') {
        this.logger.error(`presence heartbeat failed: ${(r.reason as Error).message}`);
      }
    }
  }

  /** Graceful shutdown: stop heartbeating and remove this instance's sockets from presence. */
  async onModuleDestroy(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    const local = [...(this.server?.sockets?.sockets.values() ?? [])];
    await Promise.all(local.map((s) => this.handleDisconnect(s)));
  }
```

(`handleDisconnect` already swallows/logs errors. When sockets are closed afterwards by Nest, their `handleDisconnect` runs again but `disconnect()` returns `false` for an already-removed socket, so `online:false` is not duplicated.)

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- presence.lifecycle`
Expected: PASS, 3 tests.

- [ ] **Step 5: Full suite and commit**

Run: `npx tsc --noEmit && npm test`
Expected: all pass.

```bash
git add src
git commit -m "feat: heartbeat refresh timer and graceful-shutdown presence cleanup"
```

---

### Task 8: `GET /rooms/:roomId/presence` (TDD)

**Files:**
- Create: `src/realtime/presence.controller.ts`, `src/realtime/presence.controller.spec.ts`
- Modify: `src/realtime/realtime.module.ts`

**Interfaces:**
- Produces: `GET /rooms/:roomId/presence` (`JwtAuthGuard`) → `{ onlineUserIds: string[] }`; 403 for a non-member (via `RoomsService.assertMember`); 401 without a token. Consumed by `web/` (Task 9).
- Consumes: `RoomsService.assertMember`/`listMemberIds` (Tasks 5), `PresenceService.onlineAmong` (Task 4), `createInstance` (Task 6).

- [ ] **Step 1: Write the failing test**

Create `src/realtime/presence.controller.spec.ts`:

```typescript
import { Socket as ClientSocket } from 'socket.io-client';
import { createInstance, TestInstance } from '../../test/instance';
import { cleanDatabase } from '../../test/db-cleanup';
import { cleanRedis } from '../../test/redis-cleanup';
import { connectSocket } from '../../test/helpers';
import { AuthService } from '../auth/auth.service';
import { PrismaService } from '../prisma/prisma.service';
import { RoomsService } from '../rooms/rooms.service';
import { WsTicketService } from './ws-ticket.service';
import { PresenceService } from './presence.service';

describe('GET /rooms/:roomId/presence', () => {
  let app: TestInstance;
  let prisma: PrismaService;
  let rooms: RoomsService;
  let sockets: ClientSocket[] = [];

  beforeAll(async () => {
    app = await createInstance();
    prisma = app.get(PrismaService);
    rooms = app.get(RoomsService);
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
    await cleanRedis(app.redis);
    sockets = [];
  });

  afterEach(() => {
    sockets.forEach((s) => s.disconnect());
  });

  async function registerUser(email: string) {
    const { accessToken } = await app
      .get<AuthService>(AuthService)
      .register({ email, password: 'password123', displayName: email });
    const user = await prisma.user.findUniqueOrThrow({ where: { email } });
    return { id: user.id, accessToken };
  }

  async function goOnline(userId: string) {
    const ticket = await app.get<WsTicketService>(WsTicketService).issue(userId);
    const socket = await connectSocket(app.url, ticket);
    sockets.push(socket);
    await app.get<PresenceService>(PresenceService).isOnline(userId); // ensure round-trip
    return socket;
  }

  function get(roomId: string, token?: string) {
    return fetch(`${app.url}/rooms/${roomId}/presence`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  }

  it('returns 401 without a token', async () => {
    const res = await get('any-room');
    expect(res.status).toBe(401);
  });

  it('returns 403 when the caller is not a member of the room', async () => {
    const owner = await registerUser('pc-owner@example.com');
    const outsider = await registerUser('pc-outsider@example.com');
    const room = await rooms.createRoom(owner.id, { name: 'Private' });

    const res = await get(room.id, outsider.accessToken);

    expect(res.status).toBe(403);
  });

  it('lists only the members that are currently online', async () => {
    const me = await registerUser('pc-me@example.com');
    const online = await registerUser('pc-online@example.com');
    const offline = await registerUser('pc-offline@example.com');
    const room = await rooms.createRoom(me.id, { name: 'Team' });
    await rooms.joinRoom(online.id, room.id);
    await rooms.joinRoom(offline.id, room.id);
    await goOnline(online.id);

    const res = await get(room.id, me.accessToken);

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ onlineUserIds: [online.id] });
  });

  it('counts a user online with two tabs once, and offline after both close', async () => {
    const me = await registerUser('pc-me2@example.com');
    const peer = await registerUser('pc-peer@example.com');
    const room = await rooms.createRoom(me.id, { name: 'Tabs' });
    await rooms.joinRoom(peer.id, room.id);
    const tab1 = await goOnline(peer.id);
    const tab2 = await goOnline(peer.id);

    tab1.disconnect();
    await new Promise((r) => setTimeout(r, 300));
    expect(await (await get(room.id, me.accessToken)).json()).toEqual({ onlineUserIds: [peer.id] });

    tab2.disconnect();
    await new Promise((r) => setTimeout(r, 300));
    expect(await (await get(room.id, me.accessToken)).json()).toEqual({ onlineUserIds: [] });
  });
});
```

(`AuthService.register` returns `{ accessToken, refreshToken }` — same usage as `auth.controller.spec.ts`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- presence.controller`
Expected: FAIL — requests return 404 (route does not exist), so the 401/403/200 assertions fail.

- [ ] **Step 3: Implement**

Create `src/realtime/presence.controller.ts`:

```typescript
import { Controller, Get, Param, Req, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { RoomsService } from '../rooms/rooms.service';
import { PresenceService } from './presence.service';

@Controller('rooms')
@UseGuards(JwtAuthGuard)
export class PresenceController {
  constructor(
    private readonly roomsService: RoomsService,
    private readonly presenceService: PresenceService,
  ) {}

  @Get(':roomId/presence')
  async getPresence(
    @Req() req: { user: { id: string } },
    @Param('roomId') roomId: string,
  ): Promise<{ onlineUserIds: string[] }> {
    await this.roomsService.assertMember(req.user.id, roomId);
    const memberIds = await this.roomsService.listMemberIds(roomId);
    return { onlineUserIds: await this.presenceService.onlineAmong(memberIds) };
  }
}
```

Update `src/realtime/realtime.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { RoomsModule } from '../rooms/rooms.module';
import { WsTicketService } from './ws-ticket.service';
import { EventsGateway } from './events.gateway';
import { PresenceController } from './presence.controller';
import {
  DEFAULT_PRESENCE_OPTIONS,
  PRESENCE_OPTIONS,
  PresenceService,
} from './presence.service';

@Module({
  imports: [RoomsModule, PassportModule],
  controllers: [PresenceController],
  providers: [
    WsTicketService,
    EventsGateway,
    PresenceService,
    { provide: PRESENCE_OPTIONS, useValue: DEFAULT_PRESENCE_OPTIONS },
  ],
  exports: [WsTicketService, PresenceService],
})
export class RealtimeModule {}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- presence.controller`
Expected: PASS, 4 tests.

- [ ] **Step 5: Full suite and commit**

Run: `npx tsc --noEmit && npm test`
Expected: all pass.

```bash
git add src
git commit -m "feat: add GET /rooms/:roomId/presence"
```

---

### Task 9: Bootstrap wiring and `web/` client

**Files:**
- Modify: `src/main.ts`, `web/app/chat/chat-client.tsx`
- Create: `web/app/api/rooms/[roomId]/presence/route.ts`

**Interfaces:**
- Produces: production bootstrap uses `RedisIoAdapter` + `enableShutdownHooks()`; the browser client connects with `transports: ['websocket']`, shows online members of the active room (initial fetch from `/api/rooms/:id/presence`, deltas from `presence:update`, re-fetch on reconnect).
- Consumes: `RedisIoAdapter`, `REDIS` (Task 2); `GET /rooms/:roomId/presence` (Task 8). No new automated tests (bootstrap + UI are exempt per spec); verified by typecheck and the smoke test in Task 10.

- [ ] **Step 1: Update `src/main.ts`**

```typescript
import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import Redis from 'ioredis';
import { AppModule } from './app.module';
import { REDIS } from './redis/redis.constants';
import { RedisIoAdapter } from './redis/redis-io.adapter';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.useWebSocketAdapter(new RedisIoAdapter(app, app.get<Redis>(REDIS)));
  app.enableShutdownHooks(); // SIGTERM/SIGINT => gateway cleans presence, then Redis quits
  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
```

- [ ] **Step 2: Add the presence proxy route**

Create `web/app/api/rooms/[roomId]/presence/route.ts`:

```typescript
import { NextResponse } from 'next/server';
import { nestFetch } from '../../../../../lib/nest-fetch';

export async function GET(_request: Request, { params }: { params: { roomId: string } }) {
  const nestResponse = await nestFetch(`/rooms/${params.roomId}/presence`);
  return NextResponse.json(await nestResponse.json(), { status: nestResponse.status });
}
```

- [ ] **Step 3: Update `web/app/chat/chat-client.tsx`**

a) Add state and a loader next to the other state hooks:

```typescript
  const [onlineUserIds, setOnlineUserIds] = useState<string[]>([]);
```

b) Add this helper inside the component (above `selectRoom`):

```typescript
  async function loadPresence(roomId: string) {
    const res = await fetch(`/api/rooms/${roomId}/presence`);
    if (!res.ok) return;
    const { onlineUserIds: ids } = await res.json();
    if (roomId === activeRoomIdRef.current) setOnlineUserIds(ids);
  }
```

c) In the socket `useEffect`: force the websocket transport, apply `presence:update` deltas, and re-sync on (re)connect. Replace the `io(...)` options and the `connect` handler with:

```typescript
    const socket = io(process.env.NEXT_PUBLIC_NEST_URL ?? 'http://localhost:3000', {
      transports: ['websocket'], // no sticky sessions needed behind a load balancer
      auth: (cb) => {
        fetch('/api/ws-ticket')
          .then((res) => res.json())
          .then(({ ticket }) => cb({ ticket }))
          .catch(() => cb({}));
      },
    });

    socket.on('presence:update', ({ userId, online }: { userId: string; online: boolean }) => {
      setOnlineUserIds((prev) => {
        const without = prev.filter((id) => id !== userId);
        return online ? [...without, userId] : without;
      });
    });

    socket.on('connect', () => {
      const roomId = activeRoomIdRef.current;
      if (roomId) {
        socket.emit('room:subscribe', { roomId });
        void loadPresence(roomId); // deltas may have been missed while disconnected
      }
    });
```

(keep the existing `message:new` handler.)

d) In `selectRoom`, after `socketRef.current?.emit('room:subscribe', { roomId });` add `void loadPresence(roomId);`. Because `activeRoomIdRef` is updated in an effect, set it eagerly in `selectRoom` before calling `loadPresence`: add `activeRoomIdRef.current = roomId;` right after `setActiveRoomId(roomId);`.

e) Render the online count under the message list, inside `<main>` before the send box:

```tsx
        {activeRoomId && <p>Online now: {onlineUserIds.length}</p>}
```

- [ ] **Step 4: Typecheck backend and web**

Run: `npx tsc --noEmit`
Expected: clean.

Run: `cd web && npm ci && npx tsc --noEmit; cd ..`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/main.ts web
git commit -m "feat: wire RedisIoAdapter + shutdown hooks and show room presence in web client"
```

---

### Task 10: Final verification and smoke test

**Files:** none (verification only; fix and commit separately if anything fails).

- [ ] **Step 1: Full automated check**

Run: `npx tsc --noEmit && (cd web && npx tsc --noEmit) && npm run build && npm test`
Expected: everything clean; all suites pass (Phase 1/2 suites plus redis, ws-ticket, presence.service, rooms member lookups, multi-instance, presence.lifecycle, presence.controller).

- [ ] **Step 2: Run the suite twice to catch order/flakiness**

Run: `npm test && npm test`
Expected: both runs pass.

- [ ] **Step 3: Two real processes + graceful shutdown smoke test**

```bash
set -a; . ./.env.test; set +a
PORT=3000 node dist/main.js & P1=$!
PORT=3002 node dist/main.js & P2=$!
sleep 4
curl -s -o /dev/null -w "A:%{http_code} " localhost:3000/auth/me
curl -s -o /dev/null -w "B:%{http_code}\n" localhost:3002/auth/me
kill -TERM $P1 $P2; wait $P1 $P2; echo "exit codes: $?"
```
Expected: `A:401 B:401` (both instances up, guard working), and both processes exit cleanly after SIGTERM with no stack trace.

- [ ] **Step 4: Tick the spec's completion checklist**

Edit `docs/superpowers/specs/2026-11-03-phase3-redis-scale-readiness-design.md`: change each `- [ ]` under "Tiêu chí hoàn thành Phase 3" to `- [x]` for items the tests above prove, and commit:

```bash
git add docs
git commit -m "docs: mark Phase 3 completion criteria"
```
