# Phase 5 — Gateway Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract a standalone `gateway/` NestJS service that owns all WebSocket connections and reverse-proxies REST traffic to `auth-service`/chat-service (`src/`), so chat-service no longer runs any WebSocket code and instead notifies gateway of new messages via Redis pub/sub.

**Architecture:** `gateway/` is a new, independent NestJS project (own `package.json`, own Postgres-free — it never touches Prisma) that owns `EventsGateway`, ws-ticket issuance, and a dumb HTTP reverse proxy. chat-service keeps rooms/messages/presence-read and gains two internal-only endpoints plus a `JwtOrInternalAuthGuard` so gateway can act on a WS user's behalf without a real JWT. `web/` talks to gateway exclusively.

**Tech Stack:** NestJS 10, `ioredis` (shared Redis, pub/sub + presence Sets), `socket.io` + `@socket.io/redis-adapter` (moves to gateway), `@nestjs/axios` (new, gateway's reverse proxy), Jest + real I/O (Postgres for chat-service, Redis for both, a lightweight real HTTP stub standing in for chat-service in gateway's own test suite — same pattern Phase 4 already established with `startJwksServer`).

**Spec:** `docs/superpowers/specs/2026-11-05-phase5-gateway-design.md`

## Global Constraints

- REST through gateway is a **dumb proxy**: forward the `Authorization` header as-is, do not verify it in the proxy path. `auth-service`/chat-service keep verifying JWTs themselves exactly as today.
- Internal calls (gateway → chat-service) use header `X-Internal-Secret` (must equal env `INTERNAL_SECRET` on both sides) and, where a trusted identity is needed, `X-Internal-User-Id`.
- `message:send` over WS must NOT call `RoomsService` directly (gateway has no DB access) — it calls `POST /rooms/:roomId/messages` internally and relies on `RoomsService.sendMessage()` publishing to Redis channel `chat:message:new` for the actual `message:new` emit. The gateway's own WS handler never emits `message:new` directly — only the pub/sub subscriber does, so REST-originated sends also broadcast (closing today's gap where REST sends never notified WS clients).
- `room:subscribe` membership check and presence `roomIds` lookup also go through internal HTTP calls (`GET /internal/rooms/:roomId/members/:userId`, `GET /internal/users/:userId/room-ids`) — gateway never queries Postgres.
- `POST /ws-ticket` moves to gateway entirely (not proxied) — gateway needs its own `JwksService`/`JwtStrategy`/`JwtAuthGuard` copy, same `AUTH_JWKS_URL`/`JWT_ISSUER`/`JWT_AUDIENCE` env contract as chat-service.
- TDD with real I/O (no mocking Prisma/Redis/socket/HTTP) — same convention as Phases 1-4. For gateway's own test suite specifically, "real I/O" for its chat-service dependency means a real (if minimal) HTTP server standing in for chat-service — exactly the precedent `test/jwt.ts`'s `startJwksServer` already set for the auth-service boundary, not a mocked function.
- Ports: chat-service `3000` (unchanged), auth-service `3010` (unchanged), gateway `3020` (new).
- `tsconfig.json`/`tsconfig.build.json` at repo root must exclude `gateway` (same as they already exclude `web` and `auth-service`).

---

## File Structure

```
gateway/                                  # NEW — independent NestJS project
├── package.json  tsconfig.json  tsconfig.build.json  nest-cli.json  jest.config.js
├── src/
│   ├── main.ts
│   ├── app.module.ts
│   ├── auth/                             # copied from src/auth/ (JWKS verify), unchanged logic
│   │   ├── jwks.service.ts  jwks.service.spec.ts
│   │   ├── jwt.strategy.ts
│   │   ├── jwt-auth.guard.ts
│   │   └── auth.module.ts
│   ├── redis/                            # copied from src/redis/, unchanged
│   │   ├── redis.constants.ts
│   │   ├── redis.module.ts  redis.module.spec.ts
│   │   ├── redis-io.adapter.ts  redis-io.adapter.spec.ts
│   ├── proxy/
│   │   └── proxy.controller.ts  proxy.controller.spec.ts      # NEW
│   └── realtime/
│       ├── ws-ticket.service.ts  ws-ticket.service.spec.ts    # moved verbatim from src/realtime/
│       ├── ws-ticket.controller.ts  ws-ticket.controller.spec.ts  # moved, no proxy fallback route
│       ├── presence.service.ts  presence.service.spec.ts      # moved verbatim (full read+write)
│       ├── internal-chat.client.ts  internal-chat.client.spec.ts  # NEW
│       ├── message-subscriber.service.ts  message-subscriber.service.spec.ts  # NEW
│       └── events.gateway.ts  events.gateway.spec.ts          # NEW (rewritten against internal-chat.client)
└── test/                                 # gateway's own self-contained test helpers
    ├── jest.env.ts
    ├── helpers.ts                        # copied from root test/helpers.ts
    ├── redis-cleanup.ts                  # copied from root test/redis-cleanup.ts
    ├── jwt.ts                            # copied from root test/jwt.ts
    └── chat-stub.ts                      # NEW — real minimal HTTP server standing in for chat-service

src/                                      # chat-service — modified
├── auth/
│   ├── internal-auth.guard.ts  internal-auth.guard.spec.ts           # NEW
│   └── jwt-or-internal-auth.guard.ts  jwt-or-internal-auth.guard.spec.ts  # NEW
├── realtime/
│   ├── presence.service.ts               # TRIMMED to isOnline/onlineAmong only
│   ├── presence.service.spec.ts          # TRIMMED to the 3 read-path tests
│   ├── presence.controller.ts            # unchanged
│   ├── presence.controller.spec.ts       # REWRITTEN (seeds Redis directly, no real sockets)
│   └── presence.module.ts                # renamed from realtime.module.ts, trimmed
│   # DELETED: events.gateway.ts, events.gateway.spec.ts, ws-ticket.controller.ts,
│   #          ws-ticket.controller.spec.ts, ws-ticket.service.ts, ws-ticket.service.spec.ts,
│   #          multi-instance.spec.ts, presence.lifecycle.spec.ts, realtime.module.ts
└── rooms/
    ├── rooms.service.ts                  # + isMember(), sendMessage() publishes to Redis
    ├── rooms.controller.ts               # sendMessage route guard -> JwtOrInternalAuthGuard
    ├── internal.controller.ts  internal.controller.spec.ts     # NEW

web/
├── lib/gateway.ts                        # NEW, replaces lib/nest.ts + lib/auth.ts
├── lib/nest-fetch.ts                     # MODIFIED — GATEWAY_URL only
├── middleware.ts                         # MODIFIED — GATEWAY_URL for refresh
├── app/api/**/route.ts                   # MODIFIED — import gateway.ts instead of nest.ts/auth.ts
└── app/chat/page.tsx, chat-client.tsx    # MODIFIED — GATEWAY_URL / NEXT_PUBLIC_GATEWAY_URL

docker-compose.yml                        # unchanged (gateway needs no new container)
tsconfig.json / tsconfig.build.json       # + "gateway" to exclude list
```

---

## Part A — chat-service: internal API surface

### Task 1: `RoomsService.isMember()` (TDD)

**Files:**
- Modify: `src/rooms/rooms.service.ts`, `src/rooms/rooms.service.spec.ts`

**Interfaces:**
- Produces: `RoomsService.isMember(userId: string, roomId: string): Promise<boolean>` — consumed by Task 4's `InternalController` and reused inside `assertMember()`.

- [ ] **Step 1: Write the failing test**

Add to `src/rooms/rooms.service.spec.ts`, inside the existing `describe('RoomsService', ...)` block (reuse whatever user/room factory helpers that file already has for `createRoom`/`assertMember` tests):

```typescript
describe('isMember', () => {
  it('returns true for a member and false for a non-member', async () => {
    const owner = randomUUID();
    const outsider = randomUUID();
    const room = await service.createRoom(owner, { name: 'IsMember' });

    expect(await service.isMember(owner, room.id)).toBe(true);
    expect(await service.isMember(outsider, room.id)).toBe(false);
  });

  it('returns false for a non-existent room', async () => {
    expect(await service.isMember(randomUUID(), 'does-not-exist')).toBe(false);
  });
});
```

(Match the exact variable names already used by the surrounding tests in this file — e.g. if the service instance is bound to a different local name than `service`, use that name instead.)

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- rooms.service`
Expected: FAIL — `service.isMember is not a function`.

- [ ] **Step 3: Write minimal implementation**

In `src/rooms/rooms.service.ts`, replace the existing `assertMember` method with:

```typescript
async isMember(userId: string, roomId: string): Promise<boolean> {
  const membership = await this.prisma.roomMember.findUnique({
    where: { roomId_userId: { roomId, userId } },
  });
  return membership !== null;
}

async assertMember(userId: string, roomId: string): Promise<void> {
  if (!(await this.isMember(userId, roomId))) {
    throw new ForbiddenException('Not a member of this room');
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- rooms.service`
Expected: PASS, all existing tests plus the 2 new ones (no behavior change to `assertMember`'s exceptions/messages).

- [ ] **Step 5: Commit**

```bash
git add src/rooms/rooms.service.ts src/rooms/rooms.service.spec.ts
git commit -m "feat: add RoomsService.isMember, reuse it inside assertMember"
```

---

### Task 2: `InternalAuthGuard` (TDD)

**Files:**
- Create: `src/auth/internal-auth.guard.ts`, `src/auth/internal-auth.guard.spec.ts`

**Interfaces:**
- Consumes: env `INTERNAL_SECRET`.
- Produces: `InternalAuthGuard` (implements `CanActivate`) — consumed by Task 4's `InternalController`.

- [ ] **Step 1: Write the failing test**

```typescript
// src/auth/internal-auth.guard.spec.ts
import { ExecutionContext, UnauthorizedException } from '@nestjs/common';
import { InternalAuthGuard } from './internal-auth.guard';

function contextWith(headers: Record<string, string>): ExecutionContext {
  return {
    switchToHttp: () => ({
      getRequest: () => ({ headers }),
    }),
  } as unknown as ExecutionContext;
}

describe('InternalAuthGuard', () => {
  const guard = new InternalAuthGuard();
  const OLD_ENV = process.env.INTERNAL_SECRET;

  beforeEach(() => {
    process.env.INTERNAL_SECRET = 'test-internal-secret';
  });

  afterAll(() => {
    process.env.INTERNAL_SECRET = OLD_ENV;
  });

  it('allows a request with the correct secret', () => {
    expect(guard.canActivate(contextWith({ 'x-internal-secret': 'test-internal-secret' }))).toBe(
      true,
    );
  });

  it('rejects a request with the wrong secret', () => {
    expect(() => guard.canActivate(contextWith({ 'x-internal-secret': 'wrong' }))).toThrow(
      UnauthorizedException,
    );
  });

  it('rejects a request with no secret header', () => {
    expect(() => guard.canActivate(contextWith({}))).toThrow(UnauthorizedException);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- internal-auth.guard`
Expected: FAIL — `Cannot find module './internal-auth.guard'`.

- [ ] **Step 3: Write minimal implementation**

```typescript
// src/auth/internal-auth.guard.ts
import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';

@Injectable()
export class InternalAuthGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const req = context.switchToHttp().getRequest();
    const secret = req.headers['x-internal-secret'];
    if (!secret || secret !== process.env.INTERNAL_SECRET) {
      throw new UnauthorizedException('Invalid internal secret');
    }
    return true;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- internal-auth.guard`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add src/auth/internal-auth.guard.ts src/auth/internal-auth.guard.spec.ts
git commit -m "feat: add InternalAuthGuard for gateway-only chat-service endpoints"
```

---

### Task 3: `JwtOrInternalAuthGuard` (TDD)

**Files:**
- Create: `src/auth/jwt-or-internal-auth.guard.ts`, `src/auth/jwt-or-internal-auth.guard.spec.ts`

**Interfaces:**
- Consumes: `InternalAuthGuard`'s secret check logic (inlined, not reused by composition, to keep this guard self-contained and directly testable), `JwtAuthGuard` (already exists at `src/auth/jwt-auth.guard.ts`).
- Produces: `JwtOrInternalAuthGuard` — consumed by Task 5 (`rooms.controller.ts`'s `sendMessage` route).

- [ ] **Step 1: Write the failing test**

```typescript
// src/auth/jwt-or-internal-auth.guard.spec.ts
import { ExecutionContext, UnauthorizedException } from '@nestjs/common';
import { JwtOrInternalAuthGuard } from './jwt-or-internal-auth.guard';
import { JwtAuthGuard } from './jwt-auth.guard';

function contextWith(headers: Record<string, string>): ExecutionContext {
  const req: { headers: Record<string, string>; user?: { id: string } } = { headers };
  return {
    switchToHttp: () => ({ getRequest: () => req }),
  } as unknown as ExecutionContext;
}

describe('JwtOrInternalAuthGuard', () => {
  const OLD_ENV = process.env.INTERNAL_SECRET;

  beforeEach(() => {
    process.env.INTERNAL_SECRET = 'test-internal-secret';
  });

  afterAll(() => {
    process.env.INTERNAL_SECRET = OLD_ENV;
  });

  it('accepts a valid internal secret + user id, and sets req.user', async () => {
    const jwtGuard = { canActivate: jest.fn() } as unknown as JwtAuthGuard;
    const guard = new JwtOrInternalAuthGuard(jwtGuard);
    const context = contextWith({
      'x-internal-secret': 'test-internal-secret',
      'x-internal-user-id': 'user-123',
    });

    expect(await guard.canActivate(context)).toBe(true);
    expect(context.switchToHttp().getRequest().user).toEqual({ id: 'user-123' });
    expect(jwtGuard.canActivate).not.toHaveBeenCalled();
  });

  it('rejects a valid internal secret with no X-Internal-User-Id', async () => {
    const jwtGuard = { canActivate: jest.fn() } as unknown as JwtAuthGuard;
    const guard = new JwtOrInternalAuthGuard(jwtGuard);
    const context = contextWith({ 'x-internal-secret': 'test-internal-secret' });

    await expect(guard.canActivate(context)).rejects.toThrow(UnauthorizedException);
  });

  it('falls back to the JWT guard when there is no internal secret', async () => {
    const jwtGuard = { canActivate: jest.fn().mockResolvedValue(true) } as unknown as JwtAuthGuard;
    const guard = new JwtOrInternalAuthGuard(jwtGuard);
    const context = contextWith({ authorization: 'Bearer some.jwt.token' });

    expect(await guard.canActivate(context)).toBe(true);
    expect(jwtGuard.canActivate).toHaveBeenCalledWith(context);
  });

  it('falls back to the JWT guard (and its rejection) on a wrong internal secret', async () => {
    const jwtGuard = {
      canActivate: jest.fn().mockRejectedValue(new UnauthorizedException()),
    } as unknown as JwtAuthGuard;
    const guard = new JwtOrInternalAuthGuard(jwtGuard);
    const context = contextWith({ 'x-internal-secret': 'wrong' });

    await expect(guard.canActivate(context)).rejects.toThrow(UnauthorizedException);
  });
});
```

This test mocks `JwtAuthGuard` itself (a thin passport wrapper with no business logic of its own — Phase 1 already covers real JWT verification end-to-end elsewhere) rather than Prisma/Redis/HTTP, so it stays within the "don't mock real I/O" rule.

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- jwt-or-internal-auth.guard`
Expected: FAIL — `Cannot find module './jwt-or-internal-auth.guard'`.

- [ ] **Step 3: Write minimal implementation**

```typescript
// src/auth/jwt-or-internal-auth.guard.ts
import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtAuthGuard } from './jwt-auth.guard';

@Injectable()
export class JwtOrInternalAuthGuard implements CanActivate {
  constructor(private readonly jwtGuard: JwtAuthGuard) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest();
    const secret = req.headers['x-internal-secret'];

    if (secret === process.env.INTERNAL_SECRET) {
      const userId = req.headers['x-internal-user-id'];
      if (!userId || typeof userId !== 'string') {
        throw new UnauthorizedException('Missing X-Internal-User-Id');
      }
      req.user = { id: userId };
      return true;
    }

    return this.jwtGuard.canActivate(context) as Promise<boolean>;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- jwt-or-internal-auth.guard`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add src/auth/jwt-or-internal-auth.guard.ts src/auth/jwt-or-internal-auth.guard.spec.ts
git commit -m "feat: add JwtOrInternalAuthGuard (dual JWT/internal auth for sendMessage)"
```

---

### Task 4: `InternalController` — 2 internal endpoints (TDD)

**Files:**
- Create: `src/rooms/internal.controller.ts`, `src/rooms/internal.controller.spec.ts`
- Modify: `src/rooms/rooms.module.ts`

**Interfaces:**
- Consumes: `RoomsService.isMember()` (Task 1), `RoomsService.listRoomIdsOf()` (already exists), `InternalAuthGuard` (Task 2).
- Produces: `GET /internal/rooms/:roomId/members/:userId` → `{isMember: boolean}`; `GET /internal/users/:userId/room-ids` → `{roomIds: string[]}` — consumed by gateway's `internal-chat.client.ts` (Task 16).

- [ ] **Step 1: Write the failing test**

```typescript
// src/rooms/internal.controller.spec.ts
import { randomUUID } from 'crypto';
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import { RoomsService } from './rooms.service';
import { PrismaService } from '../prisma/prisma.service';
import { InternalController } from './internal.controller';
import { InternalAuthGuard } from '../auth/internal-auth.guard';
import { cleanDatabase } from '../../test/db-cleanup';

describe('InternalController', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let rooms: RoomsService;
  let baseUrl: string;
  const OLD_SECRET = process.env.INTERNAL_SECRET;

  beforeAll(async () => {
    process.env.INTERNAL_SECRET = 'test-internal-secret';
    const moduleRef = await Test.createTestingModule({
      controllers: [InternalController],
      providers: [RoomsService, PrismaService, InternalAuthGuard],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;

    prisma = moduleRef.get(PrismaService);
    rooms = moduleRef.get(RoomsService);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await app.close();
    process.env.INTERNAL_SECRET = OLD_SECRET;
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  function get(path: string, secret?: string) {
    return fetch(`${baseUrl}${path}`, {
      headers: secret ? { 'x-internal-secret': secret } : {},
    });
  }

  it('rejects both endpoints without the internal secret', async () => {
    expect((await get('/internal/rooms/any/members/any')).status).toBe(401);
    expect((await get('/internal/users/any/room-ids')).status).toBe(401);
  });

  it('GET /internal/rooms/:roomId/members/:userId reports membership correctly', async () => {
    const owner = randomUUID();
    const outsider = randomUUID();
    const room = await rooms.createRoom(owner, { name: 'Internal' });

    const memberRes = await get(`/internal/rooms/${room.id}/members/${owner}`, 'test-internal-secret');
    expect(await memberRes.json()).toEqual({ isMember: true });

    const outsiderRes = await get(
      `/internal/rooms/${room.id}/members/${outsider}`,
      'test-internal-secret',
    );
    expect(await outsiderRes.json()).toEqual({ isMember: false });
  });

  it('GET /internal/users/:userId/room-ids returns every room the user belongs to', async () => {
    const user = randomUUID();
    const roomA = await rooms.createRoom(user, { name: 'A' });
    const roomB = await rooms.createRoom(user, { name: 'B' });

    const res = await get(`/internal/users/${user}/room-ids`, 'test-internal-secret');
    const body = (await res.json()) as { roomIds: string[] };
    expect(body.roomIds.sort()).toEqual([roomA.id, roomB.id].sort());
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- internal.controller`
Expected: FAIL — `Cannot find module './internal.controller'`.

- [ ] **Step 3: Write minimal implementation**

```typescript
// src/rooms/internal.controller.ts
import { Controller, Get, Param, UseGuards } from '@nestjs/common';
import { InternalAuthGuard } from '../auth/internal-auth.guard';
import { RoomsService } from './rooms.service';

@Controller('internal')
@UseGuards(InternalAuthGuard)
export class InternalController {
  constructor(private readonly roomsService: RoomsService) {}

  @Get('rooms/:roomId/members/:userId')
  async checkMembership(
    @Param('roomId') roomId: string,
    @Param('userId') userId: string,
  ): Promise<{ isMember: boolean }> {
    return { isMember: await this.roomsService.isMember(userId, roomId) };
  }

  @Get('users/:userId/room-ids')
  async roomIds(@Param('userId') userId: string): Promise<{ roomIds: string[] }> {
    return { roomIds: await this.roomsService.listRoomIdsOf(userId) };
  }
}
```

Add it to `src/rooms/rooms.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { RoomsService } from './rooms.service';
import { RoomsController } from './rooms.controller';
import { InternalController } from './internal.controller';

@Module({
  imports: [PassportModule],
  controllers: [RoomsController, InternalController],
  providers: [RoomsService],
  exports: [RoomsService],
})
export class RoomsModule {}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- internal.controller`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add src/rooms/internal.controller.ts src/rooms/internal.controller.spec.ts src/rooms/rooms.module.ts
git commit -m "feat: add internal membership/room-ids endpoints for gateway"
```

---

### Task 5: `sendMessage` dual guard + Redis publish (TDD)

**Files:**
- Modify: `src/rooms/rooms.controller.ts`, `src/rooms/rooms.service.ts`, `src/rooms/rooms.service.spec.ts`

**Interfaces:**
- Consumes: `REDIS` token (`src/redis/redis.constants.ts`, already global), `JwtOrInternalAuthGuard` (Task 3).
- Produces: `RoomsService.sendMessage()` now publishes to Redis channel `chat:message:new` — consumed by gateway's `MessageSubscriberService` (Task 19). Route `POST /rooms/:roomId/messages` now accepts either a real JWT or internal headers.

- [ ] **Step 1: Write the failing test**

Add to `src/rooms/rooms.service.spec.ts` (reuse the existing `PrismaService` instance already wired in that file's `beforeAll`):

```typescript
import Redis from 'ioredis';
// ...alongside the file's other imports

describe('sendMessage Redis publish', () => {
  let redis: Redis;

  beforeAll(() => {
    redis = new Redis(process.env.REDIS_URL!);
  });

  afterAll(async () => {
    await redis.quit();
  });

  it('publishes {roomId, message} on chat:message:new after persisting', async () => {
    const owner = randomUUID();
    const room = await service.createRoom(owner, { name: 'PubSub' });

    const sub = redis.duplicate();
    await sub.subscribe('chat:message:new');
    const received = new Promise<{ roomId: string; message: { content: string } }>((resolve) => {
      sub.on('message', (_channel, raw) => resolve(JSON.parse(raw)));
    });

    const message = await service.sendMessage(owner, room.id, { content: 'hello pubsub' });

    const payload = await received;
    expect(payload.roomId).toBe(room.id);
    expect(payload.message.content).toBe('hello pubsub');
    expect(payload.message).toMatchObject({ id: message.id });

    await sub.unsubscribe('chat:message:new');
    await sub.quit();
  });
});
```

(Use whatever local variable name the file's existing `beforeAll` already binds the `RoomsService` instance to, matching the convention used by the `isMember`/`createRoom` tests added in Task 1.)

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- rooms.service`
Expected: FAIL — the `received` promise never resolves (times out), because `sendMessage` doesn't publish yet.

- [ ] **Step 3: Write minimal implementation**

In `src/rooms/rooms.service.ts`, inject Redis and publish after creating the message:

```typescript
import { ForbiddenException, Inject, Injectable, NotFoundException } from '@nestjs/common';
import { Message, Room, RoomMember } from '@prisma/client';
import Redis from 'ioredis';
import { PrismaService } from '../prisma/prisma.service';
import { REDIS } from '../redis/redis.constants';
import { CreateRoomDto } from './dto/create-room.dto';
import { SendMessageDto } from './dto/send-message.dto';

@Injectable()
export class RoomsService {
  constructor(
    private readonly prisma: PrismaService,
    @Inject(REDIS) private readonly redis: Redis,
  ) {}

  // ...createRoom / joinRoom / listRooms / listMemberIds / listRoomIdsOf / isMember / assertMember unchanged...

  async sendMessage(userId: string, roomId: string, dto: SendMessageDto): Promise<Message> {
    await this.assertMember(userId, roomId);
    const message = await this.prisma.message.create({
      data: { roomId, senderId: userId, content: dto.content },
    });
    await this.redis.publish('chat:message:new', JSON.stringify({ roomId, message }));
    return message;
  }

  // ...getMessages unchanged...
}
```

(Keep every other method's body exactly as it already is — only the constructor and `sendMessage` change.)

Now update the route guard in `src/rooms/rooms.controller.ts` — only `sendMessage` changes:

```typescript
import { Body, Controller, Get, Param, Post, Query, Req, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { JwtOrInternalAuthGuard } from '../auth/jwt-or-internal-auth.guard';
import { RoomsService } from './rooms.service';
import { CreateRoomDto } from './dto/create-room.dto';
import { SendMessageDto } from './dto/send-message.dto';

@Controller('rooms')
@UseGuards(JwtAuthGuard)
export class RoomsController {
  constructor(private readonly roomsService: RoomsService) {}

  @Post()
  createRoom(@Req() req: { user: { id: string } }, @Body() dto: CreateRoomDto) {
    return this.roomsService.createRoom(req.user.id, dto);
  }

  @Post(':roomId/join')
  joinRoom(@Req() req: { user: { id: string } }, @Param('roomId') roomId: string) {
    return this.roomsService.joinRoom(req.user.id, roomId);
  }

  @Get()
  listRooms(@Req() req: { user: { id: string } }) {
    return this.roomsService.listRooms(req.user.id);
  }

  @Get(':roomId/messages')
  getMessages(
    @Req() req: { user: { id: string } },
    @Param('roomId') roomId: string,
    @Query('cursor') cursor?: string,
    @Query('limit') limit?: string,
  ) {
    const parsedLimit = limit ? parseInt(limit, 10) : 20;
    return this.roomsService.getMessages(req.user.id, roomId, cursor, parsedLimit);
  }

  @Post(':roomId/messages')
  @UseGuards(JwtOrInternalAuthGuard)
  sendMessage(
    @Req() req: { user: { id: string } },
    @Param('roomId') roomId: string,
    @Body() dto: SendMessageDto,
  ) {
    return this.roomsService.sendMessage(req.user.id, roomId, dto);
  }
}
```

`JwtOrInternalAuthGuard` needs `JwtAuthGuard` injected — Nest resolves this automatically as long as `JwtAuthGuard` is a provider somewhere reachable (it already is, via `AuthModule`, which `AppModule` imports globally-ish — if Nest complains about `JwtOrInternalAuthGuard` not finding a `JwtAuthGuard` provider in `RoomsModule`'s scope, add `JwtOrInternalAuthGuard` to `RoomsModule`'s `providers` array and import `AuthModule` into `RoomsModule`; verify by running the step below before deciding this is needed).

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- rooms.service rooms.controller`
Expected: PASS. If `JwtOrInternalAuthGuard` fails to resolve `JwtAuthGuard` as a dependency, add to `src/rooms/rooms.module.ts`:

```typescript
import { AuthModule } from '../auth/auth.module';
import { JwtOrInternalAuthGuard } from '../auth/jwt-or-internal-auth.guard';
// ...
@Module({
  imports: [PassportModule, AuthModule],
  controllers: [RoomsController, InternalController],
  providers: [RoomsService, JwtOrInternalAuthGuard],
  exports: [RoomsService],
})
export class RoomsModule {}
```

- [ ] **Step 5: Verify the existing REST `sendMessage` test (JWT path) still passes untouched**

Run: `npm test -- rooms.controller`
Expected: PASS (no new test needed here — this just confirms the guard swap didn't break the real-JWT path already covered elsewhere, e.g. in `multi-instance.spec.ts` or wherever `POST /rooms/:id/messages` is exercised over real HTTP with a signed token today).

- [ ] **Step 6: Commit**

```bash
git add src/rooms/rooms.service.ts src/rooms/rooms.service.spec.ts src/rooms/rooms.controller.ts src/rooms/rooms.module.ts
git commit -m "feat: publish chat:message:new on send, accept internal auth for sendMessage"
```

---

### Task 6: Trim chat-service's `PresenceService` to read-only (TDD)

**Files:**
- Modify: `src/realtime/presence.service.ts`, `src/realtime/presence.service.spec.ts`

**Interfaces:**
- Produces: `PresenceService.isOnline(userId): Promise<boolean>`, `PresenceService.onlineAmong(userIds): Promise<string[]>` — same signatures as before, consumed unchanged by `PresenceController` (Task 6 doesn't touch the controller). `connect`/`disconnect`/`touch` are REMOVED from this copy (they move to gateway's own full copy in Task 13).

- [ ] **Step 1: Replace the spec file with only the read-path tests**

Replace `src/realtime/presence.service.spec.ts` entirely:

```typescript
import Redis from 'ioredis';
import { PresenceService } from './presence.service';
import { cleanRedis } from '../../test/redis-cleanup';

describe('PresenceService (read-only, chat-service)', () => {
  let redis: Redis;
  let service: PresenceService;

  beforeAll(() => {
    redis = new Redis(process.env.REDIS_URL!);
    service = new PresenceService(redis);
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

  it('ignores an orphaned socketId (no heartbeat key) so the user is not "online" forever', async () => {
    await redis.sadd('presence:user:u1', 'ghost-socket'); // instance crashed, key never removed
    expect(await service.isOnline('u1')).toBe(false);
    expect(await redis.scard('presence:user:u1')).toBe(0); // lazily pruned
  });

  it('onlineAmong returns only online users and handles an empty list', async () => {
    await redis.sadd('presence:user:u1', 'sock-a');
    await redis.set('presence:socket:sock-a', 'u1', 'EX', 60);
    await redis.sadd('presence:user:u3', 'sock-c');
    await redis.set('presence:socket:sock-c', 'u3', 'EX', 60);

    expect((await service.onlineAmong(['u1', 'u2', 'u3'])).sort()).toEqual(['u1', 'u3']);
    expect(await service.onlineAmong([])).toEqual([]);
  });
});
```

Note this version seeds Redis directly (`SADD`/`SET`) instead of calling `service.connect(...)`, since `connect` no longer exists on this trimmed service — it simulates exactly what gateway's `connect()` would have written.

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- presence.service`
Expected: FAIL — `new PresenceService(redis)` signature mismatch (current constructor takes `(redis, options)` via DI tokens) and `isOnline`/`onlineAmong` still require the full original class shape, so this will fail to compile/run until Step 3.

- [ ] **Step 3: Write the trimmed implementation**

Replace `src/realtime/presence.service.ts` entirely:

```typescript
import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS } from '../redis/redis.constants';

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
  presenceIsOnline(userKey: string): Promise<number>;
}

const userKey = (userId: string) => `presence:user:${userId}`;

@Injectable()
export class PresenceService implements OnModuleInit {
  private readonly client: PresenceRedis;

  constructor(@Inject(REDIS) redis: Redis) {
    this.client = redis as PresenceRedis;
  }

  onModuleInit(): void {
    this.client.defineCommand('presenceIsOnline', {
      numberOfKeys: 1,
      lua: `${PRUNE_LUA}
if prune(KEYS[1]) > 0 then return 1 else return 0 end`,
    });
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
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add src/realtime/presence.service.ts src/realtime/presence.service.spec.ts
git commit -m "refactor: trim chat-service PresenceService to read-only (onlineAmong/isOnline)"
```

---

### Task 7: Rewrite `presence.controller.spec.ts` without real sockets (TDD)

**Files:**
- Modify: `src/realtime/presence.controller.spec.ts`
- (No change needed to `src/realtime/presence.controller.ts` itself — its code is unchanged.)

**Interfaces:**
- Consumes: `PresenceController` (unchanged), `PresenceService.onlineAmong` (Task 6's trimmed version).

- [ ] **Step 1: Replace the spec file**

The existing version uses `createInstance()` (full app boot) and real WebSocket connections to go "online" — that no longer works once `createInstance()`'s `AppModule` has no `EventsGateway` (Task 9 removes it). Replace `src/realtime/presence.controller.spec.ts` entirely:

```typescript
import { randomUUID } from 'crypto';
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { PresenceController } from './presence.controller';
import { PresenceService } from './presence.service';
import { RoomsService } from '../rooms/rooms.service';
import { PrismaService } from '../prisma/prisma.service';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { AuthModule } from '../auth/auth.module';
import { REDIS } from '../redis/redis.constants';
import { cleanDatabase } from '../../test/db-cleanup';
import { cleanRedis } from '../../test/redis-cleanup';
import { signToken, acquireJwksServer, releaseJwksServer } from '../../test/jwt';

describe('GET /rooms/:roomId/presence', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let rooms: RoomsService;
  let redis: Redis;
  let baseUrl: string;

  beforeAll(async () => {
    await acquireJwksServer();
    const moduleRef = await Test.createTestingModule({
      imports: [AuthModule],
      controllers: [PresenceController],
      providers: [
        RoomsService,
        PrismaService,
        PresenceService,
        { provide: REDIS, useFactory: () => new Redis(process.env.REDIS_URL!) },
        JwtAuthGuard,
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;

    prisma = moduleRef.get(PrismaService);
    rooms = moduleRef.get(RoomsService);
    redis = moduleRef.get(REDIS);
  });

  afterAll(async () => {
    await app.close();
    await redis.quit();
    await releaseJwksServer();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
    await cleanRedis(redis);
  });

  async function goOnline(userId: string) {
    const socketId = randomUUID();
    await redis.sadd(`presence:user:${userId}`, socketId);
    await redis.set(`presence:socket:${socketId}`, userId, 'EX', 60);
  }

  function get(roomId: string, token?: string) {
    return fetch(`${baseUrl}/rooms/${roomId}/presence`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  }

  it('returns 401 without a token', async () => {
    expect((await get('any-room')).status).toBe(401);
  });

  it('returns 403 when the caller is not a member of the room', async () => {
    const owner = randomUUID();
    const outsider = randomUUID();
    const room = await rooms.createRoom(owner, { name: 'Private' });

    expect((await get(room.id, signToken(outsider))).status).toBe(403);
  });

  it('lists only the members that are currently online', async () => {
    const me = randomUUID();
    const online = randomUUID();
    const offline = randomUUID();
    const room = await rooms.createRoom(me, { name: 'Team' });
    await rooms.joinRoom(online, room.id);
    await rooms.joinRoom(offline, room.id);
    await goOnline(online);

    const res = await get(room.id, signToken(me));

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ onlineUserIds: [online] });
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- presence.controller`
Expected: FAIL initially for the same reason as Task 6 (`PresenceService` constructor shape) if run before Task 6's commit lands — since Task 6 already landed by the time this task runs, the actual expected failure here is just "file doesn't compile because of a typo/missing import" the first time you save it; if it compiles cleanly and passes immediately, that's fine too — the point of this task is the rewrite itself, not a strict RED/GREEN cycle on top of already-passing infrastructure. Run once, read the output, fix any import mistakes, then proceed.

- [ ] **Step 3: Run test to verify it passes**

Run: `npm test -- presence.controller`
Expected: PASS, 3 tests.

- [ ] **Step 4: Commit**

```bash
git add src/realtime/presence.controller.spec.ts
git commit -m "test: rewrite presence.controller.spec without real WS connections"
```

---

### Task 8: Delete chat-service's WS files, rename module (scaffold — no TDD)

**Files:**
- Delete: `src/realtime/events.gateway.ts`, `src/realtime/events.gateway.spec.ts`, `src/realtime/ws-ticket.controller.ts`, `src/realtime/ws-ticket.controller.spec.ts`, `src/realtime/ws-ticket.service.ts`, `src/realtime/ws-ticket.service.spec.ts`, `src/realtime/multi-instance.spec.ts`, `src/realtime/presence.lifecycle.spec.ts`, `src/realtime/realtime.module.ts`, `src/redis/redis-io.adapter.ts`, `src/redis/redis-io.adapter.spec.ts`
- Create: `src/realtime/presence.module.ts`
- Modify: `src/app.module.ts`, `src/main.ts`, `package.json`

**Interfaces:**
- Produces: `PresenceModule` (replaces `RealtimeModule` in `AppModule`'s imports) — exports nothing (chat-service doesn't need `PresenceService` outside this module; only `PresenceController` uses it internally).

- [ ] **Step 1: Delete the WS-only files**

```bash
git rm src/realtime/events.gateway.ts src/realtime/events.gateway.spec.ts \
  src/realtime/ws-ticket.controller.ts src/realtime/ws-ticket.controller.spec.ts \
  src/realtime/ws-ticket.service.ts src/realtime/ws-ticket.service.spec.ts \
  src/realtime/multi-instance.spec.ts src/realtime/presence.lifecycle.spec.ts \
  src/realtime/realtime.module.ts \
  src/redis/redis-io.adapter.ts src/redis/redis-io.adapter.spec.ts
```

These Socket.IO-specific files (events gateway, ws-ticket, the Redis Socket.IO adapter, and the WS-dependent presence/multi-instance specs) move to `gateway/` in Part B-D of this plan — they are NOT lost, just relocated with a rewrite against the new internal-HTTP-client shape.

- [ ] **Step 2: Create `src/realtime/presence.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { RoomsModule } from '../rooms/rooms.module';
import { PresenceController } from './presence.controller';
import { PresenceService } from './presence.service';

@Module({
  imports: [RoomsModule, PassportModule],
  controllers: [PresenceController],
  providers: [PresenceService],
})
export class PresenceModule {}
```

- [ ] **Step 3: Update `src/app.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from './prisma/prisma.module';
import { AuthModule } from './auth/auth.module';
import { RoomsModule } from './rooms/rooms.module';
import { PresenceModule } from './realtime/presence.module';
import { RedisModule } from './redis/redis.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    RedisModule,
    PrismaModule,
    AuthModule,
    RoomsModule,
    PresenceModule,
  ],
})
export class AppModule {}
```

- [ ] **Step 4: Simplify `src/main.ts`** (no more Socket.IO adapter — chat-service is plain HTTP now)

```typescript
import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.enableShutdownHooks();
  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
```

- [ ] **Step 5: Remove now-unused Socket.IO dependencies from `package.json`**

Remove these entries (gateway now owns them): `@nestjs/platform-socket.io`, `@nestjs/websockets`, `@socket.io/redis-adapter`, `socket.io`, `socket.io-client` (the last is a devDependency). Keep `ioredis` (still needed for `RoomsService`'s publish and `PresenceService`'s reads).

- [ ] **Step 6: Reinstall and verify**

Run: `npm install && npm run typecheck && npm test`
Expected: `npm install` exits 0 (package-lock updates); typecheck clean; full suite passes (count will be lower than before — the moved specs are gone from this package).

- [ ] **Step 7: Commit**

```bash
git add -A src/realtime src/redis src/app.module.ts src/main.ts package.json package-lock.json
git commit -m "refactor: remove WebSocket layer from chat-service (moved to gateway/)"
```

---

## Part B — gateway scaffold

### Task 9: Scaffold `gateway/` project (no TDD — scaffolding)

**Files:**
- Create: `gateway/package.json`, `gateway/tsconfig.json`, `gateway/tsconfig.build.json`, `gateway/nest-cli.json`, `gateway/jest.config.js`, `gateway/src/main.ts`, `gateway/src/app.module.ts`, `gateway/test/jest.env.ts`
- Modify: `tsconfig.json`, `tsconfig.build.json` (repo root)

**Interfaces:**
- Produces: a runnable empty NestJS app on port 3020, with its own Jest test runner wired to its own `.env.test`.

- [ ] **Step 1: Create `gateway/package.json`**

```json
{
  "name": "realtime-chat-gateway",
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
    "@nestjs/axios": "^3.1.3",
    "@nestjs/common": "^10.4.0",
    "@nestjs/config": "^3.3.0",
    "@nestjs/core": "^10.4.0",
    "@nestjs/passport": "^10.0.3",
    "@nestjs/platform-express": "^10.4.0",
    "@nestjs/platform-socket.io": "^10.4.0",
    "@nestjs/websockets": "^10.4.0",
    "@socket.io/redis-adapter": "^8.3.0",
    "axios": "^1.7.9",
    "class-transformer": "^0.5.1",
    "class-validator": "^0.14.1",
    "ioredis": "^6.0.0",
    "passport": "^0.7.0",
    "passport-jwt": "^4.0.1",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1",
    "socket.io": "^4.7.5"
  },
  "devDependencies": {
    "@nestjs/cli": "^10.4.5",
    "@nestjs/jwt": "^10.2.0",
    "@nestjs/testing": "^10.4.0",
    "@types/express": "^4.17.21",
    "@types/jest": "^29.5.13",
    "@types/node": "^20.16.0",
    "@types/passport-jwt": "^4.0.1",
    "dotenv": "^16.4.5",
    "jest": "^29.7.0",
    "socket.io-client": "^4.7.5",
    "ts-jest": "^29.2.5",
    "ts-node": "^10.9.2",
    "typescript": "^5.6.2"
  }
}
```

- [ ] **Step 2: Create `gateway/tsconfig.json`**

```json
{
  "compilerOptions": {
    "module": "commonjs",
    "declaration": false,
    "removeComments": true,
    "emitDecoratorMetadata": true,
    "experimentalDecorators": true,
    "allowSyntheticDefaultImports": true,
    "target": "ES2021",
    "sourceMap": true,
    "outDir": "./dist",
    "baseUrl": "./",
    "incremental": true,
    "skipLibCheck": true,
    "strictNullChecks": true,
    "noImplicitAny": true,
    "strictBindCallApply": true,
    "forceConsistentCasingInFileNames": true,
    "noFallthroughCasesInSwitch": true
  },
  "exclude": ["node_modules", "dist"]
}
```

- [ ] **Step 3: Create `gateway/tsconfig.build.json`**

```json
{
  "extends": "./tsconfig.json",
  "exclude": ["node_modules", "dist", "**/*spec.ts", "test"]
}
```

- [ ] **Step 4: Create `gateway/nest-cli.json`**

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src"
}
```

- [ ] **Step 5: Create `gateway/jest.config.js`**

```javascript
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  rootDir: '.',
  roots: ['<rootDir>/src'],
  testRegex: '.*\\.spec\\.ts$',
  setupFiles: ['<rootDir>/test/jest.env.ts'],
  testTimeout: 15000,
  maxWorkers: 1,
};
```

- [ ] **Step 6: Create `gateway/test/jest.env.ts`**

```typescript
import { config } from 'dotenv';
import { resolve } from 'path';

config({ path: resolve(__dirname, '../.env.test') });

process.env.REDIS_URL ??= 'redis://localhost:6379/1';
```

- [ ] **Step 7: Create `gateway/src/app.module.ts`** (empty for now, filled in by later tasks)

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true })],
})
export class AppModule {}
```

- [ ] **Step 8: Create `gateway/src/main.ts`**

```typescript
import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.enableShutdownHooks();
  await app.listen(process.env.PORT ?? 3020);
}
bootstrap();
```

- [ ] **Step 9: Add `gateway` to the root tsconfig excludes**

In repo-root `tsconfig.json`, change:

```json
  "exclude": ["node_modules", "dist", "web", "auth-service"]
```

to:

```json
  "exclude": ["node_modules", "dist", "web", "auth-service", "gateway"]
```

Same change in repo-root `tsconfig.build.json`.

- [ ] **Step 10: Install and verify it boots**

Run: `cd gateway && npm install && npm run build`. Then start it in the background, hit it with curl, and stop it:

```bash
node dist/main.js &
sleep 1
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3020/
kill %1
```

Expect a `404` (no routes yet, but the process answers HTTP). From repo root, run `npm run typecheck` to confirm the root project still ignores `gateway/` cleanly.

- [ ] **Step 11: Commit**

```bash
git add gateway/package.json gateway/package-lock.json gateway/tsconfig.json gateway/tsconfig.build.json gateway/nest-cli.json gateway/jest.config.js gateway/test/jest.env.ts gateway/src/app.module.ts gateway/src/main.ts tsconfig.json tsconfig.build.json
git commit -m "chore: scaffold gateway service"
```

---

### Task 10: Gateway test helpers (no TDD — infra)

**Files:**
- Create: `gateway/test/helpers.ts`, `gateway/test/redis-cleanup.ts`, `gateway/test/jwt.ts`

**Interfaces:**
- Produces: `sleep`, `connectSocket`, `emitWithAck`, `waitFor` (from `helpers.ts`); `cleanRedis` (from `redis-cleanup.ts`); `generateTestKey`, `defaultKey`, `signToken`, `startJwksServer`, `acquireJwksServer`, `releaseJwksServer` (from `jwt.ts`) — consumed by every gateway test task from here on.

- [ ] **Step 1: Create `gateway/test/helpers.ts`** (identical to the root project's `test/helpers.ts`)

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

- [ ] **Step 2: Create `gateway/test/redis-cleanup.ts`** (identical to root)

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

- [ ] **Step 3: Create `gateway/test/jwt.ts`** (identical to root — gateway needs its own JWKS test infra for its own `JwtAuthGuard`/`JwksService` copy)

```typescript
import { JwtService } from '@nestjs/jwt';
import { createHash, generateKeyPairSync } from 'crypto';
import { createServer } from 'http';
import { AddressInfo } from 'net';

export interface TestKey {
  kid: string;
  privatePem: string;
  publicPem: string;
  jwk: Record<string, string>;
}

export function generateTestKey(): TestKey {
  const { privateKey, publicKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const { kty, n, e } = publicKey.export({ format: 'jwk' }) as {
    kty: string;
    n: string;
    e: string;
  };
  const kid = createHash('sha256').update(JSON.stringify({ e, kty, n })).digest('base64url');
  return {
    kid,
    privatePem: privateKey.export({ type: 'pkcs8', format: 'pem' }) as string,
    publicPem: publicKey.export({ type: 'spki', format: 'pem' }) as string,
    jwk: { kty, n, e, kid, use: 'sig', alg: 'RS256' },
  };
}

export const defaultKey = generateTestKey();

export interface SignTokenOptions {
  key?: TestKey;
  kid?: string;
  issuer?: string;
  audience?: string;
  expiresInSec?: number;
}

/** Signs an RS256 access token like auth-service does. */
export function signToken(userId: string, opts: SignTokenOptions = {}): string {
  const key = opts.key ?? defaultKey;
  return new JwtService().sign(
    { sub: userId },
    {
      privateKey: key.privatePem,
      algorithm: 'RS256',
      keyid: opts.kid ?? key.kid,
      issuer: opts.issuer ?? 'auth-service',
      audience: opts.audience ?? 'realtime-chat',
      expiresIn: opts.expiresInSec ?? 900,
    },
  );
}

export interface JwksServer {
  url: string;
  hits(): number;
  setKeys(keys: TestKey[]): void;
  setDown(down: boolean): void;
  close(): Promise<void>;
}

/** A real HTTP server that serves a JWKS (network-level test infrastructure). */
export async function startJwksServer(initial: TestKey[] = [defaultKey]): Promise<JwksServer> {
  let keys = initial;
  let down = false;
  let hits = 0;

  const server = createServer((req, res) => {
    if (req.url !== '/.well-known/jwks.json') {
      res.statusCode = 404;
      res.end();
      return;
    }
    hits++;
    if (down) {
      res.statusCode = 503;
      res.end();
      return;
    }
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ keys: keys.map((k) => k.jwk) }));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    url: `http://127.0.0.1:${port}/.well-known/jwks.json`,
    hits: () => hits,
    setKeys: (next) => {
      keys = next;
    },
    setDown: (next) => {
      down = next;
    },
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

let shared: { server: JwksServer; refs: number } | undefined;

/** Shared JWKS server for app-level specs; sets AUTH_JWKS_URL for the app under test. */
export async function acquireJwksServer(): Promise<JwksServer> {
  if (!shared) {
    shared = { server: await startJwksServer(), refs: 0 };
  }
  shared.refs++;
  process.env.AUTH_JWKS_URL = shared.server.url;
  return shared.server;
}

export async function releaseJwksServer(): Promise<void> {
  if (!shared) return;
  shared.refs--;
  if (shared.refs <= 0) {
    const { server } = shared;
    shared = undefined;
    await server.close();
  }
}
```

- [ ] **Step 4: Verify it compiles**

Run: `cd gateway && npx tsc --noEmit`
Expected: exits 0 (these files aren't imported by anything yet, but must still type-check standalone).

- [ ] **Step 5: Commit**

```bash
git add gateway/test
git commit -m "chore: add gateway's self-contained test helpers"
```

---

### Task 11: Copy `RedisModule` + `RedisIoAdapter` to gateway (TDD)

**Files:**
- Create: `gateway/src/redis/redis.constants.ts`, `gateway/src/redis/redis.module.ts`, `gateway/src/redis/redis.module.spec.ts`, `gateway/src/redis/redis-io.adapter.ts`, `gateway/src/redis/redis-io.adapter.spec.ts`

**Interfaces:**
- Produces: `REDIS` token, `RedisModule` (global), `RedisIoAdapter` — consumed by every later gateway task needing Redis or Socket.IO.

- [ ] **Step 1: Create `gateway/src/redis/redis.constants.ts`**

```typescript
export const REDIS = Symbol('REDIS');
```

- [ ] **Step 2: Write the failing test** — `gateway/src/redis/redis.module.spec.ts`

```typescript
import { Test } from '@nestjs/testing';
import Redis from 'ioredis';
import { RedisModule } from './redis.module';
import { REDIS } from './redis.constants';

describe('RedisModule', () => {
  it('provides a connected ioredis client and cleans it up on shutdown', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [RedisModule] }).compile();
    const app = moduleRef.createNestApplication();
    app.enableShutdownHooks();
    await app.init();

    const redis = app.get<Redis>(REDIS);
    expect(await redis.ping()).toBe('PONG');

    await app.close();
    expect(redis.status).toBe('end');
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd gateway && npm test -- redis.module`
Expected: FAIL — `Cannot find module './redis.module'`.

- [ ] **Step 4: Create `gateway/src/redis/redis.module.ts`** (identical to chat-service's)

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
    if (this.redis.status === 'end') return;
    const ended = new Promise<void>((resolve) => this.redis.once('end', () => resolve()));
    await this.redis.quit();
    await ended;
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd gateway && npm test -- redis.module`
Expected: PASS, 1 test.

- [ ] **Step 6: Write the failing test for the IO adapter** — `gateway/src/redis/redis-io.adapter.spec.ts`

```typescript
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
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

@WebSocketGateway({ cors: true, transports: ['websocket'] })
class EchoGateway {
  @WebSocketServer() server!: Server;

  @SubscribeMessage('join')
  join(@ConnectedSocket() client: Socket, @MessageBody() room: string) {
    client.join(room);
  }

  @SubscribeMessage('broadcast')
  broadcast(@MessageBody() payload: { room: string; text: string }) {
    this.server.to(payload.room).emit('echo', payload.text);
  }
}

async function boot() {
  const moduleRef = await Test.createTestingModule({
    imports: [RedisModule],
    providers: [EchoGateway],
  }).compile();
  const app = moduleRef.createNestApplication();
  const redis = app.get<Redis>(REDIS);
  app.useWebSocketAdapter(new RedisIoAdapter(app, redis));
  await app.listen(0);
  const port = (app.getHttpServer().address() as AddressInfo).port;
  return { app, url: `http://localhost:${port}` };
}

function connect(url: string): Promise<ClientSocket> {
  return new Promise((resolve, reject) => {
    const socket = io(url, { transports: ['websocket'], reconnection: false, forceNew: true });
    socket.on('connect', () => resolve(socket));
    socket.on('connect_error', reject);
  });
}

describe('RedisIoAdapter', () => {
  it('fans out .to(room).emit() across two separate app instances', async () => {
    const instanceA = await boot();
    const instanceB = await boot();

    const socketOnA = await connect(instanceA.url);
    const socketOnB = await connect(instanceB.url);
    socketOnB.emit('join', 'room-1');
    await sleep(100); // let the join propagate through the adapter

    const received = new Promise<string>((resolve) => socketOnB.once('echo', resolve));
    socketOnA.emit('broadcast', { room: 'room-1', text: 'hello cross-instance' });

    expect(await received).toBe('hello cross-instance');

    socketOnA.disconnect();
    socketOnB.disconnect();
    await instanceA.app.close();
    await instanceB.app.close();
  });
});
```

- [ ] **Step 7: Run test to verify it fails**

Run: `cd gateway && npm test -- redis-io.adapter`
Expected: FAIL — `Cannot find module './redis-io.adapter'`.

- [ ] **Step 8: Create `gateway/src/redis/redis-io.adapter.ts`** (identical to chat-service's)

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

- [ ] **Step 9: Run test to verify it passes**

Run: `cd gateway && npm test -- redis-io.adapter`
Expected: PASS, 1 test.

- [ ] **Step 10: Commit**

```bash
git add gateway/src/redis
git commit -m "feat(gateway): add RedisModule and RedisIoAdapter"
```

---

### Task 12: Copy JWKS auth verification to gateway (TDD)

**Files:**
- Create: `gateway/src/auth/jwks.service.ts`, `gateway/src/auth/jwks.service.spec.ts`, `gateway/src/auth/jwt.strategy.ts`, `gateway/src/auth/jwt-auth.guard.ts`, `gateway/src/auth/auth.module.ts`

**Interfaces:**
- Produces: `JwksService`, `JwtStrategy`, `JwtAuthGuard`, `AuthModule` — consumed by Task 14's `WsTicketController`.

- [ ] **Step 1: Write the failing test** — `gateway/src/auth/jwks.service.spec.ts` (identical to chat-service's existing test; copy it verbatim first to see it fail, then implement)

Copy the full content of `src/auth/jwks.service.spec.ts` (repo root) into `gateway/src/auth/jwks.service.spec.ts` unchanged — its only imports are `./jwks.service` (relative, resolves within `gateway/src/auth/`) and `../../test/jwt` (resolves to `gateway/test/jwt.ts`, created in Task 10) and `net`/`crypto`-style built-ins, so no import paths need editing.

- [ ] **Step 2: Run test to verify it fails**

Run: `cd gateway && npm test -- jwks.service`
Expected: FAIL — `Cannot find module './jwks.service'`.

- [ ] **Step 3: Create `gateway/src/auth/jwks.service.ts`** (identical to chat-service's)

```typescript
import { Inject, Injectable, Logger, UnauthorizedException } from '@nestjs/common';
import { createPublicKey } from 'crypto';

export const JWKS_OPTIONS = Symbol('JWKS_OPTIONS');

export interface JwksOptions {
  url: string;
  ttlMs: number;
  minRefetchMs: number;
}

interface PublishedJwk {
  kid?: string;
  kty?: string;
  n?: string;
  e?: string;
}

@Injectable()
export class JwksService {
  private readonly logger = new Logger(JwksService.name);
  private keys = new Map<string, string>();
  private lastSuccessAt = 0;
  private lastAttemptAt = 0;
  private inflight?: Promise<void>;

  constructor(@Inject(JWKS_OPTIONS) private readonly options: JwksOptions) {}

  async getPublicKeyPem(kid: string | undefined): Promise<string> {
    if (!kid) {
      throw new UnauthorizedException('Missing key id');
    }

    const now = Date.now();
    const cached = this.keys.get(kid);
    const fresh = now - this.lastSuccessAt < this.options.ttlMs;
    if (cached && fresh) {
      return cached;
    }

    if (now - this.lastAttemptAt >= this.options.minRefetchMs) {
      await this.refresh();
    } else if (this.inflight) {
      await this.inflight;
    }

    const key = this.keys.get(kid);
    if (!key) {
      throw new UnauthorizedException('Unknown signing key');
    }
    return key;
  }

  private refresh(): Promise<void> {
    this.lastAttemptAt = Date.now();
    this.inflight = this.fetchKeys().finally(() => {
      this.inflight = undefined;
    });
    return this.inflight;
  }

  private async fetchKeys(): Promise<void> {
    try {
      const res = await fetch(this.options.url, { signal: AbortSignal.timeout(3000) });
      if (!res.ok) {
        throw new Error(`JWKS endpoint responded ${res.status}`);
      }
      const body = (await res.json()) as { keys?: PublishedJwk[] };

      const next = new Map<string, string>();
      for (const jwk of body.keys ?? []) {
        if (!jwk.kid || jwk.kty !== 'RSA') continue;
        const pem = createPublicKey({ key: jwk as never, format: 'jwk' }).export({
          type: 'spki',
          format: 'pem',
        }) as string;
        next.set(jwk.kid, pem);
      }
      this.keys = next;
      this.lastSuccessAt = Date.now();
    } catch (err) {
      this.logger.warn(`JWKS refresh failed: ${(err as Error).message}`);
    }
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd gateway && npm test -- jwks.service`
Expected: PASS (same test count as chat-service's version).

- [ ] **Step 5: Create the remaining auth files** (identical to chat-service, no TDD needed — thin wrappers already proven by Step 4's test plus Task 14's upcoming controller test)

`gateway/src/auth/jwt.strategy.ts`:

```typescript
import { Inject, Injectable } from '@nestjs/common';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';
import { JwksService } from './jwks.service';

export const JWT_VERIFY_OPTIONS = Symbol('JWT_VERIFY_OPTIONS');

export interface JwtVerifyOptions {
  issuer: string;
  audience: string;
}

function readKid(rawJwt: string): string | undefined {
  try {
    const header = JSON.parse(Buffer.from(rawJwt.split('.')[0], 'base64url').toString());
    return typeof header.kid === 'string' ? header.kid : undefined;
  } catch {
    return undefined;
  }
}

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(jwks: JwksService, @Inject(JWT_VERIFY_OPTIONS) options: JwtVerifyOptions) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      algorithms: ['RS256'],
      issuer: options.issuer,
      audience: options.audience,
      secretOrKeyProvider: (
        _req: unknown,
        rawJwt: string,
        done: (err: unknown, key?: string) => void,
      ) => {
        jwks.getPublicKeyPem(readKid(rawJwt)).then(
          (pem) => done(null, pem),
          (err) => done(err),
        );
      },
    });
  }

  validate(payload: { sub: string }) {
    return { id: payload.sub };
  }
}
```

`gateway/src/auth/jwt-auth.guard.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';

@Injectable()
export class JwtAuthGuard extends AuthGuard('jwt') {}
```

`gateway/src/auth/auth.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { JWKS_OPTIONS, JwksService } from './jwks.service';
import { JWT_VERIFY_OPTIONS, JwtStrategy } from './jwt.strategy';

@Module({
  imports: [PassportModule],
  providers: [
    {
      provide: JWKS_OPTIONS,
      useFactory: () => ({
        url: process.env.AUTH_JWKS_URL ?? 'http://localhost:3010/.well-known/jwks.json',
        ttlMs: 10 * 60_000,
        minRefetchMs: 10_000,
      }),
    },
    {
      provide: JWT_VERIFY_OPTIONS,
      useFactory: () => ({
        issuer: process.env.JWT_ISSUER ?? 'auth-service',
        audience: process.env.JWT_AUDIENCE ?? 'realtime-chat',
      }),
    },
    JwksService,
    JwtStrategy,
  ],
  exports: [JwksService],
})
export class AuthModule {}
```

- [ ] **Step 6: Verify everything compiles**

Run: `cd gateway && npx tsc --noEmit`
Expected: exits 0.

- [ ] **Step 7: Commit**

```bash
git add gateway/src/auth
git commit -m "feat(gateway): copy JWKS-based JWT verification from chat-service"
```

---

## Part C — gateway WS core

### Task 13: Copy `WsTicketService` + `WsTicketController` to gateway (TDD)

**Files:**
- Create: `gateway/src/realtime/ws-ticket.service.ts`, `gateway/src/realtime/ws-ticket.service.spec.ts`, `gateway/src/realtime/ws-ticket.controller.ts`, `gateway/src/realtime/ws-ticket.controller.spec.ts`

**Interfaces:**
- Produces: `WsTicketService.issue(userId, ttlSec?): Promise<string>`, `.consume(ticket): Promise<string|null>`, `WS_TICKET_TTL_SEC = 30` — consumed by Task 17's `EventsGateway`. `POST /ws-ticket` endpoint.

- [ ] **Step 1: Write the failing test** — copy `src/realtime/ws-ticket.service.spec.ts` (repo root) verbatim into `gateway/src/realtime/ws-ticket.service.spec.ts` (its imports — `ioredis`, `./ws-ticket.service`, `../../test/redis-cleanup`, `../../test/helpers` — all resolve correctly at this new location unchanged).

- [ ] **Step 2: Run test to verify it fails**

Run: `cd gateway && npm test -- ws-ticket.service`
Expected: FAIL — `Cannot find module './ws-ticket.service'`.

- [ ] **Step 3: Create `gateway/src/realtime/ws-ticket.service.ts`** (identical to chat-service's)

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

Run: `cd gateway && npm test -- ws-ticket.service`
Expected: PASS, 7 tests.

- [ ] **Step 5: Write the failing test for the controller** — create `gateway/src/realtime/ws-ticket.controller.spec.ts`:

```typescript
import { randomUUID } from 'crypto';
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { WsTicketController } from './ws-ticket.controller';
import { WsTicketService } from './ws-ticket.service';
import { REDIS } from '../redis/redis.constants';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { AuthModule } from '../auth/auth.module';
import { cleanRedis } from '../../test/redis-cleanup';
import { signToken, acquireJwksServer, releaseJwksServer } from '../../test/jwt';

describe('POST /ws-ticket', () => {
  let app: INestApplication;
  let redis: Redis;
  let baseUrl: string;

  beforeAll(async () => {
    await acquireJwksServer();
    const moduleRef = await Test.createTestingModule({
      imports: [AuthModule],
      controllers: [WsTicketController],
      providers: [
        WsTicketService,
        { provide: REDIS, useFactory: () => new Redis(process.env.REDIS_URL!) },
        JwtAuthGuard,
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
    redis = moduleRef.get(REDIS);
  });

  afterAll(async () => {
    await app.close();
    await redis.quit();
    await releaseJwksServer();
  });

  beforeEach(async () => {
    await cleanRedis(redis);
  });

  it('returns 401 without a token', async () => {
    const res = await fetch(`${baseUrl}/ws-ticket`, { method: 'POST' });
    expect(res.status).toBe(401);
  });

  it('returns a 48-char hex ticket with expiresIn 30 for a valid access token', async () => {
    const res = await fetch(`${baseUrl}/ws-ticket`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${signToken(randomUUID())}` },
    });

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.expiresIn).toBe(30);
    expect(body.ticket).toMatch(/^[0-9a-f]{48}$/);
  });
});
```

- [ ] **Step 6: Run test to verify it fails**

Run: `cd gateway && npm test -- ws-ticket.controller`
Expected: FAIL — `Cannot find module './ws-ticket.controller'`.

- [ ] **Step 7: Create `gateway/src/realtime/ws-ticket.controller.ts`**

```typescript
import { Controller, Post, Req, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { WS_TICKET_TTL_SEC, WsTicketService } from './ws-ticket.service';

@Controller('ws-ticket')
@UseGuards(JwtAuthGuard)
export class WsTicketController {
  constructor(private readonly wsTicketService: WsTicketService) {}

  @Post()
  async issue(@Req() req: { user: { id: string } }) {
    return {
      ticket: await this.wsTicketService.issue(req.user.id),
      expiresIn: WS_TICKET_TTL_SEC,
    };
  }
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `cd gateway && npm test -- ws-ticket.controller`
Expected: PASS, 2 tests.

- [ ] **Step 9: Commit**

```bash
git add gateway/src/realtime/ws-ticket.service.ts gateway/src/realtime/ws-ticket.service.spec.ts gateway/src/realtime/ws-ticket.controller.ts gateway/src/realtime/ws-ticket.controller.spec.ts
git commit -m "feat(gateway): add ws-ticket issuance (moved from chat-service)"
```

---

### Task 14: Copy full `PresenceService` to gateway (TDD)

**Files:**
- Create: `gateway/src/realtime/presence.service.ts`, `gateway/src/realtime/presence.service.spec.ts`

**Interfaces:**
- Produces: `PresenceService.connect(userId, socketId): Promise<boolean>`, `.disconnect(userId, socketId): Promise<boolean>`, `.touch(userId, socketId): Promise<void>`, `.isOnline(userId): Promise<boolean>`, `.onlineAmong(userIds): Promise<string[]>`, `PRESENCE_OPTIONS` token, `PresenceOptions`, `DEFAULT_PRESENCE_OPTIONS` — consumed by Task 17's `EventsGateway`.

This is the FULL original version (unlike chat-service's Task 6 trim) — gateway needs `connect`/`disconnect`/`touch` for its own WS lifecycle; keeping `isOnline`/`onlineAmong` too costs nothing and preserves full test parity with zero logic changes.

- [ ] **Step 1: Write the failing test** — copy `src/realtime/presence.service.spec.ts`'s **original pre-Task-6 version** (the one with `service.connect(...)`/`service.disconnect(...)`/`service.touch(...)` calls and the `{heartbeatTtlSec, refreshIntervalMs}` constructor options) into `gateway/src/realtime/presence.service.spec.ts`:

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
    await redis.sadd('presence:user:u1', 'ghost-socket');

    expect(await service.isOnline('u1')).toBe(false);
    expect(await redis.scard('presence:user:u1')).toBe(0);
  });

  it('treats a user with only orphaned sockets as offline when a new socket connects', async () => {
    await redis.sadd('presence:user:u1', 'ghost-socket');

    expect(await service.connect('u1', 'sock-a')).toBe(true);
    expect(await redis.smembers('presence:user:u1')).toEqual(['sock-a']);
  });

  it('expires the heartbeat so a silent socket stops counting as online', async () => {
    await service.connect('u1', 'sock-a');
    await sleep(1200);

    expect(await service.isOnline('u1')).toBe(false);
  });

  it('touch keeps a socket alive past the original TTL', async () => {
    await service.connect('u1', 'sock-a');
    await sleep(700);
    await service.touch('u1', 'sock-a');
    await sleep(700);

    expect(await service.isOnline('u1')).toBe(true);
  });

  it('touch re-registers a socket whose heartbeat expired but is still connected', async () => {
    await service.connect('u1', 'sock-a');
    await sleep(1200);
    await service.touch('u1', 'sock-a');

    expect(await service.isOnline('u1')).toBe(true);
  });

  it('puts a TTL on the user Set so a crashed instance cannot leak it forever', async () => {
    await service.connect('u1', 'sock-a');
    const ttl = await redis.ttl('presence:user:u1');
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(3);
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

Run: `cd gateway && npm test -- presence.service`
Expected: FAIL — `Cannot find module './presence.service'`.

- [ ] **Step 3: Create `gateway/src/realtime/presence.service.ts`** (full original version)

```typescript
import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS } from '../redis/redis.constants';

export const PRESENCE_OPTIONS = Symbol('PRESENCE_OPTIONS');

export interface PresenceOptions {
  heartbeatTtlSec: number;
  refreshIntervalMs: number;
}

export const DEFAULT_PRESENCE_OPTIONS: PresenceOptions = {
  heartbeatTtlSec: 60,
  refreshIntervalMs: 20_000,
};

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
    this.client.defineCommand('presenceConnect', {
      numberOfKeys: 1,
      lua: `${PRUNE_LUA}
local before = prune(KEYS[1])
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('SET', 'presence:socket:' .. ARGV[1], ARGV[2], 'EX', ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]) * 3)
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
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]) * 3)
return 1`,
    });
    this.client.defineCommand('presenceIsOnline', {
      numberOfKeys: 1,
      lua: `${PRUNE_LUA}
if prune(KEYS[1]) > 0 then return 1 else return 0 end`,
    });
  }

  async connect(userId: string, socketId: string): Promise<boolean> {
    const r = await this.client.presenceConnect(
      userKey(userId),
      socketId,
      userId,
      this.options.heartbeatTtlSec,
    );
    return r === 1;
  }

  async disconnect(userId: string, socketId: string): Promise<boolean> {
    const r = await this.client.presenceDisconnect(userKey(userId), socketId);
    return r === 1;
  }

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

Run: `cd gateway && npm test -- presence.service`
Expected: PASS, 14 tests.

- [ ] **Step 5: Commit**

```bash
git add gateway/src/realtime/presence.service.ts gateway/src/realtime/presence.service.spec.ts
git commit -m "feat(gateway): add PresenceService (moved from chat-service)"
```

---

### Task 15: `test/chat-stub.ts` — real HTTP stub for chat-service (no TDD — infra)

**Files:**
- Create: `gateway/test/chat-stub.ts`

**Interfaces:**
- Produces: `startChatStub(): Promise<ChatStub>` where `ChatStub` exposes `url`, `setMember(roomId, userId, isMember)`, `setRoomIds(userId, roomIds)`, `setSendMessageBehavior(fn)`, `lastInternalHeaders(): {secret?, userId?}`, `close(): Promise<void>` — consumed by Tasks 16-19's gateway tests.

- [ ] **Step 1: Create `gateway/test/chat-stub.ts`**

```typescript
import { createServer, IncomingMessage, ServerResponse } from 'http';
import { randomUUID } from 'crypto';
import { AddressInfo } from 'net';

export interface StubMessage {
  id: string;
  roomId: string;
  senderId: string;
  content: string;
  createdAt: string;
}

export type StubSendResult = { ok: true } | { ok: false; status: number; error: string };

export interface ChatStub {
  url: string;
  /** Configures the response for GET /internal/rooms/:roomId/members/:userId. */
  setMember(roomId: string, userId: string, isMember: boolean): void;
  /** Configures the response for GET /internal/users/:userId/room-ids. */
  setRoomIds(userId: string, roomIds: string[]): void;
  /** Configures how POST /rooms/:roomId/messages responds: accept, or a given status + error body. */
  setSendMessageBehavior(fn: (roomId: string, userId: string, content: string) => StubSendResult): void;
  lastInternalHeaders(): { secret?: string; userId?: string } | undefined;
  close(): Promise<void>;
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', (chunk) => (data += chunk));
    req.on('end', () => resolve(data));
  });
}

export async function startChatStub(): Promise<ChatStub> {
  const members = new Map<string, boolean>(); // `${roomId}:${userId}` -> isMember
  const roomIdsByUser = new Map<string, string[]>();
  let sendBehavior: (roomId: string, userId: string, content: string) => StubSendResult = () => ({
    ok: true,
  });
  let lastHeaders: { secret?: string; userId?: string } | undefined;

  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    const url = new URL(req.url ?? '/', 'http://stub');
    lastHeaders = {
      secret: req.headers['x-internal-secret'] as string | undefined,
      userId: req.headers['x-internal-user-id'] as string | undefined,
    };

    const membersMatch = url.pathname.match(/^\/internal\/rooms\/([^/]+)\/members\/([^/]+)$/);
    if (membersMatch) {
      const [, roomId, userId] = membersMatch;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ isMember: members.get(`${roomId}:${userId}`) ?? false }));
      return;
    }

    const roomIdsMatch = url.pathname.match(/^\/internal\/users\/([^/]+)\/room-ids$/);
    if (roomIdsMatch) {
      const [, userId] = roomIdsMatch;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ roomIds: roomIdsByUser.get(userId) ?? [] }));
      return;
    }

    const messagesMatch = url.pathname.match(/^\/rooms\/([^/]+)\/messages$/);
    if (messagesMatch && req.method === 'POST') {
      const [, roomId] = messagesMatch;
      const userId = req.headers['x-internal-user-id'] as string;
      const body = JSON.parse((await readBody(req)) || '{}') as { content: string };
      const result = sendBehavior(roomId, userId, body.content);
      res.setHeader('Content-Type', 'application/json');
      if (result.ok) {
        const message: StubMessage = {
          id: randomUUID(),
          roomId,
          senderId: userId,
          content: body.content,
          createdAt: new Date().toISOString(),
        };
        res.statusCode = 201;
        res.end(JSON.stringify(message));
      } else {
        res.statusCode = result.status;
        res.end(JSON.stringify({ message: result.error }));
      }
      return;
    }

    res.statusCode = 404;
    res.end();
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    url: `http://127.0.0.1:${port}`,
    setMember: (roomId, userId, isMember) => members.set(`${roomId}:${userId}`, isMember),
    setRoomIds: (userId, roomIds) => roomIdsByUser.set(userId, roomIds),
    setSendMessageBehavior: (fn) => {
      sendBehavior = fn;
    },
    lastInternalHeaders: () => lastHeaders,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}
```

- [ ] **Step 2: Verify it compiles**

Run: `cd gateway && npx tsc --noEmit`
Expected: exits 0.

- [ ] **Step 3: Commit**

```bash
git add gateway/test/chat-stub.ts
git commit -m "chore(gateway): add real HTTP stub standing in for chat-service in tests"
```

---

### Task 16: `internal-chat.client.ts` (TDD)

**Files:**
- Create: `gateway/src/realtime/internal-chat.client.ts`, `gateway/src/realtime/internal-chat.client.spec.ts`

**Interfaces:**
- Consumes: env `CHAT_SERVICE_URL`, `INTERNAL_SECRET`; `gateway/test/chat-stub.ts` (Task 15).
- Produces: `InternalChatClient.checkMembership(roomId, userId): Promise<boolean>`, `.listRoomIds(userId): Promise<string[]>`, `.sendMessage(roomId, userId, content): Promise<{success: true} | {success: false, error: string}>` — consumed by Tasks 17-19's `EventsGateway`.

- [ ] **Step 1: Write the failing test**

```typescript
// gateway/src/realtime/internal-chat.client.spec.ts
import { randomUUID } from 'crypto';
import { InternalChatClient } from './internal-chat.client';
import { startChatStub, ChatStub } from '../../test/chat-stub';

describe('InternalChatClient', () => {
  let stub: ChatStub;
  let client: InternalChatClient;
  const OLD_URL = process.env.CHAT_SERVICE_URL;
  const OLD_SECRET = process.env.INTERNAL_SECRET;

  beforeAll(async () => {
    stub = await startChatStub();
    process.env.CHAT_SERVICE_URL = stub.url;
    process.env.INTERNAL_SECRET = 'test-internal-secret';
    client = new InternalChatClient();
  });

  afterAll(async () => {
    await stub.close();
    process.env.CHAT_SERVICE_URL = OLD_URL;
    process.env.INTERNAL_SECRET = OLD_SECRET;
  });

  it('checkMembership returns true/false per the stub and sends the internal secret', async () => {
    const roomId = randomUUID();
    const userId = randomUUID();
    stub.setMember(roomId, userId, true);

    expect(await client.checkMembership(roomId, userId)).toBe(true);
    expect(stub.lastInternalHeaders()?.secret).toBe('test-internal-secret');

    stub.setMember(roomId, userId, false);
    expect(await client.checkMembership(roomId, userId)).toBe(false);
  });

  it('listRoomIds returns the stub-configured list', async () => {
    const userId = randomUUID();
    stub.setRoomIds(userId, ['room-a', 'room-b']);

    expect(await client.listRoomIds(userId)).toEqual(['room-a', 'room-b']);
  });

  it('sendMessage succeeds and sends X-Internal-User-Id', async () => {
    const roomId = randomUUID();
    const userId = randomUUID();
    stub.setSendMessageBehavior(() => ({ ok: true }));

    const result = await client.sendMessage(roomId, userId, 'hello');

    expect(result).toEqual({ success: true });
    expect(stub.lastInternalHeaders()?.userId).toBe(userId);
  });

  it("sendMessage surfaces the stub's error message on failure", async () => {
    const roomId = randomUUID();
    const userId = randomUUID();
    stub.setSendMessageBehavior(() => ({
      ok: false,
      status: 403,
      error: 'Not a member of this room',
    }));

    const result = await client.sendMessage(roomId, userId, 'hello');

    expect(result).toEqual({ success: false, error: 'Not a member of this room' });
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd gateway && npm test -- internal-chat.client`
Expected: FAIL — `Cannot find module './internal-chat.client'`.

- [ ] **Step 3: Write minimal implementation**

```typescript
// gateway/src/realtime/internal-chat.client.ts
import { Injectable } from '@nestjs/common';

@Injectable()
export class InternalChatClient {
  private get baseUrl(): string {
    return process.env.CHAT_SERVICE_URL ?? 'http://localhost:3000';
  }

  private get secret(): string {
    return process.env.INTERNAL_SECRET ?? '';
  }

  async checkMembership(roomId: string, userId: string): Promise<boolean> {
    const res = await fetch(`${this.baseUrl}/internal/rooms/${roomId}/members/${userId}`, {
      headers: { 'x-internal-secret': this.secret },
    });
    const body = (await res.json()) as { isMember: boolean };
    return body.isMember;
  }

  async listRoomIds(userId: string): Promise<string[]> {
    const res = await fetch(`${this.baseUrl}/internal/users/${userId}/room-ids`, {
      headers: { 'x-internal-secret': this.secret },
    });
    const body = (await res.json()) as { roomIds: string[] };
    return body.roomIds;
  }

  async sendMessage(
    roomId: string,
    userId: string,
    content: string,
  ): Promise<{ success: true } | { success: false; error: string }> {
    const res = await fetch(`${this.baseUrl}/rooms/${roomId}/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-internal-secret': this.secret,
        'x-internal-user-id': userId,
      },
      body: JSON.stringify({ content }),
    });

    if (res.ok) {
      return { success: true };
    }
    const body = (await res.json().catch(() => ({ message: 'Invalid request' }))) as {
      message?: string;
    };
    return { success: false, error: body.message ?? 'Invalid request' };
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd gateway && npm test -- internal-chat.client`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add gateway/src/realtime/internal-chat.client.ts gateway/src/realtime/internal-chat.client.spec.ts
git commit -m "feat(gateway): add InternalChatClient (membership/room-ids/sendMessage)"
```

---

### Task 17: `EventsGateway` — connection auth + presence (TDD)

**Files:**
- Create: `gateway/src/realtime/events.gateway.ts`, `gateway/src/realtime/events.gateway.spec.ts`

**Interfaces:**
- Consumes: `WsTicketService` (Task 13), `PresenceService` (Task 14), `InternalChatClient.listRoomIds` (Task 16).
- Produces: `EventsGateway` class — ticket-based connection auth (namespace middleware, same pattern Phase 2-4 already proved works), presence connect/disconnect/heartbeat, `broadcastPresence`. `room:subscribe`/`unsubscribe`/`message:send` are added in Task 18-19 on top of this same file.

- [ ] **Step 1: Create a minimal gateway skeleton**

```typescript
// gateway/src/realtime/events.gateway.ts
import { Injectable } from '@nestjs/common';
import { OnGatewayInit, WebSocketGateway } from '@nestjs/websockets';

@Injectable()
@WebSocketGateway({ cors: true, transports: ['websocket'] })
export class EventsGateway implements OnGatewayInit {
  afterInit(): void {}
}
```

- [ ] **Step 2: Write the failing test**

```typescript
// gateway/src/realtime/events.gateway.spec.ts
import { randomUUID } from 'crypto';
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { RedisModule } from '../redis/redis.module';
import { EventsGateway } from './events.gateway';
import { WsTicketService } from './ws-ticket.service';
import { PRESENCE_OPTIONS, DEFAULT_PRESENCE_OPTIONS, PresenceService } from './presence.service';
import { InternalChatClient } from './internal-chat.client';
import { startChatStub, ChatStub } from '../../test/chat-stub';

describe('EventsGateway', () => {
  let app: INestApplication;
  let wsTicketService: WsTicketService;
  let stub: ChatStub;
  let baseUrl: string;

  beforeAll(async () => {
    stub = await startChatStub();
    process.env.CHAT_SERVICE_URL = stub.url;
    process.env.INTERNAL_SECRET = 'test-internal-secret';

    const moduleRef = await Test.createTestingModule({
      imports: [RedisModule],
      providers: [
        EventsGateway,
        WsTicketService,
        PresenceService,
        { provide: PRESENCE_OPTIONS, useValue: DEFAULT_PRESENCE_OPTIONS },
        InternalChatClient,
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
    wsTicketService = moduleRef.get(WsTicketService);
  });

  afterAll(async () => {
    await app.close();
    await stub.close();
  });

  function connectWithTicket(ticket: string): Promise<ClientSocket> {
    return new Promise((resolve, reject) => {
      const socket = io(baseUrl, {
        auth: { ticket },
        transports: ['websocket'],
        reconnection: false,
        forceNew: true,
      });
      socket.on('connect', () => resolve(socket));
      socket.on('connect_error', (err) => reject(err));
    });
  }

  describe('handleConnection', () => {
    it('accepts a connection with a valid ticket', async () => {
      const ticket = await wsTicketService.issue(randomUUID());
      const socket = await connectWithTicket(ticket);
      expect(socket.connected).toBe(true);
      socket.disconnect();
    });

    it('rejects a connection with an unknown ticket', async () => {
      await expect(connectWithTicket('not-a-real-ticket')).rejects.toBeDefined();
    });

    it('rejects a connection when the ticket is reused', async () => {
      const ticket = await wsTicketService.issue(randomUUID());
      const first = await connectWithTicket(ticket);
      first.disconnect();
      await expect(connectWithTicket(ticket)).rejects.toBeDefined();
    });
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd gateway && npm test -- events.gateway`
Expected: FAIL — the 2 "rejects" tests fail (bare skeleton accepts every connection).

- [ ] **Step 4: Implement connection auth + presence lifecycle**

```typescript
// gateway/src/realtime/events.gateway.ts
import { Inject, Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import { OnGatewayInit, WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { WsTicketService } from './ws-ticket.service';
import { PRESENCE_OPTIONS, PresenceOptions, PresenceService } from './presence.service';
import { InternalChatClient } from './internal-chat.client';

@Injectable()
@WebSocketGateway({ cors: true, transports: ['websocket'] })
export class EventsGateway implements OnGatewayInit, OnModuleDestroy {
  private readonly logger = new Logger(EventsGateway.name);
  private heartbeatTimer?: NodeJS.Timeout;

  @WebSocketServer()
  server!: Server;

  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly presenceService: PresenceService,
    private readonly internalChatClient: InternalChatClient,
    @Inject(PRESENCE_OPTIONS) private readonly presenceOptions: PresenceOptions,
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

    server.on('connection', (socket: Socket) => void this.handleConnection(socket));
    server.on('connection', (socket: Socket) =>
      socket.on('disconnect', () => void this.handleDisconnect(socket)),
    );

    this.heartbeatTimer = setInterval(
      () => void this.refreshHeartbeats(),
      this.presenceOptions.refreshIntervalMs,
    );
    this.heartbeatTimer.unref();
  }

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

  async onModuleDestroy(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    const local = [...(this.server?.sockets?.sockets.values() ?? [])];
    await Promise.all(local.map((s) => this.handleDisconnect(s)));
  }

  private async handleConnection(client: Socket): Promise<void> {
    try {
      const userId = client.data.userId as string;
      const cameOnline = await this.presenceService.connect(userId, client.id);
      if (cameOnline) await this.broadcastPresence(userId, true);
    } catch (err) {
      this.logger.error(`presence connect failed: ${(err as Error).message}`);
    }
  }

  private async handleDisconnect(client: Socket): Promise<void> {
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
    const roomIds = await this.internalChatClient.listRoomIds(userId);
    if (roomIds.length === 0) return;
    this.server.to(roomIds).emit('presence:update', { userId, online });
  }
}
```

Note: unlike the old chat-service version, `handleConnection`/`handleDisconnect` are NOT `OnGatewayConnection`/`OnGatewayDisconnect` lifecycle hooks here — to keep the middleware (auth) and connection-handling cleanly ordered (a lesson from Phase 2: relying on `OnGatewayConnection` for anything order-sensitive relative to middleware is fragile), this version wires both directly off `server.on('connection', ...)` inside `afterInit`, registered right after the auth middleware — middleware always runs before Socket.IO's own `connection` event fires, so `socket.data.userId` is guaranteed set by the time these handlers run.

- [ ] **Step 5: Run test to verify it passes**

Run: `cd gateway && npm test -- events.gateway`
Expected: PASS, 3 tests.

- [ ] **Step 6: Add presence lifecycle tests**

Append to `gateway/src/realtime/events.gateway.spec.ts`:

```typescript
  describe('presence', () => {
    function emitWithAck<T>(socket: ClientSocket, event: string, payload: unknown): Promise<T> {
      return new Promise((resolve) => socket.emit(event, payload, resolve));
    }

    it('broadcasts presence:update to every room the user belongs to on connect/disconnect', async () => {
      const userId = randomUUID();
      const roomId = randomUUID();
      stub.setRoomIds(userId, [roomId]);
      stub.setMember(roomId, userId, true);

      const watcherTicket = await wsTicketService.issue(randomUUID());
      const watcher = await connectWithTicket(watcherTicket);
      await emitWithAck(watcher, 'room:subscribe', { roomId }); // Task 18 adds this handler

      const onlineUpdate = new Promise((resolve) => watcher.once('presence:update', resolve));
      const ticket = await wsTicketService.issue(userId);
      const socket = await connectWithTicket(ticket);

      expect(await onlineUpdate).toEqual({ userId, online: true });

      const offlineUpdate = new Promise((resolve) => watcher.once('presence:update', resolve));
      socket.disconnect();

      expect(await offlineUpdate).toEqual({ userId, online: false });

      watcher.disconnect();
    });
  });
```

This test calls `room:subscribe`, which Task 18 adds — if you're executing this plan strictly task-by-task, **defer running this specific test until after Task 18 lands** (note it in your report rather than leaving a failing test uncommitted); if Task 18 is already done by the time you reach this step, it should pass immediately.

- [ ] **Step 7: Commit**

```bash
git add gateway/src/realtime/events.gateway.ts gateway/src/realtime/events.gateway.spec.ts
git commit -m "feat(gateway): add EventsGateway connection auth + presence lifecycle"
```

---

### Task 18: `EventsGateway` — `room:subscribe`/`unsubscribe` (TDD)

**Files:**
- Modify: `gateway/src/realtime/events.gateway.ts`, `gateway/src/realtime/events.gateway.spec.ts`

**Interfaces:**
- Consumes: `InternalChatClient.checkMembership()` (Task 16).
- Produces: `room:subscribe`/`room:unsubscribe` WS events.

- [ ] **Step 1: Write the failing test**

Add to `gateway/src/realtime/events.gateway.spec.ts`:

```typescript
  describe('room:subscribe / room:unsubscribe', () => {
    function emitWithAck<T>(socket: ClientSocket, event: string, payload: unknown): Promise<T> {
      return new Promise((resolve) => socket.emit(event, payload, resolve));
    }

    it('allows a member to subscribe to a room', async () => {
      const userId = randomUUID();
      const roomId = randomUUID();
      stub.setMember(roomId, userId, true);
      const socket = await connectWithTicket(await wsTicketService.issue(userId));

      const ack = await emitWithAck<{ success: boolean }>(socket, 'room:subscribe', { roomId });

      expect(ack.success).toBe(true);
      socket.disconnect();
    });

    it('rejects subscribe when the user is not a member', async () => {
      const userId = randomUUID();
      const roomId = randomUUID();
      stub.setMember(roomId, userId, false);
      const socket = await connectWithTicket(await wsTicketService.issue(userId));

      const ack = await emitWithAck<{ success: boolean; error?: string }>(socket, 'room:subscribe', {
        roomId,
      });

      expect(ack.success).toBe(false);
      expect(ack.error).toBe('Not a member of this room');
      socket.disconnect();
    });

    it('allows unsubscribing without a membership check', async () => {
      const socket = await connectWithTicket(await wsTicketService.issue(randomUUID()));

      const ack = await emitWithAck<{ success: boolean }>(socket, 'room:unsubscribe', {
        roomId: randomUUID(),
      });

      expect(ack.success).toBe(true);
      socket.disconnect();
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd gateway && npm test -- events.gateway`
Expected: FAIL — no `room:subscribe`/`room:unsubscribe` listener exists; ack callbacks never fire, timing out.

- [ ] **Step 3: Implement the handlers**

Add to `gateway/src/realtime/events.gateway.ts` (imports first, then class body):

```typescript
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayInit,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
```

```typescript
interface AckResponse {
  success: boolean;
  error?: string;
}
```

Inside the class, after `broadcastPresence`:

```typescript
  @SubscribeMessage('room:subscribe')
  async handleSubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string },
  ): Promise<AckResponse> {
    const isMember = await this.internalChatClient.checkMembership(body.roomId, client.data.userId);
    if (!isMember) {
      return { success: false, error: 'Not a member of this room' };
    }
    await client.join(body.roomId);
    return { success: true };
  }

  @SubscribeMessage('room:unsubscribe')
  async handleUnsubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string },
  ): Promise<AckResponse> {
    await client.leave(body.roomId);
    return { success: true };
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd gateway && npm test -- events.gateway`
Expected: PASS, all tests including Task 17's deferred presence test.

- [ ] **Step 5: Commit**

```bash
git add gateway/src/realtime/events.gateway.ts gateway/src/realtime/events.gateway.spec.ts
git commit -m "feat(gateway): add room:subscribe/unsubscribe via internal membership check"
```

---

### Task 19: `message:send` + `MessageSubscriberService` (TDD)

**Files:**
- Modify: `gateway/src/realtime/events.gateway.ts`, `gateway/src/realtime/events.gateway.spec.ts`
- Create: `gateway/src/realtime/message-subscriber.service.ts`, `gateway/src/realtime/message-subscriber.service.spec.ts`

**Interfaces:**
- Consumes: `InternalChatClient.sendMessage()` (Task 16), `REDIS` token (Task 11), `EventsGateway.server` (Task 17).
- Produces: `message:send` WS event (ack only, no direct emit); `MessageSubscriberService` — the sole place `message:new` is emitted.

- [ ] **Step 1: Write the failing test for `message:send`**

Add to `gateway/src/realtime/events.gateway.spec.ts`:

```typescript
  describe('message:send', () => {
    function emitWithAck<T>(socket: ClientSocket, event: string, payload: unknown): Promise<T> {
      return new Promise((resolve) => socket.emit(event, payload, resolve));
    }

    it('acks success when the stub accepts the send', async () => {
      const userId = randomUUID();
      const roomId = randomUUID();
      stub.setSendMessageBehavior(() => ({ ok: true }));
      const socket = await connectWithTicket(await wsTicketService.issue(userId));

      const ack = await emitWithAck<{ success: boolean }>(socket, 'message:send', {
        roomId,
        content: 'hello',
      });

      expect(ack.success).toBe(true);
      socket.disconnect();
    });

    it('rejects empty content without calling the internal client', async () => {
      const socket = await connectWithTicket(await wsTicketService.issue(randomUUID()));

      const ack = await emitWithAck<{ success: boolean; error?: string }>(socket, 'message:send', {
        roomId: randomUUID(),
        content: '   ',
      });

      expect(ack.success).toBe(false);
      expect(ack.error).toBe('content must not be empty');
      socket.disconnect();
    });

    it("surfaces the stub's rejection as an ack error", async () => {
      stub.setSendMessageBehavior(() => ({
        ok: false,
        status: 403,
        error: 'Not a member of this room',
      }));
      const socket = await connectWithTicket(await wsTicketService.issue(randomUUID()));

      const ack = await emitWithAck<{ success: boolean; error?: string }>(socket, 'message:send', {
        roomId: randomUUID(),
        content: 'intruder',
      });

      expect(ack.success).toBe(false);
      expect(ack.error).toBe('Not a member of this room');
      socket.disconnect();
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd gateway && npm test -- events.gateway`
Expected: FAIL — no `message:send` listener; ack callbacks never fire.

- [ ] **Step 3: Implement `message:send`**

Add to `gateway/src/realtime/events.gateway.ts`, inside the class:

```typescript
  @SubscribeMessage('message:send')
  async handleMessageSend(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string; content: string },
  ): Promise<AckResponse> {
    if (!body?.content || typeof body.content !== 'string' || !body.content.trim()) {
      return { success: false, error: 'content must not be empty' };
    }

    const result = await this.internalChatClient.sendMessage(body.roomId, client.data.userId, body.content);
    return result;
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd gateway && npm test -- events.gateway`
Expected: PASS.

- [ ] **Step 5: Write the failing test for `MessageSubscriberService`**

```typescript
// gateway/src/realtime/message-subscriber.service.spec.ts
import { randomUUID } from 'crypto';
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { RedisModule } from '../redis/redis.module';
import { REDIS } from '../redis/redis.constants';
import { RedisIoAdapter } from '../redis/redis-io.adapter';
import { EventsGateway } from './events.gateway';
import { WsTicketService } from './ws-ticket.service';
import { PRESENCE_OPTIONS, DEFAULT_PRESENCE_OPTIONS, PresenceService } from './presence.service';
import { InternalChatClient } from './internal-chat.client';
import { MessageSubscriberService } from './message-subscriber.service';
import { startChatStub, ChatStub } from '../../test/chat-stub';

describe('MessageSubscriberService', () => {
  let app: INestApplication;
  let redis: Redis;
  let wsTicketService: WsTicketService;
  let stub: ChatStub;
  let baseUrl: string;

  beforeAll(async () => {
    stub = await startChatStub();
    process.env.CHAT_SERVICE_URL = stub.url;
    process.env.INTERNAL_SECRET = 'test-internal-secret';

    const moduleRef = await Test.createTestingModule({
      imports: [RedisModule],
      providers: [
        EventsGateway,
        WsTicketService,
        PresenceService,
        { provide: PRESENCE_OPTIONS, useValue: DEFAULT_PRESENCE_OPTIONS },
        InternalChatClient,
        MessageSubscriberService,
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    redis = app.get<Redis>(REDIS);
    app.useWebSocketAdapter(new RedisIoAdapter(app, redis));
    await app.init();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
    wsTicketService = moduleRef.get(WsTicketService);
  });

  afterAll(async () => {
    await app.close();
    await stub.close();
  });

  function connectWithTicket(ticket: string): Promise<ClientSocket> {
    return new Promise((resolve, reject) => {
      const socket = io(baseUrl, {
        auth: { ticket },
        transports: ['websocket'],
        reconnection: false,
        forceNew: true,
      });
      socket.on('connect', () => resolve(socket));
      socket.on('connect_error', reject);
    });
  }

  it('emits message:new to every socket in the room when chat:message:new is published', async () => {
    const userId = randomUUID();
    const roomId = randomUUID();
    stub.setMember(roomId, userId, true);

    const socket = await connectWithTicket(await wsTicketService.issue(userId));
    await new Promise<void>((resolve) => socket.emit('room:subscribe', { roomId }, () => resolve()));

    const received = new Promise((resolve) => socket.once('message:new', resolve));

    const message = {
      id: randomUUID(),
      roomId,
      senderId: randomUUID(),
      content: 'pub/sub hello',
      createdAt: new Date().toISOString(),
    };
    await redis.publish('chat:message:new', JSON.stringify({ roomId, message }));

    expect(await received).toEqual(message);
    socket.disconnect();
  });
});
```

- [ ] **Step 6: Run test to verify it fails**

Run: `cd gateway && npm test -- message-subscriber`
Expected: FAIL — `Cannot find module './message-subscriber.service'`.

- [ ] **Step 7: Write minimal implementation**

```typescript
// gateway/src/realtime/message-subscriber.service.ts
import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS } from '../redis/redis.constants';
import { EventsGateway } from './events.gateway';

const CHANNEL = 'chat:message:new';

interface ChatMessagePayload {
  roomId: string;
  message: unknown;
}

@Injectable()
export class MessageSubscriberService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MessageSubscriberService.name);
  private readonly sub: Redis;

  constructor(
    @Inject(REDIS) redis: Redis,
    private readonly gateway: EventsGateway,
  ) {
    this.sub = redis.duplicate();
  }

  async onModuleInit(): Promise<void> {
    await this.sub.subscribe(CHANNEL);
    this.sub.on('message', (_channel: string, raw: string) => {
      try {
        const { roomId, message } = JSON.parse(raw) as ChatMessagePayload;
        this.gateway.server.to(roomId).emit('message:new', message);
      } catch (err) {
        this.logger.error(`failed to handle ${CHANNEL} payload: ${(err as Error).message}`);
      }
    });
  }

  async onModuleDestroy(): Promise<void> {
    await this.sub.unsubscribe(CHANNEL).catch(() => undefined);
    await this.sub.quit().catch(() => undefined);
  }
}
```

- [ ] **Step 8: Run test to verify it passes**

Run: `cd gateway && npm test -- message-subscriber`
Expected: PASS, 1 test.

- [ ] **Step 9: Run the full gateway suite**

Run: `cd gateway && npm test`
Expected: all specs PASS.

- [ ] **Step 10: Commit**

```bash
git add gateway/src/realtime/events.gateway.ts gateway/src/realtime/events.gateway.spec.ts gateway/src/realtime/message-subscriber.service.ts gateway/src/realtime/message-subscriber.service.spec.ts
git commit -m "feat(gateway): add message:send + Redis pub/sub message:new broadcast"
```

---

## Part D — reverse proxy + module wiring

### Task 20: `ProxyController` (TDD)

**Files:**
- Create: `gateway/src/proxy/proxy.controller.ts`, `gateway/src/proxy/proxy.controller.spec.ts`

**Interfaces:**
- Consumes: `@nestjs/axios`'s `HttpService`, env `AUTH_SERVICE_URL`/`CHAT_SERVICE_URL`.
- Produces: `ProxyController` — forwards `/auth/*` to auth-service, `/rooms*` to chat-service.

- [ ] **Step 1: Write the failing test**

```typescript
// gateway/src/proxy/proxy.controller.spec.ts
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { AddressInfo } from 'net';
import { createServer, Server as HttpServer } from 'http';
import { ProxyController } from './proxy.controller';

interface FakeBackend {
  url: string;
  lastAuth?: string;
  server: HttpServer;
}

function startFakeBackend(statusCode: number, body: unknown): Promise<FakeBackend> {
  const backend: FakeBackend = { url: '', server: null as unknown as HttpServer };
  const server = createServer((req, res) => {
    backend.lastAuth = req.headers.authorization;
    res.statusCode = statusCode;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(body));
  });
  backend.server = server;
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as AddressInfo).port;
      backend.url = `http://127.0.0.1:${port}`;
      resolve(backend);
    });
  });
}

describe('ProxyController', () => {
  let app: INestApplication;
  let baseUrl: string;
  let authBackend: FakeBackend;
  let chatBackend: FakeBackend;

  beforeAll(async () => {
    authBackend = await startFakeBackend(200, { ok: 'from-auth' });
    chatBackend = await startFakeBackend(403, { message: 'Not a member of this room' });
    process.env.AUTH_SERVICE_URL = authBackend.url;
    process.env.CHAT_SERVICE_URL = chatBackend.url;

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
    await new Promise((r) => authBackend.server.close(r));
    await new Promise((r) => chatBackend.server.close(r));
  });

  it('forwards /auth/* to AUTH_SERVICE_URL and returns its body/status', async () => {
    const res = await fetch(`${baseUrl}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer abc' },
      body: JSON.stringify({ email: 'a@b.com', password: 'x' }),
    });

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: 'from-auth' });
    expect(authBackend.lastAuth).toBe('Bearer abc');
  });

  it('forwards /rooms* to CHAT_SERVICE_URL, preserving a non-2xx status and body', async () => {
    const res = await fetch(`${baseUrl}/rooms/some-room/messages`, {
      headers: { Authorization: 'Bearer xyz' },
    });

    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({ message: 'Not a member of this room' });
    expect(chatBackend.lastAuth).toBe('Bearer xyz');
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd gateway && npm test -- proxy.controller`
Expected: FAIL — `Cannot find module './proxy.controller'`.

- [ ] **Step 3: Write minimal implementation**

```typescript
// gateway/src/proxy/proxy.controller.ts
import { All, Controller, Req, Res } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { Request, Response } from 'express';
import { firstValueFrom } from 'rxjs';
import { AxiosRequestConfig } from 'axios';

@Controller()
export class ProxyController {
  constructor(private readonly http: HttpService) {}

  @All('auth/*')
  proxyAuth(@Req() req: Request, @Res() res: Response) {
    return this.forward(req, res, process.env.AUTH_SERVICE_URL ?? 'http://localhost:3010');
  }

  @All('rooms')
  @All('rooms/*')
  proxyRooms(@Req() req: Request, @Res() res: Response) {
    return this.forward(req, res, process.env.CHAT_SERVICE_URL ?? 'http://localhost:3000');
  }

  private async forward(req: Request, res: Response, target: string): Promise<void> {
    const config: AxiosRequestConfig = {
      method: req.method,
      url: `${target}${req.originalUrl}`,
      data: req.body,
      headers: req.headers.authorization ? { authorization: req.headers.authorization } : {},
      validateStatus: () => true,
    };
    const response = await firstValueFrom(this.http.request(config));
    res.status(response.status).json(response.data);
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd gateway && npm test -- proxy.controller`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add gateway/src/proxy
git commit -m "feat(gateway): add dumb reverse-proxy for /auth/* and /rooms*"
```

---

### Task 21: Wire `gateway/src/app.module.ts` + `main.ts` (no TDD — module wiring)

**Files:**
- Modify: `gateway/src/app.module.ts`, `gateway/src/main.ts`

**Interfaces:**
- Produces: a fully wired gateway app — every provider/controller from Tasks 9-20 registered.

- [ ] **Step 1: Write `gateway/src/app.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { HttpModule } from '@nestjs/axios';
import { PassportModule } from '@nestjs/passport';
import { RedisModule } from './redis/redis.module';
import { AuthModule } from './auth/auth.module';
import { ProxyController } from './proxy/proxy.controller';
import { WsTicketController } from './realtime/ws-ticket.controller';
import { WsTicketService } from './realtime/ws-ticket.service';
import { EventsGateway } from './realtime/events.gateway';
import { MessageSubscriberService } from './realtime/message-subscriber.service';
import { InternalChatClient } from './realtime/internal-chat.client';
import {
  DEFAULT_PRESENCE_OPTIONS,
  PRESENCE_OPTIONS,
  PresenceService,
} from './realtime/presence.service';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    RedisModule,
    HttpModule,
    PassportModule,
    AuthModule,
  ],
  controllers: [ProxyController, WsTicketController],
  providers: [
    WsTicketService,
    EventsGateway,
    MessageSubscriberService,
    InternalChatClient,
    PresenceService,
    { provide: PRESENCE_OPTIONS, useValue: DEFAULT_PRESENCE_OPTIONS },
  ],
})
export class AppModule {}
```

- [ ] **Step 2: Update `gateway/src/main.ts`** to install the Redis Socket.IO adapter and shutdown hooks:

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
  app.enableShutdownHooks();
  await app.listen(process.env.PORT ?? 3020);
}
bootstrap();
```

- [ ] **Step 3: Run the full gateway suite + typecheck**

Run: `cd gateway && npm run typecheck && npm test`
Expected: both exit 0.

- [ ] **Step 4: Boot it for real against the (also-booted) chat-service and auth-service**

Start chat-service, auth-service, and gateway each in the background from their own directories (`node dist/main.js` after `npm run build` in each). Set `gateway/.env` first (see Task 22) with matching `INTERNAL_SECRET` to chat-service's `.env`. Then:

```bash
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3020/rooms   # expect 401 (proxied to chat-service, no token)
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:3020/auth/me # expect 401 (proxied to auth-service, no token)
```

Stop all 3 processes afterward.

- [ ] **Step 5: Commit**

```bash
git add gateway/src/app.module.ts gateway/src/main.ts
git commit -m "feat(gateway): wire full AppModule, Redis Socket.IO adapter, shutdown hooks"
```

---

### Task 22: Env files + `INTERNAL_SECRET` wiring (no TDD — config)

**Files:**
- Create: `gateway/.env`, `gateway/.env.test`
- Modify: `.env`, `.env.test` (repo root — chat-service)

**Interfaces:** none — configuration only.

- [ ] **Step 1: Generate a shared `INTERNAL_SECRET`**

```bash
node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
```

Use the printed value as `INTERNAL_SECRET` in every file below (same value everywhere).

- [ ] **Step 2: Add to repo-root `.env`** (chat-service dev)

Append:

```
INTERNAL_SECRET="<the generated value>"
```

- [ ] **Step 3: Add to repo-root `.env.test`** (chat-service test)

Append:

```
INTERNAL_SECRET="test-internal-secret"
```

(Use a different, fixed, non-secret value for tests — real tests already set this directly per-file via `process.env.INTERNAL_SECRET = 'test-internal-secret'` in Tasks 2-5's specs; this `.env.test` entry is a fallback for any test that boots the full `AppModule` without overriding it, and for the final smoke test.)

- [ ] **Step 4: Create `gateway/.env`**

```
PORT=3020
REDIS_URL="redis://localhost:6379"
AUTH_JWKS_URL="http://localhost:3010/.well-known/jwks.json"
JWT_ISSUER="auth-service"
JWT_AUDIENCE="realtime-chat"
AUTH_SERVICE_URL="http://localhost:3010"
CHAT_SERVICE_URL="http://localhost:3000"
INTERNAL_SECRET="<same value as repo-root .env>"
```

- [ ] **Step 5: Create `gateway/.env.test`**

```
REDIS_URL="redis://localhost:6379/1"
INTERNAL_SECRET="test-internal-secret"
```

- [ ] **Step 6: Verify both `.env` files are gitignored**

Run: `git check-ignore gateway/.env gateway/.env.test .env .env.test`
Expected: all 4 paths print (confirming they're ignored — chat-service's root `.gitignore` already covers `.env`/`.env.test`; if `gateway/.env`/`gateway/.env.test` don't print, add `.env` and `.env.test` to the repo-root `.gitignore` if not already a blanket pattern covering subdirectories).

- [ ] **Step 7: Commit** (only if `.gitignore` needed changes — the `.env` files themselves must never be committed)

```bash
git status --short   # confirm no .env files are staged
git add .gitignore    # only if Step 6 required a change
git commit -m "chore: ensure gateway env files are gitignored"   # only if there's something to commit
```

---

## Part E — web/ changes

### Task 23: Consolidate `web/` to a single `GATEWAY_URL` (no TDD — BFF config)

**Files:**
- Create: `web/lib/gateway.ts`
- Delete: `web/lib/nest.ts`, `web/lib/auth.ts`
- Modify: `web/lib/nest-fetch.ts`, `web/middleware.ts`, `web/app/api/login/route.ts`, `web/app/api/register/route.ts`, `web/app/chat/page.tsx`, `web/app/chat/chat-client.tsx`, `web/.env.local`

**Interfaces:**
- Produces: `GATEWAY_URL` (server-side) — replaces `NEST_URL` and `AUTH_URL` everywhere in `web/`.

- [ ] **Step 1: Create `web/lib/gateway.ts`**

```typescript
export const GATEWAY_URL = process.env.GATEWAY_URL ?? 'http://localhost:3020';
```

- [ ] **Step 2: Delete the old lib files**

```bash
git rm web/lib/nest.ts web/lib/auth.ts
```

- [ ] **Step 3: Update `web/lib/nest-fetch.ts`** — replace `NEST_URL`/`AUTH_URL` imports with `GATEWAY_URL`, used for both the forwarded call and the refresh call:

```typescript
import { cookies } from 'next/headers';
import { GATEWAY_URL } from './gateway';

export async function nestFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const cookieStore = cookies();
  let accessToken = cookieStore.get('access_token')?.value;
  const refreshToken = cookieStore.get('refresh_token')?.value;

  const doFetch = (token?: string) =>
    fetch(`${GATEWAY_URL}${path}`, {
      ...init,
      headers: { ...init.headers, Authorization: `Bearer ${token ?? ''}` },
    });

  let response = await doFetch(accessToken);

  if (response.status === 401 && refreshToken) {
    const refreshRes = await fetch(`${GATEWAY_URL}/auth/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refreshToken }),
    });
    if (refreshRes.ok) {
      const tokens = await refreshRes.json();
      accessToken = tokens.accessToken;
      cookieStore.set('access_token', tokens.accessToken, { httpOnly: true, path: '/' });
      cookieStore.set('refresh_token', tokens.refreshToken, { httpOnly: true, path: '/' });
      response = await doFetch(accessToken);
    }
  }

  return response;
}
```

- [ ] **Step 4: Update `web/app/api/login/route.ts`** and `web/app/api/register/route.ts` — change the import and the fetch target (both files follow the same pattern; shown for `login`, apply the same substitution to `register`):

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { GATEWAY_URL } from '../../../lib/gateway';

export async function POST(request: NextRequest) {
  const body = await request.json();
  const nestResponse = await fetch(`${GATEWAY_URL}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!nestResponse.ok) {
    const error = await nestResponse.json();
    return NextResponse.json(error, { status: nestResponse.status });
  }

  const { accessToken, refreshToken } = await nestResponse.json();
  const response = NextResponse.json({ success: true });
  response.cookies.set('access_token', accessToken, { httpOnly: true, path: '/' });
  response.cookies.set('refresh_token', refreshToken, { httpOnly: true, path: '/' });
  return response;
}
```

(For `register/route.ts`, keep everything identical except `/auth/login` → `/auth/register`, same as it already differs today.)

- [ ] **Step 5: Update `web/middleware.ts`** — replace the `AUTH_URL` import/usage with `GATEWAY_URL`:

```typescript
import { GATEWAY_URL } from './lib/gateway';
```

and change the one `fetch(\`${AUTH_URL}/auth/refresh\`, ...)` call to `fetch(\`${GATEWAY_URL}/auth/refresh\`, ...)`. Leave every other line in the file unchanged.

- [ ] **Step 6: Update `web/app/chat/page.tsx`** — replace the `NEST_URL` import/usage with `GATEWAY_URL`:

```typescript
import { cookies } from 'next/headers';
import { redirect } from 'next/navigation';
import { GATEWAY_URL } from '../../lib/gateway';
import { ChatClient } from './chat-client';

interface Room {
  id: string;
  name: string;
}

export default async function ChatPage() {
  const accessToken = cookies().get('access_token')?.value;
  if (!accessToken) {
    redirect('/login');
  }

  const res = await fetch(`${GATEWAY_URL}/rooms`, {
    headers: { Authorization: `Bearer ${accessToken}` },
    cache: 'no-store',
  });

  if (!res.ok) {
    redirect('/login');
  }

  const rooms: Room[] = await res.json();

  return <ChatClient initialRooms={rooms} />;
}
```

- [ ] **Step 7: Update `web/app/chat/chat-client.tsx`** — the one `io(process.env.NEXT_PUBLIC_NEST_URL ?? 'http://localhost:3000', ...)` line becomes:

```typescript
    const socket = io(process.env.NEXT_PUBLIC_GATEWAY_URL ?? 'http://localhost:3020', {
```

Leave every other line in the file unchanged.

- [ ] **Step 8: Update `web/.env.local`**

Replace its contents:

```
GATEWAY_URL=http://localhost:3020
NEXT_PUBLIC_GATEWAY_URL=http://localhost:3020
```

- [ ] **Step 9: Verify it builds**

Run: `cd web && npm run typecheck && npm run build`
Expected: both exit 0. (The `rooms/[roomId]/join|messages|presence/route.ts` files already only import `nestFetch`, not `NEST_URL`/`AUTH_URL` directly, so they need no changes — confirm this by searching for remaining references: `grep -rn "NEST_URL\|AUTH_URL" web/app web/lib web/middleware.ts` should return nothing after this task.)

- [ ] **Step 10: Commit**

```bash
git add -A web
git commit -m "feat(web): route everything through a single GATEWAY_URL"
```

---

## Part F — final verification

### Task 24: Full end-to-end smoke test (no TDD — acceptance)

**Files:** none — verification only.

**Interfaces:** none — validates every completion criterion in the spec.

- [ ] **Step 1: Automated checks across all 4 packages**

Run, in order (each from its own directory):
1. Repo root: `npm run typecheck && npm test`
2. `auth-service/`: `npm run typecheck && npm test`
3. `gateway/`: `npm run typecheck && npm test`
4. `web/`: `npm run typecheck && npm run build`

Expected: all exit 0.

- [ ] **Step 2: Boot all 3 backend processes + web**

Start Postgres/Redis (via `docker compose up -d` or `./scripts/test-infra.sh`, whichever this machine uses). Then, each in its own terminal/background process: `npm run build && node dist/main.js` at repo root (chat-service, port 3000); the same in `auth-service/` (port 3010); the same in `gateway/` (port 3020); and in `web/`, `npm run build && npm run start` (port 3001).

- [ ] **Step 3: Verify register/login/rooms/messages work through gateway only**

```bash
TOKENS=$(curl -s -X POST http://localhost:3020/auth/register -H "Content-Type: application/json" \
  -d '{"email":"gw-smoke@example.com","password":"password123","displayName":"GW Smoke"}')
ACCESS=$(echo "$TOKENS" | node -e "process.stdin.once('data', d => console.log(JSON.parse(d).accessToken))")

ROOM=$(curl -s -X POST http://localhost:3020/rooms -H "Authorization: Bearer $ACCESS" \
  -H "Content-Type: application/json" -d '{"name":"GW Room"}')
ROOM_ID=$(echo "$ROOM" | node -e "process.stdin.once('data', d => console.log(JSON.parse(d).id))")

curl -s -X POST http://localhost:3020/rooms/$ROOM_ID/messages -H "Authorization: Bearer $ACCESS" \
  -H "Content-Type: application/json" -d '{"content":"hello via gateway REST"}'
```

Expected: register returns tokens, room creation returns 201, message send returns 201 with the persisted message.

- [ ] **Step 4: Verify WS end-to-end through gateway, including the REST-triggers-WS fix**

```bash
TICKET=$(curl -s -X POST http://localhost:3020/ws-ticket -H "Authorization: Bearer $ACCESS" \
  | node -e "process.stdin.once('data', d => console.log(JSON.parse(d).ticket))")
```

Use a small Node script (same pattern as Phase 2/3's smoke scripts) to: connect a `socket.io-client` to `http://localhost:3020` with `{auth: {ticket: TICKET}}`, emit `room:subscribe` for `ROOM_ID`, then — from a SEPARATE curl call — `POST /rooms/$ROOM_ID/messages` again (plain REST, not through the socket) and confirm the connected socket receives a `message:new` event. This is the Phase 5 regression check: REST-only sends must now also broadcast.

- [ ] **Step 5: Verify the Next.js app end-to-end**

Open `http://localhost:3001/register` in a browser, register, land on `/chat`, create a room, open a second browser profile, register a second user, join the same room, send a message from one tab, confirm it appears in the other tab without a refresh.

- [ ] **Step 6: Stop everything**

Stop all 4 background processes (chat-service, auth-service, gateway, web), and `docker compose down` if Docker was used.

---

## Completion Criteria (from spec)

- [ ] `gateway/` runs independently, imports nothing from `src/` (Tasks 9-21).
- [ ] REST through gateway behaves identically to before (dumb proxy, JWT verified at the real destination) (Task 20, Task 24 Step 3).
- [ ] WS connects through gateway via ws-ticket issued by gateway itself; `room:subscribe` rejects non-members via internal HTTP check (Tasks 13, 17-18).
- [ ] `message:send` persists via chat-service and broadcasts `message:new` via Redis pub/sub to every subscribed client, including across gateway instances (Task 19, Task 24 Step 4).
- [ ] Plain REST `POST /rooms/:roomId/messages` (no WS involved) also triggers `message:new` for connected WS clients — the gap fix (Task 5's publish + Task 19's subscriber, Task 24 Step 4).
- [ ] Presence (Phase 3) still works after moving to gateway + internal room-ids lookup (Task 14, 17).
- [ ] Internal endpoints reject missing/wrong `X-Internal-Secret`; real JWTs still work on `sendMessage` (Tasks 2-5).
- [ ] `web/` uses only `GATEWAY_URL`/`NEXT_PUBLIC_GATEWAY_URL` (Task 23).
- [ ] `npx tsc --noEmit` clean in all 4 packages; every package's test suite passes with real Postgres/Redis; full smoke passes (Task 24).
