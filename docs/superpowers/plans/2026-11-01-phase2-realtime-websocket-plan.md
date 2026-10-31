# Phase 2 — Realtime WebSocket Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add realtime messaging to the Phase 1 NestJS monolith via Socket.IO (1 instance, no Redis adapter), authenticated through a one-time short-lived ticket issued by a new minimal Next.js BFF app — plus the BFF app itself (login/register/chat pages).

**Architecture:** A new `RealtimeModule` in the existing NestJS app exposes a Socket.IO gateway (`EventsGateway`) that authenticates connections via a one-time `WsTicketService` ticket, reuses `RoomsService.sendMessage`/`assertMember` from Phase 1 for all business logic, and broadcasts `message:new` to room subscribers. A new Next.js app (`web/`) never exposes real JWT/refresh tokens to the browser — it stores them in `httpOnly` cookies and proxies every NestJS call (including the ticket exchange) through its own Route Handlers.

**Tech Stack:** `@nestjs/websockets` + `@nestjs/platform-socket.io` (backend), `socket.io-client` (backend tests + frontend), Next.js 14 App Router (frontend), existing Phase 1 stack (Prisma/Postgres/Jest) unchanged.

**Spec:** `docs/superpowers/specs/2026-11-01-phase2-realtime-websocket-design.md`

## Global Constraints

- 1 NestJS instance, no Redis adapter — Socket.IO's default in-memory adapter is enough (Redis adapter is Phase 3).
- WS ticket: random, 30-second TTL, **one-time use** — consuming it (valid or not) deletes it immediately.
- Real JWT access/refresh tokens never reach the browser — only `httpOnly` cookies set by the Next.js BFF; the browser only ever sees the short-lived ticket.
- `room:subscribe` only joins the caller to realtime updates for a room they are **already** a member of — it never creates a `RoomMember` row (that stays the job of REST `POST /rooms/:roomId/join` from Phase 1).
- Once a socket handshake succeeds, the connection stays alive regardless of token/ticket expiry — no periodic re-verification inside an open session (deferred past Phase 2).
- `message:send` must call the existing `RoomsService.sendMessage()` unchanged (same membership check, same persistence) — never duplicate that logic in the gateway.
- `message:new` is broadcast to every socket subscribed to the room, **including the sender's own socket**.
- TDD with real I/O (real Postgres test DB, real `socket.io-client` connections) for `WsTicketService` and `EventsGateway` — same "no mocking" rule as Phase 1. Next.js pages, Route Handlers, and basic payload validation do not need tests written first (same exemption Phase 1 gave controllers/DTOs).
- Out of scope: presence, Redis adapter, typing indicators, re-auth mid-session.

---

## File Structure

```
src/realtime/                        # NEW module
├── ws-ticket.service.ts             # one-time ticket issue/consume (pure, in-memory)
├── ws-ticket.service.spec.ts
├── events.gateway.ts                # Socket.IO gateway: connection auth, subscribe, send
├── events.gateway.spec.ts
└── realtime.module.ts               # wires the two above, imports RoomsModule

src/rooms/rooms.service.ts           # MODIFY: assertMember private -> public
src/rooms/rooms.service.spec.ts      # MODIFY: add direct assertMember tests
src/rooms/rooms.module.ts            # MODIFY: export RoomsService

src/auth/auth.controller.ts          # MODIFY: add POST /auth/ws-ticket
src/auth/auth.module.ts              # MODIFY: import RealtimeModule
src/app.module.ts                    # MODIFY: import RealtimeModule

package.json                         # MODIFY: add socket.io deps

web/                                 # NEW Next.js app (separate package.json)
├── package.json
├── tsconfig.json
├── next.config.js
├── next-env.d.ts
├── .gitignore
├── lib/
│   ├── nest.ts                      # NEST_URL constant
│   └── nest-fetch.ts                # server-side fetch helper: attaches cookie token, retries on 401 via refresh
├── app/
│   ├── layout.tsx
│   ├── page.tsx                     # redirects to /login
│   ├── register/page.tsx
│   ├── login/page.tsx
│   ├── api/
│   │   ├── register/route.ts
│   │   ├── login/route.ts
│   │   ├── ws-ticket/route.ts
│   │   └── rooms/
│   │       ├── route.ts             # GET list, POST create
│   │       └── [roomId]/
│   │           ├── join/route.ts    # POST join
│   │           └── messages/route.ts # GET history
│   └── chat/
│       ├── page.tsx                 # Server Component: loads initial room list
│       └── chat-client.tsx          # Client Component: socket, room switch, send/receive
```

One responsibility per file: `WsTicketService` only manages tickets; `EventsGateway` only wires socket events to existing services; `nest-fetch.ts` is the single place that knows how to attach/refresh the auth cookie for every proxied NestJS call.

---

### Task 1: Install Socket.IO dependencies (backend)

**Files:**
- Modify: `package.json`

**Interfaces:**
- Produces: `@nestjs/websockets`, `@nestjs/platform-socket.io`, `socket.io` available as deps; `socket.io-client` available as a devDependency for gateway tests.

- [ ] **Step 1: Add dependencies to `package.json`**

Add to `dependencies`:

```json
    "@nestjs/platform-socket.io": "^10.4.0",
    "@nestjs/websockets": "^10.4.0",
    "socket.io": "^4.7.5",
```

Add to `devDependencies`:

```json
    "socket.io-client": "^4.7.5",
```

- [ ] **Step 2: Install**

Run: `npm install`
Expected: installs without error.

- [ ] **Step 3: Verify the existing app still builds**

Run: `npm run build`
Expected: exits 0 (new deps installed but not yet used anywhere).

- [ ] **Step 4: Commit**

```bash
git add package.json package-lock.json
git commit -m "chore: add Socket.IO dependencies"
```

---

### Task 2: WsTicketService (TDD)

**Files:**
- Create: `src/realtime/ws-ticket.service.ts`, `src/realtime/ws-ticket.service.spec.ts`

**Interfaces:**
- Produces: `WsTicketService.issue(userId: string, ttlMs = 30_000): string` and `WsTicketService.consume(ticket: string): string | null` — consumed by `EventsGateway` (Task 4) and `AuthController` (Task 8).

- [ ] **Step 1: Write the failing test**

Create `src/realtime/ws-ticket.service.spec.ts`:

```typescript
import { WsTicketService } from './ws-ticket.service';

describe('WsTicketService', () => {
  let service: WsTicketService;

  beforeEach(() => {
    service = new WsTicketService();
  });

  it('issues a different ticket each time', () => {
    const a = service.issue('user-1');
    const b = service.issue('user-1');
    expect(a).not.toBe(b);
  });

  it('consumes a freshly issued ticket and returns the userId', () => {
    const ticket = service.issue('user-1');
    expect(service.consume(ticket)).toBe('user-1');
  });

  it('returns null when consuming the same ticket twice (one-time use)', () => {
    const ticket = service.issue('user-1');
    service.consume(ticket);
    expect(service.consume(ticket)).toBeNull();
  });

  it('returns null for an unknown ticket', () => {
    expect(service.consume('does-not-exist')).toBeNull();
  });

  it('returns null for an expired ticket', () => {
    const ticket = service.issue('user-1', -1); // already expired on issue
    expect(service.consume(ticket)).toBeNull();
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- ws-ticket.service`
Expected: FAIL — `Cannot find module './ws-ticket.service'`.

- [ ] **Step 3: Write minimal implementation**

Create `src/realtime/ws-ticket.service.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { randomBytes } from 'crypto';

@Injectable()
export class WsTicketService {
  private tickets = new Map<string, { userId: string; expiresAt: number }>();

  issue(userId: string, ttlMs = 30_000): string {
    const ticket = randomBytes(24).toString('hex');
    this.tickets.set(ticket, { userId, expiresAt: Date.now() + ttlMs });
    return ticket;
  }

  consume(ticket: string): string | null {
    const entry = this.tickets.get(ticket);
    this.tickets.delete(ticket); // one-time use: delete regardless of outcome
    if (!entry || entry.expiresAt < Date.now()) {
      return null;
    }
    return entry.userId;
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- ws-ticket.service`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add src/realtime/ws-ticket.service.ts src/realtime/ws-ticket.service.spec.ts
git commit -m "feat: add WsTicketService (one-time short-lived WS tickets)"
```

---

### Task 3: Expose `RoomsService.assertMember` publicly (TDD)

**Files:**
- Modify: `src/rooms/rooms.service.ts`, `src/rooms/rooms.service.spec.ts`, `src/rooms/rooms.module.ts`

**Interfaces:**
- Produces: `RoomsService.assertMember(userId: string, roomId: string): Promise<void>` as a **public** method (was `private`) — consumed directly by `EventsGateway` (Tasks 5-6). `RoomsModule` now `exports: [RoomsService]` — consumed by `RealtimeModule` (Task 7).

- [ ] **Step 1: Write the failing test**

Add to `src/rooms/rooms.service.spec.ts`, as a new top-level `describe` block (after the existing `getMessages` block, before the final closing `});` of the outer `describe('RoomsService', ...)`):

```typescript
  describe('assertMember', () => {
    it('resolves when the user is a member', async () => {
      const user = await createUser('member@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'X' });

      await expect(roomsService.assertMember(user.id, room.id)).resolves.toBeUndefined();
    });

    it('throws ForbiddenException when the user is not a member', async () => {
      const owner = await createUser('owner5@example.com');
      const outsider = await createUser('outsider3@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Y' });

      await expect(roomsService.assertMember(outsider.id, room.id)).rejects.toThrow(
        ForbiddenException,
      );
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- rooms.service`
Expected: FAIL — `Property 'assertMember' is private and only accessible within class 'RoomsService'` (TypeScript compile error inside the test file).

- [ ] **Step 3: Make `assertMember` public**

In `src/rooms/rooms.service.ts`, change:

```typescript
  private async assertMember(userId: string, roomId: string): Promise<void> {
```

to:

```typescript
  async assertMember(userId: string, roomId: string): Promise<void> {
```

- [ ] **Step 4: Export `RoomsService` from `RoomsModule`**

In `src/rooms/rooms.module.ts`, add an `exports` array:

```typescript
import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { RoomsService } from './rooms.service';
import { RoomsController } from './rooms.controller';

@Module({
  imports: [PassportModule],
  controllers: [RoomsController],
  providers: [RoomsService],
  exports: [RoomsService],
})
export class RoomsModule {}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `npm test -- rooms.service`
Expected: PASS, 13 tests (11 existing + 2 new).

- [ ] **Step 6: Commit**

```bash
git add src/rooms/rooms.service.ts src/rooms/rooms.service.spec.ts src/rooms/rooms.module.ts
git commit -m "feat: expose RoomsService.assertMember publicly for gateway reuse"
```

---

### Task 4: EventsGateway — connection authentication via ticket (TDD)

**Files:**
- Create: `src/realtime/events.gateway.ts`, `src/realtime/events.gateway.spec.ts`

**Interfaces:**
- Consumes: `WsTicketService.consume()` (Task 2), `RoomsService` (Task 3 — injected now, used starting Task 5).
- Produces: `EventsGateway` class that accepts/rejects Socket.IO connections based on `handshake.auth.ticket`, storing the resolved `userId` on `client.data.userId` for later tasks to read.

- [ ] **Step 1: Create a minimal gateway skeleton (no auth logic yet)**

Create `src/realtime/events.gateway.ts`:

```typescript
import { WebSocketGateway } from '@nestjs/websockets';

@WebSocketGateway({ cors: true })
export class EventsGateway {}
```

- [ ] **Step 2: Write the failing test**

Create `src/realtime/events.gateway.spec.ts`:

```typescript
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { EventsGateway } from './events.gateway';
import { WsTicketService } from './ws-ticket.service';
import { RoomsService } from '../rooms/rooms.service';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

describe('EventsGateway', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let wsTicketService: WsTicketService;
  let roomsService: RoomsService;
  let baseUrl: string;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [EventsGateway, WsTicketService, RoomsService, PrismaService],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);

    const address = app.getHttpServer().address() as AddressInfo;
    baseUrl = `http://localhost:${address.port}`;

    prisma = moduleRef.get(PrismaService);
    wsTicketService = moduleRef.get(WsTicketService);
    roomsService = moduleRef.get(RoomsService);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await app.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  async function createUser(email: string) {
    return prisma.user.create({ data: { email, passwordHash: 'x', displayName: email } });
  }

  function connectWithTicket(ticket: string): Promise<ClientSocket> {
    return new Promise((resolve, reject) => {
      const socket = io(baseUrl, { auth: { ticket }, reconnection: false, forceNew: true });
      socket.on('connect', () => resolve(socket));
      socket.on('connect_error', (err) => reject(err));
    });
  }

  describe('handleConnection', () => {
    it('accepts a connection with a valid ticket', async () => {
      const user = await createUser('wsuser@example.com');
      const ticket = wsTicketService.issue(user.id);

      const socket = await connectWithTicket(ticket);
      expect(socket.connected).toBe(true);
      socket.disconnect();
    });

    it('rejects a connection with an unknown ticket', async () => {
      await expect(connectWithTicket('not-a-real-ticket')).rejects.toBeDefined();
    });

    it('rejects a connection when the ticket is reused', async () => {
      const user = await createUser('wsuser2@example.com');
      const ticket = wsTicketService.issue(user.id);

      const first = await connectWithTicket(ticket);
      first.disconnect();

      await expect(connectWithTicket(ticket)).rejects.toBeDefined();
    });
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npm test -- events.gateway`
Expected: FAIL — the "rejects" tests fail because the empty gateway accepts every connection (no `connect_error` is ever emitted, so those `await expect(...).rejects` time out / resolve instead of rejecting).

- [ ] **Step 4: Implement connection authentication**

Replace `src/realtime/events.gateway.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { OnGatewayConnection, WebSocketGateway } from '@nestjs/websockets';
import { Socket } from 'socket.io';
import { WsTicketService } from './ws-ticket.service';
import { RoomsService } from '../rooms/rooms.service';

@Injectable()
@WebSocketGateway({ cors: true })
export class EventsGateway implements OnGatewayConnection {
  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly roomsService: RoomsService,
  ) {}

  handleConnection(client: Socket): void {
    const ticket = client.handshake.auth?.ticket as string | undefined;
    const userId = ticket ? this.wsTicketService.consume(ticket) : null;

    if (!userId) {
      client.disconnect(true);
      return;
    }

    client.data.userId = userId;
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `npm test -- events.gateway`
Expected: PASS, 3 tests.

- [ ] **Step 6: Commit**

```bash
git add src/realtime/events.gateway.ts src/realtime/events.gateway.spec.ts
git commit -m "feat: add EventsGateway connection auth via one-time ticket"
```

---

### Task 5: EventsGateway — `room:subscribe` / `room:unsubscribe` (TDD)

**Files:**
- Modify: `src/realtime/events.gateway.ts`, `src/realtime/events.gateway.spec.ts`

**Interfaces:**
- Consumes: `RoomsService.assertMember()` (Task 3).
- Produces: `room:subscribe` and `room:unsubscribe` Socket.IO events, each acking `{success: boolean, error?: string}`.

- [ ] **Step 1: Write the failing test**

Add to `src/realtime/events.gateway.spec.ts`, as a new top-level `describe` block:

```typescript
  describe('room:subscribe / room:unsubscribe', () => {
    function emitWithAck<T>(socket: ClientSocket, event: string, payload: unknown): Promise<T> {
      return new Promise((resolve) => socket.emit(event, payload, resolve));
    }

    it('allows a member to subscribe to a room', async () => {
      const user = await createUser('subuser@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Sub' });
      const ticket = wsTicketService.issue(user.id);
      const socket = await connectWithTicket(ticket);

      const ack = await emitWithAck<{ success: boolean }>(socket, 'room:subscribe', {
        roomId: room.id,
      });

      expect(ack.success).toBe(true);
      socket.disconnect();
    });

    it('rejects subscribe when the user is not a member', async () => {
      const owner = await createUser('subowner@example.com');
      const outsider = await createUser('suboutsider@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'SubPrivate' });
      const ticket = wsTicketService.issue(outsider.id);
      const socket = await connectWithTicket(ticket);

      const ack = await emitWithAck<{ success: boolean; error?: string }>(
        socket,
        'room:subscribe',
        { roomId: room.id },
      );

      expect(ack.success).toBe(false);
      expect(ack.error).toBe('Not a member of this room');
      socket.disconnect();
    });

    it('allows unsubscribing without a membership check', async () => {
      const user = await createUser('unsubuser@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Unsub' });
      const ticket = wsTicketService.issue(user.id);
      const socket = await connectWithTicket(ticket);

      const ack = await emitWithAck<{ success: boolean }>(socket, 'room:unsubscribe', {
        roomId: room.id,
      });

      expect(ack.success).toBe(true);
      socket.disconnect();
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- events.gateway`
Expected: FAIL — no `room:subscribe`/`room:unsubscribe` listener exists, so the client's ack callback is never invoked and the `await emitWithAck(...)` calls hang until Jest's test timeout.

- [ ] **Step 3: Implement the handlers**

Replace `src/realtime/events.gateway.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  SubscribeMessage,
  WebSocketGateway,
} from '@nestjs/websockets';
import { Socket } from 'socket.io';
import { WsTicketService } from './ws-ticket.service';
import { RoomsService } from '../rooms/rooms.service';

interface AckResponse {
  success: boolean;
  error?: string;
}

@Injectable()
@WebSocketGateway({ cors: true })
export class EventsGateway implements OnGatewayConnection {
  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly roomsService: RoomsService,
  ) {}

  handleConnection(client: Socket): void {
    const ticket = client.handshake.auth?.ticket as string | undefined;
    const userId = ticket ? this.wsTicketService.consume(ticket) : null;

    if (!userId) {
      client.disconnect(true);
      return;
    }

    client.data.userId = userId;
  }

  @SubscribeMessage('room:subscribe')
  async handleSubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string },
  ): Promise<AckResponse> {
    try {
      await this.roomsService.assertMember(client.data.userId, body.roomId);
      await client.join(body.roomId);
      return { success: true };
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  }

  @SubscribeMessage('room:unsubscribe')
  async handleUnsubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string },
  ): Promise<AckResponse> {
    await client.leave(body.roomId);
    return { success: true };
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- events.gateway`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add src/realtime/events.gateway.ts src/realtime/events.gateway.spec.ts
git commit -m "feat: add EventsGateway room:subscribe/unsubscribe"
```

---

### Task 6: EventsGateway — `message:send` + `message:new` broadcast (TDD)

**Files:**
- Modify: `src/realtime/events.gateway.ts`, `src/realtime/events.gateway.spec.ts`

**Interfaces:**
- Consumes: `RoomsService.sendMessage()` (Phase 1, unchanged).
- Produces: `message:send` event acking `{success: boolean, error?: string}`; `message:new` broadcast (server→client) with the created `Message`.

- [ ] **Step 1: Write the failing test**

Add to `src/realtime/events.gateway.spec.ts`:

```typescript
  describe('message:send', () => {
    function emitWithAck<T>(socket: ClientSocket, event: string, payload: unknown): Promise<T> {
      return new Promise((resolve) => socket.emit(event, payload, resolve));
    }

    it('persists the message, acks success, and broadcasts message:new to subscribers', async () => {
      const user = await createUser('sender-ws@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Broadcast' });

      const socketA = await connectWithTicket(wsTicketService.issue(user.id)); // sender
      const socketB = await connectWithTicket(wsTicketService.issue(user.id)); // second tab

      await emitWithAck(socketA, 'room:subscribe', { roomId: room.id });
      await emitWithAck(socketB, 'room:subscribe', { roomId: room.id });

      const received = new Promise<{ content: string }>((resolve) =>
        socketB.once('message:new', resolve),
      );

      const ack = await emitWithAck<{ success: boolean }>(socketA, 'message:send', {
        roomId: room.id,
        content: 'hello from A',
      });

      expect(ack.success).toBe(true);
      const broadcast = await received;
      expect(broadcast.content).toBe('hello from A');

      const stored = await prisma.message.findFirst({ where: { roomId: room.id } });
      expect(stored?.content).toBe('hello from A');

      socketA.disconnect();
      socketB.disconnect();
    });

    it('rejects an empty message with an ack error', async () => {
      const user = await createUser('emptysender@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'EmptyMsg' });
      const socket = await connectWithTicket(wsTicketService.issue(user.id));
      await emitWithAck(socket, 'room:subscribe', { roomId: room.id });

      const ack = await emitWithAck<{ success: boolean; error?: string }>(socket, 'message:send', {
        roomId: room.id,
        content: '   ',
      });

      expect(ack.success).toBe(false);
      expect(ack.error).toBe('content must not be empty');
      socket.disconnect();
    });

    it('rejects when the sender is not a room member', async () => {
      const owner = await createUser('msgowner-ws@example.com');
      const outsider = await createUser('msgoutsider-ws@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'PrivateWs' });
      const socket = await connectWithTicket(wsTicketService.issue(outsider.id));

      const ack = await emitWithAck<{ success: boolean; error?: string }>(socket, 'message:send', {
        roomId: room.id,
        content: 'intruder',
      });

      expect(ack.success).toBe(false);
      expect(ack.error).toBe('Not a member of this room');
      socket.disconnect();
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- events.gateway`
Expected: FAIL — no `message:send` listener exists; the ack callbacks never fire, so the `emitWithAck` calls hang until timeout.

- [ ] **Step 3: Implement `message:send`**

Replace `src/realtime/events.gateway.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { WsTicketService } from './ws-ticket.service';
import { RoomsService } from '../rooms/rooms.service';

interface AckResponse {
  success: boolean;
  error?: string;
}

@Injectable()
@WebSocketGateway({ cors: true })
export class EventsGateway implements OnGatewayConnection {
  @WebSocketServer()
  server!: Server;

  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly roomsService: RoomsService,
  ) {}

  handleConnection(client: Socket): void {
    const ticket = client.handshake.auth?.ticket as string | undefined;
    const userId = ticket ? this.wsTicketService.consume(ticket) : null;

    if (!userId) {
      client.disconnect(true);
      return;
    }

    client.data.userId = userId;
  }

  @SubscribeMessage('room:subscribe')
  async handleSubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string },
  ): Promise<AckResponse> {
    try {
      await this.roomsService.assertMember(client.data.userId, body.roomId);
      await client.join(body.roomId);
      return { success: true };
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  }

  @SubscribeMessage('room:unsubscribe')
  async handleUnsubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string },
  ): Promise<AckResponse> {
    await client.leave(body.roomId);
    return { success: true };
  }

  @SubscribeMessage('message:send')
  async handleMessageSend(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string; content: string },
  ): Promise<AckResponse> {
    if (!body.content || !body.content.trim()) {
      return { success: false, error: 'content must not be empty' };
    }

    try {
      const message = await this.roomsService.sendMessage(client.data.userId, body.roomId, {
        content: body.content,
      });
      this.server.to(body.roomId).emit('message:new', message);
      return { success: true };
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- events.gateway`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add src/realtime/events.gateway.ts src/realtime/events.gateway.spec.ts
git commit -m "feat: add EventsGateway message:send with message:new broadcast"
```

---

### Task 7: Wire `RealtimeModule` into the app (no TDD — module wiring)

**Files:**
- Create: `src/realtime/realtime.module.ts`
- Modify: `src/app.module.ts`

**Interfaces:**
- Produces: `RealtimeModule` (`imports: [RoomsModule]`, `providers: [WsTicketService, EventsGateway]`, `exports: [WsTicketService]`) — consumed by `AuthModule` (Task 8) to get `WsTicketService`, and registered in `AppModule` so `EventsGateway` actually starts.

- [ ] **Step 1: Create `src/realtime/realtime.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { RoomsModule } from '../rooms/rooms.module';
import { WsTicketService } from './ws-ticket.service';
import { EventsGateway } from './events.gateway';

@Module({
  imports: [RoomsModule],
  providers: [WsTicketService, EventsGateway],
  exports: [WsTicketService],
})
export class RealtimeModule {}
```

- [ ] **Step 2: Register it in `src/app.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from './prisma/prisma.module';
import { AuthModule } from './auth/auth.module';
import { RoomsModule } from './rooms/rooms.module';
import { RealtimeModule } from './realtime/realtime.module';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    PrismaModule,
    AuthModule,
    RoomsModule,
    RealtimeModule,
  ],
})
export class AppModule {}
```

- [ ] **Step 3: Verify the app still typechecks and all tests pass**

Run: `npm run typecheck && npm test`
Expected: both exit 0; 30 tests total (10 auth + 13 rooms + 9 events — wait, rooms is 13 after Task 3, events is 9 after Task 6, auth is 10 from Phase 1 → 10 + 13 + 9 = 32 tests).

- [ ] **Step 4: Commit**

```bash
git add src/realtime/realtime.module.ts src/app.module.ts
git commit -m "feat: wire RealtimeModule into the app"
```

---

### Task 8: `POST /auth/ws-ticket` endpoint (no TDD — controller wiring)

**Files:**
- Modify: `src/auth/auth.controller.ts`, `src/auth/auth.module.ts`

**Interfaces:**
- Consumes: `WsTicketService.issue()` (Task 2, available via `RealtimeModule` export from Task 7).
- Produces: `POST /auth/ws-ticket` (behind `JwtAuthGuard`) → `{ticket: string, expiresIn: number}`.

- [ ] **Step 1: Import `RealtimeModule` into `AuthModule`**

In `src/auth/auth.module.ts`, add the import and keep everything else the same:

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { PassportModule } from '@nestjs/passport';
import { AuthService } from './auth.service';
import { AuthController } from './auth.controller';
import { JwtStrategy } from './jwt.strategy';
import { RealtimeModule } from '../realtime/realtime.module';

@Module({
  imports: [
    ConfigModule,
    PassportModule,
    RealtimeModule,
    JwtModule.registerAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        secret: config.get<string>('JWT_SECRET'),
        signOptions: { expiresIn: '15m' },
      }),
    }),
  ],
  controllers: [AuthController],
  providers: [AuthService, JwtStrategy],
  exports: [AuthService],
})
export class AuthModule {}
```

- [ ] **Step 2: Add the endpoint to `AuthController`**

Replace `src/auth/auth.controller.ts`:

```typescript
import { Body, Controller, Get, HttpCode, Post, Req, UseGuards } from '@nestjs/common';
import { AuthService } from './auth.service';
import { RegisterDto } from './dto/register.dto';
import { LoginDto } from './dto/login.dto';
import { RefreshDto } from './dto/refresh.dto';
import { JwtAuthGuard } from './jwt-auth.guard';
import { PrismaService } from '../prisma/prisma.service';
import { WsTicketService } from '../realtime/ws-ticket.service';

@Controller('auth')
export class AuthController {
  constructor(
    private readonly authService: AuthService,
    private readonly prisma: PrismaService,
    private readonly wsTicketService: WsTicketService,
  ) {}

  @Post('register')
  register(@Body() dto: RegisterDto) {
    return this.authService.register(dto);
  }

  @Post('login')
  login(@Body() dto: LoginDto) {
    return this.authService.login(dto);
  }

  @Post('refresh')
  refresh(@Body() dto: RefreshDto) {
    return this.authService.refresh(dto.refreshToken);
  }

  @Post('logout')
  @HttpCode(200)
  logout(@Body() dto: RefreshDto) {
    return this.authService.logout(dto.refreshToken);
  }

  @Get('me')
  @UseGuards(JwtAuthGuard)
  async me(@Req() req: { user: { id: string } }) {
    const user = await this.prisma.user.findUniqueOrThrow({ where: { id: req.user.id } });
    return { id: user.id, email: user.email, displayName: user.displayName };
  }

  @Post('ws-ticket')
  @UseGuards(JwtAuthGuard)
  wsTicket(@Req() req: { user: { id: string } }) {
    return { ticket: this.wsTicketService.issue(req.user.id), expiresIn: 30 };
  }
}
```

- [ ] **Step 3: Verify typecheck and full test suite**

Run: `npm run typecheck && npm test`
Expected: both exit 0, 32 tests pass (no new tests added this task — pure wiring).

- [ ] **Step 4: Manual smoke test**

```bash
npm run build && node dist/main.js &
# in another shell:
TOKENS=$(curl -s -X POST http://localhost:3000/auth/register -H "Content-Type: application/json" \
  -d '{"email":"ws-smoke@example.com","password":"password123","displayName":"WS Smoke"}')
ACCESS=$(echo "$TOKENS" | node -e "process.stdin.once('data', d => console.log(JSON.parse(d).accessToken))")
curl -s -X POST http://localhost:3000/auth/ws-ticket -H "Authorization: Bearer $ACCESS"
```

Expected: `{"ticket":"<64-char hex>","expiresIn":30}`. Stop the server afterward.

- [ ] **Step 5: Commit**

```bash
git add src/auth/auth.controller.ts src/auth/auth.module.ts
git commit -m "feat: add POST /auth/ws-ticket endpoint"
```

---

### Task 9: Next.js app scaffold (no TDD — scaffolding)

**Files:**
- Create: `web/package.json`, `web/tsconfig.json`, `web/next.config.js`, `web/next-env.d.ts`, `web/.gitignore`
- Create: `web/lib/nest.ts`, `web/app/layout.tsx`, `web/app/page.tsx`

**Interfaces:**
- Produces: a runnable empty Next.js app (`npm run dev` in `web/` boots on port 3001) with `NEST_URL` constant available to every server-side file created in later tasks.

- [ ] **Step 1: Create `web/package.json`**

```json
{
  "name": "realtime-chat-web",
  "version": "0.1.0",
  "private": true,
  "scripts": {
    "dev": "next dev -p 3001",
    "build": "next build",
    "start": "next start -p 3001",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "next": "^14.2.15",
    "react": "^18.3.1",
    "react-dom": "^18.3.1",
    "socket.io-client": "^4.7.5"
  },
  "devDependencies": {
    "@types/node": "^20.16.0",
    "@types/react": "^18.3.5",
    "@types/react-dom": "^18.3.0",
    "typescript": "^5.6.2"
  }
}
```

- [ ] **Step 2: Create `web/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2017",
    "lib": ["dom", "dom.iterable", "esnext"],
    "allowJs": true,
    "skipLibCheck": true,
    "strict": true,
    "noEmit": true,
    "esModuleInterop": true,
    "module": "esnext",
    "moduleResolution": "bundler",
    "resolveJsonModule": true,
    "isolatedModules": true,
    "jsx": "preserve",
    "incremental": true,
    "plugins": [{ "name": "next" }]
  },
  "include": ["next-env.d.ts", "**/*.ts", "**/*.tsx", ".next/types/**/*.ts"],
  "exclude": ["node_modules"]
}
```

- [ ] **Step 3: Create `web/next.config.js`**

```javascript
/** @type {import('next').NextConfig} */
const nextConfig = {};
module.exports = nextConfig;
```

- [ ] **Step 4: Create `web/next-env.d.ts`**

```typescript
/// <reference types="next" />
/// <reference types="next/image-types/global" />
```

- [ ] **Step 5: Create `web/.gitignore`**

```
node_modules/
.next/
.env.local
```

- [ ] **Step 6: Create `web/lib/nest.ts`**

```typescript
export const NEST_URL = process.env.NEST_URL ?? 'http://localhost:3000';
```

- [ ] **Step 7: Create `web/app/layout.tsx`**

```tsx
export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
```

- [ ] **Step 8: Create `web/app/page.tsx`**

```tsx
import { redirect } from 'next/navigation';

export default function HomePage() {
  redirect('/login');
}
```

- [ ] **Step 9: Create `web/.env.local`** (not committed — already gitignored)

```
NEST_URL=http://localhost:3000
NEXT_PUBLIC_NEST_URL=http://localhost:3000
```

- [ ] **Step 10: Install and verify it builds**

Run (from `web/`): `npm install && npm run build`
Expected: exits 0 (build succeeds with just the redirect page).

- [ ] **Step 11: Commit**

```bash
git add web/package.json web/package-lock.json web/tsconfig.json web/next.config.js web/next-env.d.ts web/.gitignore web/lib/nest.ts web/app/layout.tsx web/app/page.tsx
git commit -m "chore: scaffold Next.js BFF app"
```

---

### Task 10: BFF auth proxy — `/api/register`, `/api/login` (no TDD — thin proxy)

**Files:**
- Create: `web/app/api/register/route.ts`, `web/app/api/login/route.ts`

**Interfaces:**
- Consumes: NestJS `POST /auth/register` and `POST /auth/login` (Phase 1).
- Produces: `POST /api/register` and `POST /api/login` — both set `httpOnly` cookies `access_token`/`refresh_token` on success, never return the raw tokens to the browser.

- [ ] **Step 1: Create `web/app/api/register/route.ts`**

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { NEST_URL } from '../../../lib/nest';

export async function POST(request: NextRequest) {
  const body = await request.json();
  const nestResponse = await fetch(`${NEST_URL}/auth/register`, {
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

- [ ] **Step 2: Create `web/app/api/login/route.ts`**

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { NEST_URL } from '../../../lib/nest';

export async function POST(request: NextRequest) {
  const body = await request.json();
  const nestResponse = await fetch(`${NEST_URL}/auth/login`, {
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

- [ ] **Step 3: Verify it builds**

Run (from `web/`): `npm run build`
Expected: exits 0.

- [ ] **Step 4: Commit**

```bash
git add web/app/api/register/route.ts web/app/api/login/route.ts
git commit -m "feat: add BFF register/login proxy routes"
```

---

### Task 11: BFF proxy — ws-ticket + rooms (no TDD — thin proxy)

**Files:**
- Create: `web/lib/nest-fetch.ts`, `web/app/api/ws-ticket/route.ts`, `web/app/api/rooms/route.ts`, `web/app/api/rooms/[roomId]/join/route.ts`, `web/app/api/rooms/[roomId]/messages/route.ts`

**Interfaces:**
- Produces: `nestFetch(path: string, init?: RequestInit): Promise<Response>` — attaches the `access_token` cookie as a Bearer header, and on a `401` retries once after refreshing via the `refresh_token` cookie. Consumed by every proxy route in this task. `GET /api/ws-ticket` → `{ticket, expiresIn}`; `GET/POST /api/rooms`; `POST /api/rooms/:roomId/join`; `GET /api/rooms/:roomId/messages`.

**Known simplification:** `nestFetch` does not write the refreshed cookie back onto the outer response (it only has a `Request`/cookie *read* context, not the route's own `NextResponse`) — a refreshed access token is used for that one call but not persisted, so the next call re-refreshes if still needed. Acceptable for Phase 2: the access token still has its full 15-minute Phase 1 lifetime; this only costs one extra `/auth/refresh` round-trip in the rare case a request lands exactly after expiry.

- [ ] **Step 1: Create `web/lib/nest-fetch.ts`**

```typescript
import { cookies } from 'next/headers';
import { NEST_URL } from './nest';

export async function nestFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const cookieStore = cookies();
  let accessToken = cookieStore.get('access_token')?.value;
  const refreshToken = cookieStore.get('refresh_token')?.value;

  const doFetch = (token?: string) =>
    fetch(`${NEST_URL}${path}`, {
      ...init,
      headers: { ...init.headers, Authorization: `Bearer ${token ?? ''}` },
    });

  let response = await doFetch(accessToken);

  if (response.status === 401 && refreshToken) {
    const refreshRes = await fetch(`${NEST_URL}/auth/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refreshToken }),
    });
    if (refreshRes.ok) {
      const tokens = await refreshRes.json();
      accessToken = tokens.accessToken;
      response = await doFetch(accessToken);
    }
  }

  return response;
}
```

- [ ] **Step 2: Create `web/app/api/ws-ticket/route.ts`**

```typescript
import { NextResponse } from 'next/server';
import { nestFetch } from '../../../lib/nest-fetch';

export async function GET() {
  const nestResponse = await nestFetch('/auth/ws-ticket', { method: 'POST' });
  if (!nestResponse.ok) {
    return NextResponse.json({ error: 'Not authenticated' }, { status: 401 });
  }
  return NextResponse.json(await nestResponse.json());
}
```

- [ ] **Step 3: Create `web/app/api/rooms/route.ts`**

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { nestFetch } from '../../../lib/nest-fetch';

export async function GET() {
  const nestResponse = await nestFetch('/rooms');
  return NextResponse.json(await nestResponse.json(), { status: nestResponse.status });
}

export async function POST(request: NextRequest) {
  const body = await request.json();
  const nestResponse = await nestFetch('/rooms', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return NextResponse.json(await nestResponse.json(), { status: nestResponse.status });
}
```

- [ ] **Step 4: Create `web/app/api/rooms/[roomId]/join/route.ts`**

```typescript
import { NextResponse } from 'next/server';
import { nestFetch } from '../../../../../lib/nest-fetch';

export async function POST(_request: Request, { params }: { params: { roomId: string } }) {
  const nestResponse = await nestFetch(`/rooms/${params.roomId}/join`, { method: 'POST' });
  return NextResponse.json(await nestResponse.json(), { status: nestResponse.status });
}
```

- [ ] **Step 5: Create `web/app/api/rooms/[roomId]/messages/route.ts`**

```typescript
import { NextRequest, NextResponse } from 'next/server';
import { nestFetch } from '../../../../../lib/nest-fetch';

export async function GET(request: NextRequest, { params }: { params: { roomId: string } }) {
  const search = request.nextUrl.search; // forwards ?cursor&limit as-is
  const nestResponse = await nestFetch(`/rooms/${params.roomId}/messages${search}`);
  return NextResponse.json(await nestResponse.json(), { status: nestResponse.status });
}
```

- [ ] **Step 6: Verify it builds**

Run (from `web/`): `npm run build`
Expected: exits 0.

- [ ] **Step 7: Commit**

```bash
git add web/lib/nest-fetch.ts web/app/api/ws-ticket/route.ts web/app/api/rooms
git commit -m "feat: add BFF proxy routes for ws-ticket and rooms"
```

---

### Task 12: Register/login pages (no TDD — UI)

**Files:**
- Create: `web/app/register/page.tsx`, `web/app/login/page.tsx`

**Interfaces:**
- Consumes: `POST /api/register`, `POST /api/login` (Task 10).

- [ ] **Step 1: Create `web/app/register/page.tsx`**

```tsx
'use client';
import { useState } from 'react';
import { useRouter } from 'next/navigation';

export default function RegisterPage() {
  const router = useRouter();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [displayName, setDisplayName] = useState('');
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    const res = await fetch('/api/register', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password, displayName }),
    });
    if (!res.ok) {
      const body = await res.json();
      setError(body.message ?? 'Register failed');
      return;
    }
    router.push('/chat');
  }

  return (
    <form onSubmit={handleSubmit}>
      <h1>Register</h1>
      {error && <p style={{ color: 'red' }}>{error}</p>}
      <input placeholder="Email" value={email} onChange={(e) => setEmail(e.target.value)} />
      <input
        placeholder="Display name"
        value={displayName}
        onChange={(e) => setDisplayName(e.target.value)}
      />
      <input
        placeholder="Password"
        type="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
      />
      <button type="submit">Register</button>
    </form>
  );
}
```

- [ ] **Step 2: Create `web/app/login/page.tsx`**

```tsx
'use client';
import { useState } from 'react';
import { useRouter } from 'next/navigation';

export default function LoginPage() {
  const router = useRouter();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setError(null);
    const res = await fetch('/api/login', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ email, password }),
    });
    if (!res.ok) {
      const body = await res.json();
      setError(body.message ?? 'Login failed');
      return;
    }
    router.push('/chat');
  }

  return (
    <form onSubmit={handleSubmit}>
      <h1>Login</h1>
      {error && <p style={{ color: 'red' }}>{error}</p>}
      <input placeholder="Email" value={email} onChange={(e) => setEmail(e.target.value)} />
      <input
        placeholder="Password"
        type="password"
        value={password}
        onChange={(e) => setPassword(e.target.value)}
      />
      <button type="submit">Login</button>
      <p>
        No account? <a href="/register">Register</a>
      </p>
    </form>
  );
}
```

- [ ] **Step 3: Verify it builds**

Run (from `web/`): `npm run build`
Expected: exits 0.

- [ ] **Step 4: Commit**

```bash
git add web/app/register/page.tsx web/app/login/page.tsx
git commit -m "feat: add register/login pages"
```

---

### Task 13: Chat page — realtime UI (no TDD — UI)

**Files:**
- Create: `web/app/chat/page.tsx`, `web/app/chat/chat-client.tsx`

**Interfaces:**
- Consumes: `GET /rooms` (via cookie directly, Server Component), `/api/rooms`, `/api/rooms/:roomId/join`, `/api/rooms/:roomId/messages`, `/api/ws-ticket` (Tasks 10-11), Socket.IO events `room:subscribe`, `message:send`, `message:new` (Tasks 5-6).

- [ ] **Step 1: Create `web/app/chat/page.tsx`**

```tsx
import { cookies } from 'next/headers';
import { redirect } from 'next/navigation';
import { NEST_URL } from '../../lib/nest';
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

  const res = await fetch(`${NEST_URL}/rooms`, {
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

- [ ] **Step 2: Create `web/app/chat/chat-client.tsx`**

```tsx
'use client';
import { useEffect, useRef, useState } from 'react';
import { io, Socket } from 'socket.io-client';

interface Room {
  id: string;
  name: string;
}

interface Message {
  id: string;
  roomId: string;
  senderId: string;
  content: string;
  createdAt: string;
}

export function ChatClient({ initialRooms }: { initialRooms: Room[] }) {
  const [rooms, setRooms] = useState<Room[]>(initialRooms);
  const [activeRoomId, setActiveRoomId] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [newRoomName, setNewRoomName] = useState('');
  const [joinRoomId, setJoinRoomId] = useState('');
  const [draft, setDraft] = useState('');
  const socketRef = useRef<Socket | null>(null);
  const activeRoomIdRef = useRef<string | null>(null);

  useEffect(() => {
    activeRoomIdRef.current = activeRoomId;
  }, [activeRoomId]);

  useEffect(() => {
    let socket: Socket;
    (async () => {
      const res = await fetch('/api/ws-ticket');
      const { ticket } = await res.json();
      socket = io(process.env.NEXT_PUBLIC_NEST_URL ?? 'http://localhost:3000', {
        auth: { ticket },
      });
      socket.on('message:new', (message: Message) => {
        if (message.roomId === activeRoomIdRef.current) {
          setMessages((prev) => [...prev, message]);
        }
      });
      socketRef.current = socket;
    })();

    return () => {
      socket?.disconnect();
    };
  }, []);

  async function selectRoom(roomId: string) {
    setActiveRoomId(roomId);
    socketRef.current?.emit('room:subscribe', { roomId });

    const res = await fetch(`/api/rooms/${roomId}/messages`);
    const { items } = await res.json();
    setMessages(items);
  }

  async function createRoom() {
    const res = await fetch('/api/rooms', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: newRoomName }),
    });
    const room: Room = await res.json();
    setRooms((prev) => [...prev, room]);
    setNewRoomName('');
  }

  async function joinRoom() {
    await fetch(`/api/rooms/${joinRoomId}/join`, { method: 'POST' });
    const res = await fetch('/api/rooms');
    setRooms(await res.json());
    setJoinRoomId('');
  }

  function sendMessage() {
    if (!activeRoomId || !draft.trim()) return;
    socketRef.current?.emit('message:send', { roomId: activeRoomId, content: draft }, () => {
      setDraft('');
    });
  }

  return (
    <div style={{ display: 'flex', gap: '2rem' }}>
      <aside>
        <h2>Rooms</h2>
        <ul>
          {rooms.map((room) => (
            <li key={room.id}>
              <button onClick={() => selectRoom(room.id)}>{room.name}</button>
            </li>
          ))}
        </ul>
        <input
          value={newRoomName}
          onChange={(e) => setNewRoomName(e.target.value)}
          placeholder="New room name"
        />
        <button onClick={createRoom}>Create room</button>
        <input
          value={joinRoomId}
          onChange={(e) => setJoinRoomId(e.target.value)}
          placeholder="Room id to join"
        />
        <button onClick={joinRoom}>Join room</button>
      </aside>
      <main>
        <ul>
          {messages.map((m) => (
            <li key={m.id}>{m.content}</li>
          ))}
        </ul>
        {activeRoomId && (
          <div>
            <input value={draft} onChange={(e) => setDraft(e.target.value)} />
            <button onClick={sendMessage}>Send</button>
          </div>
        )}
      </main>
    </div>
  );
}
```

- [ ] **Step 3: Verify it builds**

Run (from `web/`): `npm run build`
Expected: exits 0.

- [ ] **Step 4: Commit**

```bash
git add web/app/chat/page.tsx web/app/chat/chat-client.tsx
git commit -m "feat: add realtime chat page"
```

---

### Task 14: End-to-end verification (acceptance)

**Files:** none — verification only.

**Interfaces:** none — validates every completion criterion in the spec.

- [ ] **Step 1: Backend automated check**

Run (repo root): `npm run typecheck && npm test`
Expected: both exit 0; 32 tests pass (10 auth + 13 rooms + 9 events).

- [ ] **Step 2: Frontend automated check**

Run (from `web/`): `npm run typecheck && npm run build`
Expected: both exit 0.

- [ ] **Step 3: Start both apps**

```bash
# terminal 1 (repo root)
npm run build && node dist/main.js

# terminal 2 (web/)
npm run build && npm run start
```

- [ ] **Step 4: Verify the ticket is one-time via curl**

```bash
TOKENS=$(curl -s -X POST http://localhost:3000/auth/register -H "Content-Type: application/json" \
  -d '{"email":"e2e@example.com","password":"password123","displayName":"E2E"}')
ACCESS=$(echo "$TOKENS" | node -e "process.stdin.once('data', d => console.log(JSON.parse(d).accessToken))")
TICKET=$(curl -s -X POST http://localhost:3000/auth/ws-ticket -H "Authorization: Bearer $ACCESS" \
  | node -e "process.stdin.once('data', d => console.log(JSON.parse(d).ticket))")
node -e "
const { io } = require('socket.io-client');
const s1 = io('http://localhost:3000', { auth: { ticket: '$TICKET' } });
s1.on('connect', () => { console.log('first connect: OK'); s1.disconnect();
  const s2 = io('http://localhost:3000', { auth: { ticket: '$TICKET' } });
  s2.on('connect', () => console.log('REUSE SUCCEEDED (BUG)'));
  s2.on('connect_error', () => console.log('reuse rejected: OK'));
});
"
```

Expected output: `first connect: OK` then `reuse rejected: OK`.

- [ ] **Step 5: Verify realtime end-to-end in the browser**

1. Open `http://localhost:3001/register`, create user A, land on `/chat`.
2. In a second browser profile/incognito window, register user B.
3. As user A, create a room, note its id, send it to user B out of band.
4. As user B, enter that room id in "Room id to join" → Join room → select it.
5. As user A, select the same room, type a message, Send.
6. Confirm the message appears in **both** windows without a page refresh.
7. Confirm `GET http://localhost:3000/rooms/<roomId>/messages` (with either user's access token) returns that message — proving it was persisted via the same `RoomsService.sendMessage` path as REST.

- [ ] **Step 6: Stop both servers**

Stop both `node dist/main.js` and the Next.js `npm run start` process (Ctrl+C in each terminal).

---

## Completion Criteria (from spec)

- [ ] `POST /auth/ws-ticket` issues a one-time ticket; reuse is rejected (Task 8, Task 14 Step 4).
- [ ] Socket.IO connect succeeds with a valid ticket; fails for invalid/expired/reused tickets (Task 4).
- [ ] `room:subscribe` correctly blocks a non-member via ack error, without affecting other rooms (Task 5).
- [ ] Sending `message:send` delivers `message:new` to the sender and every other subscriber in the room; the message is readable via REST `GET .../messages` afterward (Task 6, Task 14 Step 5/7).
- [ ] The Next.js app (`web/`) works end-to-end: register → login → create room → two users joined to the same room → realtime message delivery across both (Task 9-13, Task 14 Step 5).
- [ ] `npx tsc --noEmit` is clean in both `src/` and `web/`, and all TDD tests pass (Task 14 Step 1-2).
