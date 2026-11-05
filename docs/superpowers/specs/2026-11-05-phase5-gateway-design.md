# Phase 5 — Tách gateway (WS + HTTP proxy)

Sub-project của roadmap tổng (`2026-10-10-realtime-chat-v2-roadmap-design.md`),
tiếp theo Phase 4 (`2026-11-05-phase4-auth-service-design.md`).

## Mục tiêu

Tách lớp WebSocket + HTTP proxy ra thành service **gateway** riêng, không còn
chạy chung process với chat-service (rooms/messages). Gateway trở thành
**điểm vào duy nhất** cho `web/` — mọi request REST lẫn kết nối WS đều qua
gateway, gateway route tiếp sang `auth-service` hoặc `chat-service` (phần còn
lại của app gốc, nay chỉ còn rooms/messages/presence-read, không còn WS).

Vì chat-service không còn giữ socket nào, nó không thể tự `emit()` khi có
message mới — phải **publish qua Redis pub/sub** để gateway (đang giữ toàn bộ
socket) emit hộ vào đúng room. Đây là lúc áp lại đúng bài học Module 3: service
không chạy WS phải đẩy update qua message broker, không giữ socket chéo
service.

## Quyết định đã chốt (brainstorm)

| Chủ đề | Quyết định | Phương án đã loại |
|---|---|---|
| Vai trò gateway với REST | **Proxy đơn thuần** — route theo path, forward nguyên `Authorization` header, không tự verify. `auth-service`/`chat-service` vẫn tự verify JWT qua JWKS như hiện tại, không đổi gì | Gateway tập trung verify rồi inject identity xuống dưới (thêm phức tạp, chat-service mất khả năng tự vệ nếu bị gọi thẳng) |
| Gateway gọi WS→chat-service (không có JWT thật) | Header nội bộ `X-Internal-Secret` (shared secret qua env) + `X-Internal-User-Id`, guard riêng ở chat-service | Chỉ dựa network isolation (không có lớp phòng thủ thứ 2) |
| `web/` trỏ backend nào | Bỏ hẳn `AUTH_URL`/`NEST_URL`, chỉ còn 1 `GATEWAY_URL` — mọi request qua gateway | Giữ gọi thẳng auth-service/chat-service, chỉ WS qua gateway (gateway không thực sự được kiểm chứng vai trò HTTP proxy) |

## Phạm vi

**Có trong Phase 5:**
- `gateway/`: NestJS app độc lập — `EventsGateway` (chuyển từ `src/realtime/`),
  `WsTicketController`/`WsTicketService` (chuyển từ chat-service — gateway là
  nơi duy nhất verify WS handshake nên ws-ticket phải ở cùng chỗ), HTTP reverse
  proxy cho `/auth/*` và `/rooms/*`, `JwksService`/`JwtStrategy`/`JwtAuthGuard`
  riêng (copy từ chat-service — cần để tự verify JWT cho chính endpoint
  `POST /ws-ticket`, endpoint này **không proxy**, gateway xử lý trực tiếp).
- chat-service: 2 endpoint nội bộ mới (`InternalAuthGuard`), route
  `POST /rooms/:roomId/messages` đổi guard thành `JwtOrInternalAuthGuard`,
  `RoomsService.sendMessage()` publish Redis sau khi lưu DB. Xoá
  `src/realtime/events.gateway.ts` và `ws-ticket.*`. `PresenceService` chỉ còn
  phần đọc (`onlineAmong`) — phần ghi (`connect`/`disconnect`/`touch`) chuyển
  sang gateway.
- `web/`: bỏ `AUTH_URL`/`NEST_URL`, thêm `GATEWAY_URL`; mọi fetch + Socket.IO
  connect đổi sang gateway.
- `docker-compose.yml`: không đổi (Redis/Postgres dùng chung, không có service
  mới cần container).
- `INTERNAL_SECRET` — biến môi trường chung giữa `gateway/` và `src/`.

**Không có trong Phase 5:**
- RabbitMQ, notification-service (Phase 6).
- Rate limiting / circuit breaker cho lời gọi nội bộ gateway→chat-service.
- Network isolation thật (chặn chat-service không cho gọi trực tiếp từ bên
  ngoài) — Phase 9 (deploy) mới xử lý; ở Phase 5 tất cả vẫn chạy local, mọi
  port đều mở, `INTERNAL_SECRET` là lớp phòng thủ duy nhất.
- Đổi cơ chế presence broadcast cross-instance (vẫn dùng
  `@socket.io/redis-adapter` có sẵn từ Phase 3, không đổi sang pub/sub thủ
  công như message — chỉ `message:new` mới cần pub/sub mới, vì đây là
  trường hợp duy nhất mà *chat-service* (không phải gateway) cần kích hoạt
  emit).

## Kiến trúc & cấu trúc repo

```
realtime-chat-v2/
├── gateway/                       # MỚI — NestJS app độc lập, port mặc định 3020
│   ├── package.json  tsconfig*.json  nest-cli.json  jest.config.js
│   ├── src/
│   │   ├── auth/                  # JwksService/JwtStrategy/JwtAuthGuard
│   │   │                          # (copy từ chat-service, cùng AUTH_JWKS_URL)
│   │   ├── realtime/
│   │   │   ├── events.gateway.ts          # chuyển từ src/realtime/
│   │   │   ├── ws-ticket.controller.ts    # chuyển từ src/realtime/
│   │   │   ├── ws-ticket.service.ts       # chuyển từ src/realtime/
│   │   │   ├── message-subscriber.service.ts  # MỚI — Redis subscribe
│   │   │   └── internal-chat.client.ts    # MỚI — gọi 3 API nội bộ
│   │   ├── redis/                 # RedisModule (copy nguyên từ chat-service)
│   │   ├── proxy/
│   │   │   └── proxy.controller.ts        # MỚI — reverse proxy /auth/*, /rooms/*
│   │   ├── app.module.ts
│   │   └── main.ts
│   └── test/
├── src/                            # chat-service (rooms/messages/presence-read)
│   ├── auth/                       # không đổi (JwksService verify JWT)
│   │   ├── internal-auth.guard.ts          # MỚI
│   │   └── jwt-or-internal-auth.guard.ts   # MỚI
│   ├── realtime/
│   │   ├── presence.service.ts     # THU HẸP — chỉ còn onlineAmong()
│   │   └── presence.controller.ts  # không đổi
│   │   # XOÁ: events.gateway.ts, ws-ticket.controller.ts, ws-ticket.service.ts
│   └── rooms/
│       ├── rooms.controller.ts     # sendMessage đổi guard
│       ├── internal.controller.ts  # MỚI — 2 endpoint nội bộ
│       └── rooms.service.ts        # sendMessage() thêm publish Redis
├── auth-service/                   # không đổi
└── web/                             # bỏ AUTH_URL/NEST_URL, thêm GATEWAY_URL
```

`tsconfig.json`/`tsconfig.build.json` ở root loại thêm `gateway` (giống cách
đã loại `web` và `auth-service`).

## Luồng REST (gateway proxy đơn thuần)

`gateway/src/proxy/proxy.controller.ts` — bắt mọi method/path dưới `/auth/*`
và `/rooms/*`, forward nguyên `Authorization` header + body sang
`AUTH_SERVICE_URL`/`CHAT_SERVICE_URL` tương ứng, trả nguyên status + body về
client. Không đụng vào `/ws-ticket` (route riêng, xử lý trực tiếp ở gateway,
xem phần dưới).

```typescript
@Controller()
export class ProxyController {
  constructor(private readonly http: HttpService) {}

  @All('auth/*')
  proxyAuth(@Req() req: Request, @Res() res: Response) {
    return this.forward(req, res, process.env.AUTH_SERVICE_URL ?? 'http://localhost:3010');
  }

  @All('rooms/*')
  @All('rooms')
  proxyRooms(@Req() req: Request, @Res() res: Response) {
    return this.forward(req, res, process.env.CHAT_SERVICE_URL ?? 'http://localhost:3000');
  }

  private async forward(req: Request, res: Response, target: string) {
    const url = `${target}${req.originalUrl}`;
    const response = await firstValueFrom(
      this.http.request({
        method: req.method,
        url,
        data: req.body,
        headers: { authorization: req.headers.authorization },
        validateStatus: () => true,
      }),
    );
    res.status(response.status).json(response.data);
  }
}
```

## Luồng WS

### `POST /ws-ticket` (chuyển hẳn vào gateway, không proxy)

Giữ nguyên logic Phase 2/3 (`JwtAuthGuard` → `WsTicketService.issue` → Redis
`SET ... EX 30`), chỉ đổi chỗ ở. Gateway cần `JwksService`/`JwtStrategy` riêng
cho route này.

### Kết nối WS (`afterInit` middleware)

Không đổi — vẫn consume ticket qua `WsTicketService.consume` (Redis, giờ nằm
trong gateway), set `socket.data.userId`.

### `room:subscribe`

Trước: `roomsService.assertMember(userId, roomId)` gọi trực tiếp. Sau: gọi
nội bộ `GET /internal/rooms/:roomId/members/:userId` ở chat-service.

```typescript
@SubscribeMessage('room:subscribe')
async handleSubscribe(@ConnectedSocket() client: Socket, @MessageBody() body: { roomId: string }) {
  const { isMember } = await this.internalClient.checkMembership(body.roomId, client.data.userId);
  if (!isMember) return { success: false, error: 'Not a member of this room' };
  await client.join(body.roomId);
  return { success: true };
}
```

### `message:send`

Gọi nội bộ `POST /rooms/:roomId/messages` (route public có sẵn, qua
`JwtOrInternalAuthGuard`), **không tự `.emit()`** — chỉ trả ack theo kết quả
HTTP. Việc emit do `MessageSubscriberService` đảm nhiệm (xem phần Redis
pub/sub).

```typescript
@SubscribeMessage('message:send')
async handleMessageSend(@ConnectedSocket() client: Socket, @MessageBody() body: { roomId: string; content: string }) {
  if (!body?.content?.trim()) return { success: false, error: 'content must not be empty' };
  try {
    await this.internalClient.sendMessage(body.roomId, client.data.userId, body.content);
    return { success: true };
  } catch (err) {
    return { success: false, error: extractErrorMessage(err) }; // map lỗi HTTP (403/404) thành message
  }
}
```

### Presence connect/disconnect

Trước: `roomsService.listRoomIdsOf(userId)` gọi trực tiếp. Sau: gọi nội bộ
`GET /internal/users/:userId/room-ids`. Phần còn lại (ghi `presence:*` vào
Redis, broadcast `presence:update` qua `server.to(roomIds).emit(...)` dựa
trên `@socket.io/redis-adapter` có sẵn) **không đổi**.

## API nội bộ + `InternalAuthGuard` / `JwtOrInternalAuthGuard`

Biến môi trường `INTERNAL_SECRET` (chuỗi ngẫu nhiên ≥32 ký tự, giống nhau ở
`gateway/.env` và `.env` gốc). Mọi lời gọi nội bộ kèm header
`X-Internal-Secret`.

```typescript
// src/auth/internal-auth.guard.ts
@Injectable()
export class InternalAuthGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const req = context.switchToHttp().getRequest();
    if (req.headers['x-internal-secret'] !== process.env.INTERNAL_SECRET) {
      throw new UnauthorizedException('Invalid internal secret');
    }
    return true;
  }
}
```

```typescript
// src/auth/jwt-or-internal-auth.guard.ts
@Injectable()
export class JwtOrInternalAuthGuard implements CanActivate {
  constructor(private readonly jwtGuard: JwtAuthGuard) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest();
    if (req.headers['x-internal-secret'] === process.env.INTERNAL_SECRET) {
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

`src/rooms/internal.controller.ts` (2 endpoint mới, chỉ `InternalAuthGuard`):

```typescript
@Controller('internal')
@UseGuards(InternalAuthGuard)
export class InternalController {
  constructor(private readonly roomsService: RoomsService) {}

  @Get('rooms/:roomId/members/:userId')
  async checkMembership(@Param('roomId') roomId: string, @Param('userId') userId: string) {
    return { isMember: await this.roomsService.isMember(userId, roomId) };
  }

  @Get('users/:userId/room-ids')
  async roomIds(@Param('userId') userId: string) {
    return { roomIds: await this.roomsService.listRoomIdsOf(userId) };
  }
}
```

`RoomsService` thêm `isMember()` (dùng chung logic với `assertMember()`, chỉ
khác là trả `boolean` thay vì throw — tránh lặp code bằng cách để
`assertMember` gọi `isMember` nội bộ).

`src/rooms/rooms.controller.ts`: route `sendMessage` đổi
`@UseGuards(JwtAuthGuard)` cấp class → giữ `JwtAuthGuard` ở class, override
riêng route này bằng `@UseGuards(JwtOrInternalAuthGuard)` (NestJS cho phép
override guard ở method-level, guard method-level thắng guard class-level).

## Redis pub/sub cho `message:new`

Channel: `chat:message:new`. Payload JSON: `{ roomId: string, message: Message }`.

`src/rooms/rooms.service.ts` — publish sau khi tạo message thành công:

```typescript
async sendMessage(userId: string, roomId: string, dto: SendMessageDto): Promise<Message> {
  await this.assertMember(userId, roomId);
  const message = await this.prisma.message.create({
    data: { roomId, senderId: userId, content: dto.content },
  });
  await this.redis.publish('chat:message:new', JSON.stringify({ roomId, message }));
  return message;
}
```

`gateway/src/realtime/message-subscriber.service.ts` — subscriber riêng (ioredis
yêu cầu 1 connection độc lập ở chế độ subscribe, không dùng chung với lệnh
thường):

```typescript
@Injectable()
export class MessageSubscriberService implements OnModuleInit, OnModuleDestroy {
  constructor(
    @Inject(REDIS_SUB) private readonly sub: Redis,
    private readonly gateway: EventsGateway,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.sub.subscribe('chat:message:new');
    this.sub.on('message', (_channel: string, raw: string) => {
      const { roomId, message } = JSON.parse(raw) as { roomId: string; message: unknown };
      this.gateway.server.to(roomId).emit('message:new', message);
    });
  }

  async onModuleDestroy(): Promise<void> {
    await this.sub.unsubscribe('chat:message:new');
  }
}
```

`REDIS_SUB` là provider riêng trong `gateway/src/redis/redis.module.ts` —
`redis.duplicate()` của connection chính (ioredis convention chuẩn cho
pub/sub: 1 connection để `publish`/lệnh thường, 1 connection riêng chỉ để
`subscribe`).

## Thay đổi ở `web/`

- Bỏ `lib/nest.ts` (`NEST_URL`) và `lib/auth.ts` (`AUTH_URL`) → gộp thành
  `lib/gateway.ts` (`GATEWAY_URL`, mặc định `http://localhost:3020`).
- Mọi chỗ gọi `NEST_URL`/`AUTH_URL` trong route handlers (`nest-fetch.ts`,
  `middleware.ts`, các `api/*/route.ts`) đổi sang `GATEWAY_URL`.
- Socket.IO client (`chat-client.tsx` hoặc tương đương) connect vào
  `GATEWAY_URL` thay vì `NEST_URL`.
- Logic cookie/refresh-token không đổi — chỉ đổi base URL, path giữ nguyên
  (`/auth/login`, `/auth/refresh`, `/rooms/...`, `/ws-ticket`).

## Testing strategy (TDD với I/O thật — giữ quy ước Phase 1-4)

Không mock Prisma/Redis/socket/HTTP. Postgres + Redis thật.

**chat-service (`chat_test`):**
- `InternalAuthGuard`: secret đúng → pass; sai/thiếu → 401. Test thật qua
  `supertest`-style (bootstrap app thật, gọi HTTP thật).
- `JwtOrInternalAuthGuard`: JWT thật hợp lệ → pass (giữ hành vi cũ); header
  nội bộ hợp lệ → pass, `req.user.id` đúng; cả hai đều thiếu/sai → 401.
- `POST /internal/rooms/:roomId/members/:userId`, `GET
  /internal/users/:userId/room-ids`: test trực tiếp qua HTTP thật, có/không
  secret.
- `RoomsService.sendMessage()`: subscribe tạm channel `chat:message:new`
  trong test, gọi `sendMessage`, assert nhận đúng payload (roomId + message
  object khớp bản ghi vừa tạo) — test thật qua Redis, không mock publish.
- Test cũ của `rooms.controller`/`rooms.service` giữ nguyên hành vi (chạy lại
  xác nhận không vỡ sau khi đổi guard route `sendMessage`).

**gateway (jest riêng, `test/`):**
- `WsTicketService`/`WsTicketController`: y hệt test đã có ở chat-service
  trước đây (chuyển nguyên file).
- `EventsGateway` connection auth, `room:subscribe`/`room:unsubscribe`: bootstrap
  **2 app NestJS thật trong 1 test process** — 1 instance gateway (port ngẫu
  nhiên) + 1 instance chat-service thật (port ngẫu nhiên, Postgres test thật)
  — gateway gọi HTTP thật sang chat-service test instance (không mock
  `internal-chat.client.ts`). `INTERNAL_SECRET` test cố định trong `.env.test`
  của cả 2.
- `message:send` → `message:new`: test end-to-end thật — gửi `message:send`
  qua socket.io-client thật, assert HTTP nội bộ gọi đúng chat-service test
  instance (DB có bản ghi), **và** client WS khác đã subscribe room nhận được
  `message:new` qua Redis pub/sub thật (không mock `MessageSubscriberService`).
- Presence connect/disconnect vẫn test như Phase 3 (giờ gọi thêm HTTP nội bộ
  lấy `roomIds` thay vì gọi `RoomsService` trực tiếp — bootstrap chat-service
  test instance tương tự).

**Smoke cuối (thủ công, không TDD):** chạy 3 process thật (`auth-service`,
chat-service gốc, `gateway`) + `web/`, verify: đăng ký/đăng nhập qua gateway
→ auth-service; tạo room/gửi message REST qua gateway → chat-service; mở 2
tab, 1 tab gửi message qua WS, tab kia nhận `message:new` realtime; gửi
message bằng `curl` thẳng vào `POST /rooms/:id/messages` qua gateway (không
qua WS) — xác nhận tab đang mở WS **vẫn** nhận được `message:new` (đây là
hành vi mới Phase 5 thêm, REST cũng realtime).

**Không cần TDD:** `web/`, `proxy.controller.ts` (chỉ forward, verify bằng
smoke test), bootstrap `main.ts` của gateway.

## Rủi ro và đánh đổi

- **`INTERNAL_SECRET` là lớp phòng thủ duy nhất** cho API nội bộ ở Phase 5 —
  chưa có network isolation thật (Phase 9). Nếu secret lộ, bất kỳ ai gọi được
  chat-service đều giả danh được user bất kỳ qua `JwtOrInternalAuthGuard`.
- **Gateway là single point of failure mới** — toàn bộ traffic (kể cả REST)
  giờ qua 1 process. Chưa có retry/circuit breaker khi `auth-service` hoặc
  chat-service down giữa lúc proxy (lỗi sẽ trả nguyên status/lỗi từ
  `axios`/`HttpService`, chưa xử lý đẹp — chấp nhận ở Phase 5, có thể cải
  thiện ở Phase 9 cross-cutting).
- **Độ trễ thêm 1 hop mạng** cho `room:subscribe`/`message:send` (gateway →
  chat-service, thêm round-trip HTTP so với gọi trực tiếp trong process như
  trước) — chấp nhận được ở quy mô học tập/demo.
- **Thứ tự publish/ack không đảm bảo tuyệt đối thấy ngay** — HTTP response
  (ack) trả về gần như đồng thời với Redis publish, nhưng về lý thuyết
  subscriber có thể emit **trước** khi HTTP response tới client (network
  jitter) — client nhận `message:new` trước khi nhận ack `success:true`. Chấp
  nhận được (không ảnh hưởng tính đúng đắn, chỉ là thứ tự 2 sự kiện độc lập).

## Tiêu chí hoàn thành Phase 5

- [ ] `gateway/` chạy độc lập, không import bất kỳ gì từ `src/` (chat-service)
      — chỉ giao tiếp qua HTTP/Redis.
- [ ] REST qua gateway hoạt động y hệt trước (proxy đơn thuần, JWT thật verify
      ở đúng service đích, không đổi hành vi auth).
- [ ] WS kết nối qua gateway bằng ws-ticket (issue tại gateway); `room:subscribe`
      từ chối đúng user không phải member (gọi nội bộ, không lộ lỗi 500).
- [ ] `message:send` qua WS: message được lưu ở chat-service, mọi client đã
      subscribe room (kể cả ở instance gateway khác, nếu test multi-instance)
      nhận được `message:new` qua Redis pub/sub.
- [ ] Gửi message qua REST thuần (không qua WS) **cũng** làm các client WS
      đang mở nhận được `message:new` — xác nhận gap cũ đã được fix.
- [ ] Presence (Phase 3) vẫn hoạt động đúng sau khi chuyển sang gọi nội bộ lấy
      `roomIds`.
- [ ] API nội bộ (`/internal/*`, `JwtOrInternalAuthGuard`) từ chối đúng khi
      thiếu/sai `X-Internal-Secret`; JWT thật vẫn gọi được route
      `sendMessage` bình thường (không bị guard mới chặn nhầm).
- [ ] `web/` chạy end-to-end chỉ qua `GATEWAY_URL`, không còn gọi thẳng
      `AUTH_URL`/`NEST_URL` ở đâu.
- [ ] `npx tsc --noEmit` sạch (gateway, chat-service, auth-service, `web/`);
      toàn bộ test 3 service pass với Postgres + Redis thật; smoke 3 process +
      web pass.
