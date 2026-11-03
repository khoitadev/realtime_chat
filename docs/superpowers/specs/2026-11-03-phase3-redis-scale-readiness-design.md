# Phase 3 — Scale-readiness: Redis adapter + Presence dùng Redis Set

Sub-project của roadmap tổng (`2026-10-10-realtime-chat-v2-roadmap-design.md`),
tiếp theo Phase 2 (`2026-11-01-phase2-realtime-websocket-design.md`).

## Mục tiêu

Cho phép chạy **nhiều instance** NestJS (Socket.IO) sau load balancer mà vẫn
đúng:

1. Một message gửi qua instance A phải tới được client đang kết nối vào
   instance B (Redis adapter).
2. WS ticket cấp bởi instance A phải dùng được ở instance B (ticket chuyển
   từ `Map` in-memory sang Redis — Phase 2 đã ghi chú sẽ làm ở đây).
3. **Presence** (ai đang online) đúng khi 1 user mở nhiều tab / nhiều device
   cùng lúc, và đúng khi 1 instance bị crash.

Bug cần tránh lặp lại từ bản gốc: dùng **1 string** cho presence
(`presence:{userId} = "online"`) — đóng 1 tab là `DEL` key → user bị coi
là offline dù tab còn lại vẫn mở. Cách đúng: mỗi user có 1 **Redis Set** chứa
`socketId` của mọi kết nối đang sống; user online khi Set còn ít nhất 1
socketId còn sống.

## Quyết định đã chốt (brainstorm)

| Chủ đề | Quyết định | Phương án đã loại |
|---|---|---|
| Phạm vi presence | Global (`presence:user:{userId}`) + truy vấn theo room (lấy member từ DB, pipeline kiểm tra online) | Set theo room (phải ghi nhiều key, "online" phụ thuộc subscribe); chỉ hạ tầng không có API |
| Crash safety | Heartbeat key có TTL + lọc socketId chết khi đọc (lazy cleanup, Lua atomic) | Set theo instance + TTL (cần SCAN); bỏ qua crash |
| Redis adapter | `@socket.io/redis-adapter` (pub/sub) | `redis-streams-adapter`; sharded adapter (cần Redis Cluster) |
| Transport | `websocket`-only ở server và client → không cần sticky session | Giữ polling + yêu cầu sticky ở LB |
| Ticket store | Redis `SET ... EX 30` + `GETDEL` (atomic, one-time) | — |
| Redis client | `ioredis` | `node-redis` (adapter hỗ trợ cả hai; chọn ioredis vì phổ biến trong hệ NestJS) |

## Phạm vi

**Có trong Phase 3:**
- `RedisModule` (global): provider `REDIS` (ioredis) từ `REDIS_URL`.
- `RedisIoAdapter`: gắn `@socket.io/redis-adapter` vào Socket.IO server;
  dùng ở `main.ts` và trong test app.
- `WsTicketService` chuyển sang Redis (async `issue`/`consume`).
- `PresenceService` (Redis Set + heartbeat) và tích hợp vào `EventsGateway`.
- Event `presence:update` (server→client broadcast) và endpoint
  `GET /rooms/:roomId/presence`.
- Heartbeat refresh định kỳ + dọn socket của instance khi graceful shutdown.
- Client `web/`: `transports: ['websocket']`; hiển thị chấm online của
  member trong room đang chọn (UI tối thiểu).
- Hạ tầng test: `docker-compose.yml` thêm service `redis`;
  `scripts/test-infra.sh` dựng Postgres + Redis native khi không có Docker.

**Không có trong Phase 3:**
- Typing indicator, "last seen", presence tuỳ chỉnh (away/busy) — Phase 10.
- Broadcast `online:false` cho instance đã crash (xem "Giới hạn đã biết").
- Connection state recovery / streams adapter.
- Redis Cluster, Sentinel, TLS.
- Rate limit, metrics (Phase 9).

## Kiến trúc & cấu trúc repo

```
src/
├── main.ts                          # + app.useWebSocketAdapter(RedisIoAdapter)
├── redis/                           # MODULE MỚI
│   ├── redis.constants.ts           # REDIS injection token
│   ├── redis.module.ts              # @Global, ioredis từ REDIS_URL, quit khi destroy
│   ├── redis-io.adapter.ts          # IoAdapter + createAdapter(pub, sub)
│   └── redis.module.spec.ts
└── realtime/
    ├── ws-ticket.service.ts         # SỬA: Redis, async
    ├── presence.service.ts          # MỚI
    ├── presence.service.spec.ts     # MỚI
    ├── presence.controller.ts       # MỚI: GET /rooms/:roomId/presence
    ├── events.gateway.ts            # SỬA: presence hooks, heartbeat, transports
    └── realtime.module.ts           # SỬA
test/
└── redis-cleanup.ts                 # flush Redis test DB
scripts/
└── test-infra.sh                    # dựng Postgres + Redis native (không Docker)
```

`RoomsService` thêm 1 hàm: `listMemberIds(roomId)` và `listRoomIdsOf(userId)`
(chỉ đọc bảng `RoomMember`, không đổi hành vi hiện có).

## Redis Adapter

```typescript
export class RedisIoAdapter extends IoAdapter {
  constructor(app: INestApplicationContext, private readonly redis: Redis) {
    super(app);
  }

  createIOServer(port: number, options?: ServerOptions): Server {
    const server = super.createIOServer(port, options) as Server;
    server.adapter(createAdapter(this.redis.duplicate(), this.redis.duplicate()));
    return server;
  }
}
```

(pub/sub cần 2 connection riêng vì connection ở subscriber mode không chạy
lệnh thường.) Code cuối cùng sẽ được xác minh với version `socket.io` /
`@socket.io/redis-adapter` thực sự cài trong plan (bài học Phase 2).

Hệ quả: `this.server.to(roomId).emit('message:new', ...)` trong gateway
**không đổi** — adapter tự phát tới socket ở mọi instance. `socket.join` /
`leave` vẫn là local; adapter lo phần fan-out.

### Transport

Server `@WebSocketGateway({ cors: true, transports: ['websocket'] })`, client
`io(url, { transports: ['websocket'], auth: { ticket } })`. Không có HTTP
long-polling nên mọi kết nối là 1 TCP duy nhất tới 1 instance → LB chỉ cần
hỗ trợ WebSocket upgrade, **không cần sticky session**.

## WS Ticket trên Redis

```
issue(userId):  SET ws:ticket:{ticket} {userId} EX 30     → trả ticket
consume(ticket): GETDEL ws:ticket:{ticket}                → userId | null
```

`GETDEL` (Redis ≥ 6.2) vừa đọc vừa xoá trong 1 lệnh atomic → hai instance
cùng consume 1 ticket đồng thời thì đúng 1 bên thắng. TTL do Redis lo, bỏ
hẳn việc ticket hết hạn nằm treo trong RAM. Chữ ký đổi thành
`issue(userId): Promise<string>`, `consume(ticket): Promise<string | null>`;
`AuthController.wsTicket` và `EventsGateway` middleware thêm `await`.

## Presence (Redis Set)

### Key

| Key | Kiểu | Nội dung | TTL |
|---|---|---|---|
| `presence:user:{userId}` | Set | `socketId` của mọi tab/device đang kết nối | 3 × `HEARTBEAT_TTL`, gia hạn mỗi connect/touch (chống rò rỉ khi instance chết và user không quay lại) |
| `presence:socket:{socketId}` | String | giá trị `userId`; là "heartbeat" | `HEARTBEAT_TTL` = 60s |

Set là nguồn sự thật; key heartbeat chỉ để biết 1 `socketId` trong Set còn
sống hay mồ côi (instance sở hữu nó đã chết).

### Thao tác (mỗi thao tác là 1 Lua script → atomic)

- `connect(userId, socketId)`: `SADD` + `SET presence:socket:{id} userId EX
  TTL`. Trước khi `SADD`, dọn socketId chết trong Set (xem `prune`). Trả
  `true` nếu **trước đó user chưa online** (Set rỗng sau khi prune) →
  chuyển offline→online.
- `disconnect(userId, socketId)`: `SREM` + `DEL` heartbeat, rồi `prune`.
  Trả `true` nếu Set rỗng sau đó → chuyển online→offline.
- `touch(userId, socketId)` (heartbeat): `SADD` + `SET ... EX TTL` (idempotent;
  tự chữa lành nếu key heartbeat lỡ hết hạn do event-loop bị đứng lâu).
- `prune(userId)`: duyệt `SMEMBERS`, với mỗi socketId mà `EXISTS
  presence:socket:{id} == 0` thì `SREM`. Là bước chung của `connect`,
  `disconnect`, `isOnline`.
- `isOnline(userId)`: `prune` rồi `SCARD > 0`.
- `onlineAmong(userIds[])`: `isOnline` cho từng user (pipeline script);
  trả danh sách userId online.

Kết quả đúng với bug gốc: user mở tab 1 (`connect` → true, phát
`online:true`), mở tab 2 (`connect` → false, không phát gì), đóng tab 1
(`disconnect` → false, vẫn online), đóng tab 2 (`disconnect` → true, phát
`online:false`).

### Heartbeat & crash

Mỗi instance, mỗi `REFRESH_INTERVAL` = 20s, duyệt socket **local** của chính
mình (`server.sockets.sockets`) và gọi `touch`. Instance chết → ngừng refresh
→ sau ≤ 60s key heartbeat hết hạn → lần đọc/ghi tiếp theo `prune` loại socket
mồ côi. `HEARTBEAT_TTL` / `REFRESH_INTERVAL` cấu hình qua provider
`PRESENCE_OPTIONS` để test dùng giá trị ngắn.

### Graceful shutdown

`EventsGateway.onModuleDestroy` (hoặc `beforeApplicationShutdown`): dừng
timer heartbeat, gọi `disconnect` cho mọi socket local (phát `online:false`
nếu cần), sau đó mới đóng Redis. Nest `enableShutdownHooks()` bật trong
`main.ts`.

### Event & API

- `presence:update` (server→client, broadcast): `{ userId: string, online:
  boolean }`. Phát khi user **chuyển trạng thái** (không phát mỗi tab). Gửi
  tới mọi room mà user là **member** (`RoomsService.listRoomIdsOf`) — qua
  `server.to(roomIds).emit(...)`, nhờ adapter nên tới được client ở mọi
  instance. (Làm rõ so với bản trình bày thiết kế: "room user là member",
  không phải "room đã subscribe", vì lúc vừa connect socket chưa subscribe
  room nào.)
- `GET /rooms/:roomId/presence` (`JwtAuthGuard`, chỉ member, 403 nếu không):
  `{ onlineUserIds: string[] }`. Nguồn sự thật cho client khi mở room hoặc
  reconnect; `presence:update` chỉ là delta.

## Giới hạn đã biết

- `presence:update` là **advisory**: các broadcast của cùng một user có thể đảo thứ tự giữa các instance (mỗi broadcast có 1 truy vấn DB). Client luôn coi `GET .../presence` là nguồn sự thật (web/ re-fetch mỗi khi nhận event).
- Instance crash: `GET .../presence` đúng sau ≤ TTL, nhưng event
  `online:false` **không được phát** (không ai chạy handler). Client xử lý
  bằng cách gọi lại `GET .../presence` khi reconnect/mở room. Có thể bổ
  sung "reaper" định kỳ ở Phase 9.
- Pub/sub là fire-and-forget: instance mất kết nối Redis tạm thời sẽ lỡ
  `message:new`/`presence:update` đang bay; lịch sử vẫn lấy lại được qua REST
  cursor.
- `prune` duyệt toàn Set của 1 user → O(số tab/device của user), nhỏ.

## Testing strategy (TDD với I/O thật — giữ quy ước Phase 1/2)

Không mock Prisma, không mock Redis, không mock socket. Hạ tầng test:
Postgres test (cổng 5433) + Redis (cổng 6379, **database 1**, flush trong
`beforeEach` bằng `test/redis-cleanup.ts`). Môi trường không có Docker dùng
`scripts/test-infra.sh`.

- `WsTicketService` (Redis thật): issue khác nhau mỗi lần; consume trả đúng
  `userId`; consume lần 2 → `null`; ticket lạ → `null`; ticket hết hạn (TTL
  ngắn qua Redis) → `null`; **2 consume đồng thời** → đúng 1 thắng.
- `PresenceService` (Redis thật): connect đầu → `true`, connect thứ 2 →
  `false`; disconnect 1 trong 2 → `false` và vẫn online; disconnect hết →
  `true` và offline; **socket mồ côi** (SADD tay không có heartbeat) bị prune
  và không làm user "online ảo"; heartbeat hết hạn thật (TTL 1s) → offline;
  `touch` gia hạn giữ online; `onlineAmong` đúng.
- `EventsGateway` đa instance (2 Nest app cùng process, 2 cổng, chung
  Postgres + Redis): client ở app A và app B cùng room nhận `message:new`
  khi gửi từ A; ticket cấp ở A consume được ở B; presence: 2 tab → đóng 1
  tab không phát `online:false`; đóng tab cuối thì member khác (kết nối ở
  instance khác) nhận `presence:update online:false`; connect đầu nhận
  `online:true` chéo instance; `GET /rooms/:id/presence` đúng, non-member 403.
- Regression: toàn bộ test Phase 1/2 vẫn pass (đã sửa phần cấp ticket sang
  async).

**Không cần TDD:** thay đổi UI `web/`, `main.ts` bootstrap,
`scripts/test-infra.sh`, `docker-compose.yml`.

## Tiêu chí hoàn thành Phase 3

- [x] `WsTicketService` dùng Redis; ticket cấp ở instance này consume được ở
      instance khác, dùng 1 lần, hết hạn sau 30s.
- [x] Hai instance chạy song song, chung Redis: `message:new` từ instance A
      tới client kết nối vào B (và ngược lại).
- [x] Presence dùng Redis Set; mở N tab chỉ phát `online:true` 1 lần, đóng
      từng tab chỉ phát `online:false` khi tab cuối đóng.
- [x] Socket mồ côi (không heartbeat) không giữ user online quá TTL.
- [x] `GET /rooms/:roomId/presence` đúng, chặn non-member.
- [x] Graceful shutdown dọn socket của instance khỏi Set.
- [x] `npx tsc --noEmit` sạch (`src/` và `web/`), mọi test (cũ và mới) pass
      với Postgres + Redis thật.
- [x] `docker-compose.yml` có service `redis`; `scripts/test-infra.sh` dựng
      được hạ tầng test khi không có Docker.
