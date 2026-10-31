# Phase 2 — Realtime qua Socket.IO (1 instance, BFF token exchange)

Sub-project của roadmap tổng (`2026-10-10-realtime-chat-v2-roadmap-design.md`),
tiếp theo Phase 1 (`2026-10-11-phase1-auth-rooms-messages-design.md`).

## Mục tiêu

Thêm realtime vào monolith NestJS hiện có (**cùng 1 app**, 1 instance,
chưa cần Redis adapter — để Phase 3), dùng Socket.IO. Gửi message chuyển
từ REST sang WS; REST message chỉ còn dùng để đọc lịch sử (cursor
pagination, giữ nguyên từ Phase 1).

WS phải xác thực qua **ticket ngắn hạn, dùng 1 lần**, được cấp thông qua
1 Next.js app đóng vai BFF (Backend-For-Frontend) — access/refresh token
thật (JWT 15 phút + refresh token Phase 1) không bao giờ lộ ra browser,
chỉ nằm trong cookie `httpOnly` do Next.js server set/đọc.

Đây là lúc áp lại đúng pattern đã học ở Module 2/3 của bản gốc
(`realtime-chat-system`): xác thực WS qua token ngắn hạn thay vì gắn
thẳng JWT dài hạn vào query string/handshake.

## Phạm vi

**Có trong Phase 2:**
- NestJS: `RealtimeModule` (Socket.IO gateway), `WsTicketService`
  (in-memory, one-time ticket), endpoint `POST /auth/ws-ticket`.
- Gửi message qua WS (`message:send` → broadcast `message:new`), tái
  dùng 100% `RoomsService.sendMessage()` đã có từ Phase 1 (kể cả check
  member → 403).
- Subscribe/unsubscribe realtime cho 1 room đã là member
  (`room:subscribe` / `room:unsubscribe`) — không tạo membership mới.
- Next.js app tối thiểu (`web/`) đóng vai BFF: trang `/register`,
  `/login`, `/chat`; Route Handlers proxy sang NestJS + set cookie
  `httpOnly`; trang `/chat` cho tạo room, join room, chọn room, gửi/nhận
  message realtime.

**Không có trong Phase 2** (để phase sau):
- Presence (ai đang online) — Phase 3.
- Redis adapter (multi-instance WS) — Phase 3.
- Typing indicator, edit/delete message — Phase 10 (tuỳ chọn).
- Re-verify token định kỳ giữa phiên WS đang mở (đã chốt: giữ kết nối
  sống sau handshake, không cần revoke WS session giữa phiên ở Phase
  này).

## Kiến trúc & cấu trúc repo

```
realtime-chat-v2/
├── src/                        # NestJS (giữ nguyên từ Phase 1)
│   ├── auth/
│   │   └── auth.controller.ts  # + endpoint mới: POST /auth/ws-ticket
│   ├── rooms/
│   │   └── rooms.service.ts    # assertMember() đổi private → public
│   └── realtime/               # MODULE MỚI
│       ├── realtime.module.ts
│       ├── events.gateway.ts
│       ├── ws-ticket.service.ts
│       └── ws-ticket.service.spec.ts
├── web/                         # APP MỚI — Next.js (BFF)
│   └── app/
│       ├── api/
│       │   ├── register/route.ts
│       │   ├── login/route.ts
│       │   └── ws-ticket/route.ts
│       ├── register/page.tsx
│       ├── login/page.tsx
│       └── chat/
│           ├── page.tsx         # Server Component: load room đã join
│           └── chat-client.tsx  # Client Component: socket, UI gửi/nhận
└── prisma/                      # giữ nguyên
```

`web/` là 1 project Next.js độc lập (package.json riêng), không dùng
chung `node_modules`/workspace với backend — đơn giản hoá, đúng tinh
thần "flat, không tách sẵn khi chưa cần" của roadmap.

## Luồng xác thực WS (BFF ticket exchange)

```
Browser          Next.js (BFF)              NestJS
  |--POST /api/login-->|                           |
  |                    |--POST /auth/login-------->|
  |                    |<--{accessToken,refreshToken}|
  |<--Set-Cookie (httpOnly: access, refresh)--|      |
  |                    |                           |
  |--GET /api/ws-ticket->|                          |
  |                    |--POST /auth/ws-ticket----->|  (Bearer: access token từ cookie)
  |                    |<--{ticket, expiresIn:30}---|
  |<--{ticket}---------|                           |
  |                                                 |
  |====== io.connect({ auth: { ticket } }) ========>|
  |                                                 |  Gateway.handleConnection:
  |                                                 |    wsTicketService.consume(ticket)
  |                                                 |    → hợp lệ: socket.data.userId = ...
  |                                                 |    → không hợp lệ/hết hạn/đã dùng:
  |                                                 |      client.disconnect(true)
  |<====== connected, giữ kết nối sống lâu dài =====|  (không re-verify token sau handshake)
```

Nếu access token trong cookie đã hết hạn (>15 phút) lúc Route Handler
`/api/ws-ticket` được gọi, handler tự gọi `POST /auth/refresh` bằng
`refreshToken` trong cookie trước khi xin ticket — tái dùng đúng cơ chế
rotation của Phase 1, không thêm logic auth mới ở BFF.

### `WsTicketService` (in-memory, 1 instance)

```typescript
@Injectable()
export class WsTicketService {
  private tickets = new Map<string, { userId: string; expiresAt: number }>();

  issue(userId: string): string {
    const ticket = randomBytes(24).toString('hex');
    this.tickets.set(ticket, { userId, expiresAt: Date.now() + 30_000 });
    return ticket;
  }

  consume(ticket: string): string | null {
    const entry = this.tickets.get(ticket);
    this.tickets.delete(ticket); // one-time: xoá ngay dù hợp lệ hay không
    if (!entry || entry.expiresAt < Date.now()) return null;
    return entry.userId;
  }
}
```

Không có TTL cleanup job riêng ở Phase 2 (ticket hết hạn tự nhiên bị bỏ
qua khi `consume()`, và số lượng ticket treo trong RAM giữa 2 lần dùng là
không đáng kể ở quy mô 1 instance/dev). Sẽ thay bằng Redis (TTL tự động)
khi multi-instance ở Phase 3.

### Endpoint mới: `POST /auth/ws-ticket`

Thêm vào `AuthController` hiện có, dùng `JwtAuthGuard` (giống `/auth/me`):

```typescript
@Post('ws-ticket')
@UseGuards(JwtAuthGuard)
wsTicket(@Req() req: { user: { id: string } }) {
  return { ticket: this.wsTicketService.issue(req.user.id), expiresIn: 30 };
}
```

## Socket.IO events contract

| Event | Hướng | Payload | Hành vi server |
|---|---|---|---|
| `room:subscribe` | client→server (có ack) | `{ roomId: string }` | Check `socket.data.userId` là member của `roomId` (gọi `RoomsService.assertMember`, đổi từ `private` sang `public`) → không phải member: ack `{success: false, error: 'Not a member of this room'}`; hợp lệ: `socket.join(roomId)`, ack `{success: true}` |
| `room:unsubscribe` | client→server (có ack) | `{ roomId: string }` | `socket.leave(roomId)`, ack `{success: true}` — không cần check member |
| `message:send` | client→server (có ack) | `{ roomId: string, content: string }` | Gọi `RoomsService.sendMessage(socket.data.userId, roomId, {content})` (tái dùng nguyên vẹn Phase 1); lỗi (403 không phải member, content rỗng) → ack `{success: false, error: <message>}`; thành công → ack `{success: true, message}` **và** `io.to(roomId).emit('message:new', message)` |
| `message:new` | server→client (broadcast) | `Message` (object giống REST, `{id, roomId, senderId, content, createdAt}`) | Gửi tới mọi socket đã `room:subscribe` room đó, **bao gồm cả người gửi** (không cần optimistic update riêng phía client) |

**Xử lý lỗi:** mọi handler (`room:subscribe`, `room:unsubscribe`,
`message:send`) bọc trong try/catch nội bộ — lỗi nghiệp vụ (403/400) trả
qua **ack callback**, không bao giờ làm crash gateway hay disconnect
socket. Chỉ ticket không hợp lệ ở bước `handleConnection` mới
disconnect.

## Next.js BFF

### Route Handlers (proxy mỏng, không có business logic, không TDD)

- `POST /api/register` → `POST {NEST_URL}/auth/register` → set cookie
  `httpOnly` cho `accessToken`, `refreshToken` (tên cookie:
  `access_token`, `refresh_token`) → trả `{success: true}`.
- `POST /api/login` → tương tự, gọi `/auth/login`.
- `GET /api/ws-ticket` → đọc cookie `access_token`; nếu hết hạn (NestJS
  trả 401), tự gọi `POST /auth/refresh` bằng `refresh_token` cookie để
  lấy cặp token mới, set lại cookie, rồi mới gọi `POST /auth/ws-ticket`
  → trả `{ticket}` cho browser.

### Pages (App Router, UI tối thiểu, không TDD)

- `/register`, `/login` — form thuần, submit gọi Route Handler tương
  ứng, redirect `/chat` khi thành công.
- `/chat/page.tsx` (Server Component) — đọc cookie, gọi `GET
  {NEST_URL}/rooms` để lấy room đã join, render list, pass xuống Client
  Component.
- `/chat/chat-client.tsx` (Client Component) — state: room đang chọn,
  list message. Lúc mount: `fetch('/api/ws-ticket')` → `io(NEST_URL,
  {auth: {ticket}})`. Khi chọn room: emit `room:subscribe`, gọi `GET
  {NEST_URL}/rooms/:id/messages` lấy lịch sử. Khi gửi: emit
  `message:send`. Lắng nghe `message:new` để append vào list. Có input
  tạo room gọi `POST {NEST_URL}/rooms`, nút join room theo id gọi `POST
  {NEST_URL}/rooms/:id/join`.

## Testing strategy (TDD lai — giống tinh thần Phase 1)

**TDD, test thật (dùng Postgres test + Nest app test instance thật,
không mock):**
- `WsTicketService`: issue trả ticket khác nhau mỗi lần; consume đúng
  trả `userId`; consume lần 2 cùng ticket trả `null` (one-time); ticket
  hết hạn (giả lập bằng cách set `expiresAt` âm) trả `null`.
- `EventsGateway` (test bằng `socket.io-client` thật kết nối vào
  `Test.createNestApplication()` đang listen cổng thật):
  - Connect với ticket hợp lệ → thành công; ticket sai/đã dùng →
    `connect_error`.
  - `room:subscribe` khi không phải member → ack `success:false`; khi
    là member → ack `success:true`.
  - `message:send` → ack `success:true` **và** 1 socket khác đã
    `room:subscribe` cùng room nhận được `message:new` với đúng content.
  - `message:send` khi không phải member → ack `success:false`, không
    có socket nào nhận `message:new`.

**Không cần TDD:** Next.js pages/Route Handlers, DTO validation cơ bản
cho `message:send` payload, scaffold `RealtimeModule`.

## Tiêu chí hoàn thành Phase 2

- [ ] `POST /auth/ws-ticket` cấp ticket hợp lệ, dùng 1 lần (dùng lại bị
      từ chối).
- [ ] Connect Socket.IO bằng ticket hợp lệ thành công; ticket sai/hết
      hạn/đã dùng bị disconnect.
- [ ] `room:subscribe` chặn đúng user không phải member (403-style qua
      ack), không ảnh hưởng tới các room khác.
- [ ] Gửi `message:send` → người gửi và các client khác đã subscribe
      cùng room đều nhận `message:new` đúng nội dung; REST `GET
      .../messages` vẫn đọc được message đó (đã lưu DB qua
      `RoomsService.sendMessage` dùng chung).
- [ ] Next.js app (`web/`) chạy được end-to-end: register → login →
      tạo room → mở 2 tab (2 user khác nhau, cùng join 1 room) → gửi
      message ở tab này, thấy xuất hiện realtime ở tab kia.
- [ ] `npx tsc --noEmit` sạch (cả `src/` và `web/`), test TDD đã viết
      đều pass.
