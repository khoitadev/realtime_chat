# Phase 1 — Auth + Rooms + Messages (REST monolith)

Sub-project của roadmap tổng (`2026-10-10-realtime-chat-v2-roadmap-design.md`).

## Mục tiêu

Dựng 1 NestJS monolith phẳng (không workspace/`apps/*`) cung cấp: đăng ký/đăng
nhập với refresh-token rotation + reuse detection đúng chuẩn, tạo/tham gia
room, gửi/đọc message qua REST (chưa có realtime — Phase 2 mới thêm
WebSocket). Đây là nền để các phase sau xây tiếp, nên phải **đúng ngay từ
đầu** ở 2 điểm mà bản gốc (`realtime-chat-system`) đã sai/thiếu:

1. Refresh-token reuse detection (bản gốc: phát hiện reuse nhưng không
   revoke các token khác — fix #1).
2. Cursor-based pagination `(createdAt, id)` cho message, không dùng
   `OFFSET`/`skip` (bản gốc đã làm đúng ở phần này — giữ nguyên, không lặp lại
   sai).

## Phạm vi

**Có trong Phase 1:**
- Đăng ký, đăng nhập, refresh, logout, lấy profile.
- Tạo room, tham gia room, danh sách room đã tham gia.
- Gửi message (REST), lấy lịch sử message (cursor pagination).
- Authorization ở mức "chỉ member mới đọc/gửi message trong room".

**Không có trong Phase 1** (để phase sau):
- WebSocket/realtime (Phase 2).
- Presence, Redis adapter (Phase 3).
- Tách service nào (Phase 4+).
- Role trong room (admin/member) — chưa cần ở Phase 1, mọi member đều ngang
  quyền.
- Rate limiting, search, notification, analytics.

## Data model (Prisma, 1 Postgres DB)

```prisma
model User {
  id           String   @id @default(uuid())
  email        String   @unique
  passwordHash String
  displayName  String
  createdAt    DateTime @default(now())

  refreshTokens RefreshToken[]
  rooms         RoomMember[]
  messages      Message[]
}

model RefreshToken {
  id         String    @id @default(uuid())
  userId     String
  tokenHash  String    @unique
  revokedAt  DateTime?
  expiresAt  DateTime
  createdAt  DateTime  @default(now())

  user User @relation(fields: [userId], references: [id])

  @@index([userId])
}

model Room {
  id        String   @id @default(uuid())
  name      String
  createdBy String
  createdAt DateTime @default(now())

  members  RoomMember[]
  messages Message[]
}

model RoomMember {
  roomId   String
  userId   String
  joinedAt DateTime @default(now())

  room Room @relation(fields: [roomId], references: [id])
  user User @relation(fields: [userId], references: [id])

  @@id([roomId, userId])
}

model Message {
  id        String   @id @default(uuid())
  roomId    String
  senderId  String
  content   String
  createdAt DateTime @default(now())

  room   Room @relation(fields: [roomId], references: [id])
  sender User @relation(fields: [senderId], references: [id])

  @@index([roomId, createdAt, id])
}
```

## API contract (REST, Bearer JWT cho các route cần auth)

```
POST /auth/register   {email, password, displayName} → 201 {accessToken, refreshToken}
POST /auth/login      {email, password}               → 200 {accessToken, refreshToken}
POST /auth/refresh    {refreshToken}                   → 200 {accessToken, refreshToken} | 401
POST /auth/logout     {refreshToken}                   → 200 {success: true}
GET  /auth/me                                          → 200 {id, email, displayName}

POST /rooms                      {name}         → 201 room
POST /rooms/:roomId/join                         → 201 membership
GET  /rooms                                      → 200 room[]
GET  /rooms/:roomId/messages     ?cursor&limit   → 200 {items, nextCursor}
POST /rooms/:roomId/messages     {content}       → 201 message
```

- Mọi route trừ `/auth/register`, `/auth/login`, `/auth/refresh` yêu cầu
  `Authorization: Bearer <accessToken>`.
- `GET/POST .../messages` trả `403 Forbidden` nếu user không phải member của
  room đó.

## Auth flow — refresh rotation + reuse detection

`issueTokens(userId)`:
- `accessToken`: JWT ký `JWT_SECRET`, payload `{sub: userId}`, TTL 15 phút.
- `refreshToken`: 48 byte random (hex), lưu `sha256(token)` vào
  `RefreshToken.tokenHash`, `expiresAt` = now + 7 ngày, `revokedAt = null`.

`refresh(refreshToken)`:
1. Hash token, tìm `RefreshToken` theo `tokenHash`.
2. Không tìm thấy → `401 Invalid refresh token`.
3. Tìm thấy, `revokedAt != null` → **reuse detected**: `updateMany` revoke
   **toàn bộ** `RefreshToken` của `userId` đó (`revokedAt = null` →
   `revokedAt = now()`) → `401 Session revoked — please log in again`
   (message/log phải **khác** case #2, để phân biệt được 2 tình huống).
4. Tìm thấy, `expiresAt < now()` → `401 Refresh token expired`.
5. Hợp lệ → revoke token này (`revokedAt = now()`), gọi `issueTokens` phát
   cặp mới, trả về.

`logout(refreshToken)`: revoke đúng 1 token được gửi lên (không revoke toàn
bộ — khác với case reuse-detected).

## Cursor pagination cho message

`getMessages(roomId, userId, cursor?, limit=20)`:
- Assert `userId` là member của `roomId`, nếu không → `403`.
- Query `Message` where `roomId = roomId` AND (không cursor, hoặc
  `(createdAt, id) < (cursor.createdAt, cursor.id)` theo thứ tự
  `ORDER BY createdAt DESC, id DESC`), `take = limit + 1`.
- `cursor` là string ghép `${createdAt.toISOString()}|${id}`.
- Nếu số dòng trả về > `limit` → còn trang sau, `nextCursor` = cursor của
  dòng thứ `limit`; ngược lại `nextCursor = null`.
- Đảo ngược `items` trước khi trả (để client nhận theo thứ tự cũ → mới).

## Testing (TDD lai — đã chốt)

**TDD (test trước, test thật trên Postgres test, không mock Prisma):**
- `AuthService.refresh()`: case hợp lệ (rotation đúng, token cũ bị revoke),
  case token sai, case **reuse** (dùng lại token đã revoke → tất cả token
  khác của user cũng bị revoke theo), case hết hạn.
- `RoomsService.getMessages()`: phân trang đúng thứ tự, không trùng/không
  lệch dòng khi có insert xảy ra giữa 2 lần gọi, xử lý đúng khi 2 message có
  `createdAt` trùng nhau (tie-break bằng `id`).

**Viết sau/không cần test:** khai báo Prisma schema, scaffold
controller/module, DTO validation cơ bản.

Dùng 1 Postgres riêng cho test (docker, port khác dev) — không test trên DB
dev để tránh rác dữ liệu.

## Error handling

Dùng exception có sẵn NestJS: `UnauthorizedException` (401),
`ConflictException` (409 — email đã tồn tại), `ForbiddenException` (403 —
không phải member), `NotFoundException` (404 — room không tồn tại). Giữ
response shape mặc định của Nest (`{statusCode, message, error}`).

## Tiêu chí hoàn thành Phase 1

- [ ] Register/login/refresh/logout hoạt động đúng qua Postman/curl.
- [ ] Reuse-detection test pass: dùng lại refresh token đã revoke → toàn bộ
      session user đó bị revoke, xác minh bằng cách gọi `refresh` với token
      hợp lệ khác của cùng user ngay sau đó → phải trả `401`.
- [ ] Tạo room, join room, gửi/đọc message qua REST hoạt động đúng, có
      kiểm tra quyền member.
- [ ] Cursor pagination test pass: không trùng/lệch dòng khi insert xảy ra
      giữa 2 lần gọi `getMessages`.
- [ ] `npx tsc --noEmit` sạch, test TDD đã viết đều pass.
