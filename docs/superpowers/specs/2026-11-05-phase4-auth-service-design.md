# Phase 4 — Tách auth-service (DB riêng `auth_db`, JWT RS256 + JWKS)

Sub-project của roadmap tổng (`2026-10-10-realtime-chat-v2-roadmap-design.md`),
tiếp theo Phase 3 (`2026-11-03-phase3-redis-scale-readiness-design.md`).

## Mục tiêu

Tách phần xác thực (register/login/refresh/logout, bảng `User` và
`RefreshToken`) ra thành **auth-service** chạy process riêng với database riêng
(`auth_db`). Phần còn lại (chat-service: rooms, messages, realtime, presence)
**verify JWT stateless** — không gọi lại auth-service mỗi request, chỉ cần
public key lấy từ JWKS.

Lý do tách (củng cố bài học Module 1): đây là **data ownership**, không chỉ
fault isolation — chỉ auth-service sở hữu dữ liệu người dùng và chỉ nó có khả
năng tạo token. Nếu dùng chung secret HS256, service nào bị lộ secret cũng ký
được token cho bất kỳ user nào; RS256 + JWKS loại bỏ rủi ro đó (service khác
chỉ giữ public key).

## Quyết định đã chốt (brainstorm)

| Chủ đề | Quyết định | Phương án đã loại |
|---|---|---|
| Ký / verify JWT | RS256; auth-service giữ private key, công khai public key qua `GET /.well-known/jwks.json`; chat-service fetch + cache JWKS | RS256 + public key qua env (rotate phải redeploy); HS256 dùng chung secret |
| Cấu trúc repo | Thư mục `auth-service/` độc lập (package.json, Prisma, test riêng), `src/` giữ nguyên là chat-service | Nest monorepo `apps/*` (di chuyển toàn bộ `src/`, dễ import chéo) |
| Dữ liệu hiện có | Bỏ FK tới `User`, migrate User + RefreshToken sang `auth_db` bằng script một lần (giữ nguyên id) trước khi drop ở chat DB | Reset dữ liệu; giữ bản sao `User` trong chat DB (cần sync, thiếu broker) |
| Gọi HTTP giữa service | Chỉ JWKS (chat → auth). BFF gọi trực tiếp cả hai service | Endpoint nội bộ lookup user (tính năng mới: cache, timeout, bảo mật nội bộ) |

## Phạm vi

**Có trong Phase 4:**
- `auth-service/`: NestJS app, `AuthModule` (chuyển từ `src/auth/`), Prisma
  schema `User`/`RefreshToken` → `auth_db`, `JwksController`, ký token RS256.
- chat-service: `JwksService`, `JwtStrategy` mới (RS256 qua JWKS), `POST
  /ws-ticket` (chuyển từ `POST /auth/ws-ticket`), bỏ `User`/`RefreshToken`
  và FK khỏi Prisma schema + migration.
- Script migrate dữ liệu `auth-service/src/migration/migrate-users.ts` + CLI `auth-service/scripts/migrate-users-from-chat.ts` (có test).
- Script sinh cặp key RS256 cho dev/test.
- `web/`: thêm `AUTH_URL`; route đăng nhập/đăng ký/refresh gọi auth-service,
  còn lại gọi chat-service.
- Hạ tầng test: `auth_dev` / `auth_test` (native: cùng cluster Postgres;
  Docker: thêm service Postgres cho auth).

**Không có trong Phase 4:**
- Gateway hợp nhất hai backend (Phase 5).
- Endpoint nội bộ lookup user / hiển thị `displayName` trong chat.
- Tự động rotate key theo lịch (chỉ hỗ trợ cơ chế: JWKS nhiều key + `kid`).
- Revoke access token đang sống (token sống tối đa 15 phút; đánh đổi cố hữu của
  verify stateless).
- Docker image / deploy cho từng service (Phase 9).

## Kiến trúc & cấu trúc repo

```
realtime-chat-v2/
├── src/                          # chat-service (giữ nguyên chỗ)
│   ├── auth/                     # THAY: chỉ còn phần verify
│   │   ├── jwks.service.ts       # fetch + cache JWKS, refetch khi kid lạ
│   │   ├── jwt.strategy.ts       # passport-jwt, secretOrKeyProvider qua JwksService
│   │   ├── jwt-auth.guard.ts
│   │   └── auth.module.ts
│   ├── realtime/
│   │   └── ws-ticket.controller.ts   # POST /ws-ticket (từ AuthController)
│   └── ... (rooms, redis, prisma giữ nguyên)
├── auth-service/                 # MỚI — project NestJS độc lập
│   ├── package.json  tsconfig*.json  nest-cli.json  jest.config.js
│   ├── prisma/schema.prisma      # User, RefreshToken → auth_db
│   ├── src/
│   │   ├── auth/                 # service/controller/DTO chuyển từ chat
│   │   ├── keys/                 # KeyService (nạp PEM, kid, JWK) + JwksController
│   │   └── prisma/
│   └── test/
├── scripts/
│   ├── gen-jwt-keys.sh           # openssl → cặp key RS256
│   └── (migrate-users-from-chat.ts: xem auth-service/scripts/)
├── test/                         # helper test chat-service (key + JWKS server)
└── web/                          # BFF: thêm AUTH_URL
```

`tsconfig.json` và `tsconfig.build.json` gốc loại thêm `auth-service`
(giống cách đã loại `web`).

## JWT: ký và verify

### Token

- Thuật toán RS256, TTL 15 phút (không đổi).
- Header: `{ alg: 'RS256', kid }`. `kid` = SHA-256 thumbprint (base64url) của
  public key — tính từ chính key, không cấu hình tay.
- Payload: `{ sub: userId, iss: 'auth-service', aud: 'realtime-chat', iat, exp }`.
- Refresh token **không đổi**: chuỗi ngẫu nhiên, lưu hash SHA-256 trong
  `auth_db`, rotation + reuse detection (thu hồi toàn bộ phiên khi dùng lại
  token đã thu hồi) giữ nguyên hành vi Phase 1.

### auth-service: `KeyService` và JWKS

- Nạp `JWT_PRIVATE_KEY` (PEM). Suy ra public key bằng `crypto.createPublicKey`
  và JWK bằng `export({ format: 'jwk' })` rồi thêm `kid`, `use: 'sig'`,
  `alg: 'RS256'`.
- Tuỳ chọn `JWT_PREVIOUS_PUBLIC_KEYS` (PEM nối nhau) để vẫn công bố key cũ trong
  JWKS khi rotate (token ký bằng key cũ còn sống tối đa 15 phút).
- `GET /.well-known/jwks.json` → `{ keys: [...] }`, `Cache-Control: public,
  max-age=300`.
- Thiếu hoặc sai `JWT_PRIVATE_KEY` → app fail ngay lúc khởi động (fail fast).

### chat-service: `JwksService`

- Cấu hình: `AUTH_JWKS_URL` (mặc định `http://localhost:3010/.well-known/jwks.json`),
  `JWT_ISSUER`, `JWT_AUDIENCE`.
- `getPublicKeyPem(kid): Promise<string>` (SPKI PEM): tra cache; cache miss (kid lạ) → fetch lại
  JWKS, nhưng **tối đa 1 lần / 10 giây** (chặn việc kẻ tấn công gửi token với
  `kid` ngẫu nhiên để dội auth-service). Cache theo TTL 10 phút.
- Fetch thất bại (auth-service chưa lên): giữ cache cũ nếu có; nếu chưa có key
  nào thì token bị từ chối (401), chat-service **vẫn khởi động được**.
- `JwtStrategy`: passport-jwt với `secretOrKeyProvider` lấy khoá từ
  `JwksService`; `algorithms: ['RS256']` (chặn `alg: none` và HS256 confusion),
  kiểm tra `issuer`, `audience`, `exp`.
- `JwtAuthGuard` và các controller/gateway dùng lại không đổi (`req.user.id`).

### `POST /ws-ticket` (chat-service)

Chuyển từ `POST /auth/ws-ticket`: `JwtAuthGuard` → `WsTicketService.issue`
(Redis, Phase 3) → `{ ticket, expiresIn: 30 }`. Hành vi giữ nguyên; chỉ đổi
chỗ ở và URL.

## Dữ liệu

### Prisma của chat-service sau Phase 4

- Xoá model `User` và `RefreshToken`.
- `Room.createdBy`, `RoomMember.userId`, `Message.senderId` giữ lại là `String`
  (UUID) **không còn relation/FK**. Bỏ các relation field trong Prisma.
- Migration mới `drop_user_tables_and_fks`.
- Hệ quả: DB không còn ép toàn vẹn "userId tồn tại". Tính toàn vẹn chuyển lên
  tầng ứng dụng — chỉ token do auth-service ký mới có `sub` hợp lệ.

### Thứ tự migrate (copy → chuyển traffic → drop)

1. Tạo `auth_db`, chạy migration schema auth.
2. `auth-service/src/migration/migrate-users.ts` (CLI `auth-service/scripts/migrate-users-from-chat.ts`): đọc `User` + `RefreshToken` từ chat DB
   (`DATABASE_URL`), ghi vào `auth_db` (`AUTH_DATABASE_URL`) giữ nguyên `id`;
   idempotent (`upsert` / `skipDuplicates`), in số bản ghi đã copy và đối soát
   số lượng, thoát mã ≠ 0 nếu lệch.
3. Chạy migration drop ở chat DB **sau** khi đối soát khớp.

Migration drop là destructive: spec yêu cầu chạy script đối soát và có bản sao
lưu trước khi áp dụng ở môi trường có dữ liệu thật.

## Thay đổi ở `web/` (BFF)

- Env mới `AUTH_URL` (mặc định `http://localhost:3010`), `lib/auth.ts`.
- `POST /api/register`, `/api/login` → `{AUTH_URL}/auth/register|login`.
- `nest-fetch.ts`: refresh token gọi `{AUTH_URL}/auth/refresh`; request
  nghiệp vụ vẫn gọi `{NEST_URL}`.
- `GET /api/ws-ticket` gọi `{NEST_URL}/ws-ticket`.
- Còn lại (rooms, messages, presence) không đổi.

## Testing strategy (TDD với I/O thật — giữ quy ước Phase 1/2/3)

Không mock Prisma / Redis / socket / HTTP verify. Postgres thật cho cả
`chat_test` lẫn `auth_test`, Redis thật.

**auth-service (jest riêng, `auth_test`):**
- Toàn bộ test `AuthService` / `AuthController` Phase 1 chuyển sang, thêm kiểm
  tra access token ký RS256 và verify được bằng JWKS của chính service.
- `KeyService`: sinh `kid` ổn định từ cùng key; JWK có `kty/n/e/kid/use/alg`
  và **không chứa** thành phần private (`d`, `p`, `q`…); key cũ xuất hiện trong
  JWKS khi cấu hình; thiếu key → khởi động lỗi.
- `GET /.well-known/jwks.json`: 200, header cache, token vừa ký verify được.

**chat-service (jest gốc, `chat_test`):**
- Helper `test/jwt.ts`: sinh cặp key RS256 trong test, hàm `signToken(userId,
  opts)`, và `startJwksServer(keys)` dựng **HTTP server thật** trên cổng ngẫu
  nhiên phục vụ JWKS (đây là hạ tầng test ở tầng mạng, không phải mock logic
  verify). `createUser` trong các spec cũ đổi thành sinh `userId` UUID — không
  còn tạo hàng `User`.
- `JwksService`: lấy key theo `kid`; cache hit không gọi lại server; `kid` lạ
  → fetch lại nhưng bị giới hạn 1 lần/10s (đếm request ở JWKS server); rotate
  key (server đổi bộ key) → token ký key mới được chấp nhận sau refetch; server
  chết → dùng cache cũ; server chết và chưa có cache → 401, app vẫn chạy.
- `JwtAuthGuard` / `JwtStrategy` qua endpoint thật: 401 khi thiếu token, chữ ký
  sai, hết hạn, sai `iss`, sai `aud`, `alg: none`, token HS256 giả mạo; 200 khi
  hợp lệ.
- `POST /ws-ticket`: 401 không token; 201 với token hợp lệ, ticket 48 ký tự hex,
  `expiresIn: 30`, ticket dùng được ở WS (Phase 2/3).
- Toàn bộ spec cũ (rooms, gateway, multi-instance, presence) vẫn pass sau khi
  chuyển sang token RS256 + userId UUID.

**Migration script (cần cả hai DB):**
- Dữ liệu mẫu trong chat DB cũ → script → `auth_db` có đủ bản ghi, `id` giữ
  nguyên, chạy lại lần 2 vẫn idempotent, lệch số lượng → lỗi.

**Smoke cuối (thủ công, không TDD):** chạy hai process thật, register/login qua
auth-service, dùng access token đó tạo room và gửi message ở chat-service,
mở WS bằng ticket.

**Không cần TDD:** `web/`, script sinh key, `docker-compose.yml`, bootstrap
`main.ts` của hai service.

## Rủi ro và đánh đổi

- **Phá vỡ API công khai:** `/auth/*` đổi cổng, `/auth/ws-ticket` đổi thành
  `/ws-ticket`. `web/` được sửa đồng bộ trong cùng phase; client khác (nếu có)
  phải cập nhật.
- **Không thu hồi được access token sớm:** logout/reuse-detection chỉ thu hồi
  refresh token; access token còn sống tối đa 15 phút. Chấp nhận như đánh đổi
  của verify stateless.
- **JWKS là điểm phụ thuộc lúc chạy:** đã giảm bằng cache, giữ key cũ khi lỗi,
  giới hạn refetch. Chưa có metric/alert (Phase 9).
- **Mất ràng buộc FK:** user bị xoá ở auth không kéo theo dữ liệu chat. Chưa có
  luồng xoá user; sẽ cần sự kiện (broker, Phase 6) khi có.
- **Migration drop là không thể đảo ngược** nếu thiếu backup.

## Tiêu chí hoàn thành Phase 4

- [x] `auth-service/` chạy độc lập với `auth_db`; register/login/refresh/logout
      /me hoạt động như Phase 1 (rotation + reuse detection còn nguyên).
- [x] Access token ký RS256 có `kid`, `iss`, `aud`; `GET
      /.well-known/jwks.json` công bố public key (không lộ thành phần private).
- [x] chat-service verify token chỉ bằng JWKS (không gọi auth-service mỗi
      request); từ chối đúng các token sai chữ ký/hết hạn/sai iss/aud/alg.
- [x] `kid` lạ chỉ kích hoạt refetch có giới hạn tần suất; rotate key hoạt
      động; auth-service chết thì chat-service vẫn phục vụ token đã có key.
- [x] Chat DB không còn bảng `User`/`RefreshToken` và FK; dữ liệu cũ được
      migrate sang `auth_db` và đối soát khớp.
- [x] `POST /ws-ticket` ở chat-service thay `POST /auth/ws-ticket`; WS vẫn
      hoạt động với multi-instance + presence (Phase 3).
- [ ] `web/` chạy end-to-end với hai backend. (chỉ mới tsc sạch; chưa chạy trình duyệt)
- [x] `npx tsc --noEmit` sạch (chat-service, auth-service, `web/`); mọi test
      của hai service pass với Postgres + Redis thật; smoke hai process pass.
