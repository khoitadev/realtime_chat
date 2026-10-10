# Phase 1 — Execution Walkthrough (step-by-step, để tự làm lại)

Tài liệu này ghi lại **chính xác** những gì đã làm để implement Phase 1
(auth + rooms + messages), theo đúng thứ tự: lệnh đã chạy, file đã tạo/sửa
(nội dung đầy đủ), và các vấn đề thực tế gặp phải (khác với kế hoạch gốc).

Dùng cùng với:
- Spec: `docs/superpowers/specs/2026-10-11-phase1-auth-rooms-messages-design.md`
- Plan chi tiết (TDD từng bước, đầy đủ code): `docs/superpowers/plans/2026-10-11-phase1-auth-rooms-messages-plan.md`

Tài liệu này là **phần "đã làm thế nào"** — nếu muốn xem lại code đầy đủ
từng bước TDD (test trước, impl sau), đọc file plan ở trên. Ở đây tập
trung vào trình tự thực thi + các chỗ thực tế khác với kế hoạch.

---

## 0. Môi trường & công cụ

- OS: Windows 11, shell chính dùng Git Bash / PowerShell.
- Node.js + npm đã có sẵn.
- Docker Desktop (cần **đang chạy** trước khi làm Task 2 — xem mục Task 2
  bên dưới, đây là chỗ bị kẹt thực tế).
- Toàn bộ việc code được làm trong **git worktree riêng** (không code trực
  tiếp trên `master`) để không ảnh hưởng nhánh chính nếu có gì sai. Nếu tự
  làm lại và không cần cách ly, có thể bỏ qua bước worktree và code thẳng
  trên một branch thường (`git checkout -b phase1-auth-rooms-messages`).

```bash
# Cách ly bằng worktree (tuỳ chọn — có thể thay bằng git checkout -b)
git worktree add .worktrees/phase1-auth-rooms-messages -b phase1-auth-rooms-messages
cd .worktrees/phase1-auth-rooms-messages
```

---

## 1. Task 1 — Scaffold NestJS project

Tạo các file cấu hình gốc (không có code logic, nên không cần test):

- `package.json` — script `build`, `start:dev`, `prisma:generate`,
  `prisma:migrate:dev`, `prisma:migrate:test`, `test`, `typecheck`; deps:
  `@nestjs/*`, `@prisma/client`, `bcrypt`, `class-validator`,
  `passport-jwt`...; devDeps: `prisma`, `jest`, `ts-jest`, `dotenv-cli`...
  (xem nội dung đầy đủ trong plan, Task 1 Step 1).
- `tsconfig.json`, `tsconfig.build.json`, `nest-cli.json`, `.gitignore`
- `src/main.ts` — bootstrap Nest, `ValidationPipe({whitelist, transform})`
- `src/app.module.ts` — module rỗng ban đầu

Lệnh chạy:

```bash
npm install
npm run build
node dist/main.js   # chỉ để xác nhận app boot được, rồi Ctrl+C
```

Commit:

```bash
git add package.json package-lock.json tsconfig.json tsconfig.build.json nest-cli.json .gitignore src/main.ts src/app.module.ts
git commit -m "chore: scaffold NestJS project"
```

---

## 2. Task 2 — Docker Compose Postgres + Prisma schema + migrations

### ⚠️ Vấn đề thực tế #1: cổng 5432 bị chiếm

File `docker-compose.yml` ban đầu map `postgres-dev` ra cổng host
**5432**, nhưng máy đã có sẵn **một Postgres native (không phải Docker)**
đang lắng nghe port 5432 → chạy `npx prisma migrate dev` bị lỗi:

```
Error: P1000: Authentication failed against database server at `localhost`
```

**Cách phát hiện:**

```bash
netstat -ano | grep -E ":5432|:5433"
# rồi dùng PowerShell để xem process nào đang giữ port đó
```

```powershell
Get-Process -Id <PID> | Select-Object Id,ProcessName,Path
```

**Fix:** đổi port map của `postgres-dev` từ `5432:5432` → `5434:5432`,
và sửa `DATABASE_URL` trong `.env` tương ứng. `postgres-test` giữ nguyên
`5433:5432` (không bị đụng).

### File cuối cùng (đã fix port)

`docker-compose.yml`:

```yaml
version: "3.8"
services:
  postgres-dev:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: chat_dev
    ports:
      - "5434:5432"   # <-- không phải 5432, vì máy đã có Postgres native chiếm port đó
    volumes:
      - pgdata_dev:/var/lib/postgresql/data
  postgres-test:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: chat_test
    ports:
      - "5433:5432"
    volumes:
      - pgdata_test:/var/lib/postgresql/data
volumes:
  pgdata_dev:
  pgdata_test:
```

`.env` (không commit — đã có trong `.gitignore`):

```
DATABASE_URL="postgresql://postgres:postgres@localhost:5434/chat_dev"
JWT_SECRET="dev-secret-change-me"
PORT=3000
```

`.env.test` (không commit):

```
DATABASE_URL="postgresql://postgres:postgres@localhost:5433/chat_test"
JWT_SECRET="test-secret"
```

`prisma/schema.prisma` — 5 model đúng như spec (`User`, `RefreshToken`,
`Room`, `RoomMember`, `Message`) — xem nội dung đầy đủ trong plan Task 2.

### Lệnh chạy (theo đúng thứ tự)

```bash
docker --version && docker compose version   # xác nhận Docker CLI có sẵn

docker compose up -d                          # nếu Docker Desktop daemon
                                               # chưa chạy, lệnh này sẽ lỗi
                                               # "failed to connect to the
                                               # docker API" — phải mở
                                               # Docker Desktop trước rồi
                                               # chạy lại

docker compose ps                             # xác nhận cả 2 container "Up"

npx prisma migrate dev --name init            # tạo + áp migration cho DB dev
npm run prisma:migrate:test                   # áp cùng migration cho DB test
                                               # (script này dùng dotenv-cli
                                               # để load .env.test)
```

Commit:

```bash
git add docker-compose.yml prisma/schema.prisma prisma/migrations
git commit -m "feat: add Postgres dev/test containers and Prisma schema"
```

---

## 3. Task 3 — PrismaService/PrismaModule + Jest/test-DB wiring

Tạo:
- `src/prisma/prisma.service.ts` — `PrismaService extends PrismaClient`,
  connect ở `onModuleInit`, disconnect ở `onModuleDestroy`.
- `src/prisma/prisma.module.ts` — `@Global()` module export
  `PrismaService`.
- Sửa `src/app.module.ts` → import `PrismaModule`.
- `test/jest.env.ts` — load `.env.test` bằng `dotenv` **trước khi** Jest
  chạy bất kỳ test file nào (`setupFiles` trong Jest config).
- `jest.config.js` — preset `ts-jest`, `setupFiles: ['test/jest.env.ts']`.
- `test/db-cleanup.ts` — `cleanDatabase(prisma)` xoá dữ liệu theo đúng thứ
  tự để không vi phạm foreign key: `message → roomMember → refreshToken →
  room → user`.

### Kiểm tra wiring bằng 1 spec dùng-rồi-xoá

Tạo tạm `src/prisma/prisma.sanity.spec.ts` chỉ để confirm Jest nối đúng
vào DB test (port 5433), chạy:

```bash
npm test -- prisma.sanity
```

Thấy PASS → **xoá file tạm này** (không commit) rồi mới commit các file
chính thức:

```bash
git add src/prisma/prisma.service.ts src/prisma/prisma.module.ts src/app.module.ts jest.config.js test/jest.env.ts test/db-cleanup.ts
git commit -m "feat: wire PrismaService and Jest to the Postgres test database"
```

---

## 4. Task 4 — Auth DTOs + JwtModule config

Tạo `src/auth/dto/{register,login,refresh}.dto.ts` (class-validator:
`@IsEmail`, `@MinLength`...), và `src/auth/auth.module.ts` import
`JwtModule.registerAsync` đọc `JWT_SECRET` từ `ConfigService`, TTL access
token `15m`. Sửa `src/app.module.ts` thêm `ConfigModule.forRoot({isGlobal:
true})` + `AuthModule`.

**Lưu ý:** ở bước này `auth.module.ts` **import `AuthService` nhưng file
đó chưa tồn tại** (sẽ tạo ở Task 5) → cố ý, vì class DTO/module scaffold
không cần TDD theo spec. Không chạy `typecheck` ở bước này.

```bash
git add src/auth/dto src/auth/auth.module.ts src/app.module.ts
git commit -m "feat: add auth DTOs and JwtModule config"
```

---

## 5-11. Task 5 đến 11 — AuthService (TDD từng method)

Đây là phần lõi, làm theo chu trình TDD chuẩn cho **từng method**:
1. Viết test trong `src/auth/auth.service.spec.ts`
2. `npm test -- auth.service` → xác nhận **FAIL** (đúng như mong đợi,
   method chưa tồn tại / case chưa xử lý)
3. Viết implementation tối thiểu trong `src/auth/auth.service.ts`
4. `npm test -- auth.service` → xác nhận **PASS**
5. `git commit`

| Task | Method / case | Commit message |
|---|---|---|
| 5 | `register()` + hash password bcrypt | `feat: add AuthService.register with password hashing` |
| 6 | `login()` | `feat: add AuthService.login` |
| 7 | `refresh()` happy-path (rotate token) | `feat: add AuthService.refresh happy-path rotation` |
| 8 | `refresh()` token không tồn tại | `test: pin AuthService.refresh invalid-token error message` (chỉ thêm test, code đã đúng từ Task 7) |
| 9 | **`refresh()` reuse detection — fix #1 của spec** | `fix: revoke ALL user sessions on refresh-token reuse detection` |
| 10 | `refresh()` token hết hạn | `feat: add AuthService.refresh expired-token case` |
| 11 | `logout()` | `feat: add AuthService.logout` |

Code cuối cùng của `refresh()` (sau Task 10, đã có đủ 3 case: không tồn
tại → reuse → hết hạn → rotate hợp lệ):

```typescript
async refresh(refreshToken: string): Promise<{ accessToken: string; refreshToken: string }> {
  const tokenHash = createHash('sha256').update(refreshToken).digest('hex');
  const stored = await this.prisma.refreshToken.findUnique({ where: { tokenHash } });

  if (!stored) {
    throw new UnauthorizedException('Invalid refresh token');
  }

  if (stored.revokedAt) {
    // Reuse detection: 1 token đã revoke bị dùng lại → revoke TẤT CẢ token
    // còn sống của user này (khác bản gốc — bản gốc chỉ phát hiện reuse mà
    // không revoke các session khác).
    await this.prisma.refreshToken.updateMany({
      where: { userId: stored.userId, revokedAt: null },
      data: { revokedAt: new Date() },
    });
    throw new UnauthorizedException('Session revoked — please log in again');
  }

  if (stored.expiresAt < new Date()) {
    throw new UnauthorizedException('Refresh token expired');
  }

  await this.prisma.refreshToken.update({
    where: { id: stored.id },
    data: { revokedAt: new Date() },
  });

  return this.issueTokens(stored.userId);
}
```

Test quan trọng nhất (Task 9 — chứng minh fix #1):

```typescript
it("detects reuse of a revoked token and revokes ALL of that user's sessions", async () => {
  const { refreshToken: sessionA } = await authService.register({ ... });
  const { refreshToken: sessionB } = await authService.login({ ... }); // session thứ 2

  await authService.refresh(sessionA); // rotate — sessionA giờ đã revoke

  await expect(authService.refresh(sessionA)).rejects.toThrow(
    'Session revoked — please log in again',
  ); // reuse token đã revoke

  await expect(authService.refresh(sessionB)).rejects.toThrow(
    'Session revoked — please log in again',
  ); // sessionB CHƯA từng bị reuse nhưng vẫn phải chết theo
});
```

(Toàn bộ code+test đầy đủ từng bước nằm trong file plan — phần này chỉ
tóm tắt trình tự + đoạn quan trọng nhất.)

---

## 12. Task 12 — JwtStrategy + JwtAuthGuard + AuthController

Tạo `src/auth/jwt.strategy.ts` (Passport strategy đọc Bearer token, verify
bằng `JWT_SECRET`), `src/auth/jwt-auth.guard.ts` (`AuthGuard('jwt')`),
`src/auth/auth.controller.ts` (5 route: `register/login/refresh/logout/me`
— chỉ `me` có `@UseGuards(JwtAuthGuard)`). Sửa `auth.module.ts` thêm
`PassportModule`, `controllers: [AuthController]`, `providers: [...,
JwtStrategy]`.

```bash
npm run typecheck && npm test   # 10/10 test pass, lần đầu chạy full suite
git add src/auth/jwt.strategy.ts src/auth/jwt-auth.guard.ts src/auth/auth.controller.ts src/auth/auth.module.ts
git commit -m "feat: add JwtStrategy, JwtAuthGuard, and AuthController"
```

---

## 13-18. Task 13 đến 18 — RoomsService (TDD từng method)

Cùng chu trình TDD như trên, file `src/rooms/rooms.service.ts` +
`src/rooms/rooms.service.spec.ts`:

| Task | Method / case | Commit message |
|---|---|---|
| 13 | `createRoom()` (tạo room + tự thành member) | `feat: add RoomsService.createRoom` |
| 14 | `joinRoom()` (404 nếu room không tồn tại, idempotent) | `feat: add RoomsService.joinRoom` |
| 15 | `listRooms()` (chỉ trả room đã join) | `feat: add RoomsService.listRooms` |
| 16 | `sendMessage()` + `assertMember()` private (403 nếu không phải member) | `feat: add RoomsService.sendMessage with membership check` |
| 17 | **`getMessages()` cursor pagination — fix #2 của spec** | `feat: add RoomsService.getMessages with cursor pagination` |
| 18 | Test bổ sung: tie-break theo `id` khi `createdAt` trùng + insert giữa 2 lần gọi không làm lệch/trùng dòng | `test: verify cursor pagination tie-break and concurrent-insert safety` |

Code cuối cùng của `getMessages()`:

```typescript
async getMessages(
  userId: string,
  roomId: string,
  cursor?: string,
  limit = 20,
): Promise<{ items: Message[]; nextCursor: string | null }> {
  await this.assertMember(userId, roomId);

  const where: {
    roomId: string;
    OR?: Array<{ createdAt: { lt: Date } } | { createdAt: Date; id: { lt: string } }>;
  } = { roomId };

  if (cursor) {
    const separatorIndex = cursor.lastIndexOf('|');
    const cursorCreatedAt = new Date(cursor.slice(0, separatorIndex));
    const cursorId = cursor.slice(separatorIndex + 1);
    where.OR = [
      { createdAt: { lt: cursorCreatedAt } },
      { createdAt: cursorCreatedAt, id: { lt: cursorId } }, // tie-break bằng id khi createdAt trùng
    ];
  }

  const rows = await this.prisma.message.findMany({
    where,
    orderBy: [{ createdAt: 'desc' }, { id: 'desc' }],
    take: limit + 1, // lấy dư 1 dòng để biết còn trang sau hay không
  });

  const hasMore = rows.length > limit;
  const page = hasMore ? rows.slice(0, limit) : rows;
  const last = page[page.length - 1];
  const nextCursor = hasMore ? `${last.createdAt.toISOString()}|${last.id}` : null;

  return { items: page.reverse(), nextCursor }; // đảo lại thành oldest→newest trước khi trả
}
```

Task 18 là lúc kiểm chứng kỹ nhất — **cả 2 test đều PASS ngay lần đầu**
(không phải sửa lại implementation), nghĩa là thiết kế cursor
`(createdAt, id)` ở Task 17 đã đúng từ đầu.

---

## 19. Task 19 — RoomsController + guards

Tạo `src/rooms/rooms.controller.ts` (5 route, `@UseGuards(JwtAuthGuard)`
ở cấp controller), `src/rooms/rooms.module.ts`. Sửa `app.module.ts` thêm
`RoomsModule`.

```bash
npm run typecheck && npm test   # 21/21 pass — full app đã hoàn chỉnh
git add src/rooms/rooms.controller.ts src/rooms/rooms.module.ts src/app.module.ts
git commit -m "feat: add RoomsController wired behind JwtAuthGuard"
```

---

## 20. Task 20 — Verify thủ công bằng curl + fix phát sinh

### ⚠️ Vấn đề thực tế #2: chạy full test suite bị lỗi ngẫu nhiên

Khi chạy `npm test` (cả 2 file spec cùng lúc) thay vì chạy riêng từng
file như các Task trước, gặp lỗi:

```
PrismaClientKnownRequestError:
Foreign key constraint violated: `RefreshToken_userId_fkey (index)`
```

**Nguyên nhân:** Jest mặc định chạy các spec file bằng nhiều worker song
song. Cả `auth.service.spec.ts` và `rooms.service.spec.ts` đều
`cleanDatabase()` (xoá toàn bộ bảng) trong `beforeEach`, và cả 2 cùng
dùng **một DB Postgres test thật** (đúng yêu cầu spec — không mock
Prisma). Khi 2 file chạy song song, lệnh xoá bảng của file này đụng vào
dữ liệu file kia đang tạo → vi phạm FK ngẫu nhiên theo thời điểm.

**Fix:** thêm `maxWorkers: 1` vào `jest.config.js` để các file spec chạy
tuần tự (không song song):

```javascript
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  rootDir: '.',
  roots: ['<rootDir>/src'],
  testRegex: '.*\\.spec\\.ts$',
  setupFiles: ['<rootDir>/test/jest.env.ts'],
  testTimeout: 15000,
  // Các spec file dùng chung 1 Postgres test thật (đề bài yêu cầu không
  // mock Prisma) và đều truncate cùng bảng trong beforeEach → chạy song
  // song sẽ đua nhau xoá/tạo dữ liệu → phải chạy tuần tự.
  maxWorkers: 1,
};
```

Chạy lại `npm test` 3 lần liên tiếp để chắc chắn hết race condition (không
phải chỉ pass ăn may 1 lần) → cả 3 lần đều PASS 21/21.

```bash
git add jest.config.js
git commit -m "fix: run Jest suites serially against the shared Postgres test DB"
```

### Build + chạy app thật, verify bằng curl

```bash
npm run build
node dist/main.js &   # chạy nền để test bằng curl
```

**1) Register:**

```bash
curl -s -X POST http://localhost:3000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"manual@example.com","password":"password123","displayName":"Manual"}'
# → 201 {accessToken, refreshToken}
```

**2) `/auth/me` có/không có token:**

```bash
curl -s http://localhost:3000/auth/me -H "Authorization: Bearer <accessToken>"
# → 200 {id, email, displayName}

curl -s http://localhost:3000/auth/me
# → 401 {"message":"Unauthorized",...}
```

**3) Refresh rotation + reuse detection (tiêu chí hoàn thành quan trọng
nhất của spec):**

```bash
# Refresh lần 1 — hợp lệ, token cũ bị revoke, nhận token mới
curl -s -X POST http://localhost:3000/auth/refresh -H "Content-Type: application/json" \
  -d '{"refreshToken":"<refreshToken gốc>"}'
# → 200 {accessToken mới, refreshToken mới}

# Dùng lại token gốc (đã bị revoke ở bước trên) — reuse detected
curl -s -X POST http://localhost:3000/auth/refresh -H "Content-Type: application/json" \
  -d '{"refreshToken":"<refreshToken gốc>"}'
# → 401 {"message":"Session revoked — please log in again",...}

# Token MỚI (chưa từng bị reuse) — phải CŨNG chết theo vì mass-revoke
curl -s -X POST http://localhost:3000/auth/refresh -H "Content-Type: application/json" \
  -d '{"refreshToken":"<refreshToken mới nhận ở bước 1>"}'
# → 401 {"message":"Session revoked — please log in again",...}  ✅ đúng như spec yêu cầu
```

**4) Room + message + kiểm tra quyền member:**

```bash
# login lại (session cũ đã chết toàn bộ ở bước 3)
LOGIN=$(curl -s -X POST http://localhost:3000/auth/login -H "Content-Type: application/json" \
  -d '{"email":"manual@example.com","password":"password123"}')

# tạo room
curl -s -X POST http://localhost:3000/rooms -H "Authorization: Bearer <accessToken>" \
  -H "Content-Type: application/json" -d '{"name":"Manual Room"}'

# gửi message
curl -s -X POST http://localhost:3000/rooms/<roomId>/messages \
  -H "Authorization: Bearer <accessToken>" -H "Content-Type: application/json" \
  -d '{"content":"hello world"}'
# → 201

# đọc message
curl -s http://localhost:3000/rooms/<roomId>/messages -H "Authorization: Bearer <accessToken>"
# → 200 {"items":[...], "nextCursor": null}

# user KHÁC (chưa join room) gửi/đọc message trong room đó
# → cả 2 đều phải 403 {"message":"Not a member of this room",...}
```

**5) Check cuối cùng:**

```bash
npm run typecheck && npm test   # tsc sạch, 21/21 test pass
```

---

## Quy trình Git: worktree → merge vào master

```bash
# (đã code xong trong worktree từ đầu bài)
# Quay lại main checkout, merge nhánh worktree vào master:
git checkout master
git merge <tên-branch-worktree>   # có thể dính conflict nhẹ nếu cả 2 bên
                                   # cùng tạo file giống nhau (vd .gitignore) —
                                   # resolve bằng cách gộp nội dung 2 bên

# verify LẠI trên bản đã merge (không tin tưởng test chạy trước đó trong worktree)
npm install        # nếu main checkout chưa có node_modules
npx prisma generate
npm run typecheck && npm test

# dọn worktree sau khi merge xong
git worktree remove <path-worktree>
git branch -d <tên-branch-worktree>
```

---

## Tổng kết — 2 chỗ khác với plan gốc (đáng nhớ nếu làm lại)

1. **Port Postgres dev đổi 5432 → 5434** vì máy có Postgres native chiếm
   port 5432 từ trước. Luôn `netstat -ano | grep 5432` trước khi chạy
   `docker compose up -d` nếu nghi ngờ bị đụng port.
2. **`jest.config.js` cần `maxWorkers: 1`** khi có ≥ 2 spec file cùng
   dùng chung 1 Postgres test thật và cùng `cleanDatabase()` trong
   `beforeEach` — nếu không, `npm test` (chạy tất cả file) sẽ **random
   fail** vì race condition, dù chạy riêng từng file (`npm test --
   <tên-file>`) luôn pass.

Ngoài 2 điểm trên, toàn bộ phần còn lại khớp 100% với
`docs/superpowers/plans/2026-10-11-phase1-auth-rooms-messages-plan.md`.
