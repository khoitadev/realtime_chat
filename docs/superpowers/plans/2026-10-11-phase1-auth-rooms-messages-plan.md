# Phase 1 — Auth + Rooms + Messages Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a flat NestJS monolith (register/login/refresh/logout, create/join room, send/read message) with correct refresh-token rotation + reuse detection and cursor-based message pagination, backed by Postgres via Prisma.

**Architecture:** Single NestJS app (no `apps/*` workspace). `PrismaService` wraps one `PrismaClient` shared by `AuthModule` and `RoomsModule`. Auth uses JWT access tokens (stateless, 15min) + opaque random refresh tokens hashed with SHA-256 and stored in Postgres (stateful, rotated on every use, revoked en masse on reuse). Rooms/messages are plain CRUD gated by `RoomMember` membership checks. No WebSocket, no Redis, no roles — all deferred to later phases.

**Tech Stack:** NestJS 10, Prisma 5 + Postgres 16, `@nestjs/jwt` + `passport-jwt`, `bcrypt`, `class-validator`/`class-transformer`, Jest + `ts-jest` + `@nestjs/testing`, Docker Compose for dev/test Postgres instances.

**Spec:** `docs/superpowers/specs/2026-10-11-phase1-auth-rooms-messages-design.md`

## Global Constraints

- No workspace/`apps/*` split — flat NestJS project at repo root.
- No WebSocket/realtime, no presence/Redis, no service extraction, no room roles, no rate limiting/search/notifications/analytics — all out of scope for Phase 1.
- Refresh-token reuse detection MUST revoke **all** of that user's refresh tokens, not just the reused one (this is the bug being fixed vs. the original system).
- Message pagination MUST use cursor `(createdAt, id)`, never `OFFSET`/`skip`.
- Access token: JWT signed with `JWT_SECRET`, payload `{sub: userId}`, TTL 15 minutes.
- Refresh token: 48-byte random hex string; only `sha256(token)` is stored (`RefreshToken.tokenHash`), `expiresAt` = now + 7 days.
- `refresh()` error messages must distinguish: unknown token → `"Invalid refresh token"`; revoked (reuse) → `"Session revoked — please log in again"`; expired → `"Refresh token expired"`.
- `logout()` revokes exactly the one token passed in — never a mass revoke.
- All routes except `/auth/register`, `/auth/login`, `/auth/refresh` require `Authorization: Bearer <accessToken>`.
- `.../messages` routes return `403 Forbidden` (via `ForbiddenException`) when the caller is not a room member.
- Use Nest's built-in exceptions and default response shape (`{statusCode, message, error}`): `UnauthorizedException` (401), `ConflictException` (409), `ForbiddenException` (403), `NotFoundException` (404).
- TDD against a **real** Postgres test database (separate container/port from dev) — never mock Prisma — specifically for `AuthService.refresh()` and `RoomsService.getMessages()`. Prisma schema declaration, controller/module scaffolding, and basic DTO validation do not need tests written first.

---

## File Structure

```
docker-compose.yml                  # postgres-dev (5432) + postgres-test (5433)
.env                                 # dev DATABASE_URL, JWT_SECRET, PORT
.env.test                            # test DATABASE_URL, JWT_SECRET
package.json / tsconfig.json / tsconfig.build.json / nest-cli.json
jest.config.js
test/jest.env.ts                    # loads .env.test before any test file imports modules
test/db-cleanup.ts                  # cleanDatabase(prisma) helper, FK-safe delete order
prisma/schema.prisma

src/main.ts                         # bootstrap, global ValidationPipe
src/app.module.ts                   # imports PrismaModule, AuthModule, RoomsModule

src/prisma/prisma.service.ts        # PrismaService extends PrismaClient
src/prisma/prisma.module.ts         # @Global() module exporting PrismaService

src/auth/dto/register.dto.ts
src/auth/dto/login.dto.ts
src/auth/dto/refresh.dto.ts
src/auth/auth.service.ts
src/auth/auth.service.spec.ts
src/auth/auth.controller.ts
src/auth/auth.module.ts
src/auth/jwt.strategy.ts
src/auth/jwt-auth.guard.ts

src/rooms/dto/create-room.dto.ts
src/rooms/dto/send-message.dto.ts
src/rooms/rooms.service.ts
src/rooms/rooms.service.spec.ts
src/rooms/rooms.controller.ts
src/rooms/rooms.module.ts
```

One clear responsibility per file: `PrismaService` only wraps the client; each `*.service.ts` holds business logic; each `*.controller.ts` only does HTTP wiring + guard application; DTOs only declare validation shape.

---

### Task 1: Project scaffold (no TDD — scaffolding)

**Files:**
- Create: `package.json`, `tsconfig.json`, `tsconfig.build.json`, `nest-cli.json`, `.gitignore`
- Create: `src/main.ts`, `src/app.module.ts`

**Interfaces:**
- Produces: a runnable empty Nest app (`npm run start:dev` boots on `PORT` env, default 3000), `npm test` wired to Jest, `npm run build` wired to `nest build`.

- [ ] **Step 1: Create `package.json`**

```json
{
  "name": "realtime-chat-v2",
  "version": "0.1.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch",
    "prisma:generate": "prisma generate",
    "prisma:migrate:dev": "prisma migrate dev --name init",
    "prisma:migrate:test": "dotenv -e .env.test -- prisma migrate deploy",
    "test": "jest",
    "typecheck": "tsc --noEmit"
  },
  "dependencies": {
    "@nestjs/common": "^10.4.0",
    "@nestjs/config": "^3.3.0",
    "@nestjs/core": "^10.4.0",
    "@nestjs/jwt": "^10.2.0",
    "@nestjs/passport": "^10.0.3",
    "@nestjs/platform-express": "^10.4.0",
    "@prisma/client": "^5.20.0",
    "bcrypt": "^5.1.1",
    "class-transformer": "^0.5.1",
    "class-validator": "^0.14.1",
    "passport": "^0.7.0",
    "passport-jwt": "^4.0.1",
    "reflect-metadata": "^0.2.2",
    "rxjs": "^7.8.1"
  },
  "devDependencies": {
    "@nestjs/cli": "^10.4.5",
    "@nestjs/testing": "^10.4.0",
    "@types/bcrypt": "^5.0.2",
    "@types/express": "^4.17.21",
    "@types/jest": "^29.5.13",
    "@types/node": "^20.16.0",
    "@types/passport-jwt": "^4.0.1",
    "dotenv": "^16.4.5",
    "dotenv-cli": "^7.4.2",
    "jest": "^29.7.0",
    "prisma": "^5.20.0",
    "ts-jest": "^29.2.5",
    "ts-node": "^10.9.2",
    "typescript": "^5.6.2"
  }
}
```

- [ ] **Step 2: Create `tsconfig.json`**

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
  }
}
```

- [ ] **Step 3: Create `tsconfig.build.json`**

```json
{
  "extends": "./tsconfig.json",
  "exclude": ["node_modules", "dist", "**/*spec.ts", "test"]
}
```

- [ ] **Step 4: Create `nest-cli.json`**

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src"
}
```

- [ ] **Step 5: Create `.gitignore`**

```
node_modules/
dist/
.env
.env.test
*.log
```

- [ ] **Step 6: Create `src/main.ts`**

```typescript
import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  await app.listen(process.env.PORT ?? 3000);
}
bootstrap();
```

- [ ] **Step 7: Create `src/app.module.ts`** (placeholder imports; will be filled in Task 3+)

```typescript
import { Module } from '@nestjs/common';

@Module({
  imports: [],
})
export class AppModule {}
```

- [ ] **Step 8: Install dependencies**

Run: `npm install`
Expected: installs without error, creates `node_modules/` and `package-lock.json`.

- [ ] **Step 9: Verify the app boots**

Run: `npm run build && node dist/main.js`
Expected: process starts, logs Nest bootstrap, listens on port 3000 (Ctrl+C to stop).

- [ ] **Step 10: Commit**

```bash
git add package.json package-lock.json tsconfig.json tsconfig.build.json nest-cli.json .gitignore src/main.ts src/app.module.ts
git commit -m "chore: scaffold NestJS project"
```

---

### Task 2: Docker Compose Postgres + Prisma schema + migrations (no TDD — schema declaration)

**Files:**
- Create: `docker-compose.yml`, `.env`, `.env.test`
- Create: `prisma/schema.prisma`

**Interfaces:**
- Produces: `prisma.schema.prisma` models `User`, `RefreshToken`, `Room`, `RoomMember`, `Message` exactly as in the spec; a dev Postgres on `localhost:5432/chat_dev` and a test Postgres on `localhost:5433/chat_test`; `DATABASE_URL` env var consumed by Prisma Client.

- [ ] **Step 1: Create `docker-compose.yml`**

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
      - "5432:5432"
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

- [ ] **Step 2: Create `.env`**

```
DATABASE_URL="postgresql://postgres:postgres@localhost:5432/chat_dev"
JWT_SECRET="dev-secret-change-me"
PORT=3000
```

- [ ] **Step 3: Create `.env.test`**

```
DATABASE_URL="postgresql://postgres:postgres@localhost:5433/chat_test"
JWT_SECRET="test-secret"
```

- [ ] **Step 4: Create `prisma/schema.prisma`**

```prisma
generator client {
  provider = "prisma-client-js"
}

datasource db {
  provider = "postgresql"
  url      = env("DATABASE_URL")
}

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
  id        String    @id @default(uuid())
  userId    String
  tokenHash String    @unique
  revokedAt DateTime?
  expiresAt DateTime
  createdAt DateTime  @default(now())

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

- [ ] **Step 5: Start the Postgres containers**

Run: `docker compose up -d`
Expected: both `postgres-dev` and `postgres-test` containers report `healthy`/running (`docker compose ps`).

- [ ] **Step 6: Run the dev migration**

Run: `npx prisma migrate dev --name init`
Expected: creates `prisma/migrations/<timestamp>_init/`, applies it to `chat_dev`, generates the Prisma Client — exits 0.

- [ ] **Step 7: Apply the same migration to the test database**

Run: `npm run prisma:migrate:test`
Expected: applies the existing migration to `chat_test` (via `.env.test`'s `DATABASE_URL`) — exits 0, no new migration files created.

- [ ] **Step 8: Commit**

```bash
git add docker-compose.yml prisma/schema.prisma prisma/migrations
git commit -m "feat: add Postgres dev/test containers and Prisma schema"
```

Note: `.env` and `.env.test` are gitignored — do not commit them. If reviewers need the values, they're documented in this task.

---

### Task 3: PrismaService/PrismaModule + Jest/test-DB wiring (no TDD — infra)

**Files:**
- Create: `src/prisma/prisma.service.ts`, `src/prisma/prisma.module.ts`
- Modify: `src/app.module.ts`
- Create: `jest.config.js`, `test/jest.env.ts`, `test/db-cleanup.ts`

**Interfaces:**
- Produces: `PrismaService` (injectable, extends `PrismaClient`, connects in `onModuleInit`, disconnects in `onModuleDestroy`) — consumed by `AuthService` and `RoomsService` in later tasks. `cleanDatabase(prisma: PrismaService): Promise<void>` — truncates all tables in FK-safe order, used in every spec's `beforeEach`.

- [ ] **Step 1: Create `src/prisma/prisma.service.ts`**

```typescript
import { Injectable, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { PrismaClient } from '@prisma/client';

@Injectable()
export class PrismaService extends PrismaClient implements OnModuleInit, OnModuleDestroy {
  async onModuleInit() {
    await this.$connect();
  }

  async onModuleDestroy() {
    await this.$disconnect();
  }
}
```

- [ ] **Step 2: Create `src/prisma/prisma.module.ts`**

```typescript
import { Global, Module } from '@nestjs/common';
import { PrismaService } from './prisma.service';

@Global()
@Module({
  providers: [PrismaService],
  exports: [PrismaService],
})
export class PrismaModule {}
```

- [ ] **Step 3: Wire `PrismaModule` into `src/app.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { PrismaModule } from './prisma/prisma.module';

@Module({
  imports: [PrismaModule],
})
export class AppModule {}
```

- [ ] **Step 4: Create `test/jest.env.ts`**

```typescript
import { config } from 'dotenv';
import { resolve } from 'path';

config({ path: resolve(__dirname, '../.env.test') });
```

- [ ] **Step 5: Create `jest.config.js`**

```javascript
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  rootDir: '.',
  roots: ['<rootDir>/src'],
  testRegex: '.*\\.spec\\.ts$',
  setupFiles: ['<rootDir>/test/jest.env.ts'],
  testTimeout: 15000,
};
```

- [ ] **Step 6: Create `test/db-cleanup.ts`**

```typescript
import { PrismaService } from '../src/prisma/prisma.service';

export async function cleanDatabase(prisma: PrismaService): Promise<void> {
  await prisma.message.deleteMany();
  await prisma.roomMember.deleteMany();
  await prisma.refreshToken.deleteMany();
  await prisma.room.deleteMany();
  await prisma.user.deleteMany();
}
```

- [ ] **Step 7: Sanity-check the test DB wiring with a throwaway spec**

Create a temporary `src/prisma/prisma.sanity.spec.ts`:

```typescript
import { PrismaService } from './prisma.service';

describe('PrismaService test DB wiring', () => {
  it('connects to the test database and can query', async () => {
    const prisma = new PrismaService();
    await prisma.$connect();
    const result = await prisma.$queryRaw<{ now: Date }[]>`SELECT now()`;
    expect(result[0].now).toBeInstanceOf(Date);
    await prisma.$disconnect();
  });
});
```

Run: `npm test -- prisma.sanity`
Expected: PASS, 1 test.

Then delete `src/prisma/prisma.sanity.spec.ts` — it was only to confirm wiring, not a permanent spec.

- [ ] **Step 8: Commit**

```bash
git add src/prisma/prisma.service.ts src/prisma/prisma.module.ts src/app.module.ts jest.config.js test/jest.env.ts test/db-cleanup.ts
git commit -m "feat: wire PrismaService and Jest to the Postgres test database"
```

---

### Task 4: Auth DTOs + JwtModule config (no TDD — DTO validation + module scaffold)

**Files:**
- Create: `src/auth/dto/register.dto.ts`, `src/auth/dto/login.dto.ts`, `src/auth/dto/refresh.dto.ts`
- Create: `src/auth/auth.module.ts`

**Interfaces:**
- Produces: `RegisterDto {email: string, password: string, displayName: string}`, `LoginDto {email: string, password: string}`, `RefreshDto {refreshToken: string}` — consumed by `AuthController` (Task 12) and directly by `AuthService` tests (Tasks 5-11). `AuthModule` imports `JwtModule.registerAsync` reading `JWT_SECRET` from `ConfigService`.

- [ ] **Step 1: Create `src/auth/dto/register.dto.ts`**

```typescript
import { IsEmail, IsString, MinLength } from 'class-validator';

export class RegisterDto {
  @IsEmail()
  email!: string;

  @MinLength(8)
  password!: string;

  @IsString()
  @MinLength(1)
  displayName!: string;
}
```

- [ ] **Step 2: Create `src/auth/dto/login.dto.ts`**

```typescript
import { IsEmail, IsString } from 'class-validator';

export class LoginDto {
  @IsEmail()
  email!: string;

  @IsString()
  password!: string;
}
```

- [ ] **Step 3: Create `src/auth/dto/refresh.dto.ts`**

```typescript
import { IsString } from 'class-validator';

export class RefreshDto {
  @IsString()
  refreshToken!: string;
}
```

- [ ] **Step 4: Create `src/auth/auth.module.ts`** (service added in next task; declare shape now, fill providers as they're created)

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { AuthService } from './auth.service';

@Module({
  imports: [
    ConfigModule,
    JwtModule.registerAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => ({
        secret: config.get<string>('JWT_SECRET'),
        signOptions: { expiresIn: '15m' },
      }),
    }),
  ],
  providers: [AuthService],
  exports: [AuthService],
})
export class AuthModule {}
```

- [ ] **Step 5: Add `ConfigModule.forRoot()` to `src/app.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from './prisma/prisma.module';
import { AuthModule } from './auth/auth.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule, AuthModule],
})
export class AppModule {}
```

This task references `AuthService`, which doesn't exist yet — that's expected; it's created in Task 5 and the project won't compile until then. Do not run `npm run typecheck` until after Task 5.

- [ ] **Step 6: Commit**

```bash
git add src/auth/dto src/auth/auth.module.ts src/app.module.ts
git commit -m "feat: add auth DTOs and JwtModule config"
```

---

### Task 5: AuthService.register (TDD)

**Files:**
- Create: `src/auth/auth.service.ts`, `src/auth/auth.service.spec.ts`

**Interfaces:**
- Consumes: `PrismaService` (Task 3), `JwtService` from `@nestjs/jwt`, `RegisterDto` (Task 4).
- Produces: `AuthService.register(dto: RegisterDto): Promise<{accessToken: string, refreshToken: string}>` — consumed by `AuthController` (Task 12) and by this spec's later `describe` blocks (Tasks 6-11) to create test users.

- [ ] **Step 1: Write the failing test**

```typescript
import { Test } from '@nestjs/testing';
import { ConfigModule } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { ConflictException } from '@nestjs/common';
import { AuthService } from './auth.service';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

describe('AuthService', () => {
  let authService: AuthService;
  let prisma: PrismaService;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        JwtModule.register({ secret: process.env.JWT_SECRET, signOptions: { expiresIn: '15m' } }),
      ],
      providers: [AuthService, PrismaService],
    }).compile();

    authService = moduleRef.get(AuthService);
    prisma = moduleRef.get(PrismaService);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  describe('register', () => {
    it('creates a user and returns an access+refresh token pair', async () => {
      const result = await authService.register({
        email: 'alice@example.com',
        password: 'password123',
        displayName: 'Alice',
      });

      expect(result.accessToken).toBeDefined();
      expect(result.refreshToken).toBeDefined();

      const user = await prisma.user.findUnique({ where: { email: 'alice@example.com' } });
      expect(user).not.toBeNull();
      expect(user!.passwordHash).not.toBe('password123');
    });

    it('rejects a duplicate email with 409', async () => {
      await authService.register({
        email: 'bob@example.com',
        password: 'password123',
        displayName: 'Bob',
      });

      await expect(
        authService.register({
          email: 'bob@example.com',
          password: 'anotherpassword',
          displayName: 'Bob 2',
        }),
      ).rejects.toThrow(ConflictException);
    });
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- auth.service`
Expected: FAIL — `Cannot find module './auth.service'` (file doesn't exist yet).

- [ ] **Step 3: Write minimal implementation**

```typescript
import { ConflictException, Injectable } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import * as bcrypt from 'bcrypt';
import { randomBytes, createHash } from 'crypto';
import { PrismaService } from '../prisma/prisma.service';
import { RegisterDto } from './dto/register.dto';

@Injectable()
export class AuthService {
  constructor(
    private readonly prisma: PrismaService,
    private readonly jwtService: JwtService,
  ) {}

  private async issueTokens(userId: string): Promise<{ accessToken: string; refreshToken: string }> {
    const accessToken = await this.jwtService.signAsync({ sub: userId });
    const refreshToken = randomBytes(48).toString('hex');
    const tokenHash = createHash('sha256').update(refreshToken).digest('hex');
    const expiresAt = new Date(Date.now() + 7 * 24 * 60 * 60 * 1000);

    await this.prisma.refreshToken.create({
      data: { userId, tokenHash, expiresAt },
    });

    return { accessToken, refreshToken };
  }

  async register(dto: RegisterDto): Promise<{ accessToken: string; refreshToken: string }> {
    const existing = await this.prisma.user.findUnique({ where: { email: dto.email } });
    if (existing) {
      throw new ConflictException('Email already registered');
    }

    const passwordHash = await bcrypt.hash(dto.password, 10);
    const user = await this.prisma.user.create({
      data: { email: dto.email, passwordHash, displayName: dto.displayName },
    });

    return this.issueTokens(user.id);
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- auth.service`
Expected: PASS, 2 tests.

- [ ] **Step 5: Commit**

```bash
git add src/auth/auth.service.ts src/auth/auth.service.spec.ts
git commit -m "feat: add AuthService.register with password hashing"
```

---

### Task 6: AuthService.login (TDD)

**Files:**
- Modify: `src/auth/auth.service.ts`, `src/auth/auth.service.spec.ts`

**Interfaces:**
- Produces: `AuthService.login(dto: LoginDto): Promise<{accessToken: string, refreshToken: string}>`.

- [ ] **Step 1: Write the failing test**

Add inside the top-level `describe('AuthService', ...)` block, after `describe('register', ...)`:

```typescript
  describe('login', () => {
    beforeEach(async () => {
      await authService.register({
        email: 'carol@example.com',
        password: 'correct-password',
        displayName: 'Carol',
      });
    });

    it('returns tokens for correct credentials', async () => {
      const result = await authService.login({
        email: 'carol@example.com',
        password: 'correct-password',
      });

      expect(result.accessToken).toBeDefined();
      expect(result.refreshToken).toBeDefined();
    });

    it('rejects an unknown email with 401', async () => {
      await expect(
        authService.login({ email: 'nobody@example.com', password: 'whatever' }),
      ).rejects.toThrow(UnauthorizedException);
    });

    it('rejects a wrong password with 401', async () => {
      await expect(
        authService.login({ email: 'carol@example.com', password: 'wrong-password' }),
      ).rejects.toThrow(UnauthorizedException);
    });
  });
```

Add `UnauthorizedException` and `LoginDto` to the spec's imports:

```typescript
import { ConflictException, UnauthorizedException } from '@nestjs/common';
```

(keep the existing `ConflictException` import from Task 5, just add `UnauthorizedException` to the same import line).

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- auth.service`
Expected: FAIL — `authService.login is not a function`.

- [ ] **Step 3: Write minimal implementation**

Add to `src/auth/auth.service.ts`, after `register(...)`, importing `LoginDto` and `UnauthorizedException`:

```typescript
import { ConflictException, Injectable, UnauthorizedException } from '@nestjs/common';
// ...
import { LoginDto } from './dto/login.dto';
```

```typescript
  async login(dto: LoginDto): Promise<{ accessToken: string; refreshToken: string }> {
    const user = await this.prisma.user.findUnique({ where: { email: dto.email } });
    if (!user) {
      throw new UnauthorizedException('Invalid credentials');
    }

    const valid = await bcrypt.compare(dto.password, user.passwordHash);
    if (!valid) {
      throw new UnauthorizedException('Invalid credentials');
    }

    return this.issueTokens(user.id);
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- auth.service`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add src/auth/auth.service.ts src/auth/auth.service.spec.ts
git commit -m "feat: add AuthService.login"
```

---

### Task 7: AuthService.refresh() — happy-path rotation (TDD)

**Files:**
- Modify: `src/auth/auth.service.ts`, `src/auth/auth.service.spec.ts`

**Interfaces:**
- Produces: `AuthService.refresh(refreshToken: string): Promise<{accessToken: string, refreshToken: string}>`.

- [ ] **Step 1: Write the failing test**

Add a new top-level `describe('refresh', ...)` block (sibling to `register`/`login`):

```typescript
  describe('refresh', () => {
    it('rotates the token: old token becomes revoked, new token is valid and unrevoked', async () => {
      const { refreshToken } = await authService.register({
        email: 'dave@example.com',
        password: 'password123',
        displayName: 'Dave',
      });

      const result = await authService.refresh(refreshToken);

      expect(result.accessToken).toBeDefined();
      expect(result.refreshToken).toBeDefined();
      expect(result.refreshToken).not.toBe(refreshToken);

      const oldHash = createHash('sha256').update(refreshToken).digest('hex');
      const oldRow = await prisma.refreshToken.findUnique({ where: { tokenHash: oldHash } });
      expect(oldRow!.revokedAt).not.toBeNull();

      const newHash = createHash('sha256').update(result.refreshToken).digest('hex');
      const newRow = await prisma.refreshToken.findUnique({ where: { tokenHash: newHash } });
      expect(newRow!.revokedAt).toBeNull();
    });
  });
```

Add `createHash` to the spec's imports:

```typescript
import { createHash } from 'crypto';
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- auth.service`
Expected: FAIL — `authService.refresh is not a function`.

- [ ] **Step 3: Write minimal implementation**

Add to `src/auth/auth.service.ts`, after `login(...)`:

```typescript
  async refresh(refreshToken: string): Promise<{ accessToken: string; refreshToken: string }> {
    const tokenHash = createHash('sha256').update(refreshToken).digest('hex');
    const stored = await this.prisma.refreshToken.findUnique({ where: { tokenHash } });

    if (!stored) {
      throw new UnauthorizedException('Invalid refresh token');
    }

    await this.prisma.refreshToken.update({
      where: { id: stored.id },
      data: { revokedAt: new Date() },
    });

    return this.issueTokens(stored.userId);
  }
```

(This is intentionally incomplete — reuse detection and expiry are added in Tasks 9-10. The happy path alone is enough to pass this test.)

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- auth.service`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add src/auth/auth.service.ts src/auth/auth.service.spec.ts
git commit -m "feat: add AuthService.refresh happy-path rotation"
```

---

### Task 8: AuthService.refresh() — invalid token case (TDD)

**Files:**
- Modify: `src/auth/auth.service.spec.ts`

**Interfaces:**
- No new interface — this task only adds a test; `refresh()`'s existing "not found" branch (Task 7) already satisfies it.

- [ ] **Step 1: Write the test**

Add to the `describe('refresh', ...)` block from Task 7:

```typescript
    it('rejects an unknown token with "Invalid refresh token"', async () => {
      await expect(authService.refresh('not-a-real-token')).rejects.toThrow(
        'Invalid refresh token',
      );
    });
```

- [ ] **Step 2: Run test to verify it passes immediately**

Run: `npm test -- auth.service`
Expected: PASS, 7 tests. (No implementation change needed — Task 7's `if (!stored)` branch already covers this. This step exists to lock the exact error message as a regression guard.)

- [ ] **Step 3: Commit**

```bash
git add src/auth/auth.service.spec.ts
git commit -m "test: pin AuthService.refresh invalid-token error message"
```

---

### Task 9: AuthService.refresh() — reuse detection, mass revoke (TDD, fix #1)

**Files:**
- Modify: `src/auth/auth.service.ts`, `src/auth/auth.service.spec.ts`

**Interfaces:**
- Produces: `refresh()` now revokes **all** of a user's unrevoked refresh tokens when a revoked token is presented, and throws with message `"Session revoked — please log in again"`.

- [ ] **Step 1: Write the failing test**

Add to the `describe('refresh', ...)` block:

```typescript
    it('detects reuse of a revoked token and revokes ALL of that user\'s sessions', async () => {
      const { refreshToken: sessionA } = await authService.register({
        email: 'erin@example.com',
        password: 'password123',
        displayName: 'Erin',
      });
      // second session for the same user
      const { refreshToken: sessionB } = await authService.login({
        email: 'erin@example.com',
        password: 'password123',
      });

      // rotate session A once — the original sessionA token is now revoked
      await authService.refresh(sessionA);

      // reuse the already-revoked original sessionA token
      await expect(authService.refresh(sessionA)).rejects.toThrow(
        'Session revoked — please log in again',
      );

      // sessionB, which was never reused, must now ALSO be revoked
      await expect(authService.refresh(sessionB)).rejects.toThrow(
        'Session revoked — please log in again',
      );
    });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- auth.service`
Expected: FAIL on the `sessionB` assertion — current implementation throws `"Invalid refresh token"`-style behavior is fine for the reused token itself (it's found, just not yet distinguished from "invalid"), but `sessionB` refresh currently *succeeds* because nothing revokes it. Actual failure: the second `expect(...).rejects.toThrow(...)` assertion fails because `authService.refresh(sessionB)` resolves instead of rejecting.

- [ ] **Step 3: Write minimal implementation**

Replace the body of `refresh()` in `src/auth/auth.service.ts`:

```typescript
  async refresh(refreshToken: string): Promise<{ accessToken: string; refreshToken: string }> {
    const tokenHash = createHash('sha256').update(refreshToken).digest('hex');
    const stored = await this.prisma.refreshToken.findUnique({ where: { tokenHash } });

    if (!stored) {
      throw new UnauthorizedException('Invalid refresh token');
    }

    if (stored.revokedAt) {
      await this.prisma.refreshToken.updateMany({
        where: { userId: stored.userId, revokedAt: null },
        data: { revokedAt: new Date() },
      });
      throw new UnauthorizedException('Session revoked — please log in again');
    }

    await this.prisma.refreshToken.update({
      where: { id: stored.id },
      data: { revokedAt: new Date() },
    });

    return this.issueTokens(stored.userId);
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- auth.service`
Expected: PASS, 8 tests.

- [ ] **Step 5: Commit**

```bash
git add src/auth/auth.service.ts src/auth/auth.service.spec.ts
git commit -m "fix: revoke ALL user sessions on refresh-token reuse detection"
```

---

### Task 10: AuthService.refresh() — expired token case (TDD)

**Files:**
- Modify: `src/auth/auth.service.ts`, `src/auth/auth.service.spec.ts`

**Interfaces:**
- Produces: `refresh()` throws `"Refresh token expired"` for a non-revoked but time-expired token.

- [ ] **Step 1: Write the failing test**

Add to the `describe('refresh', ...)` block:

```typescript
    it('rejects an expired (but not revoked) token with "Refresh token expired"', async () => {
      const { refreshToken: ownerToken } = await authService.register({
        email: 'frank@example.com',
        password: 'password123',
        displayName: 'Frank',
      });
      const owner = await prisma.user.findUniqueOrThrow({ where: { email: 'frank@example.com' } });
      void ownerToken;

      const expiredPlainToken = 'expired-token-plaintext';
      const expiredHash = createHash('sha256').update(expiredPlainToken).digest('hex');
      await prisma.refreshToken.create({
        data: {
          userId: owner.id,
          tokenHash: expiredHash,
          expiresAt: new Date(Date.now() - 1000), // 1 second in the past
        },
      });

      await expect(authService.refresh(expiredPlainToken)).rejects.toThrow(
        'Refresh token expired',
      );
    });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- auth.service`
Expected: FAIL — current code rotates the "expired" token and returns new tokens instead of throwing (no expiry check exists yet).

- [ ] **Step 3: Write minimal implementation**

In `src/auth/auth.service.ts`, add the expiry check between the reuse check and the rotation in `refresh()`:

```typescript
    if (stored.revokedAt) {
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- auth.service`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add src/auth/auth.service.ts src/auth/auth.service.spec.ts
git commit -m "feat: add AuthService.refresh expired-token case"
```

---

### Task 11: AuthService.logout (TDD)

**Files:**
- Modify: `src/auth/auth.service.ts`, `src/auth/auth.service.spec.ts`

**Interfaces:**
- Produces: `AuthService.logout(refreshToken: string): Promise<{success: true}>` — revokes exactly the one token passed, leaving the user's other sessions untouched.

- [ ] **Step 1: Write the failing test**

Add a new top-level `describe('logout', ...)` block:

```typescript
  describe('logout', () => {
    it('revokes only the given token, leaving other sessions of the same user valid', async () => {
      const { refreshToken: sessionA } = await authService.register({
        email: 'grace@example.com',
        password: 'password123',
        displayName: 'Grace',
      });
      const { refreshToken: sessionB } = await authService.login({
        email: 'grace@example.com',
        password: 'password123',
      });

      const result = await authService.logout(sessionA);
      expect(result).toEqual({ success: true });

      const hashA = createHash('sha256').update(sessionA).digest('hex');
      const rowA = await prisma.refreshToken.findUnique({ where: { tokenHash: hashA } });
      expect(rowA!.revokedAt).not.toBeNull();

      // sessionB must still be usable
      const refreshed = await authService.refresh(sessionB);
      expect(refreshed.accessToken).toBeDefined();
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- auth.service`
Expected: FAIL — `authService.logout is not a function`.

- [ ] **Step 3: Write minimal implementation**

Add to `src/auth/auth.service.ts`, after `refresh(...)`:

```typescript
  async logout(refreshToken: string): Promise<{ success: true }> {
    const tokenHash = createHash('sha256').update(refreshToken).digest('hex');
    await this.prisma.refreshToken.updateMany({
      where: { tokenHash, revokedAt: null },
      data: { revokedAt: new Date() },
    });
    return { success: true };
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- auth.service`
Expected: PASS, 10 tests.

- [ ] **Step 5: Commit**

```bash
git add src/auth/auth.service.ts src/auth/auth.service.spec.ts
git commit -m "feat: add AuthService.logout"
```

---

### Task 12: JwtStrategy + JwtAuthGuard + AuthController (no TDD — scaffold)

**Files:**
- Create: `src/auth/jwt.strategy.ts`, `src/auth/jwt-auth.guard.ts`, `src/auth/auth.controller.ts`
- Modify: `src/auth/auth.module.ts`

**Interfaces:**
- Consumes: `AuthService` (register/login/refresh/logout from Tasks 5-11), DTOs (Task 4).
- Produces: `JwtAuthGuard` (a `@UseGuards(JwtAuthGuard)`-ready Passport guard) — consumed by `RoomsController` (Task 19). Authenticated requests get `req.user = {id: string}`.

- [ ] **Step 1: Create `src/auth/jwt.strategy.ts`**

```typescript
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(config: ConfigService) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      secretOrKey: config.get<string>('JWT_SECRET')!,
    });
  }

  async validate(payload: { sub: string }) {
    return { id: payload.sub };
  }
}
```

- [ ] **Step 2: Create `src/auth/jwt-auth.guard.ts`**

```typescript
import { Injectable } from '@nestjs/common';
import { AuthGuard } from '@nestjs/passport';

@Injectable()
export class JwtAuthGuard extends AuthGuard('jwt') {}
```

- [ ] **Step 3: Create `src/auth/auth.controller.ts`**

```typescript
import { Body, Controller, Get, HttpCode, Post, Req, UseGuards } from '@nestjs/common';
import { AuthService } from './auth.service';
import { RegisterDto } from './dto/register.dto';
import { LoginDto } from './dto/login.dto';
import { RefreshDto } from './dto/refresh.dto';
import { JwtAuthGuard } from './jwt-auth.guard';
import { PrismaService } from '../prisma/prisma.service';

@Controller('auth')
export class AuthController {
  constructor(
    private readonly authService: AuthService,
    private readonly prisma: PrismaService,
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
}
```

- [ ] **Step 4: Wire `JwtStrategy` and `AuthController` into `src/auth/auth.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { PassportModule } from '@nestjs/passport';
import { AuthService } from './auth.service';
import { AuthController } from './auth.controller';
import { JwtStrategy } from './jwt.strategy';

@Module({
  imports: [
    ConfigModule,
    PassportModule,
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

- [ ] **Step 5: Typecheck and run the full test suite**

Run: `npm run typecheck && npm test`
Expected: both exit 0; all existing `auth.service.spec.ts` tests still PASS (10 tests).

- [ ] **Step 6: Commit**

```bash
git add src/auth/jwt.strategy.ts src/auth/jwt-auth.guard.ts src/auth/auth.controller.ts src/auth/auth.module.ts
git commit -m "feat: add JwtStrategy, JwtAuthGuard, and AuthController"
```

---

### Task 13: RoomsService.createRoom (TDD)

**Files:**
- Create: `src/rooms/dto/create-room.dto.ts`, `src/rooms/rooms.service.ts`, `src/rooms/rooms.service.spec.ts`

**Interfaces:**
- Consumes: `PrismaService` (Task 3).
- Produces: `RoomsService.createRoom(userId: string, dto: CreateRoomDto): Promise<Room>` where creating a room also makes the creator a member. `CreateRoomDto {name: string}`.

- [ ] **Step 1: Create `src/rooms/dto/create-room.dto.ts`**

```typescript
import { IsString, MinLength } from 'class-validator';

export class CreateRoomDto {
  @IsString()
  @MinLength(1)
  name!: string;
}
```

- [ ] **Step 2: Write the failing test**

Create `src/rooms/rooms.service.spec.ts`:

```typescript
import { Test } from '@nestjs/testing';
import { RoomsService } from './rooms.service';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

describe('RoomsService', () => {
  let roomsService: RoomsService;
  let prisma: PrismaService;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [RoomsService, PrismaService],
    }).compile();

    roomsService = moduleRef.get(RoomsService);
    prisma = moduleRef.get(PrismaService);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  async function createUser(email: string) {
    return prisma.user.create({
      data: { email, passwordHash: 'x', displayName: email },
    });
  }

  describe('createRoom', () => {
    it('creates a room and makes the creator a member', async () => {
      const user = await createUser('owner@example.com');

      const room = await roomsService.createRoom(user.id, { name: 'General' });

      expect(room.name).toBe('General');
      expect(room.createdBy).toBe(user.id);

      const membership = await prisma.roomMember.findUnique({
        where: { roomId_userId: { roomId: room.id, userId: user.id } },
      });
      expect(membership).not.toBeNull();
    });
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npm test -- rooms.service`
Expected: FAIL — `Cannot find module './rooms.service'`.

- [ ] **Step 4: Write minimal implementation**

Create `src/rooms/rooms.service.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { Room } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { CreateRoomDto } from './dto/create-room.dto';

@Injectable()
export class RoomsService {
  constructor(private readonly prisma: PrismaService) {}

  async createRoom(userId: string, dto: CreateRoomDto): Promise<Room> {
    return this.prisma.room.create({
      data: {
        name: dto.name,
        createdBy: userId,
        members: { create: { userId } },
      },
    });
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `npm test -- rooms.service`
Expected: PASS, 1 test.

- [ ] **Step 6: Commit**

```bash
git add src/rooms/dto/create-room.dto.ts src/rooms/rooms.service.ts src/rooms/rooms.service.spec.ts
git commit -m "feat: add RoomsService.createRoom"
```

---

### Task 14: RoomsService.joinRoom (TDD)

**Files:**
- Modify: `src/rooms/rooms.service.ts`, `src/rooms/rooms.service.spec.ts`

**Interfaces:**
- Produces: `RoomsService.joinRoom(userId: string, roomId: string): Promise<RoomMember>`, throws `NotFoundException` for an unknown room; idempotent for an already-joined member.

- [ ] **Step 1: Write the failing test**

Add a new `describe('joinRoom', ...)` block:

```typescript
  describe('joinRoom', () => {
    it('adds the user as a member of an existing room', async () => {
      const owner = await createUser('owner2@example.com');
      const joiner = await createUser('joiner@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Random' });

      await roomsService.joinRoom(joiner.id, room.id);

      const membership = await prisma.roomMember.findUnique({
        where: { roomId_userId: { roomId: room.id, userId: joiner.id } },
      });
      expect(membership).not.toBeNull();
    });

    it('throws NotFoundException for a non-existent room', async () => {
      const user = await createUser('lonely@example.com');

      await expect(roomsService.joinRoom(user.id, 'does-not-exist')).rejects.toThrow(
        NotFoundException,
      );
    });

    it('is idempotent when the user is already a member', async () => {
      const owner = await createUser('owner3@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Dup' });

      await expect(roomsService.joinRoom(owner.id, room.id)).resolves.toBeDefined();
    });
  });
```

Add `NotFoundException` to the spec's imports: `import { NotFoundException } from '@nestjs/common';`

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- rooms.service`
Expected: FAIL — `roomsService.joinRoom is not a function`.

- [ ] **Step 3: Write minimal implementation**

Add to `src/rooms/rooms.service.ts`:

```typescript
import { Injectable, NotFoundException } from '@nestjs/common';
import { Room, RoomMember } from '@prisma/client';
```

```typescript
  async joinRoom(userId: string, roomId: string): Promise<RoomMember> {
    const room = await this.prisma.room.findUnique({ where: { id: roomId } });
    if (!room) {
      throw new NotFoundException('Room not found');
    }

    return this.prisma.roomMember.upsert({
      where: { roomId_userId: { roomId, userId } },
      create: { roomId, userId },
      update: {},
    });
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- rooms.service`
Expected: PASS, 4 tests.

- [ ] **Step 5: Commit**

```bash
git add src/rooms/rooms.service.ts src/rooms/rooms.service.spec.ts
git commit -m "feat: add RoomsService.joinRoom"
```

---

### Task 15: RoomsService.listRooms (TDD)

**Files:**
- Modify: `src/rooms/rooms.service.ts`, `src/rooms/rooms.service.spec.ts`

**Interfaces:**
- Produces: `RoomsService.listRooms(userId: string): Promise<Room[]>` — only rooms the user is a member of.

- [ ] **Step 1: Write the failing test**

```typescript
  describe('listRooms', () => {
    it('returns only rooms the user has joined', async () => {
      const user = await createUser('lister@example.com');
      const outsider = await createUser('outsider@example.com');

      const roomA = await roomsService.createRoom(user.id, { name: 'A' });
      await roomsService.createRoom(outsider.id, { name: 'B' });
      await roomsService.joinRoom(user.id, roomA.id); // already a member, no-op

      const rooms = await roomsService.listRooms(user.id);

      expect(rooms.map((r) => r.id)).toEqual([roomA.id]);
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- rooms.service`
Expected: FAIL — `roomsService.listRooms is not a function`.

- [ ] **Step 3: Write minimal implementation**

```typescript
  async listRooms(userId: string): Promise<Room[]> {
    const memberships = await this.prisma.roomMember.findMany({
      where: { userId },
      include: { room: true },
    });
    return memberships.map((m) => m.room);
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- rooms.service`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add src/rooms/rooms.service.ts src/rooms/rooms.service.spec.ts
git commit -m "feat: add RoomsService.listRooms"
```

---

### Task 16: RoomsService.sendMessage with membership check (TDD)

**Files:**
- Create: `src/rooms/dto/send-message.dto.ts`
- Modify: `src/rooms/rooms.service.ts`, `src/rooms/rooms.service.spec.ts`

**Interfaces:**
- Produces: a private `assertMember(userId, roomId): Promise<void>` (throws `ForbiddenException`) reused by `sendMessage` and `getMessages` (Task 17); `RoomsService.sendMessage(userId: string, roomId: string, dto: SendMessageDto): Promise<Message>`. `SendMessageDto {content: string}`.

- [ ] **Step 1: Create `src/rooms/dto/send-message.dto.ts`**

```typescript
import { IsString, MinLength } from 'class-validator';

export class SendMessageDto {
  @IsString()
  @MinLength(1)
  content!: string;
}
```

- [ ] **Step 2: Write the failing test**

```typescript
  describe('sendMessage', () => {
    it('creates a message when the sender is a room member', async () => {
      const user = await createUser('sender@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Chat' });

      const message = await roomsService.sendMessage(user.id, room.id, { content: 'hello' });

      expect(message.content).toBe('hello');
      expect(message.senderId).toBe(user.id);
      expect(message.roomId).toBe(room.id);
    });

    it('throws ForbiddenException when the sender is not a room member', async () => {
      const owner = await createUser('owner4@example.com');
      const outsider = await createUser('outsider2@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Private' });

      await expect(
        roomsService.sendMessage(outsider.id, room.id, { content: 'intruder' }),
      ).rejects.toThrow(ForbiddenException);
    });
  });
```

Add `ForbiddenException` to the spec's imports: `import { ForbiddenException, NotFoundException } from '@nestjs/common';`

- [ ] **Step 3: Run test to verify it fails**

Run: `npm test -- rooms.service`
Expected: FAIL — `roomsService.sendMessage is not a function`.

- [ ] **Step 4: Write minimal implementation**

```typescript
import { ForbiddenException, Injectable, NotFoundException } from '@nestjs/common';
import { Message, Room, RoomMember } from '@prisma/client';
import { SendMessageDto } from './dto/send-message.dto';
```

```typescript
  private async assertMember(userId: string, roomId: string): Promise<void> {
    const membership = await this.prisma.roomMember.findUnique({
      where: { roomId_userId: { roomId, userId } },
    });
    if (!membership) {
      throw new ForbiddenException('Not a member of this room');
    }
  }

  async sendMessage(userId: string, roomId: string, dto: SendMessageDto): Promise<Message> {
    await this.assertMember(userId, roomId);
    return this.prisma.message.create({
      data: { roomId, senderId: userId, content: dto.content },
    });
  }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `npm test -- rooms.service`
Expected: PASS, 7 tests.

- [ ] **Step 6: Commit**

```bash
git add src/rooms/dto/send-message.dto.ts src/rooms/rooms.service.ts src/rooms/rooms.service.spec.ts
git commit -m "feat: add RoomsService.sendMessage with membership check"
```

---

### Task 17: RoomsService.getMessages — cursor pagination happy path (TDD, fix #2)

**Files:**
- Modify: `src/rooms/rooms.service.ts`, `src/rooms/rooms.service.spec.ts`

**Interfaces:**
- Produces: `RoomsService.getMessages(userId: string, roomId: string, cursor?: string, limit = 20): Promise<{items: Message[], nextCursor: string | null}>`. Cursor format: `` `${createdAt.toISOString()}|${id}` ``. Items ordered oldest→newest in the response (DB query is `DESC`, reversed before returning).

- [ ] **Step 1: Write the failing test**

```typescript
  describe('getMessages', () => {
    async function seedMessages(roomId: string, senderId: string, count: number) {
      const messages = [];
      for (let i = 0; i < count; i++) {
        const message = await prisma.message.create({
          data: {
            roomId,
            senderId,
            content: `message-${i}`,
            createdAt: new Date(Date.now() + i * 1000), // strictly increasing
          },
        });
        messages.push(message);
      }
      return messages;
    }

    it('throws ForbiddenException when the caller is not a member', async () => {
      const owner = await createUser('msgowner@example.com');
      const outsider = await createUser('msgoutsider@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Secret' });

      await expect(roomsService.getMessages(outsider.id, room.id)).rejects.toThrow(
        ForbiddenException,
      );
    });

    it('paginates in newest-first pages but returns each page oldest-to-newest, with a nextCursor while more remain', async () => {
      const user = await createUser('paginator@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Paged' });
      const seeded = await seedMessages(room.id, user.id, 5); // oldest..newest

      const page1 = await roomsService.getMessages(user.id, room.id, undefined, 2);
      expect(page1.items.map((m) => m.content)).toEqual(['message-3', 'message-4']);
      expect(page1.nextCursor).not.toBeNull();

      const page2 = await roomsService.getMessages(user.id, room.id, page1.nextCursor!, 2);
      expect(page2.items.map((m) => m.content)).toEqual(['message-1', 'message-2']);
      expect(page2.nextCursor).not.toBeNull();

      const page3 = await roomsService.getMessages(user.id, room.id, page2.nextCursor!, 2);
      expect(page3.items.map((m) => m.content)).toEqual(['message-0']);
      expect(page3.nextCursor).toBeNull();

      void seeded;
    });
  });
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npm test -- rooms.service`
Expected: FAIL — `roomsService.getMessages is not a function`.

- [ ] **Step 3: Write minimal implementation**

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
        { createdAt: cursorCreatedAt, id: { lt: cursorId } },
      ];
    }

    const rows = await this.prisma.message.findMany({
      where,
      orderBy: [{ createdAt: 'desc' }, { id: 'desc' }],
      take: limit + 1,
    });

    const hasMore = rows.length > limit;
    const page = hasMore ? rows.slice(0, limit) : rows;
    const last = page[page.length - 1];
    const nextCursor = hasMore ? `${last.createdAt.toISOString()}|${last.id}` : null;

    return { items: page.reverse(), nextCursor };
  }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npm test -- rooms.service`
Expected: PASS, 9 tests.

- [ ] **Step 5: Commit**

```bash
git add src/rooms/rooms.service.ts src/rooms/rooms.service.spec.ts
git commit -m "feat: add RoomsService.getMessages with cursor pagination"
```

---

### Task 18: RoomsService.getMessages — concurrent insert + tie-break by id (TDD, fix #2)

**Files:**
- Modify: `src/rooms/rooms.service.spec.ts` (implementation from Task 17 already handles both cases — this task is regression-proof verification)

**Interfaces:**
- No new interface — verifies the `(createdAt, id)` compound cursor already implemented is immune to the two failure modes the original system had: (a) a row inserted between two paginated calls must not shift or duplicate results, (b) two messages with an identical `createdAt` must be deterministically ordered by `id`.

- [ ] **Step 1: Write the failing-until-proven test for identical timestamps (tie-break by id)**

Add inside `describe('getMessages', ...)`:

```typescript
    it('tie-breaks by id when two messages share the exact same createdAt', async () => {
      const user = await createUser('tiebreak@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Tied' });
      const sameInstant = new Date();

      const [msgA, msgB] = await Promise.all([
        prisma.message.create({
          data: { roomId: room.id, senderId: user.id, content: 'tied-a', createdAt: sameInstant },
        }),
        prisma.message.create({
          data: { roomId: room.id, senderId: user.id, content: 'tied-b', createdAt: sameInstant },
        }),
      ]);
      const [lower, higher] = [msgA, msgB].sort((a, b) => (a.id < b.id ? -1 : 1));

      const page1 = await roomsService.getMessages(user.id, room.id, undefined, 1);
      expect(page1.items[0].id).toBe(higher.id);
      expect(page1.nextCursor).toBe(`${higher.createdAt.toISOString()}|${higher.id}`);

      const page2 = await roomsService.getMessages(user.id, room.id, page1.nextCursor!, 1);
      expect(page2.items[0].id).toBe(lower.id);
      expect(page2.nextCursor).toBeNull();
    });
```

- [ ] **Step 2: Write the test for an insert happening between two paginated calls**

```typescript
    it('is not affected by a new message inserted between two paginated calls', async () => {
      const user = await createUser('midinsert@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'MidInsert' });
      await seedMessages(room.id, user.id, 4); // message-0..message-3, oldest..newest

      const page1 = await roomsService.getMessages(user.id, room.id, undefined, 2);
      expect(page1.items.map((m) => m.content)).toEqual(['message-2', 'message-3']);

      // simulate a new message arriving after page1 was fetched but before page2
      await prisma.message.create({
        data: {
          roomId: room.id,
          senderId: user.id,
          content: 'message-new',
          createdAt: new Date(Date.now() + 10_000), // newer than everything seeded
        },
      });

      const page2 = await roomsService.getMessages(user.id, room.id, page1.nextCursor!, 2);
      expect(page2.items.map((m) => m.content)).toEqual(['message-0', 'message-1']);
    });
```

- [ ] **Step 3: Run both tests**

Run: `npm test -- rooms.service`
Expected: PASS, 11 tests. If either fails, it means Task 17's `where` clause or ordering is wrong (e.g. using `<=` instead of `<`, or ordering by `createdAt` alone without the `id` tiebreak) — re-check the implementation against Task 17 Step 3 exactly; do not add a mock or weaken the assertion.

- [ ] **Step 4: Commit**

```bash
git add src/rooms/rooms.service.spec.ts
git commit -m "test: verify cursor pagination tie-break and concurrent-insert safety"
```

---

### Task 19: RoomsController + guards (no TDD — scaffold)

**Files:**
- Create: `src/rooms/rooms.controller.ts`, `src/rooms/rooms.module.ts`
- Modify: `src/app.module.ts`

**Interfaces:**
- Consumes: `RoomsService` (Tasks 13-18), `JwtAuthGuard` (Task 12), `CreateRoomDto`/`SendMessageDto`.
- Produces: the five room/message HTTP routes from the spec, all behind `JwtAuthGuard`.

- [ ] **Step 1: Create `src/rooms/rooms.controller.ts`**

```typescript
import { Body, Controller, Get, Param, Post, Query, Req, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
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
  sendMessage(
    @Req() req: { user: { id: string } },
    @Param('roomId') roomId: string,
    @Body() dto: SendMessageDto,
  ) {
    return this.roomsService.sendMessage(req.user.id, roomId, dto);
  }
}
```

- [ ] **Step 2: Create `src/rooms/rooms.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { RoomsService } from './rooms.service';
import { RoomsController } from './rooms.controller';

@Module({
  imports: [PassportModule],
  controllers: [RoomsController],
  providers: [RoomsService],
})
export class RoomsModule {}
```

- [ ] **Step 3: Wire `RoomsModule` into `src/app.module.ts`**

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from './prisma/prisma.module';
import { AuthModule } from './auth/auth.module';
import { RoomsModule } from './rooms/rooms.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule, AuthModule, RoomsModule],
})
export class AppModule {}
```

- [ ] **Step 4: Typecheck and run the full test suite**

Run: `npm run typecheck && npm test`
Expected: both exit 0; 21 total tests pass (10 auth + 11 rooms).

- [ ] **Step 5: Commit**

```bash
git add src/rooms/rooms.controller.ts src/rooms/rooms.module.ts src/app.module.ts
git commit -m "feat: add RoomsController wired behind JwtAuthGuard"
```

---

### Task 20: Manual verification + final check (no TDD — acceptance)

**Files:** none (verification only)

**Interfaces:** none — this task validates the completion criteria from the spec end-to-end.

- [ ] **Step 1: Boot the app**

Run: `npm run start:dev`
Expected: app listening on `http://localhost:3000`, dev Postgres (not test) in use.

- [ ] **Step 2: Register and log in via curl**

```bash
curl -s -X POST http://localhost:3000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"manual@example.com","password":"password123","displayName":"Manual"}'
```

Expected: `201` with `{accessToken, refreshToken}`. Save both values.

- [ ] **Step 3: Verify `/auth/me` requires the access token**

```bash
curl -s http://localhost:3000/auth/me -H "Authorization: Bearer <accessToken>"
```

Expected: `200` with `{id, email: "manual@example.com", displayName: "Manual"}`. Omitting the header returns `401`.

- [ ] **Step 4: Verify refresh rotation and reuse detection manually**

```bash
curl -s -X POST http://localhost:3000/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refreshToken":"<original refreshToken>"}'
```

Expected: `200` with a **new** token pair. Save the new `refreshToken` as `<rotated>`.

Then reuse the original token:

```bash
curl -s -X POST http://localhost:3000/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refreshToken":"<original refreshToken>"}'
```

Expected: `401 {"message":"Session revoked — please log in again", ...}`.

Then confirm the rotated token (issued one step earlier, never itself reused) is now also dead:

```bash
curl -s -X POST http://localhost:3000/auth/refresh \
  -H "Content-Type: application/json" \
  -d '{"refreshToken":"<rotated>"}'
```

Expected: `401` — this is the reuse-detection mass-revoke from Task 9 confirmed against the live dev DB, matching the spec's completion criterion.

- [ ] **Step 5: Verify rooms/messages + membership enforcement**

```bash
# log in again to get a fresh token pair (previous session is fully revoked)
curl -s -X POST http://localhost:3000/auth/login -H "Content-Type: application/json" \
  -d '{"email":"manual@example.com","password":"password123"}'

# create a room
curl -s -X POST http://localhost:3000/rooms -H "Authorization: Bearer <accessToken>" \
  -H "Content-Type: application/json" -d '{"name":"Manual Room"}'

# send a message (use the roomId from the previous response)
curl -s -X POST http://localhost:3000/rooms/<roomId>/messages \
  -H "Authorization: Bearer <accessToken>" -H "Content-Type: application/json" \
  -d '{"content":"hello world"}'

# read messages back
curl -s http://localhost:3000/rooms/<roomId>/messages -H "Authorization: Bearer <accessToken>"
```

Expected: room creation `201`, message send `201`, message list `200` with `{items: [...], nextCursor: null}` containing the sent message. Repeat the message-send/list calls with a second registered user who has NOT joined the room — expect `403 Forbidden`.

- [ ] **Step 6: Full automated check**

Run: `npm run typecheck && npm test`
Expected: `tsc --noEmit` exits 0 with no errors; all Jest specs pass (21 tests: 10 in `auth.service.spec.ts`, 11 in `rooms.service.spec.ts`).

- [ ] **Step 7: Stop the dev server, tear down containers if desired**

Run: `docker compose down` (optional — only if done working for the session; keep volumes to preserve data, add `-v` to wipe).

- [ ] **Step 8: Final commit (if any stray changes remain)**

```bash
git status
```

If clean, nothing to commit — Phase 1 is done. If anything is uncommitted (e.g. a fixed typo found during manual testing), commit it with a message describing the fix.

---

## Completion Criteria (from spec)

- [ ] Register/login/refresh/logout work correctly via curl/Postman (Task 20).
- [ ] Reuse-detection test passes: reusing a revoked refresh token revokes the user's entire session set, confirmed by immediately trying a different valid token for the same user → `401` (Task 9, re-verified manually in Task 20).
- [ ] Room creation, joining, and sending/reading messages work via REST with membership checks enforced (Tasks 13-19, Task 20).
- [ ] Cursor pagination test passes: no duplicate/skipped rows when an insert happens between two `getMessages` calls (Task 18).
- [ ] `npx tsc --noEmit` is clean and all TDD tests pass (Task 20, Step 6).
