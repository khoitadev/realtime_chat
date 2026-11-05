# Phase 4 — auth-service Split (RS256 + JWKS) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract authentication (register/login/refresh/logout, `User` + `RefreshToken`) from the chat monolith into a standalone `auth-service/` with its own database (`auth_db`) that signs RS256 JWTs and publishes its public key via JWKS. The chat-service (`src/`) verifies tokens statelessly from the cached JWKS and no longer owns users.

**Architecture:** `auth-service/` is an independent NestJS project (own `package.json`, Prisma schema, jest config) on port 3010. `KeyService` loads `JWT_PRIVATE_KEY`, derives the public key and a thumbprint `kid`, and serves `GET /.well-known/jwks.json`. `AuthService` (moved from chat) signs RS256 access tokens `{sub, iss, aud}` and keeps refresh-token rotation + reuse detection. In chat-service a `JwksService` fetches/caches the JWKS (TTL, rate-limited refetch on unknown `kid`, stale-on-error, single-flight) and a new `JwtStrategy` verifies tokens through `secretOrKeyProvider`. `POST /ws-ticket` moves into chat-service. Chat's Prisma schema drops `User`/`RefreshToken` and the cross-table FKs; a one-time script copies existing users into `auth_db` first. The Next.js BFF calls auth-service for register/login/refresh and chat-service for everything else.

**Tech Stack:** NestJS 10, Prisma 5, Postgres (real), Jest, `@nestjs/jwt` (RS256), `passport-jwt`, Node 22 `crypto` (JWK export, thumbprints), `pg` (migration script only), existing Redis/Socket.IO stack unchanged.

**Spec:** `docs/superpowers/specs/2026-11-05-phase4-auth-service-design.md`
**Ledger (Rulings):** `docs/superpowers/plans/2026-11-05-phase4-auth-service-ledger.md`

## Global Constraints

- Work on branch `phase-4-auth-service` (created from `master`). Never push to `master`.
- TDD with real I/O: real Postgres test DBs (`chat_test`, `auth_test`), real Redis (db 1), real HTTP/WS clients. Never mock Prisma, Redis, sockets, or the JWT verification path. A tiny **real HTTP server** serving JWKS in chat tests is allowed (it is test infrastructure at the network layer, not a mock of verification logic).
- **Never add a `Co-Authored-By` line (or any Claude/AI attribution) to a git commit message.**
- JWT: RS256 only (`algorithms: ['RS256']` on every verify); header carries `kid`; payload `{ sub, iss: 'auth-service', aud: 'realtime-chat', iat, exp }`; TTL 15 min. Refresh tokens unchanged from Phase 1 (random, SHA-256 hash stored, rotation, reuse detection revokes all sessions of the user).
- JWKS must never contain private members (`d, p, q, dp, dq, qi`).
- chat-service never calls auth-service per request; it only fetches JWKS (TTL 10 min, refetch on unknown `kid` at most once per 10 s, keep stale keys when the fetch fails, chat-service must still boot when auth-service is down).
- chat-service Prisma loses `User`/`RefreshToken` and the FKs from `RoomMember.userId` / `Message.senderId`; those columns stay plain UUID strings.
- Tests run serially (`maxWorkers: 1`) in both projects (shared DBs).
- Run commands from the repo root unless a step says `cd auth-service`.
- Infra: `./scripts/test-infra.sh` (native Postgres + Redis, no Docker here) or Docker via `docker-compose.yml`.
- Out of scope: gateway (Phase 5), internal user-lookup endpoint, automatic key rotation, access-token revocation, per-service Docker images.

---

## File Structure

```
auth-service/                             # NEW independent project
├── package.json  package-lock.json  tsconfig.json  tsconfig.build.json
├── nest-cli.json  jest.config.js
├── prisma/schema.prisma                  # User, RefreshToken -> auth_db (+ migrations/)
├── scripts/migrate-users-from-chat.ts    # CLI wrapper
├── src/
│   ├── main.ts  app.module.ts
│   ├── prisma/{prisma.module.ts,prisma.service.ts}
│   ├── keys/{key.service.ts,key.service.spec.ts,jwks.controller.ts,jwks.controller.spec.ts,keys.module.ts}
│   ├── auth/{auth.service.ts,auth.service.spec.ts,auth.controller.ts,auth.controller.spec.ts,
│   │         jwt.strategy.ts,jwt-auth.guard.ts,auth.module.ts,dto/*}
│   └── migration/{migrate-users.ts,migrate-users.spec.ts}
└── test/{jest.env.ts,db-cleanup.ts}

src/auth/                                 # chat-service: verify-only
├── jwks.service.ts   jwks.service.spec.ts
├── jwt.strategy.ts   jwt-auth.guard.ts   auth.module.ts
└── jwt-auth.spec.ts
src/realtime/ws-ticket.controller.ts      # NEW: POST /ws-ticket
src/realtime/ws-ticket.controller.spec.ts # NEW
src/realtime/realtime.module.ts           # MODIFY
test/jwt.ts                               # NEW: test keys, signToken, JWKS test server
test/instance.ts                          # MODIFY: acquire/release JWKS server
test/db-cleanup.ts                        # MODIFY (Task 7)
prisma/schema.prisma + migrations/<ts>_drop_user_tables_and_fks   # Task 7
scripts/gen-jwt-keys.sh  scripts/test-infra.sh  scripts/smoke-phase4.sh
docker-compose.yml  tsconfig.json  tsconfig.build.json
web/lib/auth.ts  web/app/api/{login,register,ws-ticket}/route.ts  web/lib/nest-fetch.ts
```

---

### Task 1: Scaffold `auth-service/` and its infrastructure

**Files:**
- Create: `auth-service/package.json`, `auth-service/tsconfig.json`, `auth-service/tsconfig.build.json`, `auth-service/nest-cli.json`, `auth-service/jest.config.js`, `auth-service/test/jest.env.ts`, `auth-service/test/db-cleanup.ts`, `auth-service/prisma/schema.prisma`, `auth-service/src/main.ts`, `auth-service/src/app.module.ts`, `auth-service/src/prisma/prisma.module.ts`, `auth-service/src/prisma/prisma.service.ts`, `scripts/gen-jwt-keys.sh`
- Modify: `scripts/test-infra.sh`, `docker-compose.yml`, `tsconfig.json`, `tsconfig.build.json`

**Interfaces:**
- Produces: a compiling, empty-but-bootable `auth-service` project; Prisma models `User { id, email (unique), passwordHash, displayName, createdAt }` and `RefreshToken { id, userId, tokenHash (unique), revokedAt?, expiresAt, createdAt }` (identical columns to chat's, with FK `RefreshToken.userId -> User.id`); databases `auth_dev` / `auth_test`; `auth-service/.env.test` (`DATABASE_URL` only; jest.env generates an ephemeral RSA key when `JWT_PRIVATE_KEY` is unset); `cleanDatabase(prisma)` test helper; `scripts/gen-jwt-keys.sh` printing a single-line `JWT_PRIVATE_KEY="..."` (PEM with literal `\n`).

- [ ] **Step 1: Create `auth-service/package.json`**

```json
{
  "name": "realtime-chat-auth-service",
  "version": "0.1.0",
  "private": true,
  "scripts": {
    "build": "nest build",
    "start": "node dist/main.js",
    "start:dev": "nest start --watch",
    "prisma:generate": "prisma generate",
    "prisma:migrate:dev": "prisma migrate dev",
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
    "@types/pg": "^8.11.0",
    "dotenv": "^16.4.5",
    "dotenv-cli": "^7.4.2",
    "jest": "^29.7.0",
    "pg": "^8.13.0",
    "prisma": "^5.20.0",
    "ts-jest": "^29.2.5",
    "ts-node": "^10.9.2",
    "typescript": "^5.6.2"
  }
}
```

- [ ] **Step 2: Create the project config files**

`auth-service/tsconfig.json`:

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

`auth-service/tsconfig.build.json`:

```json
{
  "extends": "./tsconfig.json",
  "exclude": ["node_modules", "dist", "**/*spec.ts", "test", "scripts"]
}
```

`auth-service/nest-cli.json`:

```json
{
  "$schema": "https://json.schemastore.org/nest-cli",
  "collection": "@nestjs/schematics",
  "sourceRoot": "src"
}
```

`auth-service/jest.config.js`:

```javascript
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  rootDir: '.',
  roots: ['<rootDir>/src'],
  testRegex: '.*\\.spec\\.ts$',
  setupFiles: ['<rootDir>/test/jest.env.ts'],
  testTimeout: 15000,
  // Specs share one real Postgres test database; run files serially.
  maxWorkers: 1,
};
```

`auth-service/test/jest.env.ts`:

```typescript
import { config } from 'dotenv';
import { generateKeyPairSync } from 'crypto';
import { resolve } from 'path';

config({ path: resolve(__dirname, '../.env.test') });

// Tests get an ephemeral RSA key unless one is configured explicitly.
if (!process.env.JWT_PRIVATE_KEY) {
  const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  process.env.JWT_PRIVATE_KEY = privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;
}
```

`auth-service/test/db-cleanup.ts`:

```typescript
import { PrismaService } from '../src/prisma/prisma.service';

export async function cleanDatabase(prisma: PrismaService): Promise<void> {
  await prisma.refreshToken.deleteMany();
  await prisma.user.deleteMany();
}
```

`auth-service/prisma/schema.prisma`:

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
```

- [ ] **Step 3: Create the minimal app**

`auth-service/src/prisma/prisma.service.ts`:

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

`auth-service/src/prisma/prisma.module.ts`:

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

`auth-service/src/app.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { PrismaModule } from './prisma/prisma.module';

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule],
})
export class AppModule {}
```

`auth-service/src/main.ts`:

```typescript
import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.enableShutdownHooks();
  await app.listen(process.env.PORT ?? 3010);
}
bootstrap();
```

- [ ] **Step 4: Create `scripts/gen-jwt-keys.sh`**

```bash
#!/usr/bin/env bash
# Prints a single-line JWT_PRIVATE_KEY (PEM with literal \n) for an .env file.
# Usage: ./scripts/gen-jwt-keys.sh >> auth-service/.env
set -euo pipefail
KEY="$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 2>/dev/null)"
printf 'JWT_PRIVATE_KEY="%s"\n' "$(printf '%s\n' "$KEY" | awk '{printf "%s\\n", $0}')"
```

Run: `chmod +x scripts/gen-jwt-keys.sh && ./scripts/gen-jwt-keys.sh | cut -c1-60`
Expected: a line starting `JWT_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...`.

- [ ] **Step 5: Extend `scripts/test-infra.sh` for the auth databases**

Change the database loop to create four databases and append env-file generation for auth-service. Replace `for db in chat_dev chat_test; do` with `for db in chat_dev chat_test auth_dev auth_test; do`, and add before the final `echo`:

```bash
if [ ! -f "$ROOT/auth-service/.env.test" ]; then
  cat > "$ROOT/auth-service/.env.test" <<ENV
DATABASE_URL="postgresql://postgres:postgres@localhost:${PG_PORT}/auth_test"
ENV
fi

if [ ! -f "$ROOT/auth-service/.env" ]; then
  {
    echo "DATABASE_URL=\"postgresql://postgres:postgres@localhost:${PG_PORT}/auth_dev\""
    "$ROOT/scripts/gen-jwt-keys.sh"
  } > "$ROOT/auth-service/.env"
fi
```

(Both files are git-ignored by the existing `.env*` rules.)

- [ ] **Step 6: Extend `docker-compose.yml` (Docker users)**

Add under `services:`:

```yaml
  postgres-auth-dev:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: auth_dev
    ports:
      - "5435:5432"
    volumes:
      - pgdata_auth_dev:/var/lib/postgresql/data
  postgres-auth-test:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: auth_test
    ports:
      - "5436:5432"
    volumes:
      - pgdata_auth_test:/var/lib/postgresql/data
```

and under `volumes:` add `pgdata_auth_dev:` and `pgdata_auth_test:`. (With Docker, `auth-service/.env.test` must point at port 5436.)

- [ ] **Step 7: Exclude `auth-service` from the chat-service TypeScript projects**

In root `tsconfig.json` change `"exclude": ["node_modules", "dist", "web"]` to `"exclude": ["node_modules", "dist", "web", "auth-service"]`. In root `tsconfig.build.json` change the exclude to `["node_modules", "dist", "web", "auth-service", "**/*spec.ts", "test"]`.

- [ ] **Step 8: Install, create the first migration, verify**

```bash
./scripts/test-infra.sh
cd auth-service
npm install
npx prisma generate
npx prisma migrate dev --name init
npm run prisma:migrate:test
npx tsc --noEmit
npx jest --passWithNoTests
cd ..
npx tsc --noEmit && npm test
```

Expected: infra prints ready; migration `..._init` created under `auth-service/prisma/migrations/` and applied to `auth_dev` and `auth_test`; both `tsc` clean; auth-service jest exits 0 (no tests yet); chat-service suite unchanged and green (74 tests).

- [ ] **Step 9: Commit**

```bash
git add auth-service scripts docker-compose.yml tsconfig.json tsconfig.build.json
git commit -m "chore: scaffold auth-service project with its own Prisma schema and test infra"
```

(Make sure `auth-service/node_modules` and `auth-service/.env*` are not staged.)

---

### Task 2: `KeyService` and the JWKS endpoint (TDD)

**Files:**
- Create: `auth-service/src/keys/key.service.ts`, `auth-service/src/keys/key.service.spec.ts`, `auth-service/src/keys/jwks.controller.ts`, `auth-service/src/keys/jwks.controller.spec.ts`, `auth-service/src/keys/keys.module.ts`

**Interfaces:**
- Produces: `Jwk { kty: string; n: string; e: string; kid: string; use: 'sig'; alg: 'RS256' }`; `KeyService(config: ConfigService)` with `privatePem: string` (pkcs8), `publicPem: string` (spki), `kid: string` (base64url SHA-256 of the RFC 7638 thumbprint input `{"e","kty","n"}`), `getJwks(): { keys: Jwk[] }` (current key first, then any `JWT_PREVIOUS_PUBLIC_KEYS` entries, de-duplicated by `kid`). Reads `JWT_PRIVATE_KEY` (PEM; literal `\n` sequences accepted) and optional `JWT_PREVIOUS_PUBLIC_KEYS` (one or more concatenated PEM public keys). Throws on startup if the private key is missing, malformed, or not RSA. `JwksController`: `GET /.well-known/jwks.json` -> `{ keys }` with `Cache-Control: public, max-age=300`. `KeysModule` exports `KeyService`. Consumed by `AuthModule` (Task 3).

- [ ] **Step 1: Write the failing tests**

`auth-service/src/keys/key.service.spec.ts`:

```typescript
import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { createPublicKey, generateKeyPairSync } from 'crypto';
import { KeyService } from './key.service';

function newPrivatePem(): string {
  const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  return privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;
}

function publicPemOf(privatePem: string): string {
  return createPublicKey(privatePem).export({ type: 'spki', format: 'pem' }) as string;
}

function service(env: Record<string, string | undefined>): KeyService {
  return new KeyService(new ConfigService(env));
}

describe('KeyService', () => {
  it('derives a stable kid from the key (same key => same kid, different key => different kid)', () => {
    const pem = newPrivatePem();
    expect(service({ JWT_PRIVATE_KEY: pem }).kid).toBe(service({ JWT_PRIVATE_KEY: pem }).kid);
    expect(service({ JWT_PRIVATE_KEY: newPrivatePem() }).kid).not.toBe(service({ JWT_PRIVATE_KEY: pem }).kid);
  });

  it('publishes a JWK with kty/n/e/kid/use/alg and no private members', () => {
    const keys = service({ JWT_PRIVATE_KEY: newPrivatePem() });
    const [jwk] = keys.getJwks().keys;

    expect(Object.keys(jwk).sort()).toEqual(['alg', 'e', 'kid', 'kty', 'n', 'use']);
    expect(jwk).toMatchObject({ kty: 'RSA', use: 'sig', alg: 'RS256', kid: keys.kid });
    for (const secret of ['d', 'p', 'q', 'dp', 'dq', 'qi']) {
      expect(jwk).not.toHaveProperty(secret);
    }
  });

  it('produces a public key that verifies tokens signed with the private key', async () => {
    const keys = service({ JWT_PRIVATE_KEY: newPrivatePem() });
    const jwt = new JwtService();
    const token = jwt.sign(
      { sub: 'u1' },
      { privateKey: keys.privatePem, algorithm: 'RS256', keyid: keys.kid },
    );

    const [jwk] = keys.getJwks().keys;
    const pem = createPublicKey({ key: jwk, format: 'jwk' }).export({
      type: 'spki',
      format: 'pem',
    }) as string;

    expect((await jwt.verifyAsync(token, { publicKey: pem, algorithms: ['RS256'] })).sub).toBe('u1');
  });

  it('accepts a single-line PEM with literal \\n sequences (as written in .env files)', () => {
    const pem = newPrivatePem();
    const escaped = pem.trim().replace(/\n/g, '\\n');

    expect(service({ JWT_PRIVATE_KEY: escaped }).kid).toBe(service({ JWT_PRIVATE_KEY: pem }).kid);
  });

  it('keeps previous public keys in the JWKS after the current one, without duplicates', () => {
    const currentPem = newPrivatePem();
    const oldPem = newPrivatePem();
    const previous = [publicPemOf(oldPem), publicPemOf(currentPem)].join('\n');

    const keys = service({ JWT_PRIVATE_KEY: currentPem, JWT_PREVIOUS_PUBLIC_KEYS: previous });
    const published = keys.getJwks().keys;

    expect(published).toHaveLength(2);
    expect(published[0].kid).toBe(keys.kid);
    expect(published[1].kid).toBe(service({ JWT_PRIVATE_KEY: oldPem }).kid);
  });

  it('fails fast when the private key is missing', () => {
    expect(() => service({})).toThrow(/JWT_PRIVATE_KEY/);
  });

  it('fails fast when the private key is not valid PEM', () => {
    expect(() => service({ JWT_PRIVATE_KEY: 'not a key' })).toThrow();
  });

  it('rejects non-RSA keys', () => {
    const { privateKey } = generateKeyPairSync('ed25519');
    const pem = privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;

    expect(() => service({ JWT_PRIVATE_KEY: pem })).toThrow(/RSA/);
  });
});
```

`auth-service/src/keys/jwks.controller.spec.ts`:

```typescript
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { AddressInfo } from 'net';
import { createPublicKey } from 'crypto';
import { JwtService } from '@nestjs/jwt';
import { JwksController } from './jwks.controller';
import { KeyService } from './key.service';

describe('GET /.well-known/jwks.json', () => {
  let app: INestApplication;
  let baseUrl: string;
  let keys: KeyService;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      controllers: [JwksController],
      providers: [
        KeyService,
        {
          provide: ConfigService,
          useValue: new ConfigService({ JWT_PRIVATE_KEY: process.env.JWT_PRIVATE_KEY }),
        },
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
    keys = moduleRef.get(KeyService);
  });

  afterAll(async () => {
    await app.close();
  });

  it('serves the JWKS with a public cache header', async () => {
    const res = await fetch(`${baseUrl}/.well-known/jwks.json`);

    expect(res.status).toBe(200);
    expect(res.headers.get('cache-control')).toBe('public, max-age=300');
    const body = await res.json();
    expect(body.keys).toHaveLength(1);
    expect(body.keys[0].kid).toBe(keys.kid);
  });

  it('publishes a key that verifies a token signed by this service', async () => {
    const jwt = new JwtService();
    const token = jwt.sign(
      { sub: 'u1' },
      { privateKey: keys.privatePem, algorithm: 'RS256', keyid: keys.kid },
    );

    const { keys: published } = await (await fetch(`${baseUrl}/.well-known/jwks.json`)).json();
    const pem = createPublicKey({ key: published[0], format: 'jwk' }).export({
      type: 'spki',
      format: 'pem',
    }) as string;

    expect((await jwt.verifyAsync(token, { publicKey: pem, algorithms: ['RS256'] })).sub).toBe('u1');
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd auth-service && npx jest src/keys`
Expected: FAIL — `Cannot find module './key.service'` / `'./jwks.controller'`.

- [ ] **Step 3: Implement**

`auth-service/src/keys/key.service.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHash, createPrivateKey, createPublicKey, KeyObject } from 'crypto';

export interface Jwk {
  kty: string;
  n: string;
  e: string;
  kid: string;
  use: 'sig';
  alg: 'RS256';
}

/** RFC 7638-style thumbprint of an RSA public key => stable `kid`. */
function toJwk(publicKey: KeyObject): Jwk {
  const { kty, n, e } = publicKey.export({ format: 'jwk' }) as {
    kty: string;
    n: string;
    e: string;
  };
  const kid = createHash('sha256').update(JSON.stringify({ e, kty, n })).digest('base64url');
  return { kty, n, e, kid, use: 'sig', alg: 'RS256' };
}

@Injectable()
export class KeyService {
  readonly privatePem: string;
  readonly publicPem: string;
  readonly kid: string;
  private readonly jwks: Jwk[];

  constructor(config: ConfigService) {
    const raw = config.get<string>('JWT_PRIVATE_KEY');
    if (!raw) {
      throw new Error('JWT_PRIVATE_KEY is required');
    }

    // .env files commonly store PEM on one line with literal \n sequences.
    const privateKey = createPrivateKey(raw.replace(/\\n/g, '\n'));
    if (privateKey.asymmetricKeyType !== 'rsa') {
      throw new Error('JWT_PRIVATE_KEY must be an RSA key (RS256)');
    }

    const publicKey = createPublicKey(privateKey);
    this.privatePem = privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;
    this.publicPem = publicKey.export({ type: 'spki', format: 'pem' }) as string;

    const current = toJwk(publicKey);
    this.kid = current.kid;

    const previous = (config.get<string>('JWT_PREVIOUS_PUBLIC_KEYS') ?? '')
      .replace(/\\n/g, '\n')
      .split(/(?=-----BEGIN PUBLIC KEY-----)/)
      .map((pem) => pem.trim())
      .filter(Boolean)
      .map((pem) => toJwk(createPublicKey(pem)))
      .filter((jwk) => jwk.kid !== current.kid);

    const seen = new Set<string>();
    this.jwks = [current, ...previous].filter((jwk) => !seen.has(jwk.kid) && !!seen.add(jwk.kid));
  }

  getJwks(): { keys: Jwk[] } {
    return { keys: this.jwks };
  }
}
```

`auth-service/src/keys/jwks.controller.ts`:

```typescript
import { Controller, Get, Res } from '@nestjs/common';
import { Response } from 'express';
import { KeyService } from './key.service';

@Controller('.well-known')
export class JwksController {
  constructor(private readonly keys: KeyService) {}

  @Get('jwks.json')
  jwks(@Res({ passthrough: true }) res: Response) {
    res.setHeader('Cache-Control', 'public, max-age=300');
    return this.keys.getJwks();
  }
}
```

`auth-service/src/keys/keys.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { JwksController } from './jwks.controller';
import { KeyService } from './key.service';

@Module({
  imports: [ConfigModule],
  controllers: [JwksController],
  providers: [KeyService],
  exports: [KeyService],
})
export class KeysModule {}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd auth-service && npx jest src/keys && npx tsc --noEmit`
Expected: PASS — 8 `KeyService` tests + 2 controller tests; tsc clean.

- [ ] **Step 5: Commit**

```bash
git add auth-service/src/keys
git commit -m "feat(auth-service): add KeyService and JWKS endpoint"
```

---

### Task 3: Move auth into `auth-service` with RS256 (TDD)

**Files:**
- Create: `auth-service/src/auth/{auth.service.ts, auth.controller.ts, jwt.strategy.ts, jwt-auth.guard.ts, auth.module.ts, dto/login.dto.ts, dto/register.dto.ts, dto/refresh.dto.ts}`, `auth-service/src/auth/auth.service.spec.ts`, `auth-service/src/auth/auth.controller.spec.ts`
- Modify: `auth-service/src/app.module.ts`

**Interfaces:**
- Produces: `AuthService.register/login/refresh/logout` with the same contracts and error messages as Phase 1 (`Email already registered` 409, `Invalid credentials` 401, `Invalid refresh token` 401, `Session revoked — please log in again` 401, `Refresh token expired` 401) but access tokens signed RS256 with header `kid` and payload `{ sub, iss, aud }` (`JWT_ISSUER` default `auth-service`, `JWT_AUDIENCE` default `realtime-chat`, TTL 15m); HTTP `POST /auth/register|login|refresh|logout`, `GET /auth/me` (guarded by `JwtAuthGuard` verifying with this service's own public key). `AppModule` imports `AuthModule`. The chat-service copy of these files stays untouched until Task 6.

- [ ] **Step 1: Copy the Phase 1 sources and tests, then adapt**

```bash
mkdir -p auth-service/src/auth/dto
cp src/auth/dto/*.ts auth-service/src/auth/dto/
cp src/auth/auth.service.ts src/auth/auth.service.spec.ts src/auth/jwt-auth.guard.ts auth-service/src/auth/
```

(`auth.service.ts` and `jwt-auth.guard.ts` need no edits: `JwtService.signAsync({ sub })` picks the RS256 options from the module configuration below. `auth.service.spec.ts` needs only the module-setup change in Step 2; its imports of `../prisma/prisma.service` and `../../test/db-cleanup` already resolve inside `auth-service`.)

- [ ] **Step 2: Rewrite the module setup in `auth-service/src/auth/auth.service.spec.ts`**

Replace the imports and `beforeAll` block (everything from the top of the file through the end of `beforeAll`) with:

```typescript
import { Test } from '@nestjs/testing';
import { ConfigModule } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { ConflictException, UnauthorizedException } from '@nestjs/common';
import { createHash, createPublicKey, generateKeyPairSync } from 'crypto';
import { AuthService } from './auth.service';
import { AuthModule } from './auth.module';
import { KeyService } from '../keys/key.service';
import { PrismaModule } from '../prisma/prisma.module';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

describe('AuthService', () => {
  let authService: AuthService;
  let prisma: PrismaService;
  let keys: KeyService;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule, AuthModule],
    }).compile();

    authService = moduleRef.get(AuthService);
    prisma = moduleRef.get(PrismaService);
    keys = moduleRef.get(KeyService);
    await prisma.$connect();
  });
```

(Keep the existing `afterAll`, `beforeEach` and every existing `describe`.) Then add this new block as a sibling `describe` inside the outer `describe('AuthService', ...)`, right after the `beforeEach`:

```typescript
  describe('access token', () => {
    it('is an RS256 JWT with kid, iss, aud and sub, verifiable with the published JWK', async () => {
      const { accessToken } = await authService.register({
        email: 'rs256@example.com',
        password: 'password123',
        displayName: 'Rs256',
      });
      const user = await prisma.user.findUniqueOrThrow({ where: { email: 'rs256@example.com' } });

      const header = JSON.parse(Buffer.from(accessToken.split('.')[0], 'base64url').toString());
      expect(header).toMatchObject({ alg: 'RS256', kid: keys.kid });

      const [jwk] = keys.getJwks().keys;
      const pem = createPublicKey({ key: jwk, format: 'jwk' }).export({
        type: 'spki',
        format: 'pem',
      }) as string;
      const payload = await new JwtService().verifyAsync(accessToken, {
        publicKey: pem,
        algorithms: ['RS256'],
        issuer: 'auth-service',
        audience: 'realtime-chat',
      });
      expect(payload.sub).toBe(user.id);
    });

    it('cannot be verified with a different key', async () => {
      const { accessToken } = await authService.register({
        email: 'otherkey@example.com',
        password: 'password123',
        displayName: 'OtherKey',
      });
      const { publicKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
      const wrongPem = publicKey.export({ type: 'spki', format: 'pem' }) as string;

      await expect(
        new JwtService().verifyAsync(accessToken, { publicKey: wrongPem, algorithms: ['RS256'] }),
      ).rejects.toThrow();
    });
  });
```

(The `createHash` import is still used by the existing refresh tests.)

- [ ] **Step 3: Run the spec to verify it fails**

Run: `cd auth-service && npx jest src/auth/auth.service`
Expected: FAIL — `Cannot find module './auth.module'` (and other missing files).

- [ ] **Step 4: Implement the module, controller, strategy**

`auth-service/src/auth/jwt.strategy.ts`:

```typescript
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';
import { KeyService } from '../keys/key.service';

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(keys: KeyService, config: ConfigService) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      algorithms: ['RS256'],
      secretOrKey: keys.publicPem,
      issuer: config.get<string>('JWT_ISSUER', 'auth-service'),
      audience: config.get<string>('JWT_AUDIENCE', 'realtime-chat'),
    });
  }

  validate(payload: { sub: string }) {
    return { id: payload.sub };
  }
}
```

`auth-service/src/auth/auth.controller.ts`:

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

`auth-service/src/auth/auth.module.ts`:

```typescript
import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { PassportModule } from '@nestjs/passport';
import { AuthService } from './auth.service';
import { AuthController } from './auth.controller';
import { JwtStrategy } from './jwt.strategy';
import { KeysModule } from '../keys/keys.module';
import { KeyService } from '../keys/key.service';

@Module({
  imports: [
    ConfigModule,
    PassportModule,
    KeysModule,
    JwtModule.registerAsync({
      imports: [ConfigModule, KeysModule],
      inject: [ConfigService, KeyService],
      useFactory: (config: ConfigService, keys: KeyService) => ({
        privateKey: keys.privatePem,
        publicKey: keys.publicPem,
        signOptions: {
          algorithm: 'RS256' as const,
          keyid: keys.kid,
          issuer: config.get<string>('JWT_ISSUER', 'auth-service'),
          audience: config.get<string>('JWT_AUDIENCE', 'realtime-chat'),
          expiresIn: '15m',
        },
      }),
    }),
  ],
  controllers: [AuthController],
  providers: [AuthService, JwtStrategy],
  exports: [AuthService, KeysModule],
})
export class AuthModule {}
```

Update `auth-service/src/app.module.ts`:

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

(`moduleRef.get(KeyService)` in the spec resolves because `KeysModule` is re-exported from `AuthModule`.)

- [ ] **Step 5: Write the HTTP-level controller spec**

`auth-service/src/auth/auth.controller.spec.ts`:

```typescript
import { Test } from '@nestjs/testing';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AddressInfo } from 'net';
import { AuthModule } from './auth.module';
import { PrismaModule } from '../prisma/prisma.module';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

describe('AuthController (HTTP)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let baseUrl: string;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule, AuthModule],
    }).compile();

    app = moduleRef.createNestApplication();
    app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
    prisma = moduleRef.get(PrismaService);
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  function post(path: string, body: unknown) {
    return fetch(`${baseUrl}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }

  const alice = { email: 'alice@example.com', password: 'password123', displayName: 'Alice' };

  it('registers and returns a token pair (201)', async () => {
    const res = await post('/auth/register', alice);

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(typeof body.accessToken).toBe('string');
    expect(typeof body.refreshToken).toBe('string');
  });

  it('rejects an invalid payload with 400 and a duplicate email with 409', async () => {
    expect((await post('/auth/register', { email: 'nope' })).status).toBe(400);

    await post('/auth/register', alice);
    expect((await post('/auth/register', alice)).status).toBe(409);
  });

  it('logs in with valid credentials and rejects bad ones with 401', async () => {
    await post('/auth/register', alice);

    expect((await post('/auth/login', { email: alice.email, password: alice.password })).status).toBe(201);
    expect((await post('/auth/login', { email: alice.email, password: 'wrongpassword' })).status).toBe(401);
  });

  it('rotates the refresh token and treats reuse of the old one as theft (401, all sessions revoked)', async () => {
    const first = await (await post('/auth/register', alice)).json();
    const rotated = await (await post('/auth/refresh', { refreshToken: first.refreshToken })).json();
    expect(rotated.refreshToken).not.toBe(first.refreshToken);

    const reuse = await post('/auth/refresh', { refreshToken: first.refreshToken });
    expect(reuse.status).toBe(401);

    const afterTheft = await post('/auth/refresh', { refreshToken: rotated.refreshToken });
    expect(afterTheft.status).toBe(401);
  });

  it('logs out by revoking the refresh token', async () => {
    const { refreshToken } = await (await post('/auth/register', alice)).json();

    expect((await post('/auth/logout', { refreshToken })).status).toBe(200);
    expect((await post('/auth/refresh', { refreshToken })).status).toBe(401);
  });

  it('GET /auth/me requires a valid token and returns the profile', async () => {
    expect((await fetch(`${baseUrl}/auth/me`)).status).toBe(401);

    const { accessToken } = await (await post('/auth/register', alice)).json();
    const res = await fetch(`${baseUrl}/auth/me`, {
      headers: { Authorization: `Bearer ${accessToken}` },
    });

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ email: alice.email, displayName: 'Alice' });
  });

  it('serves a JWKS whose key verifies the issued access token header kid', async () => {
    const { accessToken } = await (await post('/auth/register', alice)).json();
    const { kid } = JSON.parse(Buffer.from(accessToken.split('.')[0], 'base64url').toString());

    const jwks = await (await fetch(`${baseUrl}/.well-known/jwks.json`)).json();

    expect(jwks.keys.map((k: { kid: string }) => k.kid)).toContain(kid);
  });
});
```

- [ ] **Step 6: Run the whole auth-service suite**

Run: `cd auth-service && npx jest && npx tsc --noEmit`
Expected: PASS — all key tests, all migrated `AuthService` tests plus the 2 new access-token tests, 7 controller tests; tsc clean. If a migrated test fails only because of the token format change, fix the test minimally and record a Ruling; do not weaken the refresh-token reuse tests.

- [ ] **Step 7: Commit**

```bash
git add auth-service/src
git commit -m "feat(auth-service): move auth to the new service and sign RS256 access tokens"
```

---

### Task 4: User migration script (TDD)

**Files:**
- Create: `auth-service/src/migration/migrate-users.ts`, `auth-service/src/migration/migrate-users.spec.ts`, `auth-service/scripts/migrate-users-from-chat.ts`

**Interfaces:**
- Produces: `migrateUsers(sourceUrl: string, target: PrismaClient): Promise<{ users: number; refreshTokens: number }>` — reads every row of `"User"` and `"RefreshToken"` from the legacy chat database with `pg`, inserts them into `auth_db` through Prisma (`createMany` + `skipDuplicates`, ids preserved) inside a transaction, then reconciles (every source id must exist in the target) and throws if not. CLI: `SOURCE_DATABASE_URL=<chat db> DATABASE_URL=<auth db> npx ts-node scripts/migrate-users-from-chat.ts`.

- [ ] **Step 1: Write the failing test**

`auth-service/src/migration/migrate-users.spec.ts`:

```typescript
import { Client } from 'pg';
import { randomUUID } from 'crypto';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';
import { migrateUsers } from './migrate-users';

// DDL copied from the chat-service init migration (the legacy schema being migrated away from).
const LEGACY_DDL = `
CREATE TABLE "User" (
  "id" TEXT NOT NULL,
  "email" TEXT NOT NULL,
  "passwordHash" TEXT NOT NULL,
  "displayName" TEXT NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "User_pkey" PRIMARY KEY ("id")
);
CREATE TABLE "RefreshToken" (
  "id" TEXT NOT NULL,
  "userId" TEXT NOT NULL,
  "tokenHash" TEXT NOT NULL,
  "revokedAt" TIMESTAMP(3),
  "expiresAt" TIMESTAMP(3) NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "RefreshToken_pkey" PRIMARY KEY ("id")
);`;

describe('migrateUsers (legacy chat DB -> auth_db)', () => {
  const legacyName = `legacy_chat_${randomUUID().replace(/-/g, '').slice(0, 12)}`;
  let admin: Client;
  let legacyUrl: string;
  let prisma: PrismaService;

  function urlFor(database: string): string {
    const url = new URL(process.env.DATABASE_URL!);
    url.pathname = `/${database}`;
    return url.toString();
  }

  beforeAll(async () => {
    admin = new Client({ connectionString: urlFor('postgres') });
    await admin.connect();
    await admin.query(`CREATE DATABASE "${legacyName}"`);
    legacyUrl = urlFor(legacyName);

    prisma = new PrismaService();
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await admin.query(`DROP DATABASE IF EXISTS "${legacyName}" WITH (FORCE)`);
    await admin.end();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
    const legacy = new Client({ connectionString: legacyUrl });
    await legacy.connect();
    await legacy.query('DROP TABLE IF EXISTS "RefreshToken"; DROP TABLE IF EXISTS "User";');
    await legacy.query(LEGACY_DDL);
    await legacy.end();
  });

  async function seedLegacy(users: number, tokensPerUser: number) {
    const legacy = new Client({ connectionString: legacyUrl });
    await legacy.connect();
    const ids: string[] = [];
    for (let i = 0; i < users; i++) {
      const id = randomUUID();
      ids.push(id);
      await legacy.query(
        'INSERT INTO "User" ("id","email","passwordHash","displayName") VALUES ($1,$2,$3,$4)',
        [id, `u${i}-${id}@example.com`, `hash-${i}`, `User ${i}`],
      );
      for (let t = 0; t < tokensPerUser; t++) {
        await legacy.query(
          'INSERT INTO "RefreshToken" ("id","userId","tokenHash","revokedAt","expiresAt") VALUES ($1,$2,$3,$4,$5)',
          [randomUUID(), id, `th-${id}-${t}`, t === 0 ? new Date('2026-01-01T00:00:00Z') : null, new Date('2027-01-01T00:00:00Z')],
        );
      }
    }
    await legacy.end();
    return ids;
  }

  it('copies users and refresh tokens preserving ids and fields', async () => {
    const ids = await seedLegacy(3, 2);

    const result = await migrateUsers(legacyUrl, prisma);

    expect(result).toEqual({ users: 3, refreshTokens: 6 });
    const users = await prisma.user.findMany({ orderBy: { email: 'asc' } });
    expect(users.map((u) => u.id).sort()).toEqual([...ids].sort());
    const tokens = await prisma.refreshToken.findMany();
    expect(tokens).toHaveLength(6);
    expect(tokens.filter((t) => t.revokedAt !== null)).toHaveLength(3); // revoked state preserved
    expect(users[0].passwordHash).toMatch(/^hash-/);
  });

  it('is idempotent: running twice does not duplicate rows', async () => {
    await seedLegacy(2, 1);

    await migrateUsers(legacyUrl, prisma);
    await migrateUsers(legacyUrl, prisma);

    expect(await prisma.user.count()).toBe(2);
    expect(await prisma.refreshToken.count()).toBe(2);
  });

  it('handles an empty legacy database', async () => {
    expect(await migrateUsers(legacyUrl, prisma)).toEqual({ users: 0, refreshTokens: 0 });
  });

  it('fails reconciliation (and writes nothing) when a source row cannot be inserted', async () => {
    await seedLegacy(1, 0);
    // Same email already owned by a different id in the target: createMany skips it silently.
    const legacy = new Client({ connectionString: legacyUrl });
    await legacy.connect();
    const { rows } = await legacy.query('SELECT "email" FROM "User" LIMIT 1');
    await legacy.end();
    await prisma.user.create({
      data: { id: randomUUID(), email: rows[0].email, passwordHash: 'x', displayName: 'squatter' },
    });

    await expect(migrateUsers(legacyUrl, prisma)).rejects.toThrow(/reconciliation/i);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd auth-service && npx jest src/migration`
Expected: FAIL — `Cannot find module './migrate-users'`.

- [ ] **Step 3: Implement**

`auth-service/src/migration/migrate-users.ts`:

```typescript
import { PrismaClient } from '@prisma/client';
import { Client } from 'pg';

export interface MigrationResult {
  users: number;
  refreshTokens: number;
}

/**
 * One-time copy of User + RefreshToken from the legacy chat database into auth_db.
 * Ids are preserved; re-running is safe (skipDuplicates). Throws if any source row
 * is missing from the target afterwards (e.g. an email collision).
 */
export async function migrateUsers(
  sourceUrl: string,
  target: PrismaClient,
): Promise<MigrationResult> {
  const source = new Client({ connectionString: sourceUrl });
  await source.connect();
  try {
    const users = (
      await source.query(
        'SELECT "id","email","passwordHash","displayName","createdAt" FROM "User"',
      )
    ).rows as Array<{ id: string; email: string; passwordHash: string; displayName: string; createdAt: Date }>;
    const tokens = (
      await source.query(
        'SELECT "id","userId","tokenHash","revokedAt","expiresAt","createdAt" FROM "RefreshToken"',
      )
    ).rows as Array<{
      id: string;
      userId: string;
      tokenHash: string;
      revokedAt: Date | null;
      expiresAt: Date;
      createdAt: Date;
    }>;

    await target.$transaction([
      target.user.createMany({ data: users, skipDuplicates: true }),
      target.refreshToken.createMany({ data: tokens, skipDuplicates: true }),
    ]);

    const migratedUsers = await target.user.count({ where: { id: { in: users.map((u) => u.id) } } });
    const migratedTokens = await target.refreshToken.count({
      where: { id: { in: tokens.map((t) => t.id) } },
    });
    if (migratedUsers !== users.length || migratedTokens !== tokens.length) {
      throw new Error(
        `Reconciliation failed: users ${migratedUsers}/${users.length}, ` +
          `refresh tokens ${migratedTokens}/${tokens.length}`,
      );
    }

    return { users: users.length, refreshTokens: tokens.length };
  } finally {
    await source.end();
  }
}
```

`auth-service/scripts/migrate-users-from-chat.ts`:

```typescript
import { PrismaClient } from '@prisma/client';
import { migrateUsers } from '../src/migration/migrate-users';

async function main() {
  const sourceUrl = process.env.SOURCE_DATABASE_URL;
  if (!sourceUrl || !process.env.DATABASE_URL) {
    throw new Error('Set SOURCE_DATABASE_URL (legacy chat DB) and DATABASE_URL (auth DB)');
  }
  const prisma = new PrismaClient();
  try {
    const result = await migrateUsers(sourceUrl, prisma);
    console.log(`Migrated ${result.users} users and ${result.refreshTokens} refresh tokens`);
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd auth-service && npx jest src/migration && npx tsc --noEmit`
Expected: PASS — 4 tests; tsc clean. Note: the "reconciliation" test relies on `createMany` + `skipDuplicates` skipping the email collision; the transaction itself still commits the other rows, which is acceptable because the run exits non-zero and is re-runnable after the collision is resolved (record this as a Ruling if you change the semantics).

- [ ] **Step 5: Commit**

```bash
git add auth-service/src/migration auth-service/scripts
git commit -m "feat(auth-service): add one-time user migration from the legacy chat database"
```

---

### Task 5: chat-service JWT test helpers and `JwksService` (TDD)

**Files:**
- Create: `test/jwt.ts`, `src/auth/jwks.service.ts`, `src/auth/jwks.service.spec.ts`

**Interfaces:**
- Produces (test helpers, `test/jwt.ts`): `TestKey { kid; privatePem; publicPem; jwk }`; `generateTestKey()`; `defaultKey`; `signToken(userId, opts?: { key?; kid?; issuer?; audience?; expiresInSec? })` (RS256, defaults iss `auth-service`, aud `realtime-chat`, 900 s); `signHs256WithPublicKey(userId, key?)`; `unsignedToken(userId)`; `startJwksServer(keys?)` -> `JwksServer { url; hits(): number; setKeys(keys); setDown(bool); close() }` (a real `http` server serving `/.well-known/jwks.json`, 503 when down); `acquireJwksServer()` / `releaseJwksServer()` (ref-counted shared server serving `defaultKey`; `acquire` also sets `process.env.AUTH_JWKS_URL`).
- Produces (production): `JWKS_OPTIONS` token; `JwksOptions { url: string; ttlMs: number; minRefetchMs: number }`; `JwksService.getPublicKeyPem(kid: string | undefined): Promise<string>` — returns the cached SPKI PEM; refetches the JWKS when the cache is older than `ttlMs`, or when the `kid` is unknown and the previous attempt was at least `minRefetchMs` ago; concurrent callers share one in-flight fetch; a failed fetch keeps previously cached keys (stale-on-error); throws `UnauthorizedException` for a missing/unknown `kid`.

- [ ] **Step 1: Create `test/jwt.ts`**

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

/** Classic algorithm-confusion forgery: HS256 signed with the public key as the secret. */
export function signHs256WithPublicKey(userId: string, key: TestKey = defaultKey): string {
  return new JwtService().sign(
    { sub: userId },
    {
      secret: key.publicPem,
      algorithm: 'HS256',
      keyid: key.kid,
      issuer: 'auth-service',
      audience: 'realtime-chat',
      expiresIn: 900,
    },
  );
}

export function unsignedToken(userId: string): string {
  const enc = (o: unknown) => Buffer.from(JSON.stringify(o)).toString('base64url');
  const header = enc({ alg: 'none', typ: 'JWT', kid: defaultKey.kid });
  const payload = enc({
    sub: userId,
    iss: 'auth-service',
    aud: 'realtime-chat',
    exp: Math.floor(Date.now() / 1000) + 900,
  });
  return `${header}.${payload}.`;
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

- [ ] **Step 2: Write the failing test**

`src/auth/jwks.service.spec.ts`:

```typescript
import { UnauthorizedException } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { JwksService } from './jwks.service';
import { defaultKey, generateTestKey, JwksServer, signToken, startJwksServer } from '../../test/jwt';
import { sleep } from '../../test/helpers';

describe('JwksService', () => {
  let server: JwksServer;

  beforeEach(async () => {
    server = await startJwksServer();
  });

  afterEach(async () => {
    await server.close();
  });

  function service(overrides: Partial<{ ttlMs: number; minRefetchMs: number }> = {}) {
    return new JwksService({
      url: server.url,
      ttlMs: overrides.ttlMs ?? 60_000,
      minRefetchMs: overrides.minRefetchMs ?? 300,
    });
  }

  it('returns a PEM that verifies tokens signed by the published key', async () => {
    const pem = await service().getPublicKeyPem(defaultKey.kid);

    const payload = await new JwtService().verifyAsync(signToken('user-1'), {
      publicKey: pem,
      algorithms: ['RS256'],
    });
    expect(payload.sub).toBe('user-1');
  });

  it('caches keys: repeated lookups do not hit the server again', async () => {
    const jwks = service();

    await jwks.getPublicKeyPem(defaultKey.kid);
    await jwks.getPublicKeyPem(defaultKey.kid);
    await jwks.getPublicKeyPem(defaultKey.kid);

    expect(server.hits()).toBe(1);
  });

  it('shares one in-flight fetch between concurrent callers', async () => {
    const jwks = service();

    await Promise.all(Array.from({ length: 5 }, () => jwks.getPublicKeyPem(defaultKey.kid)));

    expect(server.hits()).toBe(1);
  });

  it('rejects a missing kid without calling the server', async () => {
    await expect(service().getPublicKeyPem(undefined)).rejects.toThrow(UnauthorizedException);
    expect(server.hits()).toBe(0);
  });

  it('rate-limits refetches triggered by unknown kids (at most one per minRefetchMs)', async () => {
    const jwks = service({ minRefetchMs: 400 });
    await jwks.getPublicKeyPem(defaultKey.kid); // hit #1

    await expect(jwks.getPublicKeyPem('unknown-1')).rejects.toThrow(UnauthorizedException);
    await expect(jwks.getPublicKeyPem('unknown-2')).rejects.toThrow(UnauthorizedException);
    expect(server.hits()).toBe(1); // still inside the window: no refetch

    await sleep(450);
    await expect(jwks.getPublicKeyPem('unknown-3')).rejects.toThrow(UnauthorizedException);
    expect(server.hits()).toBe(2); // window elapsed: exactly one refetch
  });

  it('picks up a rotated key after the rate-limit window', async () => {
    const jwks = service({ minRefetchMs: 300 });
    await jwks.getPublicKeyPem(defaultKey.kid);

    const rotated = generateTestKey();
    server.setKeys([rotated, defaultKey]);
    await sleep(350);

    const pem = await jwks.getPublicKeyPem(rotated.kid);
    const payload = await new JwtService().verifyAsync(signToken('user-2', { key: rotated }), {
      publicKey: pem,
      algorithms: ['RS256'],
    });
    expect(payload.sub).toBe('user-2');
  });

  it('refreshes after the TTL and drops keys that are no longer published', async () => {
    const jwks = service({ ttlMs: 250, minRefetchMs: 100 });
    await jwks.getPublicKeyPem(defaultKey.kid);

    const replacement = generateTestKey();
    server.setKeys([replacement]);
    await sleep(300);

    await jwks.getPublicKeyPem(replacement.kid);
    await expect(jwks.getPublicKeyPem(defaultKey.kid)).rejects.toThrow(UnauthorizedException);
  });

  it('keeps serving cached keys when the JWKS endpoint goes down (stale-on-error)', async () => {
    const jwks = service({ ttlMs: 200, minRefetchMs: 100 });
    await jwks.getPublicKeyPem(defaultKey.kid);

    server.setDown(true);
    await sleep(250); // cache is now stale and a refresh will fail

    await expect(jwks.getPublicKeyPem(defaultKey.kid)).resolves.toContain('BEGIN PUBLIC KEY');
  });

  it('rejects (instead of crashing) when the endpoint is down and nothing is cached', async () => {
    server.setDown(true);

    await expect(service().getPublicKeyPem(defaultKey.kid)).rejects.toThrow(UnauthorizedException);
  });

  it('recovers once the endpoint comes back', async () => {
    server.setDown(true);
    const jwks = service({ minRefetchMs: 200 });
    await expect(jwks.getPublicKeyPem(defaultKey.kid)).rejects.toThrow(UnauthorizedException);

    server.setDown(false);
    await sleep(250);

    await expect(jwks.getPublicKeyPem(defaultKey.kid)).resolves.toContain('BEGIN PUBLIC KEY');
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npm test -- jwks.service`
Expected: FAIL — `Cannot find module './jwks.service'`.

- [ ] **Step 4: Implement `JwksService`**

`src/auth/jwks.service.ts`:

```typescript
import { Inject, Injectable, Logger, UnauthorizedException } from '@nestjs/common';
import { createPublicKey } from 'crypto';

export const JWKS_OPTIONS = Symbol('JWKS_OPTIONS');

export interface JwksOptions {
  url: string;
  /** After this long the cached key set is considered stale and is refreshed. */
  ttlMs: number;
  /** Minimum gap between two fetch attempts (protects auth-service from unknown-kid floods). */
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
  private keys = new Map<string, string>(); // kid -> SPKI PEM
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

    // After a failed refresh the previous key set is still in place (stale-on-error).
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

- [ ] **Step 5: Run test to verify it passes**

Run: `npm test -- jwks.service && npx tsc --noEmit`
Expected: PASS, 10 tests; tsc clean. (If `createPublicKey({ key: jwk as never ... })` fails to type-check, use a `JsonWebKey` cast from `crypto` and record a Ruling.)

- [ ] **Step 6: Commit**

```bash
git add test/jwt.ts src/auth/jwks.service.ts src/auth/jwks.service.spec.ts
git commit -m "feat: add JwksService (cached, rate-limited, stale-on-error) and JWT test helpers"
```

---

### Task 6: chat-service cut-over to JWKS verification and `POST /ws-ticket` (TDD)

**Files:**
- Create: `src/realtime/ws-ticket.controller.ts`, `src/realtime/ws-ticket.controller.spec.ts`, `src/auth/jwt-auth.spec.ts`
- Modify: `src/auth/jwt.strategy.ts`, `src/auth/auth.module.ts`, `src/realtime/realtime.module.ts`, `test/instance.ts`, `src/realtime/presence.controller.spec.ts`, `package.json`, `package-lock.json`
- Delete: `src/auth/auth.service.ts`, `src/auth/auth.service.spec.ts`, `src/auth/auth.controller.ts`, `src/auth/auth.controller.spec.ts`, `src/auth/dto/` (all three DTOs)

**Interfaces:**
- Produces: `JWT_VERIFY_OPTIONS` token and `{ issuer, audience }` (env `JWT_ISSUER` default `auth-service`, `JWT_AUDIENCE` default `realtime-chat`); `JwtStrategy` verifying RS256 with `secretOrKeyProvider` -> `JwksService.getPublicKeyPem(kid)`, `validate` -> `{ id: sub }`; `AuthModule` providing `JWKS_OPTIONS` (env `AUTH_JWKS_URL`, default `http://localhost:3010/.well-known/jwks.json`; `ttlMs` 600000, `minRefetchMs` 10000), `JwksService`, `JwtStrategy`; `POST /ws-ticket` (`JwtAuthGuard`) -> `201 { ticket, expiresIn: 30 }`; `createInstance()` now calls `acquireJwksServer()` before compiling the app and `releaseJwksServer()` after `app.close()`.
- Note: Prisma still has `User`/`RefreshToken` in this task (dropped in Task 7), so tests that need user rows keep creating them; only the token path changes.

- [ ] **Step 1: Write the failing tests**

`src/auth/jwt-auth.spec.ts`:

```typescript
import { randomUUID } from 'crypto';
import { createInstance, TestInstance } from '../../test/instance';
import {
  defaultKey,
  generateTestKey,
  signHs256WithPublicKey,
  signToken,
  unsignedToken,
} from '../../test/jwt';

describe('JWT verification through JwtAuthGuard (GET /rooms)', () => {
  let app: TestInstance;

  beforeAll(async () => {
    app = await createInstance();
  });

  afterAll(async () => {
    await app.close();
  });

  function get(token?: string) {
    return fetch(`${app.url}/rooms`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  }

  const userId = () => randomUUID();

  it('accepts a valid RS256 token signed by the published key', async () => {
    const res = await get(signToken(userId()));

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual([]);
  });

  it('rejects a request without a token', async () => {
    expect((await get()).status).toBe(401);
  });

  it('rejects garbage', async () => {
    expect((await get('not.a.jwt')).status).toBe(401);
  });

  it('rejects a token whose kid is not published', async () => {
    const stranger = generateTestKey();
    expect((await get(signToken(userId(), { key: stranger }))).status).toBe(401);
  });

  it('rejects a token that claims a known kid but is signed with another private key', async () => {
    const forger = generateTestKey();
    expect((await get(signToken(userId(), { key: forger, kid: defaultKey.kid }))).status).toBe(401);
  });

  it('rejects an expired token', async () => {
    expect((await get(signToken(userId(), { expiresInSec: -60 }))).status).toBe(401);
  });

  it('rejects the wrong issuer', async () => {
    expect((await get(signToken(userId(), { issuer: 'evil-service' }))).status).toBe(401);
  });

  it('rejects the wrong audience', async () => {
    expect((await get(signToken(userId(), { audience: 'another-app' }))).status).toBe(401);
  });

  it('rejects alg:none', async () => {
    expect((await get(unsignedToken(userId()))).status).toBe(401);
  });

  it('rejects an HS256 token forged with the public key as secret (algorithm confusion)', async () => {
    expect((await get(signHs256WithPublicKey(userId()))).status).toBe(401);
  });
});
```

`src/realtime/ws-ticket.controller.spec.ts`:

```typescript
import { randomUUID } from 'crypto';
import { createInstance, TestInstance } from '../../test/instance';
import { cleanRedis } from '../../test/redis-cleanup';
import { connectSocket } from '../../test/helpers';
import { signToken } from '../../test/jwt';

describe('POST /ws-ticket', () => {
  let app: TestInstance;

  beforeAll(async () => {
    app = await createInstance();
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    await cleanRedis(app.redis);
  });

  it('returns 401 without a token', async () => {
    const res = await fetch(`${app.url}/ws-ticket`, { method: 'POST' });
    expect(res.status).toBe(401);
  });

  it('returns a 48-char hex ticket with expiresIn 30 for a valid access token', async () => {
    const res = await fetch(`${app.url}/ws-ticket`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${signToken(randomUUID())}` },
    });

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.expiresIn).toBe(30);
    expect(body.ticket).toMatch(/^[0-9a-f]{48}$/);
  });

  it('issues a ticket that opens a websocket exactly once', async () => {
    const res = await fetch(`${app.url}/ws-ticket`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${signToken(randomUUID())}` },
    });
    const { ticket } = await res.json();

    const socket = await connectSocket(app.url, ticket);
    expect(socket.connected).toBe(true);
    socket.disconnect();

    await expect(connectSocket(app.url, ticket)).rejects.toBeDefined();
  });

  it('no longer serves the old POST /auth/ws-ticket route', async () => {
    const res = await fetch(`${app.url}/auth/ws-ticket`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${signToken(randomUUID())}` },
    });
    expect(res.status).toBe(404);
  });
});
```

- [ ] **Step 2: Update `test/instance.ts` to run a JWKS server**

Add the import `import { acquireJwksServer, releaseJwksServer } from './jwt';`, call `await acquireJwksServer();` as the first statement of `createInstance`, and change `close`:

```typescript
    close: async () => {
      await app.close();
      await releaseJwksServer();
    },
```

- [ ] **Step 3: Run the new specs to verify they fail**

Run: `npm test -- jwt-auth ws-ticket.controller`
Expected: FAIL — `GET /rooms` with a valid RS256 token returns 401 (old HS256 strategy still active) and `/ws-ticket` returns 404.

- [ ] **Step 4: Implement verification and the ticket controller**

Replace `src/auth/jwt.strategy.ts`:

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

/** Reads the (unverified) `kid` from the JWT header; verification happens afterwards. */
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
  constructor(
    jwks: JwksService,
    @Inject(JWT_VERIFY_OPTIONS) options: JwtVerifyOptions,
  ) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      algorithms: ['RS256'],
      issuer: options.issuer,
      audience: options.audience,
      secretOrKeyProvider: (_req, rawJwt, done) => {
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

Replace `src/auth/auth.module.ts`:

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

Create `src/realtime/ws-ticket.controller.ts`:

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

In `src/realtime/realtime.module.ts` add `import { WsTicketController } from './ws-ticket.controller';` and change `controllers: [PresenceController]` to `controllers: [PresenceController, WsTicketController]`.

- [ ] **Step 5: Remove the old chat-side auth and fix the spec that used it**

```bash
git rm src/auth/auth.service.ts src/auth/auth.service.spec.ts src/auth/auth.controller.ts src/auth/auth.controller.spec.ts
git rm -r src/auth/dto
npm uninstall bcrypt @types/bcrypt
npm install --save-dev @nestjs/jwt@^10.2.0
```

(`@nestjs/jwt` is now only used by the test helper, so it moves to `devDependencies`.)

In `src/realtime/presence.controller.spec.ts`: remove `import { AuthService } from '../auth/auth.service';`, add `import { signToken } from '../../test/jwt';`, and replace the `registerUser` helper with:

```typescript
  async function registerUser(email: string) {
    // Users no longer live in this service: only a user row for the FK (dropped in Task 7) and a signed token.
    const user = await prisma.user.create({
      data: { email, passwordHash: 'x', displayName: email },
    });
    return { id: user.id, accessToken: signToken(user.id) };
  }
```

- [ ] **Step 6: Run everything**

Run: `npx tsc --noEmit && npm test`
Expected: tsc clean; all suites pass — new `jwt-auth` (10), `ws-ticket.controller` (4), `jwks.service` (10); removed auth specs no longer run. Total tests = previous 74 − 12 removed (`auth.service.spec` + `auth.controller.spec`) + 24 new = 86. If the count differs, explain it in the report rather than adjusting tests to match.

- [ ] **Step 7: Commit**

```bash
git add -A src test package.json package-lock.json
git commit -m "feat: verify access tokens via JWKS (RS256) and move ws-ticket to chat-service"
```

---

### Task 7: Drop `User`/`RefreshToken` and the FKs from the chat database

**Files:**
- Modify: `prisma/schema.prisma`, `test/db-cleanup.ts`, `src/rooms/rooms.service.spec.ts`, `src/realtime/events.gateway.spec.ts`, `src/realtime/multi-instance.spec.ts`, `src/realtime/presence.lifecycle.spec.ts`, `src/realtime/presence.controller.spec.ts`
- Create: `prisma/migrations/20261105000000_drop_user_tables_and_fks/migration.sql`

**Interfaces:**
- Produces: chat Prisma schema without `User`/`RefreshToken`; `RoomMember.userId` and `Message.senderId` plain `String` columns with no FK; `cleanDatabase` no longer touches those tables. Spec helpers create users as `{ id: randomUUID(), email, displayName }` with no DB row.
- This migration is destructive and irreversible without a backup: on any database holding real data run Task 4's script and verify the counts first (`SELECT count(*)` on both sides) and back up (`pg_dump`) before `prisma migrate deploy`.

- [ ] **Step 1: Edit `prisma/schema.prisma`**

Delete the `User` and `RefreshToken` models entirely. In `RoomMember` remove the line `user User @relation(fields: [userId], references: [id])`. In `Message` remove the line `sender User @relation(fields: [senderId], references: [id])`. Leave `userId` / `senderId` / `createdBy` as plain `String` fields. The resulting file has only `Room`, `RoomMember` and `Message` models.

- [ ] **Step 2: Write the migration by hand**

Create `prisma/migrations/20261105000000_drop_user_tables_and_fks/migration.sql`:

```sql
-- Users now live in auth-service (auth_db). Run the data migration (auth-service/scripts/migrate-users-from-chat.ts)
-- and reconcile counts BEFORE applying this migration anywhere that holds real data.

-- DropForeignKey
ALTER TABLE "RoomMember" DROP CONSTRAINT "RoomMember_userId_fkey";

-- DropForeignKey
ALTER TABLE "Message" DROP CONSTRAINT "Message_senderId_fkey";

-- DropTable (its FK to "User" goes with it)
DROP TABLE "RefreshToken";

-- DropTable
DROP TABLE "User";
```

- [ ] **Step 3: Update test helpers**

`test/db-cleanup.ts`:

```typescript
import { PrismaService } from '../src/prisma/prisma.service';

export async function cleanDatabase(prisma: PrismaService): Promise<void> {
  await prisma.message.deleteMany();
  await prisma.roomMember.deleteMany();
  await prisma.room.deleteMany();
}
```

In each of `src/rooms/rooms.service.spec.ts`, `src/realtime/events.gateway.spec.ts`, `src/realtime/multi-instance.spec.ts`, `src/realtime/presence.lifecycle.spec.ts`, add `import { randomUUID } from 'crypto';` and replace the body of the local `createUser` helper with:

```typescript
  async function createUser(email: string) {
    return { id: randomUUID(), email, displayName: email };
  }
```

In `src/realtime/presence.controller.spec.ts` replace `registerUser` with:

```typescript
  async function registerUser(_email: string) {
    const id = randomUUID();
    return { id, accessToken: signToken(id) };
  }
```

and add `import { randomUUID } from 'crypto';` (remove the now-unused `prisma.user` code there).

- [ ] **Step 4: Apply the migration to the test database and regenerate the client**

```bash
npm run prisma:migrate:test
npx prisma generate
npx tsc --noEmit
```

Expected: the new migration is applied to `chat_test`; tsc reports no references to `prisma.user` / `prisma.refreshToken` left anywhere (fix any stragglers with the same pattern).

- [ ] **Step 5: Run the full suite twice**

Run: `npm test && npm test`
Expected: all suites pass both times, same test count as after Task 6 (86). Rooms, gateway, multi-instance, presence and ticket specs work with UUID-only user ids.

- [ ] **Step 6: Commit**

```bash
git add -A prisma src test
git commit -m "feat: drop User/RefreshToken and cross-service FKs from the chat database"
```

---

### Task 8: `web/` BFF talks to both services

**Files:**
- Create: `web/lib/auth.ts`
- Modify: `web/app/api/login/route.ts`, `web/app/api/register/route.ts`, `web/app/api/ws-ticket/route.ts`, `web/lib/nest-fetch.ts`

**Interfaces:**
- Produces: `AUTH_URL` (`process.env.AUTH_URL ?? 'http://localhost:3010'`); register/login/refresh calls go to auth-service; `/api/ws-ticket` proxies to chat-service `POST /ws-ticket`; every rooms/messages/presence proxy is unchanged. No automated tests (BFF is exempt per the Phase 2 spec); verified by typecheck, build and the Task 9 smoke test.

- [ ] **Step 1: Create `web/lib/auth.ts`**

```typescript
export const AUTH_URL = process.env.AUTH_URL ?? 'http://localhost:3010';
```

- [ ] **Step 2: Point register/login at auth-service**

In `web/app/api/login/route.ts` and `web/app/api/register/route.ts`: replace `import { NEST_URL } from '../../../lib/nest';` with `import { AUTH_URL } from '../../../lib/auth';` and replace `${NEST_URL}/auth/login` / `${NEST_URL}/auth/register` with `${AUTH_URL}/auth/login` / `${AUTH_URL}/auth/register` (rest of the files unchanged).

- [ ] **Step 3: Refresh against auth-service, ws-ticket against chat-service**

In `web/lib/nest-fetch.ts`: add `import { AUTH_URL } from './auth';` and change the refresh call from `${NEST_URL}/auth/refresh` to `${AUTH_URL}/auth/refresh` (the request fetches to `${NEST_URL}${path}` stay as they are).

In `web/app/api/ws-ticket/route.ts` change `nestFetch('/auth/ws-ticket', { method: 'POST' })` to `nestFetch('/ws-ticket', { method: 'POST' })`.

- [ ] **Step 4: Verify**

```bash
cd web && npx tsc --noEmit && npm run build; cd ..
grep -rn "NEST_URL}/auth" web/app web/lib || echo "no leftover chat-service auth calls"
```

Expected: tsc clean, `next build` succeeds, grep prints the "no leftover" message.

- [ ] **Step 5: Commit**

```bash
git add web
git commit -m "feat(web): call auth-service for register/login/refresh and chat-service for ws-ticket"
```

---

### Task 9: Smoke test and final verification

**Files:**
- Create: `scripts/smoke-phase4.sh`
- Modify: `docs/superpowers/specs/2026-11-05-phase4-auth-service-design.md` (tick checklist, correct two details), `docs/superpowers/plans/2026-11-05-phase4-auth-service-ledger.md`

**Interfaces:** none (verification only).

- [ ] **Step 1: Create `scripts/smoke-phase4.sh`**

```bash
#!/usr/bin/env bash
# Boots auth-service (3010) and chat-service (3000) as real processes against the test
# databases and walks the whole flow. Requires: ./scripts/test-infra.sh, both projects built.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

set -a; . ./.env.test; set +a
CHAT_DB="$DATABASE_URL"
unset DATABASE_URL
(cd auth-service && set -a && . ./.env.test && set +a && npx prisma migrate deploy >/dev/null)

AUTH_DB="$(grep '^DATABASE_URL=' auth-service/.env.test | cut -d'"' -f2)"
KEY="$(./scripts/gen-jwt-keys.sh | cut -d'"' -f2)"
FAIL=0
pids=()
cleanup() { for p in "${pids[@]:-}"; do kill -TERM "$p" 2>/dev/null || true; done; wait 2>/dev/null || true; }
trap cleanup EXIT

PORT=3010 DATABASE_URL="$AUTH_DB" JWT_PRIVATE_KEY="$KEY" node auth-service/dist/main.js >/tmp/smoke-auth.log 2>&1 &
pids+=($!)
PORT=3000 DATABASE_URL="$CHAT_DB" AUTH_JWKS_URL="http://localhost:3010/.well-known/jwks.json" node dist/main.js >/tmp/smoke-chat.log 2>&1 &
pids+=($!)
sleep 6

check() { if [ "$2" = "$3" ]; then echo "ok   $1"; else echo "FAIL $1 (expected $2, got $3)"; FAIL=1; fi; }
EMAIL="smoke-$(date +%s)@example.com"

code=$(curl -s -o /dev/null -w '%{http_code}' localhost:3000/rooms)
check "chat rejects anonymous" 401 "$code"

TOKENS=$(curl -s -X POST localhost:3010/auth/register -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"password123\",\"displayName\":\"Smoke\"}")
ACCESS=$(printf '%s' "$TOKENS" | node -pe 'JSON.parse(require("fs").readFileSync(0)).accessToken')
check "register returns access token" true "$([ -n "$ACCESS" ] && echo true || echo false)"

code=$(curl -s -o /dev/null -w '%{http_code}' localhost:3000/rooms -H "Authorization: Bearer $ACCESS")
check "chat accepts auth-service token (JWKS)" 200 "$code"

ROOM=$(curl -s -X POST localhost:3000/rooms -H "Authorization: Bearer $ACCESS" \
  -H 'Content-Type: application/json' -d '{"name":"smoke"}')
ROOM_ID=$(printf '%s' "$ROOM" | node -pe 'JSON.parse(require("fs").readFileSync(0)).id')
check "create room" true "$([ -n "$ROOM_ID" ] && echo true || echo false)"

TICKET=$(curl -s -X POST localhost:3000/ws-ticket -H "Authorization: Bearer $ACCESS" \
  | node -pe 'JSON.parse(require("fs").readFileSync(0)).ticket')
RESULT=$(node -e '
const { io } = require("socket.io-client");
const s = io("http://localhost:3000", { transports: ["websocket"], auth: { ticket: process.argv[1] }, reconnection: false });
s.on("connect", () => { console.log("connected"); s.disconnect(); });
s.on("connect_error", (e) => { console.log("error:" + e.message); process.exit(0); });
' "$TICKET")
check "websocket connects with ticket" connected "$RESULT"

code=$(curl -s -o /dev/null -w '%{http_code}' localhost:3010/auth/me -H "Authorization: Bearer $ACCESS")
check "auth-service /auth/me" 200 "$code"

exit $FAIL
```

- [ ] **Step 2: Full automated verification**

```bash
chmod +x scripts/smoke-phase4.sh
npx tsc --noEmit && (cd auth-service && npx tsc --noEmit) && (cd web && npx tsc --noEmit)
npm run build && (cd auth-service && npm run build)
npm test && npm test
(cd auth-service && npx jest && npx jest)
```

Expected: all type checks and builds clean; chat suite passes twice (86 tests); auth-service suite passes twice (8 + 2 key tests, auth.service tests incl. 2 new, 7 controller tests, 4 migration tests).

- [ ] **Step 3: Run the smoke test**

Run: `./scripts/smoke-phase4.sh`
Expected: seven `ok` lines and exit code 0; `/tmp/smoke-auth.log` and `/tmp/smoke-chat.log` contain no stack traces.

- [ ] **Step 4: Update the spec and ledger, tick the checklist**

In the spec: change the migration script path from `scripts/migrate-users-to-auth-db.ts` (all occurrences) to `auth-service/src/migration/migrate-users.ts` + CLI `auth-service/scripts/migrate-users-from-chat.ts`; change `JwksService.getKey` wording to `getPublicKeyPem` (returns the SPKI PEM); change each `- [ ]` under "Tiêu chí hoàn thành Phase 4" to `- [x]` for items proven above. In the ledger mark every task `done` and append any Rulings recorded by the task reports.

- [ ] **Step 5: Commit**

```bash
git add scripts/smoke-phase4.sh docs
git commit -m "docs: add Phase 4 smoke test and mark completion criteria"
```
