# Phase 4 — Execution Ledger

Plan: `2026-11-05-phase4-auth-service-plan.md` · Spec: `../specs/2026-11-05-phase4-auth-service-design.md`

Rulings = technical decisions made without asking (allowed by the task brief). Every Ruling records the reason.

## Rulings

| # | Ruling | Reason |
|---|---|---|
| P4-R1 | Phase 4 is developed on branch `phase-4-auth-service`, created from `master`. | Explicit user instruction (branch-per-phase convention). |
| P4-R2 | The user migration lives in `auth-service/src/migration/migrate-users.ts` (+ CLI in `auth-service/scripts/`), not in a root `scripts/` file. | It needs the auth-service Prisma client and `pg`; after Task 7 the root Prisma client no longer has `User`. |
| P4-R3 | `JwksService.getPublicKeyPem()` returns SPKI PEM strings (not `KeyObject`s). | passport-jwt's `secretOrKeyProvider` types accept `string \| Buffer` only; PEM is also computed once per fetch, not per request. |
| P4-R4 | `JWT_PRIVATE_KEY` accepts single-line PEM with literal `\n`; tests use an ephemeral key generated in `auth-service/test/jest.env.ts`. | `.env` files cannot hold multi-line values portably; avoids committing any key material. |
| P4-R5 | Chat tests use a ref-counted real HTTP JWKS server (`acquireJwksServer`/`releaseJwksServer`) shared by app instances in one spec file. | Keeps verification real (no mocks) while avoiding leaked handles. |
| P4-R6 | The chat-service cut-over is two commits: Task 6 (verification + ws-ticket, Prisma tables kept) then Task 7 (drop tables/FKs). | Every commit stays green; the FK to `User` would otherwise break specs between the steps. |
| P4-R7 | `test-infra.sh` creates `auth_dev`/`auth_test` in the same native cluster and writes `auth-service/.env*`; `docker-compose.yml` gets separate Postgres services (ports 5435/5436). | No Docker in the cloud sandbox; Docker users still get isolated instances. |
| P4-R8 | Task 2: `Jwk` is a `type` alias (assignable to `JsonWebKey`); `key.service.spec` stubs only config lookup via a `{get}` object. | Nest `ConfigService.get` reads `process.env` before internal config, so a real ConfigService would ignore the stub. |
| P4-R9 | Tasks 3 and 5: no Rulings. | n/a |
| P4-R10 | Task 6: explicit parameter types in `secretOrKeyProvider`; `await sleep(200)` in `presence.lifecycle.spec` afterAll. | `@types/passport-jwt` union typing needs explicit types; a disconnect handler publishing through the redis adapter raced with Redis closing (unhandled "Connection is closed"). Known latent issue: `EventsGateway.broadcastPresence` adapter publish rejection is not catchable by the caller; only bites when Redis closes while disconnects are in flight. |
| P4-R11 | Task 4 follow-up: reconciliation test title clarified. | Other rows stay committed on mismatch; the script is re-runnable. |
| P4-R12 | Branch review fixes: `web/middleware.ts` refreshed against chat-service (`/auth/refresh` no longer exists there) -> now `AUTH_URL`; chat drop-migration gets a guard (`RAISE EXCEPTION` while `User` has rows unless marker table `_users_migrated_to_auth` exists) plus the required order in comments, verified on a scratch DB; migrate-users reads both tables in one `REPEATABLE READ READ ONLY` snapshot; stale `JWT_SECRET` removed from test infra; spec updated (env var names, write-freeze, marker step). | Independent whole-branch review. Not done (minor): JWK `use`/`alg` filtering and per-key try/catch in JwksService, content-level reconciliation, smoke-script readiness polling, clock injection in jwks specs. |
| P4-R13 | `web/` end-to-end was verified manually with 3 real processes (`next start` after rebuilding web/); no automated test for the BFF (exempt per Phase 2 spec). | tsc alone cannot catch routing mistakes such as the middleware one. |

## Task status

| Task | Status | Notes |
|---|---|---|
| 1 Scaffold auth-service + infra | done | |
| 2 KeyService + JWKS endpoint | done | |
| 3 Move auth (RS256) | done | |
| 4 User migration script | done | |
| 5 JwksService + JWT test helpers | done | |
| 6 chat cut-over (verify + ws-ticket) | done | |
| 7 Drop User/RefreshToken + FKs | done | |
| 8 web BFF | done | |
| 9 Smoke + final verification | done | |
