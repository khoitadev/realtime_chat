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

## Task status

| Task | Status | Notes |
|---|---|---|
| 1 Scaffold auth-service + infra | pending | |
| 2 KeyService + JWKS endpoint | pending | |
| 3 Move auth (RS256) | pending | |
| 4 User migration script | pending | |
| 5 JwksService + JWT test helpers | pending | |
| 6 chat cut-over (verify + ws-ticket) | pending | |
| 7 Drop User/RefreshToken + FKs | pending | |
| 8 web BFF | pending | |
| 9 Smoke + final verification | pending | |
