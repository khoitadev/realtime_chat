# Phase 3 — Execution Ledger

Plan: `2026-11-03-phase3-redis-scale-readiness-plan.md` · Spec: `../specs/2026-11-03-phase3-redis-scale-readiness-design.md`

Rulings = technical decisions made without asking (allowed by the task brief). Every Ruling records the reason.

## Rulings

| # | Ruling | Reason |
|---|---|---|
| R1 | Work on the designated branch `claude/loving-bohr-hekgat` in the main checkout instead of creating a separate git worktree. | The branch was created from `master` (same tip `9fbe2bc`) and git forbids two worktrees on one branch; master is untouched, so the "no coding on master" intent holds. |
| R2 | `superpowers:*` skills are not installed in this environment; the brainstorming → spec → plan → subagent execution → finish workflow is followed manually. | Skill list contains no `superpowers:*` entries. |
| R3 | Test infra is native Postgres 16 + Redis 7 via `scripts/test-infra.sh` (Postgres on 5432, not 5433). `docker-compose.yml` still gets a `redis` service for local dev. | No Docker daemon in the cloud sandbox; user chose "native, run in session". Real I/O is preserved (no mocks, no SQLite). |
| R4 | Redis client is `ioredis@^6.0.0`; `GETDEL`, `defineCommand`, `duplicate()`, pipeline custom commands were verified against the installed 6.0.0 typings and by running prototypes. | Version is newer than prior knowledge; API verified instead of assumed (Phase 2 lesson). |
| R5 | `presence:update` is broadcast to rooms the user is a **member** of (DB lookup), not rooms the socket has subscribed to. | At connect time the socket has not subscribed to anything yet. Clients re-sync with `GET /rooms/:id/presence` on room open/reconnect. |
| R6 | `disconnect()` only reports an online→offline transition when it actually removed the socket (`SREM` returned 1). | Makes graceful-shutdown cleanup and Nest's later socket-close handlers idempotent (no duplicate `online:false`). |
| R7 | Test env defaults `REDIS_URL` to `redis://localhost:6379/1`; `cleanRedis` refuses to flush db 0. | `.env.test` is git-ignored; guarantees tests can never wipe dev data. |
| R8 | Multi-instance tests boot the real `AppModule` twice in one process (random ports, shared Postgres + Redis) via `test/instance.ts`. | Exercises the real wiring (including the adapter) without Docker/multi-process orchestration. |
| R9 | `RedisIoAdapter.dispose()` is idempotent and relied on to be called by Nest's `SocketModule.close()` (verified in `@nestjs/websockets/socket-module.js`). | Avoids leaking pub/sub connections on `app.close()`. |

## Task status

| Task | Status | Notes |
|---|---|---|
| 1 Deps + test infra | pending | |
| 2 RedisModule + RedisIoAdapter | pending | |
| 3 WsTicketService → Redis | pending | |
| 4 PresenceService | pending | |
| 5 RoomsService member lookups | pending | |
| 6 Gateway presence + multi-instance harness | pending | |
| 7 Heartbeat / crash / shutdown | pending | |
| 8 Presence REST endpoint | pending | |
| 9 Bootstrap + web client | pending | |
| 10 Final verification | pending | |
