# Phase 6 — Execution Ledger

Plan: `2026-11-08-phase6-rabbitmq-notification-plan.md` · Spec: `../specs/2026-11-08-phase6-rabbitmq-notification-design.md`

Rulings = technical decisions made without asking (allowed by the task brief). Every Ruling records the reason.

## Rulings

| # | Ruling | Reason |
|---|---|---|
| P6-R1 | Phase 6 is developed on branch `phase-6-rabbitmq-notification`, created from `master`. | Explicit user instruction (branch-per-phase convention). |
| P6-R2 | RabbitMQ 3.12 is installed natively with apt (`rabbitmq-server`), started with `rabbitmq-server -detached`; vhost `chat_test`, user `chat`/`chat`. `docker-compose.yml` gets a `rabbitmq` service for Docker users. | No Docker daemon in the cloud sandbox; consistent with the earlier "native, run in session" decision for Postgres/Redis. Real broker, no mocks. |
| P6-R3 | Retry queues dead-letter through the default exchange (`x-dead-letter-exchange=""`, routing key `notifications.work`) instead of back through `chat.events`. | Verified on RabbitMQ 3.12: avoids duplicate delivery of retried messages to any other queue bound to `message.created` (e.g. future analytics-service). |
| P6-R4 | HTTP 4xx (except 429) from the webhook is a permanent failure: marked `rejected`, never retried. 429, 5xx, timeouts and network errors are retried. | Retrying a 400/404/422 cannot succeed and would only burn the retry tiers. |
| P6-R5 | At the user's request the outbox variant was tried and then reverted: Phase 6 keeps best-effort publishing with confirms; losing an event on a crash between DB commit and publish is a documented known limitation. | User decision (spec commits 848b701 → revert). |
| P6-R6 | `sendMessage` does not await the broker publish (fire-and-forget; the publisher swallows and counts its own errors). | A slow or dead broker must never delay or fail a user's message send. |
| P6-R7 | notification-service tests use Redis db 2; chat-service/gateway keep db 1. | Prevents the flushes of one project's specs from racing with the other's data. |
| P6-R8 | Task 1: root `npm install amqplib` resolved to 2.x; pinned `^0.10.9` in both projects (root and notification-service). | The API was verified on 0.10 and the installed types are 0.10.8. |
| P6-R9 | Task 3: `rabbitmqctl close_all_connections` only touches vhost `/` by default; all tests and scripts use `--vhost chat_test`. | Otherwise the command silently closes nothing in the test vhost. |
| P6-R10 | Task 5: the webhook sink clears pending delay timers on close. | A pending timer was an open handle that kept Jest/the process alive. |
| P6-R11 | Task 7: the harness uses an independent Redis client for assertions because the app's own client is quit on shutdown; the queue-deleted test waits for `isReady` false->true before publishing (`isReady` stays true until the broker's cancel notification arrives). amqplib 0.10.9 verified to deliver `null` on a server-initiated consumer cancel. | Avoids asserting through a closed client and publishing into the window before the consumer notices the deletion. |
| P6-R12 | Task 9: `MessagingModule` is imported in `internal.controller.spec` and `presence.controller.spec` and in the second `rooms.service.spec` describe (the publisher stays unconnected there and silently skips). | Those modules now depend on the publisher provider; no broker is needed for them. |
| P6-R13 | Branch review fixes: consumer validates `correlationId` (`[A-Za-z0-9._-]{1,128}`) in `parseEvent` (header/log injection from other publishers; bad events go straight to the DLQ); unexpected processing errors are capped (5 per message, then a confirmed DLQ publish with `x-reason=processing-error`, instead of a ~1 Hz nack loop); consumer `inflight` is a Set (a stale handler after a reconnect no longer hides the new one from graceful shutdown); the webhook client no longer follows redirects (3xx = rejected); the chat publisher clears its confirm timer when `publish` throws synchronously. +5 tests (notification-service 72 -> 77). | Independent whole-branch review (no Critical findings). |
| P6-R14 | Known limitations accepted (not fixed): events published before notification-service has ever started are dropped by the broker (the work queue only exists once it has declared it; chat does not declare it); a busy claim consumes one retry tier; a delivered-but-not-completed recipient can be re-sent after the 60s processing TTL (mitigated by the `Idempotency-Key` header); no exponential backoff / connect timeout when topology assertion keeps failing (e.g. changed retry TTLs need the queues recreated); `X-Correlation-Id` is echoed by the gateway only on successful proxy responses; message `preview` (<=100 chars of user content) is sent to the external webhook; timing-based negative assertions in some specs. | Cost/benefit for this phase; documented here and in the spec's risk list. |

## Task status

| Task | Status | Notes |
|---|---|---|
| 1 RabbitMQ infra + scaffold | done | |
| 2 Config + event schema | done | |
| 3 Topology + BrokerService | done | |
| 4 IdempotencyService | done | |
| 5 WebhookClient + sink | done | |
| 6 NotificationConsumer | done | |
| 7 Bootstrap / shutdown / reconnect | done | |
| 8 chat EventPublisher | done | |
| 9 sendMessage publishes + correlationId | done | |
| 10 gateway correlation id | done | |
| 11 Smoke + final verification | done | |
