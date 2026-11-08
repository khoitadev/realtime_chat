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

## Task status

| Task | Status | Notes |
|---|---|---|
| 1 RabbitMQ infra + scaffold | pending | |
| 2 Config + event schema | pending | |
| 3 Topology + BrokerService | pending | |
| 4 IdempotencyService | pending | |
| 5 WebhookClient + sink | pending | |
| 6 NotificationConsumer | pending | |
| 7 Bootstrap / shutdown / reconnect | pending | |
| 8 chat EventPublisher | pending | |
| 9 sendMessage publishes + correlationId | pending | |
| 10 gateway correlation id | pending | |
| 11 Smoke + final verification | pending | |
