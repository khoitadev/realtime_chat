# Phase 6 — RabbitMQ + notification-service (consumer đầu tiên)

Sub-project của roadmap tổng (`2026-10-10-realtime-chat-v2-roadmap-design.md`),
tiếp theo Phase 5 (`2026-11-05-phase5-gateway-design.md`).

## Mục tiêu

Đưa **RabbitMQ** vào hệ thống và tách **notification-service** — consumer đầu
tiên. Khi có message mới, chat-service publish event `message.created`;
notification-service nhận và gọi một webhook ngoài (giả lập push/email
provider) cho từng người nhận.

Phase này sửa đúng từ đầu các lỗi đã phát hiện ở bản gốc:

| Lỗi bản gốc | Cách làm đúng ở Phase 6 |
|---|---|
| Idempotency: set key **trước** side-effect, side-effect fail thì key vẫn còn → retry bị bỏ qua, mất thông báo | Claim → side-effect → đánh dấu `done` **sau** khi thành công; fail thì **nhả** key |
| `prefetch` không đặt → nhiều message xử lý đồng thời, thứ tự không xác định | `prefetch=1`, xử lý tuần tự |
| Retry bằng `setTimeout` trong RAM → crash là mất | Retry bền ở broker: TTL queue + dead-letter |
| Event không có `correlationId` → không lần vết được xuyên service | `correlationId` trong payload + log + header webhook |

## Quyết định đã chốt (brainstorm)

| Chủ đề | Quyết định | Phương án đã loại |
|---|---|---|
| Side-effect của consumer | Gọi webhook HTTP ra ngoài (`PUSH_WEBHOOK_URL`), **không có DB riêng**; idempotency bằng Redis | Lưu in-app notification vào `notification_db` (idempotency tự nhiên nhờ unique, mất bài học); làm cả hai (quá lớn) |
| Publish từ chat-service | Publisher confirms, best-effort; lỗi publish chỉ log/đếm, không làm hỏng việc gửi message; ghi known limitation | Transactional outbox (sub-project riêng) |
| Retry | 3 retry queue theo bậc (5s / 30s / 5m, TTL cố định theo queue) + DLQ | 1 queue delay cố định; per-message TTL (head-of-line blocking) |
| Prefetch | `prefetch=1`, xử lý tuần tự | prefetch lớn (thứ tự không xác định) |

## Phạm vi

**Có trong Phase 6:**
- `notification-service/`: project NestJS độc lập (application context, không
  HTTP), consumer, topology broker, idempotency Redis, webhook client.
- chat-service: `RabbitMqModule` (publisher có confirm), publish
  `message.created` trong `RoomsService.sendMessage`, đọc/sinh `correlationId`.
- gateway: sinh và forward `X-Correlation-Id` (proxy REST + `InternalChatClient`).
- Hạ tầng: `rabbitmq` trong `docker-compose.yml`; `scripts/test-infra.sh` cài/khởi
  động RabbitMQ native, tạo vhost `chat_test` + user test.

**Không có trong Phase 6:**
- Outbox / đảm bảo at-least-once từ đầu đến cuối phía publisher.
- Lọc người nhận theo presence (đang online thì không cần thông báo), web push,
  unread count — Phase 10.
- Provider thật (email/FCM): webhook là giả lập.
- Correlation-id forward xuyên **mọi** proxy và mọi service — Phase 9 (ở đây chỉ
  làm đoạn gateway → chat-service → event → notification-service).
- Metrics/alert cho DLQ — Phase 9.
- analytics-service (Phase 7) — nhưng topology được thiết kế để consumer thứ hai
  không bị nhận trùng (xem "Retry").

## Kiến trúc & cấu trúc repo

```
realtime-chat-v2/
├── notification-service/              # MỚI — NestJS app độc lập, không HTTP
│   ├── package.json  tsconfig*.json  nest-cli.json  jest.config.js
│   ├── src/
│   │   ├── main.ts                    # createApplicationContext + shutdown hooks
│   │   ├── app.module.ts
│   │   ├── config/                    # env: AMQP_URL, REDIS_URL, PUSH_WEBHOOK_URL, TTL
│   │   ├── broker/
│   │   │   ├── topology.ts            # khai báo exchange/queue (idempotent)
│   │   │   ├── broker.service.ts      # connection + channel, reconnect, shutdown
│   │   │   └── event.schema.ts        # validate message.created v1
│   │   ├── consumer/
│   │   │   ├── notification.consumer.ts   # nhận, xử lý tuần tự, ack/retry/dlq
│   │   │   └── retry.policy.ts        # chọn tier theo x-attempt
│   │   ├── idempotency/idempotency.service.ts   # claim / complete / release (Redis)
│   │   └── delivery/webhook.client.ts # POST PUSH_WEBHOOK_URL
│   └── test/                          # webhook sink (HTTP server thật), helpers
├── src/                               # chat-service
│   ├── messaging/                     # MỚI
│   │   ├── rabbitmq.module.ts
│   │   ├── event.publisher.ts         # confirm channel, persistent
│   │   └── events.ts                  # type MessageCreatedEvent
│   └── rooms/rooms.service.ts         # sendMessage: publish message.created
├── gateway/                           # forward X-Correlation-Id
└── scripts/test-infra.sh, docker-compose.yml
```

`tsconfig.json` / `tsconfig.build.json` ở root loại thêm `notification-service`.

## Event `message.created` (v1)

```json
{
  "eventId": "uuid-v4",
  "type": "message.created",
  "version": 1,
  "occurredAt": "2026-11-08T10:00:00.000Z",
  "correlationId": "string",
  "data": {
    "messageId": "uuid",
    "roomId": "uuid",
    "senderId": "uuid",
    "preview": "tối đa 100 ký tự đầu của nội dung",
    "recipientIds": ["uuid", "..."]
  }
}
```

- `recipientIds` = mọi member của room **trừ người gửi** (đọc trong
  `RoomsService.sendMessage`, sau khi lưu DB). Rỗng thì **không publish**.
- Chỉ gửi `preview` (cắt 100 ký tự), không gửi toàn bộ nội dung sang provider.
- `eventId` mới cho mỗi lần gửi message; là khoá idempotency.
- Consumer chấp nhận `version === 1`; version khác hoặc thiếu field → DLQ
  (không retry), xem "Consumer".

### Publisher (chat-service)

- `amqplib` confirm channel, kết nối từ `AMQP_URL`.
- Exchange `chat.events` (topic, durable). Routing key `message.created`.
  Message `persistent: true`, `contentType: application/json`, header
  `x-correlation-id`.
- `publish()` chờ confirm với timeout (mặc định 2s). Lỗi/timeout → log
  `warn` + tăng bộ đếm, **không throw** — việc gửi message của user vẫn thành
  công. Đây là **known limitation**: crash giữa lúc commit DB và lúc publish,
  hoặc broker chết, làm mất event (outbox để sau nếu cần).
- Kết nối tự khôi phục sau khi broker khởi động lại (publisher tạo lại channel
  khi `close`/`error`); trong lúc mất kết nối, publish bị bỏ qua (log).
- Thiếu `AMQP_URL` (dev không chạy RabbitMQ) → publisher ở chế độ no-op, log
  một lần lúc khởi động; chat-service vẫn chạy.

### `correlationId`

- Gateway: lấy `X-Correlation-Id` từ request nếu client gửi (cắt 128 ký tự, chỉ
  `[A-Za-z0-9._-]`), nếu không thì sinh UUID. Forward header này trong
  `ProxyController` và trong `InternalChatClient` (đường WS `message:send`).
- chat-service: `RoomsController`/internal đọc `X-Correlation-Id`, truyền vào
  `RoomsService.sendMessage`; thiếu thì sinh UUID. Đưa vào event.
- notification-service: log mọi dòng kèm `correlationId` và gửi header
  `X-Correlation-Id` sang webhook.

## Topology broker

Khai báo idempotent (`assert*`) khi service khởi động, ở **notification-service**
(chủ sở hữu queue của consumer) và exchange `chat.events` ở cả hai bên.

| Thành phần | Loại | Cấu hình |
|---|---|---|
| `chat.events` | exchange topic, durable | publisher: chat-service |
| `notifications.work` | queue durable | bind `chat.events` với `message.created` |
| `notifications.retry.1` | queue durable | `x-message-ttl=5000` |
| `notifications.retry.2` | queue durable | `x-message-ttl=30000` |
| `notifications.retry.3` | queue durable | `x-message-ttl=300000` |
| `notifications.dlq` | queue durable | không TTL, không consumer (park) |

Cả ba retry queue có `x-dead-letter-exchange=""` (default exchange) và
`x-dead-letter-routing-key=notifications.work`: hết TTL thì message quay **thẳng
vào `notifications.work`**, không đi lại qua `chat.events`.

> Lý do: nếu dead-letter qua `chat.events`, mọi queue khác cùng bind
> `message.created` (ví dụ analytics ở Phase 7) sẽ nhận **bản trùng** mỗi lần
> retry. Dead-letter qua default exchange chỉ chạm đúng queue của consumer
> đang retry (đã kiểm chứng thực tế trên RabbitMQ 3.12).

TTL các bậc cấu hình qua env (`RETRY_TTL_MS=5000,30000,300000`) để test dùng
giá trị ngắn. TTL gắn theo **queue** (không per-message) để tránh head-of-line
blocking. Vì argument của queue không đổi được sau khi tạo, đổi TTL cần xoá/tạo
lại queue (ghi trong runbook; test dùng vhost sạch).

## Consumer

- `channel.prefetch(1)`; mỗi message được xử lý xong (ack/retry/dlq) mới nhận
  message kế → thứ tự giao lần đầu xác định. Retry làm đảo thứ tự; chấp nhận vì
  thông báo không cần thứ tự chặt.
- Callback consume xử lý `msg === null` (consumer bị broker huỷ, ví dụ queue bị
  xoá) bằng cách log và để `BrokerService` đăng ký lại.
- Luồng xử lý một message:

```
parse + validate (event.schema)
  ├─ lỗi parse / sai version / thiếu field ........... → DLQ (header x-reason), ack
  └─ hợp lệ
       for each recipient (tuần tự):
         claim(eventId, recipient)
           ├─ done ............................ bỏ qua (đã gửi)
           ├─ processing (worker khác/crash) .. đánh dấu cần retry, tiếp recipient sau
           └─ claimed:
                POST webhook (timeout 3s, Idempotency-Key, X-Correlation-Id)
                ├─ 2xx ... complete() = SET done EX 7d
                └─ lỗi ... release() = DEL key; đánh dấu cần retry
       có recipient cần retry? → retry (tier theo x-attempt) rồi ack bản gốc
       ngược lại ................................ ack
```

- **Retry**: `attempt = header x-attempt ?? 0`. Nếu `attempt < 3` thì
  `sendToQueue('notifications.retry.{attempt+1}', content, { persistent,
  headers: { ...headers, 'x-attempt': attempt+1 } })`, **chờ confirm**, rồi mới
  ack bản gốc (nếu broker chết giữa chừng, message được giao lại và idempotency
  chặn gửi trùng). `attempt >= 3` → `notifications.dlq` với `x-reason=
  retries-exhausted`, rồi ack.
- Retry xử lý lại **cả event** nhưng recipient đã `done` bị bỏ qua nên không gửi
  lại cho người đã nhận (idempotency theo từng recipient).
- Webhook coi là lỗi: timeout, lỗi mạng, HTTP ≥ 500, 429. HTTP 4xx khác (trừ 429)
  là lỗi vĩnh viễn: ghi nhận recipient đó `done` với kết quả `rejected`, **không
  retry** (retry sẽ vô ích) và log `warn`. (Quyết định ghi trong ledger.)

### Idempotency (Redis, `notif:{eventId}:{recipientId}`)

- `claim`: `SET key processing NX PX 60000`.
  - Thành công → `claimed`.
  - Thất bại → `GET key`: `done`/`rejected` → `done`; `processing` → `busy`.
- `complete`: `SET key done EX 604800` (7 ngày) — **chỉ gọi sau khi webhook
  thành công**.
- `release`: `DEL key` — gọi khi webhook lỗi, để lần retry claim lại được.
- `PX 60000` bảo vệ trường hợp worker chết giữa chừng: key `processing` tự hết
  hạn, lần giao lại sau đó claim được. `complete`/`release` dùng Lua so sánh giá
  trị (`processing`) để không ghi đè trạng thái của worker khác.
- Hệ quả đúng với bug gốc: webhook fail → key bị nhả → retry thực sự gửi lại;
  webhook thành công → redelivery của cùng `eventId` không gọi webhook lần hai.
- Giới hạn đã biết: nếu worker chết **sau khi** webhook thành công nhưng **trước
  khi** `complete`, lần giao lại sau khi `processing` hết hạn sẽ gửi **lần hai**
  (at-least-once). Giảm nhẹ bằng header `Idempotency-Key` để provider tự khử
  trùng.

## Hạ tầng và cấu hình

- `docker-compose.yml`: `rabbitmq:3-management` (5672, 15672), healthcheck.
- `scripts/test-infra.sh` (native): cài `rabbitmq-server` bằng apt nếu thiếu,
  khởi động `rabbitmq-server -detached` (container không có systemd), chờ
  `rabbitmqctl await_startup`, tạo vhost `chat_test`, user `chat`/`chat`,
  quyền `".*" ".*" ".*"`; ghi `AMQP_URL` vào `.env.test` của chat-service và
  `notification-service/.env.test`.
- Env mới: chat-service `AMQP_URL`; notification-service `AMQP_URL`,
  `REDIS_URL`, `PUSH_WEBHOOK_URL`, `RETRY_TTL_MS`, `WEBHOOK_TIMEOUT_MS`.
- Test dùng vhost `chat_test`, Redis db 1 (riêng cho notification-service dùng
  db 2 để không dẫm lên test của chat-service), queue/exchange được xoá và tạo
  lại trong `beforeEach`.

## Testing strategy (TDD với I/O thật — giữ quy ước các phase trước)

Không mock Prisma / Redis / RabbitMQ / socket / HTTP. Hạ tầng test: Postgres,
Redis, **RabbitMQ thật**. Webhook sink là **HTTP server thật** chạy local với
chế độ lỗi lập trình được (tầng mạng, như JWKS server Phase 4).

**chat-service (publisher):**
- `sendMessage` publish đúng 1 event `message.created`: schema đúng, `recipientIds`
  = member trừ người gửi, `preview` cắt 100 ký tự, `correlationId` lấy từ header
  hoặc tự sinh, message persistent.
- Room chỉ có người gửi → không publish.
- Broker không khả dụng → `sendMessage` vẫn thành công, message đã lưu DB, có log
  warn (không throw).
- Không cấu hình `AMQP_URL` → no-op, chat-service chạy bình thường.
- Cả đường REST và đường internal (gateway) đều publish.

**gateway:** `X-Correlation-Id` được forward qua proxy và `InternalChatClient`;
sinh mới khi thiếu; giá trị không hợp lệ bị thay bằng UUID.

**notification-service (consumer):**
- Topology: queue/exchange/TTL/DLX đúng như bảng (kiểm bằng `checkQueue` và gửi
  thử một message qua retry queue để thấy nó quay về `notifications.work`
  **mà không** xuất hiện ở queue khác cùng bind).
- Giao đúng một lần cho mỗi recipient (webhook sink đếm request, kiểm
  `Idempotency-Key`, `X-Correlation-Id`).
- Redelivery cùng `eventId` (publish lại bản sao) không gọi webhook lần hai.
- Webhook lỗi 5xx → lên tier 1 (quay lại sau ≈ TTL), lỗi tiếp → tier 2, tier 3 →
  DLQ sau lần thứ 3; webhook hồi phục giữa chừng → gửi thành công và dừng retry.
- Lỗi từng phần (3 recipient, người thứ 2 lỗi): người 1 và 3 chỉ nhận 1 lần
  dù retry nhiều lần; người 2 nhận sau khi hồi phục.
- Key không bị "đốt": sau khi webhook fail, key bị nhả (`GET` trả null); sau khi
  thành công key là `done`.
- Claim đang `processing` (key đặt sẵn) → event đi retry, không gọi webhook.
- 4xx (≠ 429) → không retry, ghi `rejected`; 429 → retry.
- Event không parse được / `version: 2` / thiếu field → DLQ với `x-reason`, không
  retry, không gọi webhook.
- `prefetch=1`: gửi N event liên tiếp, webhook sink ghi nhận **không bao giờ có
  hơn 1 request đồng thời** và thứ tự khớp thứ tự publish.
- Shutdown êm: dừng consumer, hoàn tất message đang xử lý, đóng channel/connection
  (không mất ack).
- Mất kết nối broker (khởi động lại RabbitMQ trong test hoặc đóng connection
  bằng `rabbitmqctl close_all_connections`) → consumer tự kết nối lại và tiếp tục.

**Smoke cuối (thủ công):** chạy auth-service, chat-service, gateway,
notification-service + webhook sink: gửi message qua gateway (REST và WS), thấy
webhook nhận đúng 1 request cho mỗi recipient với cùng `correlationId` xuyên suốt;
tắt sink → thấy retry theo tier (TTL ngắn), bật lại → giao thành công.

**Không cần TDD:** `main.ts` bootstrap, `docker-compose.yml`,
`scripts/test-infra.sh`.

## Rủi ro và đánh đổi

- **Mất event phía publisher** khi crash giữa commit và publish, hoặc broker chết
  (đã chấp nhận; outbox là bước sau).
- **Gửi trùng hiếm** khi worker chết sau webhook thành công, trước `complete`.
- **Thứ tự**: retry làm đảo thứ tự thông báo.
- **Thông báo cho cả người đang online** (chưa lọc theo presence) — có thể ồn.
- **Đổi TTL retry** cần tạo lại queue (argument không đổi được tại chỗ).
- **Phụ thuộc mới ở runtime**: RabbitMQ. chat-service vẫn phục vụ khi thiếu
  broker (publish no-op/log); notification-service thì không chạy được.
- Webhook là giả lập; chưa có provider thật, chưa có rate limit hay circuit
  breaker cho lời gọi ra ngoài.

## Tiêu chí hoàn thành Phase 6

- [x] `message.created` được publish đúng schema (có `eventId`, `correlationId`,
      `recipientIds`) cho cả REST và WS; lỗi publish không làm hỏng việc gửi.
- [x] notification-service giao đúng một lần cho mỗi recipient; redelivery không
      gửi trùng.
- [x] Webhook fail → key idempotency được nhả và retry thực sự gửi lại (bug gốc đã
      tránh).
- [x] Retry bền ở broker: 3 bậc TTL (5s/30s/5m, cấu hình được) rồi DLQ; không
      dùng timer trong RAM; retry không làm consumer khác nhận trùng.
- [x] `prefetch=1`, xử lý tuần tự, thứ tự lần giao đầu xác định.
- [x] `correlationId` xuyên gateway → chat-service → event → webhook.
- [x] Event hỏng đi thẳng DLQ; 4xx vĩnh viễn không retry.
- [x] Tự kết nối lại khi broker khởi động lại; shutdown êm.
- [x] `npx tsc --noEmit` sạch cho mọi project; mọi test của các service pass với
      Postgres + Redis + RabbitMQ thật; smoke nhiều process pass.
