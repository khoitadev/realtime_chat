# Realtime Chat v2 — Roadmap Design

## Mục tiêu

Dựng lại hệ thống realtime chat bằng tay (không dùng AI code-gen cho logic chính),
xuất phát từ dự án `realtime-chat-system` (đã được review kiến trúc qua 6 module —
xem trang Notion "Ôn tập: Kiến trúc Realtime Chat System"), với 2 mục tiêu:

1. **Hiểu sâu** kiến trúc, pattern, công nghệ qua việc tự tay triển khai từng quyết
   định (không chỉ đọc/nghe giải thích).
2. **Sửa đúng từ đầu** 9 vấn đề/gap đã phát hiện trong bản gốc, thay vì tái tạo
   lại nguyên bug.

## Nguyên tắc tiếp cận

- Bắt đầu từ **1 monolith nhỏ**, tách dần thành microservice — **không** tách sẵn
  6 service từ đầu (bài học Module 1: chỉ tách khi có lý do thật — scale độc lập,
  ranh giới dữ liệu rõ, hoặc công nghệ lưu trữ khác nhau).
- Giữ nguyên stack công nghệ với bản gốc: NestJS, Next.js, PostgreSQL/Prisma,
  Redis, RabbitMQ, Elasticsearch — để kiến thức học được áp dụng thẳng vào
  CV/phỏng vấn.
- Mỗi phase là 1 sub-project có spec + implementation plan riêng khi bắt đầu làm
  (dự án tổng quá lớn để viết 1 spec/plan duy nhất).
- Code do người dùng tự viết; vai trò hỗ trợ là hướng dẫn (hỏi/gợi ý), không viết
  thay logic nghiệp vụ.

## Lộ trình Phase

| Phase | Nội dung | Bài học / fix áp dụng từ đầu |
|---|---|---|
| 0 | Scaffold: 1 NestJS app, 1 Postgres DB (Prisma) | — |
| 1 | Auth (register/login, access+refresh token rotation + reuse detection) + Rooms (tạo/join) + Message (REST, cursor pagination `(createdAt, id)` ngay từ đầu — không dùng `OFFSET`) | Fix #1 (reuse detection); tránh lại lỗi OFFSET pagination |
| 2 | Realtime: Socket.IO trong cùng app (1 instance, chưa cần Redis adapter) — WS auth qua short-lived token (BFF pattern từ Next.js Route Handler) | Áp lại Module 2/3 |
| 3 | Scale-readiness: Redis adapter (multi-instance) + presence dùng **Redis Set** (không dùng 1 string) | Fix #2 (bug multi-tab presence) |
| 4 | Tách **auth-service** (DB riêng `auth_db`) — phần còn lại gọi HTTP sang auth-service, verify JWT stateless (không gọi lại auth-service mỗi request) | Củng cố lý do Module 1 (data ownership, không phải chỉ fault isolation) |
| 5 | Đưa RabbitMQ vào, tách **notification-service** (consumer đầu tiên) — idempotency-rollback đúng từ đầu (set key sau side-effect hoặc rollback khi fail), `prefetch` set đúng (tránh non-deterministic ordering), retry bền ở broker (TTL + Dead Letter Exchange, không `setTimeout` trong RAM), payload event có field `correlationId` | Fix #3, #4, #5 (một phần), #9 |
| 6 | Tách **analytics-service** — xử lý đúng side-effect non-commutative (so sánh timestamp trước khi ghi đè `readAt`, tránh last-write-wins sai do xử lý concurrent) | Fix #5 (phần còn lại) |
| 7 | Tách **search-service** (Elasticsearch) + cache-aside thật cho trang đầu message (nối `getMessages()` không-cursor với Redis cache, không để cache write-only) | Fix #6 |
| 8 | Cross-cutting: `/health` thành readiness check thật (ping Postgres/Redis/RabbitMQ, trả `503` khi dependency chết), correlation-id forward xuyên suốt mọi proxy HTTP, graceful shutdown dùng 1 cách nhất quán (không giữ abstraction chết kiểu `ShutdownService` cũ), deploy có rollback khi smoke test fail | Fix #7, #8 |
| 9 (tuỳ chọn) | Chọn 1-2 tính năng mới từ danh sách đã brainstorm (typing indicator, edit/delete message, room roles, Web Push, unread count...) | — |

## Ngoài phạm vi (giai đoạn này)

- Chưa quyết định tách search/analytics ngay — chỉ tách khi Phase 6-7 tới, để
  trải nghiệm đúng quyết định "khi nào nên tách" tại đúng thời điểm, không tách
  trước cho "gọn".
- Không làm end-to-end encryption (xung đột kiến trúc với search/analytics —
  đã phân tích ở phần brainstorm tính năng, xem Notion).
- Chưa xác định quy mô Phase 9 — sẽ brainstorm riêng khi tới.

## Bước tiếp theo

Phase 1 sẽ được brainstorm/spec chi tiết riêng (schema Prisma, API contract,
cấu trúc module NestJS) ngay trước khi bắt đầu viết code, theo đúng tinh thần
"mỗi sub-project có spec + plan riêng" nêu trên.
