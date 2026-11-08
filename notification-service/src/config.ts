export const CONFIG = Symbol('NOTIFICATION_CONFIG');

export interface NotificationConfig {
  amqpUrl: string;
  redisUrl: string;
  pushWebhookUrl: string;
  /** Fixed TTL of retry queues 1..3 (ms). */
  retryTtlMs: [number, number, number];
  webhookTimeoutMs: number;
  /** How long a `processing` idempotency claim survives a crashed worker (ms). */
  processingTtlMs: number;
  /** How long `done`/`rejected` idempotency marks are kept (s). */
  doneTtlSec: number;
  reconnectDelayMs: number;
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name];
  if (!value) {
    throw new Error(`${name} is required`);
  }
  return value;
}

function positiveInt(env: NodeJS.ProcessEnv, name: string, fallback: number): number {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const n = Number(raw);
  if (!Number.isInteger(n) || n <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return n;
}

function parseRetryTtls(raw: string | undefined): [number, number, number] {
  if (!raw) return [5000, 30000, 300000];
  const parts = raw.split(',').map((p) => Number(p.trim()));
  if (parts.length !== 3 || parts.some((n) => !Number.isInteger(n) || n <= 0)) {
    throw new Error('RETRY_TTL_MS must be three comma-separated positive integers');
  }
  return parts as [number, number, number];
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): NotificationConfig {
  return {
    amqpUrl: required(env, 'AMQP_URL'),
    redisUrl: env.REDIS_URL ?? 'redis://localhost:6379',
    pushWebhookUrl: required(env, 'PUSH_WEBHOOK_URL'),
    retryTtlMs: parseRetryTtls(env.RETRY_TTL_MS),
    webhookTimeoutMs: positiveInt(env, 'WEBHOOK_TIMEOUT_MS', 3000),
    processingTtlMs: positiveInt(env, 'PROCESSING_TTL_MS', 60000),
    doneTtlSec: positiveInt(env, 'DONE_TTL_SEC', 604800),
    reconnectDelayMs: positiveInt(env, 'RECONNECT_DELAY_MS', 1000),
  };
}
