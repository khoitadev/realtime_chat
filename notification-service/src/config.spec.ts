import { loadConfig } from './config';

const base = { AMQP_URL: 'amqp://x', PUSH_WEBHOOK_URL: 'http://hook' };

describe('loadConfig', () => {
  it('applies defaults', () => {
    const c = loadConfig(base);

    expect(c).toMatchObject({
      amqpUrl: 'amqp://x',
      pushWebhookUrl: 'http://hook',
      redisUrl: 'redis://localhost:6379',
      retryTtlMs: [5000, 30000, 300000],
      webhookTimeoutMs: 3000,
      processingTtlMs: 60000,
      doneTtlSec: 604800,
      reconnectDelayMs: 1000,
    });
  });

  it('reads overrides from env', () => {
    const c = loadConfig({
      ...base,
      REDIS_URL: 'redis://r:1/2',
      RETRY_TTL_MS: '100, 200,300',
      WEBHOOK_TIMEOUT_MS: '500',
    });

    expect(c.redisUrl).toBe('redis://r:1/2');
    expect(c.retryTtlMs).toEqual([100, 200, 300]);
    expect(c.webhookTimeoutMs).toBe(500);
  });

  it.each([
    ['AMQP_URL', { PUSH_WEBHOOK_URL: 'http://hook' }],
    ['PUSH_WEBHOOK_URL', { AMQP_URL: 'amqp://x' }],
  ])('throws when %s is missing', (name, env) => {
    expect(() => loadConfig(env)).toThrow(new RegExp(name));
  });

  it.each(['1,2', '1,2,3,4', 'a,b,c', '1,0,3', '1,-2,3'])('rejects RETRY_TTL_MS=%s', (value) => {
    expect(() => loadConfig({ ...base, RETRY_TTL_MS: value })).toThrow(/RETRY_TTL_MS/);
  });
});
