import Redis from 'ioredis';
import { loadConfig } from '../config';
import { IdempotencyService } from './idempotency.service';
import { cleanRedis } from '../../test/redis-cleanup';

describe('IdempotencyService', () => {
  let redis: Redis;
  let service: IdempotencyService;

  beforeAll(() => {
    redis = new Redis(process.env.REDIS_URL!);
    const config = {
      ...loadConfig({ AMQP_URL: 'x', PUSH_WEBHOOK_URL: 'y' }),
      processingTtlMs: 400,
    };
    service = new IdempotencyService(redis, config);
    service.onModuleInit();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await cleanRedis(redis);
  });

  const key = (e: string, r: string) => `notif:${e}:${r}`;

  it('the first claim wins and marks the key as processing', async () => {
    expect(await service.claim('e1', 'u1')).toBe('claimed');
    expect(await redis.get(key('e1', 'u1'))).toBe('processing');
  });

  it('a second claim while processing is busy', async () => {
    await service.claim('e1', 'u1');
    expect(await service.claim('e1', 'u1')).toBe('busy');
  });

  it('claims are independent per recipient and per event', async () => {
    expect(await service.claim('e1', 'u1')).toBe('claimed');
    expect(await service.claim('e1', 'u2')).toBe('claimed');
    expect(await service.claim('e2', 'u1')).toBe('claimed');
  });

  it('complete marks done (with a long TTL) and later claims report done', async () => {
    await service.claim('e1', 'u1');

    await service.complete('e1', 'u1', 'done');

    expect(await redis.get(key('e1', 'u1'))).toBe('done');
    expect(await redis.ttl(key('e1', 'u1'))).toBeGreaterThan(600000); // ~7 days (604800s)
    expect(await service.claim('e1', 'u1')).toBe('done');
  });

  it('a rejected mark also counts as done for later claims', async () => {
    await service.claim('e1', 'u1');
    await service.complete('e1', 'u1', 'rejected');

    expect(await redis.get(key('e1', 'u1'))).toBe('rejected');
    expect(await service.claim('e1', 'u1')).toBe('done');
  });

  it('release frees the key so the next attempt can claim it again (key is not burned)', async () => {
    await service.claim('e1', 'u1');

    await service.release('e1', 'u1');

    expect(await redis.get(key('e1', 'u1'))).toBeNull();
    expect(await service.claim('e1', 'u1')).toBe('claimed');
  });

  it('release never erases a completed mark', async () => {
    await service.claim('e1', 'u1');
    await service.complete('e1', 'u1', 'done');

    await service.release('e1', 'u1');

    expect(await redis.get(key('e1', 'u1'))).toBe('done');
  });

  it('complete does not downgrade an existing done/rejected mark', async () => {
    await service.claim('e1', 'u1');
    await service.complete('e1', 'u1', 'rejected');

    await service.complete('e1', 'u1', 'done');

    expect(await redis.get(key('e1', 'u1'))).toBe('rejected');
  });

  it('a crashed worker claim expires by itself, so a later claim succeeds', async () => {
    await service.claim('e1', 'u1'); // worker "crashes": never completes or releases
    expect(await service.claim('e1', 'u1')).toBe('busy');

    await new Promise((r) => setTimeout(r, 500));

    expect(await service.claim('e1', 'u1')).toBe('claimed');
  });

  it('complete still marks done if the processing claim expired mid-flight', async () => {
    await service.claim('e1', 'u1');
    await new Promise((r) => setTimeout(r, 500)); // claim expired while the webhook was running

    await service.complete('e1', 'u1', 'done');

    expect(await redis.get(key('e1', 'u1'))).toBe('done');
  });
});
