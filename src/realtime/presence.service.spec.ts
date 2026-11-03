import Redis from 'ioredis';
import { PresenceService } from './presence.service';
import { cleanRedis } from '../../test/redis-cleanup';
import { sleep } from '../../test/helpers';

describe('PresenceService (Redis Set)', () => {
  let redis: Redis;
  let service: PresenceService;

  beforeAll(() => {
    redis = new Redis(process.env.REDIS_URL!);
    service = new PresenceService(redis, { heartbeatTtlSec: 1, refreshIntervalMs: 1000 });
    service.onModuleInit();
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await cleanRedis(redis);
  });

  it('reports offline for a user nobody connected', async () => {
    expect(await service.isOnline('u1')).toBe(false);
  });

  it('first connect is an offline->online transition, second connect is not', async () => {
    expect(await service.connect('u1', 'sock-a')).toBe(true);
    expect(await service.connect('u1', 'sock-b')).toBe(false);
    expect(await redis.scard('presence:user:u1')).toBe(2);
  });

  it('keeps the user online after closing one of two tabs (the single-string bug)', async () => {
    await service.connect('u1', 'sock-a');
    await service.connect('u1', 'sock-b');

    expect(await service.disconnect('u1', 'sock-a')).toBe(false);

    expect(await service.isOnline('u1')).toBe(true);
  });

  it('goes offline only when the last socket disconnects', async () => {
    await service.connect('u1', 'sock-a');
    await service.connect('u1', 'sock-b');
    await service.disconnect('u1', 'sock-a');

    expect(await service.disconnect('u1', 'sock-b')).toBe(true);

    expect(await service.isOnline('u1')).toBe(false);
    expect(await redis.exists('presence:socket:sock-b')).toBe(0);
  });

  it('disconnecting an unknown or already-removed socket is not a transition', async () => {
    await service.connect('u1', 'sock-a');
    await service.disconnect('u1', 'sock-a');

    expect(await service.disconnect('u1', 'sock-a')).toBe(false);
    expect(await service.disconnect('never-seen', 'sock-x')).toBe(false);
  });

  it('tracks users independently', async () => {
    await service.connect('u1', 'sock-a');
    await service.connect('u2', 'sock-b');
    await service.disconnect('u1', 'sock-a');

    expect(await service.isOnline('u1')).toBe(false);
    expect(await service.isOnline('u2')).toBe(true);
  });

  it('ignores an orphaned socketId (no heartbeat key) so the user is not "online" forever', async () => {
    await redis.sadd('presence:user:u1', 'ghost-socket'); // instance crashed, key never removed

    expect(await service.isOnline('u1')).toBe(false);
    expect(await redis.scard('presence:user:u1')).toBe(0); // lazily pruned
  });

  it('treats a user with only orphaned sockets as offline when a new socket connects', async () => {
    await redis.sadd('presence:user:u1', 'ghost-socket');

    expect(await service.connect('u1', 'sock-a')).toBe(true);
    expect(await redis.smembers('presence:user:u1')).toEqual(['sock-a']);
  });

  it('expires the heartbeat so a silent socket stops counting as online', async () => {
    await service.connect('u1', 'sock-a');
    await sleep(1200); // heartbeat TTL is 1s and nobody refreshes it

    expect(await service.isOnline('u1')).toBe(false);
  });

  it('touch keeps a socket alive past the original TTL', async () => {
    await service.connect('u1', 'sock-a');
    await sleep(700);
    await service.touch('u1', 'sock-a');
    await sleep(700); // 1.4s since connect, but only 0.7s since touch

    expect(await service.isOnline('u1')).toBe(true);
  });

  it('touch re-registers a socket whose heartbeat expired but is still connected', async () => {
    await service.connect('u1', 'sock-a');
    await sleep(1200);
    await service.touch('u1', 'sock-a');

    expect(await service.isOnline('u1')).toBe(true);
  });

  it('puts a TTL on the user Set so a crashed instance cannot leak it forever', async () => {
    await service.connect('u1', 'sock-a');
    const ttl = await redis.ttl('presence:user:u1');
    expect(ttl).toBeGreaterThan(0);
    expect(ttl).toBeLessThanOrEqual(3);
  });

  it('onlineAmong returns only online users and handles an empty list', async () => {
    await service.connect('u1', 'sock-a');
    await service.connect('u3', 'sock-c');

    expect((await service.onlineAmong(['u1', 'u2', 'u3'])).sort()).toEqual(['u1', 'u3']);
    expect(await service.onlineAmong([])).toEqual([]);
  });
});
