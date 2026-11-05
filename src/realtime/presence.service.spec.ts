import Redis from 'ioredis';
import { PresenceService } from './presence.service';
import { cleanRedis } from '../../test/redis-cleanup';

describe('PresenceService (read-only, chat-service)', () => {
  let redis: Redis;
  let service: PresenceService;

  beforeAll(() => {
    redis = new Redis(process.env.REDIS_URL!);
    service = new PresenceService(redis);
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

  it('ignores an orphaned socketId (no heartbeat key) so the user is not "online" forever', async () => {
    await redis.sadd('presence:user:u1', 'ghost-socket'); // instance crashed, key never removed
    expect(await service.isOnline('u1')).toBe(false);
    expect(await redis.scard('presence:user:u1')).toBe(0); // lazily pruned
  });

  it('onlineAmong returns only online users and handles an empty list', async () => {
    await redis.sadd('presence:user:u1', 'sock-a');
    await redis.set('presence:socket:sock-a', 'u1', 'EX', 60);
    await redis.sadd('presence:user:u3', 'sock-c');
    await redis.set('presence:socket:sock-c', 'u3', 'EX', 60);

    expect((await service.onlineAmong(['u1', 'u2', 'u3'])).sort()).toEqual(['u1', 'u3']);
    expect(await service.onlineAmong([])).toEqual([]);
  });
});
