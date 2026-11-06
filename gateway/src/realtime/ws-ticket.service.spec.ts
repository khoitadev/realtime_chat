import Redis from 'ioredis';
import { WsTicketService } from './ws-ticket.service';
import { cleanRedis } from '../../test/redis-cleanup';
import { sleep } from '../../test/helpers';

describe('WsTicketService (Redis)', () => {
  let redis: Redis;
  let service: WsTicketService;

  beforeAll(() => {
    redis = new Redis(process.env.REDIS_URL!);
    service = new WsTicketService(redis);
  });

  afterAll(async () => {
    await redis.quit();
  });

  beforeEach(async () => {
    await cleanRedis(redis);
  });

  it('issues a different ticket each time', async () => {
    const a = await service.issue('user-1');
    const b = await service.issue('user-1');
    expect(a).not.toBe(b);
  });

  it('consumes a freshly issued ticket and returns the userId', async () => {
    const ticket = await service.issue('user-1');
    expect(await service.consume(ticket)).toBe('user-1');
  });

  it('returns null when consuming the same ticket twice (one-time use)', async () => {
    const ticket = await service.issue('user-1');
    await service.consume(ticket);
    expect(await service.consume(ticket)).toBeNull();
  });

  it('returns null for an unknown ticket', async () => {
    expect(await service.consume('does-not-exist')).toBeNull();
  });

  it('returns null for an expired ticket (Redis TTL)', async () => {
    const ticket = await service.issue('user-1', 1);
    await sleep(1200);
    expect(await service.consume(ticket)).toBeNull();
  });

  it('sets a 30 second TTL by default', async () => {
    const ticket = await service.issue('user-1');
    const ttl = await redis.ttl(`ws:ticket:${ticket}`);
    expect(ttl).toBeGreaterThan(25);
    expect(ttl).toBeLessThanOrEqual(30);
  });

  it('lets exactly one of two concurrent consumers win', async () => {
    const ticket = await service.issue('user-1');
    const results = await Promise.all([service.consume(ticket), service.consume(ticket)]);
    expect(results.filter((r) => r === 'user-1')).toHaveLength(1);
    expect(results.filter((r) => r === null)).toHaveLength(1);
  });
});
