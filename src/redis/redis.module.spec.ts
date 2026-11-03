import { Test } from '@nestjs/testing';
import Redis from 'ioredis';
import { RedisModule } from './redis.module';
import { REDIS } from './redis.constants';

describe('RedisModule', () => {
  it('provides a connected ioredis client', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [RedisModule] }).compile();
    const redis = moduleRef.get<Redis>(REDIS);

    expect(await redis.ping()).toBe('PONG');

    await moduleRef.close();
  });

  it('closes the connection on application shutdown', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [RedisModule] }).compile();
    const redis = moduleRef.get<Redis>(REDIS);
    await redis.ping();

    await moduleRef.close();

    expect(redis.status).toBe('end');
  });
});
