import { Test } from '@nestjs/testing';
import Redis from 'ioredis';
import { RedisModule } from './redis.module';
import { REDIS } from './redis.constants';

describe('RedisModule', () => {
  it('provides a connected ioredis client and cleans it up on shutdown', async () => {
    const moduleRef = await Test.createTestingModule({ imports: [RedisModule] }).compile();
    const app = moduleRef.createNestApplication();
    app.enableShutdownHooks();
    await app.init();

    const redis = app.get<Redis>(REDIS);
    expect(await redis.ping()).toBe('PONG');

    await app.close();
    expect(redis.status).toBe('end');
  });
});
