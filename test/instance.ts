import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { AppModule } from '../src/app.module';
import { REDIS } from '../src/redis/redis.constants';
import { RedisIoAdapter } from '../src/redis/redis-io.adapter';
import {
  DEFAULT_PRESENCE_OPTIONS,
  PRESENCE_OPTIONS,
  PresenceOptions,
} from '../src/realtime/presence.service';

export interface TestInstance {
  app: INestApplication;
  url: string;
  redis: Redis;
  get<T>(token: unknown): T;
  close(): Promise<void>;
}

/** Boots a full app instance (real Postgres + real Redis + Redis adapter) on a random port. */
export async function createInstance(presence: Partial<PresenceOptions> = {}): Promise<TestInstance> {
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
    .overrideProvider(PRESENCE_OPTIONS)
    .useValue({ ...DEFAULT_PRESENCE_OPTIONS, ...presence })
    .compile();

  const app = moduleRef.createNestApplication();
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  const redis = app.get<Redis>(REDIS);
  app.useWebSocketAdapter(new RedisIoAdapter(app, redis));
  await app.listen(0);

  const port = (app.getHttpServer().address() as AddressInfo).port;
  return {
    app,
    url: `http://localhost:${port}`,
    redis,
    get: <T>(token: unknown) => app.get<T>(token as never),
    close: () => app.close(),
  };
}
