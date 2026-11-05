import { INestApplication, ValidationPipe } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { acquireJwksServer, releaseJwksServer } from './jwt';
import { AppModule } from '../src/app.module';
import { REDIS } from '../src/redis/redis.constants';

export interface TestInstance {
  app: INestApplication;
  url: string;
  redis: Redis;
  get<T>(token: unknown): T;
  close(): Promise<void>;
}

/** Boots a full app instance (real Postgres + real Redis) on a random port. */
export async function createInstance(): Promise<TestInstance> {
  await acquireJwksServer();
  const moduleRef = await Test.createTestingModule({ imports: [AppModule] }).compile();

  const app = moduleRef.createNestApplication();
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  const redis = app.get<Redis>(REDIS);
  await app.listen(0);

  const port = (app.getHttpServer().address() as AddressInfo).port;
  return {
    app,
    url: `http://localhost:${port}`,
    redis,
    get: <T>(token: unknown) => app.get<T>(token as never),
    close: async () => {
      await app.close();
      await releaseJwksServer();
    },
  };
}
