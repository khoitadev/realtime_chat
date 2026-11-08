import { Global, Inject, Module, OnApplicationShutdown } from '@nestjs/common';
import Redis from 'ioredis';
import { CONFIG, NotificationConfig } from '../config';

export const REDIS = Symbol('REDIS');

@Global()
@Module({
  providers: [
    {
      provide: REDIS,
      inject: [CONFIG],
      useFactory: (config: NotificationConfig): Redis =>
        new Redis(config.redisUrl, { maxRetriesPerRequest: 3 }),
    },
  ],
  exports: [REDIS],
})
export class RedisModule implements OnApplicationShutdown {
  constructor(@Inject(REDIS) private readonly redis: Redis) {}

  async onApplicationShutdown(): Promise<void> {
    if (this.redis.status === 'end') return;
    const ended = new Promise<void>((resolve) => this.redis.once('end', () => resolve()));
    await this.redis.quit();
    // quit() resolves on +OK, before the socket is actually closed.
    await ended;
  }
}
