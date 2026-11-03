import Redis from 'ioredis';

/** Flushes the test Redis database. Refuses to run against db 0. */
export async function cleanRedis(redis: Redis): Promise<void> {
  if ((redis.options.db ?? 0) === 0) {
    throw new Error('Refusing to flush Redis db 0; tests must use REDIS_URL with /1');
  }
  await redis.flushdb();
}
