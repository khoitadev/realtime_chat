import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { CONFIG, NotificationConfig } from '../config';
import { REDIS } from '../redis/redis.module';

export type ClaimResult = 'claimed' | 'done' | 'busy';

interface IdempotencyRedis extends Redis {
  idemComplete(key: string, status: string, ttlSec: number): Promise<number>;
  idemRelease(key: string): Promise<number>;
}

/**
 * Idempotency for the webhook side-effect, per (eventId, recipient):
 *   claim -> side-effect -> complete          (success)
 *   claim -> side-effect fails -> release     (so the retry really re-sends)
 * `complete` is only ever called AFTER the side-effect succeeded.
 */
@Injectable()
export class IdempotencyService implements OnModuleInit {
  private readonly client: IdempotencyRedis;

  constructor(
    @Inject(REDIS) redis: Redis,
    @Inject(CONFIG) private readonly config: NotificationConfig,
  ) {
    this.client = redis as IdempotencyRedis;
  }

  onModuleInit(): void {
    // Never downgrade a final mark; a missing key (expired claim) is still completed.
    this.client.defineCommand('idemComplete', {
      numberOfKeys: 1,
      lua: `local v = redis.call('GET', KEYS[1])
if v == 'done' or v == 'rejected' then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1`,
    });
    // Only a still-processing claim may be released.
    this.client.defineCommand('idemRelease', {
      numberOfKeys: 1,
      lua: `if redis.call('GET', KEYS[1]) == 'processing' then redis.call('DEL', KEYS[1]) return 1 end
return 0`,
    });
  }

  keyFor(eventId: string, recipientId: string): string {
    return `notif:${eventId}:${recipientId}`;
  }

  async claim(eventId: string, recipientId: string): Promise<ClaimResult> {
    const key = this.keyFor(eventId, recipientId);
    const ok = await this.client.set(key, 'processing', 'PX', this.config.processingTtlMs, 'NX');
    if (ok === 'OK') return 'claimed';
    const state = await this.client.get(key);
    if (state === 'done' || state === 'rejected') return 'done';
    return 'busy';
  }

  async complete(eventId: string, recipientId: string, status: 'done' | 'rejected'): Promise<void> {
    await this.client.idemComplete(this.keyFor(eventId, recipientId), status, this.config.doneTtlSec);
  }

  async release(eventId: string, recipientId: string): Promise<void> {
    await this.client.idemRelease(this.keyFor(eventId, recipientId));
  }
}
