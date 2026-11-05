import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS } from '../redis/redis.constants';

const PRUNE_LUA = `
local function prune(userKey)
  local members = redis.call('SMEMBERS', userKey)
  for _, id in ipairs(members) do
    if redis.call('EXISTS', 'presence:socket:' .. id) == 0 then
      redis.call('SREM', userKey, id)
    end
  end
  return redis.call('SCARD', userKey)
end
`;

interface PresenceRedis extends Redis {
  presenceIsOnline(userKey: string): Promise<number>;
}

const userKey = (userId: string) => `presence:user:${userId}`;

@Injectable()
export class PresenceService implements OnModuleInit {
  private readonly client: PresenceRedis;

  constructor(@Inject(REDIS) redis: Redis) {
    this.client = redis as PresenceRedis;
  }

  onModuleInit(): void {
    this.client.defineCommand('presenceIsOnline', {
      numberOfKeys: 1,
      lua: `${PRUNE_LUA}
if prune(KEYS[1]) > 0 then return 1 else return 0 end`,
    });
  }

  async isOnline(userId: string): Promise<boolean> {
    return (await this.client.presenceIsOnline(userKey(userId))) === 1;
  }

  /** Returns the subset of userIds that are currently online. */
  async onlineAmong(userIds: string[]): Promise<string[]> {
    if (userIds.length === 0) return [];
    const pipeline = this.client.pipeline();
    for (const id of userIds) {
      (pipeline as unknown as PresenceRedis).presenceIsOnline(userKey(id));
    }
    const results = (await pipeline.exec()) ?? [];
    return userIds.filter((_, i) => results[i]?.[1] === 1);
  }
}
