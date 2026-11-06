import { Inject, Injectable, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS } from '../redis/redis.constants';

export const PRESENCE_OPTIONS = Symbol('PRESENCE_OPTIONS');

export interface PresenceOptions {
  heartbeatTtlSec: number;
  refreshIntervalMs: number;
}

export const DEFAULT_PRESENCE_OPTIONS: PresenceOptions = {
  heartbeatTtlSec: 60,
  refreshIntervalMs: 20_000,
};

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
  presenceConnect(userKey: string, socketId: string, userId: string, ttl: number): Promise<number>;
  presenceDisconnect(userKey: string, socketId: string): Promise<number>;
  presenceTouch(userKey: string, socketId: string, userId: string, ttl: number): Promise<number>;
  presenceIsOnline(userKey: string): Promise<number>;
}

const userKey = (userId: string) => `presence:user:${userId}`;

@Injectable()
export class PresenceService implements OnModuleInit {
  private readonly client: PresenceRedis;

  constructor(
    @Inject(REDIS) redis: Redis,
    @Inject(PRESENCE_OPTIONS) private readonly options: PresenceOptions,
  ) {
    this.client = redis as PresenceRedis;
  }

  onModuleInit(): void {
    this.client.defineCommand('presenceConnect', {
      numberOfKeys: 1,
      lua: `${PRUNE_LUA}
local before = prune(KEYS[1])
redis.call('SADD', KEYS[1], ARGV[1])
redis.call('SET', 'presence:socket:' .. ARGV[1], ARGV[2], 'EX', ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]) * 3)
if before == 0 then return 1 else return 0 end`,
    });
    this.client.defineCommand('presenceDisconnect', {
      numberOfKeys: 1,
      lua: `${PRUNE_LUA}
local removed = redis.call('SREM', KEYS[1], ARGV[1])
redis.call('DEL', 'presence:socket:' .. ARGV[1])
local left = prune(KEYS[1])
if removed == 1 and left == 0 then return 1 else return 0 end`,
    });
    this.client.defineCommand('presenceTouch', {
      numberOfKeys: 1,
      lua: `redis.call('SADD', KEYS[1], ARGV[1])
redis.call('SET', 'presence:socket:' .. ARGV[1], ARGV[2], 'EX', ARGV[3])
redis.call('EXPIRE', KEYS[1], tonumber(ARGV[3]) * 3)
return 1`,
    });
    this.client.defineCommand('presenceIsOnline', {
      numberOfKeys: 1,
      lua: `${PRUNE_LUA}
if prune(KEYS[1]) > 0 then return 1 else return 0 end`,
    });
  }

  async connect(userId: string, socketId: string): Promise<boolean> {
    const r = await this.client.presenceConnect(
      userKey(userId),
      socketId,
      userId,
      this.options.heartbeatTtlSec,
    );
    return r === 1;
  }

  async disconnect(userId: string, socketId: string): Promise<boolean> {
    const r = await this.client.presenceDisconnect(userKey(userId), socketId);
    return r === 1;
  }

  async touch(userId: string, socketId: string): Promise<void> {
    await this.client.presenceTouch(
      userKey(userId),
      socketId,
      userId,
      this.options.heartbeatTtlSec,
    );
  }

  async isOnline(userId: string): Promise<boolean> {
    return (await this.client.presenceIsOnline(userKey(userId))) === 1;
  }

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
