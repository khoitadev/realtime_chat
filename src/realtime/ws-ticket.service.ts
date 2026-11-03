import { Inject, Injectable } from '@nestjs/common';
import { randomBytes } from 'crypto';
import Redis from 'ioredis';
import { REDIS } from '../redis/redis.constants';

export const WS_TICKET_TTL_SEC = 30;

@Injectable()
export class WsTicketService {
  constructor(@Inject(REDIS) private readonly redis: Redis) {}

  async issue(userId: string, ttlSec = WS_TICKET_TTL_SEC): Promise<string> {
    const ticket = randomBytes(24).toString('hex');
    await this.redis.set(`ws:ticket:${ticket}`, userId, 'EX', ttlSec);
    return ticket;
  }

  /** One-time: GETDEL reads and deletes atomically, so only one caller can win. */
  async consume(ticket: string): Promise<string | null> {
    return this.redis.getdel(`ws:ticket:${ticket}`);
  }
}
