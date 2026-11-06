import { Inject, Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import Redis from 'ioredis';
import { REDIS } from '../redis/redis.constants';
import { EventsGateway } from './events.gateway';

const CHANNEL = 'chat:message:new';

interface ChatMessagePayload {
  roomId: string;
  message: unknown;
}

/**
 * Sole emitter of `message:new`. Chat-service publishes here after persisting
 * (RoomsService.sendMessage), so this also covers REST-only sends with no WS
 * connection involved in the send itself.
 */
@Injectable()
export class MessageSubscriberService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(MessageSubscriberService.name);
  private readonly sub: Redis;

  constructor(
    @Inject(REDIS) redis: Redis,
    private readonly gateway: EventsGateway,
  ) {
    // Subscribe mode locks the connection to pub/sub commands only, so this
    // must never be the shared REDIS client used elsewhere for normal commands.
    this.sub = redis.duplicate();
  }

  async onModuleInit(): Promise<void> {
    await this.sub.subscribe(CHANNEL);
    this.sub.on('message', (_channel: string, raw: string) => {
      try {
        const { roomId, message } = JSON.parse(raw) as ChatMessagePayload;
        this.gateway.server.to(roomId).emit('message:new', message);
      } catch (err) {
        this.logger.error(`failed to handle ${CHANNEL} payload: ${(err as Error).message}`);
      }
    });
  }

  async onModuleDestroy(): Promise<void> {
    await this.sub.unsubscribe(CHANNEL).catch(() => undefined);
    await this.sub.quit().catch(() => undefined);
  }
}
