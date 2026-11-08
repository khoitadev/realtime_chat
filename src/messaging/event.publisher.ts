import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { MessageCreatedEvent } from './events';

export const MESSAGING_OPTIONS = Symbol('MESSAGING_OPTIONS');

export interface MessagingOptions {
  /** Unset => the publisher is a no-op (e.g. local dev without RabbitMQ). */
  amqpUrl?: string;
  confirmTimeoutMs: number;
  reconnectDelayMs: number;
}

export const EXCHANGE = 'chat.events';
const ROUTING_KEY = 'message.created';

/**
 * Best-effort publisher: confirm channel, persistent messages, errors swallowed (logged and
 * counted) so a broker problem can never fail or delay a user's message send.
 * Known limitation: a crash between the DB commit and the publish loses the event.
 */
@Injectable()
export class EventPublisher implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(EventPublisher.name);
  readonly stats = { published: 0, failed: 0, skipped: 0 };
  private connection?: amqp.ChannelModel;
  private channel?: amqp.ConfirmChannel;
  private closing = false;
  private reconnectTimer?: NodeJS.Timeout;
  private readonly inflight = new Set<Promise<void>>();

  constructor(@Inject(MESSAGING_OPTIONS) private readonly options: MessagingOptions) {}

  async onModuleInit(): Promise<void> {
    if (!this.options.amqpUrl) {
      this.logger.warn('AMQP_URL is not set: event publishing is disabled');
      return;
    }
    await this.connect();
  }

  isConnected(): boolean {
    return this.channel !== undefined;
  }

  /** Never rejects. */
  publishMessageCreated(event: MessageCreatedEvent): Promise<void> {
    const promise = this.doPublish(event).finally(() => this.inflight.delete(promise));
    this.inflight.add(promise);
    return promise;
  }

  async onApplicationShutdown(): Promise<void> {
    this.closing = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    await Promise.allSettled([...this.inflight]);
    const { channel, connection } = this;
    this.channel = undefined;
    this.connection = undefined;
    await channel?.close().catch(() => undefined);
    await connection?.close().catch(() => undefined);
  }

  private async doPublish(event: MessageCreatedEvent): Promise<void> {
    const channel = this.channel;
    if (!channel) {
      this.stats.skipped++;
      if (this.options.amqpUrl) {
        this.logger.warn(`[${event.correlationId}] broker unavailable; skipped event ${event.eventId}`);
      }
      return;
    }

    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(
          () => reject(new Error(`no broker confirm within ${this.options.confirmTimeoutMs}ms`)),
          this.options.confirmTimeoutMs,
        );
        channel.publish(
          EXCHANGE,
          ROUTING_KEY,
          Buffer.from(JSON.stringify(event)),
          {
            persistent: true,
            contentType: 'application/json',
            messageId: event.eventId,
            headers: { 'x-correlation-id': event.correlationId },
          },
          (err) => {
            clearTimeout(timer);
            if (err) reject(err);
            else resolve();
          },
        );
      });
      this.stats.published++;
    } catch (err) {
      this.stats.failed++;
      this.logger.warn(
        `[${event.correlationId}] publish of ${event.eventId} failed: ${(err as Error).message}`,
      );
    }
  }

  private async connect(): Promise<void> {
    if (this.closing || !this.options.amqpUrl) return;
    let connection: amqp.ChannelModel | undefined;
    try {
      connection = await amqp.connect(this.options.amqpUrl);
      const conn = connection;
      conn.on('error', (err: Error) => this.logger.warn(`AMQP connection error: ${err.message}`));
      conn.on('close', () => this.onClosed(conn));
      const channel = await conn.createConfirmChannel();
      channel.on('error', (err: Error) => this.logger.warn(`AMQP channel error: ${err.message}`));
      channel.on('close', () => {
        conn.close().catch(() => undefined);
      });
      await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
      this.connection = conn;
      this.channel = channel;
      this.logger.log('AMQP publisher ready');
    } catch (err) {
      this.logger.warn(`AMQP connect failed: ${(err as Error).message}`);
      if (connection) await connection.close().catch(() => undefined);
      this.channel = undefined;
      this.connection = undefined;
      this.scheduleReconnect();
    }
  }

  private onClosed(connection: amqp.ChannelModel): void {
    if (this.connection === connection) {
      this.connection = undefined;
      this.channel = undefined;
    }
    if (!this.closing) this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (this.closing || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connect();
    }, this.options.reconnectDelayMs);
  }
}
