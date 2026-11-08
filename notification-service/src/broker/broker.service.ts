import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { CONFIG, NotificationConfig } from '../config';
import { assertTopology } from './topology';

export type ReadyHandler = (channel: amqp.ConfirmChannel) => Promise<void>;

/**
 * Owns the AMQP connection and one confirm channel. Reconnects automatically and, on every
 * (re)connect, re-asserts the topology and re-runs every registered ready handler.
 */
@Injectable()
export class BrokerService implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(BrokerService.name);
  private connection?: amqp.ChannelModel;
  private channel?: amqp.ConfirmChannel;
  private readonly handlers: ReadyHandler[] = [];
  private closing = false;
  private reconnectTimer?: NodeJS.Timeout;

  constructor(@Inject(CONFIG) private readonly config: NotificationConfig) {}

  onReady(handler: ReadyHandler): void {
    this.handlers.push(handler);
    if (this.channel) {
      const channel = this.channel;
      handler(channel).catch((err) => this.logger.error(`ready handler failed: ${err.message}`));
    }
  }

  isReady(): boolean {
    return this.channel !== undefined;
  }

  /** The first attempt is awaited; a failure schedules a retry instead of throwing. */
  async onModuleInit(): Promise<void> {
    await this.connect();
  }

  /** Drops the current connection; the close handler reconnects. */
  async restart(): Promise<void> {
    await this.connection?.close().catch(() => undefined);
  }

  async onApplicationShutdown(): Promise<void> {
    this.closing = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    const { channel, connection } = this;
    this.channel = undefined;
    this.connection = undefined;
    await channel?.close().catch(() => undefined);
    await connection?.close().catch(() => undefined);
  }

  private async connect(): Promise<void> {
    if (this.closing) return;
    let connection: amqp.ChannelModel | undefined;
    try {
      connection = await amqp.connect(this.config.amqpUrl);
      const conn = connection;
      conn.on('error', (err: Error) => this.logger.warn(`AMQP connection error: ${err.message}`));
      conn.on('close', () => this.onClosed(conn));

      const channel = await conn.createConfirmChannel();
      channel.on('error', (err: Error) => this.logger.warn(`AMQP channel error: ${err.message}`));
      channel.on('close', () => {
        // A closed channel (e.g. PRECONDITION_FAILED) is unrecoverable: rebuild everything.
        conn.close().catch(() => undefined);
      });

      await assertTopology(channel, this.config.retryTtlMs);
      this.connection = conn;
      this.channel = channel;
      for (const handler of this.handlers) {
        await handler(channel);
      }
      this.logger.log('AMQP ready');
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
    if (!this.closing) {
      this.logger.warn('AMQP connection closed; reconnecting');
      this.scheduleReconnect();
    }
  }

  private scheduleReconnect(): void {
    if (this.closing || this.reconnectTimer) return;
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connect();
    }, this.config.reconnectDelayMs);
  }
}
