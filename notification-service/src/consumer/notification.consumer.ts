import { BeforeApplicationShutdown, Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import * as amqp from 'amqplib';
import { createHash } from 'crypto';
import { CONFIG, NotificationConfig } from '../config';
import { BrokerService } from '../broker/broker.service';
import { MessageCreatedEvent, parseEvent } from '../broker/event.schema';
import { DLQ, RETRY_QUEUES, WORK_QUEUE } from '../broker/topology';
import { IdempotencyService } from '../idempotency/idempotency.service';
import { WebhookClient } from '../delivery/webhook.client';

type RecipientOutcome = 'done' | 'retry';

/** After this many unexpected processing errors for the same message it is parked in the DLQ. */
const MAX_UNEXPECTED_FAILURES = 5;

@Injectable()
export class NotificationConsumer implements OnModuleInit, BeforeApplicationShutdown {
  private readonly logger = new Logger(NotificationConsumer.name);
  private current?: { channel: amqp.ConfirmChannel; tag: string };
  // A Set (not one slot): after a reconnect an old handler may still be running next to a new one.
  private readonly inflight = new Set<Promise<void>>();
  private readonly unexpectedFailures = new Map<string, number>();

  constructor(
    private readonly broker: BrokerService,
    private readonly idempotency: IdempotencyService,
    private readonly webhook: WebhookClient,
    @Inject(CONFIG) private readonly config: NotificationConfig,
  ) {}

  onModuleInit(): void {
    this.broker.onReady((channel) => this.start(channel));
  }

  async beforeApplicationShutdown(): Promise<void> {
    if (this.current) {
      await this.current.channel.cancel(this.current.tag).catch(() => undefined);
      this.current = undefined;
    }
    await Promise.allSettled([...this.inflight]);
  }

  private async start(channel: amqp.ConfirmChannel): Promise<void> {
    // One message at a time: deterministic order for first deliveries, no concurrent webhook calls.
    await channel.prefetch(1);
    const { consumerTag } = await channel.consume(WORK_QUEUE, (msg) => {
      if (!msg) {
        // The broker cancelled this consumer (e.g. the queue was deleted): rebuild everything.
        this.logger.warn('consumer cancelled by the broker; restarting the connection');
        void this.broker.restart();
        return;
      }
      const task = this.handle(channel, msg).finally(() => this.inflight.delete(task));
      this.inflight.add(task);
    });
    this.current = { channel, tag: consumerTag };
  }

  private async handle(channel: amqp.ConfirmChannel, msg: amqp.ConsumeMessage): Promise<void> {
    try {
      const parsed = parseEvent(msg.content);
      if (!parsed.ok) {
        this.logger.warn(`dropping unprocessable event to DLQ: ${parsed.reason}`);
        await this.publishConfirmed(channel, DLQ, msg, { 'x-reason': parsed.reason });
        channel.ack(msg);
        return;
      }

      const { event } = parsed;
      let needsRetry = false;
      for (const recipientId of event.data.recipientIds) {
        if ((await this.deliverTo(event, recipientId)) === 'retry') needsRetry = true;
      }

      if (needsRetry) {
        await this.retryOrPark(channel, msg, event);
      }
      channel.ack(msg);
      this.unexpectedFailures.delete(this.failureKey(msg));
    } catch (err) {
      await this.onUnexpectedFailure(channel, msg, err as Error);
    }
  }

  /**
   * Unexpected failure (Redis/broker hiccup). The message goes back to the broker after a pause,
   * but only a bounded number of times: after MAX_UNEXPECTED_FAILURES it is parked in the DLQ
   * (confirmed publish, then ack) so a persistent fault cannot spin forever at ~1 Hz.
   */
  private async onUnexpectedFailure(
    channel: amqp.ConfirmChannel,
    msg: amqp.ConsumeMessage,
    err: Error,
  ): Promise<void> {
    const id = this.failureKey(msg);
    const failures = (this.unexpectedFailures.get(id) ?? 0) + 1;
    this.logger.error(`unexpected failure #${failures}: ${err.message}`);

    if (failures >= MAX_UNEXPECTED_FAILURES) {
      try {
        await this.publishConfirmed(channel, DLQ, msg, { 'x-reason': 'processing-error' });
        channel.ack(msg);
        this.unexpectedFailures.delete(id);
        this.logger.error(`parked message in the DLQ after ${failures} unexpected failures`);
        return;
      } catch {
        /* DLQ publish failed as well: fall through and requeue */
      }
    }

    this.unexpectedFailures.set(id, failures);
    await new Promise((r) => setTimeout(r, this.config.reconnectDelayMs));
    try {
      channel.nack(msg, false, true);
    } catch {
      /* channel already gone: the broker redelivers on its own */
    }
  }

  private failureKey(msg: amqp.ConsumeMessage): string {
    return createHash('sha1').update(msg.content).digest('hex');
  }

  private async deliverTo(event: MessageCreatedEvent, recipientId: string): Promise<RecipientOutcome> {
    const claim = await this.idempotency.claim(event.eventId, recipientId);
    if (claim === 'done') return 'done';
    if (claim === 'busy') {
      this.logger.log(`[${event.correlationId}] ${recipientId} is busy elsewhere; will retry`);
      return 'retry';
    }

    let outcome: Awaited<ReturnType<WebhookClient['deliver']>>;
    try {
      outcome = await this.webhook.deliver({ event, recipientId });
    } catch (err) {
      await this.idempotency.release(event.eventId, recipientId);
      throw err;
    }

    if (outcome === 'ok') {
      // Mark done only now that the side-effect really happened.
      await this.idempotency.complete(event.eventId, recipientId, 'done');
      this.logger.log(`[${event.correlationId}] notified ${recipientId}`);
      return 'done';
    }
    if (outcome === 'rejected') {
      await this.idempotency.complete(event.eventId, recipientId, 'rejected');
      this.logger.warn(`[${event.correlationId}] provider rejected ${recipientId}; not retrying`);
      return 'done';
    }

    await this.idempotency.release(event.eventId, recipientId); // never burn the key on failure
    return 'retry';
  }

  private async retryOrPark(
    channel: amqp.ConfirmChannel,
    msg: amqp.ConsumeMessage,
    event: MessageCreatedEvent,
  ): Promise<void> {
    const attempt = Number(msg.properties.headers?.['x-attempt'] ?? 0);
    if (attempt < RETRY_QUEUES.length) {
      this.logger.log(`[${event.correlationId}] retry tier ${attempt + 1} for ${event.eventId}`);
      await this.publishConfirmed(channel, RETRY_QUEUES[attempt], msg, { 'x-attempt': attempt + 1 });
    } else {
      this.logger.error(`[${event.correlationId}] parking ${event.eventId} in the DLQ`);
      await this.publishConfirmed(channel, DLQ, msg, { 'x-reason': 'retries-exhausted' });
    }
  }

  /** Publishes a copy of the message and waits for the broker confirm (before the caller acks). */
  private publishConfirmed(
    channel: amqp.ConfirmChannel,
    queue: string,
    msg: amqp.ConsumeMessage,
    extraHeaders: Record<string, unknown>,
  ): Promise<void> {
    const { 'x-death': _death, ...headers } = (msg.properties.headers ?? {}) as Record<string, unknown>;
    return new Promise<void>((resolve, reject) => {
      channel.sendToQueue(
        queue,
        msg.content,
        {
          persistent: true,
          contentType: 'application/json',
          headers: { ...headers, ...extraHeaders },
        },
        (err) => (err ? reject(err) : resolve()),
      );
    });
  }
}
