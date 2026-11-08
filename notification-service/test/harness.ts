import * as amqp from 'amqplib';
import { randomUUID } from 'crypto';
import Redis from 'ioredis';
import { Test, TestingModule } from '@nestjs/testing';
import { AppModule } from '../src/app.module';
import { CONFIG, loadConfig, NotificationConfig } from '../src/config';
import { MessageCreatedEvent } from '../src/broker/event.schema';
import { BrokerService } from '../src/broker/broker.service';
import { cleanRedis } from './redis-cleanup';
import { resetBroker, withChannel } from './broker';
import { startWebhookSink, WebhookSink } from './webhook-sink';

export function makeEvent(overrides: Partial<MessageCreatedEvent['data']> & { eventId?: string; correlationId?: string } = {}): MessageCreatedEvent {
  return {
    eventId: overrides.eventId ?? randomUUID(),
    type: 'message.created',
    version: 1,
    occurredAt: new Date().toISOString(),
    correlationId: overrides.correlationId ?? 'corr-test',
    data: {
      messageId: overrides.messageId ?? randomUUID(),
      roomId: overrides.roomId ?? 'room-1',
      senderId: overrides.senderId ?? 'sender-1',
      preview: overrides.preview ?? 'hello',
      recipientIds: overrides.recipientIds ?? ['u1'],
    },
  };
}

export async function waitFor(fn: () => boolean | Promise<boolean>, timeoutMs = 8000, intervalMs = 25): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, intervalMs));
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}

export interface Harness {
  sink: WebhookSink;
  redis: Redis;
  config: NotificationConfig;
  moduleRef: TestingModule;
  publish(event: MessageCreatedEvent): Promise<void>;
  publishRaw(content: Buffer): Promise<void>;
  /** messageCount of every queue. */
  counts(): Promise<Record<string, number>>;
  dlqMessages(): Promise<amqp.GetMessage[]>;
  /** Shuts the app down (graceful) but keeps sink/redis for assertions. */
  closeApp(): Promise<void>;
  close(): Promise<void>;
}

export async function createHarness(overrides: Partial<NotificationConfig> = {}): Promise<Harness> {
  const url = process.env.AMQP_URL!;
  await resetBroker(url);
  const sink = await startWebhookSink();
  const config: NotificationConfig = {
    ...loadConfig({ AMQP_URL: url, REDIS_URL: process.env.REDIS_URL!, PUSH_WEBHOOK_URL: sink.url }),
    retryTtlMs: [200, 400, 600],
    webhookTimeoutMs: 1000,
    reconnectDelayMs: 200,
    ...overrides,
  };

  const moduleRef = await Test.createTestingModule({ imports: [AppModule] })
    .overrideProvider(CONFIG)
    .useValue(config)
    .compile();
  // Independent client for assertions: the app's own client is closed by closeApp().
  const redis = new Redis(config.redisUrl);
  await cleanRedis(redis);
  await moduleRef.init();

  const broker = moduleRef.get(BrokerService);
  if (config.amqpUrl === url) {
    await waitFor(() => broker.isReady());
  }

  let appClosed = false;
  const closeApp = async () => {
    if (appClosed) return;
    appClosed = true;
    await moduleRef.close();
  };

  return {
    sink,
    redis,
    config,
    moduleRef,
    publish: (event) => harnessPublish(url, Buffer.from(JSON.stringify(event))),
    publishRaw: (content) => harnessPublish(url, content),
    counts: async () =>
      withChannel(url, async (ch) => {
        const out: Record<string, number> = {};
        for (const q of ['notifications.work', 'notifications.retry.1', 'notifications.retry.2', 'notifications.retry.3', 'notifications.dlq']) {
          out[q] = (await ch.checkQueue(q)).messageCount;
        }
        return out;
      }),
    dlqMessages: async () =>
      withChannel(url, async (ch) => {
        const out: amqp.GetMessage[] = [];
        for (;;) {
          const m = await ch.get('notifications.dlq', { noAck: true });
          if (!m) break;
          out.push(m);
        }
        return out;
      }),
    closeApp,
    close: async () => {
      await closeApp();
      await redis.quit();
      await sink.close();
      await resetBroker(url);
    },
  };
}

/** Publishes through a short-lived connection (robust against broker restarts in tests). */
async function harnessPublish(url: string, content: Buffer): Promise<void> {
  await withChannel(url, async (ch) => {
    ch.publish('chat.events', 'message.created', content, { persistent: true, contentType: 'application/json' });
    await ch.waitForConfirms();
  });
}
