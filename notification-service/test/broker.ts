import * as amqp from 'amqplib';

export const QUEUES = [
  'notifications.work',
  'notifications.retry.1',
  'notifications.retry.2',
  'notifications.retry.3',
  'notifications.dlq',
];

export async function withChannel<T>(
  url: string,
  fn: (ch: amqp.ConfirmChannel) => Promise<T>,
): Promise<T> {
  const connection = await amqp.connect(url);
  try {
    const ch = await connection.createConfirmChannel();
    return await fn(ch);
  } finally {
    await connection.close().catch(() => undefined);
  }
}

/** Deletes all queues and the exchange so topology is re-created from scratch. */
export async function resetBroker(url: string): Promise<void> {
  await withChannel(url, async (ch) => {
    for (const q of QUEUES) {
      await ch.deleteQueue(q).catch(() => undefined);
    }
  });
  // A failed deleteQueue closes its channel, so use a fresh one for the exchange.
  await withChannel(url, async (ch) => {
    await ch.deleteExchange('chat.events').catch(() => undefined);
  });
}

export async function queueCount(url: string, name: string): Promise<number> {
  return withChannel(url, async (ch) => (await ch.checkQueue(name)).messageCount);
}
