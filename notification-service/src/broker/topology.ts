import { Channel } from 'amqplib';

export const EXCHANGE = 'chat.events';
export const ROUTING_KEY = 'message.created';
export const WORK_QUEUE = 'notifications.work';
export const DLQ = 'notifications.dlq';
export const RETRY_QUEUES = [
  'notifications.retry.1',
  'notifications.retry.2',
  'notifications.retry.3',
] as const;

/**
 * Idempotent topology declaration. Retry queues have a fixed per-queue TTL and dead-letter
 * through the DEFAULT exchange straight back into the work queue, so other consumers bound
 * to chat.events never receive a retried duplicate.
 */
export async function assertTopology(ch: Channel, retryTtlMs: readonly number[]): Promise<void> {
  await ch.assertExchange(EXCHANGE, 'topic', { durable: true });
  await ch.assertQueue(WORK_QUEUE, { durable: true });
  await ch.bindQueue(WORK_QUEUE, EXCHANGE, ROUTING_KEY);
  await ch.assertQueue(DLQ, { durable: true });
  for (let i = 0; i < RETRY_QUEUES.length; i++) {
    await ch.assertQueue(RETRY_QUEUES[i], {
      durable: true,
      arguments: {
        'x-message-ttl': retryTtlMs[i],
        'x-dead-letter-exchange': '',
        'x-dead-letter-routing-key': WORK_QUEUE,
      },
    });
  }
}
