import * as amqp from 'amqplib';

export const EXCHANGE = 'chat.events';

export function amqpUrl(): string {
  return process.env.AMQP_URL!;
}

/** Deletes the exchange so each spec starts from a clean broker state. */
export async function resetExchange(): Promise<void> {
  const connection = await amqp.connect(amqpUrl());
  const channel = await connection.createChannel();
  await channel.deleteExchange(EXCHANGE).catch(() => undefined);
  await connection.close().catch(() => undefined);
}

export interface EventListener {
  messages: amqp.ConsumeMessage[];
  close(): Promise<void>;
}

/**
 * Binds an exclusive auto-delete queue to chat.events for `message.created` and records
 * everything published (a stand-in for "some consumer").
 */
export async function listenForEvents(): Promise<EventListener> {
  const connection = await amqp.connect(amqpUrl());
  const channel = await connection.createChannel();
  await channel.assertExchange(EXCHANGE, 'topic', { durable: true });
  const { queue } = await channel.assertQueue('', { exclusive: true, autoDelete: true });
  await channel.bindQueue(queue, EXCHANGE, 'message.created');
  const messages: amqp.ConsumeMessage[] = [];
  await channel.consume(
    queue,
    (msg) => {
      if (msg) {
        messages.push(msg);
        channel.ack(msg);
      }
    },
    { noAck: false },
  );
  return {
    messages,
    close: async () => {
      await connection.close().catch(() => undefined);
    },
  };
}
