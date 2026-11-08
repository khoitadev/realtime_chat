import * as amqp from 'amqplib';
import { assertTopology, DLQ, EXCHANGE, RETRY_QUEUES, ROUTING_KEY, WORK_QUEUE } from './topology';
import { resetBroker, withChannel } from '../../test/broker';

const url = process.env.AMQP_URL!;

describe('topology', () => {
  beforeEach(async () => {
    await resetBroker(url);
  });

  afterAll(async () => {
    await resetBroker(url);
  });

  it('declares every queue and the exchange, and is idempotent', async () => {
    await withChannel(url, async (ch) => {
      await assertTopology(ch, [200, 400, 600]);
      await assertTopology(ch, [200, 400, 600]);

      for (const q of [WORK_QUEUE, DLQ, ...RETRY_QUEUES]) {
        expect((await ch.checkQueue(q)).queue).toBe(q);
      }
      await ch.checkExchange(EXCHANGE);
    });
  });

  it('routes message.created into the work queue only', async () => {
    await withChannel(url, async (ch) => {
      await assertTopology(ch, [200, 400, 600]);
      ch.publish(EXCHANGE, ROUTING_KEY, Buffer.from('x'), { persistent: true });
      await ch.waitForConfirms();

      expect((await ch.checkQueue(WORK_QUEUE)).messageCount).toBe(1);
      expect((await ch.checkQueue(DLQ)).messageCount).toBe(0);
    });
  });

  it('dead-letters an expired retry message straight back into the work queue (no fan-out to other consumers)', async () => {
    await withChannel(url, async (ch) => {
      await assertTopology(ch, [200, 400, 600]);
      // A second consumer's queue bound to the same routing key (e.g. a future analytics-service).
      const { queue: other } = await ch.assertQueue('', { exclusive: true, autoDelete: true });
      await ch.bindQueue(other, EXCHANGE, ROUTING_KEY);

      ch.sendToQueue(RETRY_QUEUES[0], Buffer.from('retried'), {
        persistent: true,
        headers: { 'x-attempt': 1 },
      });
      await ch.waitForConfirms();
      await new Promise((r) => setTimeout(r, 450));

      const msg = await ch.get(WORK_QUEUE, { noAck: true });
      expect(msg).not.toBe(false);
      expect((msg as amqp.GetMessage).properties.headers?.['x-attempt']).toBe(1);
      expect((await ch.checkQueue(other)).messageCount).toBe(0);
      expect((await ch.checkQueue(RETRY_QUEUES[0])).messageCount).toBe(0);
    });
  });

  it('uses the configured TTL per retry tier', async () => {
    await withChannel(url, async (ch) => {
      await assertTopology(ch, [150, 1200, 1200]);
      ch.sendToQueue(RETRY_QUEUES[0], Buffer.from('fast'), { persistent: true });
      ch.sendToQueue(RETRY_QUEUES[1], Buffer.from('slow'), { persistent: true });
      await ch.waitForConfirms();
      await new Promise((r) => setTimeout(r, 400));

      expect((await ch.checkQueue(WORK_QUEUE)).messageCount).toBe(1); // only tier 1 expired
      expect((await ch.checkQueue(RETRY_QUEUES[1])).messageCount).toBe(1);
    });
  });
});
