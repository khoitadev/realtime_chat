import { execFileSync, spawnSync } from 'child_process';
import { createHarness, Harness, makeEvent, waitFor } from '../../test/harness';
import { BrokerService } from '../broker/broker.service';
import { withChannel } from '../../test/broker';

const hasRabbitmqctl = spawnSync('rabbitmqctl', ['--version']).status === 0;
const key = (e: string, r: string) => `notif:${e}:${r}`;

describe('notification-service lifecycle', () => {
  let h: Harness;

  afterEach(async () => {
    await h.close();
  });

  it('graceful shutdown finishes the in-flight message, acknowledges it and does not redeliver', async () => {
    h = await createHarness();
    h.sink.setResponder(() => ({ status: 200, delayMs: 500 }));
    const event = makeEvent({ recipientIds: ['u1'] });
    await h.publish(event);
    await waitFor(() => h.sink.requests.length === 1); // the handler is now blocked inside the webhook call

    await h.closeApp();

    expect(await h.redis.get(key(event.eventId, 'u1'))).toBe('done'); // finished before closing
    await new Promise((r) => setTimeout(r, 400));
    expect(h.sink.requests).toHaveLength(1); // not redelivered after shutdown
    const counts = await h.counts();
    expect(counts['notifications.work']).toBe(0);
  });

  (hasRabbitmqctl ? it : it.skip)(
    'recovers after the broker drops every connection and keeps delivering',
    async () => {
      h = await createHarness();
      const first = makeEvent({ recipientIds: ['u1'] });
      await h.publish(first);
      await waitFor(() => h.sink.requests.length === 1);

      execFileSync('rabbitmqctl', ['close_all_connections', '--vhost', 'chat_test', 'lifecycle-test']);
      const broker = h.moduleRef.get(BrokerService);
      await waitFor(() => broker.isReady(), 10000);

      const second = makeEvent({ recipientIds: ['u2'] });
      await h.publish(second);
      await waitFor(() => h.sink.requests.length === 2, 10000);
      expect(h.sink.requests[1].body.recipientId).toBe('u2');
    },
  );

  it('rebuilds itself when the work queue is deleted under the consumer', async () => {
    h = await createHarness();
    await withChannel(process.env.AMQP_URL!, async (ch) => {
      await ch.deleteQueue('notifications.work');
    });
    const broker = h.moduleRef.get(BrokerService);

    // isReady() is still true until the broker's cancel notification arrives: wait for the
    // drop first, otherwise the publish below can hit the window where the queue is gone.
    await waitFor(() => !broker.isReady(), 10000);
    await waitFor(() => broker.isReady(), 10000);
    const event = makeEvent({ recipientIds: ['u1'] });
    await h.publish(event);

    await waitFor(() => h.sink.requests.length === 1, 10000);
  });

  it('starts and shuts down cleanly while the broker is unreachable', async () => {
    h = await createHarness({ amqpUrl: 'amqp://chat:chat@localhost:5999/chat_test' });
    const broker = h.moduleRef.get(BrokerService);

    expect(broker.isReady()).toBe(false);
    await new Promise((r) => setTimeout(r, 500)); // a few reconnect attempts
    expect(broker.isReady()).toBe(false);

    await h.closeApp(); // must not hang or throw
  });
});
