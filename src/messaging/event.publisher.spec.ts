import { execFileSync, spawnSync } from 'child_process';
import { EventPublisher } from './event.publisher';
import { buildMessageCreatedEvent } from './events';
import { amqpUrl, listenForEvents, resetExchange } from '../../test/amqp';
import { waitFor } from '../../test/helpers';

const hasRabbitmqctl = spawnSync('rabbitmqctl', ['--version']).status === 0;

function makeEvent(correlationId = 'corr-1') {
  return buildMessageCreatedEvent({
    message: { id: 'm1', roomId: 'r1', senderId: 's1', content: 'hello' },
    recipientIds: ['u1'],
    correlationId,
  });
}

describe('EventPublisher', () => {
  let publisher: EventPublisher | undefined;

  beforeEach(async () => {
    await resetExchange();
  });

  afterEach(async () => {
    await publisher?.onApplicationShutdown();
    publisher = undefined;
  });

  async function start(options: Partial<ConstructorParameters<typeof EventPublisher>[0]> = {}) {
    publisher = new EventPublisher({
      amqpUrl: amqpUrl(),
      confirmTimeoutMs: 1000,
      reconnectDelayMs: 200,
      ...options,
    });
    await publisher.onModuleInit();
    return publisher;
  }

  it('publishes a persistent message.created with correlation and message ids', async () => {
    const listener = await listenForEvents();
    const p = await start();
    const event = makeEvent('corr-77');

    await p.publishMessageCreated(event);

    await waitFor(() => listener.messages.length === 1);
    const msg = listener.messages[0];
    expect(JSON.parse(msg.content.toString())).toEqual(event);
    expect(msg.properties.deliveryMode).toBe(2); // persistent
    expect(msg.properties.contentType).toBe('application/json');
    expect(msg.properties.messageId).toBe(event.eventId);
    expect(msg.properties.headers?.['x-correlation-id']).toBe('corr-77');
    expect(msg.fields.routingKey).toBe('message.created');
    expect(p.stats).toMatchObject({ published: 1, failed: 0, skipped: 0 });
    await listener.close();
  });

  it('never rejects and counts a skip when the broker is unreachable', async () => {
    const p = await start({ amqpUrl: 'amqp://chat:chat@localhost:5999/chat_test' });

    await expect(p.publishMessageCreated(makeEvent())).resolves.toBeUndefined();

    expect(p.isConnected()).toBe(false);
    expect(p.stats.skipped).toBe(1);
    expect(p.stats.published).toBe(0);
  });

  it('is a silent no-op when AMQP_URL is not configured', async () => {
    const p = await start({ amqpUrl: undefined });

    await expect(p.publishMessageCreated(makeEvent())).resolves.toBeUndefined();

    expect(p.isConnected()).toBe(false);
    expect(p.stats.skipped).toBe(1);
  });

  (hasRabbitmqctl ? it : it.skip)('reconnects after the broker closes all connections', async () => {
    const listener = await listenForEvents();
    const p = await start();
    await p.publishMessageCreated(makeEvent('before'));
    await waitFor(() => listener.messages.length === 1);

    execFileSync('rabbitmqctl', ['close_all_connections', '--vhost', 'chat_test', 'publisher-test']);
    await waitFor(() => !p.isConnected(), 5000).catch(() => undefined);
    await waitFor(() => p.isConnected(), 10000);
    const listener2 = await listenForEvents(); // the old listener's connection was closed too
    await p.publishMessageCreated(makeEvent('after'));

    await waitFor(() => listener2.messages.length === 1);
    expect(listener2.messages[0].properties.headers?.['x-correlation-id']).toBe('after');
    await listener2.close();
  });

  it('waits for in-flight publishes on shutdown', async () => {
    const listener = await listenForEvents();
    const p = await start();
    void p.publishMessageCreated(makeEvent('inflight')); // not awaited

    await p.onApplicationShutdown();

    await waitFor(() => listener.messages.length === 1);
    await listener.close();
  });
});
