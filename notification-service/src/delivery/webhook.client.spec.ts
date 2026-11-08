import { loadConfig } from '../config';
import { MessageCreatedEvent } from '../broker/event.schema';
import { WebhookClient } from './webhook.client';
import { startWebhookSink, WebhookSink } from '../../test/webhook-sink';

const event: MessageCreatedEvent = {
  eventId: 'e1',
  type: 'message.created',
  version: 1,
  occurredAt: '2026-11-08T10:00:00.000Z',
  correlationId: 'corr-1',
  data: { messageId: 'm1', roomId: 'r1', senderId: 's1', preview: 'hello', recipientIds: ['u1'] },
};

describe('WebhookClient', () => {
  let sink: WebhookSink;
  let client: WebhookClient;

  beforeEach(async () => {
    sink = await startWebhookSink();
    client = new WebhookClient({
      ...loadConfig({ AMQP_URL: 'x', PUSH_WEBHOOK_URL: sink.url }),
      webhookTimeoutMs: 300,
    });
  });

  afterEach(async () => {
    await sink.close();
  });

  it('POSTs the notification with idempotency and correlation headers', async () => {
    const result = await client.deliver({ event, recipientId: 'u1' });

    expect(result).toBe('ok');
    expect(sink.requests).toHaveLength(1);
    const req = sink.requests[0];
    expect(req.headers['idempotency-key']).toBe('e1:u1');
    expect(req.headers['x-correlation-id']).toBe('corr-1');
    expect(req.headers['content-type']).toContain('application/json');
    expect(req.body).toEqual({
      eventId: 'e1',
      type: 'message.created',
      recipientId: 'u1',
      correlationId: 'corr-1',
      messageId: 'm1',
      roomId: 'r1',
      senderId: 's1',
      preview: 'hello',
    });
  });

  it.each([
    [200, 'ok'],
    [204, 'ok'],
    [429, 'retry'],
    [500, 'retry'],
    [503, 'retry'],
    [400, 'rejected'],
    [404, 'rejected'],
    [422, 'rejected'],
  ])('maps HTTP %i to %s', async (status, expected) => {
    sink.setResponder(() => ({ status }));
    expect(await client.deliver({ event, recipientId: 'u1' })).toBe(expected);
  });

  it('treats a timeout as retry', async () => {
    sink.setResponder(() => ({ status: 200, delayMs: 800 }));
    expect(await client.deliver({ event, recipientId: 'u1' })).toBe('retry');
  });

  it('treats a dropped connection as retry', async () => {
    sink.setResponder(() => ({ destroy: true }));
    expect(await client.deliver({ event, recipientId: 'u1' })).toBe('retry');
  });

  it('treats an unreachable endpoint as retry', async () => {
    await sink.close();
    expect(await client.deliver({ event, recipientId: 'u1' })).toBe('retry');
    sink = await startWebhookSink(); // keep afterEach's close() valid
  });
});
