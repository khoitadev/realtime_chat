import { createHarness, Harness, makeEvent, waitFor } from '../../test/harness';
import { SinkResponse } from '../../test/webhook-sink';

describe('NotificationConsumer', () => {
  let h: Harness;

  afterEach(async () => {
    await h.close();
  });

  const key = (e: string, r: string) => `notif:${e}:${r}`;
  const requestsFor = (recipient: string) =>
    h.sink.requests.filter((r) => r.body.recipientId === recipient);

  it('delivers exactly once to every recipient with idempotency and correlation headers', async () => {
    h = await createHarness();
    const event = makeEvent({ recipientIds: ['u1', 'u2'], correlationId: 'corr-42' });

    await h.publish(event);

    await waitFor(() => h.sink.requests.length === 2);
    expect(h.sink.requests.map((r) => r.body.recipientId).sort()).toEqual(['u1', 'u2']);
    expect(h.sink.requests[0].headers['idempotency-key']).toBe(`${event.eventId}:u1`);
    expect(h.sink.requests[0].headers['x-correlation-id']).toBe('corr-42');
    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u2'))) === 'done');
    expect(await h.redis.get(key(event.eventId, 'u1'))).toBe('done');
    const counts = await h.counts();
    expect(Object.values(counts).every((n) => n === 0)).toBe(true);
  });

  it('does not call the webhook again when the same event is redelivered', async () => {
    h = await createHarness();
    const event = makeEvent({ recipientIds: ['u1'] });
    await h.publish(event);
    await waitFor(() => h.sink.requests.length === 1);
    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u1'))) === 'done');

    await h.publish(event); // broker redelivery / duplicate publish with the same eventId
    await new Promise((r) => setTimeout(r, 500));

    expect(h.sink.requests).toHaveLength(1);
    expect(Object.values(await h.counts()).every((n) => n === 0)).toBe(true);
  });

  it('retries through the TTL tiers until the webhook recovers, then stops', async () => {
    h = await createHarness();
    h.sink.setResponder((req) => ({ status: req.n <= 2 ? 500 : 200 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(() => h.sink.requests.length === 3);
    const [a, b, c] = h.sink.requests;
    expect(b.at - a.at).toBeGreaterThanOrEqual(150); // tier 1 TTL = 200ms
    expect(c.at - b.at).toBeGreaterThanOrEqual(330); // tier 2 TTL = 400ms
    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u1'))) === 'done');
    await new Promise((r) => setTimeout(r, 900));
    expect(h.sink.requests).toHaveLength(3);
    expect(Object.values(await h.counts()).every((n) => n === 0)).toBe(true);
  });

  it('parks the event in the DLQ after the retry tiers are exhausted and releases the key', async () => {
    h = await createHarness();
    h.sink.setResponder(() => ({ status: 500 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(async () => (await h.counts())['notifications.dlq'] === 1, 10000);
    expect(h.sink.requests).toHaveLength(4); // first delivery + 3 retries
    const [parked] = await h.dlqMessages();
    expect(parked.properties.headers?.['x-reason']).toBe('retries-exhausted');
    expect(JSON.parse(parked.content.toString()).eventId).toBe(event.eventId);
    expect(await h.redis.get(key(event.eventId, 'u1'))).toBeNull(); // not burned
  });

  it('does not burn the idempotency key while a failed delivery waits in a retry queue', async () => {
    h = await createHarness({ retryTtlMs: [5000, 5000, 5000] });
    h.sink.setResponder(() => ({ status: 500 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(async () => (await h.counts())['notifications.retry.1'] === 1);
    expect(await h.redis.get(key(event.eventId, 'u1'))).toBeNull();
  });

  it('after a partial failure only the failed recipient is re-sent', async () => {
    h = await createHarness();
    let u2Calls = 0;
    h.sink.setResponder((req) => {
      if (req.body.recipientId === 'u2') {
        u2Calls++;
        return { status: u2Calls <= 2 ? 500 : 200 };
      }
      return { status: 200 };
    });
    const event = makeEvent({ recipientIds: ['u1', 'u2', 'u3'] });

    await h.publish(event);

    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u2'))) === 'done', 10000);
    expect(requestsFor('u1')).toHaveLength(1);
    expect(requestsFor('u3')).toHaveLength(1);
    expect(requestsFor('u2')).toHaveLength(3);
  });

  it('a busy claim (another worker in flight) goes to retry instead of calling the webhook', async () => {
    h = await createHarness();
    const event = makeEvent({ recipientIds: ['u1'] });
    await h.redis.set(key(event.eventId, 'u1'), 'processing', 'PX', 500);

    await h.publish(event);

    await new Promise((r) => setTimeout(r, 150));
    expect(h.sink.requests).toHaveLength(0);
    await waitFor(() => h.sink.requests.length === 1, 5000); // claim expired, retry delivers once
    expect(requestsFor('u1')).toHaveLength(1);
  });

  it('a permanent 4xx is rejected without retrying', async () => {
    h = await createHarness();
    h.sink.setResponder(() => ({ status: 400 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u1'))) === 'rejected');
    await new Promise((r) => setTimeout(r, 900));
    expect(h.sink.requests).toHaveLength(1);
    expect(Object.values(await h.counts()).every((n) => n === 0)).toBe(true);
  });

  it('429 is retried', async () => {
    h = await createHarness();
    h.sink.setResponder((req) => ({ status: req.n === 1 ? 429 : 200 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(() => h.sink.requests.length === 2);
    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u1'))) === 'done');
  });

  it('a hanging webhook is treated as a failure and retried', async () => {
    h = await createHarness({ webhookTimeoutMs: 150 });
    h.sink.setResponder((req) => (req.n === 1 ? { status: 200, delayMs: 700 } : { status: 200 }));
    const event = makeEvent({ recipientIds: ['u1'] });

    await h.publish(event);

    await waitFor(async () => (await h.redis.get(key(event.eventId, 'u1'))) === 'done', 8000);
    expect(h.sink.requests.length).toBeGreaterThanOrEqual(2);
  });

  it('sends malformed events straight to the DLQ without retrying or calling the webhook', async () => {
    h = await createHarness();

    await h.publishRaw(Buffer.from('not json'));
    await h.publishRaw(Buffer.from(JSON.stringify({ ...makeEvent(), version: 2 })));
    await h.publishRaw(Buffer.from(JSON.stringify({ type: 'message.created', version: 1 })));

    await waitFor(async () => (await h.counts())['notifications.dlq'] === 3);
    const reasons = (await h.dlqMessages()).map((m) => m.properties.headers?.['x-reason']).sort();
    expect(reasons).toEqual(['invalid-json', 'invalid-schema', 'unsupported-version']);
    expect(h.sink.requests).toHaveLength(0);
    const counts = await h.counts();
    expect(counts['notifications.retry.1']).toBe(0);
  });

  it('acknowledges an event with no recipients without calling the webhook', async () => {
    h = await createHarness();

    await h.publish(makeEvent({ recipientIds: [] }));

    await new Promise((r) => setTimeout(r, 400));
    expect(h.sink.requests).toHaveLength(0);
    expect(Object.values(await h.counts()).every((n) => n === 0)).toBe(true);
  });

  it('prefetch=1: handles events one at a time, in publish order', async () => {
    h = await createHarness();
    h.sink.setResponder(() => ({ status: 200, delayMs: 100 } as SinkResponse));
    const events = Array.from({ length: 5 }, (_, i) => makeEvent({ recipientIds: ['u1'], preview: `m${i}` }));

    for (const e of events) await h.publish(e);

    await waitFor(() => h.sink.requests.length === 5, 10000);
    expect(h.sink.maxConcurrent()).toBe(1);
    expect(h.sink.requests.map((r) => r.body.eventId)).toEqual(events.map((e) => e.eventId));
  });
});
