import { parseEvent } from './event.schema';

const valid = {
  eventId: 'e1',
  type: 'message.created',
  version: 1,
  occurredAt: '2026-11-08T10:00:00.000Z',
  correlationId: 'c1',
  data: { messageId: 'm1', roomId: 'r1', senderId: 's1', preview: 'hi', recipientIds: ['u1', 'u2'] },
};

const buf = (o: unknown) => Buffer.from(JSON.stringify(o));

describe('parseEvent', () => {
  it('accepts a valid v1 event', () => {
    const r = parseEvent(buf(valid));
    expect(r).toEqual({ ok: true, event: valid });
  });

  it('accepts an empty recipient list', () => {
    const r = parseEvent(buf({ ...valid, data: { ...valid.data, recipientIds: [] } }));
    expect(r.ok).toBe(true);
  });

  it('rejects non-JSON', () => {
    expect(parseEvent(Buffer.from('not json'))).toEqual({ ok: false, reason: 'invalid-json' });
  });

  it('rejects an unsupported version', () => {
    expect(parseEvent(buf({ ...valid, version: 2 }))).toEqual({
      ok: false,
      reason: 'unsupported-version',
    });
  });

  it.each([
    ['wrong type', { ...valid, type: 'message.deleted' }],
    ['missing eventId', { ...valid, eventId: undefined }],
    ['empty eventId', { ...valid, eventId: '' }],
    ['missing correlationId', { ...valid, correlationId: undefined }],
    ['missing data', { ...valid, data: undefined }],
    ['missing recipientIds', { ...valid, data: { ...valid.data, recipientIds: undefined } }],
    ['non-string recipient', { ...valid, data: { ...valid.data, recipientIds: [1] } }],
    ['missing roomId', { ...valid, data: { ...valid.data, roomId: undefined } }],
    ['non-object payload', 'hello'],
    ['null payload', null],
  ])('rejects %s as invalid-schema', (_name, payload) => {
    expect(parseEvent(buf(payload))).toEqual({ ok: false, reason: 'invalid-schema' });
  });
});
