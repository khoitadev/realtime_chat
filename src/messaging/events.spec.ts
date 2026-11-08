import { buildMessageCreatedEvent } from './events';

const message = { id: 'm1', roomId: 'r1', senderId: 's1', content: 'hello' };

describe('buildMessageCreatedEvent', () => {
  it('builds a v1 event', () => {
    const e = buildMessageCreatedEvent({ message, recipientIds: ['u1', 'u2'], correlationId: 'c1' });

    expect(e).toMatchObject({
      type: 'message.created',
      version: 1,
      correlationId: 'c1',
      data: { messageId: 'm1', roomId: 'r1', senderId: 's1', preview: 'hello', recipientIds: ['u1', 'u2'] },
    });
    expect(e.eventId).toMatch(/^[0-9a-f-]{36}$/);
    expect(new Date(e.occurredAt).toString()).not.toBe('Invalid Date');
  });

  it('generates a new eventId every time', () => {
    const a = buildMessageCreatedEvent({ message, recipientIds: ['u1'], correlationId: 'c' });
    const b = buildMessageCreatedEvent({ message, recipientIds: ['u1'], correlationId: 'c' });
    expect(a.eventId).not.toBe(b.eventId);
  });

  it('cuts the preview at 100 characters without splitting surrogate pairs', () => {
    const long = '😀'.repeat(150);
    const e = buildMessageCreatedEvent({
      message: { ...message, content: long },
      recipientIds: ['u1'],
      correlationId: 'c',
    });

    expect(Array.from(e.data.preview)).toHaveLength(100);
    expect(e.data.preview).toBe('😀'.repeat(100));
  });
});
