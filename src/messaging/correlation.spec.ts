import { sanitizeCorrelationId } from './correlation';

describe('sanitizeCorrelationId', () => {
  it.each(['abc', 'req-123.x_y', 'A'.repeat(128), '550e8400-e29b-41d4-a716-446655440000'])(
    'accepts %s',
    (v) => expect(sanitizeCorrelationId(v)).toBe(v),
  );

  it.each([undefined, null, 42, '', 'has space', 'semi;colon', 'new\nline', 'A'.repeat(129), ['a']])(
    'rejects %p',
    (v) => expect(sanitizeCorrelationId(v)).toBeUndefined(),
  );
});
