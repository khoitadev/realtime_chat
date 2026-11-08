import { resolveCorrelationId } from './correlation';

describe('resolveCorrelationId', () => {
  it('keeps a valid id', () => {
    expect(resolveCorrelationId('req-1.a_b')).toBe('req-1.a_b');
  });

  it.each([undefined, null, '', 'has space', 'bad;char', 'A'.repeat(129), 42, ['x']])(
    'replaces %p with a fresh uuid',
    (v) => {
      expect(resolveCorrelationId(v)).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);
    },
  );

  it('generates a different id each time', () => {
    expect(resolveCorrelationId(undefined)).not.toBe(resolveCorrelationId(undefined));
  });
});
