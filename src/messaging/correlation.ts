const VALID = /^[A-Za-z0-9._-]{1,128}$/;

/** Returns the value if it is a safe correlation id, otherwise undefined. */
export function sanitizeCorrelationId(value: unknown): string | undefined {
  return typeof value === 'string' && VALID.test(value) ? value : undefined;
}
