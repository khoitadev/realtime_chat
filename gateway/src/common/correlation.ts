import { randomUUID } from 'crypto';

const VALID = /^[A-Za-z0-9._-]{1,128}$/;

/** Keeps a client-supplied correlation id if it is safe, otherwise generates a fresh one. */
export function resolveCorrelationId(value: unknown): string {
  return typeof value === 'string' && VALID.test(value) ? value : randomUUID();
}
