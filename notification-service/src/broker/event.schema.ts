export interface MessageCreatedEvent {
  eventId: string;
  type: 'message.created';
  version: 1;
  occurredAt: string;
  correlationId: string;
  data: {
    messageId: string;
    roomId: string;
    senderId: string;
    preview: string;
    recipientIds: string[];
  };
}

export type ParseResult =
  | { ok: true; event: MessageCreatedEvent }
  | { ok: false; reason: 'invalid-json' | 'invalid-schema' | 'unsupported-version' };

const isNonEmptyString = (v: unknown): v is string => typeof v === 'string' && v.length > 0;
// Same alphabet the producers accept; keeps header/log injection out of everything downstream.
const isCorrelationId = (v: unknown): v is string =>
  typeof v === 'string' && /^[A-Za-z0-9._-]{1,128}$/.test(v);

export function parseEvent(content: Buffer): ParseResult {
  let raw: unknown;
  try {
    raw = JSON.parse(content.toString('utf8'));
  } catch {
    return { ok: false, reason: 'invalid-json' };
  }

  if (typeof raw !== 'object' || raw === null) {
    return { ok: false, reason: 'invalid-schema' };
  }
  const e = raw as Record<string, unknown>;

  if (e.type !== 'message.created') {
    return { ok: false, reason: 'invalid-schema' };
  }
  if (e.version !== 1) {
    return { ok: false, reason: typeof e.version === 'number' ? 'unsupported-version' : 'invalid-schema' };
  }

  const data = e.data as Record<string, unknown> | undefined;
  const valid =
    isNonEmptyString(e.eventId) &&
    typeof e.occurredAt === 'string' &&
    isCorrelationId(e.correlationId) &&
    typeof data === 'object' &&
    data !== null &&
    isNonEmptyString(data.messageId) &&
    isNonEmptyString(data.roomId) &&
    isNonEmptyString(data.senderId) &&
    typeof data.preview === 'string' &&
    Array.isArray(data.recipientIds) &&
    data.recipientIds.every(isNonEmptyString);

  if (!valid) {
    return { ok: false, reason: 'invalid-schema' };
  }
  return { ok: true, event: raw as MessageCreatedEvent };
}
