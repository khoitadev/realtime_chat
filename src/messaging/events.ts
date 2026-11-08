import { randomUUID } from 'crypto';

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

const PREVIEW_CHARS = 100;

export function buildMessageCreatedEvent(input: {
  message: { id: string; roomId: string; senderId: string; content: string };
  recipientIds: string[];
  correlationId: string;
}): MessageCreatedEvent {
  const { message, recipientIds, correlationId } = input;
  return {
    eventId: randomUUID(),
    type: 'message.created',
    version: 1,
    occurredAt: new Date().toISOString(),
    correlationId,
    data: {
      messageId: message.id,
      roomId: message.roomId,
      senderId: message.senderId,
      // Code points, not UTF-16 units: never cut a surrogate pair in half.
      preview: Array.from(message.content).slice(0, PREVIEW_CHARS).join(''),
      recipientIds,
    },
  };
}
