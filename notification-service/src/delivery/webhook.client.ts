import { Inject, Injectable, Logger } from '@nestjs/common';
import { CONFIG, NotificationConfig } from '../config';
import { MessageCreatedEvent } from '../broker/event.schema';

export type DeliveryOutcome = 'ok' | 'retry' | 'rejected';

@Injectable()
export class WebhookClient {
  private readonly logger = new Logger(WebhookClient.name);

  constructor(@Inject(CONFIG) private readonly config: NotificationConfig) {}

  async deliver(input: { event: MessageCreatedEvent; recipientId: string }): Promise<DeliveryOutcome> {
    const { event, recipientId } = input;
    try {
      const res = await fetch(this.config.pushWebhookUrl, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `${event.eventId}:${recipientId}`,
          'X-Correlation-Id': event.correlationId,
        },
        body: JSON.stringify({
          eventId: event.eventId,
          type: event.type,
          recipientId,
          correlationId: event.correlationId,
          messageId: event.data.messageId,
          roomId: event.data.roomId,
          senderId: event.data.senderId,
          preview: event.data.preview,
        }),
        signal: AbortSignal.timeout(this.config.webhookTimeoutMs),
      });

      if (res.ok) return 'ok';
      if (res.status === 429 || res.status >= 500) return 'retry';
      return 'rejected';
    } catch (err) {
      this.logger.warn(
        `[${event.correlationId}] webhook call failed for ${recipientId}: ${(err as Error).message}`,
      );
      return 'retry';
    }
  }
}
