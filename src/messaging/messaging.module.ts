import { Global, Module } from '@nestjs/common';
import { EventPublisher, MESSAGING_OPTIONS, MessagingOptions } from './event.publisher';

@Global()
@Module({
  providers: [
    {
      provide: MESSAGING_OPTIONS,
      useFactory: (): MessagingOptions => ({
        amqpUrl: process.env.AMQP_URL || undefined,
        confirmTimeoutMs: 2000,
        reconnectDelayMs: 1000,
      }),
    },
    EventPublisher,
  ],
  exports: [EventPublisher],
})
export class MessagingModule {}
