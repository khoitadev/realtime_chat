import { Module } from '@nestjs/common';
import { ConfigModule } from './config.module';
import { RedisModule } from './redis/redis.module';
import { BrokerService } from './broker/broker.service';
import { IdempotencyService } from './idempotency/idempotency.service';
import { WebhookClient } from './delivery/webhook.client';
import { NotificationConsumer } from './consumer/notification.consumer';

@Module({
  imports: [ConfigModule, RedisModule],
  providers: [BrokerService, IdempotencyService, WebhookClient, NotificationConsumer],
})
export class AppModule {}
