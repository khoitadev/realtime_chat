import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { HttpModule } from '@nestjs/axios';
import { PassportModule } from '@nestjs/passport';
import { RedisModule } from './redis/redis.module';
import { AuthModule } from './auth/auth.module';
import { ProxyController } from './proxy/proxy.controller';
import { WsTicketController } from './realtime/ws-ticket.controller';
import { WsTicketService } from './realtime/ws-ticket.service';
import { EventsGateway } from './realtime/events.gateway';
import { MessageSubscriberService } from './realtime/message-subscriber.service';
import { InternalChatClient } from './realtime/internal-chat.client';
import {
  DEFAULT_PRESENCE_OPTIONS,
  PRESENCE_OPTIONS,
  PresenceService,
} from './realtime/presence.service';

@Module({
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    RedisModule,
    HttpModule,
    PassportModule,
    AuthModule,
  ],
  controllers: [ProxyController, WsTicketController],
  providers: [
    WsTicketService,
    EventsGateway,
    MessageSubscriberService,
    InternalChatClient,
    PresenceService,
    { provide: PRESENCE_OPTIONS, useValue: DEFAULT_PRESENCE_OPTIONS },
  ],
})
export class AppModule {}
