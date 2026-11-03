import { Module } from '@nestjs/common';
import { RoomsModule } from '../rooms/rooms.module';
import { WsTicketService } from './ws-ticket.service';
import { EventsGateway } from './events.gateway';
import {
  DEFAULT_PRESENCE_OPTIONS,
  PRESENCE_OPTIONS,
  PresenceService,
} from './presence.service';

@Module({
  imports: [RoomsModule],
  providers: [
    WsTicketService,
    EventsGateway,
    PresenceService,
    { provide: PRESENCE_OPTIONS, useValue: DEFAULT_PRESENCE_OPTIONS },
  ],
  exports: [WsTicketService, PresenceService],
})
export class RealtimeModule {}
