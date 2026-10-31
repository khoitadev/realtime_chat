import { Module } from '@nestjs/common';
import { RoomsModule } from '../rooms/rooms.module';
import { WsTicketService } from './ws-ticket.service';
import { EventsGateway } from './events.gateway';

@Module({
  imports: [RoomsModule],
  providers: [WsTicketService, EventsGateway],
  exports: [WsTicketService],
})
export class RealtimeModule {}
