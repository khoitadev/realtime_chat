import { Injectable } from '@nestjs/common';
import { OnGatewayInit, WebSocketGateway } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { WsTicketService } from './ws-ticket.service';
import { RoomsService } from '../rooms/rooms.service';

@Injectable()
@WebSocketGateway({ cors: true })
export class EventsGateway implements OnGatewayInit {
  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly roomsService: RoomsService,
  ) {}

  afterInit(server: Server): void {
    server.use((socket: Socket, next: (err?: Error) => void) => {
      const ticket = socket.handshake.auth?.ticket as string | undefined;
      const userId = ticket ? this.wsTicketService.consume(ticket) : null;

      if (!userId) {
        next(new Error('Unauthorized'));
        return;
      }

      socket.data.userId = userId;
      next();
    });
  }
}
