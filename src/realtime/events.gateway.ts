import { Injectable } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayInit,
  SubscribeMessage,
  WebSocketGateway,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { WsTicketService } from './ws-ticket.service';
import { RoomsService } from '../rooms/rooms.service';

interface AckResponse {
  success: boolean;
  error?: string;
}

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

  @SubscribeMessage('room:subscribe')
  async handleSubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string },
  ): Promise<AckResponse> {
    try {
      await this.roomsService.assertMember(client.data.userId, body.roomId);
      await client.join(body.roomId);
      return { success: true };
    } catch (err) {
      return { success: false, error: (err as Error).message };
    }
  }

  @SubscribeMessage('room:unsubscribe')
  async handleUnsubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string },
  ): Promise<AckResponse> {
    await client.leave(body.roomId);
    return { success: true };
  }
}
