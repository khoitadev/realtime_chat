import { HttpException, Injectable } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayInit,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { WsTicketService } from './ws-ticket.service';
import { RoomsService } from '../rooms/rooms.service';

interface AckResponse {
  success: boolean;
  error?: string;
}

@Injectable()
@WebSocketGateway({ cors: true, transports: ['websocket'] })
export class EventsGateway implements OnGatewayInit {
  @WebSocketServer()
  server!: Server;

  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly roomsService: RoomsService,
  ) {}

  afterInit(server: Server): void {
    server.use(async (socket: Socket, next: (err?: Error) => void) => {
      try {
        const ticket = socket.handshake.auth?.ticket as string | undefined;
        const userId = ticket ? await this.wsTicketService.consume(ticket) : null;

        if (!userId) {
          next(new Error('Unauthorized'));
          return;
        }

        socket.data.userId = userId;
        next();
      } catch {
        next(new Error('Unauthorized'));
      }
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

  @SubscribeMessage('message:send')
  async handleMessageSend(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string; content: string },
  ): Promise<AckResponse> {
    try {
      if (!body?.content || typeof body.content !== 'string' || !body.content.trim()) {
        return { success: false, error: 'content must not be empty' };
      }

      const message = await this.roomsService.sendMessage(client.data.userId, body.roomId, {
        content: body.content,
      });
      this.server.to(body.roomId).emit('message:new', message);
      return { success: true };
    } catch (err) {
      if (err instanceof HttpException) {
        return { success: false, error: err.message };
      }
      return { success: false, error: 'Invalid request' };
    }
  }
}
