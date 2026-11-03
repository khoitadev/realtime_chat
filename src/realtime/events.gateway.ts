import { HttpException, Inject, Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
  OnGatewayConnection,
  OnGatewayDisconnect,
  OnGatewayInit,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { WsTicketService } from './ws-ticket.service';
import { PRESENCE_OPTIONS, PresenceOptions, PresenceService } from './presence.service';
import { RoomsService } from '../rooms/rooms.service';

interface AckResponse {
  success: boolean;
  error?: string;
}

@Injectable()
@WebSocketGateway({ cors: true, transports: ['websocket'] })
export class EventsGateway
  implements OnGatewayInit, OnGatewayConnection, OnGatewayDisconnect, OnModuleDestroy
{
  private readonly logger = new Logger(EventsGateway.name);
  private heartbeatTimer?: NodeJS.Timeout;

  @WebSocketServer()
  server!: Server;

  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly roomsService: RoomsService,
    private readonly presenceService: PresenceService,
    @Inject(PRESENCE_OPTIONS) private readonly presenceOptions: PresenceOptions,
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

    this.heartbeatTimer = setInterval(
      () => void this.refreshHeartbeats(),
      this.presenceOptions.refreshIntervalMs,
    );
    this.heartbeatTimer.unref();
  }

  /** Refreshes heartbeats of the sockets connected to THIS instance only. */
  private async refreshHeartbeats(): Promise<void> {
    const local = [...this.server.sockets.sockets.values()].filter((s) => s.data.userId);
    const results = await Promise.allSettled(
      local.map((s) => this.presenceService.touch(s.data.userId as string, s.id)),
    );
    for (const r of results) {
      if (r.status === 'rejected') {
        this.logger.error(`presence heartbeat failed: ${(r.reason as Error).message}`);
      }
    }
  }

  /** Graceful shutdown: stop heartbeating and remove this instance's sockets from presence. */
  async onModuleDestroy(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    const local = [...(this.server?.sockets?.sockets.values() ?? [])];
    await Promise.all(local.map((s) => this.handleDisconnect(s)));
  }

  async handleConnection(client: Socket): Promise<void> {
    try {
      const userId = client.data.userId as string;
      const cameOnline = await this.presenceService.connect(userId, client.id);
      if (cameOnline) await this.broadcastPresence(userId, true);
    } catch (err) {
      this.logger.error(`presence connect failed: ${(err as Error).message}`);
    }
  }

  async handleDisconnect(client: Socket): Promise<void> {
    const userId = client.data.userId as string | undefined;
    if (!userId) return;
    try {
      const wentOffline = await this.presenceService.disconnect(userId, client.id);
      if (wentOffline) await this.broadcastPresence(userId, false);
    } catch (err) {
      this.logger.error(`presence disconnect failed: ${(err as Error).message}`);
    }
  }

  /** Tells every room the user belongs to (on any instance, via the adapter). */
  private async broadcastPresence(userId: string, online: boolean): Promise<void> {
    const roomIds = await this.roomsService.listRoomIdsOf(userId);
    if (roomIds.length === 0) return;
    this.server.to(roomIds).emit('presence:update', { userId, online });
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
