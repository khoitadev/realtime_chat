import { Inject, Injectable, Logger, OnModuleDestroy } from '@nestjs/common';
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
import { PRESENCE_OPTIONS, PresenceOptions, PresenceService } from './presence.service';
import { InternalChatClient } from './internal-chat.client';

interface AckResponse {
  success: boolean;
  error?: string;
}

@Injectable()
@WebSocketGateway({ cors: true, transports: ['websocket'] })
export class EventsGateway implements OnGatewayInit, OnModuleDestroy {
  private readonly logger = new Logger(EventsGateway.name);
  private heartbeatTimer?: NodeJS.Timeout;

  @WebSocketServer()
  server!: Server;

  constructor(
    private readonly wsTicketService: WsTicketService,
    private readonly presenceService: PresenceService,
    private readonly internalChatClient: InternalChatClient,
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

    server.on('connection', (socket: Socket) => void this.handleConnection(socket));
    server.on('connection', (socket: Socket) =>
      socket.on('disconnect', () => void this.handleDisconnect(socket)),
    );

    this.heartbeatTimer = setInterval(
      () => void this.refreshHeartbeats(),
      this.presenceOptions.refreshIntervalMs,
    );
    this.heartbeatTimer.unref();
  }

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

  async onModuleDestroy(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    const local = [...(this.server?.sockets?.sockets.values() ?? [])];
    await Promise.all(local.map((s) => this.handleDisconnect(s)));
  }

  private async handleConnection(client: Socket): Promise<void> {
    try {
      const userId = client.data.userId as string;
      const cameOnline = await this.presenceService.connect(userId, client.id);
      if (cameOnline) await this.broadcastPresence(userId, true);
    } catch (err) {
      this.logger.error(`presence connect failed: ${(err as Error).message}`);
    }
  }

  private async handleDisconnect(client: Socket): Promise<void> {
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
    const roomIds = await this.internalChatClient.listRoomIds(userId);
    if (roomIds.length === 0) return;
    this.server.to(roomIds).emit('presence:update', { userId, online });
  }

  @SubscribeMessage('room:subscribe')
  async handleSubscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: { roomId: string },
  ): Promise<AckResponse> {
    const isMember = await this.internalChatClient.checkMembership(body.roomId, client.data.userId);
    if (!isMember) {
      return { success: false, error: 'Not a member of this room' };
    }
    await client.join(body.roomId);
    return { success: true };
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
