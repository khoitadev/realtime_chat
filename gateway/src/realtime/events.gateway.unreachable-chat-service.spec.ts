import { randomUUID } from 'crypto';
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { RedisModule } from '../redis/redis.module';
import { EventsGateway } from './events.gateway';
import { WsTicketService } from './ws-ticket.service';
import { PRESENCE_OPTIONS, DEFAULT_PRESENCE_OPTIONS, PresenceService } from './presence.service';
import { InternalChatClient } from './internal-chat.client';
import { startChatStub } from '../../test/chat-stub';

describe('EventsGateway when chat-service is unreachable', () => {
  let app: INestApplication;
  let wsTicketService: WsTicketService;
  let baseUrl: string;

  beforeAll(async () => {
    // Start then immediately close a stub so CHAT_SERVICE_URL points at a
    // real, previously-listening port that now refuses connections.
    const deadStub = await startChatStub();
    await deadStub.close();

    process.env.CHAT_SERVICE_URL = deadStub.url;
    process.env.INTERNAL_SECRET = 'test-internal-secret';

    const moduleRef = await Test.createTestingModule({
      imports: [RedisModule],
      providers: [
        EventsGateway,
        WsTicketService,
        PresenceService,
        { provide: PRESENCE_OPTIONS, useValue: DEFAULT_PRESENCE_OPTIONS },
        InternalChatClient,
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
    wsTicketService = moduleRef.get(WsTicketService);
  });

  afterAll(async () => {
    await app.close();
  });

  function connectWithTicket(ticket: string): Promise<ClientSocket> {
    return new Promise((resolve, reject) => {
      const socket = io(baseUrl, {
        auth: { ticket },
        transports: ['websocket'],
        reconnection: false,
        forceNew: true,
      });
      socket.on('connect', () => resolve(socket));
      socket.on('connect_error', (err) => reject(err));
    });
  }

  function emitWithAck<T>(socket: ClientSocket, event: string, payload: unknown): Promise<T> {
    return new Promise((resolve) => socket.emit(event, payload, resolve));
  }

  it('acks room:subscribe with an error instead of hanging', async () => {
    const socket = await connectWithTicket(await wsTicketService.issue(randomUUID()));

    const ack = await emitWithAck<{ success: boolean; error?: string }>(socket, 'room:subscribe', {
      roomId: randomUUID(),
    });

    expect(ack.success).toBe(false);
    expect(ack.error).toBe('Service unavailable');
    socket.disconnect();
  });

  it('acks message:send with an error instead of hanging', async () => {
    const socket = await connectWithTicket(await wsTicketService.issue(randomUUID()));

    const ack = await emitWithAck<{ success: boolean; error?: string }>(socket, 'message:send', {
      roomId: randomUUID(),
      content: 'hello',
    });

    expect(ack.success).toBe(false);
    expect(ack.error).toBe('Service unavailable');
    socket.disconnect();
  });
});
