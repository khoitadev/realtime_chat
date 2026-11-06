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
import { startChatStub, ChatStub } from '../../test/chat-stub';

describe('EventsGateway', () => {
  let app: INestApplication;
  let wsTicketService: WsTicketService;
  let stub: ChatStub;
  let baseUrl: string;

  beforeAll(async () => {
    stub = await startChatStub();
    process.env.CHAT_SERVICE_URL = stub.url;
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
    await stub.close();
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

  describe('handleConnection', () => {
    it('accepts a connection with a valid ticket', async () => {
      const ticket = await wsTicketService.issue(randomUUID());
      const socket = await connectWithTicket(ticket);
      expect(socket.connected).toBe(true);
      socket.disconnect();
    });

    it('rejects a connection with an unknown ticket', async () => {
      await expect(connectWithTicket('not-a-real-ticket')).rejects.toBeDefined();
    });

    it('rejects a connection when the ticket is reused', async () => {
      const ticket = await wsTicketService.issue(randomUUID());
      const first = await connectWithTicket(ticket);
      first.disconnect();
      await expect(connectWithTicket(ticket)).rejects.toBeDefined();
    });
  });
});
