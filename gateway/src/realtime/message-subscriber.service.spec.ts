import { randomUUID } from 'crypto';
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { RedisModule } from '../redis/redis.module';
import { REDIS } from '../redis/redis.constants';
import { RedisIoAdapter } from '../redis/redis-io.adapter';
import { EventsGateway } from './events.gateway';
import { WsTicketService } from './ws-ticket.service';
import { PRESENCE_OPTIONS, DEFAULT_PRESENCE_OPTIONS, PresenceService } from './presence.service';
import { InternalChatClient } from './internal-chat.client';
import { MessageSubscriberService } from './message-subscriber.service';
import { startChatStub, ChatStub } from '../../test/chat-stub';

describe('MessageSubscriberService', () => {
  let app: INestApplication;
  let redis: Redis;
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
        MessageSubscriberService,
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    redis = app.get<Redis>(REDIS);
    app.useWebSocketAdapter(new RedisIoAdapter(app, redis));
    await app.init();
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
      socket.on('connect_error', reject);
    });
  }

  it('emits message:new to every socket in the room when chat:message:new is published', async () => {
    const userId = randomUUID();
    const roomId = randomUUID();
    stub.setMember(roomId, userId, true);

    const socket = await connectWithTicket(await wsTicketService.issue(userId));
    await new Promise<void>((resolve) => socket.emit('room:subscribe', { roomId }, () => resolve()));

    const received = new Promise((resolve) => socket.once('message:new', resolve));

    const message = {
      id: randomUUID(),
      roomId,
      senderId: randomUUID(),
      content: 'pub/sub hello',
      createdAt: new Date().toISOString(),
    };
    await redis.publish('chat:message:new', JSON.stringify({ roomId, message }));

    expect(await received).toEqual(message);
    socket.disconnect();
  });
});
