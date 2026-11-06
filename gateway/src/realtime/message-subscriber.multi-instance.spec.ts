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
import { sleep } from '../../test/helpers';

interface Instance {
  app: INestApplication;
  redis: Redis;
  wsTicketService: WsTicketService;
  url: string;
}

async function boot(): Promise<Instance> {
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

  const app = moduleRef.createNestApplication();
  const redis = app.get<Redis>(REDIS);
  app.useWebSocketAdapter(new RedisIoAdapter(app, redis));
  await app.init();
  await app.listen(0);
  const url = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
  const wsTicketService = moduleRef.get(WsTicketService);
  return { app, redis, wsTicketService, url };
}

function connectWithTicket(url: string, ticket: string): Promise<ClientSocket> {
  return new Promise((resolve, reject) => {
    const socket = io(url, {
      auth: { ticket },
      transports: ['websocket'],
      reconnection: false,
      forceNew: true,
    });
    socket.on('connect', () => resolve(socket));
    socket.on('connect_error', reject);
  });
}

describe('MessageSubscriberService (multi-instance)', () => {
  let stub: ChatStub;
  let instanceA: Instance;
  let instanceB: Instance;

  beforeAll(async () => {
    stub = await startChatStub();
    process.env.CHAT_SERVICE_URL = stub.url;
    process.env.INTERNAL_SECRET = 'test-internal-secret';

    instanceA = await boot();
    instanceB = await boot();
  });

  afterAll(async () => {
    await instanceA.app.close();
    await instanceB.app.close();
    await stub.close();
  });

  it('delivers a single chat:message:new publish to each client exactly once, not once per instance', async () => {
    const roomId = randomUUID();
    const userOnA = randomUUID();
    const userOnB = randomUUID();
    stub.setMember(roomId, userOnA, true);
    stub.setMember(roomId, userOnB, true);

    const socketOnA = await connectWithTicket(instanceA.url, await instanceA.wsTicketService.issue(userOnA));
    const socketOnB = await connectWithTicket(instanceB.url, await instanceB.wsTicketService.issue(userOnB));

    await new Promise<void>((resolve) => socketOnA.emit('room:subscribe', { roomId }, () => resolve()));
    await new Promise<void>((resolve) => socketOnB.emit('room:subscribe', { roomId }, () => resolve()));
    await sleep(100); // let the room join propagate through the Redis adapter

    const receivedOnA: unknown[] = [];
    const receivedOnB: unknown[] = [];
    socketOnA.on('message:new', (msg) => receivedOnA.push(msg));
    socketOnB.on('message:new', (msg) => receivedOnB.push(msg));

    const message = {
      id: randomUUID(),
      roomId,
      senderId: randomUUID(),
      content: 'single delivery please',
      createdAt: new Date().toISOString(),
    };
    // Simulates chat-service publishing once after persisting the message.
    await instanceA.redis.publish('chat:message:new', JSON.stringify({ roomId, message }));

    // Give both instances' subscribers and the Redis adapter time to deliver
    // (and, if buggy, to double-deliver).
    await sleep(300);

    expect(receivedOnA).toHaveLength(1);
    expect(receivedOnA[0]).toEqual(message);
    expect(receivedOnB).toHaveLength(1);
    expect(receivedOnB[0]).toEqual(message);

    socketOnA.disconnect();
    socketOnB.disconnect();
  });
});
