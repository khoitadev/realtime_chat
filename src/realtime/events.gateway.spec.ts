import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { RedisModule } from '../redis/redis.module';
import { EventsGateway } from './events.gateway';
import { WsTicketService } from './ws-ticket.service';
import { RoomsService } from '../rooms/rooms.service';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

describe('EventsGateway', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let wsTicketService: WsTicketService;
  let roomsService: RoomsService;
  let baseUrl: string;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [RedisModule],
      providers: [EventsGateway, WsTicketService, RoomsService, PrismaService],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);

    const address = app.getHttpServer().address() as AddressInfo;
    baseUrl = `http://localhost:${address.port}`;

    prisma = moduleRef.get(PrismaService);
    wsTicketService = moduleRef.get(WsTicketService);
    roomsService = moduleRef.get(RoomsService);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await app.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  async function createUser(email: string) {
    return prisma.user.create({ data: { email, passwordHash: 'x', displayName: email } });
  }

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
      const user = await createUser('wsuser@example.com');
      const ticket = await wsTicketService.issue(user.id);

      const socket = await connectWithTicket(ticket);
      expect(socket.connected).toBe(true);
      socket.disconnect();
    });

    it('rejects a connection with an unknown ticket', async () => {
      await expect(connectWithTicket('not-a-real-ticket')).rejects.toBeDefined();
    });

    it('rejects a connection when the ticket is reused', async () => {
      const user = await createUser('wsuser2@example.com');
      const ticket = await wsTicketService.issue(user.id);

      const first = await connectWithTicket(ticket);
      first.disconnect();

      await expect(connectWithTicket(ticket)).rejects.toBeDefined();
    });
  });

  describe('room:subscribe / room:unsubscribe', () => {
    function emitWithAck<T>(socket: ClientSocket, event: string, payload: unknown): Promise<T> {
      return new Promise((resolve) => socket.emit(event, payload, resolve));
    }

    it('allows a member to subscribe to a room', async () => {
      const user = await createUser('subuser@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Sub' });
      const ticket = await wsTicketService.issue(user.id);
      const socket = await connectWithTicket(ticket);

      const ack = await emitWithAck<{ success: boolean }>(socket, 'room:subscribe', {
        roomId: room.id,
      });

      expect(ack.success).toBe(true);
      socket.disconnect();
    });

    it('rejects subscribe when the user is not a member', async () => {
      const owner = await createUser('subowner@example.com');
      const outsider = await createUser('suboutsider@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'SubPrivate' });
      const ticket = await wsTicketService.issue(outsider.id);
      const socket = await connectWithTicket(ticket);

      const ack = await emitWithAck<{ success: boolean; error?: string }>(
        socket,
        'room:subscribe',
        { roomId: room.id },
      );

      expect(ack.success).toBe(false);
      expect(ack.error).toBe('Not a member of this room');
      socket.disconnect();
    });

    it('allows unsubscribing without a membership check', async () => {
      const user = await createUser('unsubuser@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Unsub' });
      const ticket = await wsTicketService.issue(user.id);
      const socket = await connectWithTicket(ticket);

      const ack = await emitWithAck<{ success: boolean }>(socket, 'room:unsubscribe', {
        roomId: room.id,
      });

      expect(ack.success).toBe(true);
      socket.disconnect();
    });
  });

  describe('message:send', () => {
    function emitWithAck<T>(socket: ClientSocket, event: string, payload: unknown): Promise<T> {
      return new Promise((resolve) => socket.emit(event, payload, resolve));
    }

    it('persists the message, acks success, and broadcasts message:new to subscribers', async () => {
      const user = await createUser('sender-ws@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Broadcast' });

      const socketA = await connectWithTicket(await wsTicketService.issue(user.id)); // sender
      const socketB = await connectWithTicket(await wsTicketService.issue(user.id)); // second tab

      await emitWithAck(socketA, 'room:subscribe', { roomId: room.id });
      await emitWithAck(socketB, 'room:subscribe', { roomId: room.id });

      const receivedByB = new Promise<{ content: string }>((resolve) =>
        socketB.once('message:new', resolve),
      );
      const receivedByA = new Promise<{ content: string }>((resolve) =>
        socketA.once('message:new', resolve),
      );

      const ack = await emitWithAck<{ success: boolean }>(socketA, 'message:send', {
        roomId: room.id,
        content: 'hello from A',
      });

      expect(ack.success).toBe(true);
      const [broadcastToB, broadcastToA] = await Promise.all([receivedByB, receivedByA]);
      expect(broadcastToB.content).toBe('hello from A');
      expect(broadcastToA.content).toBe('hello from A');

      const stored = await prisma.message.findFirst({ where: { roomId: room.id } });
      expect(stored?.content).toBe('hello from A');

      socketA.disconnect();
      socketB.disconnect();
    });

    it('rejects an empty message with an ack error', async () => {
      const user = await createUser('emptysender@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'EmptyMsg' });
      const socket = await connectWithTicket(await wsTicketService.issue(user.id));
      await emitWithAck(socket, 'room:subscribe', { roomId: room.id });

      const ack = await emitWithAck<{ success: boolean; error?: string }>(socket, 'message:send', {
        roomId: room.id,
        content: '   ',
      });

      expect(ack.success).toBe(false);
      expect(ack.error).toBe('content must not be empty');
      socket.disconnect();
    });

    it('rejects when the sender is not a room member', async () => {
      const owner = await createUser('msgowner-ws@example.com');
      const outsider = await createUser('msgoutsider-ws@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'PrivateWs' });
      const socket = await connectWithTicket(await wsTicketService.issue(outsider.id));

      const ack = await emitWithAck<{ success: boolean; error?: string }>(socket, 'message:send', {
        roomId: room.id,
        content: 'intruder',
      });

      expect(ack.success).toBe(false);
      expect(ack.error).toBe('Not a member of this room');
      socket.disconnect();
    });
  });
});
