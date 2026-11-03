import { Socket as ClientSocket } from 'socket.io-client';
import { createInstance, TestInstance } from '../../test/instance';
import { cleanDatabase } from '../../test/db-cleanup';
import { cleanRedis } from '../../test/redis-cleanup';
import { connectSocket, emitWithAck, sleep, waitFor } from '../../test/helpers';
import { PrismaService } from '../prisma/prisma.service';
import { RoomsService } from '../rooms/rooms.service';
import { WsTicketService } from './ws-ticket.service';
import { PresenceService } from './presence.service';

interface PresenceEvent {
  userId: string;
  online: boolean;
}

describe('multi-instance realtime (2 app instances, 1 Redis, 1 Postgres)', () => {
  let a: TestInstance;
  let b: TestInstance;
  let prisma: PrismaService;
  let rooms: RoomsService;
  let sockets: ClientSocket[] = [];

  beforeAll(async () => {
    a = await createInstance();
    b = await createInstance();
    prisma = a.get(PrismaService);
    rooms = a.get(RoomsService);
  });

  afterAll(async () => {
    await a.close();
    await b.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
    await cleanRedis(a.redis);
    sockets = [];
  });

  afterEach(() => {
    sockets.forEach((s) => s.disconnect());
  });

  async function createUser(email: string) {
    return prisma.user.create({ data: { email, passwordHash: 'x', displayName: email } });
  }

  async function connectTo(instance: TestInstance, userId: string): Promise<ClientSocket> {
    const ticket = await instance.get<WsTicketService>(WsTicketService).issue(userId);
    const socket = await connectSocket(instance.url, ticket);
    sockets.push(socket);
    return socket;
  }

  function presenceEvents(socket: ClientSocket, userId: string): PresenceEvent[] {
    const events: PresenceEvent[] = [];
    socket.on('presence:update', (e: PresenceEvent) => {
      if (e.userId === userId) events.push(e);
    });
    return events;
  }

  describe('messaging and tickets', () => {
    it('delivers message:new sent via instance A to a subscriber connected to instance B', async () => {
      const sender = await createUser('x-sender@example.com');
      const receiver = await createUser('x-receiver@example.com');
      const room = await rooms.createRoom(sender.id, { name: 'Cross' });
      await rooms.joinRoom(receiver.id, room.id);

      const onA = await connectTo(a, sender.id);
      const onB = await connectTo(b, receiver.id);
      await emitWithAck(onA, 'room:subscribe', { roomId: room.id });
      await emitWithAck(onB, 'room:subscribe', { roomId: room.id });

      const received = new Promise<{ content: string }>((resolve) => onB.once('message:new', resolve));
      const ack = await emitWithAck<{ success: boolean }>(onA, 'message:send', {
        roomId: room.id,
        content: 'hello across instances',
      });

      expect(ack.success).toBe(true);
      expect((await received).content).toBe('hello across instances');
    });

    it('accepts on instance B a ticket that was issued by instance A, once only', async () => {
      const user = await createUser('x-ticket@example.com');
      const ticket = await a.get<WsTicketService>(WsTicketService).issue(user.id);

      const first = await connectSocket(b.url, ticket);
      sockets.push(first);
      expect(first.connected).toBe(true);

      await expect(connectSocket(a.url, ticket)).rejects.toBeDefined();
    });
  });

  describe('presence', () => {
    async function setup() {
      const subject = await createUser('p-subject@example.com');
      const observer = await createUser('p-observer@example.com');
      const room = await rooms.createRoom(subject.id, { name: 'Presence' });
      await rooms.joinRoom(observer.id, room.id);

      const observerSocket = await connectTo(b, observer.id);
      await emitWithAck(observerSocket, 'room:subscribe', { roomId: room.id });
      const events = presenceEvents(observerSocket, subject.id);
      return { subject, observer, room, observerSocket, events };
    }

    it('broadcasts online:true across instances when a user connects', async () => {
      const { subject, events } = await setup();

      await connectTo(a, subject.id);

      await waitFor(() => events.some((e) => e.online));
      expect(events).toEqual([{ userId: subject.id, online: true }]);
    });

    it('does not announce extra tabs, and only announces offline when the last tab closes', async () => {
      const { subject, events } = await setup();
      const redisKey = `presence:user:${subject.id}`;

      const tab1 = await connectTo(a, subject.id);
      await waitFor(() => events.length === 1);
      const tab2 = await connectTo(b, subject.id); // second device, other instance
      await waitFor(async () => (await a.redis.scard(redisKey)) === 2);
      await sleep(250);
      expect(events).toEqual([{ userId: subject.id, online: true }]); // no second online event

      tab1.disconnect();
      await waitFor(async () => (await a.redis.scard(redisKey)) === 1);
      await sleep(250);
      expect(events).toHaveLength(1); // still online: no offline event yet
      expect(await a.get<PresenceService>(PresenceService).isOnline(subject.id)).toBe(true);

      tab2.disconnect();
      await waitFor(() => events.length === 2);
      expect(events[1]).toEqual({ userId: subject.id, online: false });
    });

    it('does not send presence updates for rooms the user is not a member of', async () => {
      const subject = await createUser('p-subj2@example.com');
      const stranger = await createUser('p-stranger@example.com');
      const strangerRoom = await rooms.createRoom(stranger.id, { name: 'Elsewhere' });
      const strangerSocket = await connectTo(b, stranger.id);
      await emitWithAck(strangerSocket, 'room:subscribe', { roomId: strangerRoom.id });
      const events = presenceEvents(strangerSocket, subject.id);

      await connectTo(a, subject.id);
      await sleep(400);

      expect(events).toEqual([]);
    });
  });
});
