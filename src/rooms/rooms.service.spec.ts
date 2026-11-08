import { randomUUID } from 'crypto';
import { Test, TestingModule } from '@nestjs/testing';
import { ForbiddenException, NotFoundException } from '@nestjs/common';
import Redis from 'ioredis';
import { RoomsService } from './rooms.service';
import { PrismaService } from '../prisma/prisma.service';
import { REDIS } from '../redis/redis.constants';
import { cleanDatabase } from '../../test/db-cleanup';
import { MessagingModule } from '../messaging/messaging.module';
import { EventPublisher } from '../messaging/event.publisher';
import { listenForEvents, resetExchange, EventListener } from '../../test/amqp';
import { waitFor } from '../../test/helpers';

describe('RoomsService', () => {
  let roomsService: RoomsService;
  let prisma: PrismaService;
  let injectedRedis: Redis;
  let publisher: EventPublisher;
  let moduleRef: TestingModule;

  beforeAll(async () => {
    moduleRef = await Test.createTestingModule({
      imports: [MessagingModule],
      providers: [
        RoomsService,
        PrismaService,
        { provide: REDIS, useFactory: () => new Redis(process.env.REDIS_URL!) },
      ],
    }).compile();
    await moduleRef.init();
    publisher = moduleRef.get(EventPublisher);

    roomsService = moduleRef.get(RoomsService);
    prisma = moduleRef.get(PrismaService);
    injectedRedis = moduleRef.get(REDIS);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await injectedRedis.quit();
    await moduleRef.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  async function createUser(email: string) {
    return { id: randomUUID(), email, displayName: email };
  }

  describe('createRoom', () => {
    it('creates a room and makes the creator a member', async () => {
      const user = await createUser('owner@example.com');

      const room = await roomsService.createRoom(user.id, { name: 'General' });

      expect(room.name).toBe('General');
      expect(room.createdBy).toBe(user.id);

      const membership = await prisma.roomMember.findUnique({
        where: { roomId_userId: { roomId: room.id, userId: user.id } },
      });
      expect(membership).not.toBeNull();
    });
  });

  describe('joinRoom', () => {
    it('adds the user as a member of an existing room', async () => {
      const owner = await createUser('owner2@example.com');
      const joiner = await createUser('joiner@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Random' });

      await roomsService.joinRoom(joiner.id, room.id);

      const membership = await prisma.roomMember.findUnique({
        where: { roomId_userId: { roomId: room.id, userId: joiner.id } },
      });
      expect(membership).not.toBeNull();
    });

    it('throws NotFoundException for a non-existent room', async () => {
      const user = await createUser('lonely@example.com');

      await expect(roomsService.joinRoom(user.id, 'does-not-exist')).rejects.toThrow(
        NotFoundException,
      );
    });

    it('is idempotent when the user is already a member', async () => {
      const owner = await createUser('owner3@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Dup' });

      await expect(roomsService.joinRoom(owner.id, room.id)).resolves.toBeDefined();
    });
  });

  describe('listRooms', () => {
    it('returns only rooms the user has joined', async () => {
      const user = await createUser('lister@example.com');
      const outsider = await createUser('outsider@example.com');

      const roomA = await roomsService.createRoom(user.id, { name: 'A' });
      await roomsService.createRoom(outsider.id, { name: 'B' });
      await roomsService.joinRoom(user.id, roomA.id); // already a member, no-op

      const rooms = await roomsService.listRooms(user.id);

      expect(rooms.map((r) => r.id)).toEqual([roomA.id]);
    });
  });

  describe('sendMessage', () => {
    it('creates a message when the sender is a room member', async () => {
      const user = await createUser('sender@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Chat' });

      const message = await roomsService.sendMessage(user.id, room.id, { content: 'hello' });

      expect(message.content).toBe('hello');
      expect(message.senderId).toBe(user.id);
      expect(message.roomId).toBe(room.id);
    });

    it('throws ForbiddenException when the sender is not a room member', async () => {
      const owner = await createUser('owner4@example.com');
      const outsider = await createUser('outsider2@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Private' });

      await expect(
        roomsService.sendMessage(outsider.id, room.id, { content: 'intruder' }),
      ).rejects.toThrow(ForbiddenException);
    });
  });

  describe('getMessages', () => {
    async function seedMessages(roomId: string, senderId: string, count: number) {
      const messages = [];
      for (let i = 0; i < count; i++) {
        const message = await prisma.message.create({
          data: {
            roomId,
            senderId,
            content: `message-${i}`,
            createdAt: new Date(Date.now() + i * 1000), // strictly increasing
          },
        });
        messages.push(message);
      }
      return messages;
    }

    it('throws ForbiddenException when the caller is not a member', async () => {
      const owner = await createUser('msgowner@example.com');
      const outsider = await createUser('msgoutsider@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Secret' });

      await expect(roomsService.getMessages(outsider.id, room.id)).rejects.toThrow(
        ForbiddenException,
      );
    });

    it('paginates in newest-first pages but returns each page oldest-to-newest, with a nextCursor while more remain', async () => {
      const user = await createUser('paginator@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Paged' });
      await seedMessages(room.id, user.id, 5); // oldest..newest

      const page1 = await roomsService.getMessages(user.id, room.id, undefined, 2);
      expect(page1.items.map((m) => m.content)).toEqual(['message-3', 'message-4']);
      expect(page1.nextCursor).not.toBeNull();

      const page2 = await roomsService.getMessages(user.id, room.id, page1.nextCursor!, 2);
      expect(page2.items.map((m) => m.content)).toEqual(['message-1', 'message-2']);
      expect(page2.nextCursor).not.toBeNull();

      const page3 = await roomsService.getMessages(user.id, room.id, page2.nextCursor!, 2);
      expect(page3.items.map((m) => m.content)).toEqual(['message-0']);
      expect(page3.nextCursor).toBeNull();
    });

    it('tie-breaks by id when two messages share the exact same createdAt', async () => {
      const user = await createUser('tiebreak@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'Tied' });
      const sameInstant = new Date();

      const [msgA, msgB] = await Promise.all([
        prisma.message.create({
          data: { roomId: room.id, senderId: user.id, content: 'tied-a', createdAt: sameInstant },
        }),
        prisma.message.create({
          data: { roomId: room.id, senderId: user.id, content: 'tied-b', createdAt: sameInstant },
        }),
      ]);
      const [lower, higher] = [msgA, msgB].sort((a, b) => (a.id < b.id ? -1 : 1));

      const page1 = await roomsService.getMessages(user.id, room.id, undefined, 1);
      expect(page1.items[0].id).toBe(higher.id);
      expect(page1.nextCursor).toBe(`${higher.createdAt.toISOString()}|${higher.id}`);

      const page2 = await roomsService.getMessages(user.id, room.id, page1.nextCursor!, 1);
      expect(page2.items[0].id).toBe(lower.id);
      expect(page2.nextCursor).toBeNull();
    });

    it('is not affected by a new message inserted between two paginated calls', async () => {
      const user = await createUser('midinsert@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'MidInsert' });
      await seedMessages(room.id, user.id, 4); // message-0..message-3, oldest..newest

      const page1 = await roomsService.getMessages(user.id, room.id, undefined, 2);
      expect(page1.items.map((m) => m.content)).toEqual(['message-2', 'message-3']);

      // simulate a new message arriving after page1 was fetched but before page2
      await prisma.message.create({
        data: {
          roomId: room.id,
          senderId: user.id,
          content: 'message-new',
          createdAt: new Date(Date.now() + 10_000), // newer than everything seeded
        },
      });

      const page2 = await roomsService.getMessages(user.id, room.id, page1.nextCursor!, 2);
      expect(page2.items.map((m) => m.content)).toEqual(['message-0', 'message-1']);
    });
  });

  describe('assertMember', () => {
    it('resolves when the user is a member', async () => {
      const user = await createUser('member@example.com');
      const room = await roomsService.createRoom(user.id, { name: 'X' });

      await expect(roomsService.assertMember(user.id, room.id)).resolves.toBeUndefined();
    });

    it('throws ForbiddenException when the user is not a member', async () => {
      const owner = await createUser('owner5@example.com');
      const outsider = await createUser('outsider3@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Y' });

      await expect(roomsService.assertMember(outsider.id, room.id)).rejects.toThrow(
        ForbiddenException,
      );
      await expect(roomsService.assertMember(outsider.id, room.id)).rejects.toThrow(
        'Not a member of this room',
      );
    });
  });

  describe('isMember', () => {
    it('returns true for a member and false for a non-member', async () => {
      const owner = await createUser('im-owner@example.com');
      const outsider = await createUser('im-outsider@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'IsMember' });

      expect(await roomsService.isMember(owner.id, room.id)).toBe(true);
      expect(await roomsService.isMember(outsider.id, room.id)).toBe(false);
    });

    it('returns false for a non-existent room', async () => {
      const user = await createUser('im-lonely@example.com');

      expect(await roomsService.isMember(user.id, 'does-not-exist')).toBe(false);
    });
  });

  describe('member lookups', () => {
    it('listMemberIds returns the userIds of every member of the room', async () => {
      const a = await createUser('lm-a@example.com');
      const b = await createUser('lm-b@example.com');
      const c = await createUser('lm-c@example.com');
      const room = await roomsService.createRoom(a.id, { name: 'LM' });
      await roomsService.joinRoom(b.id, room.id);

      const ids = await roomsService.listMemberIds(room.id);

      expect(ids.sort()).toEqual([a.id, b.id].sort());
      expect(ids).not.toContain(c.id);
    });

    it('listRoomIdsOf returns the ids of every room the user belongs to', async () => {
      const a = await createUser('lr-a@example.com');
      const b = await createUser('lr-b@example.com');
      const r1 = await roomsService.createRoom(a.id, { name: 'R1' });
      const r2 = await roomsService.createRoom(a.id, { name: 'R2' });
      await roomsService.createRoom(b.id, { name: 'Other' });

      const ids = await roomsService.listRoomIdsOf(a.id);

      expect(ids.sort()).toEqual([r1.id, r2.id].sort());
    });

    it('returns empty arrays for unknown ids', async () => {
      expect(await roomsService.listMemberIds('00000000-0000-0000-0000-000000000000')).toEqual([]);
      expect(await roomsService.listRoomIdsOf('00000000-0000-0000-0000-000000000000')).toEqual([]);
    });
  });

  describe('message.created event', () => {
    let listener: EventListener;

    beforeEach(async () => {
      await resetExchange();
      await new Promise((r) => setTimeout(r, 50));
      listener = await listenForEvents();
    });

    afterEach(async () => {
      await listener.close();
    });

    it('publishes one event with the other members as recipients and the given correlation id', async () => {
      const sender = await createUser('ev-sender@example.com');
      const a = await createUser('ev-a@example.com');
      const b = await createUser('ev-b@example.com');
      const room = await roomsService.createRoom(sender.id, { name: 'Ev' });
      await roomsService.joinRoom(a.id, room.id);
      await roomsService.joinRoom(b.id, room.id);

      const message = await roomsService.sendMessage(
        sender.id,
        room.id,
        { content: 'hi everyone' },
        'corr-abc',
      );

      await waitFor(() => listener.messages.length === 1);
      const event = JSON.parse(listener.messages[0].content.toString());
      expect(event).toMatchObject({
        type: 'message.created',
        version: 1,
        correlationId: 'corr-abc',
        data: { messageId: message.id, roomId: room.id, senderId: sender.id, preview: 'hi everyone' },
      });
      expect(event.data.recipientIds.sort()).toEqual([a.id, b.id].sort());
      expect(event.data.recipientIds).not.toContain(sender.id);
    });

    it('generates a correlation id when none is given', async () => {
      const sender = await createUser('ev-gen@example.com');
      const other = await createUser('ev-gen2@example.com');
      const room = await roomsService.createRoom(sender.id, { name: 'Gen' });
      await roomsService.joinRoom(other.id, room.id);

      await roomsService.sendMessage(sender.id, room.id, { content: 'x' });

      await waitFor(() => listener.messages.length === 1);
      const event = JSON.parse(listener.messages[0].content.toString());
      expect(event.correlationId).toMatch(/^[0-9a-f-]{36}$/);
    });

    it('cuts a long message preview at 100 characters', async () => {
      const sender = await createUser('ev-long@example.com');
      const other = await createUser('ev-long2@example.com');
      const room = await roomsService.createRoom(sender.id, { name: 'Long' });
      await roomsService.joinRoom(other.id, room.id);

      await roomsService.sendMessage(sender.id, room.id, { content: 'a'.repeat(300) });

      await waitFor(() => listener.messages.length === 1);
      const event = JSON.parse(listener.messages[0].content.toString());
      expect(event.data.preview).toBe('a'.repeat(100));
    });

    it('publishes nothing when the sender is the only member', async () => {
      const sender = await createUser('ev-solo@example.com');
      const room = await roomsService.createRoom(sender.id, { name: 'Solo' });

      await roomsService.sendMessage(sender.id, room.id, { content: 'alone' });

      await new Promise((r) => setTimeout(r, 400));
      expect(listener.messages).toHaveLength(0);
    });

    it('does not publish when the sender is not a member (send is rejected)', async () => {
      const owner = await createUser('ev-owner@example.com');
      const outsider = await createUser('ev-out@example.com');
      const room = await roomsService.createRoom(owner.id, { name: 'Closed' });

      await expect(
        roomsService.sendMessage(outsider.id, room.id, { content: 'nope' }),
      ).rejects.toThrow(ForbiddenException);

      await new Promise((r) => setTimeout(r, 300));
      expect(listener.messages).toHaveLength(0);
    });

    it('still saves and returns the message when the broker is unavailable', async () => {
      const sender = await createUser('ev-down@example.com');
      const other = await createUser('ev-down2@example.com');
      const room = await roomsService.createRoom(sender.id, { name: 'Down' });
      await roomsService.joinRoom(other.id, room.id);
      const before = { ...publisher.stats };
      // Simulate an unavailable broker: the publisher has no channel.
      (publisher as unknown as { channel: undefined }).channel = undefined;

      const message = await roomsService.sendMessage(sender.id, room.id, { content: 'offline' });

      expect(message.content).toBe('offline');
      expect(await prisma.message.findUnique({ where: { id: message.id } })).not.toBeNull();
      await waitFor(() => publisher.stats.skipped === before.skipped + 1);
    });
  });
});

describe('sendMessage Redis publish', () => {
  let service: RoomsService;
  let prisma: PrismaService;
  let injectedRedis: Redis;
  let redis: Redis;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [MessagingModule],
      providers: [
        RoomsService,
        PrismaService,
        { provide: REDIS, useFactory: () => new Redis(process.env.REDIS_URL!) },
      ],
    }).compile();

    service = moduleRef.get(RoomsService);
    prisma = moduleRef.get(PrismaService);
    injectedRedis = moduleRef.get(REDIS);
    await prisma.$connect();

    redis = new Redis(process.env.REDIS_URL!);
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await injectedRedis.quit();
    await redis.quit();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  it('publishes {roomId, message} on chat:message:new after persisting', async () => {
    const owner = randomUUID();
    const room = await service.createRoom(owner, { name: 'PubSub' });

    const sub = redis.duplicate();
    await sub.subscribe('chat:message:new');
    const received = new Promise<{ roomId: string; message: { content: string } }>((resolve) => {
      sub.on('message', (_channel, raw) => resolve(JSON.parse(raw)));
    });

    const message = await service.sendMessage(owner, room.id, { content: 'hello pubsub' });

    const payload = await received;
    expect(payload.roomId).toBe(room.id);
    expect(payload.message.content).toBe('hello pubsub');
    expect(payload.message).toMatchObject({ id: message.id });

    await sub.unsubscribe('chat:message:new');
    await sub.quit();
  });
});
