import { Test } from '@nestjs/testing';
import { ForbiddenException, NotFoundException } from '@nestjs/common';
import { RoomsService } from './rooms.service';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

describe('RoomsService', () => {
  let roomsService: RoomsService;
  let prisma: PrismaService;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [RoomsService, PrismaService],
    }).compile();

    roomsService = moduleRef.get(RoomsService);
    prisma = moduleRef.get(PrismaService);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  async function createUser(email: string) {
    return prisma.user.create({
      data: { email, passwordHash: 'x', displayName: email },
    });
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
  });
});
