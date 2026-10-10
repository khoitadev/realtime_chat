import { Test } from '@nestjs/testing';
import { NotFoundException } from '@nestjs/common';
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
});
