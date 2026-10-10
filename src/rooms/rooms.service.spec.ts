import { Test } from '@nestjs/testing';
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
});
