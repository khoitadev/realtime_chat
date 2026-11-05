import { randomUUID } from 'crypto';
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { RoomsService } from './rooms.service';
import { PrismaService } from '../prisma/prisma.service';
import { InternalController } from './internal.controller';
import { InternalAuthGuard } from '../auth/internal-auth.guard';
import { REDIS } from '../redis/redis.constants';
import { cleanDatabase } from '../../test/db-cleanup';

describe('InternalController', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let rooms: RoomsService;
  let baseUrl: string;
  let redis: Redis;
  const OLD_SECRET = process.env.INTERNAL_SECRET;

  beforeAll(async () => {
    process.env.INTERNAL_SECRET = 'test-internal-secret';
    const moduleRef = await Test.createTestingModule({
      controllers: [InternalController],
      providers: [
        RoomsService,
        PrismaService,
        InternalAuthGuard,
        { provide: REDIS, useFactory: () => new Redis(process.env.REDIS_URL!) },
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;

    prisma = moduleRef.get(PrismaService);
    rooms = moduleRef.get(RoomsService);
    redis = moduleRef.get(REDIS);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await redis.quit();
    await app.close();
    process.env.INTERNAL_SECRET = OLD_SECRET;
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  function get(path: string, secret?: string) {
    return fetch(`${baseUrl}${path}`, {
      headers: secret ? { 'x-internal-secret': secret } : {},
    });
  }

  it('rejects both endpoints without the internal secret', async () => {
    expect((await get('/internal/rooms/any/members/any')).status).toBe(401);
    expect((await get('/internal/users/any/room-ids')).status).toBe(401);
  });

  it('GET /internal/rooms/:roomId/members/:userId reports membership correctly', async () => {
    const owner = randomUUID();
    const outsider = randomUUID();
    const room = await rooms.createRoom(owner, { name: 'Internal' });

    const memberRes = await get(`/internal/rooms/${room.id}/members/${owner}`, 'test-internal-secret');
    expect(await memberRes.json()).toEqual({ isMember: true });

    const outsiderRes = await get(
      `/internal/rooms/${room.id}/members/${outsider}`,
      'test-internal-secret',
    );
    expect(await outsiderRes.json()).toEqual({ isMember: false });
  });

  it('GET /internal/users/:userId/room-ids returns every room the user belongs to', async () => {
    const user = randomUUID();
    const roomA = await rooms.createRoom(user, { name: 'A' });
    const roomB = await rooms.createRoom(user, { name: 'B' });

    const res = await get(`/internal/users/${user}/room-ids`, 'test-internal-secret');
    const body = (await res.json()) as { roomIds: string[] };
    expect(body.roomIds.sort()).toEqual([roomA.id, roomB.id].sort());
  });
});
