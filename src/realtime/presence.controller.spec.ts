import { randomUUID } from 'crypto';
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { PresenceController } from './presence.controller';
import { PresenceService } from './presence.service';
import { RoomsService } from '../rooms/rooms.service';
import { PrismaService } from '../prisma/prisma.service';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { AuthModule } from '../auth/auth.module';
import { REDIS } from '../redis/redis.constants';
import { cleanDatabase } from '../../test/db-cleanup';
import { cleanRedis } from '../../test/redis-cleanup';
import { signToken, acquireJwksServer, releaseJwksServer } from '../../test/jwt';

describe('GET /rooms/:roomId/presence', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let rooms: RoomsService;
  let redis: Redis;
  let baseUrl: string;

  beforeAll(async () => {
    await acquireJwksServer();
    const moduleRef = await Test.createTestingModule({
      imports: [AuthModule],
      controllers: [PresenceController],
      providers: [
        RoomsService,
        PrismaService,
        PresenceService,
        { provide: REDIS, useFactory: () => new Redis(process.env.REDIS_URL!) },
        JwtAuthGuard,
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;

    prisma = moduleRef.get(PrismaService);
    rooms = moduleRef.get(RoomsService);
    redis = moduleRef.get(REDIS);
  });

  afterAll(async () => {
    await app.close();
    await redis.quit();
    await releaseJwksServer();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
    await cleanRedis(redis);
  });

  async function goOnline(userId: string) {
    const socketId = randomUUID();
    await redis.sadd(`presence:user:${userId}`, socketId);
    await redis.set(`presence:socket:${socketId}`, userId, 'EX', 60);
  }

  function get(roomId: string, token?: string) {
    return fetch(`${baseUrl}/rooms/${roomId}/presence`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  }

  it('returns 401 without a token', async () => {
    expect((await get('any-room')).status).toBe(401);
  });

  it('returns 403 when the caller is not a member of the room', async () => {
    const owner = randomUUID();
    const outsider = randomUUID();
    const room = await rooms.createRoom(owner, { name: 'Private' });

    expect((await get(room.id, signToken(outsider))).status).toBe(403);
  });

  it('lists only the members that are currently online', async () => {
    const me = randomUUID();
    const online = randomUUID();
    const offline = randomUUID();
    const room = await rooms.createRoom(me, { name: 'Team' });
    await rooms.joinRoom(online, room.id);
    await rooms.joinRoom(offline, room.id);
    await goOnline(online);

    const res = await get(room.id, signToken(me));

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ onlineUserIds: [online] });
  });
});
