import { randomUUID } from 'crypto';
import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { WsTicketController } from './ws-ticket.controller';
import { WsTicketService } from './ws-ticket.service';
import { REDIS } from '../redis/redis.constants';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { AuthModule } from '../auth/auth.module';
import { cleanRedis } from '../../test/redis-cleanup';
import { signToken, acquireJwksServer, releaseJwksServer } from '../../test/jwt';

describe('POST /ws-ticket', () => {
  let app: INestApplication;
  let redis: Redis;
  let baseUrl: string;

  beforeAll(async () => {
    await acquireJwksServer();
    const moduleRef = await Test.createTestingModule({
      imports: [AuthModule],
      controllers: [WsTicketController],
      providers: [
        WsTicketService,
        { provide: REDIS, useFactory: () => new Redis(process.env.REDIS_URL!) },
        JwtAuthGuard,
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
    redis = moduleRef.get(REDIS);
  });

  afterAll(async () => {
    await app.close();
    await redis.quit();
    await releaseJwksServer();
  });

  beforeEach(async () => {
    await cleanRedis(redis);
  });

  it('returns 401 without a token', async () => {
    const res = await fetch(`${baseUrl}/ws-ticket`, { method: 'POST' });
    expect(res.status).toBe(401);
  });

  it('returns a 48-char hex ticket with expiresIn 30 for a valid access token', async () => {
    const res = await fetch(`${baseUrl}/ws-ticket`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${signToken(randomUUID())}` },
    });

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.expiresIn).toBe(30);
    expect(body.ticket).toMatch(/^[0-9a-f]{48}$/);
  });
});
