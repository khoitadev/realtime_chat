import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { PassportModule } from '@nestjs/passport';
import { AddressInfo } from 'net';
import { AuthController } from './auth.controller';
import { AuthService } from './auth.service';
import { JwtStrategy } from './jwt.strategy';
import { PrismaService } from '../prisma/prisma.service';
import { WsTicketService } from '../realtime/ws-ticket.service';
import { RedisModule } from '../redis/redis.module';
import { cleanDatabase } from '../../test/db-cleanup';

describe('AuthController (POST /auth/ws-ticket)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let authService: AuthService;
  let baseUrl: string;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        RedisModule,
        PassportModule,
        JwtModule.registerAsync({
          imports: [ConfigModule],
          inject: [ConfigService],
          useFactory: (config: ConfigService) => ({
            secret: config.get<string>('JWT_SECRET'),
            signOptions: { expiresIn: '15m' },
          }),
        }),
      ],
      controllers: [AuthController],
      providers: [AuthService, JwtStrategy, PrismaService, WsTicketService],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);

    const address = app.getHttpServer().address() as AddressInfo;
    baseUrl = `http://localhost:${address.port}`;

    prisma = moduleRef.get(PrismaService);
    authService = moduleRef.get(AuthService);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await app.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  it('returns 401 without an Authorization header', async () => {
    const res = await fetch(`${baseUrl}/auth/ws-ticket`, { method: 'POST' });
    expect(res.status).toBe(401);
  });

  it('returns a 48-char hex ticket with expiresIn 30 for a valid access token', async () => {
    const { accessToken } = await authService.register({
      email: 'wsticket@example.com',
      password: 'password123',
      displayName: 'WsTicket',
    });

    const res = await fetch(`${baseUrl}/auth/ws-ticket`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${accessToken}` },
    });

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.expiresIn).toBe(30);
    expect(typeof body.ticket).toBe('string');
    expect(body.ticket).toMatch(/^[0-9a-f]{48}$/);
  });
});
