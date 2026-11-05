import { Test } from '@nestjs/testing';
import { INestApplication, ValidationPipe } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { AddressInfo } from 'net';
import { AuthModule } from './auth.module';
import { PrismaModule } from '../prisma/prisma.module';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

describe('AuthController (HTTP)', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let baseUrl: string;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule, AuthModule],
    }).compile();

    app = moduleRef.createNestApplication();
    app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
    prisma = moduleRef.get(PrismaService);
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  function post(path: string, body: unknown) {
    return fetch(`${baseUrl}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
  }

  const alice = { email: 'alice@example.com', password: 'password123', displayName: 'Alice' };

  it('registers and returns a token pair (201)', async () => {
    const res = await post('/auth/register', alice);

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(typeof body.accessToken).toBe('string');
    expect(typeof body.refreshToken).toBe('string');
  });

  it('rejects an invalid payload with 400 and a duplicate email with 409', async () => {
    expect((await post('/auth/register', { email: 'nope' })).status).toBe(400);

    await post('/auth/register', alice);
    expect((await post('/auth/register', alice)).status).toBe(409);
  });

  it('logs in with valid credentials and rejects bad ones with 401', async () => {
    await post('/auth/register', alice);

    expect((await post('/auth/login', { email: alice.email, password: alice.password })).status).toBe(201);
    expect((await post('/auth/login', { email: alice.email, password: 'wrongpassword' })).status).toBe(401);
  });

  it('rotates the refresh token and treats reuse of the old one as theft (401, all sessions revoked)', async () => {
    const first = await (await post('/auth/register', alice)).json();
    const rotated = await (await post('/auth/refresh', { refreshToken: first.refreshToken })).json();
    expect(rotated.refreshToken).not.toBe(first.refreshToken);

    const reuse = await post('/auth/refresh', { refreshToken: first.refreshToken });
    expect(reuse.status).toBe(401);

    const afterTheft = await post('/auth/refresh', { refreshToken: rotated.refreshToken });
    expect(afterTheft.status).toBe(401);
  });

  it('logs out by revoking the refresh token', async () => {
    const { refreshToken } = await (await post('/auth/register', alice)).json();

    expect((await post('/auth/logout', { refreshToken })).status).toBe(200);
    expect((await post('/auth/refresh', { refreshToken })).status).toBe(401);
  });

  it('GET /auth/me requires a valid token and returns the profile', async () => {
    expect((await fetch(`${baseUrl}/auth/me`)).status).toBe(401);

    const { accessToken } = await (await post('/auth/register', alice)).json();
    const res = await fetch(`${baseUrl}/auth/me`, {
      headers: { Authorization: `Bearer ${accessToken}` },
    });

    expect(res.status).toBe(200);
    expect(await res.json()).toMatchObject({ email: alice.email, displayName: 'Alice' });
  });

  it('serves a JWKS whose key verifies the issued access token header kid', async () => {
    const { accessToken } = await (await post('/auth/register', alice)).json();
    const { kid } = JSON.parse(Buffer.from(accessToken.split('.')[0], 'base64url').toString());

    const jwks = await (await fetch(`${baseUrl}/.well-known/jwks.json`)).json();

    expect(jwks.keys.map((k: { kid: string }) => k.kid)).toContain(kid);
  });
});
