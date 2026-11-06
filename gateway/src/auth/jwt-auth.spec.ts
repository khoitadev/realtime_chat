import { randomUUID } from 'crypto';
import { Controller, Get, INestApplication, Module, Req, UseGuards } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { Test } from '@nestjs/testing';
import { AddressInfo } from 'net';
import {
  acquireJwksServer,
  defaultKey,
  generateTestKey,
  releaseJwksServer,
  signHs256WithPublicKey,
  signToken,
  unsignedToken,
} from '../../test/jwt';
import { AuthModule } from './auth.module';
import { JwtAuthGuard } from './jwt-auth.guard';

// Gateway has no app-level route yet to exercise the guard over, so this spec
// mounts a throwaway controller purely to drive JwtAuthGuard through real HTTP.
@Controller('ping')
class PingController {
  @Get()
  @UseGuards(JwtAuthGuard)
  ping(@Req() req: { user: { id: string } }) {
    return { userId: req.user.id };
  }
}

@Module({
  imports: [PassportModule, AuthModule],
  controllers: [PingController],
  providers: [JwtAuthGuard],
})
class PingModule {}

describe('JWT verification through JwtAuthGuard (GET /ping)', () => {
  let app: INestApplication;
  let url: string;

  beforeAll(async () => {
    await acquireJwksServer();
    const moduleRef = await Test.createTestingModule({ imports: [PingModule] }).compile();
    app = moduleRef.createNestApplication();
    await app.listen(0);
    const port = (app.getHttpServer().address() as AddressInfo).port;
    url = `http://localhost:${port}`;
  });

  afterAll(async () => {
    await app.close();
    await releaseJwksServer();
  });

  function get(token?: string) {
    return fetch(`${url}/ping`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  }

  const userId = () => randomUUID();

  it('accepts a valid RS256 token signed by the published key', async () => {
    const id = userId();
    const res = await get(signToken(id));

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ userId: id });
  });

  it('rejects a request without a token', async () => {
    expect((await get()).status).toBe(401);
  });

  it('rejects garbage', async () => {
    expect((await get('not.a.jwt')).status).toBe(401);
  });

  it('rejects a token whose kid is not published', async () => {
    const stranger = generateTestKey();
    expect((await get(signToken(userId(), { key: stranger }))).status).toBe(401);
  });

  it('rejects a token that claims a known kid but is signed with another private key', async () => {
    const forger = generateTestKey();
    expect((await get(signToken(userId(), { key: forger, kid: defaultKey.kid }))).status).toBe(401);
  });

  it('rejects an expired token', async () => {
    expect((await get(signToken(userId(), { expiresInSec: -60 }))).status).toBe(401);
  });

  it('rejects the wrong issuer', async () => {
    expect((await get(signToken(userId(), { issuer: 'evil-service' }))).status).toBe(401);
  });

  it('rejects the wrong audience', async () => {
    expect((await get(signToken(userId(), { audience: 'another-app' }))).status).toBe(401);
  });

  it('rejects alg:none', async () => {
    expect((await get(unsignedToken(userId()))).status).toBe(401);
  });

  it('rejects an HS256 token forged with the public key as secret (algorithm confusion)', async () => {
    expect((await get(signHs256WithPublicKey(userId()))).status).toBe(401);
  });
});
