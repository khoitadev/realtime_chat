import { Test } from '@nestjs/testing';
import { ConfigModule } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { ConflictException, UnauthorizedException } from '@nestjs/common';
import { createHash, createPublicKey, generateKeyPairSync } from 'crypto';
import { AuthService } from './auth.service';
import { AuthModule } from './auth.module';
import { KeyService } from '../keys/key.service';
import { PrismaModule } from '../prisma/prisma.module';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

describe('AuthService', () => {
  let authService: AuthService;
  let prisma: PrismaService;
  let keys: KeyService;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [ConfigModule.forRoot({ isGlobal: true }), PrismaModule, AuthModule],
    }).compile();

    authService = moduleRef.get(AuthService);
    prisma = moduleRef.get(PrismaService);
    keys = moduleRef.get(KeyService);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  describe('access token', () => {
    it('is an RS256 JWT with kid, iss, aud and sub, verifiable with the published JWK', async () => {
      const { accessToken } = await authService.register({
        email: 'rs256@example.com',
        password: 'password123',
        displayName: 'Rs256',
      });
      const user = await prisma.user.findUniqueOrThrow({ where: { email: 'rs256@example.com' } });

      const header = JSON.parse(Buffer.from(accessToken.split('.')[0], 'base64url').toString());
      expect(header).toMatchObject({ alg: 'RS256', kid: keys.kid });

      const [jwk] = keys.getJwks().keys;
      const pem = createPublicKey({ key: jwk, format: 'jwk' }).export({
        type: 'spki',
        format: 'pem',
      }) as string;
      const payload = await new JwtService().verifyAsync(accessToken, {
        publicKey: pem,
        algorithms: ['RS256'],
        issuer: 'auth-service',
        audience: 'realtime-chat',
      });
      expect(payload.sub).toBe(user.id);
    });

    it('cannot be verified with a different key', async () => {
      const { accessToken } = await authService.register({
        email: 'otherkey@example.com',
        password: 'password123',
        displayName: 'OtherKey',
      });
      const { publicKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
      const wrongPem = publicKey.export({ type: 'spki', format: 'pem' }) as string;

      await expect(
        new JwtService().verifyAsync(accessToken, { publicKey: wrongPem, algorithms: ['RS256'] }),
      ).rejects.toThrow();
    });
  });

  describe('register', () => {
    it('creates a user and returns an access+refresh token pair', async () => {
      const result = await authService.register({
        email: 'alice@example.com',
        password: 'password123',
        displayName: 'Alice',
      });

      expect(result.accessToken).toBeDefined();
      expect(result.refreshToken).toBeDefined();

      const user = await prisma.user.findUnique({ where: { email: 'alice@example.com' } });
      expect(user).not.toBeNull();
      expect(user!.passwordHash).not.toBe('password123');
    });

    it('rejects a duplicate email with 409', async () => {
      await authService.register({
        email: 'bob@example.com',
        password: 'password123',
        displayName: 'Bob',
      });

      await expect(
        authService.register({
          email: 'bob@example.com',
          password: 'anotherpassword',
          displayName: 'Bob 2',
        }),
      ).rejects.toThrow(ConflictException);
    });
  });

  describe('login', () => {
    beforeEach(async () => {
      await authService.register({
        email: 'carol@example.com',
        password: 'correct-password',
        displayName: 'Carol',
      });
    });

    it('returns tokens for correct credentials', async () => {
      const result = await authService.login({
        email: 'carol@example.com',
        password: 'correct-password',
      });

      expect(result.accessToken).toBeDefined();
      expect(result.refreshToken).toBeDefined();
    });

    it('rejects an unknown email with 401', async () => {
      await expect(
        authService.login({ email: 'nobody@example.com', password: 'whatever' }),
      ).rejects.toThrow(UnauthorizedException);
    });

    it('rejects a wrong password with 401', async () => {
      await expect(
        authService.login({ email: 'carol@example.com', password: 'wrong-password' }),
      ).rejects.toThrow(UnauthorizedException);
    });
  });

  describe('refresh', () => {
    it('rotates the token: old token becomes revoked, new token is valid and unrevoked', async () => {
      const { refreshToken } = await authService.register({
        email: 'dave@example.com',
        password: 'password123',
        displayName: 'Dave',
      });

      const result = await authService.refresh(refreshToken);

      expect(result.accessToken).toBeDefined();
      expect(result.refreshToken).toBeDefined();
      expect(result.refreshToken).not.toBe(refreshToken);

      const oldHash = createHash('sha256').update(refreshToken).digest('hex');
      const oldRow = await prisma.refreshToken.findUnique({ where: { tokenHash: oldHash } });
      expect(oldRow!.revokedAt).not.toBeNull();

      const newHash = createHash('sha256').update(result.refreshToken).digest('hex');
      const newRow = await prisma.refreshToken.findUnique({ where: { tokenHash: newHash } });
      expect(newRow!.revokedAt).toBeNull();
    });

    it('rejects an unknown token with "Invalid refresh token"', async () => {
      await expect(authService.refresh('not-a-real-token')).rejects.toThrow(
        'Invalid refresh token',
      );
    });

    it('detects reuse of a revoked token and revokes ALL of that user\'s sessions', async () => {
      const { refreshToken: sessionA } = await authService.register({
        email: 'erin@example.com',
        password: 'password123',
        displayName: 'Erin',
      });
      // second session for the same user
      const { refreshToken: sessionB } = await authService.login({
        email: 'erin@example.com',
        password: 'password123',
      });

      // rotate session A once — the original sessionA token is now revoked
      await authService.refresh(sessionA);

      // reuse the already-revoked original sessionA token
      await expect(authService.refresh(sessionA)).rejects.toThrow(
        'Session revoked — please log in again',
      );

      // sessionB, which was never reused, must now ALSO be revoked
      await expect(authService.refresh(sessionB)).rejects.toThrow(
        'Session revoked — please log in again',
      );
    });

    it('rejects an expired (but not revoked) token with "Refresh token expired"', async () => {
      await authService.register({
        email: 'frank@example.com',
        password: 'password123',
        displayName: 'Frank',
      });
      const owner = await prisma.user.findUniqueOrThrow({ where: { email: 'frank@example.com' } });

      const expiredPlainToken = 'expired-token-plaintext';
      const expiredHash = createHash('sha256').update(expiredPlainToken).digest('hex');
      await prisma.refreshToken.create({
        data: {
          userId: owner.id,
          tokenHash: expiredHash,
          expiresAt: new Date(Date.now() - 1000), // 1 second in the past
        },
      });

      await expect(authService.refresh(expiredPlainToken)).rejects.toThrow(
        'Refresh token expired',
      );
    });
  });

  describe('logout', () => {
    it('revokes only the given token, leaving other sessions of the same user valid', async () => {
      const { refreshToken: sessionA } = await authService.register({
        email: 'grace@example.com',
        password: 'password123',
        displayName: 'Grace',
      });
      const { refreshToken: sessionB } = await authService.login({
        email: 'grace@example.com',
        password: 'password123',
      });

      const result = await authService.logout(sessionA);
      expect(result).toEqual({ success: true });

      const hashA = createHash('sha256').update(sessionA).digest('hex');
      const rowA = await prisma.refreshToken.findUnique({ where: { tokenHash: hashA } });
      expect(rowA!.revokedAt).not.toBeNull();

      // sessionB must still be usable
      const refreshed = await authService.refresh(sessionB);
      expect(refreshed.accessToken).toBeDefined();
    });
  });
});
