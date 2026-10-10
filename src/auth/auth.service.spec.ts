import { Test } from '@nestjs/testing';
import { ConfigModule } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { ConflictException, UnauthorizedException } from '@nestjs/common';
import { AuthService } from './auth.service';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';
import { createHash } from 'crypto';

describe('AuthService', () => {
  let authService: AuthService;
  let prisma: PrismaService;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        JwtModule.register({ secret: process.env.JWT_SECRET, signOptions: { expiresIn: '15m' } }),
      ],
      providers: [AuthService, PrismaService],
    }).compile();

    authService = moduleRef.get(AuthService);
    prisma = moduleRef.get(PrismaService);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
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
});
