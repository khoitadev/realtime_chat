import { Test } from '@nestjs/testing';
import { ConfigModule } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { ConflictException } from '@nestjs/common';
import { AuthService } from './auth.service';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

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
});
