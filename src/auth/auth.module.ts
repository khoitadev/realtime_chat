import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { JWKS_OPTIONS, JwksService } from './jwks.service';
import { JWT_VERIFY_OPTIONS, JwtStrategy } from './jwt.strategy';

@Module({
  imports: [PassportModule],
  providers: [
    {
      provide: JWKS_OPTIONS,
      useFactory: () => ({
        url: process.env.AUTH_JWKS_URL ?? 'http://localhost:3010/.well-known/jwks.json',
        ttlMs: 10 * 60_000,
        minRefetchMs: 10_000,
      }),
    },
    {
      provide: JWT_VERIFY_OPTIONS,
      useFactory: () => ({
        issuer: process.env.JWT_ISSUER ?? 'auth-service',
        audience: process.env.JWT_AUDIENCE ?? 'realtime-chat',
      }),
    },
    JwksService,
    JwtStrategy,
  ],
  exports: [JwksService],
})
export class AuthModule {}
