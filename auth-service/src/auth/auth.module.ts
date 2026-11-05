import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { JwtModule } from '@nestjs/jwt';
import { PassportModule } from '@nestjs/passport';
import { AuthService } from './auth.service';
import { AuthController } from './auth.controller';
import { JwtStrategy } from './jwt.strategy';
import { KeysModule } from '../keys/keys.module';
import { KeyService } from '../keys/key.service';

@Module({
  imports: [
    ConfigModule,
    PassportModule,
    KeysModule,
    JwtModule.registerAsync({
      imports: [ConfigModule, KeysModule],
      inject: [ConfigService, KeyService],
      useFactory: (config: ConfigService, keys: KeyService) => ({
        privateKey: keys.privatePem,
        publicKey: keys.publicPem,
        signOptions: {
          algorithm: 'RS256' as const,
          keyid: keys.kid,
          issuer: config.get<string>('JWT_ISSUER', 'auth-service'),
          audience: config.get<string>('JWT_AUDIENCE', 'realtime-chat'),
          expiresIn: '15m',
        },
      }),
    }),
  ],
  controllers: [AuthController],
  providers: [AuthService, JwtStrategy],
  exports: [AuthService, KeysModule],
})
export class AuthModule {}
