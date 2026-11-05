import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';
import { KeyService } from '../keys/key.service';

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(keys: KeyService, config: ConfigService) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      algorithms: ['RS256'],
      secretOrKey: keys.publicPem,
      issuer: config.get<string>('JWT_ISSUER', 'auth-service'),
      audience: config.get<string>('JWT_AUDIENCE', 'realtime-chat'),
    });
  }

  validate(payload: { sub: string }) {
    return { id: payload.sub };
  }
}
