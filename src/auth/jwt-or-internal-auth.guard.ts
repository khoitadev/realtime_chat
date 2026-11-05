import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from '@nestjs/common';
import { JwtAuthGuard } from './jwt-auth.guard';

@Injectable()
export class JwtOrInternalAuthGuard implements CanActivate {
  constructor(private readonly jwtGuard: JwtAuthGuard) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const req = context.switchToHttp().getRequest();
    const secret = req.headers['x-internal-secret'];

    if (secret === process.env.INTERNAL_SECRET) {
      const userId = req.headers['x-internal-user-id'];
      if (!userId || typeof userId !== 'string') {
        throw new UnauthorizedException('Missing X-Internal-User-Id');
      }
      req.user = { id: userId };
      return true;
    }

    return this.jwtGuard.canActivate(context) as Promise<boolean>;
  }
}
