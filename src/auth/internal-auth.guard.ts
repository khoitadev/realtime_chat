import { CanActivate, ExecutionContext, Injectable, UnauthorizedException } from '@nestjs/common';

@Injectable()
export class InternalAuthGuard implements CanActivate {
  canActivate(context: ExecutionContext): boolean {
    const req = context.switchToHttp().getRequest();
    const secret = req.headers['x-internal-secret'];
    if (!secret || secret !== process.env.INTERNAL_SECRET) {
      throw new UnauthorizedException('Invalid internal secret');
    }
    return true;
  }
}
