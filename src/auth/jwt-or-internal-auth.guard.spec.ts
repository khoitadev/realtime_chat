import { ExecutionContext, UnauthorizedException } from '@nestjs/common';
import { JwtOrInternalAuthGuard } from './jwt-or-internal-auth.guard';
import { JwtAuthGuard } from './jwt-auth.guard';

function contextWith(headers: Record<string, string>): ExecutionContext {
  const req: { headers: Record<string, string>; user?: { id: string } } = { headers };
  return {
    switchToHttp: () => ({ getRequest: () => req }),
  } as unknown as ExecutionContext;
}

describe('JwtOrInternalAuthGuard', () => {
  const OLD_ENV = process.env.INTERNAL_SECRET;

  beforeEach(() => {
    process.env.INTERNAL_SECRET = 'test-internal-secret';
  });

  afterAll(() => {
    process.env.INTERNAL_SECRET = OLD_ENV;
  });

  it('accepts a valid internal secret + user id, and sets req.user', async () => {
    const jwtGuard = { canActivate: jest.fn() } as unknown as JwtAuthGuard;
    const guard = new JwtOrInternalAuthGuard(jwtGuard);
    const context = contextWith({
      'x-internal-secret': 'test-internal-secret',
      'x-internal-user-id': 'user-123',
    });

    expect(await guard.canActivate(context)).toBe(true);
    expect(context.switchToHttp().getRequest().user).toEqual({ id: 'user-123' });
    expect(jwtGuard.canActivate).not.toHaveBeenCalled();
  });

  it('rejects a valid internal secret with no X-Internal-User-Id', async () => {
    const jwtGuard = { canActivate: jest.fn() } as unknown as JwtAuthGuard;
    const guard = new JwtOrInternalAuthGuard(jwtGuard);
    const context = contextWith({ 'x-internal-secret': 'test-internal-secret' });

    await expect(guard.canActivate(context)).rejects.toThrow(UnauthorizedException);
  });

  it('falls back to the JWT guard when there is no internal secret', async () => {
    const jwtGuard = { canActivate: jest.fn().mockResolvedValue(true) } as unknown as JwtAuthGuard;
    const guard = new JwtOrInternalAuthGuard(jwtGuard);
    const context = contextWith({ authorization: 'Bearer some.jwt.token' });

    expect(await guard.canActivate(context)).toBe(true);
    expect(jwtGuard.canActivate).toHaveBeenCalledWith(context);
  });

  it('falls back to the JWT guard (and its rejection) on a wrong internal secret', async () => {
    const jwtGuard = {
      canActivate: jest.fn().mockRejectedValue(new UnauthorizedException()),
    } as unknown as JwtAuthGuard;
    const guard = new JwtOrInternalAuthGuard(jwtGuard);
    const context = contextWith({ 'x-internal-secret': 'wrong' });

    await expect(guard.canActivate(context)).rejects.toThrow(UnauthorizedException);
  });
});
