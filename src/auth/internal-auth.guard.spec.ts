import { ExecutionContext, UnauthorizedException } from '@nestjs/common';
import { InternalAuthGuard } from './internal-auth.guard';

function contextWith(headers: Record<string, string>): ExecutionContext {
  return {
    switchToHttp: () => ({
      getRequest: () => ({ headers }),
    }),
  } as unknown as ExecutionContext;
}

describe('InternalAuthGuard', () => {
  const guard = new InternalAuthGuard();
  const OLD_ENV = process.env.INTERNAL_SECRET;

  beforeEach(() => {
    process.env.INTERNAL_SECRET = 'test-internal-secret';
  });

  afterAll(() => {
    process.env.INTERNAL_SECRET = OLD_ENV;
  });

  it('allows a request with the correct secret', () => {
    expect(guard.canActivate(contextWith({ 'x-internal-secret': 'test-internal-secret' }))).toBe(
      true,
    );
  });

  it('rejects a request with the wrong secret', () => {
    expect(() => guard.canActivate(contextWith({ 'x-internal-secret': 'wrong' }))).toThrow(
      UnauthorizedException,
    );
  });

  it('rejects a request with no secret header', () => {
    expect(() => guard.canActivate(contextWith({}))).toThrow(UnauthorizedException);
  });
});
