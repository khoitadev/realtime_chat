import { UnauthorizedException } from '@nestjs/common';
import { JwtService } from '@nestjs/jwt';
import { JwksService } from './jwks.service';
import { defaultKey, generateTestKey, JwksServer, signToken, startJwksServer } from '../../test/jwt';
import { sleep } from '../../test/helpers';

describe('JwksService', () => {
  let server: JwksServer;

  beforeEach(async () => {
    server = await startJwksServer();
  });

  afterEach(async () => {
    await server.close();
  });

  function service(overrides: Partial<{ ttlMs: number; minRefetchMs: number }> = {}) {
    return new JwksService({
      url: server.url,
      ttlMs: overrides.ttlMs ?? 60_000,
      minRefetchMs: overrides.minRefetchMs ?? 300,
    });
  }

  it('returns a PEM that verifies tokens signed by the published key', async () => {
    const pem = await service().getPublicKeyPem(defaultKey.kid);

    const payload = await new JwtService().verifyAsync(signToken('user-1'), {
      publicKey: pem,
      algorithms: ['RS256'],
    });
    expect(payload.sub).toBe('user-1');
  });

  it('caches keys: repeated lookups do not hit the server again', async () => {
    const jwks = service();

    await jwks.getPublicKeyPem(defaultKey.kid);
    await jwks.getPublicKeyPem(defaultKey.kid);
    await jwks.getPublicKeyPem(defaultKey.kid);

    expect(server.hits()).toBe(1);
  });

  it('shares one in-flight fetch between concurrent callers', async () => {
    const jwks = service();

    await Promise.all(Array.from({ length: 5 }, () => jwks.getPublicKeyPem(defaultKey.kid)));

    expect(server.hits()).toBe(1);
  });

  it('rejects a missing kid without calling the server', async () => {
    await expect(service().getPublicKeyPem(undefined)).rejects.toThrow(UnauthorizedException);
    expect(server.hits()).toBe(0);
  });

  it('rate-limits refetches triggered by unknown kids (at most one per minRefetchMs)', async () => {
    const jwks = service({ minRefetchMs: 400 });
    await jwks.getPublicKeyPem(defaultKey.kid); // hit #1

    await expect(jwks.getPublicKeyPem('unknown-1')).rejects.toThrow(UnauthorizedException);
    await expect(jwks.getPublicKeyPem('unknown-2')).rejects.toThrow(UnauthorizedException);
    expect(server.hits()).toBe(1); // still inside the window: no refetch

    await sleep(450);
    await expect(jwks.getPublicKeyPem('unknown-3')).rejects.toThrow(UnauthorizedException);
    expect(server.hits()).toBe(2); // window elapsed: exactly one refetch
  });

  it('picks up a rotated key after the rate-limit window', async () => {
    const jwks = service({ minRefetchMs: 300 });
    await jwks.getPublicKeyPem(defaultKey.kid);

    const rotated = generateTestKey();
    server.setKeys([rotated, defaultKey]);
    await sleep(350);

    const pem = await jwks.getPublicKeyPem(rotated.kid);
    const payload = await new JwtService().verifyAsync(signToken('user-2', { key: rotated }), {
      publicKey: pem,
      algorithms: ['RS256'],
    });
    expect(payload.sub).toBe('user-2');
  });

  it('refreshes after the TTL and drops keys that are no longer published', async () => {
    const jwks = service({ ttlMs: 250, minRefetchMs: 100 });
    await jwks.getPublicKeyPem(defaultKey.kid);

    const replacement = generateTestKey();
    server.setKeys([replacement]);
    await sleep(300);

    await jwks.getPublicKeyPem(replacement.kid);
    await expect(jwks.getPublicKeyPem(defaultKey.kid)).rejects.toThrow(UnauthorizedException);
  });

  it('keeps serving cached keys when the JWKS endpoint goes down (stale-on-error)', async () => {
    const jwks = service({ ttlMs: 200, minRefetchMs: 100 });
    await jwks.getPublicKeyPem(defaultKey.kid);

    server.setDown(true);
    await sleep(250); // cache is now stale and a refresh will fail

    await expect(jwks.getPublicKeyPem(defaultKey.kid)).resolves.toContain('BEGIN PUBLIC KEY');
  });

  it('rejects (instead of crashing) when the endpoint is down and nothing is cached', async () => {
    server.setDown(true);

    await expect(service().getPublicKeyPem(defaultKey.kid)).rejects.toThrow(UnauthorizedException);
  });

  it('recovers once the endpoint comes back', async () => {
    server.setDown(true);
    const jwks = service({ minRefetchMs: 200 });
    await expect(jwks.getPublicKeyPem(defaultKey.kid)).rejects.toThrow(UnauthorizedException);

    server.setDown(false);
    await sleep(250);

    await expect(jwks.getPublicKeyPem(defaultKey.kid)).resolves.toContain('BEGIN PUBLIC KEY');
  });
});
