import { ConfigService } from '@nestjs/config';
import { JwtService } from '@nestjs/jwt';
import { createPublicKey, generateKeyPairSync } from 'crypto';
import { KeyService } from './key.service';

function newPrivatePem(): string {
  const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  return privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;
}

function publicPemOf(privatePem: string): string {
  return createPublicKey(privatePem).export({ type: 'spki', format: 'pem' }) as string;
}

function service(env: Record<string, string | undefined>): KeyService {
  // ConfigService.get() prefers process.env over its internal config, and jest.env.ts sets
  // JWT_PRIVATE_KEY there, so read strictly from the supplied env instead.
  return new KeyService({ get: (key: string) => env[key] } as unknown as ConfigService);
}

describe('KeyService', () => {
  it('derives a stable kid from the key (same key => same kid, different key => different kid)', () => {
    const pem = newPrivatePem();
    expect(service({ JWT_PRIVATE_KEY: pem }).kid).toBe(service({ JWT_PRIVATE_KEY: pem }).kid);
    expect(service({ JWT_PRIVATE_KEY: newPrivatePem() }).kid).not.toBe(service({ JWT_PRIVATE_KEY: pem }).kid);
  });

  it('publishes a JWK with kty/n/e/kid/use/alg and no private members', () => {
    const keys = service({ JWT_PRIVATE_KEY: newPrivatePem() });
    const [jwk] = keys.getJwks().keys;

    expect(Object.keys(jwk).sort()).toEqual(['alg', 'e', 'kid', 'kty', 'n', 'use']);
    expect(jwk).toMatchObject({ kty: 'RSA', use: 'sig', alg: 'RS256', kid: keys.kid });
    for (const secret of ['d', 'p', 'q', 'dp', 'dq', 'qi']) {
      expect(jwk).not.toHaveProperty(secret);
    }
  });

  it('produces a public key that verifies tokens signed with the private key', async () => {
    const keys = service({ JWT_PRIVATE_KEY: newPrivatePem() });
    const jwt = new JwtService();
    const token = jwt.sign(
      { sub: 'u1' },
      { privateKey: keys.privatePem, algorithm: 'RS256', keyid: keys.kid },
    );

    const [jwk] = keys.getJwks().keys;
    const pem = createPublicKey({ key: jwk, format: 'jwk' }).export({
      type: 'spki',
      format: 'pem',
    }) as string;

    expect((await jwt.verifyAsync(token, { publicKey: pem, algorithms: ['RS256'] })).sub).toBe('u1');
  });

  it('accepts a single-line PEM with literal \\n sequences (as written in .env files)', () => {
    const pem = newPrivatePem();
    const escaped = pem.trim().replace(/\n/g, '\\n');

    expect(service({ JWT_PRIVATE_KEY: escaped }).kid).toBe(service({ JWT_PRIVATE_KEY: pem }).kid);
  });

  it('keeps previous public keys in the JWKS after the current one, without duplicates', () => {
    const currentPem = newPrivatePem();
    const oldPem = newPrivatePem();
    const previous = [publicPemOf(oldPem), publicPemOf(currentPem)].join('\n');

    const keys = service({ JWT_PRIVATE_KEY: currentPem, JWT_PREVIOUS_PUBLIC_KEYS: previous });
    const published = keys.getJwks().keys;

    expect(published).toHaveLength(2);
    expect(published[0].kid).toBe(keys.kid);
    expect(published[1].kid).toBe(service({ JWT_PRIVATE_KEY: oldPem }).kid);
  });

  it('fails fast when the private key is missing', () => {
    expect(() => service({})).toThrow(/JWT_PRIVATE_KEY/);
  });

  it('fails fast when the private key is not valid PEM', () => {
    expect(() => service({ JWT_PRIVATE_KEY: 'not a key' })).toThrow();
  });

  it('rejects non-RSA keys', () => {
    const { privateKey } = generateKeyPairSync('ed25519');
    const pem = privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;

    expect(() => service({ JWT_PRIVATE_KEY: pem })).toThrow(/RSA/);
  });
});
