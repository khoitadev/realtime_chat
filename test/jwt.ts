import { JwtService } from '@nestjs/jwt';
import { createHash, generateKeyPairSync } from 'crypto';
import { createServer } from 'http';
import { AddressInfo } from 'net';

export interface TestKey {
  kid: string;
  privatePem: string;
  publicPem: string;
  jwk: Record<string, string>;
}

export function generateTestKey(): TestKey {
  const { privateKey, publicKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  const { kty, n, e } = publicKey.export({ format: 'jwk' }) as {
    kty: string;
    n: string;
    e: string;
  };
  const kid = createHash('sha256').update(JSON.stringify({ e, kty, n })).digest('base64url');
  return {
    kid,
    privatePem: privateKey.export({ type: 'pkcs8', format: 'pem' }) as string,
    publicPem: publicKey.export({ type: 'spki', format: 'pem' }) as string,
    jwk: { kty, n, e, kid, use: 'sig', alg: 'RS256' },
  };
}

export const defaultKey = generateTestKey();

export interface SignTokenOptions {
  key?: TestKey;
  kid?: string;
  issuer?: string;
  audience?: string;
  expiresInSec?: number;
}

/** Signs an RS256 access token like auth-service does. */
export function signToken(userId: string, opts: SignTokenOptions = {}): string {
  const key = opts.key ?? defaultKey;
  return new JwtService().sign(
    { sub: userId },
    {
      privateKey: key.privatePem,
      algorithm: 'RS256',
      keyid: opts.kid ?? key.kid,
      issuer: opts.issuer ?? 'auth-service',
      audience: opts.audience ?? 'realtime-chat',
      expiresIn: opts.expiresInSec ?? 900,
    },
  );
}

/** Classic algorithm-confusion forgery: HS256 signed with the public key as the secret. */
export function signHs256WithPublicKey(userId: string, key: TestKey = defaultKey): string {
  return new JwtService().sign(
    { sub: userId },
    {
      secret: key.publicPem,
      algorithm: 'HS256',
      keyid: key.kid,
      issuer: 'auth-service',
      audience: 'realtime-chat',
      expiresIn: 900,
    },
  );
}

export function unsignedToken(userId: string): string {
  const enc = (o: unknown) => Buffer.from(JSON.stringify(o)).toString('base64url');
  const header = enc({ alg: 'none', typ: 'JWT', kid: defaultKey.kid });
  const payload = enc({
    sub: userId,
    iss: 'auth-service',
    aud: 'realtime-chat',
    exp: Math.floor(Date.now() / 1000) + 900,
  });
  return `${header}.${payload}.`;
}

export interface JwksServer {
  url: string;
  hits(): number;
  setKeys(keys: TestKey[]): void;
  setDown(down: boolean): void;
  close(): Promise<void>;
}

/** A real HTTP server that serves a JWKS (network-level test infrastructure). */
export async function startJwksServer(initial: TestKey[] = [defaultKey]): Promise<JwksServer> {
  let keys = initial;
  let down = false;
  let hits = 0;

  const server = createServer((req, res) => {
    if (req.url !== '/.well-known/jwks.json') {
      res.statusCode = 404;
      res.end();
      return;
    }
    hits++;
    if (down) {
      res.statusCode = 503;
      res.end();
      return;
    }
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify({ keys: keys.map((k) => k.jwk) }));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    url: `http://127.0.0.1:${port}/.well-known/jwks.json`,
    hits: () => hits,
    setKeys: (next) => {
      keys = next;
    },
    setDown: (next) => {
      down = next;
    },
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}

let shared: { server: JwksServer; refs: number } | undefined;

/** Shared JWKS server for app-level specs; sets AUTH_JWKS_URL for the app under test. */
export async function acquireJwksServer(): Promise<JwksServer> {
  if (!shared) {
    shared = { server: await startJwksServer(), refs: 0 };
  }
  shared.refs++;
  process.env.AUTH_JWKS_URL = shared.server.url;
  return shared.server;
}

export async function releaseJwksServer(): Promise<void> {
  if (!shared) return;
  shared.refs--;
  if (shared.refs <= 0) {
    const { server } = shared;
    shared = undefined;
    await server.close();
  }
}
