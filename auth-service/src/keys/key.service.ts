import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { createHash, createPrivateKey, createPublicKey, KeyObject } from 'crypto';

export type Jwk = {
  kty: string;
  n: string;
  e: string;
  kid: string;
  use: 'sig';
  alg: 'RS256';
};

/** RFC 7638-style thumbprint of an RSA public key => stable `kid`. */
function toJwk(publicKey: KeyObject): Jwk {
  const { kty, n, e } = publicKey.export({ format: 'jwk' }) as {
    kty: string;
    n: string;
    e: string;
  };
  const kid = createHash('sha256').update(JSON.stringify({ e, kty, n })).digest('base64url');
  return { kty, n, e, kid, use: 'sig', alg: 'RS256' };
}

@Injectable()
export class KeyService {
  readonly privatePem: string;
  readonly publicPem: string;
  readonly kid: string;
  private readonly jwks: Jwk[];

  constructor(config: ConfigService) {
    const raw = config.get<string>('JWT_PRIVATE_KEY');
    if (!raw) {
      throw new Error('JWT_PRIVATE_KEY is required');
    }

    // .env files commonly store PEM on one line with literal \n sequences.
    const privateKey = createPrivateKey(raw.replace(/\\n/g, '\n'));
    if (privateKey.asymmetricKeyType !== 'rsa') {
      throw new Error('JWT_PRIVATE_KEY must be an RSA key (RS256)');
    }

    const publicKey = createPublicKey(privateKey);
    this.privatePem = privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;
    this.publicPem = publicKey.export({ type: 'spki', format: 'pem' }) as string;

    const current = toJwk(publicKey);
    this.kid = current.kid;

    const previous = (config.get<string>('JWT_PREVIOUS_PUBLIC_KEYS') ?? '')
      .replace(/\\n/g, '\n')
      .split(/(?=-----BEGIN PUBLIC KEY-----)/)
      .map((pem) => pem.trim())
      .filter(Boolean)
      .map((pem) => toJwk(createPublicKey(pem)))
      .filter((jwk) => jwk.kid !== current.kid);

    const seen = new Set<string>();
    this.jwks = [current, ...previous].filter((jwk) => !seen.has(jwk.kid) && !!seen.add(jwk.kid));
  }

  getJwks(): { keys: Jwk[] } {
    return { keys: this.jwks };
  }
}
