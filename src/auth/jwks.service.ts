import { Inject, Injectable, Logger, UnauthorizedException } from '@nestjs/common';
import { createPublicKey } from 'crypto';

export const JWKS_OPTIONS = Symbol('JWKS_OPTIONS');

export interface JwksOptions {
  url: string;
  /** After this long the cached key set is considered stale and is refreshed. */
  ttlMs: number;
  /** Minimum gap between two fetch attempts (protects auth-service from unknown-kid floods). */
  minRefetchMs: number;
}

interface PublishedJwk {
  kid?: string;
  kty?: string;
  n?: string;
  e?: string;
}

@Injectable()
export class JwksService {
  private readonly logger = new Logger(JwksService.name);
  private keys = new Map<string, string>(); // kid -> SPKI PEM
  private lastSuccessAt = 0;
  private lastAttemptAt = 0;
  private inflight?: Promise<void>;

  constructor(@Inject(JWKS_OPTIONS) private readonly options: JwksOptions) {}

  async getPublicKeyPem(kid: string | undefined): Promise<string> {
    if (!kid) {
      throw new UnauthorizedException('Missing key id');
    }

    const now = Date.now();
    const cached = this.keys.get(kid);
    const fresh = now - this.lastSuccessAt < this.options.ttlMs;
    if (cached && fresh) {
      return cached;
    }

    if (now - this.lastAttemptAt >= this.options.minRefetchMs) {
      await this.refresh();
    } else if (this.inflight) {
      await this.inflight;
    }

    // After a failed refresh the previous key set is still in place (stale-on-error).
    const key = this.keys.get(kid);
    if (!key) {
      throw new UnauthorizedException('Unknown signing key');
    }
    return key;
  }

  private refresh(): Promise<void> {
    this.lastAttemptAt = Date.now();
    this.inflight = this.fetchKeys().finally(() => {
      this.inflight = undefined;
    });
    return this.inflight;
  }

  private async fetchKeys(): Promise<void> {
    try {
      const res = await fetch(this.options.url, { signal: AbortSignal.timeout(3000) });
      if (!res.ok) {
        throw new Error(`JWKS endpoint responded ${res.status}`);
      }
      const body = (await res.json()) as { keys?: PublishedJwk[] };

      const next = new Map<string, string>();
      for (const jwk of body.keys ?? []) {
        if (!jwk.kid || jwk.kty !== 'RSA') continue;
        const pem = createPublicKey({ key: jwk as never, format: 'jwk' }).export({
          type: 'spki',
          format: 'pem',
        }) as string;
        next.set(jwk.kid, pem);
      }
      this.keys = next;
      this.lastSuccessAt = Date.now();
    } catch (err) {
      this.logger.warn(`JWKS refresh failed: ${(err as Error).message}`);
    }
  }
}
