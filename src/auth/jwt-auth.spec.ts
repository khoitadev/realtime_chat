import { randomUUID } from 'crypto';
import { createInstance, TestInstance } from '../../test/instance';
import {
  defaultKey,
  generateTestKey,
  signHs256WithPublicKey,
  signToken,
  unsignedToken,
} from '../../test/jwt';

describe('JWT verification through JwtAuthGuard (GET /rooms)', () => {
  let app: TestInstance;

  beforeAll(async () => {
    app = await createInstance();
  });

  afterAll(async () => {
    await app.close();
  });

  function get(token?: string) {
    return fetch(`${app.url}/rooms`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  }

  const userId = () => randomUUID();

  it('accepts a valid RS256 token signed by the published key', async () => {
    const res = await get(signToken(userId()));

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual([]);
  });

  it('rejects a request without a token', async () => {
    expect((await get()).status).toBe(401);
  });

  it('rejects garbage', async () => {
    expect((await get('not.a.jwt')).status).toBe(401);
  });

  it('rejects a token whose kid is not published', async () => {
    const stranger = generateTestKey();
    expect((await get(signToken(userId(), { key: stranger }))).status).toBe(401);
  });

  it('rejects a token that claims a known kid but is signed with another private key', async () => {
    const forger = generateTestKey();
    expect((await get(signToken(userId(), { key: forger, kid: defaultKey.kid }))).status).toBe(401);
  });

  it('rejects an expired token', async () => {
    expect((await get(signToken(userId(), { expiresInSec: -60 }))).status).toBe(401);
  });

  it('rejects the wrong issuer', async () => {
    expect((await get(signToken(userId(), { issuer: 'evil-service' }))).status).toBe(401);
  });

  it('rejects the wrong audience', async () => {
    expect((await get(signToken(userId(), { audience: 'another-app' }))).status).toBe(401);
  });

  it('rejects alg:none', async () => {
    expect((await get(unsignedToken(userId()))).status).toBe(401);
  });

  it('rejects an HS256 token forged with the public key as secret (algorithm confusion)', async () => {
    expect((await get(signHs256WithPublicKey(userId()))).status).toBe(401);
  });
});
