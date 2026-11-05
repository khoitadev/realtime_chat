import { randomUUID } from 'crypto';
import { createInstance, TestInstance } from '../../test/instance';
import { cleanRedis } from '../../test/redis-cleanup';
import { connectSocket } from '../../test/helpers';
import { signToken } from '../../test/jwt';

describe('POST /ws-ticket', () => {
  let app: TestInstance;

  beforeAll(async () => {
    app = await createInstance();
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    await cleanRedis(app.redis);
  });

  it('returns 401 without a token', async () => {
    const res = await fetch(`${app.url}/ws-ticket`, { method: 'POST' });
    expect(res.status).toBe(401);
  });

  it('returns a 48-char hex ticket with expiresIn 30 for a valid access token', async () => {
    const res = await fetch(`${app.url}/ws-ticket`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${signToken(randomUUID())}` },
    });

    expect(res.status).toBe(201);
    const body = await res.json();
    expect(body.expiresIn).toBe(30);
    expect(body.ticket).toMatch(/^[0-9a-f]{48}$/);
  });

  it('issues a ticket that opens a websocket exactly once', async () => {
    const res = await fetch(`${app.url}/ws-ticket`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${signToken(randomUUID())}` },
    });
    const { ticket } = await res.json();

    const socket = await connectSocket(app.url, ticket);
    expect(socket.connected).toBe(true);
    socket.disconnect();

    await expect(connectSocket(app.url, ticket)).rejects.toBeDefined();
  });

  it('no longer serves the old POST /auth/ws-ticket route', async () => {
    const res = await fetch(`${app.url}/auth/ws-ticket`, {
      method: 'POST',
      headers: { Authorization: `Bearer ${signToken(randomUUID())}` },
    });
    expect(res.status).toBe(404);
  });
});
