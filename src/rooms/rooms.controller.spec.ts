import { randomUUID } from 'crypto';
import { createInstance, TestInstance } from '../../test/instance';
import { cleanDatabase } from '../../test/db-cleanup';
import { signToken } from '../../test/jwt';
import { PrismaService } from '../prisma/prisma.service';
import { RoomsService } from './rooms.service';
import { listenForEvents, resetExchange } from '../../test/amqp';
import { waitFor } from '../../test/helpers';

describe('POST /rooms/:roomId/messages (HTTP, real guards composed)', () => {
  let app: TestInstance;
  let prisma: PrismaService;
  let rooms: RoomsService;
  const OLD_SECRET = process.env.INTERNAL_SECRET;

  beforeAll(async () => {
    process.env.INTERNAL_SECRET = 'test-internal-secret';
    app = await createInstance();
    prisma = app.get(PrismaService);
    rooms = app.get(RoomsService);
  });

  afterAll(async () => {
    await app.close();
    process.env.INTERNAL_SECRET = OLD_SECRET;
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  function postMessage(
    roomId: string,
    body: { content: string },
    headers: Record<string, string> = {},
  ) {
    return fetch(`${app.url}/rooms/${roomId}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
      body: JSON.stringify(body),
    });
  }

  it('accepts a real signed JWT and creates the message', async () => {
    const owner = randomUUID();
    const room = await rooms.createRoom(owner, { name: 'JWT room' });
    const token = signToken(owner);

    const res = await postMessage(room.id, { content: 'hello from jwt' }, {
      Authorization: `Bearer ${token}`,
    });

    expect(res.status).toBeLessThan(300);
    const body = (await res.json()) as { content: string; senderId: string };
    expect(body.content).toBe('hello from jwt');
    expect(body.senderId).toBe(owner);
  });

  it('accepts real internal headers with no JWT/Authorization header at all', async () => {
    const owner = randomUUID();
    const room = await rooms.createRoom(owner, { name: 'Internal room' });

    const res = await postMessage(room.id, { content: 'hello from gateway' }, {
      'x-internal-secret': 'test-internal-secret',
      'x-internal-user-id': owner,
    });

    expect(res.status).toBeLessThan(300);
    const body = (await res.json()) as { content: string; senderId: string };
    expect(body.content).toBe('hello from gateway');
    expect(body.senderId).toBe(owner);
  });

  it('rejects the request when neither a JWT nor internal headers are present', async () => {
    const owner = randomUUID();
    const room = await rooms.createRoom(owner, { name: 'No auth room' });

    const res = await postMessage(room.id, { content: 'should not land' });

    expect(res.status).toBe(401);
  });

  describe('the other routes on this controller still require a real JWT only', () => {
    it('GET /rooms rejects internal headers (no JWT bypass) with 401', async () => {
      const owner = randomUUID();
      await rooms.createRoom(owner, { name: 'JWT-only room' });

      const res = await fetch(`${app.url}/rooms`, {
        headers: {
          'x-internal-secret': 'test-internal-secret',
          'x-internal-user-id': owner,
        },
      });

      expect(res.status).toBe(401);
    });

    it('GET /rooms returns 401 with no token', async () => {
      const res = await fetch(`${app.url}/rooms`);
      expect(res.status).toBe(401);
    });

    it('GET /rooms succeeds with a real JWT', async () => {
      const owner = randomUUID();
      const room = await rooms.createRoom(owner, { name: 'Listed room' });
      const token = signToken(owner);

      const res = await fetch(`${app.url}/rooms`, {
        headers: { Authorization: `Bearer ${token}` },
      });

      expect(res.status).toBe(200);
      const body = (await res.json()) as Array<{ id: string }>;
      expect(body.map((r) => r.id)).toContain(room.id);
    });

    it('POST /rooms (createRoom) rejects internal headers with 401', async () => {
      const res = await fetch(`${app.url}/rooms`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'x-internal-secret': 'test-internal-secret',
          'x-internal-user-id': randomUUID(),
        },
        body: JSON.stringify({ name: 'Should not be created' }),
      });

      expect(res.status).toBe(401);
    });

    it('POST /:roomId/join rejects internal headers with 401', async () => {
      const owner = randomUUID();
      const room = await rooms.createRoom(owner, { name: 'Join room' });

      const res = await fetch(`${app.url}/rooms/${room.id}/join`, {
        method: 'POST',
        headers: {
          'x-internal-secret': 'test-internal-secret',
          'x-internal-user-id': randomUUID(),
        },
      });

      expect(res.status).toBe(401);
    });

    it('GET /:roomId/messages rejects internal headers with 401', async () => {
      const owner = randomUUID();
      const room = await rooms.createRoom(owner, { name: 'Messages room' });

      const res = await fetch(`${app.url}/rooms/${room.id}/messages`, {
        headers: {
          'x-internal-secret': 'test-internal-secret',
          'x-internal-user-id': owner,
        },
      });

      expect(res.status).toBe(401);
    });
  });

  describe('X-Correlation-Id', () => {
    async function sendWithHeader(headerValue: string) {
      const sender = randomUUID();
      const other = randomUUID();
      const room = await rooms.createRoom(sender, { name: 'Corr room' });
      await rooms.joinRoom(other, room.id);
      await resetExchange();
      await new Promise((r) => setTimeout(r, 50));
      const listener = await listenForEvents();
      try {
        const res = await postMessage(room.id, { content: 'corr test' }, {
          Authorization: `Bearer ${signToken(sender)}`,
          'X-Correlation-Id': headerValue,
        });
        expect(res.status).toBeLessThan(300);
        await waitFor(() => listener.messages.length === 1);
        return JSON.parse(listener.messages[0].content.toString());
      } finally {
        await listener.close();
      }
    }

    it('propagates a valid X-Correlation-Id into the event', async () => {
      const event = await sendWithHeader('ctrl-corr-1');
      expect(event.correlationId).toBe('ctrl-corr-1');
    });

    it('replaces an invalid X-Correlation-Id with a generated UUID', async () => {
      const event = await sendWithHeader('bad value with spaces');
      expect(event.correlationId).not.toBe('bad value with spaces');
      expect(event.correlationId).toMatch(/^[0-9a-f-]{36}$/);
    });
  });
});
