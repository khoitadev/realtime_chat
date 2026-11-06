import { randomUUID } from 'crypto';
import { createServer, Server as HttpServer } from 'http';
import { AddressInfo } from 'net';
import { InternalChatClient } from './internal-chat.client';
import { startChatStub, ChatStub } from '../../test/chat-stub';

/** A bare HTTP server that returns a fixed non-2xx status for every request. */
function startFailingServer(status: number): Promise<{ url: string; server: HttpServer }> {
  return new Promise((resolve) => {
    const server = createServer((_req, res) => {
      res.statusCode = status;
      res.end(JSON.stringify({ message: 'boom' }));
    });
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as AddressInfo).port;
      resolve({ url: `http://127.0.0.1:${port}`, server });
    });
  });
}

/** A bare HTTP server that records the raw (still percent-encoded) request path. */
function startCapturingServer(): Promise<{ url: string; server: HttpServer; lastPath: () => string | undefined }> {
  let lastPath: string | undefined;
  return new Promise((resolve) => {
    const server = createServer((req, res) => {
      lastPath = req.url;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ isMember: false, roomIds: [] }));
    });
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as AddressInfo).port;
      resolve({ url: `http://127.0.0.1:${port}`, server, lastPath: () => lastPath });
    });
  });
}

describe('InternalChatClient', () => {
  let stub: ChatStub;
  let client: InternalChatClient;
  const OLD_URL = process.env.CHAT_SERVICE_URL;
  const OLD_SECRET = process.env.INTERNAL_SECRET;

  beforeAll(async () => {
    stub = await startChatStub();
    process.env.CHAT_SERVICE_URL = stub.url;
    process.env.INTERNAL_SECRET = 'test-internal-secret';
    client = new InternalChatClient();
  });

  afterAll(async () => {
    await stub.close();
    process.env.CHAT_SERVICE_URL = OLD_URL;
    process.env.INTERNAL_SECRET = OLD_SECRET;
  });

  it('checkMembership returns true/false per the stub and sends the internal secret', async () => {
    const roomId = randomUUID();
    const userId = randomUUID();
    stub.setMember(roomId, userId, true);

    expect(await client.checkMembership(roomId, userId)).toBe(true);
    expect(stub.lastInternalHeaders()?.secret).toBe('test-internal-secret');

    stub.setMember(roomId, userId, false);
    expect(await client.checkMembership(roomId, userId)).toBe(false);
  });

  it('listRoomIds returns the stub-configured list', async () => {
    const userId = randomUUID();
    stub.setRoomIds(userId, ['room-a', 'room-b']);

    expect(await client.listRoomIds(userId)).toEqual(['room-a', 'room-b']);
  });

  it('sendMessage succeeds and sends X-Internal-User-Id', async () => {
    const roomId = randomUUID();
    const userId = randomUUID();
    stub.setSendMessageBehavior(() => ({ ok: true }));

    const result = await client.sendMessage(roomId, userId, 'hello');

    expect(result).toEqual({ success: true });
    expect(stub.lastInternalHeaders()?.userId).toBe(userId);
  });

  it("sendMessage surfaces the stub's error message on failure", async () => {
    const roomId = randomUUID();
    const userId = randomUUID();
    stub.setSendMessageBehavior(() => ({
      ok: false,
      status: 403,
      error: 'Not a member of this room',
    }));

    const result = await client.sendMessage(roomId, userId, 'hello');

    expect(result).toEqual({ success: false, error: 'Not a member of this room' });
  });

  describe('when chat-service responds with a non-2xx status', () => {
    let failing: { url: string; server: HttpServer };
    let failingClient: InternalChatClient;

    beforeAll(async () => {
      failing = await startFailingServer(500);
      process.env.CHAT_SERVICE_URL = failing.url;
      failingClient = new InternalChatClient();
    });

    afterAll(async () => {
      process.env.CHAT_SERVICE_URL = stub.url;
      await new Promise<void>((resolve) => failing.server.close(() => resolve()));
    });

    it('checkMembership throws a clear error instead of treating the body as a valid response', async () => {
      await expect(failingClient.checkMembership(randomUUID(), randomUUID())).rejects.toThrow(
        /chat-service responded 500/,
      );
    });

    it('listRoomIds throws a clear error instead of treating the body as a valid response', async () => {
      await expect(failingClient.listRoomIds(randomUUID())).rejects.toThrow(
        /chat-service responded 500/,
      );
    });
  });

  describe('path injection via unescaped roomId/userId', () => {
    let capturing: { url: string; server: HttpServer; lastPath: () => string | undefined };
    let capturingClient: InternalChatClient;

    beforeAll(async () => {
      capturing = await startCapturingServer();
      process.env.CHAT_SERVICE_URL = capturing.url;
      capturingClient = new InternalChatClient();
    });

    afterAll(async () => {
      process.env.CHAT_SERVICE_URL = stub.url;
      await new Promise<void>((resolve) => capturing.server.close(() => resolve()));
    });

    it('percent-encodes a roomId containing / and # before checkMembership builds the URL', async () => {
      const dangerousRoomId = '../internal/users/victim/room-ids#';
      await capturingClient.checkMembership(dangerousRoomId, randomUUID());

      const requestedPath = capturing.lastPath();
      expect(requestedPath).toBeDefined();
      // The dangerous segment must land, escaped, inside the single
      // /members/ segment it belongs to -- never break out into a
      // different path or carry a literal "/" or "#".
      expect(requestedPath).toMatch(/^\/internal\/rooms\/[^/]+\/members\/[^/]+$/);
      expect(requestedPath).not.toContain('/internal/users/victim/room-ids');
      expect(requestedPath).toContain(encodeURIComponent(dangerousRoomId));
    });

    it('percent-encodes a userId containing / and # before listRoomIds builds the URL', async () => {
      const dangerousUserId = '../rooms/TARGET/members/OTHER#';
      await capturingClient.listRoomIds(dangerousUserId);

      const requestedPath = capturing.lastPath();
      expect(requestedPath).toBeDefined();
      expect(requestedPath).toMatch(/^\/internal\/users\/[^/]+\/room-ids$/);
      expect(requestedPath).not.toContain('/rooms/TARGET/members/OTHER');
      expect(requestedPath).toContain(encodeURIComponent(dangerousUserId));
    });
  });
});
