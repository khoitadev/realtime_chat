import { createServer, IncomingMessage, ServerResponse } from 'http';
import { randomUUID } from 'crypto';
import { AddressInfo } from 'net';

export interface StubMessage {
  id: string;
  roomId: string;
  senderId: string;
  content: string;
  createdAt: string;
}

export type StubSendResult = { ok: true } | { ok: false; status: number; error: string };

export interface ChatStub {
  url: string;
  /** Configures the response for GET /internal/rooms/:roomId/members/:userId. */
  setMember(roomId: string, userId: string, isMember: boolean): void;
  /** Configures the response for GET /internal/users/:userId/room-ids. */
  setRoomIds(userId: string, roomIds: string[]): void;
  /** Configures how POST /rooms/:roomId/messages responds: accept, or a given status + error body. */
  setSendMessageBehavior(fn: (roomId: string, userId: string, content: string) => StubSendResult): void;
  lastInternalHeaders(): { secret?: string; userId?: string } | undefined;
  /** The x-correlation-id header of the most recent POST /rooms/:roomId/messages. */
  lastCorrelationId(): string | undefined;
  close(): Promise<void>;
}

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', (chunk) => (data += chunk));
    req.on('end', () => resolve(data));
  });
}

export async function startChatStub(): Promise<ChatStub> {
  const members = new Map<string, boolean>(); // `${roomId}:${userId}` -> isMember
  const roomIdsByUser = new Map<string, string[]>();
  let sendBehavior: (roomId: string, userId: string, content: string) => StubSendResult = () => ({
    ok: true,
  });
  let lastHeaders: { secret?: string; userId?: string } | undefined;
  let lastCorrelation: string | undefined;

  const server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    const url = new URL(req.url ?? '/', 'http://stub');
    lastHeaders = {
      secret: req.headers['x-internal-secret'] as string | undefined,
      userId: req.headers['x-internal-user-id'] as string | undefined,
    };

    const membersMatch = url.pathname.match(/^\/internal\/rooms\/([^/]+)\/members\/([^/]+)$/);
    if (membersMatch) {
      const [, roomId, userId] = membersMatch;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ isMember: members.get(`${roomId}:${userId}`) ?? false }));
      return;
    }

    const roomIdsMatch = url.pathname.match(/^\/internal\/users\/([^/]+)\/room-ids$/);
    if (roomIdsMatch) {
      const [, userId] = roomIdsMatch;
      res.setHeader('Content-Type', 'application/json');
      res.end(JSON.stringify({ roomIds: roomIdsByUser.get(userId) ?? [] }));
      return;
    }

    const messagesMatch = url.pathname.match(/^\/rooms\/([^/]+)\/messages$/);
    if (messagesMatch && req.method === 'POST') {
      const [, roomId] = messagesMatch;
      lastCorrelation = req.headers['x-correlation-id'] as string | undefined;
      const userId = req.headers['x-internal-user-id'] as string;
      const body = JSON.parse((await readBody(req)) || '{}') as { content: string };
      const result = sendBehavior(roomId, userId, body.content);
      res.setHeader('Content-Type', 'application/json');
      if (result.ok) {
        const message: StubMessage = {
          id: randomUUID(),
          roomId,
          senderId: userId,
          content: body.content,
          createdAt: new Date().toISOString(),
        };
        res.statusCode = 201;
        res.end(JSON.stringify(message));
      } else {
        res.statusCode = result.status;
        res.end(JSON.stringify({ message: result.error }));
      }
      return;
    }

    res.statusCode = 404;
    res.end();
  });

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    url: `http://127.0.0.1:${port}`,
    setMember: (roomId, userId, isMember) => members.set(`${roomId}:${userId}`, isMember),
    setRoomIds: (userId, roomIds) => roomIdsByUser.set(userId, roomIds),
    setSendMessageBehavior: (fn) => {
      sendBehavior = fn;
    },
    lastInternalHeaders: () => lastHeaders,
    lastCorrelationId: () => lastCorrelation,
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}
