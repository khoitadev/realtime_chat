import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { AddressInfo } from 'net';
import { createServer, Server as HttpServer } from 'http';
import { ProxyController } from './proxy.controller';

interface FakeBackend {
  url: string;
  lastAuth?: string;
  server: HttpServer;
}

function startFakeBackend(statusCode: number, body: unknown): Promise<FakeBackend> {
  const backend: FakeBackend = { url: '', server: null as unknown as HttpServer };
  const server = createServer((req, res) => {
    backend.lastAuth = req.headers.authorization;
    res.statusCode = statusCode;
    res.setHeader('Content-Type', 'application/json');
    res.end(JSON.stringify(body));
  });
  backend.server = server;
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const port = (server.address() as AddressInfo).port;
      backend.url = `http://127.0.0.1:${port}`;
      resolve(backend);
    });
  });
}

describe('ProxyController', () => {
  let app: INestApplication;
  let baseUrl: string;
  let authBackend: FakeBackend;
  let chatBackend: FakeBackend;

  beforeAll(async () => {
    authBackend = await startFakeBackend(200, { ok: 'from-auth' });
    chatBackend = await startFakeBackend(403, { message: 'Not a member of this room' });
    process.env.AUTH_SERVICE_URL = authBackend.url;
    process.env.CHAT_SERVICE_URL = chatBackend.url;

    const moduleRef = await Test.createTestingModule({
      imports: [HttpModule],
      controllers: [ProxyController],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
  });

  afterAll(async () => {
    await app.close();
    await new Promise((r) => authBackend.server.close(r));
    await new Promise((r) => chatBackend.server.close(r));
  });

  it('forwards /auth/* to AUTH_SERVICE_URL and returns its body/status', async () => {
    const res = await fetch(`${baseUrl}/auth/login`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: 'Bearer abc' },
      body: JSON.stringify({ email: 'a@b.com', password: 'x' }),
    });

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ ok: 'from-auth' });
    expect(authBackend.lastAuth).toBe('Bearer abc');
  });

  it('forwards /rooms* to CHAT_SERVICE_URL, preserving a non-2xx status and body', async () => {
    const res = await fetch(`${baseUrl}/rooms/some-room/messages`, {
      headers: { Authorization: 'Bearer xyz' },
    });

    expect(res.status).toBe(403);
    expect(await res.json()).toEqual({ message: 'Not a member of this room' });
    expect(chatBackend.lastAuth).toBe('Bearer xyz');
  });
});
