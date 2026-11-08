import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { HttpModule } from '@nestjs/axios';
import { AddressInfo } from 'net';
import { createServer, Server as HttpServer } from 'http';
import { ProxyController } from './proxy.controller';

function startBackend(): Promise<{ url: string; server: HttpServer; seen: string[] }> {
  const seen: string[] = [];
  return new Promise((resolve) => {
    const server = createServer((req, res) => {
      seen.push(String(req.headers['x-correlation-id']));
      res.setHeader('Content-Type', 'application/json');
      res.end('{}');
    });
    server.listen(0, '127.0.0.1', () => {
      resolve({ url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`, server, seen });
    });
  });
}

describe('ProxyController correlation id', () => {
  let app: INestApplication;
  let baseUrl: string;
  let auth: Awaited<ReturnType<typeof startBackend>>;
  let chat: Awaited<ReturnType<typeof startBackend>>;

  beforeAll(async () => {
    auth = await startBackend();
    chat = await startBackend();
    process.env.AUTH_SERVICE_URL = auth.url;
    process.env.CHAT_SERVICE_URL = chat.url;
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
    await new Promise((r) => auth.server.close(r));
    await new Promise((r) => chat.server.close(r));
  });

  it('forwards a client-supplied correlation id to chat-service and echoes it back', async () => {
    const res = await fetch(`${baseUrl}/rooms`, { headers: { 'X-Correlation-Id': 'client-corr-1' } });

    expect(chat.seen.at(-1)).toBe('client-corr-1');
    expect(res.headers.get('x-correlation-id')).toBe('client-corr-1');
  });

  it('forwards it to auth-service too', async () => {
    await fetch(`${baseUrl}/auth/me`, { headers: { 'X-Correlation-Id': 'client-corr-2' } });

    expect(auth.seen.at(-1)).toBe('client-corr-2');
  });

  it('generates one when the client sends none', async () => {
    const res = await fetch(`${baseUrl}/rooms`);

    const echoed = res.headers.get('x-correlation-id');
    expect(echoed).toMatch(/^[0-9a-f-]{36}$/);
    expect(chat.seen.at(-1)).toBe(echoed);
  });

  it('replaces an unsafe value with a generated one', async () => {
    const res = await fetch(`${baseUrl}/rooms`, { headers: { 'X-Correlation-Id': 'bad value; drop table' } });

    const echoed = res.headers.get('x-correlation-id');
    expect(echoed).toMatch(/^[0-9a-f-]{36}$/);
    expect(chat.seen.at(-1)).toBe(echoed);
  });
});
