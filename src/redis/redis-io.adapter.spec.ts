import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import {
  ConnectedSocket,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { AddressInfo } from 'net';
import Redis from 'ioredis';
import { Server, Socket } from 'socket.io';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { RedisModule } from './redis.module';
import { REDIS } from './redis.constants';
import { RedisIoAdapter } from './redis-io.adapter';
import { sleep } from '../../test/helpers';

@WebSocketGateway({ transports: ['websocket'] })
class EchoGateway {
  @WebSocketServer()
  server!: Server;

  @SubscribeMessage('join')
  async join(@ConnectedSocket() client: Socket) {
    await client.join('room');
    return { ok: true };
  }

  @SubscribeMessage('say')
  say() {
    this.server.to('room').emit('said', 'hello');
    return { ok: true };
  }
}

async function boot(useAdapter: boolean) {
  const moduleRef = await Test.createTestingModule({
    imports: [RedisModule],
    providers: [EchoGateway],
  }).compile();
  const app = moduleRef.createNestApplication();
  if (useAdapter) {
    app.useWebSocketAdapter(new RedisIoAdapter(app, moduleRef.get<Redis>(REDIS)));
  }
  await app.listen(0);
  const port = (app.getHttpServer().address() as AddressInfo).port;
  return { app, url: `http://localhost:${port}` };
}

function connect(url: string): Promise<ClientSocket> {
  return new Promise((resolve, reject) => {
    const socket = io(url, { transports: ['websocket'], forceNew: true, reconnection: false });
    socket.on('connect', () => resolve(socket));
    socket.on('connect_error', reject);
  });
}

describe('RedisIoAdapter', () => {
  const apps: INestApplication[] = [];
  const clients: ClientSocket[] = [];

  afterEach(async () => {
    clients.splice(0).forEach((c) => c.disconnect());
    await Promise.all(apps.splice(0).map((a) => a.close()));
  });

  it('delivers a room broadcast emitted on instance A to a client on instance B', async () => {
    const a = await boot(true);
    const b = await boot(true);
    apps.push(a.app, b.app);
    const ca = await connect(a.url);
    const cb = await connect(b.url);
    clients.push(ca, cb);
    await ca.emitWithAck('join');
    await cb.emitWithAck('join');

    const received = new Promise<string>((resolve) => cb.once('said', resolve));
    await ca.emitWithAck('say');

    expect(await received).toBe('hello');
  });

  it('control: without the adapter a broadcast does NOT cross instances', async () => {
    const a = await boot(false);
    const b = await boot(false);
    apps.push(a.app, b.app);
    const ca = await connect(a.url);
    const cb = await connect(b.url);
    clients.push(ca, cb);
    await ca.emitWithAck('join');
    await cb.emitWithAck('join');

    let receivedOnB = false;
    cb.on('said', () => {
      receivedOnB = true;
    });
    await ca.emitWithAck('say');
    await sleep(300);

    expect(receivedOnB).toBe(false);
  });
});
