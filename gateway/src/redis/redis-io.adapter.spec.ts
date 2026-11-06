import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import {
  ConnectedSocket,
  MessageBody,
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

@WebSocketGateway({ cors: true, transports: ['websocket'] })
class EchoGateway {
  @WebSocketServer() server!: Server;

  @SubscribeMessage('join')
  join(@ConnectedSocket() client: Socket, @MessageBody() room: string) {
    client.join(room);
  }

  @SubscribeMessage('broadcast')
  broadcast(@MessageBody() payload: { room: string; text: string }) {
    this.server.to(payload.room).emit('echo', payload.text);
  }
}

async function boot() {
  const moduleRef = await Test.createTestingModule({
    imports: [RedisModule],
    providers: [EchoGateway],
  }).compile();
  const app = moduleRef.createNestApplication();
  const redis = app.get<Redis>(REDIS);
  app.useWebSocketAdapter(new RedisIoAdapter(app, redis));
  await app.listen(0);
  const port = (app.getHttpServer().address() as AddressInfo).port;
  return { app, url: `http://localhost:${port}` };
}

function connect(url: string): Promise<ClientSocket> {
  return new Promise((resolve, reject) => {
    const socket = io(url, { transports: ['websocket'], reconnection: false, forceNew: true });
    socket.on('connect', () => resolve(socket));
    socket.on('connect_error', reject);
  });
}

describe('RedisIoAdapter', () => {
  it('fans out .to(room).emit() across two separate app instances', async () => {
    const instanceA = await boot();
    const instanceB = await boot();

    const socketOnA = await connect(instanceA.url);
    const socketOnB = await connect(instanceB.url);
    socketOnB.emit('join', 'room-1');
    await sleep(100); // let the join propagate through the adapter

    const received = new Promise<string>((resolve) => socketOnB.once('echo', resolve));
    socketOnA.emit('broadcast', { room: 'room-1', text: 'hello cross-instance' });

    expect(await received).toBe('hello cross-instance');

    socketOnA.disconnect();
    socketOnB.disconnect();
    await instanceA.app.close();
    await instanceB.app.close();
  });
});
