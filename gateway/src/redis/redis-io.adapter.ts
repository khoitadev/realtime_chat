import { INestApplicationContext } from '@nestjs/common';
import { IoAdapter } from '@nestjs/platform-socket.io';
import { createAdapter } from '@socket.io/redis-adapter';
import Redis from 'ioredis';
import { Server, ServerOptions } from 'socket.io';

/**
 * Socket.IO adapter backed by Redis pub/sub so that `server.to(room).emit()`
 * reaches sockets connected to any instance. pub/sub need their own
 * connections (a subscriber-mode connection cannot run normal commands).
 */
export class RedisIoAdapter extends IoAdapter {
  private readonly clients: Redis[] = [];

  constructor(
    app: INestApplicationContext,
    private readonly redis: Redis,
  ) {
    super(app);
  }

  createIOServer(port: number, options?: ServerOptions): Server {
    const server = super.createIOServer(port, options) as Server;
    const pub = this.redis.duplicate();
    const sub = this.redis.duplicate();
    this.clients.push(pub, sub);
    server.adapter(createAdapter(pub, sub));
    return server;
  }

  /** Called by Nest's SocketModule.close() after the servers are closed. */
  async dispose(): Promise<void> {
    await Promise.all(this.clients.splice(0).map((c) => c.quit().catch(() => undefined)));
  }
}
