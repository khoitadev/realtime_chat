import { randomUUID } from 'crypto';
import { Socket as ClientSocket } from 'socket.io-client';
import { createInstance, TestInstance } from '../../test/instance';
import { cleanDatabase } from '../../test/db-cleanup';
import { cleanRedis } from '../../test/redis-cleanup';
import { connectSocket, emitWithAck, sleep, waitFor } from '../../test/helpers';
import { PrismaService } from '../prisma/prisma.service';
import { RoomsService } from '../rooms/rooms.service';
import { WsTicketService } from './ws-ticket.service';
import { PresenceService } from './presence.service';

describe('presence lifecycle (heartbeat, crash, shutdown)', () => {
  const instances: TestInstance[] = [];
  let sockets: ClientSocket[] = [];
  let observerInstance: TestInstance;
  let prisma: PrismaService;
  let rooms: RoomsService;

  async function boot(presence: { heartbeatTtlSec: number; refreshIntervalMs: number }) {
    const instance = await createInstance(presence);
    instances.push(instance);
    return instance;
  }

  async function connectTo(instance: TestInstance, userId: string) {
    const ticket = await instance.get<WsTicketService>(WsTicketService).issue(userId);
    const socket = await connectSocket(instance.url, ticket);
    sockets.push(socket);
    return socket;
  }

  async function createUser(email: string) {
    return { id: randomUUID(), email, displayName: email };
  }

  beforeAll(async () => {
    observerInstance = await boot({ heartbeatTtlSec: 60, refreshIntervalMs: 20_000 });
    prisma = observerInstance.get(PrismaService);
    rooms = observerInstance.get(RoomsService);
  });

  afterAll(async () => {
    await sleep(200); // let the server finish handling the clients' last disconnects before Redis closes
    await Promise.all(instances.map((i) => i.close().catch(() => undefined)));
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
    await cleanRedis(observerInstance.redis);
    sockets = [];
  });

  afterEach(() => {
    sockets.forEach((s) => s.disconnect());
  });

  it('keeps a live socket online past the heartbeat TTL because the gateway refreshes it', async () => {
    const instance = await boot({ heartbeatTtlSec: 1, refreshIntervalMs: 200 });
    const user = await createUser('hb-live@example.com');
    await connectTo(instance, user.id);

    await sleep(1600); // > 1.5x TTL; would be offline without the refresh timer

    expect(await observerInstance.get<PresenceService>(PresenceService).isOnline(user.id)).toBe(true);
  });

  it('drops a user whose instance stopped heartbeating (simulated crash)', async () => {
    // refresh interval far longer than the TTL => this instance never refreshes in time,
    // exactly what other instances observe when it is killed with SIGKILL.
    const dying = await boot({ heartbeatTtlSec: 1, refreshIntervalMs: 600_000 });
    const user = await createUser('hb-dead@example.com');
    await connectTo(dying, user.id);
    expect(await observerInstance.get<PresenceService>(PresenceService).isOnline(user.id)).toBe(true);

    await sleep(1300);

    expect(await observerInstance.get<PresenceService>(PresenceService).isOnline(user.id)).toBe(false);
    expect(await observerInstance.redis.scard(`presence:user:${user.id}`)).toBe(0);
  });

  it('on graceful shutdown removes the instance sockets and announces online:false', async () => {
    const leaving = await boot({ heartbeatTtlSec: 60, refreshIntervalMs: 20_000 });
    const subject = await createUser('sd-subject@example.com');
    const observer = await createUser('sd-observer@example.com');
    const room = await rooms.createRoom(subject.id, { name: 'Shutdown' });
    await rooms.joinRoom(observer.id, room.id);

    const observerSocket = await connectTo(observerInstance, observer.id);
    await emitWithAck(observerSocket, 'room:subscribe', { roomId: room.id });
    const events: Array<{ userId: string; online: boolean }> = [];
    observerSocket.on('presence:update', (e) => {
      if (e.userId === subject.id) events.push(e);
    });

    await connectTo(leaving, subject.id);
    await waitFor(() => events.some((e) => e.online));

    await leaving.close();

    await waitFor(() => events.some((e) => !e.online));
    expect(await observerInstance.redis.scard(`presence:user:${subject.id}`)).toBe(0);
    expect(events.filter((e) => !e.online)).toHaveLength(1); // not duplicated by socket close handlers
  });
});
