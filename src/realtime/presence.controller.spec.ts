import { randomUUID } from 'crypto';
import { Socket as ClientSocket } from 'socket.io-client';
import { createInstance, TestInstance } from '../../test/instance';
import { cleanDatabase } from '../../test/db-cleanup';
import { cleanRedis } from '../../test/redis-cleanup';
import { connectSocket } from '../../test/helpers';
import { signToken } from '../../test/jwt';
import { PrismaService } from '../prisma/prisma.service';
import { RoomsService } from '../rooms/rooms.service';
import { WsTicketService } from './ws-ticket.service';
import { PresenceService } from './presence.service';

describe('GET /rooms/:roomId/presence', () => {
  let app: TestInstance;
  let prisma: PrismaService;
  let rooms: RoomsService;
  let sockets: ClientSocket[] = [];

  beforeAll(async () => {
    app = await createInstance();
    prisma = app.get(PrismaService);
    rooms = app.get(RoomsService);
  });

  afterAll(async () => {
    await app.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
    await cleanRedis(app.redis);
    sockets = [];
  });

  afterEach(() => {
    sockets.forEach((s) => s.disconnect());
  });

  async function registerUser(_email: string) {
    const id = randomUUID();
    return { id, accessToken: signToken(id) };
  }

  async function goOnline(userId: string) {
    const ticket = await app.get<WsTicketService>(WsTicketService).issue(userId);
    const socket = await connectSocket(app.url, ticket);
    sockets.push(socket);
    await app.get<PresenceService>(PresenceService).isOnline(userId); // ensure round-trip
    return socket;
  }

  function get(roomId: string, token?: string) {
    return fetch(`${app.url}/rooms/${roomId}/presence`, {
      headers: token ? { Authorization: `Bearer ${token}` } : {},
    });
  }

  it('returns 401 without a token', async () => {
    const res = await get('any-room');
    expect(res.status).toBe(401);
  });

  it('returns 403 when the caller is not a member of the room', async () => {
    const owner = await registerUser('pc-owner@example.com');
    const outsider = await registerUser('pc-outsider@example.com');
    const room = await rooms.createRoom(owner.id, { name: 'Private' });

    const res = await get(room.id, outsider.accessToken);

    expect(res.status).toBe(403);
  });

  it('lists only the members that are currently online', async () => {
    const me = await registerUser('pc-me@example.com');
    const online = await registerUser('pc-online@example.com');
    const offline = await registerUser('pc-offline@example.com');
    const room = await rooms.createRoom(me.id, { name: 'Team' });
    await rooms.joinRoom(online.id, room.id);
    await rooms.joinRoom(offline.id, room.id);
    await goOnline(online.id);

    const res = await get(room.id, me.accessToken);

    expect(res.status).toBe(200);
    expect(await res.json()).toEqual({ onlineUserIds: [online.id] });
  });

  it('counts a user online with two tabs once, and offline after both close', async () => {
    const me = await registerUser('pc-me2@example.com');
    const peer = await registerUser('pc-peer@example.com');
    const room = await rooms.createRoom(me.id, { name: 'Tabs' });
    await rooms.joinRoom(peer.id, room.id);
    const tab1 = await goOnline(peer.id);
    const tab2 = await goOnline(peer.id);

    tab1.disconnect();
    await new Promise((r) => setTimeout(r, 300));
    expect(await (await get(room.id, me.accessToken)).json()).toEqual({ onlineUserIds: [peer.id] });

    tab2.disconnect();
    await new Promise((r) => setTimeout(r, 300));
    expect(await (await get(room.id, me.accessToken)).json()).toEqual({ onlineUserIds: [] });
  });
});
