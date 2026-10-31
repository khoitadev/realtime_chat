import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { AddressInfo } from 'net';
import { io, Socket as ClientSocket } from 'socket.io-client';
import { EventsGateway } from './events.gateway';
import { WsTicketService } from './ws-ticket.service';
import { RoomsService } from '../rooms/rooms.service';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';

describe('EventsGateway', () => {
  let app: INestApplication;
  let prisma: PrismaService;
  let wsTicketService: WsTicketService;
  let roomsService: RoomsService;
  let baseUrl: string;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      providers: [EventsGateway, WsTicketService, RoomsService, PrismaService],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);

    const address = app.getHttpServer().address() as AddressInfo;
    baseUrl = `http://localhost:${address.port}`;

    prisma = moduleRef.get(PrismaService);
    wsTicketService = moduleRef.get(WsTicketService);
    roomsService = moduleRef.get(RoomsService);
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await app.close();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
  });

  async function createUser(email: string) {
    return prisma.user.create({ data: { email, passwordHash: 'x', displayName: email } });
  }

  function connectWithTicket(ticket: string): Promise<ClientSocket> {
    return new Promise((resolve, reject) => {
      const socket = io(baseUrl, { auth: { ticket }, reconnection: false, forceNew: true });
      socket.on('connect', () => resolve(socket));
      socket.on('connect_error', (err) => reject(err));
    });
  }

  describe('handleConnection', () => {
    it('accepts a connection with a valid ticket', async () => {
      const user = await createUser('wsuser@example.com');
      const ticket = wsTicketService.issue(user.id);

      const socket = await connectWithTicket(ticket);
      expect(socket.connected).toBe(true);
      socket.disconnect();
    });

    it('rejects a connection with an unknown ticket', async () => {
      await expect(connectWithTicket('not-a-real-ticket')).rejects.toBeDefined();
    });

    it('rejects a connection when the ticket is reused', async () => {
      const user = await createUser('wsuser2@example.com');
      const ticket = wsTicketService.issue(user.id);

      const first = await connectWithTicket(ticket);
      first.disconnect();

      await expect(connectWithTicket(ticket)).rejects.toBeDefined();
    });
  });
});
