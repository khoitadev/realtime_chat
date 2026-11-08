import { randomUUID } from 'crypto';
import { ForbiddenException, Inject, Injectable, Logger, NotFoundException } from '@nestjs/common';
import { Message, Room, RoomMember } from '@prisma/client';
import Redis from 'ioredis';
import { PrismaService } from '../prisma/prisma.service';
import { REDIS } from '../redis/redis.constants';
import { CreateRoomDto } from './dto/create-room.dto';
import { SendMessageDto } from './dto/send-message.dto';
import { EventPublisher } from '../messaging/event.publisher';
import { buildMessageCreatedEvent } from '../messaging/events';

@Injectable()
export class RoomsService {
  private readonly logger = new Logger(RoomsService.name);

  constructor(
    private readonly prisma: PrismaService,
    @Inject(REDIS) private readonly redis: Redis,
    private readonly events: EventPublisher,
  ) {}

  async createRoom(userId: string, dto: CreateRoomDto): Promise<Room> {
    return this.prisma.room.create({
      data: {
        name: dto.name,
        createdBy: userId,
        members: { create: { userId } },
      },
    });
  }

  async joinRoom(userId: string, roomId: string): Promise<RoomMember> {
    const room = await this.prisma.room.findUnique({ where: { id: roomId } });
    if (!room) {
      throw new NotFoundException('Room not found');
    }

    return this.prisma.roomMember.upsert({
      where: { roomId_userId: { roomId, userId } },
      create: { roomId, userId },
      update: {},
    });
  }

  async listRooms(userId: string): Promise<Room[]> {
    const memberships = await this.prisma.roomMember.findMany({
      where: { userId },
      include: { room: true },
    });
    return memberships.map((m) => m.room);
  }

  async listMemberIds(roomId: string): Promise<string[]> {
    const rows = await this.prisma.roomMember.findMany({
      where: { roomId },
      select: { userId: true },
    });
    return rows.map((r) => r.userId);
  }

  async listRoomIdsOf(userId: string): Promise<string[]> {
    const rows = await this.prisma.roomMember.findMany({
      where: { userId },
      select: { roomId: true },
    });
    return rows.map((r) => r.roomId);
  }

  async isMember(userId: string, roomId: string): Promise<boolean> {
    const membership = await this.prisma.roomMember.findUnique({
      where: { roomId_userId: { roomId, userId } },
    });
    return membership !== null;
  }

  async assertMember(userId: string, roomId: string): Promise<void> {
    if (!(await this.isMember(userId, roomId))) {
      throw new ForbiddenException('Not a member of this room');
    }
  }

  async sendMessage(
    userId: string,
    roomId: string,
    dto: SendMessageDto,
    correlationId?: string,
  ): Promise<Message> {
    await this.assertMember(userId, roomId);
    const message = await this.prisma.message.create({
      data: { roomId, senderId: userId, content: dto.content },
    });
    await this.redis.publish('chat:message:new', JSON.stringify({ roomId, message }));
    await this.announceMessageCreated(message, correlationId);
    return message;
  }

  /**
   * Best-effort `message.created` event for notification-service. Never fails the send and is
   * not awaited by the caller beyond preparing the event (the publish itself runs in the
   * background and swallows its own errors).
   */
  private async announceMessageCreated(message: Message, correlationId?: string): Promise<void> {
    try {
      const memberIds = await this.listMemberIds(message.roomId);
      const recipientIds = memberIds.filter((id) => id !== message.senderId);
      if (recipientIds.length === 0) return;

      const event = buildMessageCreatedEvent({
        message,
        recipientIds,
        correlationId: correlationId ?? randomUUID(),
      });
      void this.events.publishMessageCreated(event);
    } catch (err) {
      this.logger.warn(`could not prepare message.created for ${message.id}: ${(err as Error).message}`);
    }
  }

  async getMessages(
    userId: string,
    roomId: string,
    cursor?: string,
    limit = 20,
  ): Promise<{ items: Message[]; nextCursor: string | null }> {
    await this.assertMember(userId, roomId);

    const where: {
      roomId: string;
      OR?: Array<{ createdAt: { lt: Date } } | { createdAt: Date; id: { lt: string } }>;
    } = { roomId };

    if (cursor) {
      const separatorIndex = cursor.lastIndexOf('|');
      const cursorCreatedAt = new Date(cursor.slice(0, separatorIndex));
      const cursorId = cursor.slice(separatorIndex + 1);
      where.OR = [
        { createdAt: { lt: cursorCreatedAt } },
        { createdAt: cursorCreatedAt, id: { lt: cursorId } },
      ];
    }

    const rows = await this.prisma.message.findMany({
      where,
      orderBy: [{ createdAt: 'desc' }, { id: 'desc' }],
      take: limit + 1,
    });

    const hasMore = rows.length > limit;
    const page = hasMore ? rows.slice(0, limit) : rows;
    const last = page[page.length - 1];
    const nextCursor = hasMore ? `${last.createdAt.toISOString()}|${last.id}` : null;

    return { items: page.reverse(), nextCursor };
  }
}
