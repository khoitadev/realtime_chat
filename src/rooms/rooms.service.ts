import { ForbiddenException, Injectable, NotFoundException } from '@nestjs/common';
import { Message, Room, RoomMember } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { CreateRoomDto } from './dto/create-room.dto';
import { SendMessageDto } from './dto/send-message.dto';

@Injectable()
export class RoomsService {
  constructor(private readonly prisma: PrismaService) {}

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

  private async assertMember(userId: string, roomId: string): Promise<void> {
    const membership = await this.prisma.roomMember.findUnique({
      where: { roomId_userId: { roomId, userId } },
    });
    if (!membership) {
      throw new ForbiddenException('Not a member of this room');
    }
  }

  async sendMessage(userId: string, roomId: string, dto: SendMessageDto): Promise<Message> {
    await this.assertMember(userId, roomId);
    return this.prisma.message.create({
      data: { roomId, senderId: userId, content: dto.content },
    });
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
