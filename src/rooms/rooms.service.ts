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
}
