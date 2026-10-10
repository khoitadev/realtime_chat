import { Injectable, NotFoundException } from '@nestjs/common';
import { Room, RoomMember } from '@prisma/client';
import { PrismaService } from '../prisma/prisma.service';
import { CreateRoomDto } from './dto/create-room.dto';

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
}
