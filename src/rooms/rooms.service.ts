import { Injectable } from '@nestjs/common';
import { Room } from '@prisma/client';
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
}
