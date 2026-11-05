import { Controller, Get, Param, UseGuards } from '@nestjs/common';
import { InternalAuthGuard } from '../auth/internal-auth.guard';
import { RoomsService } from './rooms.service';

@Controller('internal')
@UseGuards(InternalAuthGuard)
export class InternalController {
  constructor(private readonly roomsService: RoomsService) {}

  @Get('rooms/:roomId/members/:userId')
  async checkMembership(
    @Param('roomId') roomId: string,
    @Param('userId') userId: string,
  ): Promise<{ isMember: boolean }> {
    return { isMember: await this.roomsService.isMember(userId, roomId) };
  }

  @Get('users/:userId/room-ids')
  async roomIds(@Param('userId') userId: string): Promise<{ roomIds: string[] }> {
    return { roomIds: await this.roomsService.listRoomIdsOf(userId) };
  }
}
