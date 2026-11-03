import { Controller, Get, Param, Req, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { RoomsService } from '../rooms/rooms.service';
import { PresenceService } from './presence.service';

@Controller('rooms')
@UseGuards(JwtAuthGuard)
export class PresenceController {
  constructor(
    private readonly roomsService: RoomsService,
    private readonly presenceService: PresenceService,
  ) {}

  @Get(':roomId/presence')
  async getPresence(
    @Req() req: { user: { id: string } },
    @Param('roomId') roomId: string,
  ): Promise<{ onlineUserIds: string[] }> {
    await this.roomsService.assertMember(req.user.id, roomId);
    const memberIds = await this.roomsService.listMemberIds(roomId);
    return { onlineUserIds: await this.presenceService.onlineAmong(memberIds) };
  }
}
