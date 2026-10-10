import { Body, Controller, Get, Param, Post, Query, Req, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { RoomsService } from './rooms.service';
import { CreateRoomDto } from './dto/create-room.dto';
import { SendMessageDto } from './dto/send-message.dto';

@Controller('rooms')
@UseGuards(JwtAuthGuard)
export class RoomsController {
  constructor(private readonly roomsService: RoomsService) {}

  @Post()
  createRoom(@Req() req: { user: { id: string } }, @Body() dto: CreateRoomDto) {
    return this.roomsService.createRoom(req.user.id, dto);
  }

  @Post(':roomId/join')
  joinRoom(@Req() req: { user: { id: string } }, @Param('roomId') roomId: string) {
    return this.roomsService.joinRoom(req.user.id, roomId);
  }

  @Get()
  listRooms(@Req() req: { user: { id: string } }) {
    return this.roomsService.listRooms(req.user.id);
  }

  @Get(':roomId/messages')
  getMessages(
    @Req() req: { user: { id: string } },
    @Param('roomId') roomId: string,
    @Query('cursor') cursor?: string,
    @Query('limit') limit?: string,
  ) {
    const parsedLimit = limit ? parseInt(limit, 10) : 20;
    return this.roomsService.getMessages(req.user.id, roomId, cursor, parsedLimit);
  }

  @Post(':roomId/messages')
  sendMessage(
    @Req() req: { user: { id: string } },
    @Param('roomId') roomId: string,
    @Body() dto: SendMessageDto,
  ) {
    return this.roomsService.sendMessage(req.user.id, roomId, dto);
  }
}
