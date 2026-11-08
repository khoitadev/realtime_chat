import { Body, Controller, Get, Headers, Param, Post, Query, Req, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { JwtOrInternalAuthGuard } from '../auth/jwt-or-internal-auth.guard';
import { RoomsService } from './rooms.service';
import { CreateRoomDto } from './dto/create-room.dto';
import { SendMessageDto } from './dto/send-message.dto';
import { sanitizeCorrelationId } from '../messaging/correlation';

@Controller('rooms')
export class RoomsController {
  constructor(private readonly roomsService: RoomsService) {}

  @Post()
  @UseGuards(JwtAuthGuard)
  createRoom(@Req() req: { user: { id: string } }, @Body() dto: CreateRoomDto) {
    return this.roomsService.createRoom(req.user.id, dto);
  }

  @Post(':roomId/join')
  @UseGuards(JwtAuthGuard)
  joinRoom(@Req() req: { user: { id: string } }, @Param('roomId') roomId: string) {
    return this.roomsService.joinRoom(req.user.id, roomId);
  }

  @Get()
  @UseGuards(JwtAuthGuard)
  listRooms(@Req() req: { user: { id: string } }) {
    return this.roomsService.listRooms(req.user.id);
  }

  @Get(':roomId/messages')
  @UseGuards(JwtAuthGuard)
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
  @UseGuards(JwtOrInternalAuthGuard)
  sendMessage(
    @Req() req: { user: { id: string } },
    @Param('roomId') roomId: string,
    @Body() dto: SendMessageDto,
    @Headers('x-correlation-id') correlationId?: string,
  ) {
    return this.roomsService.sendMessage(req.user.id, roomId, dto, sanitizeCorrelationId(correlationId));
  }
}
