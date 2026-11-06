import { Controller, Post, Req, UseGuards } from '@nestjs/common';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { WS_TICKET_TTL_SEC, WsTicketService } from './ws-ticket.service';

@Controller('ws-ticket')
@UseGuards(JwtAuthGuard)
export class WsTicketController {
  constructor(private readonly wsTicketService: WsTicketService) {}

  @Post()
  async issue(@Req() req: { user: { id: string } }) {
    return {
      ticket: await this.wsTicketService.issue(req.user.id),
      expiresIn: WS_TICKET_TTL_SEC,
    };
  }
}
