import { Injectable } from '@nestjs/common';
import { randomBytes } from 'crypto';

@Injectable()
export class WsTicketService {
  private tickets = new Map<string, { userId: string; expiresAt: number }>();

  issue(userId: string, ttlMs = 30_000): string {
    const ticket = randomBytes(24).toString('hex');
    this.tickets.set(ticket, { userId, expiresAt: Date.now() + ttlMs });
    return ticket;
  }

  consume(ticket: string): string | null {
    const entry = this.tickets.get(ticket);
    this.tickets.delete(ticket); // one-time use: delete regardless of outcome
    if (!entry || entry.expiresAt < Date.now()) {
      return null;
    }
    return entry.userId;
  }
}
