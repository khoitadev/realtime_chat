import { Injectable } from '@nestjs/common';

const INTERNAL_REQUEST_TIMEOUT_MS = 5000;

@Injectable()
export class InternalChatClient {
  private get baseUrl(): string {
    return process.env.CHAT_SERVICE_URL ?? 'http://localhost:3000';
  }

  private get secret(): string {
    return process.env.INTERNAL_SECRET ?? '';
  }

  async checkMembership(roomId: string, userId: string): Promise<boolean> {
    const res = await fetch(`${this.baseUrl}/internal/rooms/${roomId}/members/${userId}`, {
      headers: { 'x-internal-secret': this.secret },
      signal: AbortSignal.timeout(INTERNAL_REQUEST_TIMEOUT_MS),
    });
    if (!res.ok) {
      throw new Error(`checkMembership failed: chat-service responded ${res.status}`);
    }
    const body = (await res.json()) as { isMember: boolean };
    return body.isMember;
  }

  async listRoomIds(userId: string): Promise<string[]> {
    const res = await fetch(`${this.baseUrl}/internal/users/${userId}/room-ids`, {
      headers: { 'x-internal-secret': this.secret },
      signal: AbortSignal.timeout(INTERNAL_REQUEST_TIMEOUT_MS),
    });
    if (!res.ok) {
      throw new Error(`listRoomIds failed: chat-service responded ${res.status}`);
    }
    const body = (await res.json()) as { roomIds: string[] };
    return body.roomIds;
  }

  async sendMessage(
    roomId: string,
    userId: string,
    content: string,
  ): Promise<{ success: true } | { success: false; error: string }> {
    const res = await fetch(`${this.baseUrl}/rooms/${roomId}/messages`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'x-internal-secret': this.secret,
        'x-internal-user-id': userId,
      },
      body: JSON.stringify({ content }),
      signal: AbortSignal.timeout(INTERNAL_REQUEST_TIMEOUT_MS),
    });

    if (res.ok) {
      return { success: true };
    }
    const body = (await res.json().catch(() => ({ message: 'Invalid request' }))) as {
      message?: string;
    };
    return { success: false, error: body.message ?? 'Invalid request' };
  }
}
