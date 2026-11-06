import { randomUUID } from 'crypto';
import { InternalChatClient } from './internal-chat.client';
import { startChatStub, ChatStub } from '../../test/chat-stub';

describe('InternalChatClient', () => {
  let stub: ChatStub;
  let client: InternalChatClient;
  const OLD_URL = process.env.CHAT_SERVICE_URL;
  const OLD_SECRET = process.env.INTERNAL_SECRET;

  beforeAll(async () => {
    stub = await startChatStub();
    process.env.CHAT_SERVICE_URL = stub.url;
    process.env.INTERNAL_SECRET = 'test-internal-secret';
    client = new InternalChatClient();
  });

  afterAll(async () => {
    await stub.close();
    process.env.CHAT_SERVICE_URL = OLD_URL;
    process.env.INTERNAL_SECRET = OLD_SECRET;
  });

  it('checkMembership returns true/false per the stub and sends the internal secret', async () => {
    const roomId = randomUUID();
    const userId = randomUUID();
    stub.setMember(roomId, userId, true);

    expect(await client.checkMembership(roomId, userId)).toBe(true);
    expect(stub.lastInternalHeaders()?.secret).toBe('test-internal-secret');

    stub.setMember(roomId, userId, false);
    expect(await client.checkMembership(roomId, userId)).toBe(false);
  });

  it('listRoomIds returns the stub-configured list', async () => {
    const userId = randomUUID();
    stub.setRoomIds(userId, ['room-a', 'room-b']);

    expect(await client.listRoomIds(userId)).toEqual(['room-a', 'room-b']);
  });

  it('sendMessage succeeds and sends X-Internal-User-Id', async () => {
    const roomId = randomUUID();
    const userId = randomUUID();
    stub.setSendMessageBehavior(() => ({ ok: true }));

    const result = await client.sendMessage(roomId, userId, 'hello');

    expect(result).toEqual({ success: true });
    expect(stub.lastInternalHeaders()?.userId).toBe(userId);
  });

  it("sendMessage surfaces the stub's error message on failure", async () => {
    const roomId = randomUUID();
    const userId = randomUUID();
    stub.setSendMessageBehavior(() => ({
      ok: false,
      status: 403,
      error: 'Not a member of this room',
    }));

    const result = await client.sendMessage(roomId, userId, 'hello');

    expect(result).toEqual({ success: false, error: 'Not a member of this room' });
  });
});
