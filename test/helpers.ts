import { io, Socket as ClientSocket } from 'socket.io-client';

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export function connectSocket(url: string, ticket: string): Promise<ClientSocket> {
  return new Promise((resolve, reject) => {
    const socket = io(url, {
      auth: { ticket },
      transports: ['websocket'],
      reconnection: false,
      forceNew: true,
    });
    socket.on('connect', () => resolve(socket));
    socket.on('connect_error', (err) => reject(err));
  });
}

export function emitWithAck<T>(socket: ClientSocket, event: string, payload?: unknown): Promise<T> {
  return new Promise((resolve) => socket.emit(event, payload, resolve));
}

/** Polls until fn() is truthy; throws on timeout. */
export async function waitFor(
  fn: () => boolean | Promise<boolean>,
  timeoutMs = 3000,
  intervalMs = 25,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return;
    await sleep(intervalMs);
  }
  throw new Error(`waitFor timed out after ${timeoutMs}ms`);
}
