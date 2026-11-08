import { createServer, IncomingMessage, ServerResponse } from 'http';
import { AddressInfo } from 'net';

export interface RecordedRequest {
  n: number;
  at: number;
  headers: IncomingMessage['headers'];
  body: any;
}

export interface SinkResponse {
  status?: number;
  delayMs?: number;
  destroy?: boolean;
  /** When set, respond with this Location header (use with a 3xx status). */
  location?: string;
}

export interface WebhookSink {
  url: string;
  requests: RecordedRequest[];
  setResponder(fn: (req: RecordedRequest) => SinkResponse): void;
  maxConcurrent(): number;
  close(): Promise<void>;
}

/** A real HTTP server standing in for the external push provider. */
export async function startWebhookSink(): Promise<WebhookSink> {
  const requests: RecordedRequest[] = [];
  let responder: (req: RecordedRequest) => SinkResponse = () => ({ status: 200 });
  let active = 0;
  let max = 0;
  const timers = new Set<NodeJS.Timeout>();

  const server = createServer((req: IncomingMessage, res: ServerResponse) => {
    let raw = '';
    req.on('data', (c) => (raw += c));
    req.on('end', async () => {
      active++;
      max = Math.max(max, active);
      let body: unknown = raw;
      try {
        body = JSON.parse(raw);
      } catch {
        /* keep raw */
      }
      const recorded: RecordedRequest = { n: requests.length + 1, at: Date.now(), headers: req.headers, body };
      requests.push(recorded);
      const response = responder(recorded);
      if (response.delayMs) {
        await new Promise<void>((r) => {
          const t = setTimeout(() => {
            timers.delete(t);
            r();
          }, response.delayMs);
          timers.add(t);
        });
      }
      active--;
      if (response.destroy) {
        req.socket.destroy();
        return;
      }
      res.statusCode = response.status ?? 200;
      if (response.location) res.setHeader('Location', response.location);
      res.end('{}');
    });
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const port = (server.address() as AddressInfo).port;

  return {
    url: `http://127.0.0.1:${port}/hook`,
    requests,
    setResponder: (fn) => {
      responder = fn;
    },
    maxConcurrent: () => max,
    close: () =>
      new Promise<void>((resolve) => {
        timers.forEach(clearTimeout);
        timers.clear();
        server.closeAllConnections();
        server.close(() => resolve());
      }),
  };
}
