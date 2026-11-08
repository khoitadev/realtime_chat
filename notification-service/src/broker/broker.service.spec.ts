import { execFileSync, spawnSync } from 'child_process';
import { loadConfig, NotificationConfig } from '../config';
import { BrokerService } from './broker.service';
import { WORK_QUEUE } from './topology';
import { queueCount, resetBroker } from '../../test/broker';

const url = process.env.AMQP_URL!;
const hasRabbitmqctl = spawnSync('rabbitmqctl', ['--version']).status === 0;

function config(overrides: Partial<NotificationConfig> = {}): NotificationConfig {
  return {
    ...loadConfig({ AMQP_URL: url, PUSH_WEBHOOK_URL: 'http://unused' }),
    retryTtlMs: [200, 400, 600],
    reconnectDelayMs: 200,
    ...overrides,
  };
}

async function waitFor(fn: () => boolean | Promise<boolean>, timeoutMs = 8000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await fn()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error('waitFor timed out');
}

describe('BrokerService', () => {
  let broker: BrokerService | undefined;

  beforeEach(async () => {
    await resetBroker(url);
  });

  afterEach(async () => {
    await broker?.onApplicationShutdown();
    broker = undefined;
  });

  afterAll(async () => {
    await resetBroker(url);
  });

  it('connects, asserts the topology and runs ready handlers', async () => {
    broker = new BrokerService(config());
    let handled = 0;
    broker.onReady(async () => {
      handled++;
    });

    await broker.onModuleInit();

    expect(broker.isReady()).toBe(true);
    expect(handled).toBe(1);
    expect(await queueCount(url, WORK_QUEUE)).toBe(0); // queue exists
  });

  it('runs a handler registered after the connection is up', async () => {
    broker = new BrokerService(config());
    await broker.onModuleInit();
    let handled = 0;

    broker.onReady(async () => {
      handled++;
    });

    await waitFor(() => handled === 1);
  });

  it('does not throw when the broker is unreachable and reports not ready', async () => {
    broker = new BrokerService(config({ amqpUrl: 'amqp://chat:chat@localhost:5999/chat_test' }));

    await expect(broker.onModuleInit()).resolves.toBeUndefined();

    expect(broker.isReady()).toBe(false);
  });

  it('restart() drops the connection and reconnects, re-running handlers', async () => {
    broker = new BrokerService(config());
    let handled = 0;
    broker.onReady(async () => {
      handled++;
    });
    await broker.onModuleInit();

    await broker.restart();

    await waitFor(() => handled === 2 && broker!.isReady());
  });

  (hasRabbitmqctl ? it : it.skip)(
    'reconnects by itself after the broker closes all connections',
    async () => {
      broker = new BrokerService(config());
      let handled = 0;
      broker.onReady(async () => {
        handled++;
      });
      await broker.onModuleInit();

      const vhost = decodeURIComponent(new URL(url).pathname.slice(1)) || '/';
      execFileSync('rabbitmqctl', ['close_all_connections', '--vhost', vhost, 'test-reconnect']);

      await waitFor(() => handled >= 2 && broker!.isReady());
    },
  );

  it('stops reconnecting after shutdown', async () => {
    broker = new BrokerService(config());
    await broker.onModuleInit();

    await broker.onApplicationShutdown();

    expect(broker.isReady()).toBe(false);
    await new Promise((r) => setTimeout(r, 500));
    expect(broker.isReady()).toBe(false);
  });
});
