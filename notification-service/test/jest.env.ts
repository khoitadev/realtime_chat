import { config } from 'dotenv';
import { resolve } from 'path';

config({ path: resolve(__dirname, '../.env.test') });

process.env.AMQP_URL ??= 'amqp://chat:chat@localhost:5672/chat_test';
// db 2: notification-service tests never touch the db 1 used by chat-service/gateway specs.
process.env.REDIS_URL ??= 'redis://localhost:6379/2';
