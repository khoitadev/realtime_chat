import { config } from 'dotenv';
import { resolve } from 'path';

config({ path: resolve(__dirname, '../.env.test') });

process.env.REDIS_URL ??= 'redis://localhost:6379/1';
