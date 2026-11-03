import { config } from 'dotenv';
import { resolve } from 'path';

config({ path: resolve(__dirname, '../.env.test') });

// Tests always use Redis database 1 so they can never touch dev data in db 0.
process.env.REDIS_URL ??= 'redis://localhost:6379/1';
