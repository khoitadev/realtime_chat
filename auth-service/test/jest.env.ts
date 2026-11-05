import { config } from 'dotenv';
import { generateKeyPairSync } from 'crypto';
import { resolve } from 'path';

config({ path: resolve(__dirname, '../.env.test') });

// Tests get an ephemeral RSA key unless one is configured explicitly.
if (!process.env.JWT_PRIVATE_KEY) {
  const { privateKey } = generateKeyPairSync('rsa', { modulusLength: 2048 });
  process.env.JWT_PRIVATE_KEY = privateKey.export({ type: 'pkcs8', format: 'pem' }) as string;
}
