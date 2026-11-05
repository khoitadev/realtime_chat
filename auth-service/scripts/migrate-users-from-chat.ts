import { PrismaClient } from '@prisma/client';
import { migrateUsers } from '../src/migration/migrate-users';

async function main() {
  const sourceUrl = process.env.SOURCE_DATABASE_URL;
  if (!sourceUrl || !process.env.DATABASE_URL) {
    throw new Error('Set SOURCE_DATABASE_URL (legacy chat DB) and DATABASE_URL (auth DB)');
  }
  const prisma = new PrismaClient();
  try {
    const result = await migrateUsers(sourceUrl, prisma);
    console.log(`Migrated ${result.users} users and ${result.refreshTokens} refresh tokens`);
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
