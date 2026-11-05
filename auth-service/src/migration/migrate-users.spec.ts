import { Client } from 'pg';
import { randomUUID } from 'crypto';
import { PrismaService } from '../prisma/prisma.service';
import { cleanDatabase } from '../../test/db-cleanup';
import { migrateUsers } from './migrate-users';

// DDL copied from the chat-service init migration (the legacy schema being migrated away from).
const LEGACY_DDL = `
CREATE TABLE "User" (
  "id" TEXT NOT NULL,
  "email" TEXT NOT NULL,
  "passwordHash" TEXT NOT NULL,
  "displayName" TEXT NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "User_pkey" PRIMARY KEY ("id")
);
CREATE TABLE "RefreshToken" (
  "id" TEXT NOT NULL,
  "userId" TEXT NOT NULL,
  "tokenHash" TEXT NOT NULL,
  "revokedAt" TIMESTAMP(3),
  "expiresAt" TIMESTAMP(3) NOT NULL,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "RefreshToken_pkey" PRIMARY KEY ("id")
);`;

describe('migrateUsers (legacy chat DB -> auth_db)', () => {
  const legacyName = `legacy_chat_${randomUUID().replace(/-/g, '').slice(0, 12)}`;
  let admin: Client;
  let legacyUrl: string;
  let prisma: PrismaService;

  function urlFor(database: string): string {
    const url = new URL(process.env.DATABASE_URL!);
    url.pathname = `/${database}`;
    return url.toString();
  }

  beforeAll(async () => {
    admin = new Client({ connectionString: urlFor('postgres') });
    await admin.connect();
    await admin.query(`CREATE DATABASE "${legacyName}"`);
    legacyUrl = urlFor(legacyName);

    prisma = new PrismaService();
    await prisma.$connect();
  });

  afterAll(async () => {
    await prisma.$disconnect();
    await admin.query(`DROP DATABASE IF EXISTS "${legacyName}" WITH (FORCE)`);
    await admin.end();
  });

  beforeEach(async () => {
    await cleanDatabase(prisma);
    const legacy = new Client({ connectionString: legacyUrl });
    await legacy.connect();
    await legacy.query('DROP TABLE IF EXISTS "RefreshToken"; DROP TABLE IF EXISTS "User";');
    await legacy.query(LEGACY_DDL);
    await legacy.end();
  });

  async function seedLegacy(users: number, tokensPerUser: number) {
    const legacy = new Client({ connectionString: legacyUrl });
    await legacy.connect();
    const ids: string[] = [];
    for (let i = 0; i < users; i++) {
      const id = randomUUID();
      ids.push(id);
      await legacy.query(
        'INSERT INTO "User" ("id","email","passwordHash","displayName") VALUES ($1,$2,$3,$4)',
        [id, `u${i}-${id}@example.com`, `hash-${i}`, `User ${i}`],
      );
      for (let t = 0; t < tokensPerUser; t++) {
        await legacy.query(
          'INSERT INTO "RefreshToken" ("id","userId","tokenHash","revokedAt","expiresAt") VALUES ($1,$2,$3,$4,$5)',
          [randomUUID(), id, `th-${id}-${t}`, t === 0 ? new Date('2026-01-01T00:00:00Z') : null, new Date('2027-01-01T00:00:00Z')],
        );
      }
    }
    await legacy.end();
    return ids;
  }

  it('copies users and refresh tokens preserving ids and fields', async () => {
    const ids = await seedLegacy(3, 2);

    const result = await migrateUsers(legacyUrl, prisma);

    expect(result).toEqual({ users: 3, refreshTokens: 6 });
    const users = await prisma.user.findMany({ orderBy: { email: 'asc' } });
    expect(users.map((u) => u.id).sort()).toEqual([...ids].sort());
    const tokens = await prisma.refreshToken.findMany();
    expect(tokens).toHaveLength(6);
    expect(tokens.filter((t) => t.revokedAt !== null)).toHaveLength(3); // revoked state preserved
    expect(users[0].passwordHash).toMatch(/^hash-/);
  });

  it('is idempotent: running twice does not duplicate rows', async () => {
    await seedLegacy(2, 1);

    await migrateUsers(legacyUrl, prisma);
    await migrateUsers(legacyUrl, prisma);

    expect(await prisma.user.count()).toBe(2);
    expect(await prisma.refreshToken.count()).toBe(2);
  });

  it('handles an empty legacy database', async () => {
    expect(await migrateUsers(legacyUrl, prisma)).toEqual({ users: 0, refreshTokens: 0 });
  });

  it('fails reconciliation (and writes nothing) when a source row cannot be inserted', async () => {
    await seedLegacy(1, 0);
    // Same email already owned by a different id in the target: createMany skips it silently.
    const legacy = new Client({ connectionString: legacyUrl });
    await legacy.connect();
    const { rows } = await legacy.query('SELECT "email" FROM "User" LIMIT 1');
    await legacy.end();
    await prisma.user.create({
      data: { id: randomUUID(), email: rows[0].email, passwordHash: 'x', displayName: 'squatter' },
    });

    await expect(migrateUsers(legacyUrl, prisma)).rejects.toThrow(/reconciliation/i);
  });
});
