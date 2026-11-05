import { PrismaClient } from '@prisma/client';
import { Client } from 'pg';

export interface MigrationResult {
  users: number;
  refreshTokens: number;
}

/**
 * One-time copy of User + RefreshToken from the legacy chat database into auth_db.
 * Ids are preserved; re-running is safe (skipDuplicates). Throws if any source row
 * is missing from the target afterwards (e.g. an email collision).
 */
export async function migrateUsers(
  sourceUrl: string,
  target: PrismaClient,
): Promise<MigrationResult> {
  const source = new Client({ connectionString: sourceUrl });
  await source.connect();
  try {
    // One consistent snapshot of both tables (users and their tokens cannot skew).
    // Writes to the source must still be frozen for the whole migration window.
    await source.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    const users = (
      await source.query(
        'SELECT "id","email","passwordHash","displayName","createdAt" FROM "User"',
      )
    ).rows as Array<{ id: string; email: string; passwordHash: string; displayName: string; createdAt: Date }>;
    const tokens = (
      await source.query(
        'SELECT "id","userId","tokenHash","revokedAt","expiresAt","createdAt" FROM "RefreshToken"',
      )
    ).rows as Array<{
      id: string;
      userId: string;
      tokenHash: string;
      revokedAt: Date | null;
      expiresAt: Date;
      createdAt: Date;
    }>;

    await source.query('COMMIT');

    await target.$transaction([
      target.user.createMany({ data: users, skipDuplicates: true }),
      target.refreshToken.createMany({ data: tokens, skipDuplicates: true }),
    ]);

    const migratedUsers = await target.user.count({ where: { id: { in: users.map((u) => u.id) } } });
    const migratedTokens = await target.refreshToken.count({
      where: { id: { in: tokens.map((t) => t.id) } },
    });
    if (migratedUsers !== users.length || migratedTokens !== tokens.length) {
      throw new Error(
        `Reconciliation failed: users ${migratedUsers}/${users.length}, ` +
          `refresh tokens ${migratedTokens}/${tokens.length}`,
      );
    }

    return { users: users.length, refreshTokens: tokens.length };
  } finally {
    await source.end();
  }
}
