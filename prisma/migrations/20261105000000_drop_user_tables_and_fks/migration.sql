-- Users now live in auth-service (auth_db).
-- Required order on any database that holds real data:
--   1. freeze writes (registrations/logins) on the chat service
--   2. run auth-service/scripts/migrate-users-from-chat.ts and verify the reported counts
--   3. CREATE TABLE _users_migrated_to_auth (confirmed_at timestamptz DEFAULT now());
--   4. apply this migration (it removes the marker table again)
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM "User") AND to_regclass('public._users_migrated_to_auth') IS NULL THEN
    RAISE EXCEPTION 'Refusing to drop "User": it still has rows. Migrate users to auth_db first, then create the _users_migrated_to_auth marker table (see the steps at the top of this migration).';
  END IF;
END $$;

-- DropForeignKey
ALTER TABLE "RoomMember" DROP CONSTRAINT "RoomMember_userId_fkey";

-- DropForeignKey
ALTER TABLE "Message" DROP CONSTRAINT "Message_senderId_fkey";

-- DropTable (its FK to "User" goes with it)
DROP TABLE "RefreshToken";

-- DropTable
DROP TABLE "User";

-- Remove the confirmation marker, if it was used
DROP TABLE IF EXISTS "_users_migrated_to_auth";
