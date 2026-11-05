-- Users now live in auth-service (auth_db). Run the data migration (auth-service/scripts/migrate-users-from-chat.ts)
-- and reconcile counts BEFORE applying this migration anywhere that holds real data.

-- DropForeignKey
ALTER TABLE "RoomMember" DROP CONSTRAINT "RoomMember_userId_fkey";

-- DropForeignKey
ALTER TABLE "Message" DROP CONSTRAINT "Message_senderId_fkey";

-- DropTable (its FK to "User" goes with it)
DROP TABLE "RefreshToken";

-- DropTable
DROP TABLE "User";
