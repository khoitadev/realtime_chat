import { PrismaService } from '../src/prisma/prisma.service';

export async function cleanDatabase(prisma: PrismaService): Promise<void> {
  await prisma.message.deleteMany();
  await prisma.roomMember.deleteMany();
  await prisma.refreshToken.deleteMany();
  await prisma.room.deleteMany();
  await prisma.user.deleteMany();
}
