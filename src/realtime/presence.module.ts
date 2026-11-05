import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { RoomsModule } from '../rooms/rooms.module';
import { PresenceController } from './presence.controller';
import { PresenceService } from './presence.service';

@Module({
  imports: [RoomsModule, PassportModule],
  controllers: [PresenceController],
  providers: [PresenceService],
})
export class PresenceModule {}
