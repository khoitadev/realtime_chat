import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { RoomsService } from './rooms.service';
import { RoomsController } from './rooms.controller';
import { InternalController } from './internal.controller';

@Module({
  imports: [PassportModule],
  controllers: [RoomsController, InternalController],
  providers: [RoomsService],
  exports: [RoomsService],
})
export class RoomsModule {}
