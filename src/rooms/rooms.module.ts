import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { RoomsService } from './rooms.service';
import { RoomsController } from './rooms.controller';

@Module({
  imports: [PassportModule],
  controllers: [RoomsController],
  providers: [RoomsService],
})
export class RoomsModule {}
