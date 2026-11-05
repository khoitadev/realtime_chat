import { Module } from '@nestjs/common';
import { PassportModule } from '@nestjs/passport';
import { AuthModule } from '../auth/auth.module';
import { JwtAuthGuard } from '../auth/jwt-auth.guard';
import { JwtOrInternalAuthGuard } from '../auth/jwt-or-internal-auth.guard';
import { RoomsService } from './rooms.service';
import { RoomsController } from './rooms.controller';
import { InternalController } from './internal.controller';

@Module({
  imports: [PassportModule, AuthModule],
  controllers: [RoomsController, InternalController],
  providers: [RoomsService, JwtAuthGuard, JwtOrInternalAuthGuard],
  exports: [RoomsService],
})
export class RoomsModule {}
