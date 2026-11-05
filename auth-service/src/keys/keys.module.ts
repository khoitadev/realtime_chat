import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { JwksController } from './jwks.controller';
import { KeyService } from './key.service';

@Module({
  imports: [ConfigModule],
  controllers: [JwksController],
  providers: [KeyService],
  exports: [KeyService],
})
export class KeysModule {}
