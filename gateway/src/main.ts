import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import Redis from 'ioredis';
import { AppModule } from './app.module';
import { REDIS } from './redis/redis.constants';
import { RedisIoAdapter } from './redis/redis-io.adapter';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);
  app.useGlobalPipes(new ValidationPipe({ whitelist: true, transform: true }));
  app.useWebSocketAdapter(new RedisIoAdapter(app, app.get<Redis>(REDIS)));
  app.enableShutdownHooks();
  await app.listen(process.env.PORT ?? 3020);
}
bootstrap();
