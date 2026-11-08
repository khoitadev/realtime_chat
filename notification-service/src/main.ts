import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

async function bootstrap() {
  const app = await NestFactory.createApplicationContext(AppModule);
  // SIGTERM/SIGINT: finish the in-flight message, then close the channel and Redis.
  app.enableShutdownHooks();
  new Logger('Bootstrap').log('notification-service started');
}

bootstrap().catch((err) => {
  new Logger('Bootstrap').error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
