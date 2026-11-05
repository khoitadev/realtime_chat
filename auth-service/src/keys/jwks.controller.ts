import { Controller, Get, Res } from '@nestjs/common';
import { Response } from 'express';
import { KeyService } from './key.service';

@Controller('.well-known')
export class JwksController {
  constructor(private readonly keys: KeyService) {}

  @Get('jwks.json')
  jwks(@Res({ passthrough: true }) res: Response) {
    res.setHeader('Cache-Control', 'public, max-age=300');
    return this.keys.getJwks();
  }
}
