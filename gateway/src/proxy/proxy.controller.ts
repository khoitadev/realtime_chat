import { All, Controller, Req, Res } from '@nestjs/common';
import { HttpService } from '@nestjs/axios';
import { Request, Response } from 'express';
import { firstValueFrom } from 'rxjs';
import { AxiosRequestConfig } from 'axios';

@Controller()
export class ProxyController {
  constructor(private readonly http: HttpService) {}

  @All('auth/*')
  proxyAuth(@Req() req: Request, @Res() res: Response) {
    return this.forward(req, res, process.env.AUTH_SERVICE_URL ?? 'http://localhost:3010');
  }

  @All(['rooms', 'rooms/*'])
  proxyRooms(@Req() req: Request, @Res() res: Response) {
    return this.forward(req, res, process.env.CHAT_SERVICE_URL ?? 'http://localhost:3000');
  }

  private async forward(req: Request, res: Response, target: string): Promise<void> {
    const config: AxiosRequestConfig = {
      method: req.method,
      url: `${target}${req.originalUrl}`,
      data: req.body,
      headers: req.headers.authorization ? { authorization: req.headers.authorization } : {},
      validateStatus: () => true,
    };
    const response = await firstValueFrom(this.http.request(config));
    res.status(response.status).json(response.data);
  }
}
