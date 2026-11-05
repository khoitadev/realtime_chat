import { Test } from '@nestjs/testing';
import { INestApplication } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { AddressInfo } from 'net';
import { createPublicKey } from 'crypto';
import { JwtService } from '@nestjs/jwt';
import { JwksController } from './jwks.controller';
import { KeyService } from './key.service';

describe('GET /.well-known/jwks.json', () => {
  let app: INestApplication;
  let baseUrl: string;
  let keys: KeyService;

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      controllers: [JwksController],
      providers: [
        KeyService,
        {
          provide: ConfigService,
          useValue: new ConfigService({ JWT_PRIVATE_KEY: process.env.JWT_PRIVATE_KEY }),
        },
      ],
    }).compile();

    app = moduleRef.createNestApplication();
    await app.listen(0);
    baseUrl = `http://localhost:${(app.getHttpServer().address() as AddressInfo).port}`;
    keys = moduleRef.get(KeyService);
  });

  afterAll(async () => {
    await app.close();
  });

  it('serves the JWKS with a public cache header', async () => {
    const res = await fetch(`${baseUrl}/.well-known/jwks.json`);

    expect(res.status).toBe(200);
    expect(res.headers.get('cache-control')).toBe('public, max-age=300');
    const body = await res.json();
    expect(body.keys).toHaveLength(1);
    expect(body.keys[0].kid).toBe(keys.kid);
  });

  it('publishes a key that verifies a token signed by this service', async () => {
    const jwt = new JwtService();
    const token = jwt.sign(
      { sub: 'u1' },
      { privateKey: keys.privatePem, algorithm: 'RS256', keyid: keys.kid },
    );

    const { keys: published } = await (await fetch(`${baseUrl}/.well-known/jwks.json`)).json();
    const pem = createPublicKey({ key: published[0], format: 'jwk' }).export({
      type: 'spki',
      format: 'pem',
    }) as string;

    expect((await jwt.verifyAsync(token, { publicKey: pem, algorithms: ['RS256'] })).sub).toBe('u1');
  });
});
