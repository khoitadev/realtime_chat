import { Inject, Injectable } from "@nestjs/common";
import { PassportStrategy } from "@nestjs/passport";
import { ExtractJwt, Strategy } from "passport-jwt";
import { JwksService } from "./jwks.service";

export const JWT_VERIFY_OPTIONS = Symbol("JWT_VERIFY_OPTIONS");

export interface JwtVerifyOptions {
  issuer: string;
  audience: string;
}

/** Reads the (unverified) `kid` from the JWT header; verification happens afterwards. */
function readKid(rawJwt: string): string | undefined {
  try {
    const header = JSON.parse(
      Buffer.from(rawJwt.split(".")[0], "base64url").toString(),
    );
    return typeof header.kid === "string" ? header.kid : undefined;
  } catch {
    return undefined;
  }
}

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
  constructor(
    jwks: JwksService,
    @Inject(JWT_VERIFY_OPTIONS) options: JwtVerifyOptions,
  ) {
    super({
      jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
      ignoreExpiration: false,
      algorithms: ["RS256"],
      issuer: options.issuer,
      audience: options.audience,
      secretOrKeyProvider: (
        _req: unknown,
        rawJwt: string,
        done: (err: unknown, key?: string) => void,
      ) => {
        jwks.getPublicKeyPem(readKid(rawJwt)).then(
          (pem) => done(null, pem),
          (err) => done(err),
        );
      },
    });
  }

  validate(payload: { sub: string }) {
    return { id: payload.sub };
  }
}
