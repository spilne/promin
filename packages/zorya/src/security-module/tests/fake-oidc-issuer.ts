import { createHash, randomUUID } from "node:crypto";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import type { LoginIdentity } from "../login-provider.ts";

export type FakeTokenMode =
  | "wrong-key"
  | "wrong-issuer"
  | "wrong-audience"
  | "expired"
  | "missing-exp"
  | "valid";

interface Authorization {
  readonly identity: LoginIdentity;
  readonly codeChallenge: string;
  readonly mode: FakeTokenMode;
}

export interface FakeOidcIssuer {
  readonly issuer: string;
  readonly clientId: string;
  readonly clientSecret: string;
  nextLogin(identity: Partial<LoginIdentity> & { readonly email: string }): void;
  signWith(mode: FakeTokenMode): void;
  rotateSigningKey(): Promise<void>;
  stop(): Promise<void>;
}

export interface FakeOidcIssuerOptions {
  readonly trailingSlash?: boolean;
  readonly discoveryIssuerTrailingSlashMismatch?: boolean;
}

/** Small authorization-code issuer used to exercise discovery, PKCE and JWT verification. */
export async function startFakeOidcIssuer(
  options: FakeOidcIssuerOptions = {},
): Promise<FakeOidcIssuer> {
  const clientId = "test-client";
  const clientSecret = "test-secret";
  let validKeys = await generateKeyPair("RS256");
  const wrongKeys = await generateKeyPair("RS256");
  let validKid = 1;
  let validJwk = await exportJWK(validKeys.publicKey);
  Object.assign(validJwk, { kid: `valid-${validKid}`, alg: "RS256", use: "sig" });
  const authorizations = new Map<string, Authorization>();
  let nextIdentity: LoginIdentity = {
    subject: "default-subject",
    email: "user@example.com",
    emailVerified: true,
  };
  let nextMode: FakeTokenMode = "valid";
  let issuer = "";
  let origin = "";

  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: async (req) => {
      const url = new URL(req.url);
      if (url.pathname === "/.well-known/openid-configuration") {
        return Response.json({
          issuer: options.discoveryIssuerTrailingSlashMismatch
            ? toggleTrailingSlash(issuer)
            : issuer,
          authorization_endpoint: `${origin}/authorize`,
          token_endpoint: `${origin}/token`,
          jwks_uri: `${origin}/jwks`,
        });
      }
      if (url.pathname === "/jwks") return Response.json({ keys: [validJwk] });
      if (url.pathname === "/authorize") {
        const redirectUri = url.searchParams.get("redirect_uri");
        const state = url.searchParams.get("state");
        const codeChallenge = url.searchParams.get("code_challenge");
        if (
          url.searchParams.get("client_id") !== clientId ||
          url.searchParams.get("code_challenge_method") !== "S256" ||
          !redirectUri ||
          !state ||
          !codeChallenge
        ) {
          return new Response("invalid authorization request", { status: 400 });
        }
        const code = randomUUID();
        authorizations.set(code, {
          identity: nextIdentity,
          codeChallenge,
          mode: nextMode,
        });
        nextMode = "valid";
        const callback = new URL(redirectUri);
        callback.searchParams.set("code", code);
        callback.searchParams.set("state", state);
        return Response.redirect(callback, 302);
      }
      if (url.pathname === "/token" && req.method === "POST") {
        const body = new URLSearchParams(await req.text());
        const code = body.get("code");
        const authorization = code ? authorizations.get(code) : undefined;
        const verifier = body.get("code_verifier");
        if (
          !code ||
          !authorization ||
          !verifier ||
          body.get("client_id") !== clientId ||
          body.get("client_secret") !== clientSecret ||
          createHash("sha256").update(verifier).digest("base64url") !== authorization.codeChallenge
        ) {
          return Response.json({ error: "invalid_grant" }, { status: 400 });
        }
        authorizations.delete(code);
        const now = Math.floor(Date.now() / 1_000);
        const tokenIssuer = authorization.mode === "wrong-issuer" ? `${origin}/wrong` : issuer;
        const audience = authorization.mode === "wrong-audience" ? "another-client" : clientId;
        const expiresAt = authorization.mode === "expired" ? now - 60 : now + 300;
        const wrongKey = authorization.mode === "wrong-key";
        let token = new SignJWT({
          email: authorization.identity.email,
          email_verified: authorization.identity.emailVerified,
        })
          .setProtectedHeader({
            alg: "RS256",
            kid: wrongKey ? `wrong-${randomUUID()}` : `valid-${validKid}`,
          })
          .setSubject(authorization.identity.subject)
          .setIssuer(tokenIssuer)
          .setAudience(audience)
          .setIssuedAt(now);
        if (authorization.mode !== "missing-exp") {
          token = token.setExpirationTime(expiresAt);
        }
        const idToken = await token.sign(wrongKey ? wrongKeys.privateKey : validKeys.privateKey);
        return Response.json({ id_token: idToken, token_type: "Bearer" });
      }
      return new Response("not found", { status: 404 });
    },
  });
  origin = `http://${server.hostname}:${server.port}`;
  issuer = `${origin}${options.trailingSlash ? "/" : ""}`;

  return {
    issuer,
    clientId,
    clientSecret,
    nextLogin(identity) {
      nextIdentity = {
        subject: identity.subject ?? identity.email.toLowerCase(),
        email: identity.email,
        emailVerified: identity.emailVerified ?? true,
      };
    },
    signWith(mode) {
      nextMode = mode;
    },
    async rotateSigningKey() {
      const rotatedKeys = await generateKeyPair("RS256");
      const rotatedJwk = await exportJWK(rotatedKeys.publicKey);
      validKid += 1;
      Object.assign(rotatedJwk, {
        kid: `valid-${validKid}`,
        alg: "RS256",
        use: "sig",
      });
      validKeys = rotatedKeys;
      validJwk = rotatedJwk;
    },
    async stop() {
      await server.stop(true);
    },
  };
}

function toggleTrailingSlash(value: string): string {
  return value.endsWith("/") ? value.slice(0, -1) : `${value}/`;
}
