import { createHash, randomUUID } from "node:crypto";
import { exportJWK, generateKeyPair, SignJWT } from "jose";
import type { LoginIdentity } from "../login-provider.ts";

export type FakeTokenMode = "wrong-key" | "wrong-issuer" | "wrong-audience" | "expired" | "valid";

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
  stop(): Promise<void>;
}

/** Small authorization-code issuer used to exercise discovery, PKCE and JWT verification. */
export async function startFakeOidcIssuer(): Promise<FakeOidcIssuer> {
  const clientId = "test-client";
  const clientSecret = "test-secret";
  const validKeys = await generateKeyPair("RS256");
  const wrongKeys = await generateKeyPair("RS256");
  const validJwk = await exportJWK(validKeys.publicKey);
  Object.assign(validJwk, { kid: "valid", alg: "RS256", use: "sig" });
  const authorizations = new Map<string, Authorization>();
  let nextIdentity: LoginIdentity = {
    subject: "default-subject",
    email: "user@example.com",
    emailVerified: true,
  };
  let nextMode: FakeTokenMode = "valid";
  let issuer = "";

  const server = Bun.serve({
    hostname: "127.0.0.1",
    port: 0,
    fetch: async (req) => {
      const url = new URL(req.url);
      if (url.pathname === "/.well-known/openid-configuration") {
        return Response.json({
          issuer,
          authorization_endpoint: `${issuer}/authorize`,
          token_endpoint: `${issuer}/token`,
          jwks_uri: `${issuer}/jwks`,
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
        const tokenIssuer = authorization.mode === "wrong-issuer" ? `${issuer}/wrong` : issuer;
        const audience = authorization.mode === "wrong-audience" ? "another-client" : clientId;
        const expiresAt = authorization.mode === "expired" ? now - 60 : now + 300;
        const wrongKey = authorization.mode === "wrong-key";
        const token = await new SignJWT({
          email: authorization.identity.email,
          email_verified: authorization.identity.emailVerified,
        })
          .setProtectedHeader({ alg: "RS256", kid: wrongKey ? "wrong" : "valid" })
          .setSubject(authorization.identity.subject)
          .setIssuer(tokenIssuer)
          .setAudience(audience)
          .setIssuedAt(now)
          .setExpirationTime(expiresAt)
          .sign(wrongKey ? wrongKeys.privateKey : validKeys.privateKey);
        return Response.json({ id_token: token, token_type: "Bearer" });
      }
      return new Response("not found", { status: 404 });
    },
  });
  issuer = `http://${server.hostname}:${server.port}`;

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
    async stop() {
      await server.stop(true);
    },
  };
}
