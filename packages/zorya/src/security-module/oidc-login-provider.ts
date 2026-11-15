import { createLocalJWKSet, jwtVerify } from "jose";
import { z } from "zod";
import type { LoginIdentity, LoginProvider } from "./login-provider.ts";

const DiscoverySchema = z.object({
  issuer: z.string().url(),
  authorization_endpoint: z.string().url(),
  token_endpoint: z.string().url(),
  jwks_uri: z.string().url(),
});

const TokenResponseSchema = z.object({ id_token: z.string().min(1) });
const JwkSchema = z.object({
  kty: z.string().min(1),
  alg: z.string().optional(),
  key_ops: z.array(z.string()).optional(),
  ext: z.boolean().optional(),
  use: z.string().optional(),
  x5c: z.array(z.string()).optional(),
  x5t: z.string().optional(),
  "x5t#S256": z.string().optional(),
  x5u: z.string().optional(),
  kid: z.string().optional(),
  crv: z.string().optional(),
  d: z.string().optional(),
  dp: z.string().optional(),
  dq: z.string().optional(),
  e: z.string().optional(),
  k: z.string().optional(),
  n: z.string().optional(),
  p: z.string().optional(),
  q: z.string().optional(),
  qi: z.string().optional(),
  x: z.string().optional(),
  y: z.string().optional(),
  pub: z.string().optional(),
  priv: z.string().optional(),
});
const JwksSchema = z.object({ keys: z.array(JwkSchema) });
const IdentityClaimsSchema = z.object({
  sub: z.string().min(1),
  email: z.string().min(1),
  email_verified: z.boolean(),
});

export interface OidcLoginProviderConfig {
  readonly id?: string;
  readonly issuer: string;
  readonly clientId: string;
  readonly clientSecret: string;
  readonly fetch?: typeof fetch;
}

/** Generic authorization-code OIDC provider with PKCE and verified ID tokens. */
export function oidcLoginProvider(config: OidcLoginProviderConfig): LoginProvider {
  const id = config.id ?? "oidc";
  const issuer = config.issuer.replace(/\/$/, "");
  const fetcher = config.fetch ?? fetch;
  let discoveryPromise: Promise<z.infer<typeof DiscoverySchema>> | undefined;
  let jwksPromise: Promise<ReturnType<typeof createLocalJWKSet>> | undefined;

  const discovery = () => {
    discoveryPromise ??= (async () => {
      const metadata = await fetchJson({
        fetcher,
        url: `${issuer}/.well-known/openid-configuration`,
        schema: DiscoverySchema,
        label: "OIDC discovery",
      });
      if (metadata.issuer.replace(/\/$/, "") !== issuer) {
        throw new Error("OIDC discovery issuer does not match configured issuer");
      }
      return metadata;
    })();
    return discoveryPromise;
  };

  const verifier = async () => {
    jwksPromise ??= (async () => {
      const metadata = await discovery();
      const parsed = await fetchJson({
        fetcher,
        url: metadata.jwks_uri,
        schema: JwksSchema,
        label: "OIDC JWKS",
      });
      return createLocalJWKSet(parsed);
    })();
    return jwksPromise;
  };

  return {
    id,
    authorizationUrl: async (params) => {
      const metadata = await discovery();
      const url = new URL(metadata.authorization_endpoint);
      url.searchParams.set("response_type", "code");
      url.searchParams.set("client_id", config.clientId);
      url.searchParams.set("redirect_uri", params.redirectUri);
      url.searchParams.set("scope", "openid email");
      url.searchParams.set("state", params.state);
      url.searchParams.set("code_challenge", params.codeChallenge);
      url.searchParams.set("code_challenge_method", "S256");
      return url.toString();
    },
    exchange: async (params): Promise<LoginIdentity> => {
      const metadata = await discovery();
      const body = new URLSearchParams({
        grant_type: "authorization_code",
        code: params.code,
        redirect_uri: params.redirectUri,
        client_id: config.clientId,
        client_secret: config.clientSecret,
        code_verifier: params.codeVerifier,
      });
      const response = await fetcher(metadata.token_endpoint, {
        method: "POST",
        headers: { "content-type": "application/x-www-form-urlencoded" },
        body,
      });
      if (!response.ok) throw new Error(`OIDC token exchange failed (${response.status})`);
      const tokenResponse = TokenResponseSchema.parse(await response.json());
      const verified = await jwtVerify(tokenResponse.id_token, await verifier(), {
        issuer,
        audience: config.clientId,
      });
      const claims = IdentityClaimsSchema.parse(verified.payload);
      return {
        subject: claims.sub,
        email: claims.email,
        emailVerified: claims.email_verified,
      };
    },
  };
}

async function fetchJson<S extends z.ZodType>(params: {
  readonly fetcher: typeof fetch;
  readonly url: string;
  readonly schema: S;
  readonly label: string;
}): Promise<z.infer<S>> {
  const response = await params.fetcher(params.url);
  if (!response.ok) throw new Error(`${params.label} failed (${response.status})`);
  return params.schema.parse(await response.json());
}
