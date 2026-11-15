import { createRemoteJWKSet, customFetch, jwtVerify } from "jose";
import { z } from "zod";
import type { LoginIdentity, LoginProvider } from "./login-provider.ts";

const DiscoverySchema = z.object({
  issuer: z.string().url(),
  authorization_endpoint: z.string().url(),
  token_endpoint: z.string().url(),
  jwks_uri: z.string().url(),
});

const TokenResponseSchema = z.object({ id_token: z.string().min(1) });
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
  const issuer = config.issuer;
  const fetcher = config.fetch ?? fetch;
  let discoveryPromise: Promise<z.infer<typeof DiscoverySchema>> | undefined;
  let jwksPromise: Promise<ReturnType<typeof createRemoteJWKSet>> | undefined;

  const discovery = () => {
    discoveryPromise ??= (async () => {
      const metadata = await fetchJson({
        fetcher,
        url: discoveryUrl(issuer),
        schema: DiscoverySchema,
        label: "OIDC discovery",
      });
      if (metadata.issuer !== issuer) {
        throw new Error("OIDC discovery issuer does not match configured issuer");
      }
      return metadata;
    })();
    return discoveryPromise;
  };

  const verifier = async () => {
    jwksPromise ??= (async () => {
      const metadata = await discovery();
      return createRemoteJWKSet(new URL(metadata.jwks_uri), {
        // On an unknown kid, immediately reload once so planned provider key
        // rotations do not require a Zorya restart.
        cooldownDuration: 0,
        [customFetch]: (url, options) => fetcher(url, options),
      });
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
        requiredClaims: ["exp"],
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

function discoveryUrl(issuer: string): string {
  const base = issuer.endsWith("/") ? issuer : `${issuer}/`;
  return new URL(".well-known/openid-configuration", base).toString();
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
