export interface LoginIdentity {
  readonly subject: string;
  readonly email: string;
  readonly emailVerified: boolean;
}

/** Authentication provider used by the security module's browser login flow. */
export interface LoginProvider {
  /** Stable route segment used by `/auth/login/:id` and `/auth/callback/:id`. */
  readonly id: string;
  authorizationUrl(params: {
    readonly state: string;
    readonly codeChallenge: string;
    readonly redirectUri: string;
  }): Promise<string>;
  exchange(params: {
    readonly code: string;
    readonly codeVerifier: string;
    readonly redirectUri: string;
  }): Promise<LoginIdentity>;
}
