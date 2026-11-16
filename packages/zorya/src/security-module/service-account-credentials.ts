import { createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import type { WallClock } from "@promin/workflow";
import type {
  SecurityServiceAccount,
  SecurityStoreV2,
  ServiceAccountCredential,
} from "./security-store.ts";

const TOKEN_PREFIX = "zsa_";
const SECRET_BYTES = 32;

export interface IssuedServiceAccountCredential {
  readonly id: string;
  readonly serviceAccountId: string;
  /** Returned once. Only its host-peppered digest is persisted. */
  readonly token: string;
  readonly createdAt: number;
  readonly expiresAt?: number;
}

export async function issueServiceAccountCredential(params: {
  readonly store: SecurityStoreV2;
  readonly serviceAccountId: string;
  readonly pepper: string;
  readonly clock: WallClock;
  readonly expiresAt?: number;
}): Promise<IssuedServiceAccountCredential> {
  const createdAt = params.clock.currentTimeMs();
  if (
    params.expiresAt !== undefined &&
    (!Number.isSafeInteger(params.expiresAt) || params.expiresAt <= createdAt)
  ) {
    throw new TypeError("expiresAt must be a safe-integer timestamp after creation");
  }
  const id = randomUUID();
  const secret = randomBytes(SECRET_BYTES).toString("base64url");
  const credential: ServiceAccountCredential = {
    id,
    serviceAccountId: params.serviceAccountId,
    secretHash: credentialDigest(params.pepper, id, secret),
    createdAt,
    ...(params.expiresAt !== undefined && { expiresAt: params.expiresAt }),
  };
  await params.store.putServiceAccountCredential(credential);
  return {
    id,
    serviceAccountId: params.serviceAccountId,
    token: `${TOKEN_PREFIX}${id}.${secret}`,
    createdAt,
    ...(params.expiresAt !== undefined && { expiresAt: params.expiresAt }),
  };
}

export async function authenticateServiceAccountCredential(params: {
  readonly store: SecurityStoreV2;
  readonly token: string;
  readonly pepper: string;
  readonly clock: WallClock;
}): Promise<SecurityServiceAccount | undefined> {
  const parsed = parseToken(params.token);
  if (!parsed) return undefined;
  const credential = await params.store.getServiceAccountCredential(parsed.id);
  if (!credential) return undefined;
  const presented = Buffer.from(credentialDigest(params.pepper, parsed.id, parsed.secret), "hex");
  const expected = digestBuffer(credential.secretHash);
  if (!expected || !timingSafeEqual(presented, expected)) return undefined;
  const now = params.clock.currentTimeMs();
  if (
    credential.revokedAt !== undefined ||
    (credential.expiresAt !== undefined && credential.expiresAt <= now)
  ) {
    return undefined;
  }
  const account = await params.store.getServiceAccount(credential.serviceAccountId);
  return account?.status === "active" ? account : undefined;
}

function credentialDigest(pepper: string, id: string, secret: string): string {
  return createHmac("sha256", pepper)
    .update("zorya-service-account-credential\0", "utf8")
    .update(id, "utf8")
    .update("\0", "utf8")
    .update(secret, "utf8")
    .digest("hex");
}

function parseToken(token: string): { readonly id: string; readonly secret: string } | undefined {
  if (!token.startsWith(TOKEN_PREFIX)) return undefined;
  const separator = token.indexOf(".", TOKEN_PREFIX.length);
  if (separator <= TOKEN_PREFIX.length || separator === token.length - 1) return undefined;
  if (token.indexOf(".", separator + 1) !== -1) return undefined;
  return {
    id: token.slice(TOKEN_PREFIX.length, separator),
    secret: token.slice(separator + 1),
  };
}

function digestBuffer(value: string): Buffer | undefined {
  if (!/^[0-9a-f]{64}$/u.test(value)) return undefined;
  return Buffer.from(value, "hex");
}
