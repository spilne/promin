import { createHmac, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import type {
  SecurityServiceAccount,
  SecurityStore,
  ServiceAccountCredential,
} from "./security-store.ts";

const TOKEN_PREFIX = "zsa_";
const SECRET_BYTES = 32;
const TOKEN_PATTERN =
  /^zsa_([0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12})\.([A-Za-z0-9_-]{43})$/u;

export interface IssuedServiceAccountCredential {
  readonly id: string;
  readonly serviceAccountId: string;
  /** Returned once. Only its host-peppered digest is persisted. */
  readonly token: string;
  readonly createdAt: number;
  readonly expiresAt?: number;
}

export async function issueServiceAccountCredential(params: {
  readonly store: SecurityStore;
  readonly serviceAccountId: string;
  readonly pepper: string;
  readonly expiresAt?: number;
}): Promise<IssuedServiceAccountCredential> {
  const material = credentialMaterial({
    ...params,
    createdAt: await params.store.currentTimeMs(),
  });
  await params.store.putServiceAccountCredential(material.stored);
  return material.issued;
}

export async function createServiceAccountWithCredential(params: {
  readonly store: SecurityStore;
  readonly account: SecurityServiceAccount;
  readonly pepper: string;
  readonly expiresAt?: number;
}): Promise<IssuedServiceAccountCredential | undefined> {
  const material = credentialMaterial({
    ...params,
    serviceAccountId: params.account.id,
    createdAt: await params.store.currentTimeMs(),
  });
  const created = await params.store.createServiceAccountWithCredential({
    account: params.account,
    credential: material.stored,
  });
  return created ? material.issued : undefined;
}

function credentialMaterial(params: {
  readonly serviceAccountId: string;
  readonly pepper: string;
  readonly createdAt: number;
  readonly expiresAt?: number;
}): {
  readonly stored: ServiceAccountCredential;
  readonly issued: IssuedServiceAccountCredential;
} {
  const createdAt = params.createdAt;
  if (
    params.expiresAt !== undefined &&
    (!Number.isSafeInteger(params.expiresAt) || params.expiresAt <= createdAt)
  ) {
    throw new TypeError("expiresAt must be a safe-integer timestamp after creation");
  }
  const id = randomUUID();
  const secret = randomBytes(SECRET_BYTES).toString("base64url");
  const stored: ServiceAccountCredential = {
    id,
    serviceAccountId: params.serviceAccountId,
    secretHash: credentialDigest(params.pepper, id, secret),
    createdAt,
    ...(params.expiresAt !== undefined && { expiresAt: params.expiresAt }),
  };
  return {
    stored,
    issued: {
      id,
      serviceAccountId: params.serviceAccountId,
      token: `${TOKEN_PREFIX}${id}.${secret}`,
      createdAt,
      ...(params.expiresAt !== undefined && { expiresAt: params.expiresAt }),
    },
  };
}

export async function authenticateServiceAccountCredential(params: {
  readonly store: SecurityStore;
  readonly token: string;
  readonly pepper: string;
}): Promise<SecurityServiceAccount | undefined> {
  const parsed = parseToken(params.token);
  if (!parsed) return undefined;
  const credential = await params.store.getServiceAccountCredential(parsed.id);
  if (!credential) return undefined;
  const presented = Buffer.from(credentialDigest(params.pepper, parsed.id, parsed.secret), "hex");
  const expected = digestBuffer(credential.secretHash);
  if (!expected || !timingSafeEqual(presented, expected)) return undefined;
  const now = await params.store.currentTimeMs();
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
  const matched = TOKEN_PATTERN.exec(token);
  return matched ? { id: matched[1]!, secret: matched[2]! } : undefined;
}

function digestBuffer(value: string): Buffer | undefined {
  if (!/^[0-9a-f]{64}$/u.test(value)) return undefined;
  return Buffer.from(value, "hex");
}
