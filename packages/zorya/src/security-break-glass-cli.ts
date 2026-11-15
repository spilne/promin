#!/usr/bin/env node

import { parseArgs } from "node:util";
import { pathToFileURL } from "node:url";
import { realpathSync } from "node:fs";
import { drizzle } from "drizzle-orm/postgres-js";
import postgres from "postgres";
import { BreakGlassAccessManager } from "./security-module/break-glass.ts";
import { DurableSecurityAudit } from "./security-module/security-audit.ts";
import { migrateSecurityStore } from "./security-postgres/migrate.ts";
import { PostgresSecurityStore } from "./security-postgres/security-store.ts";

export interface BreakGlassCommandEnvironment {
  readonly ZORYA_SECURITY_DATABASE_URL?: string;
  readonly ZORYA_PUBLIC_BASE_URL?: string;
  readonly ZORYA_AUDIT_HMAC_KEY?: string;
  readonly ZORYA_AUDIT_KEY_ID?: string;
  readonly ZORYA_BREAK_GLASS_MAX_TTL_MS?: string;
}

/** Host-local CLI entry point. No Zorya HTTP route invokes this function. */
export async function runBreakGlassCommand(
  argv: ReadonlyArray<string>,
  environment: BreakGlassCommandEnvironment = process.env as BreakGlassCommandEnvironment,
): Promise<Readonly<Record<string, unknown>>> {
  const databaseUrl = requiredEnvironment(
    environment.ZORYA_SECURITY_DATABASE_URL,
    "ZORYA_SECURITY_DATABASE_URL",
  );
  const publicBaseUrl = requiredEnvironment(
    environment.ZORYA_PUBLIC_BASE_URL,
    "ZORYA_PUBLIC_BASE_URL",
  );
  const auditKey = requiredEnvironment(environment.ZORYA_AUDIT_HMAC_KEY, "ZORYA_AUDIT_HMAC_KEY");
  const auditKeyId = environment.ZORYA_AUDIT_KEY_ID ?? "host-break-glass";
  const maxTtlMs = optionalPositiveInteger(
    environment.ZORYA_BREAK_GLASS_MAX_TTL_MS,
    "ZORYA_BREAK_GLASS_MAX_TTL_MS",
  );
  const parsed = parseArgs({
    args: [...argv],
    allowPositionals: true,
    strict: true,
    options: {
      "principal-kind": { type: "string" },
      "principal-id": { type: "string" },
      reason: { type: "string" },
      "created-by": { type: "string" },
      "ttl-seconds": { type: "string" },
      "grant-id": { type: "string" },
      "revoked-by": { type: "string" },
    },
  });
  const [command] = parsed.positionals;
  if (command !== "issue" && command !== "revoke") throw usageError();

  const client = postgres(databaseUrl, { max: 1 });
  const db = drizzle(client);
  try {
    await migrateSecurityStore(db);
    const store = new PostgresSecurityStore({ db });
    const audit = new DurableSecurityAudit({
      store,
      activeKeyId: auditKeyId,
      keys: { [auditKeyId]: auditKey },
      outbox: {},
    });
    const manager = new BreakGlassAccessManager({
      store,
      audit,
      publicBaseUrl,
      ...(maxTtlMs !== undefined && { maxTtlMs }),
    });

    if (command === "issue") {
      const principalKind = parsed.values["principal-kind"];
      if (principalKind !== "user" && principalKind !== "service-account") throw usageError();
      const principalId = requiredOption(parsed.values["principal-id"], "--principal-id");
      const reason = requiredOption(parsed.values.reason, "--reason");
      const createdBy = requiredOption(parsed.values["created-by"], "--created-by");
      const ttlSeconds = optionalPositiveInteger(parsed.values["ttl-seconds"], "--ttl-seconds");
      const issued = await manager.issue({
        principal: { kind: principalKind, id: principalId },
        reason,
        createdBy,
        ...(ttlSeconds !== undefined && { ttlMs: ttlSeconds * 1_000 }),
      });
      return {
        command: "issue",
        grantId: issued.grantId,
        principal: issued.principal,
        createdAt: new Date(issued.createdAt).toISOString(),
        expiresAt: new Date(issued.expiresAt).toISOString(),
        authorizationHeader: issued.authorizationHeader,
        csrfToken: issued.csrfToken,
        setCookieHeaders: issued.setCookieHeaders,
        warning: "Secrets are shown once. Store them only in an approved temporary secret channel.",
      };
    }

    const grantId = requiredOption(parsed.values["grant-id"], "--grant-id");
    const revokedBy = requiredOption(parsed.values["revoked-by"], "--revoked-by");
    return {
      command: "revoke",
      grantId,
      outcome: await manager.revoke({ grantId, revokedBy }),
    };
  } finally {
    await client.end({ timeout: 5 });
  }
}

function requiredEnvironment(value: string | undefined, name: string): string {
  if (!value) throw new Error(`${name} is required`);
  return value;
}

function requiredOption(value: string | undefined, name: string): string {
  if (!value) throw new Error(`${name} is required\n${usageError().message}`);
  return value;
}

function optionalPositiveInteger(value: string | undefined, name: string): number | undefined {
  if (value === undefined) return undefined;
  if (!/^[1-9][0-9]*$/.test(value)) throw new TypeError(`${name} must be a positive integer`);
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed)) throw new TypeError(`${name} exceeds the safe integer range`);
  return parsed;
}

function usageError(): Error {
  return new Error(
    "Usage:\n" +
      "  zorya-break-glass issue --principal-kind user|service-account --principal-id ID --reason TEXT --created-by OPERATOR [--ttl-seconds 900]\n" +
      "  zorya-break-glass revoke --grant-id ID --revoked-by OPERATOR",
  );
}

if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  runBreakGlassCommand(process.argv.slice(2))
    .then((result) => process.stdout.write(`${JSON.stringify(result, null, 2)}\n`))
    .catch((error: unknown) => {
      const message = error instanceof Error ? error.message : String(error);
      process.stderr.write(`zorya-break-glass: ${message}\n`);
      process.exitCode = 1;
    });
}
