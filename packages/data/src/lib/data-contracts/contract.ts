// ---------------------------------------------------------------------------
// Data Contracts — formalize agreements between data producers and consumers
// ---------------------------------------------------------------------------

import { z } from "zod";
import type { DataFrame } from "../dataframe/dataframe.ts";

// ---------------------------------------------------------------------------
// Contract definition
// ---------------------------------------------------------------------------

export interface DataContract {
  name: string;
  version: string;
  owner: string;
  description?: string;
  schema: z.ZodType<unknown>;
  sla?: {
    freshness?: { maxAgeMs: number; column: string };
    completeness?: {
      minRowCount?: number;
      maxNullPct?: Record<string, number>;
    };
    uniqueness?: { columns: string[] };
  };
}

// ---------------------------------------------------------------------------
// Validation result
// ---------------------------------------------------------------------------

export interface ContractValidationResult {
  valid: boolean;
  schemaViolations: { type: string; details: string }[];
  slaViolations: { type: string; details: string }[];
}

// ---------------------------------------------------------------------------
// ValidatableContract
// ---------------------------------------------------------------------------

export interface ValidatableContract {
  readonly contract: DataContract;
  validate(data: DataFrame<Record<string, unknown>>): Promise<ContractValidationResult>;
}

export function defineContract(contract: DataContract): ValidatableContract {
  return {
    contract,

    async validate(data: DataFrame<Record<string, unknown>>): Promise<ContractValidationResult> {
      const rows = await data.collect();
      const schemaViolations: { type: string; details: string }[] = [];
      const slaViolations: { type: string; details: string }[] = [];

      // Schema validation — check each row against the Zod schema
      let schemaErrorCount = 0;
      for (let i = 0; i < Math.min(rows.length, 100); i++) {
        const result = contract.schema.safeParse(rows[i]);
        if (!result.success) {
          schemaErrorCount++;
          if (schemaViolations.length < 5) {
            schemaViolations.push({
              type: "schema_mismatch",
              details: `Row ${i}: ${result.error.issues.map((e) => `${e.path.join(".")}: ${e.message}`).join(", ")}`,
            });
          }
        }
      }
      if (schemaErrorCount > 5) {
        schemaViolations.push({
          type: "schema_mismatch",
          details: `...and ${schemaErrorCount - 5} more rows with schema violations`,
        });
      }

      // SLA: freshness
      if (contract.sla?.freshness) {
        const { maxAgeMs, column } = contract.sla.freshness;
        const now = Date.now();
        const latest = rows.reduce((max, r) => {
          const ts = new Date(r[column] as string).getTime();
          return isNaN(ts) ? max : Math.max(max, ts);
        }, 0);
        if (latest > 0 && now - latest > maxAgeMs) {
          slaViolations.push({
            type: "freshness",
            details: `Data is ${Math.round((now - latest) / 1000)}s old, max allowed ${Math.round(maxAgeMs / 1000)}s`,
          });
        }
      }

      // SLA: completeness — row count
      if (contract.sla?.completeness?.minRowCount !== undefined) {
        if (rows.length < contract.sla.completeness.minRowCount) {
          slaViolations.push({
            type: "completeness",
            details: `${rows.length} rows, minimum ${contract.sla.completeness.minRowCount}`,
          });
        }
      }

      // SLA: completeness — null percentage
      if (contract.sla?.completeness?.maxNullPct) {
        for (const [col, maxPct] of Object.entries(contract.sla.completeness.maxNullPct)) {
          const nullCount = rows.filter((r) => r[col] == null).length;
          const pct = rows.length > 0 ? (nullCount / rows.length) * 100 : 0;
          if (pct > maxPct) {
            slaViolations.push({
              type: "completeness",
              details: `${col}: ${pct.toFixed(1)}% nulls, max allowed ${maxPct}%`,
            });
          }
        }
      }

      // SLA: uniqueness
      if (contract.sla?.uniqueness) {
        for (const col of contract.sla.uniqueness.columns) {
          const values = rows.map((r) => r[col]);
          const unique = new Set(values).size;
          if (unique < values.length) {
            slaViolations.push({
              type: "uniqueness",
              details: `${col}: ${values.length - unique} duplicates`,
            });
          }
        }
      }

      return {
        valid: schemaViolations.length === 0 && slaViolations.length === 0,
        schemaViolations,
        slaViolations,
      };
    },
  };
}
