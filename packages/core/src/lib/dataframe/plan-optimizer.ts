// ---------------------------------------------------------------------------
// Plan optimizer — push Select nodes down to reduce intermediate data
// ---------------------------------------------------------------------------

import type { LogicalPlan } from "./logical-plan.ts";

/**
 * Push Select nodes down the plan tree to reduce intermediate data.
 * Only optimizes cases where it's clearly beneficial and safe.
 */
export function optimizePlan(plan: LogicalPlan): LogicalPlan {
  return pruneColumns(plan);
}

function pruneColumns(plan: LogicalPlan): LogicalPlan {
  // For now, handle the simple case:
  // Select -> Filter -> Source  =>  Filter -> Select -> Source
  // (apply select closer to source, but filter may need columns not in select)

  if (plan._tag === "Select" && plan.input._tag === "Filter") {
    const select = plan;
    const filter = plan.input;
    // Collect columns used by filter function — we can't inspect opaque functions,
    // so only optimize if filter has an expr with inspectable column references
    if (filter.expr) {
      const filterCols = collectColumns(filter.expr);
      const selectCols = new Set(select.columns as string[]);
      const allNeeded = new Set([...selectCols, ...filterCols]);

      // If filter only uses columns that are in the select, we can push select down
      if (filterCols.every((c) => selectCols.has(c))) {
        return {
          ...filter,
          input: { ...select, input: filter.input },
        } as LogicalPlan;
      }
      // Otherwise, add a wider select that includes filter columns, then narrow after
      if (allNeeded.size < countSourceColumns(filter.input)) {
        return {
          _tag: "Select",
          input: {
            ...filter,
            input: { _tag: "Select", input: filter.input, columns: [...allNeeded] },
          },
          columns: select.columns,
        } as LogicalPlan;
      }
    }
  }

  return plan;
}

/** Extract column names referenced in an expression AST */
function collectColumns(ast: any): string[] {
  if (!ast) return [];
  if (ast.type === "col") return [ast.name];
  if (ast.type === "binary") return [...collectColumns(ast.left), ...collectColumns(ast.right)];
  if (ast.type === "unary") return collectColumns(ast.operand);
  if (ast.type === "call") return ast.args?.flatMap(collectColumns) ?? [];
  if (ast.type === "case") {
    const cols: string[] = [];
    for (const b of ast.branches ?? []) {
      cols.push(...collectColumns(b.condition), ...collectColumns(b.value));
    }
    if (ast.otherwise) cols.push(...collectColumns(ast.otherwise));
    return cols;
  }
  return [];
}

/** Estimate total columns in a source */
function countSourceColumns(plan: LogicalPlan): number {
  if (plan._tag === "Source" && plan.data.length > 0) {
    return Object.keys(plan.data[0] as any).length;
  }
  return Infinity; // Can't determine — don't optimize
}
