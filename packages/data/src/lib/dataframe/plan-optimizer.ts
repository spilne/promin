// ---------------------------------------------------------------------------
// Plan optimizer — rule-based rewriter
//
// Rules (applied bottom-up, iterated to fixed point):
//   1. Filter pushdown         — push Filter toward Source across safe ops
//   2. Filter combining        — merge adjacent Filters via AND
//   3. Filter split at Join    — split AND conjuncts and push to left/right side
//   4. Limit pushdown          — push Limit toward Source across row-preserving ops
//   5. Limit → Source slice    — materialize Limit on known source data
//   6. Column pruning          — narrow projections as close to Source as possible
//   7. CSE on WithColumn       — collapse repeated identical expressions
// ---------------------------------------------------------------------------

import type {
  LogicalPlan,
  FilterPlan,
  SelectPlan,
  SortPlan,
  LimitPlan,
  WithColumnPlan,
  DropPlan,
  RenamePlan,
  JoinPlan,
  SourcePlan,
} from "./logical-plan.ts";
import type { ExprAst } from "./expr.ts";

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

const MAX_ITERATIONS = 16;

export function optimizePlan(plan: LogicalPlan): LogicalPlan {
  let current = plan;
  for (let i = 0; i < MAX_ITERATIONS; i++) {
    const next = rewrite(current);
    if (next === current) break;
    current = next;
  }
  return current;
}

/** Bottom-up rewrite: optimize children, then apply rules at this node. */
function rewrite(plan: LogicalPlan): LogicalPlan {
  const withChildren = rewriteChildren(plan);
  return applyRules(withChildren);
}

/** Recursively optimize each child of `plan` (returns same object if nothing changed). */
function rewriteChildren(plan: LogicalPlan): LogicalPlan {
  switch (plan._tag) {
    case "Source":
      return plan;
    case "Concat": {
      const inputs = plan.inputs.map(rewrite);
      const changed = inputs.some((p, i) => p !== plan.inputs[i]);
      return changed ? { ...plan, inputs } : plan;
    }
    case "Join":
    case "Union": {
      const left = rewrite(plan.left);
      const right = rewrite(plan.right);
      if (left === plan.left && right === plan.right) return plan;
      return { ...plan, left, right };
    }
    default: {
      if (!("input" in plan)) return plan;
      const input = rewrite((plan as any).input);
      if (input === (plan as any).input) return plan;
      return { ...(plan as any), input };
    }
  }
}

/** Try each rule in priority order; return the first transformation that applies. */
function applyRules(plan: LogicalPlan): LogicalPlan {
  return (
    combineAdjacentFilters(plan) ??
    pushFilterDown(plan) ??
    splitFilterAtJoin(plan) ??
    pushLimitDown(plan) ??
    limitIntoSource(plan) ??
    columnPruning(plan) ??
    collapseDuplicateWithColumn(plan) ??
    plan
  );
}

// ---------------------------------------------------------------------------
// Rule 1: combine adjacent Filter nodes
// ---------------------------------------------------------------------------

function combineAdjacentFilters(plan: LogicalPlan): LogicalPlan | null {
  if (plan._tag !== "Filter") return null;
  const outer = plan as FilterPlan;
  const inner = outer.input;
  if (inner._tag !== "Filter") return null;

  const innerFn = inner.fn;
  const outerFn = outer.fn;
  const combinedFn = (row: any) => innerFn(row) && outerFn(row);

  // If both filters carry Expr ASTs, combine them too so downstream consumers
  // (SQL compilation, pushdown analysis) keep seeing an AST.
  if (outer.expr && inner.expr) {
    return {
      _tag: "Filter",
      input: inner.input,
      fn: combinedFn,
      expr: andExpr(inner.expr, outer.expr),
    };
  }
  return { _tag: "Filter", input: inner.input, fn: combinedFn };
}

function andExpr(a: ExprAst, b: ExprAst): ExprAst {
  return { type: "binary", op: "AND", left: a, right: b };
}

// ---------------------------------------------------------------------------
// Rule 2: push Filter toward Source
// ---------------------------------------------------------------------------

function pushFilterDown(plan: LogicalPlan): LogicalPlan | null {
  if (plan._tag !== "Filter") return null;
  const filter = plan as FilterPlan;
  if (!filter.expr) return null; // opaque predicates are not pushable
  const cols = collectColumns(filter.expr);

  const child = filter.input;
  switch (child._tag) {
    // Filter-past-Select is intentionally NOT handled here — `columnPruning`
    // handles the Select-above-Filter case in the opposite direction
    // (projection pushdown), and enabling both would oscillate.
    case "Drop":
      return pushFilterPastDrop(filter, child as DropPlan, cols);
    case "Rename":
      return pushFilterPastRename(filter, child as RenamePlan);
    case "Sort":
      return { ...(child as SortPlan), input: { ...filter, input: (child as SortPlan).input } };
    case "WithColumn":
      return pushFilterPastWithColumn(filter, child as WithColumnPlan, cols);
    default:
      return null;
  }
}

function pushFilterPastDrop(
  filter: FilterPlan,
  drop: DropPlan,
  filterCols: string[],
): LogicalPlan | null {
  const dropped = new Set(drop.columns);
  if (filterCols.some((c) => dropped.has(c))) return null; // would reference a dropped col
  return { ...drop, input: { ...filter, input: drop.input } };
}

function pushFilterPastRename(filter: FilterPlan, rename: RenamePlan): LogicalPlan | null {
  // Rewrite filter's column references using the inverse mapping.
  const inverse = new Map<string, string>();
  for (const [from, to] of Object.entries(rename.mapping)) inverse.set(to, from);
  const rewrittenAst = filter.expr ? renameColumns(filter.expr, inverse) : null;
  if (!rewrittenAst) return null;
  const rewrittenFn = (row: any) => {
    const renamed: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(row)) {
      renamed[rename.mapping[key] ?? key] = value;
    }
    return filter.fn(renamed);
  };
  return {
    ...rename,
    input: {
      _tag: "Filter",
      input: rename.input,
      fn: rewrittenFn,
      expr: rewrittenAst,
    },
  };
}

function pushFilterPastWithColumn(
  filter: FilterPlan,
  wc: WithColumnPlan,
  filterCols: string[],
): LogicalPlan | null {
  // Safe only if filter does not reference the column that WithColumn defines.
  if (filterCols.includes(wc.name)) return null;
  return { ...wc, input: { ...filter, input: wc.input } };
}

// ---------------------------------------------------------------------------
// Rule 3: split filter conjuncts at Join, push each side-specific conjunct down
// ---------------------------------------------------------------------------

function splitFilterAtJoin(plan: LogicalPlan): LogicalPlan | null {
  if (plan._tag !== "Filter") return null;
  const filter = plan as FilterPlan;
  if (!filter.expr) return null;
  const join = filter.input;
  if (join._tag !== "Join") return null;

  const conjuncts = splitAnd(filter.expr);
  if (conjuncts.length < 1) return null;

  const leftCols = inferColumns((join as JoinPlan).left);
  const rightCols = inferColumns((join as JoinPlan).right);
  if (!leftCols || !rightCols) return null;

  const forLeft: ExprAst[] = [];
  const forRight: ExprAst[] = [];
  const stay: ExprAst[] = [];
  for (const c of conjuncts) {
    const cols = collectColumns(c);
    const inLeft = cols.every((col) => leftCols.has(col));
    const inRight = cols.every((col) => rightCols.has(col));
    if (inLeft && !inRight) forLeft.push(c);
    else if (inRight && !inLeft) forRight.push(c);
    else stay.push(c);
  }

  if (forLeft.length === 0 && forRight.length === 0) return null;

  const leftInput =
    forLeft.length > 0
      ? wrapFilter((join as JoinPlan).left, andAll(forLeft))
      : (join as JoinPlan).left;
  const rightInput =
    forRight.length > 0
      ? wrapFilter((join as JoinPlan).right, andAll(forRight))
      : (join as JoinPlan).right;
  const newJoin: JoinPlan = { ...(join as JoinPlan), left: leftInput, right: rightInput };

  if (stay.length === 0) return newJoin;
  return wrapFilter(newJoin, andAll(stay));
}

function splitAnd(ast: ExprAst): ExprAst[] {
  if (ast.type === "binary" && ast.op === "AND") {
    return [...splitAnd(ast.left), ...splitAnd(ast.right)];
  }
  return [ast];
}

function andAll(conjuncts: ExprAst[]): ExprAst {
  return conjuncts.reduce((acc, c) => andExpr(acc, c));
}

function wrapFilter(input: LogicalPlan, expr: ExprAst): LogicalPlan {
  const fn = astToFn(expr);
  return { _tag: "Filter", input, fn, expr };
}

// ---------------------------------------------------------------------------
// Rule 4: push Limit past row-preserving ops
// ---------------------------------------------------------------------------

function pushLimitDown(plan: LogicalPlan): LogicalPlan | null {
  if (plan._tag !== "Limit") return null;
  const limit = plan as LimitPlan;
  const child = limit.input;

  // Row-preserving ops: projection-style nodes that never add or remove rows.
  // Swapping Limit with these reduces the work each does.
  switch (child._tag) {
    case "Select":
    case "Drop":
    case "Rename":
    case "WithColumn":
    case "Map":
      return { ...(child as any), input: { ...limit, input: (child as any).input } };
    default:
      return null;
  }
}

// ---------------------------------------------------------------------------
// Rule 5: materialize Limit on a concrete Source
// ---------------------------------------------------------------------------

function limitIntoSource(plan: LogicalPlan): LogicalPlan | null {
  if (plan._tag !== "Limit") return null;
  const limit = plan as LimitPlan;
  const child = limit.input;
  if (child._tag !== "Source") return null;
  const src = child as SourcePlan;
  // Only collapse when data is already materialized. Leave load()-backed
  // sources alone so the loader controls how much to read.
  if (src.load && src.data.length === 0) return null;
  if (src.data.length <= limit.n) return src; // nothing to slice
  return { ...src, data: src.data.slice(0, limit.n) };
}

// ---------------------------------------------------------------------------
// Rule 6: column pruning (projection pushdown)
// ---------------------------------------------------------------------------

function columnPruning(plan: LogicalPlan): LogicalPlan | null {
  // Existing behaviour: Select above Filter — push Select below Filter when safe.
  if (plan._tag !== "Select") return null;
  const select = plan as SelectPlan;
  if (select.input._tag !== "Filter") return null;
  const filter = select.input as FilterPlan;
  if (!filter.expr) return null;

  const filterCols = collectColumns(filter.expr);
  const selectCols = new Set(select.columns);

  // Case A: filter only uses columns that survive the projection.
  if (filterCols.every((c) => selectCols.has(c))) {
    return { ...filter, input: { ...select, input: filter.input } };
  }

  // Case B: filter needs extra columns. Wrap source with a wider Select, then
  // re-apply the narrow final projection above the filter.
  const sourceColumnCount = countSourceColumns(filter.input);
  const union = new Set<string>([...selectCols, ...filterCols]);
  if (union.size < sourceColumnCount) {
    return {
      _tag: "Select",
      input: {
        ...filter,
        input: { _tag: "Select", input: filter.input, columns: [...union] },
      },
      columns: select.columns,
    };
  }

  return null;
}

function countSourceColumns(plan: LogicalPlan): number {
  if (plan._tag === "Source" && plan.data.length > 0) {
    return Object.keys(plan.data[0] as Record<string, unknown>).length;
  }
  return Infinity;
}

// ---------------------------------------------------------------------------
// Rule 7: collapse duplicate WithColumn expressions
// ---------------------------------------------------------------------------

function collapseDuplicateWithColumn(plan: LogicalPlan): LogicalPlan | null {
  if (plan._tag !== "WithColumn") return null;
  const outer = plan as WithColumnPlan;
  if (!outer.expr) return null;

  // Walk the chain of WithColumn ancestors. If we find one computing the same
  // expression with a different target name, rewrite the outer to just alias
  // that column rather than recomputing.
  let cursor = outer.input;
  const outerAstKey = astKey(outer.expr);
  while (cursor._tag === "WithColumn") {
    const inner = cursor as WithColumnPlan;
    if (inner.name !== outer.name && inner.expr && astKey(inner.expr) === outerAstKey) {
      const aliasFn = (row: any) => row[inner.name];
      const aliasExpr: ExprAst = { type: "col", name: inner.name };
      return { ...outer, fn: aliasFn, expr: aliasExpr };
    }
    cursor = inner.input;
  }
  return null;
}

/** Structural key for an Expr AST — equal keys ⇒ equivalent subtrees. */
function astKey(ast: ExprAst): string {
  switch (ast.type) {
    case "col":
      return `col:${ast.name}`;
    case "lit":
      return `lit:${JSON.stringify(ast.value)}`;
    case "binary":
      return `bin:${ast.op}(${astKey(ast.left)},${astKey(ast.right)})`;
    case "unary":
      return `un:${ast.op}(${astKey(ast.operand)})`;
    case "call":
      return `call:${ast.name}(${ast.args.map(astKey).join(",")})`;
    case "between":
      return `btw:(${astKey(ast.operand)},${ast.low},${ast.high})`;
    case "in":
      return `in:(${astKey(ast.operand)},${JSON.stringify(ast.values)})`;
    case "cast":
      return `cast:${ast.to}(${astKey(ast.operand)})`;
    case "case": {
      const bs = ast.branches.map((b) => `${astKey(b.condition)}=>${astKey(b.value)}`).join(";");
      return `case:[${bs}]else(${astKey(ast.otherwise)})`;
    }
    case "field":
      return `field:${ast.name}(${astKey(ast.operand)})`;
    case "listOp":
      return `listOp:${ast.op}(${astKey(ast.operand)},${JSON.stringify(ast.args ?? [])})`;
    case "fn":
      return `fn:${ast.description}`;
  }
}

// ---------------------------------------------------------------------------
// AST utilities
// ---------------------------------------------------------------------------

export function collectColumns(ast: ExprAst): string[] {
  switch (ast.type) {
    case "col":
      return [ast.name];
    case "lit":
    case "fn":
      return [];
    case "binary":
      return [...collectColumns(ast.left), ...collectColumns(ast.right)];
    case "unary":
    case "between":
    case "in":
    case "cast":
    case "field":
    case "listOp":
      return collectColumns(ast.operand);
    case "call":
      return ast.args.flatMap(collectColumns);
    case "case": {
      const cols: string[] = [];
      for (const b of ast.branches) {
        cols.push(...collectColumns(b.condition), ...collectColumns(b.value));
      }
      cols.push(...collectColumns(ast.otherwise));
      return cols;
    }
  }
}

function renameColumns(ast: ExprAst, inverse: Map<string, string>): ExprAst {
  switch (ast.type) {
    case "col":
      return inverse.has(ast.name) ? { type: "col", name: inverse.get(ast.name)! } : ast;
    case "lit":
    case "fn":
      return ast;
    case "binary":
      return {
        ...ast,
        left: renameColumns(ast.left, inverse),
        right: renameColumns(ast.right, inverse),
      };
    case "unary":
    case "between":
    case "in":
    case "cast":
    case "field":
    case "listOp":
      return { ...ast, operand: renameColumns(ast.operand, inverse) };
    case "call":
      return { ...ast, args: ast.args.map((a) => renameColumns(a, inverse)) };
    case "case":
      return {
        ...ast,
        branches: ast.branches.map((b) => ({
          condition: renameColumns(b.condition, inverse),
          value: renameColumns(b.value, inverse),
        })),
        otherwise: renameColumns(ast.otherwise, inverse),
      };
  }
}

/** Statically infer the column set available at a plan node, or null if unknown. */
function inferColumns(plan: LogicalPlan): Set<string> | null {
  switch (plan._tag) {
    case "Source": {
      if (plan.data.length === 0) return null; // unknown schema
      return new Set(Object.keys(plan.data[0] as Record<string, unknown>));
    }
    case "Select":
      return new Set(plan.columns);
    case "Drop": {
      const input = inferColumns(plan.input);
      if (!input) return null;
      for (const c of plan.columns) input.delete(c);
      return input;
    }
    case "Rename": {
      const input = inferColumns(plan.input);
      if (!input) return null;
      const out = new Set<string>();
      for (const c of input) out.add(plan.mapping[c] ?? c);
      return out;
    }
    case "WithColumn": {
      const input = inferColumns(plan.input);
      if (!input) return null;
      input.add(plan.name);
      return input;
    }
    case "Filter":
    case "Sort":
    case "Limit":
    case "Offset":
    case "Distinct":
    case "Slice":
    case "Reverse":
      return inferColumns((plan as any).input);
    default:
      return null;
  }
}

/** Compile an ExprAst to a predicate function. Kept minimal — used by split rule. */
function astToFn(ast: ExprAst): (row: any) => any {
  switch (ast.type) {
    case "col": {
      const name = ast.name;
      return (row) => row[name];
    }
    case "lit":
      return () => ast.value;
    case "binary": {
      const l = astToFn(ast.left);
      const r = astToFn(ast.right);
      switch (ast.op) {
        case "+":
          return (row) => l(row) + r(row);
        case "-":
          return (row) => l(row) - r(row);
        case "*":
          return (row) => l(row) * r(row);
        case "/":
          return (row) => l(row) / r(row);
        case "%":
          return (row) => l(row) % r(row);
        case ">":
          return (row) => l(row) > r(row);
        case ">=":
          return (row) => l(row) >= r(row);
        case "<":
          return (row) => l(row) < r(row);
        case "<=":
          return (row) => l(row) <= r(row);
        case "=":
          return (row) => l(row) === r(row);
        case "!=":
          return (row) => l(row) !== r(row);
        case "AND":
          return (row) => l(row) && r(row);
        case "OR":
          return (row) => l(row) || r(row);
        default:
          throw new Error(`astToFn: unsupported binary op: ${ast.op}`);
      }
    }
    case "unary": {
      const inner = astToFn(ast.operand);
      switch (ast.op) {
        case "NOT":
          return (row) => !inner(row);
        case "IS NULL":
          return (row) => inner(row) == null;
        case "IS NOT NULL":
          return (row) => inner(row) != null;
        case "-":
          return (row) => -inner(row);
        default:
          throw new Error(`astToFn: unsupported unary op: ${ast.op}`);
      }
    }
    case "between": {
      const inner = astToFn(ast.operand);
      return (row) => {
        const v = inner(row);
        return v >= ast.low && v <= ast.high;
      };
    }
    case "in": {
      const inner = astToFn(ast.operand);
      const set = new Set(ast.values);
      return (row) => set.has(inner(row));
    }
    case "cast":
    case "call":
    case "case":
    case "field":
    case "listOp":
    case "fn":
      throw new Error(`astToFn: node type '${ast.type}' not supported in filter predicate`);
  }
}
