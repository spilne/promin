// ---------------------------------------------------------------------------
// Expression builder for DataFrame — declarative column expressions
//
// Each Expr carries both:
//   - fn: executable JS function for ArrayExecutor (zero overhead)
//   - ast: inspectable AST node for DuckDB to compile to SQL
//
// Usage:
//   df.withColumn("bonus", col("revenue").mul(0.1))
//   df.filter(col("age").gt(30).and(col("region").eq("north")))
//   df.withColumn("tier", when(col("score").gt(90), "A").otherwise("C"))
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// AST node types — inspectable by executors for SQL compilation
// ---------------------------------------------------------------------------

export type ExprAst =
  | { type: "col"; name: string }
  | { type: "lit"; value: unknown }
  | { type: "binary"; op: string; left: ExprAst; right: ExprAst }
  | { type: "unary"; op: string; operand: ExprAst }
  | { type: "call"; name: string; args: ExprAst[] }
  | { type: "between"; operand: ExprAst; low: number; high: number }
  | { type: "in"; operand: ExprAst; values: unknown[] }
  | { type: "case"; branches: { condition: ExprAst; value: ExprAst }[]; otherwise: ExprAst }
  | { type: "cast"; operand: ExprAst; to: string }
  | { type: "field"; operand: ExprAst; name: string }
  | { type: "listOp"; operand: ExprAst; op: string; args?: unknown[] }
  | { type: "fn"; description: string }; // opaque — can't compile to SQL

// ---------------------------------------------------------------------------
// Expr class
// ---------------------------------------------------------------------------

/** An expression that evaluates to a value given a row. */
export class Expr {
  constructor(
    readonly fn: (row: any) => any,
    readonly ast: ExprAst = { type: "fn", description: "opaque" },
  ) {}

  // -------------------------------------------------------------------------
  // Arithmetic
  // -------------------------------------------------------------------------

  add(other: Expr | number | string): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) + right.fn(row), {
      type: "binary",
      op: "+",
      left: this.ast,
      right: right.ast,
    });
  }

  sub(other: Expr | number): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) - right.fn(row), {
      type: "binary",
      op: "-",
      left: this.ast,
      right: right.ast,
    });
  }

  mul(other: Expr | number): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) * right.fn(row), {
      type: "binary",
      op: "*",
      left: this.ast,
      right: right.ast,
    });
  }

  div(other: Expr | number): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) / right.fn(row), {
      type: "binary",
      op: "/",
      left: this.ast,
      right: right.ast,
    });
  }

  mod(other: Expr | number): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) % right.fn(row), {
      type: "binary",
      op: "%",
      left: this.ast,
      right: right.ast,
    });
  }

  neg(): Expr {
    return new Expr((row) => -this.fn(row), {
      type: "unary",
      op: "-",
      operand: this.ast,
    });
  }

  // -------------------------------------------------------------------------
  // Comparison
  // -------------------------------------------------------------------------

  gt(other: Expr | number | string): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) > right.fn(row), {
      type: "binary",
      op: ">",
      left: this.ast,
      right: right.ast,
    });
  }

  gte(other: Expr | number | string): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) >= right.fn(row), {
      type: "binary",
      op: ">=",
      left: this.ast,
      right: right.ast,
    });
  }

  lt(other: Expr | number | string): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) < right.fn(row), {
      type: "binary",
      op: "<",
      left: this.ast,
      right: right.ast,
    });
  }

  lte(other: Expr | number | string): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) <= right.fn(row), {
      type: "binary",
      op: "<=",
      left: this.ast,
      right: right.ast,
    });
  }

  eq(other: Expr | number | string | boolean | null): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) === right.fn(row), {
      type: "binary",
      op: "=",
      left: this.ast,
      right: right.ast,
    });
  }

  neq(other: Expr | number | string | boolean | null): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) !== right.fn(row), {
      type: "binary",
      op: "!=",
      left: this.ast,
      right: right.ast,
    });
  }

  // -------------------------------------------------------------------------
  // Logical
  // -------------------------------------------------------------------------

  and(other: Expr): Expr {
    return new Expr((row) => this.fn(row) && other.fn(row), {
      type: "binary",
      op: "AND",
      left: this.ast,
      right: other.ast,
    });
  }

  or(other: Expr): Expr {
    return new Expr((row) => this.fn(row) || other.fn(row), {
      type: "binary",
      op: "OR",
      left: this.ast,
      right: other.ast,
    });
  }

  not(): Expr {
    return new Expr((row) => !this.fn(row), {
      type: "unary",
      op: "NOT",
      operand: this.ast,
    });
  }

  // -------------------------------------------------------------------------
  // Null checks
  // -------------------------------------------------------------------------

  isNull(): Expr {
    return new Expr((row) => this.fn(row) == null, {
      type: "unary",
      op: "IS NULL",
      operand: this.ast,
    });
  }

  isNotNull(): Expr {
    return new Expr((row) => this.fn(row) != null, {
      type: "unary",
      op: "IS NOT NULL",
      operand: this.ast,
    });
  }

  // -------------------------------------------------------------------------
  // String operations
  // -------------------------------------------------------------------------

  lower(): Expr {
    return new Expr((row) => String(this.fn(row)).toLowerCase(), {
      type: "call",
      name: "LOWER",
      args: [this.ast],
    });
  }

  upper(): Expr {
    return new Expr((row) => String(this.fn(row)).toUpperCase(), {
      type: "call",
      name: "UPPER",
      args: [this.ast],
    });
  }

  contains(substr: string): Expr {
    return new Expr((row) => String(this.fn(row)).includes(substr), {
      type: "call",
      name: "CONTAINS",
      args: [this.ast, { type: "lit", value: substr }],
    });
  }

  startsWith(prefix: string): Expr {
    return new Expr((row) => String(this.fn(row)).startsWith(prefix), {
      type: "call",
      name: "STARTS_WITH",
      args: [this.ast, { type: "lit", value: prefix }],
    });
  }

  endsWith(suffix: string): Expr {
    return new Expr((row) => String(this.fn(row)).endsWith(suffix), {
      type: "call",
      name: "ENDS_WITH",
      args: [this.ast, { type: "lit", value: suffix }],
    });
  }

  // -------------------------------------------------------------------------
  // Misc
  // -------------------------------------------------------------------------

  cast(type: "number" | "string" | "boolean"): Expr {
    const sqlType = type === "number" ? "DOUBLE" : type === "string" ? "VARCHAR" : "BOOLEAN";
    return new Expr(
      (row) => {
        const v = this.fn(row);
        switch (type) {
          case "number":
            return Number(v);
          case "string":
            return String(v);
          case "boolean":
            return Boolean(v);
        }
      },
      { type: "cast", operand: this.ast, to: sqlType },
    );
  }

  between(low: number, high: number): Expr {
    return new Expr(
      (row) => {
        const v = this.fn(row);
        return v >= low && v <= high;
      },
      { type: "between", operand: this.ast, low, high },
    );
  }

  isIn(values: (string | number | boolean)[]): Expr {
    const set = new Set(values);
    return new Expr((row) => set.has(this.fn(row)), {
      type: "in",
      operand: this.ast,
      values,
    });
  }

  // -------------------------------------------------------------------------
  // Struct field access
  // -------------------------------------------------------------------------

  field(name: string): Expr {
    return new Expr(
      (row) => {
        const v = this.fn(row);
        return v != null ? v[name] : undefined;
      },
      { type: "field", operand: this.ast, name },
    );
  }

  // -------------------------------------------------------------------------
  // List/array operations
  // -------------------------------------------------------------------------

  list(): ListExpr {
    return new ListExpr(this);
  }
}

// ---------------------------------------------------------------------------
// when() — conditional expression builder
// ---------------------------------------------------------------------------

export class WhenExpr {
  private branches: Array<{ condition: Expr; value: Expr }> = [];

  constructor(condition: Expr, value: Expr | number | string | boolean) {
    this.branches.push({ condition, value: toExpr(value) });
  }

  when(condition: Expr, value: Expr | number | string | boolean): WhenExpr {
    this.branches.push({ condition, value: toExpr(value) });
    return this;
  }

  otherwise(value: Expr | number | string | boolean | null): Expr {
    const branches = this.branches;
    const defaultVal = toExpr(value);
    return new Expr(
      (row) => {
        for (const { condition, value } of branches) {
          if (condition.fn(row)) return value.fn(row);
        }
        return defaultVal.fn(row);
      },
      {
        type: "case",
        branches: branches.map((b) => ({ condition: b.condition.ast, value: b.value.ast })),
        otherwise: defaultVal.ast,
      },
    );
  }
}

// ---------------------------------------------------------------------------
// ListExpr — operations on array-valued columns
// ---------------------------------------------------------------------------

export class ListExpr {
  constructor(private readonly expr: Expr) {}

  private _listOp(op: string, fn: (arr: unknown[]) => unknown, args?: unknown[]): Expr {
    return new Expr(
      (row) => {
        const v = this.expr.fn(row);
        return Array.isArray(v) ? fn(v) : undefined;
      },
      { type: "listOp", operand: this.expr.ast, op, args },
    );
  }

  lengths(): Expr {
    return this._listOp("length", (arr) => arr.length);
  }

  get(index: number): Expr {
    return this._listOp(
      "get",
      (arr) => {
        const i = index < 0 ? arr.length + index : index;
        return arr[i];
      },
      [index],
    );
  }

  first(): Expr {
    return this.get(0);
  }

  last(): Expr {
    return this.get(-1);
  }

  contains(value: unknown): Expr {
    return this._listOp("contains", (arr) => arr.includes(value), [value]);
  }

  unique(): Expr {
    return this._listOp("unique", (arr) => [...new Set(arr)]);
  }

  sort(): Expr {
    return this._listOp("sort", (arr) => [...arr].sort());
  }

  join(separator = ","): Expr {
    return this._listOp("join", (arr) => arr.join(separator), [separator]);
  }

  sum(): Expr {
    return this._listOp("sum", (arr) => arr.reduce<number>((acc, v) => acc + Number(v), 0));
  }

  mean(): Expr {
    return this._listOp("mean", (arr) =>
      arr.length > 0 ? arr.reduce<number>((acc, v) => acc + Number(v), 0) / arr.length : null,
    );
  }

  min(): Expr {
    return this._listOp("min", (arr) =>
      arr.length > 0 ? arr.reduce((a, b) => ((a as number) < (b as number) ? a : b)) : null,
    );
  }

  max(): Expr {
    return this._listOp("max", (arr) =>
      arr.length > 0 ? arr.reduce((a, b) => ((a as number) > (b as number) ? a : b)) : null,
    );
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/** Reference a column by name. */
export function col(name: string): Expr {
  return new Expr((row) => row[name], { type: "col", name });
}

/** A literal value (constant). */
export function lit(value: number | string | boolean | null): Expr {
  return new Expr(() => value, { type: "lit", value });
}

/**
 * Conditional expression — like SQL CASE WHEN.
 *
 * @example
 * ```ts
 * when(col("score").gt(90), "A")
 *   .when(col("score").gt(70), "B")
 *   .otherwise("C")
 * ```
 */
export function when(condition: Expr, value: Expr | number | string | boolean): WhenExpr {
  return new WhenExpr(condition, value);
}

/** Convert a value to an Expr. Primitives become lit(), Expr passes through. */
function toExpr(value: Expr | number | string | boolean | null): Expr {
  return value instanceof Expr ? value : new Expr(() => value, { type: "lit", value });
}

// ---------------------------------------------------------------------------
// AST utilities — check if an AST can be compiled to SQL
// ---------------------------------------------------------------------------

/** Returns true if the AST contains no opaque (fn) nodes — fully compilable to SQL. */
export function isCompilable(ast: ExprAst): boolean {
  switch (ast.type) {
    case "fn":
      return false;
    case "col":
    case "lit":
      return true;
    case "binary":
      return isCompilable(ast.left) && isCompilable(ast.right);
    case "unary":
      return isCompilable(ast.operand);
    case "call":
      return ast.args.every(isCompilable);
    case "between":
      return isCompilable(ast.operand);
    case "in":
      return isCompilable(ast.operand);
    case "cast":
      return isCompilable(ast.operand);
    case "case":
      return (
        ast.branches.every((b) => isCompilable(b.condition) && isCompilable(b.value)) &&
        isCompilable(ast.otherwise)
      );
    case "field":
      return isCompilable(ast.operand);
    case "listOp":
      return isCompilable(ast.operand);
  }
}

/** Compile an AST node to a DuckDB SQL expression. */
export function astToSql(ast: ExprAst): string {
  switch (ast.type) {
    case "col":
      return `"${ast.name}"`;
    case "lit":
      return typeof ast.value === "string" ? `'${ast.value}'` : String(ast.value);
    case "binary":
      return `(${astToSql(ast.left)} ${ast.op} ${astToSql(ast.right)})`;
    case "unary":
      if (ast.op === "IS NULL" || ast.op === "IS NOT NULL")
        return `(${astToSql(ast.operand)} ${ast.op})`;
      if (ast.op === "NOT") return `(NOT ${astToSql(ast.operand)})`;
      return `(${ast.op}${astToSql(ast.operand)})`;
    case "call":
      return `${ast.name}(${ast.args.map(astToSql).join(", ")})`;
    case "between":
      return `(${astToSql(ast.operand)} BETWEEN ${ast.low} AND ${ast.high})`;
    case "in": {
      const vals = ast.values.map((v) => (typeof v === "string" ? `'${v}'` : String(v))).join(", ");
      return `(${astToSql(ast.operand)} IN (${vals}))`;
    }
    case "cast":
      return `CAST(${astToSql(ast.operand)} AS ${ast.to})`;
    case "case": {
      const branches = ast.branches
        .map((b) => `WHEN ${astToSql(b.condition)} THEN ${astToSql(b.value)}`)
        .join(" ");
      return `(CASE ${branches} ELSE ${astToSql(ast.otherwise)} END)`;
    }
    case "field":
      return `(${astToSql(ast.operand)}).${ast.name}`;
    case "listOp": {
      const operand = astToSql(ast.operand);
      switch (ast.op) {
        case "length":
          return `array_length(${operand})`;
        case "get":
          return `${operand}[${(ast.args?.[0] as number) + 1}]`;
        case "contains":
          return `array_contains(${operand}, ${typeof ast.args?.[0] === "string" ? `'${ast.args[0]}'` : ast.args?.[0]})`;
        case "unique":
          return `array_distinct(${operand})`;
        case "sort":
          return `array_sort(${operand})`;
        case "join":
          return `array_to_string(${operand}, '${ast.args?.[0] ?? ","}')`;
        default:
          return `list_${ast.op}(${operand})`;
      }
    }
    case "fn":
      throw new Error("Cannot compile opaque expression to SQL");
  }
}
