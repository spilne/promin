// ---------------------------------------------------------------------------
// Expression builder for DataFrame — declarative column expressions
//
// Provides col(), when(), and lit() for building expressions that can be
// used in withColumn(), filter(), and sort(). Expressions compile to
// plain JS functions — zero overhead at runtime.
//
// Usage:
//   df.withColumn("bonus", col("revenue").mul(0.1))
//   df.filter(col("age").gt(30).and(col("region").eq("north")))
//   df.withColumn("tier", when(col("score").gt(90), "A").when(col("score").gt(70), "B").otherwise("C"))
// ---------------------------------------------------------------------------

/** An expression that evaluates to a value given a row. */
export class Expr {
  constructor(readonly fn: (row: any) => any) {}

  // -------------------------------------------------------------------------
  // Arithmetic
  // -------------------------------------------------------------------------

  add(other: Expr | number | string): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) + right.fn(row));
  }

  sub(other: Expr | number): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) - right.fn(row));
  }

  mul(other: Expr | number): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) * right.fn(row));
  }

  div(other: Expr | number): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) / right.fn(row));
  }

  mod(other: Expr | number): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) % right.fn(row));
  }

  neg(): Expr {
    return new Expr((row) => -this.fn(row));
  }

  // -------------------------------------------------------------------------
  // Comparison — returns boolean Expr for use in filter()
  // -------------------------------------------------------------------------

  gt(other: Expr | number | string): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) > right.fn(row));
  }

  gte(other: Expr | number | string): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) >= right.fn(row));
  }

  lt(other: Expr | number | string): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) < right.fn(row));
  }

  lte(other: Expr | number | string): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) <= right.fn(row));
  }

  eq(other: Expr | number | string | boolean | null): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) === right.fn(row));
  }

  neq(other: Expr | number | string | boolean | null): Expr {
    const right = toExpr(other);
    return new Expr((row) => this.fn(row) !== right.fn(row));
  }

  // -------------------------------------------------------------------------
  // Logical
  // -------------------------------------------------------------------------

  and(other: Expr): Expr {
    return new Expr((row) => this.fn(row) && other.fn(row));
  }

  or(other: Expr): Expr {
    return new Expr((row) => this.fn(row) || other.fn(row));
  }

  not(): Expr {
    return new Expr((row) => !this.fn(row));
  }

  // -------------------------------------------------------------------------
  // Null checks
  // -------------------------------------------------------------------------

  isNull(): Expr {
    return new Expr((row) => this.fn(row) == null);
  }

  isNotNull(): Expr {
    return new Expr((row) => this.fn(row) != null);
  }

  // -------------------------------------------------------------------------
  // String operations
  // -------------------------------------------------------------------------

  lower(): Expr {
    return new Expr((row) => String(this.fn(row)).toLowerCase());
  }

  upper(): Expr {
    return new Expr((row) => String(this.fn(row)).toUpperCase());
  }

  contains(substr: string): Expr {
    return new Expr((row) => String(this.fn(row)).includes(substr));
  }

  startsWith(prefix: string): Expr {
    return new Expr((row) => String(this.fn(row)).startsWith(prefix));
  }

  endsWith(suffix: string): Expr {
    return new Expr((row) => String(this.fn(row)).endsWith(suffix));
  }

  // -------------------------------------------------------------------------
  // Misc
  // -------------------------------------------------------------------------

  cast(type: "number" | "string" | "boolean"): Expr {
    return new Expr((row) => {
      const v = this.fn(row);
      switch (type) {
        case "number":
          return Number(v);
        case "string":
          return String(v);
        case "boolean":
          return Boolean(v);
      }
    });
  }

  between(low: number, high: number): Expr {
    return new Expr((row) => {
      const v = this.fn(row);
      return v >= low && v <= high;
    });
  }

  isIn(values: (string | number | boolean)[]): Expr {
    const set = new Set(values);
    return new Expr((row) => set.has(this.fn(row)));
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
    return new Expr((row) => {
      for (const { condition, value } of branches) {
        if (condition.fn(row)) return value.fn(row);
      }
      return defaultVal.fn(row);
    });
  }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/** Reference a column by name. */
export function col(name: string): Expr {
  return new Expr((row) => row[name]);
}

/** A literal value (constant). */
export function lit(value: number | string | boolean | null): Expr {
  return new Expr(() => value);
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
  return value instanceof Expr ? value : new Expr(() => value);
}
