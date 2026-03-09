// ---------------------------------------------------------------------------
// String and Date accessors for DataFrame columns
// ---------------------------------------------------------------------------

import type { LogicalPlan } from "./logical-plan.ts";
import type { DataFrameExecutor } from "./executor.ts";
import { DataFrame } from "./dataframe.ts";

// ---------------------------------------------------------------------------
// StringAccessor
// ---------------------------------------------------------------------------

export class StringAccessor<T> {
  constructor(
    private readonly _plan: LogicalPlan,
    private readonly _column: string,
    private readonly _executor: DataFrameExecutor,
  ) {}

  private _withCol(
    name: string,
    fn: (val: string) => unknown,
  ): DataFrame<T & Record<string, unknown>> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name,
        fn: (row: any) => fn(String(row[this._column] ?? "")),
      },
      this._executor,
    );
  }

  contains(substr: string): DataFrame<T & Record<string, boolean>> {
    return this._withCol(`${this._column}_contains`, (s) => s.includes(substr)) as any;
  }

  startsWith(prefix: string): DataFrame<T & Record<string, boolean>> {
    return this._withCol(`${this._column}_startsWith`, (s) => s.startsWith(prefix)) as any;
  }

  endsWith(suffix: string): DataFrame<T & Record<string, boolean>> {
    return this._withCol(`${this._column}_endsWith`, (s) => s.endsWith(suffix)) as any;
  }

  toUpperCase(): DataFrame<T> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: this._column,
        fn: (row: any) => String(row[this._column] ?? "").toUpperCase(),
      },
      this._executor,
    );
  }

  toLowerCase(): DataFrame<T> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: this._column,
        fn: (row: any) => String(row[this._column] ?? "").toLowerCase(),
      },
      this._executor,
    );
  }

  trim(): DataFrame<T> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: this._column,
        fn: (row: any) => String(row[this._column] ?? "").trim(),
      },
      this._executor,
    );
  }

  replace(search: string | RegExp, replacement: string): DataFrame<T> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: this._column,
        fn: (row: any) => String(row[this._column] ?? "").replace(search, replacement),
      },
      this._executor,
    );
  }

  split(separator: string): DataFrame<T> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: this._column,
        fn: (row: any) => String(row[this._column] ?? "").split(separator),
      },
      this._executor,
    );
  }

  length(): DataFrame<T & Record<string, number>> {
    return this._withCol(`${this._column}_len`, (s) => s.length) as any;
  }

  slice(start: number, end?: number): DataFrame<T> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: this._column,
        fn: (row: any) => String(row[this._column] ?? "").slice(start, end),
      },
      this._executor,
    );
  }
}

// ---------------------------------------------------------------------------
// DateAccessor
// ---------------------------------------------------------------------------

export class DateAccessor<T> {
  constructor(
    private readonly _plan: LogicalPlan,
    private readonly _column: string,
    private readonly _executor: DataFrameExecutor,
  ) {}

  private _withCol(name: string, fn: (d: Date) => unknown): DataFrame<T & Record<string, unknown>> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name,
        fn: (row: any) => {
          const val = row[this._column];
          const d = val instanceof Date ? val : new Date(val);
          return fn(d);
        },
      },
      this._executor,
    );
  }

  year(): DataFrame<T & Record<string, number>> {
    return this._withCol(`${this._column}_year`, (d) => d.getFullYear()) as any;
  }

  month(): DataFrame<T & Record<string, number>> {
    return this._withCol(`${this._column}_month`, (d) => d.getMonth() + 1) as any;
  }

  day(): DataFrame<T & Record<string, number>> {
    return this._withCol(`${this._column}_day`, (d) => d.getDate()) as any;
  }

  dayOfWeek(): DataFrame<T & Record<string, number>> {
    return this._withCol(`${this._column}_dow`, (d) => d.getDay()) as any;
  }

  hour(): DataFrame<T & Record<string, number>> {
    return this._withCol(`${this._column}_hour`, (d) => d.getHours()) as any;
  }

  timestamp(): DataFrame<T & Record<string, number>> {
    return this._withCol(`${this._column}_ts`, (d) => d.getTime()) as any;
  }

  truncate(unit: "year" | "month" | "day" | "hour"): DataFrame<T> {
    return DataFrame._fromPlan(
      {
        _tag: "WithColumn",
        input: this._plan,
        name: this._column,
        fn: (row: any) => {
          const val = row[this._column];
          const d = val instanceof Date ? new Date(val) : new Date(val);
          switch (unit) {
            case "year":
              return new Date(d.getFullYear(), 0, 1).toISOString();
            case "month":
              return new Date(d.getFullYear(), d.getMonth(), 1).toISOString();
            case "day":
              return new Date(d.getFullYear(), d.getMonth(), d.getDate()).toISOString();
            case "hour":
              return new Date(
                d.getFullYear(),
                d.getMonth(),
                d.getDate(),
                d.getHours(),
              ).toISOString();
          }
        },
      },
      this._executor,
    );
  }
}
