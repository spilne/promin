// ---------------------------------------------------------------------------
// BudgetFields — the agent editor's "Turn budget" inputs (`backend.budget`):
// what one turn of the agent may spend, its sub-agents included. A blank
// field sets no limit.
// ---------------------------------------------------------------------------

import { BUDGET_FIELDS, type BudgetForm } from "../../lib/recipe-budget-form.ts";

export function BudgetFields(props: { form: BudgetForm; onChange: (form: BudgetForm) => void }) {
  const { form, onChange } = props;
  return (
    <>
      <span class="col-span-2 text-[10px] text-base-content/60 uppercase tracking-wider mt-1">
        Turn budget
        <span class="normal-case tracking-normal text-base-content/50 ml-1">
          — per turn, sub-agents included; blank = no limit
        </span>
      </span>
      {BUDGET_FIELDS.map(({ key, label }) => (
        <label class="form-control" key={key}>
          <span class="text-[10px] text-base-content/50 mb-1">{label}</span>
          <input
            class="input input-bordered input-sm font-mono"
            type="number"
            min="0"
            step={key === "maxCostUsd" ? "0.01" : "1"}
            placeholder="(no limit)"
            value={form[key]}
            onInput={(e) => onChange({ ...form, [key]: (e.target as HTMLInputElement).value })}
          />
        </label>
      ))}
    </>
  );
}
