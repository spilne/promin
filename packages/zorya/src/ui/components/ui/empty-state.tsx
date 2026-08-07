import type { ComponentChildren } from "preact";

interface EmptyStateProps {
  /** Short message. */
  message: string;
  /** Optional subtitle / hint text below the message. */
  hint?: string;
  /** Optional action (e.g. a button). */
  children?: ComponentChildren;
  /** Tailwind vertical padding class. Default "py-8". */
  pad?: string;
}

export function EmptyState({ message, hint, children, pad = "py-8" }: EmptyStateProps) {
  return (
    <div class={`flex flex-col items-center text-center text-base-content/55 ${pad}`}>
      <div class="mb-3 grid h-9 w-9 place-items-center rounded border border-base-content/10 bg-base-300/70 text-base-content/35">
        -
      </div>
      <div class="text-sm font-medium text-base-content/70">{message}</div>
      {hint && <div class="mt-1 max-w-md text-xs text-base-content/45">{hint}</div>}
      {children && <div class="mt-3 flex justify-center">{children}</div>}
    </div>
  );
}
