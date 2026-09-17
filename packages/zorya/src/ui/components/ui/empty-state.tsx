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
    <div class={`text-center text-base-content/50 ${pad}`}>
      <div class="text-sm">{message}</div>
      {hint && <div class="text-xs mt-1 text-base-content/40">{hint}</div>}
      {children && <div class="mt-3 flex justify-center">{children}</div>}
    </div>
  );
}
