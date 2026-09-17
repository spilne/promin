// ---------------------------------------------------------------------------
// Dialog store — imperative API so any component can call confirm()/prompt()/
// toast() without threading props through the tree. A <DialogHost /> mounted
// once at the app root subscribes to this state and renders the actual UI.
// ---------------------------------------------------------------------------

export type DialogRequest =
  | {
      kind: "confirm";
      title: string;
      message?: string;
      confirmLabel?: string;
      cancelLabel?: string;
      variant?: "default" | "danger";
      resolve: (ok: boolean) => void;
    }
  | {
      kind: "prompt";
      title: string;
      label?: string;
      initial?: string;
      placeholder?: string;
      confirmLabel?: string;
      resolve: (value: string | null) => void;
    };

export interface Toast {
  id: number;
  message: string;
  variant: "info" | "success" | "error" | "warning";
  expiresAt: number;
}

type Listener<T> = (v: T) => void;

function makeStore<T>(initial: T) {
  let value = initial;
  const listeners = new Set<Listener<T>>();
  return {
    get: () => value,
    set(next: T) {
      value = next;
      for (const l of listeners) l(value);
    },
    subscribe(l: Listener<T>) {
      listeners.add(l);
      return () => listeners.delete(l);
    },
  };
}

export const dialogStore = makeStore<DialogRequest | null>(null);
export const toastStore = makeStore<Toast[]>([]);

let toastId = 1;

/** Imperative confirm dialog. Resolves true on confirm, false on cancel. */
export function confirm(opts: {
  title: string;
  message?: string;
  confirmLabel?: string;
  cancelLabel?: string;
  variant?: "default" | "danger";
}): Promise<boolean> {
  return new Promise((resolve) => {
    dialogStore.set({
      kind: "confirm",
      title: opts.title,
      message: opts.message,
      confirmLabel: opts.confirmLabel,
      cancelLabel: opts.cancelLabel,
      variant: opts.variant,
      resolve: (ok) => {
        dialogStore.set(null);
        resolve(ok);
      },
    });
  });
}

/** Imperative prompt dialog. Resolves the entered string or null on cancel. */
export function prompt(opts: {
  title: string;
  label?: string;
  initial?: string;
  placeholder?: string;
  confirmLabel?: string;
}): Promise<string | null> {
  return new Promise((resolve) => {
    dialogStore.set({
      kind: "prompt",
      title: opts.title,
      label: opts.label,
      initial: opts.initial,
      placeholder: opts.placeholder,
      confirmLabel: opts.confirmLabel,
      resolve: (v) => {
        dialogStore.set(null);
        resolve(v);
      },
    });
  });
}

/** Fire-and-forget toast notification. Auto-dismisses after `durationMs`. */
export function toast(
  message: string,
  opts: { variant?: Toast["variant"]; durationMs?: number } = {},
): void {
  const t: Toast = {
    id: toastId++,
    message,
    variant: opts.variant ?? "info",
    expiresAt: Date.now() + (opts.durationMs ?? 3_500),
  };
  toastStore.set([...toastStore.get(), t]);
  setTimeout(() => {
    toastStore.set(toastStore.get().filter((x) => x.id !== t.id));
  }, opts.durationMs ?? 3_500);
}

export function dismissToast(id: number): void {
  toastStore.set(toastStore.get().filter((x) => x.id !== id));
}
