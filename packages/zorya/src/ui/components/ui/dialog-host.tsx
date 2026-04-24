// ---------------------------------------------------------------------------
// DialogHost + ToastHost — mount once at the app root. Subscribes to the
// imperative dialog/toast stores and renders the current request, if any.
// ---------------------------------------------------------------------------

import { useEffect, useState } from "preact/hooks";
import {
  dialogStore,
  toastStore,
  type DialogRequest,
  type Toast,
  dismissToast,
} from "../../lib/dialogs.ts";

export function DialogHost() {
  const [req, setReq] = useState<DialogRequest | null>(dialogStore.get());
  useEffect(() => dialogStore.subscribe(setReq), []);
  if (!req) return null;
  if (req.kind === "confirm") return <ConfirmView req={req} />;
  if (req.kind === "prompt") return <PromptView req={req} />;
  return null;
}

function ConfirmView({ req }: { req: Extract<DialogRequest, { kind: "confirm" }> }) {
  const danger = req.variant === "danger";
  const onKey = (e: KeyboardEvent) => {
    if (e.key === "Enter") req.resolve(true);
    if (e.key === "Escape") req.resolve(false);
  };
  useEffect(() => {
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  return (
    <dialog open class="modal modal-open">
      <div class="modal-box anim-pop max-w-md">
        <h3 class="font-semibold text-lg">{req.title}</h3>
        {req.message && (
          <p class="text-sm text-base-content/70 mt-2 whitespace-pre-line">{req.message}</p>
        )}
        <div class="modal-action">
          <button class="btn btn-sm btn-ghost" onClick={() => req.resolve(false)}>
            {req.cancelLabel ?? "Cancel"}
          </button>
          <button
            class={`btn btn-sm ${danger ? "btn-error" : "btn-primary"}`}
            onClick={() => req.resolve(true)}
          >
            {req.confirmLabel ?? "Confirm"}
          </button>
        </div>
      </div>
      <form method="dialog" class="modal-backdrop" onClick={() => req.resolve(false)}>
        <button>close</button>
      </form>
    </dialog>
  );
}

function PromptView({ req }: { req: Extract<DialogRequest, { kind: "prompt" }> }) {
  const [value, setValue] = useState(req.initial ?? "");
  const onKey = (e: KeyboardEvent) => {
    if (e.key === "Enter") req.resolve(value);
    if (e.key === "Escape") req.resolve(null);
  };
  useEffect(() => {
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [value]);

  return (
    <dialog open class="modal modal-open">
      <div class="modal-box anim-pop max-w-md">
        <h3 class="font-semibold text-lg">{req.title}</h3>
        <label class="form-control mt-2">
          {req.label && (
            <div class="label pb-1">
              <span class="label-text text-sm">{req.label}</span>
            </div>
          )}
          <input
            type="text"
            class="input input-bordered input-sm w-full"
            placeholder={req.placeholder}
            value={value}
            autoFocus
            onInput={(e) => setValue((e.target as HTMLInputElement).value)}
          />
        </label>
        <div class="modal-action">
          <button class="btn btn-sm btn-ghost" onClick={() => req.resolve(null)}>
            Cancel
          </button>
          <button class="btn btn-sm btn-primary" onClick={() => req.resolve(value)}>
            {req.confirmLabel ?? "OK"}
          </button>
        </div>
      </div>
      <form method="dialog" class="modal-backdrop" onClick={() => req.resolve(null)}>
        <button>close</button>
      </form>
    </dialog>
  );
}

// ---------------------------------------------------------------------------
// ToastHost — stacks transient notifications in the bottom-right.

export function ToastHost() {
  const [toasts, setToasts] = useState<Toast[]>(toastStore.get());
  useEffect(() => toastStore.subscribe(setToasts), []);
  if (toasts.length === 0) return null;
  return (
    <div class="toast toast-end z-50">
      {toasts.map((t) => (
        <div class={`alert ${variantClass(t.variant)} shadow-lg anim-pop`}>
          <span class="text-sm">{t.message}</span>
          <button class="btn btn-xs btn-ghost" onClick={() => dismissToast(t.id)}>
            ✕
          </button>
        </div>
      ))}
    </div>
  );
}

function variantClass(v: Toast["variant"]): string {
  switch (v) {
    case "success":
      return "alert-success";
    case "error":
      return "alert-error";
    case "warning":
      return "alert-warning";
    default:
      return "alert-info";
  }
}
