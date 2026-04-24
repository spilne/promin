import { useEffect, useState } from "preact/hooks";
import type { RunDto, StepDto } from "../../../server/api-types.ts";
import { api } from "../../api/client.ts";

interface SignalModalProps {
  run: RunDto;
  onClose: () => void;
  onSent: () => void;
}

/**
 * Structured signal-sending form. Pre-fills the signal name from any step
 * currently in a waiting_for_signal state and lists previously-delivered
 * signal names as suggestions, so users don't have to remember them.
 */
export function SignalModal({ run, onClose, onSent }: SignalModalProps) {
  const waitingStep = findWaitingStep(run);
  const [signalName, setSignalName] = useState(waitingStep?.signalName ?? "");
  const [payloadRaw, setPayloadRaw] = useState("");
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    api
      .getRunSignals(run.workflowId)
      .then((r) => setSuggestions(Array.from(new Set(r.signals.map((s) => s.signalName))).sort()))
      .catch(() => {});
  }, [run.workflowId]);

  const submit = async () => {
    setError(undefined);
    const name = signalName.trim();
    if (!name) {
      setError("Signal name is required");
      return;
    }
    let payload: unknown = null;
    if (payloadRaw.trim()) {
      try {
        payload = JSON.parse(payloadRaw);
      } catch {
        setError("Payload must be valid JSON");
        return;
      }
    }
    setSubmitting(true);
    try {
      await api.signalRun(run.workflowId, { signalName: name, payload });
      onSent();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <dialog open class="modal modal-open">
      <div class="modal-box anim-pop max-w-lg">
        <h3 class="font-semibold text-lg mb-3">Send signal</h3>
        <p class="text-sm text-base-content/60 mb-3">
          To{" "}
          <span class="font-mono text-xs bg-base-200 px-1.5 py-0.5 rounded">{run.workflowId}</span>
        </p>

        <div class="space-y-3">
          <label class="form-control">
            <div class="label pb-1">
              <span class="label-text text-sm">Signal name</span>
            </div>
            <input
              class="input input-bordered input-sm w-full font-mono"
              placeholder={waitingStep?.signalName ?? "e.g. approval"}
              list="signal-suggestions"
              value={signalName}
              onInput={(e) => setSignalName((e.target as HTMLInputElement).value)}
            />
            <datalist id="signal-suggestions">
              {suggestions.map((s) => (
                <option value={s} />
              ))}
            </datalist>
            {waitingStep && (
              <div class="text-xs text-base-content/50 mt-1">
                Step <span class="font-mono">{waitingStep.stepName}</span> is currently waiting for{" "}
                <span class="font-mono">{waitingStep.signalName}</span>.
              </div>
            )}
          </label>

          <label class="form-control">
            <div class="label pb-1 items-center">
              <span class="label-text text-sm">Payload (JSON)</span>
              <div class="flex-1" />
              <PayloadTemplates onPick={setPayloadRaw} />
            </div>
            <textarea
              class="textarea textarea-bordered textarea-sm w-full font-mono"
              rows={5}
              placeholder='null, "ok", or {"approved":true}'
              value={payloadRaw}
              onInput={(e) => setPayloadRaw((e.target as HTMLTextAreaElement).value)}
            />
            <div class="text-xs text-base-content/50 mt-1">
              Leave blank for <code>null</code>.
            </div>
          </label>
        </div>

        {error && <div class="alert alert-error text-sm mt-3">{error}</div>}

        <div class="modal-action">
          <button class="btn btn-sm btn-ghost" onClick={onClose} disabled={submitting}>
            Cancel
          </button>
          <button class="btn btn-sm btn-primary" onClick={submit} disabled={submitting}>
            {submitting ? "Sending…" : "Send"}
          </button>
        </div>
      </div>
      <form method="dialog" class="modal-backdrop" onClick={onClose}>
        <button>close</button>
      </form>
    </dialog>
  );
}

function findWaitingStep(run: RunDto): StepDto | undefined {
  // Engine currently stores "waiting_signal" (bug: declared type is
  // "waiting_for_signal"). Match both so the prefill keeps working
  // after the upstream fix.
  return run.steps.find(
    (s) =>
      s.signalName !== undefined &&
      (s.status === "waiting_for_signal" || (s.status as string) === "waiting_signal"),
  );
}

/**
 * Tiny preset picker — the common payload shapes users tend to type
 * manually. Reduces keystrokes without pretending to know the workflow's
 * real signal schema (we don't expose that yet).
 */
function PayloadTemplates({ onPick }: { onPick: (json: string) => void }) {
  const templates: Array<{ label: string; json: string }> = [
    { label: "null", json: "" },
    { label: '{"approved":true}', json: '{"approved": true}' },
    { label: '{"approved":false}', json: '{"approved": false}' },
    { label: "{}", json: "{}" },
  ];
  return (
    <div class="flex gap-1">
      {templates.map((t) => (
        <button
          class="btn btn-xs btn-ghost font-mono"
          onClick={() => onPick(t.json)}
          title="Prefill payload"
        >
          {t.label}
        </button>
      ))}
    </div>
  );
}
