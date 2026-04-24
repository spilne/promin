import { useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type { WorkflowDefDto } from "../../../server/routes/workflow-defs.ts";
import { InputForm } from "../ui/input-form.tsx";

interface TriggerModalProps {
  def: WorkflowDefDto;
  onClose: () => void;
  onTriggered: (workflowId: string) => void;
}

/**
 * Two modes: structured form (default when the workflow has a sample input
 * that's a plain object) or raw JSON textarea (for nested/opaque inputs).
 * Users can toggle between them with the "Raw JSON" switch.
 */
export function TriggerModal({ def, onClose, onTriggered }: TriggerModalProps) {
  const sample = def.sampleInput;
  const canForm = typeof sample === "object" && sample !== null && !Array.isArray(sample);
  const [useForm, setUseForm] = useState(canForm);
  const [formValue, setFormValue] = useState<Record<string, unknown>>(
    canForm ? (sample as Record<string, unknown>) : {},
  );
  const [formError, setFormError] = useState<string | undefined>(undefined);
  const [jsonValue, setJsonValue] = useState(
    sample !== undefined ? JSON.stringify(sample, null, 2) : "{}",
  );
  const [workflowId, setWorkflowId] = useState("");
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | undefined>(undefined);

  const submit = async () => {
    setError(undefined);
    let input: unknown;
    if (useForm) {
      if (formError) {
        setError(formError);
        return;
      }
      input = formValue;
    } else {
      if (jsonValue.trim() === "") {
        input = {};
      } else {
        try {
          input = JSON.parse(jsonValue);
        } catch {
          setError("Input must be valid JSON");
          return;
        }
      }
    }
    setSubmitting(true);
    try {
      const res = await api.triggerWorkflow(def.name, {
        input,
        workflowId: workflowId.trim() || undefined,
      });
      onTriggered(res.workflowId);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <dialog open class="modal modal-open">
      <div class="modal-box anim-pop max-w-lg">
        <h3 class="font-semibold text-lg">Trigger {def.name}</h3>
        {def.type && <p class="text-xs text-base-content/50 mb-3">type: {def.type}</p>}

        <div class="space-y-3">
          <label class="form-control">
            <div class="label pb-0.5">
              <span class="label-text text-sm">Workflow ID (optional)</span>
            </div>
            <input
              class="input input-bordered input-sm w-full font-mono"
              placeholder="leave blank to auto-generate"
              value={workflowId}
              onInput={(e) => setWorkflowId((e.target as HTMLInputElement).value)}
            />
          </label>

          <div>
            <div class="flex items-center mb-1">
              <span class="label-text text-sm">Input</span>
              <div class="flex-1" />
              {canForm && (
                <label class="cursor-pointer label gap-2 py-0">
                  <span class="label-text text-xs">Raw JSON</span>
                  <input
                    type="checkbox"
                    class="toggle toggle-sm"
                    checked={!useForm}
                    onChange={() => setUseForm((v) => !v)}
                  />
                </label>
              )}
            </div>

            {useForm && canForm ? (
              <InputForm
                sample={sample as Record<string, unknown>}
                onChange={(v, err) => {
                  setFormValue(v);
                  setFormError(err);
                }}
              />
            ) : (
              <textarea
                class="textarea textarea-bordered textarea-sm w-full font-mono"
                rows={8}
                value={jsonValue}
                onInput={(e) => setJsonValue((e.target as HTMLTextAreaElement).value)}
              />
            )}
          </div>
        </div>

        {error && <div class="alert alert-error text-sm mt-3">{error}</div>}
        {formError && !error && useForm && (
          <div class="alert alert-warning text-xs mt-3">{formError}</div>
        )}

        <div class="modal-action">
          <button class="btn btn-sm btn-ghost" onClick={onClose} disabled={submitting}>
            Cancel
          </button>
          <button class="btn btn-sm btn-primary" onClick={submit} disabled={submitting}>
            {submitting ? "Triggering…" : "Trigger"}
          </button>
        </div>
      </div>
      <form method="dialog" class="modal-backdrop" onClick={onClose}>
        <button>close</button>
      </form>
    </dialog>
  );
}
