import { useEffect, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import type { ScheduleDto } from "../../../server/routes/schedules.ts";
import { Combobox } from "../ui/combobox.tsx";

interface CreateScheduleModalProps {
  onClose: () => void;
  onCreated: () => void;
  /** If provided, modal opens in edit mode pre-filled with this schedule. */
  editing?: ScheduleDto;
}

type TriggerKind = "cron" | "interval";

const CRON_PRESETS: Array<{ label: string; cron: string }> = [
  { label: "Every minute", cron: "* * * * *" },
  { label: "Every hour", cron: "0 * * * *" },
  { label: "Daily 9am UTC", cron: "0 9 * * *" },
  { label: "Weekdays 9am UTC", cron: "0 9 * * 1-5" },
  { label: "Mondays 2am UTC", cron: "0 2 * * 1" },
];

const INTERVAL_PRESETS: Array<{ label: string; ms: number }> = [
  { label: "Every 1m", ms: 60_000 },
  { label: "Every 5m", ms: 5 * 60_000 },
  { label: "Every 15m", ms: 15 * 60_000 },
  { label: "Every 1h", ms: 60 * 60_000 },
];

export function CreateScheduleModal({ onClose, onCreated, editing }: CreateScheduleModalProps) {
  const isEdit = !!editing;
  const existingInput = editing?.metadata?.["input"];

  const [id, setId] = useState(editing?.id ?? "");
  const [name, setName] = useState(editing?.name ?? "");
  const [workflowName, setWorkflowName] = useState(
    (editing?.metadata?.["workflowName"] as string | undefined) ?? "",
  );
  const [workflowNames, setWorkflowNames] = useState<string[]>([]);
  const [kind, setKind] = useState<TriggerKind>(
    editing?.intervalMs !== undefined ? "interval" : "cron",
  );
  const [cron, setCron] = useState(editing?.cron ?? "0 * * * *");
  const [intervalMs, setIntervalMs] = useState(editing?.intervalMs ?? 60 * 60_000);
  const [timezone, setTimezone] = useState(editing?.timezone ?? "UTC");
  const [inputJson, setInputJson] = useState(
    existingInput !== undefined ? JSON.stringify(existingInput, null, 2) : "",
  );
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | undefined>(undefined);

  useEffect(() => {
    api
      .listWorkflowNames()
      .then((r) => {
        setWorkflowNames(r.names);
        if (!workflowName && r.names.length > 0) setWorkflowName(r.names[0]!);
      })
      .catch(() => {});
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const canSubmit = id.trim().length > 0 && workflowName.trim().length > 0;

  const submit = async () => {
    setError(undefined);
    let input: unknown = undefined;
    if (inputJson.trim()) {
      try {
        input = JSON.parse(inputJson);
      } catch {
        setError("Input must be valid JSON");
        return;
      }
    }
    setSubmitting(true);
    try {
      // Edit is modeled as "delete + recreate" against the InMemorySchedulerStorage
      // upsert semantics. Since the server POST uses upsertSchedule, it's
      // effectively overwrite-if-exists. We just never change the id on edit.
      await api.createSchedule({
        id: id.trim(),
        name: name.trim() || undefined,
        workflowName: workflowName.trim(),
        timezone: kind === "cron" ? timezone : undefined,
        cron: kind === "cron" ? cron : undefined,
        intervalMs: kind === "interval" ? intervalMs : undefined,
        input,
        enabled: editing?.enabled ?? true,
      });
      onCreated();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <dialog open class="modal modal-open">
      <div class="modal-box anim-pop max-w-xl">
        <h3 class="font-semibold text-lg mb-3">
          {isEdit ? `Edit schedule ${editing?.id}` : "New schedule"}
        </h3>

        <div class="grid grid-cols-2 gap-3">
          <label class="form-control">
            <div class="label pb-1">
              <span class="label-text text-sm">ID</span>
            </div>
            <input
              class="input input-bordered input-sm w-full font-mono"
              placeholder="my-schedule"
              value={id}
              onInput={(e) => setId((e.target as HTMLInputElement).value)}
              disabled={isEdit}
            />
          </label>

          <label class="form-control">
            <div class="label pb-1">
              <span class="label-text text-sm">Name (optional)</span>
            </div>
            <input
              class="input input-bordered input-sm w-full"
              placeholder="Human-readable"
              value={name}
              onInput={(e) => setName((e.target as HTMLInputElement).value)}
            />
          </label>

          <label class="form-control col-span-2">
            <div class="label pb-1">
              <span class="label-text text-sm">Workflow</span>
            </div>
            <div class="flex gap-2">
              <Combobox
                class="flex-1"
                value={workflowName}
                onChange={setWorkflowName}
                placeholder={workflowNames.length === 0 ? "No workflows found" : "Pick a workflow"}
                options={workflowNames.map((n) => ({ value: n }))}
              />
              <input
                class="input input-bordered input-sm w-48"
                placeholder="or type name"
                value={workflowName}
                onInput={(e) => setWorkflowName((e.target as HTMLInputElement).value)}
              />
            </div>
          </label>

          <div class="col-span-2">
            <div class="label pb-1">
              <span class="label-text text-sm">Trigger</span>
            </div>
            <div class="join mb-2">
              <button
                class={`btn btn-sm join-item ${kind === "cron" ? "btn-primary" : "btn-ghost"}`}
                onClick={() => setKind("cron")}
              >
                Cron
              </button>
              <button
                class={`btn btn-sm join-item ${kind === "interval" ? "btn-primary" : "btn-ghost"}`}
                onClick={() => setKind("interval")}
              >
                Interval
              </button>
            </div>

            {kind === "cron" ? (
              <div class="space-y-2">
                <input
                  class="input input-bordered input-sm w-full font-mono"
                  placeholder="0 * * * *"
                  value={cron}
                  onInput={(e) => setCron((e.target as HTMLInputElement).value)}
                />
                <div class="flex gap-1 flex-wrap">
                  {CRON_PRESETS.map((p) => (
                    <button class="btn btn-xs btn-ghost" onClick={() => setCron(p.cron)}>
                      {p.label}
                    </button>
                  ))}
                </div>
                <Combobox
                  class="w-full"
                  value={timezone}
                  onChange={setTimezone}
                  options={[
                    { value: "UTC" },
                    { value: "America/Edmonton" },
                    { value: "America/New_York" },
                    { value: "America/Los_Angeles" },
                    { value: "Europe/London" },
                    { value: "Europe/Berlin" },
                    { value: "Asia/Tokyo" },
                  ]}
                />
              </div>
            ) : (
              <div class="space-y-2">
                <div class="flex items-center gap-2">
                  <input
                    type="number"
                    min="1"
                    class="input input-bordered input-sm w-32 font-mono"
                    value={intervalMs}
                    onInput={(e) => setIntervalMs(Number((e.target as HTMLInputElement).value))}
                  />
                  <span class="text-sm text-base-content/60">ms</span>
                </div>
                <div class="flex gap-1 flex-wrap">
                  {INTERVAL_PRESETS.map((p) => (
                    <button class="btn btn-xs btn-ghost" onClick={() => setIntervalMs(p.ms)}>
                      {p.label}
                    </button>
                  ))}
                </div>
              </div>
            )}
          </div>

          <label class="form-control col-span-2">
            <div class="label pb-1">
              <span class="label-text text-sm">Input (optional JSON)</span>
            </div>
            <textarea
              class="textarea textarea-bordered textarea-sm w-full font-mono"
              rows={3}
              placeholder='{"source": "scheduled"}'
              value={inputJson}
              onInput={(e) => setInputJson((e.target as HTMLTextAreaElement).value)}
            />
          </label>
        </div>

        {error && <div class="alert alert-error text-sm mt-3">{error}</div>}

        <div class="modal-action">
          <button class="btn btn-sm btn-ghost" onClick={onClose} disabled={submitting}>
            Cancel
          </button>
          <button
            class="btn btn-sm btn-primary"
            onClick={submit}
            disabled={!canSubmit || submitting}
          >
            {submitting ? (isEdit ? "Saving…" : "Creating…") : isEdit ? "Save changes" : "Create"}
          </button>
        </div>
      </div>
      <form method="dialog" class="modal-backdrop" onClick={onClose}>
        <button>close</button>
      </form>
    </dialog>
  );
}
