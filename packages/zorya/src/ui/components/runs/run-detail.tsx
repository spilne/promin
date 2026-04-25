import { useCallback, useEffect, useMemo, useRef, useState } from "preact/hooks";
import { api } from "../../api/client.ts";
import { useSse } from "../../hooks/use-sse.ts";
import type { RunDto, RunEvent, StepDto } from "../../../server/api-types.ts";
import { StatusBadge } from "../ui/status-badge.tsx";
import { Skeleton } from "../ui/skeleton.tsx";
import { Tabs, type TabDef } from "../ui/tabs.tsx";
import { StepTimeline } from "./step-timeline.tsx";
import { StepDag } from "./step-dag.tsx";
import { StepGrid } from "./step-grid.tsx";
import { OverviewTab } from "./tabs/overview-tab.tsx";
import { StepTab } from "./tabs/step-tab.tsx";
import { PayloadTab } from "./tabs/payload-tab.tsx";
import { SignalsTab } from "./tabs/signals-tab.tsx";
import { HistoryTab } from "./tabs/history-tab.tsx";
import { ChildrenTab } from "./tabs/children-tab.tsx";
import { CalendarTab } from "./tabs/calendar-tab.tsx";
import { SignalModal } from "./signal-modal.tsx";
import { formatDuration } from "../../lib/format.ts";
import { confirm, prompt, toast } from "../../lib/dialogs.ts";

interface RunDetailProps {
  id: string;
  onBack: () => void;
  /** Open another run in the detail view (used by parent-link + children-tab). */
  onOpenRun?: (id: string) => void;
  /** URL query params — drives initial tab/step/view selection. */
  queryParams?: URLSearchParams;
  /** Emit tab/step/view changes back to the URL hash. */
  onQueryChange?: (params: URLSearchParams) => void;
}

type RightTab = "overview" | "step" | "payload" | "signals" | "history" | "children" | "calendar";
type StepView = "timeline" | "graph" | "grid";

const RIGHT_TABS: ReadonlyArray<RightTab> = [
  "overview",
  "step",
  "payload",
  "signals",
  "history",
  "children",
  "calendar",
];

const STEP_VIEWS: ReadonlyArray<StepView> = ["timeline", "graph", "grid"];

export function RunDetail({ id, onBack, onOpenRun, queryParams, onQueryChange }: RunDetailProps) {
  const initialTabRaw = queryParams?.get("tab");
  const initialTab: RightTab = RIGHT_TABS.includes(initialTabRaw as RightTab)
    ? (initialTabRaw as RightTab)
    : "overview";
  const viewParam = queryParams?.get("view");
  const initialStepView: StepView = STEP_VIEWS.includes(viewParam as StepView)
    ? (viewParam as StepView)
    : "timeline";
  const initialSelectedStep = queryParams?.get("step") ?? undefined;

  const [run, setRun] = useState<RunDto | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);
  const [selectedStep, setSelectedStep] = useState<string | undefined>(initialSelectedStep);
  const [tab, setTab] = useState<RightTab>(initialTab);
  const [stepView, setStepView] = useState<StepView>(initialStepView);
  const [signalOpen, setSignalOpen] = useState(false);

  // Sync tab/view/selected-step back to the URL hash so refresh preserves them.
  useEffect(() => {
    if (!onQueryChange) return;
    const qp = new URLSearchParams();
    if (tab !== "overview") qp.set("tab", tab);
    if (stepView !== "timeline") qp.set("view", stepView);
    if (selectedStep) qp.set("step", selectedStep);
    onQueryChange(qp);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tab, stepView, selectedStep]);

  useEffect(() => {
    let cancelled = false;
    api
      .getRun(id)
      .then((r) => !cancelled && setRun(r))
      .catch((e) => !cancelled && setError(String(e)));
    return () => {
      cancelled = true;
    };
  }, [id]);

  useSse<RunEvent>(api.eventsUrl(id), (ev) => {
    setRun((prev) => applyEvent(prev, ev));
  });

  // Auto-jump to Step tab when the user selects a step in the timeline.
  // Skipped on initial mount so deep links that set { tab: overview,
  // step: foo } don't get flipped to the Step tab automatically.
  const skipFirstSelect = useRef(true);
  useEffect(() => {
    if (skipFirstSelect.current) {
      skipFirstSelect.current = false;
      return;
    }
    if (selectedStep) setTab("step");
  }, [selectedStep]);

  const cancel = useCallback(async () => {
    const ok = await confirm({
      title: "Cancel this run?",
      message: "The workflow will stop at its next checkpoint.",
      confirmLabel: "Cancel run",
      variant: "danger",
    });
    if (!ok) return;
    try {
      await api.cancelRun(id);
      setRun(await api.getRun(id));
      toast("Run cancelled", { variant: "success" });
    } catch (e) {
      toast(`Cancel failed: ${e}`, { variant: "error" });
    }
  }, [id]);

  // `signal` action now just opens the modal — the form inside handles
  // prefilling from the waiting step, JSON validation, and delivery.
  const openSignalModal = useCallback(() => setSignalOpen(true), []);

  const rerun = useCallback(async () => {
    const ok = await confirm({
      title: "Re-run this workflow?",
      message:
        "Starts from the beginning. Prior run history is preserved and visible in the History tab.",
      confirmLabel: "Re-run",
    });
    if (!ok) return;
    try {
      await api.rerunRun(id);
      setRun(await api.getRun(id));
      toast("Re-run started", { variant: "success" });
    } catch (e) {
      toast(`Re-run failed: ${e}`, { variant: "error" });
    }
  }, [id]);

  const markSuccess = useCallback(async () => {
    const ok = await confirm({
      title: "Mark run as completed?",
      message:
        "Admin override — only use when the run is stuck and you've verified it succeeded out-of-band.",
      confirmLabel: "Mark success",
    });
    if (!ok) return;
    try {
      await api.markRunSuccess(id);
      setRun(await api.getRun(id));
      toast("Marked as completed", { variant: "success" });
    } catch (e) {
      toast(`Mark success failed: ${e}`, { variant: "error" });
    }
  }, [id]);

  const markFailed = useCallback(async () => {
    const reason = await prompt({
      title: "Mark run as failed",
      label: "Reason",
      initial: "manual override",
      placeholder: "Why this run is being force-failed",
      confirmLabel: "Mark failed",
    });
    if (reason === null) return;
    try {
      await api.markRunFailed(id, reason || "manual override");
      setRun(await api.getRun(id));
      toast("Marked as failed", { variant: "warning" });
    } catch (e) {
      toast(`Mark failed: ${e}`, { variant: "error" });
    }
  }, [id]);

  const selected = useMemo(() => {
    if (!run || !selectedStep) return undefined;
    return run.steps.find((s) => s.stepName === selectedStep);
  }, [run, selectedStep]);

  if (error) {
    return (
      <div class="p-4 max-w-7xl mx-auto">
        <div class="alert alert-error">{error}</div>
      </div>
    );
  }
  if (!run) return <RunDetailSkeleton onBack={onBack} />;

  const totalMs = run.completedAt
    ? new Date(run.completedAt).getTime() - new Date(run.createdAt).getTime()
    : Date.now() - new Date(run.createdAt).getTime();

  const terminal = run.status === "completed" || run.status === "failed";

  const tabs: ReadonlyArray<TabDef<RightTab>> = [
    { id: "overview", label: "Overview" },
    { id: "step", label: "Step", enabled: !!selected },
    { id: "payload", label: "Payload" },
    { id: "signals", label: "Signals" },
    { id: "history", label: "History" },
    { id: "children", label: "Children" },
    { id: "calendar", label: "Calendar" },
  ];

  return (
    <div class="anim-page p-4 max-w-[1400px] mx-auto space-y-4">
      <div class="flex items-center gap-3 flex-wrap">
        <button class="btn btn-sm btn-ghost" onClick={onBack}>
          ← Runs
        </button>
        <div class="text-base-content/40">/</div>
        <div class="font-semibold">{run.workflowName}</div>
        {run.namespace && <span class="badge badge-sm badge-ghost font-mono">{run.namespace}</span>}
        <div class="font-mono text-sm text-base-content/60">{run.workflowId}</div>
        <StatusBadge status={run.status} size="md" />
        <div class="text-sm text-base-content/60">{formatDuration(totalMs)} total</div>
        <div class="flex-1" />
        <button class="btn btn-sm btn-outline" onClick={openSignalModal}>
          Send signal
        </button>
        {!terminal && (
          <button class="btn btn-sm btn-error btn-outline" onClick={cancel}>
            Cancel
          </button>
        )}
        {/* Overflow menu for less-common admin actions. */}
        <div class="dropdown dropdown-end">
          <button tabIndex={0} class="btn btn-sm btn-ghost" title="More actions">
            ⋯
          </button>
          <ul
            tabIndex={0}
            class="dropdown-content z-10 menu menu-sm bg-base-200 shadow rounded-box w-52 mt-1"
          >
            <li>
              <a onClick={rerun}>↻ Re-run from start</a>
            </li>
            {!terminal && (
              <>
                <li>
                  <a onClick={markSuccess}>✓ Mark success</a>
                </li>
                <li>
                  <a class="text-error" onClick={markFailed}>
                    ✕ Mark failed
                  </a>
                </li>
              </>
            )}
          </ul>
        </div>
      </div>

      {/* Timeline / Graph / Grid toggle */}
      <div class="flex items-center gap-2">
        <div class="join">
          <button
            class={`btn btn-sm join-item ${stepView === "timeline" ? "btn-primary" : "btn-ghost"}`}
            onClick={() => setStepView("timeline")}
          >
            Timeline
          </button>
          <button
            class={`btn btn-sm join-item ${stepView === "graph" ? "btn-primary" : "btn-ghost"}`}
            onClick={() => setStepView("graph")}
          >
            Graph
          </button>
          <button
            class={`btn btn-sm join-item ${stepView === "grid" ? "btn-primary" : "btn-ghost"}`}
            onClick={() => setStepView("grid")}
          >
            Grid
          </button>
        </div>
      </div>

      {/* Step view at full width, tabs panel directly below. Stacks
          vertically so the graph gets the whole row and the tabs don't
          fight for horizontal space. */}
      <div class="space-y-4">
        {stepView === "timeline" && (
          <StepTimeline run={run} selectedStep={selectedStep} onSelectStep={setSelectedStep} />
        )}
        {stepView === "graph" && (
          <StepDag run={run} selectedStep={selectedStep} onSelectStep={setSelectedStep} />
        )}
        {stepView === "grid" && (
          <StepGrid
            workflowName={run.workflowName}
            currentWorkflowId={run.workflowId}
            onOpenRun={(id) => onOpenRun?.(id)}
          />
        )}

        <div class="card bg-base-100 shadow border border-base-content/15">
          <div class="card-body p-0">
            <Tabs tabs={tabs} active={tab} onChange={setTab} class="px-2 pt-2" />
            <div class="p-4">
              {tab === "overview" && <OverviewTab run={run} onOpenRun={onOpenRun} />}
              {tab === "step" && selected && <StepTab runId={id} step={selected} />}
              {tab === "step" && !selected && (
                <div class="text-base-content/50 text-sm py-6 text-center">
                  Click a step in the timeline to inspect
                </div>
              )}
              {tab === "payload" && <PayloadTab run={run} />}
              {tab === "signals" && <SignalsTab runId={id} />}
              {tab === "history" && <HistoryTab runId={id} />}
              {tab === "children" && <ChildrenTab runId={id} onOpenRun={onOpenRun ?? (() => {})} />}
              {tab === "calendar" && <CalendarTab workflowName={run.workflowName} />}
            </div>
          </div>
        </div>
      </div>

      {signalOpen && (
        <SignalModal
          run={run}
          onClose={() => setSignalOpen(false)}
          onSent={() => setSignalOpen(false)}
        />
      )}
    </div>
  );
}

function RunDetailSkeleton({ onBack }: { onBack: () => void }) {
  return (
    <div class="anim-page p-4 max-w-[1400px] mx-auto space-y-4">
      <div class="flex items-center gap-3">
        <button class="btn btn-sm btn-ghost" onClick={onBack}>
          ← Runs
        </button>
        <div class="text-base-content/40">/</div>
        <Skeleton w="w-32" h="h-5" />
        <Skeleton w="w-48" h="h-4" />
        <Skeleton w="w-20" h="h-5" />
      </div>
      <div class="grid grid-cols-1 lg:grid-cols-[minmax(0,1fr)_380px] gap-4">
        <div class="card bg-base-100 shadow">
          <div class="card-body p-4 space-y-3">
            <Skeleton w="w-40" h="h-5" />
            <div class="space-y-2 pt-2">
              {Array.from({ length: 5 }).map((_, i) => (
                <div class="flex items-center gap-2">
                  <Skeleton w="w-48" h="h-4" />
                  <Skeleton
                    w={i === 0 ? "w-24" : i === 1 ? "w-48" : i === 2 ? "w-32" : "w-40"}
                    h="h-5"
                  />
                </div>
              ))}
            </div>
          </div>
        </div>
        <div class="card bg-base-100 shadow self-start">
          <div class="card-body p-4 space-y-2">
            <Skeleton w="w-24" h="h-5" />
            {Array.from({ length: 6 }).map(() => (
              <Skeleton w="w-full" h="h-3" />
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function applyEvent(prev: RunDto | undefined, ev: RunEvent): RunDto | undefined {
  if (ev.type === "snapshot") return ev.run;
  if (!prev) return prev;
  if (ev.type === "status") return { ...prev, status: ev.status };
  if (ev.type === "step") return { ...prev, steps: mergeStep(prev.steps, ev.stepName, ev.step) };
  return prev;
}

function mergeStep(steps: StepDto[], name: string, next: StepDto): StepDto[] {
  const idx = steps.findIndex((s) => s.stepName === name);
  if (idx === -1) return [...steps, next];
  const copy = steps.slice();
  copy[idx] = next;
  return copy;
}
