import type { RunDto } from "../../../../server/api-types.ts";
import { DataList } from "../../ui/data-list.tsx";
import { Section } from "../../ui/section.tsx";
import { JsonBlock } from "../../ui/json-block.tsx";
import { formatRelative } from "../../../lib/format.ts";

interface OverviewTabProps {
  run: RunDto;
  onOpenRun?: (id: string) => void;
}

export function OverviewTab({ run, onOpenRun }: OverviewTabProps) {
  return (
    <div class="space-y-4">
      <DataList
        items={[
          {
            label: "ID",
            value: run.workflowId,
            valueClass: "font-mono text-xs break-all",
          },
          { label: "Name", value: run.workflowName },
          { label: "Type", value: run.workflowType, skipEmpty: true },
          { label: "Namespace", value: run.namespace, skipEmpty: true },
          { label: "Version", value: run.version, skipEmpty: true },
          { label: "Run", value: `#${run.run}` },
          {
            label: "Parent",
            value: run.parentWorkflowId ? (
              <button
                class="btn btn-xs btn-ghost font-mono"
                onClick={() => run.parentWorkflowId && onOpenRun?.(run.parentWorkflowId)}
              >
                ↗ {run.parentWorkflowId}
              </button>
            ) : undefined,
            skipEmpty: true,
          },
          { label: "Created", value: formatRelative(run.createdAt) },
          {
            label: "Started",
            value: run.startedAt && formatRelative(run.startedAt),
            skipEmpty: true,
          },
          {
            label: "Completed",
            value: run.completedAt && formatRelative(run.completedAt),
            skipEmpty: true,
          },
        ]}
      />
      {run.metadata && Object.keys(run.metadata).length > 0 && (
        <Section title="Metadata">
          <JsonBlock value={run.metadata} />
        </Section>
      )}
    </div>
  );
}
