import type { RunDto } from "../../../../server/api-types.ts";
import { Section } from "../../ui/section.tsx";
import { JsonBlock } from "../../ui/json-block.tsx";

export function PayloadTab({ run }: { run: RunDto }) {
  return (
    <div class="space-y-3">
      <Section title="Input">
        <JsonBlock value={run.input} />
      </Section>
      {run.result !== undefined && run.result !== null && (
        <Section title="Result">
          <JsonBlock value={run.result} />
        </Section>
      )}
      {run.error && (
        <Section title="Error">
          <JsonBlock value={run.error} variant="error" />
        </Section>
      )}
    </div>
  );
}
