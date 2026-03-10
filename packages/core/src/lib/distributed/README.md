# Distributed Workflow Execution

Run workflow steps on different machines. A coordinator dispatches steps to workers via Postgres task queues. Workers poll their assigned queues, execute steps, and checkpoint results.

## How It Works

```
┌──────────────┐         ┌─────────────────────────────────┐
│   Client     │         │       Postgres                  │
│              │         │                                 │
│  submit()  ──┼────────►│  wf_workflows (state)           │
│              │         │  wf_step_queue (task dispatch)   │
└──────────────┘         └──────────┬──────────────────────┘
                                    │
                         ┌──────────┴──────────┐
                         │                     │
                    ┌────┴─────┐         ┌─────┴────┐
                    │Coordinator│         │Coordinator│  (leader election)
                    │          │         │ (standby) │
                    └────┬─────┘         └──────────┘
                         │
              ┌──────────┼──────────┐
              │          │          │
        ┌─────┴──┐  ┌───┴────┐  ┌──┴──────┐
        │Worker A│  │Worker B│  │Worker C │
        │"default"│ │ "gpu"  │  │  "ai"   │
        │        │  │        │  │         │
        │download│  │transcrb│  │summarize│
        │store   │  │encode  │  │classify │
        └────────┘  └────────┘  └─────────┘
```

### The Coordination Loop

```
1. Client calls coordinator.submit(workflow, workflowId, input)
   └─► Creates workflow in storage
   └─► Stores DAG structure
   └─► Enqueues initial ready steps (no dependencies)

2. Coordinator polls (every 1s):
   ┌─► Load workflow state from storage
   ├─► Compute DAG ready-set (steps whose deps are all completed)
   ├─► Enqueue ready steps to their assigned queues
   └─► If all steps done → mark workflow completed

3. Workers poll their queues (SKIP LOCKED):
   ┌─► Claim pending tasks from assigned queues
   ├─► Look up step handler in local StepRegistry
   ├─► Execute step with context (input, prev, deps)
   ├─► Checkpoint result to WorkflowStorage
   └─► Mark task as completed/failed in step queue

4. Coordinator detects completion on next poll:
   └─► Newly completed steps unlock downstream steps
   └─► Cycle continues until DAG is fully executed
```

### Step Routing

The coordinator routes steps to queues. Workers only poll their assigned queues.

```typescript
const coordinator = createCoordinator({
  storage,
  stepQueue,
  routing: {
    transcribe: "gpu", // → GPU workers
    summarize: "ai", // → AI workers
    "train-model": "gpu", // → GPU workers
    // everything else → "default" queue
  },
});
```

Routing is deployment config, not code. The same workflow definition works in-process (dev) and distributed (prod):

```typescript
// Same workflow — runs locally in dev, distributed in prod
const processVideo = workflow<{ videoId: string }>({ name: "process-video", storage })
  .step("download", ({ input }) => downloadVideo(input.videoId))
  .step("transcribe", { dependsOn: ["download"] }, ({ deps }) => transcribe(deps.download))
  .step("summarize", { dependsOn: ["transcribe"] }, ({ deps }) => summarize(deps.transcribe))
  .build();

// Dev: run in-process
await processVideo.run({ workflowId: "v1", input: { videoId: "abc" } });

// Prod: submit to coordinator, workers execute steps
await coordinator.submit({ workflow: processVideo, workflowId: "v1", input: { videoId: "abc" } });
```

## Example: Multi-Queue Video Processing

```typescript
import { createCoordinator, createWorker, MapStepRegistry, Pipeline } from "@promin/core";
import { PgStepQueue, PostgresWorkflowStorage, migrate } from "@promin/postgres";

// --- Shared setup (all processes) ---

const storage = await PostgresWorkflowStorage.create({ db });
const stepQueue = new PgStepQueue({ db });
await stepQueue.ensureTable();

// --- Coordinator process ---

const coordinator = createCoordinator({
  storage,
  stepQueue,
  routing: {
    transcribe: "gpu",
    summarize: "ai",
  },
  pollIntervalMs: 500,
});

// Submit a workflow
await coordinator.submit({
  workflow: processVideo,
  workflowId: "video-abc",
  input: { videoId: "abc" },
});

// Start coordination loop
coordinator.start(); // runs forever, enqueuing ready steps

// --- Default worker process ---

const defaultRegistry = new MapStepRegistry();
defaultRegistry.register("download", (ctx) =>
  Pipeline.fromPromise(() => downloadVideo((ctx.input as any).videoId)),
);

const defaultWorker = createWorker({
  storage,
  stepQueue,
  registry: defaultRegistry,
  queues: ["default"],
  concurrency: 5,
  pollIntervalMs: 500,
});
defaultWorker.start();

// --- GPU worker process (different machine, has GPU) ---

const gpuRegistry = new MapStepRegistry();
gpuRegistry.register("transcribe", (ctx) =>
  Pipeline.fromPromise(() => whisperTranscribe(ctx.prev as Buffer)),
);

const gpuWorker = createWorker({
  storage,
  stepQueue,
  registry: gpuRegistry,
  queues: ["gpu"],
  concurrency: 2, // limited by GPU memory
  pollIntervalMs: 500,
});
gpuWorker.start();

// --- AI worker process ---

const aiRegistry = new MapStepRegistry();
aiRegistry.register("summarize", (ctx) =>
  Pipeline.fromPromise(() => llmSummarize(ctx.prev as string)),
);

const aiWorker = createWorker({
  storage,
  stepQueue,
  registry: aiRegistry,
  queues: ["ai"],
  concurrency: 10,
  pollIntervalMs: 500,
});
aiWorker.start();
```

### What Happens

```
Time 0s:  Client submits workflow "video-abc"
          Coordinator creates workflow state, enqueues "download" → default queue

Time 1s:  Default worker claims "download", starts executing
          GPU/AI workers poll their queues — nothing there yet

Time 3s:  Default worker completes "download", checkpoints result
          Coordinator detects completion, enqueues "transcribe" → gpu queue

Time 4s:  GPU worker claims "transcribe", starts Whisper inference
          Default/AI workers idle

Time 15s: GPU worker completes "transcribe", checkpoints result
          Coordinator enqueues "summarize" → ai queue

Time 16s: AI worker claims "summarize", calls LLM
          Default/GPU workers idle

Time 18s: AI worker completes "summarize", checkpoints result
          Coordinator detects all steps done → marks workflow completed
```

## Architecture Details

### SKIP LOCKED — Why Not Polling?

Workers claim tasks with `SELECT FOR UPDATE SKIP LOCKED`:

```sql
UPDATE wf_step_queue
SET status = 'running', claimed_by = 'worker-1'
WHERE id IN (
  SELECT id FROM wf_step_queue
  WHERE status = 'pending' AND queue IN ('gpu')
  ORDER BY created_at ASC
  LIMIT 5
  FOR UPDATE SKIP LOCKED
)
RETURNING *;
```

This gives:

- **Exactly-once delivery** — locked rows are invisible to other workers
- **Natural load balancing** — fastest worker gets the next task
- **FIFO ordering** — `ORDER BY created_at`
- **No infrastructure** — just Postgres, no Redis/RabbitMQ/SQS

### StepRegistry vs ActivityRegistry

|              | StepRegistry                    | ActivityRegistry                    |
| ------------ | ------------------------------- | ----------------------------------- |
| Purpose      | Worker step execution           | Visual editor compilation           |
| Used by      | WorkflowWorker                  | compileWorkflow()                   |
| Context      | StepContext (input, prev, deps) | ActivityContext (input, prev, deps) |
| Returns      | Pipeline or Promise             | Pipeline                            |
| Registration | By step name                    | By activity ref                     |

Both map names to functions. StepRegistry is for workers; ActivityRegistry is for the visual editor compiler. A worker could use both if it runs compiled visual editor workflows.

### Graceful Shutdown

```typescript
// Signal handler
process.on("SIGTERM", async () => {
  await worker.stop(); // finishes current tasks, then exits
  process.exit(0);
});
```

`worker.stop()` sets `running = false`, then waits for active tasks to complete. No in-flight work is lost.

### Scaling

- **More throughput?** Add more workers polling the same queue
- **Specialized hardware?** Create a new queue name + workers with that capability
- **Backpressure?** Workers only claim up to `concurrency` tasks at a time
- **Monitoring?** `stepQueue.metrics()` shows pending/running/completed/failed per queue
