// ---------------------------------------------------------------------------
// Workers route — /api/workers
//
// Returns the list of registered workers via a pluggable provider. Zorya
// itself doesn't manage a worker registry; the embedder passes one in.
// ---------------------------------------------------------------------------

import { json } from "../router.ts";
import type { WorkerDto, WorkersResponse } from "../api-types.ts";

export interface WorkersProvider {
  listWorkers(): Promise<WorkerDto[]>;
}

export const emptyWorkersProvider: WorkersProvider = {
  listWorkers: async () => [],
};

export function listWorkers(provider: WorkersProvider) {
  return async (): Promise<Response> => {
    const workers = await provider.listWorkers();
    const response: WorkersResponse = { workers };
    return json(200, response);
  };
}
