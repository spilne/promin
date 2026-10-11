// ---------------------------------------------------------------------------
// Fetch-based Server-Sent Events.
//
// `EventSource` cannot send an `Authorization` header, so under API keys it
// gets a 401 on every stream. These helpers read `text/event-stream` bodies
// through `fetch`, which carries the dashboard key like every other call.
// ---------------------------------------------------------------------------

export interface SseMessage {
  /** `event:` field; "message" when absent. */
  event: string;
  /** `data:` lines joined with "\n". */
  data: string;
}

/** Thrown by `readSse` when the server answers with a non-2xx status. */
export class SseHttpError extends Error {
  constructor(readonly status: number) {
    super(`HTTP ${status}`);
    this.name = "SseHttpError";
  }
}

/** Parse one SSE frame (the text between blank lines). Comments and data-less frames give null. */
export function parseSseFrame(frame: string): SseMessage | null {
  let event = "message";
  const dataLines: string[] = [];
  for (const line of frame.split(/\r?\n/)) {
    if (line.startsWith(":")) continue; // comment / heartbeat
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice(5).replace(/^ /, ""));
  }
  if (dataLines.length === 0) return null;
  return { event, data: dataLines.join("\n") };
}

/** Feed decoded body text through `onMessage`, frame by frame. */
export async function readSseBody(params: {
  body: ReadableStream<Uint8Array>;
  onMessage: (message: SseMessage) => void;
}): Promise<void> {
  const reader = params.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  while (true) {
    const { value, done } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
    let sep: number;
    while ((sep = buffer.indexOf("\n\n")) !== -1) {
      const frame = buffer.slice(0, sep);
      buffer = buffer.slice(sep + 2);
      const message = parseSseFrame(frame);
      if (message) params.onMessage(message);
    }
  }
}

/**
 * GET `url` as an event stream and deliver its messages until the server
 * ends it or `signal` aborts. Throws `SseHttpError` on a non-2xx answer.
 */
export async function readSse(params: {
  url: string;
  headers?: HeadersInit;
  signal?: AbortSignal;
  fetchImpl?: typeof fetch;
  onMessage: (message: SseMessage) => void;
}): Promise<void> {
  const headers = new Headers(params.headers);
  headers.set("accept", "text/event-stream");
  const res = await (params.fetchImpl ?? fetch)(params.url, {
    headers,
    ...(params.signal && { signal: params.signal }),
  });
  if (!res.ok || !res.body) throw new SseHttpError(res.status);
  await readSseBody({ body: res.body, onMessage: params.onMessage });
}

/**
 * Keep an event stream open: reconnect with backoff when it drops, until
 * the returned function is called. `shouldReconnect` (default: always)
 * decides whether a stream the server ended should be reopened. 401, 403
 * and 404 are final.
 */
export function subscribeSse(params: {
  url: string;
  /** Read per connection so a key entered later is picked up. */
  headers?: () => HeadersInit;
  onMessage: (message: SseMessage) => void;
  shouldReconnect?: () => boolean;
  initialRetryDelayMs?: number;
  maxRetryDelayMs?: number;
  fetchImpl?: typeof fetch;
}): () => void {
  const ctrl = new AbortController();
  const initialDelay = params.initialRetryDelayMs ?? 1_000;
  const maxDelay = params.maxRetryDelayMs ?? 30_000;
  let delay = initialDelay;
  let retryTimer: ReturnType<typeof setTimeout> | undefined;

  const connect = async (): Promise<void> => {
    if (ctrl.signal.aborted) return;
    try {
      await readSse({
        url: params.url,
        ...(params.headers && { headers: params.headers() }),
        signal: ctrl.signal,
        ...(params.fetchImpl && { fetchImpl: params.fetchImpl }),
        onMessage: (message) => {
          delay = initialDelay;
          params.onMessage(message);
        },
      });
      if (params.shouldReconnect && !params.shouldReconnect()) return;
    } catch (err) {
      if (ctrl.signal.aborted) return;
      if (err instanceof SseHttpError && [401, 403, 404].includes(err.status)) return;
    }
    if (ctrl.signal.aborted) return;
    retryTimer = setTimeout(() => void connect(), delay);
    delay = Math.min(maxDelay, delay * 2);
  };

  void connect();
  return () => {
    ctrl.abort();
    if (retryTimer !== undefined) clearTimeout(retryTimer);
  };
}
