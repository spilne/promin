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
  /** `id:` field, when the frame has one. */
  id?: string;
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
  let id: string | undefined;
  const dataLines: string[] = [];
  for (const line of frame.split(/\r?\n/)) {
    if (line.startsWith(":")) continue; // comment / heartbeat
    if (line.startsWith("event:")) event = line.slice(6).trim();
    else if (line.startsWith("data:")) dataLines.push(line.slice(5).replace(/^ /, ""));
    else if (line.startsWith("id:")) id = line.slice(3).trim();
  }
  if (dataLines.length === 0) return null;
  return { event, data: dataLines.join("\n"), ...(id !== undefined && { id }) };
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

/**
 * Read an event stream that can be resumed: open it with `start`, and when
 * the connection drops before a terminal message, reconnect to
 * `resumeUrl` with `Last-Event-ID` set to the last id received, so the
 * server sends only what came after (nothing is delivered twice). Gives
 * up after `maxReconnects` failed attempts in a row, or when the server
 * no longer has the stream (404). Throws `SseHttpError` when `start`
 * answers non-2xx.
 *
 * Resolves `ended` after a terminal message (or a clean end without one
 * and nothing to resume from), `lost` when it gave up. A first request
 * that fails before any event arrived rejects with its error.
 */
export async function readResumableSse(params: {
  start: { url: string; init: RequestInit };
  resumeUrl: string;
  /** Read per connection so a key entered later is picked up. */
  headers?: () => HeadersInit;
  signal: AbortSignal;
  isTerminal: (message: SseMessage) => boolean;
  onMessage: (message: SseMessage) => void;
  maxReconnects?: number;
  initialRetryDelayMs?: number;
  fetchImpl?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
}): Promise<"ended" | "lost"> {
  const doFetch = params.fetchImpl ?? fetch;
  const sleep = params.sleep ?? ((ms: number) => new Promise<void>((r) => setTimeout(r, ms)));
  const maxReconnects = params.maxReconnects ?? 5;
  let delay = params.initialRetryDelayMs ?? 500;
  let lastEventId: string | undefined;
  let terminal = false;
  let failures = 0;
  const onMessage = (message: SseMessage): void => {
    if (message.id !== undefined) lastEventId = message.id;
    failures = 0;
    if (params.isTerminal(message)) terminal = true;
    params.onMessage(message);
  };

  for (let attempt = 0; ; attempt++) {
    try {
      const headers = new Headers(params.headers?.());
      headers.set("accept", "text/event-stream");
      let res: Response;
      if (attempt === 0) {
        new Headers(params.start.init.headers).forEach((v, k) => headers.set(k, v));
        res = await doFetch(params.start.url, {
          ...params.start.init,
          headers,
          signal: params.signal,
        });
        if (!res.ok || !res.body) throw new SseHttpError(res.status);
      } else {
        headers.set("last-event-id", lastEventId!);
        res = await doFetch(params.resumeUrl, { headers, signal: params.signal });
        // The server no longer has the stream: nothing left to resume.
        if (res.status === 404) return "lost";
        if (!res.ok || !res.body) throw new SseHttpError(res.status);
      }
      await readSseBody({ body: res.body, onMessage });
      if (terminal) return "ended";
    } catch (err) {
      if (params.signal.aborted) return "ended";
      // Nothing received to resume from: the caller sees the failure.
      if (attempt === 0 && (err instanceof SseHttpError || lastEventId === undefined)) throw err;
      if (err instanceof SseHttpError && [401, 403].includes(err.status)) return "lost";
    }
    // Dropped before the end. Without an event id there is nothing to
    // resume from.
    if (lastEventId === undefined) return attempt === 0 ? "ended" : "lost";
    if (++failures > maxReconnects) return "lost";
    await sleep(delay);
    delay = Math.min(10_000, delay * 2);
    if (params.signal.aborted) return "ended";
  }
}
