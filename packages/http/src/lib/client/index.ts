// Errors
export {
  HttpNetworkError,
  HttpTimeoutError,
  HttpStatusError,
  HttpParseError,
  PollTimeoutError,
  type HttpClientError,
  type ResponseParser,
} from "./http-client-error.ts";

// Core request functions (Effect-level)
export {
  httpFetch,
  httpFetchOk,
  httpRequest,
  httpRequestJson,
  httpRequestText,
  FetchTransport,
  type HttpTransport,
  type HttpRequestOptions,
} from "./http-client.ts";

// Streaming (Effect-level)
export {
  httpStreamText,
  httpStreamLines,
  httpStreamSSE,
  httpStreamNDJSON,
  HttpStreamPipeline,
  type SSEvent,
} from "./http-stream.ts";

// Re-export Pipeline from @ts-backend/core for convenience
export { Pipeline } from "@ts-backend/core";

// Chainable pipeline API
export {
  HttpPipeline,
  createHttpPipeline,
  HTTP_RETRYABLE,
  AbstractHttpClient,
  DefaultHttpClient,
  type HttpClient,
  type HttpClientConfig,
  type HttpRequestParams,
  type RequestOptions,
  type RequestBodyOptions,
  type MultipartOptions,
  type HttpMiddleware,
  type HttpRequestContext,
} from "./http-pipeline.ts";

// @effect/platform transport
export { EffectPlatformTransport } from "./effect-platform-transport.ts";
