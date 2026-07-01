/**
 * Omnigent (Omni) runtime — HTTP client for the Omnigent server Sessions API.
 *
 * T3 Code talks to an Omnigent server over REST + SSE. Users run
 * `omni server start` (and typically `omni host`) locally, or point
 * `serverUrl` at a deployed host such as Databricks Apps.
 *
 * @module omniRuntime
 */
import type { OmniSettings } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as P from "effect/Predicate";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientError, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

const DEFAULT_OMNI_SERVER_URL = "http://127.0.0.1:6767";
const OMNI_SERVER_HEALTH_PATH = "/health";
export const OMNI_VERSION_PROBE_TIMEOUT_MS = 4_000;

const OMNI_RUNTIME_ERROR_TAG = "OmniRuntimeError";
export class OmniRuntimeError extends Data.TaggedError(OMNI_RUNTIME_ERROR_TAG)<{
  readonly operation: string;
  readonly cause?: unknown;
  readonly detail: string;
}> {
  static readonly is = (u: unknown): u is OmniRuntimeError =>
    P.isTagged(u, OMNI_RUNTIME_ERROR_TAG);
}

export function omniRuntimeErrorDetail(cause: unknown): string {
  if (OmniRuntimeError.is(cause)) return cause.detail;
  if (cause instanceof Error && cause.message.trim().length > 0) return cause.message.trim();
  return String(cause);
}

export interface OmniAgentSummary {
  readonly id: string;
  readonly name: string;
  readonly description: string | undefined;
}

export interface OmniSessionSnapshot {
  readonly id: string;
  readonly agentId: string;
  readonly status: string;
  readonly runnerId: string | null;
}

export interface OmniServerConnection {
  readonly baseUrl: string;
  readonly authToken: string | undefined;
}

export interface OmniRuntimeShape {
  readonly resolveServerUrl: (settings: OmniSettings) => string;
  readonly connectToOmniServer: (
    settings: OmniSettings,
  ) => Effect.Effect<OmniServerConnection, OmniRuntimeError>;
  readonly probeOmniServerHealth: (
    connection: OmniServerConnection,
  ) => Effect.Effect<void, OmniRuntimeError, HttpClient.HttpClient>;
  readonly listOmniAgents: (
    connection: OmniServerConnection,
  ) => Effect.Effect<ReadonlyArray<OmniAgentSummary>, OmniRuntimeError, HttpClient.HttpClient>;
  readonly createOmniSession: (input: {
    readonly connection: OmniServerConnection;
    readonly agentId: string;
    readonly hostId: string | undefined;
    readonly workspace: string | undefined;
    readonly title: string | undefined;
  }) => Effect.Effect<OmniSessionSnapshot, OmniRuntimeError, HttpClient.HttpClient>;
  readonly postOmniSessionEvent: (input: {
    readonly connection: OmniServerConnection;
    readonly sessionId: string;
    readonly type: string;
    readonly data: Record<string, unknown>;
  }) => Effect.Effect<void, OmniRuntimeError, HttpClient.HttpClient>;
  readonly resolveOmniElicitation: (input: {
    readonly connection: OmniServerConnection;
    readonly sessionId: string;
    readonly elicitationId: string;
    readonly action: "accept" | "decline" | "cancel";
  }) => Effect.Effect<void, OmniRuntimeError, HttpClient.HttpClient>;
  readonly streamOmniSessionEvents: (input: {
    readonly connection: OmniServerConnection;
    readonly sessionId: string;
    readonly signal: AbortSignal;
  }) => Effect.Effect<
    Stream.Stream<{ readonly event: string; readonly data: unknown }, OmniRuntimeError>,
    OmniRuntimeError,
    HttpClient.HttpClient
  >;
}

export class OmniRuntime extends Context.Service<OmniRuntime, OmniRuntimeShape>()(
  "@t3tools/server/provider/omniRuntime/OmniRuntime",
) {}

function trimTrailingSlash(url: string): string {
  return url.endsWith("/") ? url.slice(0, -1) : url;
}

function resolveConfiguredServerUrl(settings: OmniSettings): string {
  const configured = settings.serverUrl.trim();
  return trimTrailingSlash(configured.length > 0 ? configured : DEFAULT_OMNI_SERVER_URL);
}

function withAuth(
  request: HttpClientRequest.HttpClientRequest,
  authToken: string | undefined,
): HttpClientRequest.HttpClientRequest {
  const token = authToken?.trim();
  return token && token.length > 0
    ? request.pipe(HttpClientRequest.bearerToken(token))
    : request;
}

function decodeJsonBody<A>(
  operation: string,
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<A, OmniRuntimeError> {
  return HttpClientResponse.schemaBodyJson(Schema.Unknown)(response).pipe(
    Effect.mapError(
      (cause) =>
        new OmniRuntimeError({
          operation,
          detail: omniRuntimeErrorDetail(cause),
          cause,
        }),
    ),
    Effect.map((body) => body as A),
  );
}

function ensureOkStatus(
  operation: string,
  response: HttpClientResponse.HttpClientResponse,
): Effect.Effect<HttpClientResponse.HttpClientResponse, OmniRuntimeError> {
  if (response.status >= 200 && response.status < 300) {
    return Effect.succeed(response);
  }
  return Effect.fail(
    new OmniRuntimeError({
      operation,
      detail: `HTTP ${response.status}`,
    }),
  );
}

function parseAgentList(body: unknown): ReadonlyArray<OmniAgentSummary> {
  if (!body || typeof body !== "object" || !("data" in body)) {
    return [];
  }
  const data = (body as { data?: unknown }).data;
  if (!Array.isArray(data)) {
    return [];
  }
  return data.flatMap((entry): ReadonlyArray<OmniAgentSummary> => {
    if (!entry || typeof entry !== "object") {
      return [];
    }
    const record = entry as Record<string, unknown>;
    const id = typeof record.id === "string" ? record.id : undefined;
    const name = typeof record.name === "string" ? record.name : undefined;
    if (!id || !name) {
      return [];
    }
    const description =
      typeof record.description === "string" && record.description.trim().length > 0
        ? record.description.trim()
        : undefined;
    return [{ id, name, description }];
  });
}

function parseSessionSnapshot(body: unknown): OmniSessionSnapshot | undefined {
  if (!body || typeof body !== "object") {
    return undefined;
  }
  const record = body as Record<string, unknown>;
  const id =
    typeof record.id === "string"
      ? record.id
      : typeof record.session_id === "string"
        ? record.session_id
        : undefined;
  const agentId = typeof record.agent_id === "string" ? record.agent_id : undefined;
  const status = typeof record.status === "string" ? record.status : "idle";
  if (!id || !agentId) {
    return undefined;
  }
  const runnerId = typeof record.runner_id === "string" ? record.runner_id : null;
  return { id, agentId, status, runnerId };
}

type OmniSseParseState = {
  readonly buffer: string;
  readonly currentEvent: string | undefined;
};

function decodeOmniSseChunk(
  state: OmniSseParseState,
  text: string,
): [OmniSseParseState, ReadonlyArray<{ readonly event: string; readonly data: unknown }>] {
  let buffer = state.buffer + text;
  const frames: Array<{ readonly event: string; readonly data: unknown }> = [];
  let currentEvent = state.currentEvent;

  while (true) {
    const newlineIndex = buffer.indexOf("\n");
    if (newlineIndex < 0) {
      break;
    }
    const line = buffer.slice(0, newlineIndex).replace(/\r$/, "");
    buffer = buffer.slice(newlineIndex + 1);

    if (line.startsWith("event: ")) {
      currentEvent = line.slice("event: ".length);
      continue;
    }
    if (!line.startsWith("data: ")) {
      continue;
    }
    const dataText = line.slice("data: ".length);
    if (dataText.trim() === "[DONE]") {
      return [{ buffer, currentEvent: undefined }, frames];
    }
    // Per the SSE spec a `data:` line with no preceding `event:` field
    // defaults to the "message" event type; emit it rather than dropping it.
    const eventName = currentEvent ?? "message";
    try {
      frames.push({ event: eventName, data: JSON.parse(dataText) as unknown });
    } catch {
      // Ignore malformed SSE frames.
    }
    currentEvent = undefined;
  }

  return [{ buffer, currentEvent }, frames];
}

export function omniSseFramesFromByteStream(
  byteStream: Stream.Stream<Uint8Array, HttpClientError.HttpClientError>,
): Stream.Stream<{ readonly event: string; readonly data: unknown }, HttpClientError.HttpClientError> {
  // A single streaming decoder so a multibyte UTF-8 codepoint split across two
  // network chunks is reassembled correctly rather than mis-decoded.
  const decoder = new TextDecoder();
  // `mapAccum` flattens the per-chunk `ReadonlyArray` of frames into individual
  // stream elements, so no extra flattening step is needed.
  return byteStream.pipe(
    Stream.mapAccum(
      (): OmniSseParseState => ({ buffer: "", currentEvent: undefined }),
      (state, chunk) => decodeOmniSseChunk(state, decoder.decode(chunk, { stream: true })),
    ),
  );
}

const make = Effect.gen(function* () {
  const resolveServerUrl = (settings: OmniSettings): string => resolveConfiguredServerUrl(settings);

  const connectToOmniServer = (settings: OmniSettings) =>
    Effect.succeed({
      baseUrl: resolveConfiguredServerUrl(settings),
      authToken: settings.authToken.trim() || undefined,
    } satisfies OmniServerConnection);

  const probeOmniServerHealth: OmniRuntimeShape["probeOmniServerHealth"] = Effect.fn(
    "probeOmniServerHealth",
  )(function* (connection) {
    const httpClient = yield* HttpClient.HttpClient;
    const request = withAuth(
      HttpClientRequest.get(`${connection.baseUrl}${OMNI_SERVER_HEALTH_PATH}`).pipe(
        HttpClientRequest.acceptJson,
      ),
      connection.authToken,
    );
    const response = yield* httpClient.execute(request).pipe(
      Effect.mapError(
        (cause) =>
          new OmniRuntimeError({
            operation: "probeOmniServerHealth",
            detail: omniRuntimeErrorDetail(cause),
            cause,
          }),
      ),
    );
    yield* ensureOkStatus("probeOmniServerHealth", response);
  });

  const listOmniAgents: OmniRuntimeShape["listOmniAgents"] = Effect.fn("listOmniAgents")(
    function* (connection) {
      const httpClient = yield* HttpClient.HttpClient;
      const request = withAuth(
        HttpClientRequest.get(`${connection.baseUrl}/api/agents?limit=100`).pipe(
          HttpClientRequest.acceptJson,
        ),
        connection.authToken,
      );
      const response = yield* httpClient.execute(request).pipe(
        Effect.mapError(
          (cause) =>
            new OmniRuntimeError({
              operation: "listOmniAgents",
              detail: omniRuntimeErrorDetail(cause),
              cause,
            }),
        ),
      );
      yield* ensureOkStatus("listOmniAgents", response);
      const body = yield* decodeJsonBody<unknown>("listOmniAgents", response);
      return parseAgentList(body);
    },
  );

  const createOmniSession: OmniRuntimeShape["createOmniSession"] = Effect.fn("createOmniSession")(
    function* (input) {
      const httpClient = yield* HttpClient.HttpClient;
      const payload: Record<string, unknown> = {
        agent_id: input.agentId,
      };
      if (input.title && input.title.trim().length > 0) {
        payload.title = input.title.trim();
      }
      const hostId = input.hostId?.trim();
      const workspace = input.workspace?.trim();
      if (hostId && hostId.length > 0) {
        payload.host_id = hostId;
        if (workspace && workspace.length > 0) {
          payload.workspace = workspace;
        }
      }
      const request = withAuth(
        HttpClientRequest.post(`${input.connection.baseUrl}/v1/sessions`).pipe(
          HttpClientRequest.acceptJson,
          HttpClientRequest.bodyJsonUnsafe(payload),
        ),
        input.connection.authToken,
      );
      const response = yield* httpClient.execute(request).pipe(
        Effect.mapError(
          (cause) =>
            new OmniRuntimeError({
              operation: "createOmniSession",
              detail: omniRuntimeErrorDetail(cause),
              cause,
            }),
        ),
      );
      yield* ensureOkStatus("createOmniSession", response);
      const body = yield* decodeJsonBody<unknown>("createOmniSession", response);
      const snapshot = parseSessionSnapshot(body);
      if (!snapshot) {
        return yield* Effect.fail(
          new OmniRuntimeError({
            operation: "createOmniSession",
            detail: "Omnigent server returned an invalid session payload.",
          }),
        );
      }
      return snapshot;
    },
  );

  const postOmniSessionEvent: OmniRuntimeShape["postOmniSessionEvent"] = Effect.fn(
    "postOmniSessionEvent",
  )(function* (input) {
    const httpClient = yield* HttpClient.HttpClient;
    const request = withAuth(
      HttpClientRequest.post(
        `${input.connection.baseUrl}/v1/sessions/${encodeURIComponent(input.sessionId)}/events`,
      ).pipe(
        HttpClientRequest.acceptJson,
        HttpClientRequest.bodyJsonUnsafe({
          type: input.type,
          data: input.data,
        }),
      ),
      input.connection.authToken,
    );
    const response = yield* httpClient.execute(request).pipe(
      Effect.mapError(
        (cause) =>
          new OmniRuntimeError({
            operation: "postOmniSessionEvent",
            detail: omniRuntimeErrorDetail(cause),
            cause,
          }),
      ),
    );
    yield* ensureOkStatus("postOmniSessionEvent", response);
  });

  const resolveOmniElicitation: OmniRuntimeShape["resolveOmniElicitation"] = Effect.fn(
    "resolveOmniElicitation",
  )(function* (input) {
    const httpClient = yield* HttpClient.HttpClient;
    const request = withAuth(
      HttpClientRequest.post(
        `${input.connection.baseUrl}/v1/sessions/${encodeURIComponent(input.sessionId)}/elicitations/${encodeURIComponent(input.elicitationId)}/resolve`,
      ).pipe(
        HttpClientRequest.acceptJson,
        HttpClientRequest.bodyJsonUnsafe({ action: input.action }),
      ),
      input.connection.authToken,
    );
    const response = yield* httpClient.execute(request).pipe(
      Effect.mapError(
        (cause) =>
          new OmniRuntimeError({
            operation: "resolveOmniElicitation",
            detail: omniRuntimeErrorDetail(cause),
            cause,
          }),
      ),
    );
    yield* ensureOkStatus("resolveOmniElicitation", response);
  });

  const streamOmniSessionEvents: OmniRuntimeShape["streamOmniSessionEvents"] = Effect.fn(
    "streamOmniSessionEvents",
  )(function* (input) {
    const httpClient = yield* HttpClient.HttpClient;
    const request = withAuth(
      HttpClientRequest.get(
        `${input.connection.baseUrl}/v1/sessions/${encodeURIComponent(input.sessionId)}/stream`,
      ).pipe(HttpClientRequest.setHeader("accept", "text/event-stream")),
      input.connection.authToken,
    );
    const response = yield* httpClient.execute(request).pipe(
      Effect.mapError(
        (cause) =>
          new OmniRuntimeError({
            operation: "streamOmniSessionEvents",
            detail: omniRuntimeErrorDetail(cause),
            cause,
          }),
      ),
    );
    if (response.status < 200 || response.status >= 300) {
      return yield* Effect.fail(
        new OmniRuntimeError({
          operation: "streamOmniSessionEvents",
          detail: `HTTP ${response.status}`,
        }),
      );
    }
    return omniSseFramesFromByteStream(response.stream).pipe(
      Stream.mapError(
        (cause) =>
          new OmniRuntimeError({
            operation: "streamOmniSessionEvents",
            detail: omniRuntimeErrorDetail(cause),
            cause,
          }),
      ),
    );
  });

  return OmniRuntime.of({
    resolveServerUrl,
    connectToOmniServer,
    probeOmniServerHealth,
    listOmniAgents,
    createOmniSession,
    postOmniSessionEvent,
    resolveOmniElicitation,
    streamOmniSessionEvents,
  });
});

export const OmniRuntimeLive = Layer.effect(OmniRuntime, make);

export const runOmniSdk = <A>(
  operation: string,
  effect: Effect.Effect<A, OmniRuntimeError, HttpClient.HttpClient>,
): Effect.Effect<A, OmniRuntimeError, HttpClient.HttpClient> =>
  effect.pipe(Effect.withSpan(`omni.${operation}`));

export function resolveOmniDefaultAgentId(
  settings: OmniSettings,
  agents: ReadonlyArray<OmniAgentSummary>,
): string | undefined {
  const configured = settings.defaultAgentId.trim();
  if (configured.length > 0) {
    return configured;
  }
  return agents[0]?.id;
}

export function omniModelSlugFromAgent(agent: OmniAgentSummary): string {
  return agent.id;
}

export function omniModelNameFromAgent(agent: OmniAgentSummary): string {
  return agent.description ? `${agent.name} — ${agent.description}` : agent.name;
}

export function formatOmniServerProbeError(input: {
  readonly cause: unknown;
  readonly serverUrl: string;
}): string {
  const detail = omniRuntimeErrorDetail(input.cause).toLowerCase();
  if (
    detail.includes("401") ||
    detail.includes("403") ||
    detail.includes("unauthorized") ||
    detail.includes("forbidden")
  ) {
    return "Omnigent server rejected authentication. Check the auth token and server URL.";
  }
  if (
    detail.includes("econnrefused") ||
    detail.includes("enotfound") ||
    detail.includes("fetch failed") ||
    detail.includes("networkerror") ||
    detail.includes("timed out") ||
    detail.includes("timeout") ||
    detail.includes("socket hang up")
  ) {
    return `Couldn't reach the Omnigent server at ${input.serverUrl}. Run \`omni server start\` locally or check the configured URL.`;
  }
  return omniRuntimeErrorDetail(input.cause);
}

export function parseOmniCliVersion(output: string): string | null {
  const trimmed = output.trim();
  if (trimmed.length === 0) {
    return null;
  }
  const match = trimmed.match(/(\d+\.\d+\.\d+(?:[-+][\w.-]+)?)/);
  return match?.[1] ?? trimmed.split(/\s+/)[0] ?? null;
}

export function omniServerConnectionFromSettings(settings: OmniSettings): OmniServerConnection {
  return {
    baseUrl: resolveConfiguredServerUrl(settings),
    authToken: settings.authToken.trim() || undefined,
  };
}

export function omniHostConfigured(settings: OmniSettings): boolean {
  return settings.hostId.trim().length > 0;
}

export function omniServerUrlLabel(settings: OmniSettings): string {
  const url = resolveConfiguredServerUrl(settings);
  return url;
}

export function omniOptionalString(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed && trimmed.length > 0 ? trimmed : undefined;
}

export function omniAgentById(
  agents: ReadonlyArray<OmniAgentSummary>,
  agentId: string,
): OmniAgentSummary | undefined {
  return agents.find((agent) => agent.id === agentId);
}

export function omniAgentsOrEmpty(
  agents: ReadonlyArray<OmniAgentSummary> | undefined,
): ReadonlyArray<OmniAgentSummary> {
  return agents ?? [];
}

export function omniConnectionOption(
  connection: OmniServerConnection,
): Option.Option<OmniServerConnection> {
  return Option.some(connection);
}
