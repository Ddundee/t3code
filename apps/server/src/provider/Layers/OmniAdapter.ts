import {
  ApprovalRequestId,
  type OmniSettings,
  ProviderDriverKind,
  type ProviderApprovalDecision,
  type ProviderRuntimeEvent,
  type ProviderSession,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/unstable/http";

import { ServerConfig } from "../../config.ts";
import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionClosedError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import {
  makeOmniAssistantItemEvent,
  makeOmniContentDeltaEvent,
  makeOmniRequestOpenedEvent,
  makeOmniRequestResolvedEvent,
  makeOmniRuntimeErrorEvent,
  makeOmniSessionExitedEvent,
  makeOmniSessionStartedEvent,
  makeOmniThreadStartedEvent,
  makeOmniToolCallEvent,
  makeOmniTurnCompletedEvent,
  makeOmniTurnStartedEvent,
} from "../omni/OmniCoreRuntimeEvents.ts";
import {
  omniRuntimeErrorDetail,
  OmniRuntime,
  omniServerConnectionFromSettings,
} from "../omniRuntime.ts";
import type { OmniAdapterShape } from "../Services/OmniAdapter.ts";

const PROVIDER = ProviderDriverKind.make("omni");

interface PendingApproval {
  readonly elicitationId: string;
  readonly requestId: RuntimeRequestId;
}

interface OmniSessionContext {
  readonly threadId: ThreadId;
  readonly omniSessionId: string;
  readonly agentId: string;
  session: ProviderSession;
  readonly sessionScope: Scope.Closeable;
  readonly abortController: AbortController;
  activeTurnId: TurnId | undefined;
  assistantTextByItemId: Map<string, string>;
  readonly pendingApprovals: Map<ApprovalRequestId, PendingApproval>;
  readonly turns: Array<{ id: TurnId; items: Array<unknown> }>;
  readonly stopped: Ref.Ref<boolean>;
}

export interface OmniAdapterLiveOptions {
  readonly environment?: NodeJS.ProcessEnv;
  readonly instanceId?: string;
}

function readEventType(data: unknown): string | undefined {
  if (!data || typeof data !== "object" || !("type" in data)) {
    return undefined;
  }
  const type = (data as { type?: unknown }).type;
  return typeof type === "string" ? type : undefined;
}

function readEventPayload(data: unknown): Record<string, unknown> {
  if (!data || typeof data !== "object") {
    return {};
  }
  if (
    "data" in data &&
    (data as { data?: unknown }).data &&
    typeof (data as { data: unknown }).data === "object"
  ) {
    return (data as { data: Record<string, unknown> }).data;
  }
  return data as Record<string, unknown>;
}

function resolveAgentId(settings: OmniSettings, modelSlug: string | undefined): string {
  const configured = settings.defaultAgentId.trim();
  if (modelSlug && modelSlug.trim().length > 0) {
    return modelSlug.trim();
  }
  if (configured.length > 0) {
    return configured;
  }
  return "";
}

export function makeOmniAdapter(omniSettings: OmniSettings, _options?: OmniAdapterLiveOptions) {
  return Effect.gen(function* () {
    const omniRuntime = yield* OmniRuntime;
    const httpClient = yield* HttpClient.HttpClient;
    const serverConfig = yield* ServerConfig;
    const crypto = yield* Crypto.Crypto;
    const eventPubSub = yield* PubSub.unbounded<ProviderRuntimeEvent>();
    const sessionsRef = yield* Ref.make(new Map<ThreadId, OmniSessionContext>());
    const connection = omniServerConnectionFromSettings(omniSettings);

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
    const randomUUIDv4 = crypto.randomUUIDv4.pipe(
      Effect.mapError(
        (cause) =>
          new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "crypto/randomUUIDv4",
            detail: "Failed to generate Omni runtime identifier.",
            cause,
          }),
      ),
    );
    const makeEventStamp = () =>
      Effect.all({
        eventId: Effect.map(randomUUIDv4, (id) => id as ProviderRuntimeEvent["eventId"]),
        createdAt: nowIso,
      });

    const withHttp = <A, E>(effect: Effect.Effect<A, E, HttpClient.HttpClient>) =>
      effect.pipe(Effect.provideService(HttpClient.HttpClient, httpClient));

    const publish = (event: ProviderRuntimeEvent) =>
      PubSub.publish(eventPubSub, event).pipe(Effect.asVoid);

    // Accumulate completed items onto the active turn so `readThread` can
    // return a non-empty history snapshot (the Omni server has no thread-read
    // endpoint, so history is reconstructed from the streamed events).
    const appendTurnItem = (context: OmniSessionContext, item: unknown): void => {
      const turnId = context.activeTurnId;
      if (!turnId) return;
      const turn = context.turns.find((entry) => entry.id === turnId);
      if (turn) turn.items.push(item);
    };

    const getContext = (threadId: ThreadId) =>
      Effect.gen(function* () {
        const sessions = yield* Ref.get(sessionsRef);
        const context = sessions.get(threadId);
        if (!context) {
          return yield* new ProviderAdapterSessionNotFoundError({ provider: PROVIDER, threadId });
        }
        if (yield* Ref.get(context.stopped)) {
          return yield* new ProviderAdapterSessionClosedError({ provider: PROVIDER, threadId });
        }
        return context;
      });

    const stopContext = Effect.fn("stopOmniContext")(function* (context: OmniSessionContext) {
      if (yield* Ref.getAndSet(context.stopped, true)) {
        return;
      }
      context.abortController.abort();
      yield* Scope.close(context.sessionScope, Exit.void);
    });

    const handleStreamEvent = Effect.fn("handleOmniStreamEvent")(function* (
      context: OmniSessionContext,
      wireEvent: string,
      data: unknown,
    ) {
      const stamp = yield* makeEventStamp();
      const eventType = readEventType(data) ?? wireEvent;
      const payload = readEventPayload(data);

      switch (eventType) {
        case "response.output_text.delta": {
          const delta = typeof payload.delta === "string" ? payload.delta : "";
          if (delta.length === 0) return;
          const itemId =
            typeof payload.item_id === "string"
              ? payload.item_id
              : typeof payload.message_id === "string"
                ? payload.message_id
                : undefined;
          yield* publish(
            makeOmniContentDeltaEvent({
              stamp,
              provider: PROVIDER,
              threadId: context.threadId,
              turnId: context.activeTurnId,
              itemId,
              delta,
              streamKind: "assistant_text",
              raw: data,
            }),
          );
          if (itemId) {
            const previous = context.assistantTextByItemId.get(itemId) ?? "";
            context.assistantTextByItemId.set(itemId, previous + delta);
          }
          return;
        }
        case "response.reasoning_text.delta": {
          const delta = typeof payload.delta === "string" ? payload.delta : "";
          if (delta.length === 0) return;
          yield* publish(
            makeOmniContentDeltaEvent({
              stamp,
              provider: PROVIDER,
              threadId: context.threadId,
              turnId: context.activeTurnId,
              itemId: undefined,
              delta,
              streamKind: "reasoning_text",
              raw: data,
            }),
          );
          return;
        }
        case "response.output_item.done": {
          const item = payload.item;
          if (item && typeof item === "object") {
            const record = item as Record<string, unknown>;
            const itemId = typeof record.id === "string" ? record.id : undefined;
            const content = record.content;
            if (Array.isArray(content)) {
              const textParts = content.flatMap((part) => {
                if (!part || typeof part !== "object") return [];
                const partRecord = part as Record<string, unknown>;
                if (partRecord.type === "output_text" && typeof partRecord.text === "string") {
                  return [partRecord.text];
                }
                return [];
              });
              const text = textParts.join("");
              if (text.length > 0) {
                yield* publish(
                  makeOmniAssistantItemEvent({
                    stamp,
                    provider: PROVIDER,
                    threadId: context.threadId,
                    turnId: context.activeTurnId,
                    itemId,
                    detail: text,
                    raw: data,
                  }),
                );
                appendTurnItem(context, { type: "assistant_message", itemId, text });
              }
            }
            if (record.type === "function_call" && typeof record.name === "string") {
              const args = typeof record.arguments === "string" ? record.arguments : undefined;
              yield* publish(
                makeOmniToolCallEvent({
                  stamp,
                  provider: PROVIDER,
                  threadId: context.threadId,
                  turnId: context.activeTurnId,
                  itemId,
                  toolName: record.name,
                  status: "inProgress",
                  detail: args,
                  raw: data,
                }),
              );
              appendTurnItem(context, {
                type: "tool_call",
                itemId,
                toolName: record.name,
                ...(args ? { arguments: args } : {}),
              });
            }
          }
          return;
        }
        case "response.elicitation_request": {
          const params = payload.params;
          const elicitationId =
            params &&
            typeof params === "object" &&
            typeof (params as { id?: unknown }).id === "string"
              ? (params as { id: string }).id
              : undefined;
          if (!elicitationId) return;
          const requestId = RuntimeRequestId.make(yield* randomUUIDv4);
          const approvalRequestId = ApprovalRequestId.make(requestId);
          context.pendingApprovals.set(approvalRequestId, { elicitationId, requestId });
          yield* publish(
            makeOmniRequestOpenedEvent({
              stamp,
              provider: PROVIDER,
              threadId: context.threadId,
              turnId: context.activeTurnId,
              requestId,
              detail: "Omnigent is waiting for approval.",
              raw: data,
            }),
          );
          return;
        }
        case "response.completed":
        case "response.incomplete":
        case "response.cancelled": {
          yield* publish(
            makeOmniTurnCompletedEvent({
              stamp,
              provider: PROVIDER,
              threadId: context.threadId,
              turnId: context.activeTurnId,
              raw: data,
            }),
          );
          context.activeTurnId = undefined;
          return;
        }
        case "response.failed": {
          const message =
            typeof payload.message === "string"
              ? payload.message
              : typeof payload.error === "string"
                ? payload.error
                : "Omnigent turn failed.";
          yield* publish(
            makeOmniTurnCompletedEvent({
              stamp,
              provider: PROVIDER,
              threadId: context.threadId,
              turnId: context.activeTurnId,
              state: "failed",
              errorMessage: message,
              raw: data,
            }),
          );
          context.activeTurnId = undefined;
          return;
        }
        case "session.status": {
          const status = typeof payload.status === "string" ? payload.status : undefined;
          if (status === "idle" && context.activeTurnId) {
            yield* publish(
              makeOmniTurnCompletedEvent({
                stamp,
                provider: PROVIDER,
                threadId: context.threadId,
                turnId: context.activeTurnId,
                raw: data,
              }),
            );
            context.activeTurnId = undefined;
          }
          return;
        }
        default:
          return;
      }
    });

    const startStreamPump = Effect.fn("startOmniStreamPump")(function* (context: OmniSessionContext) {
      yield* Scope.addFinalizer(
        context.sessionScope,
        Effect.sync(() => context.abortController.abort()),
      );

      yield* withHttp(
        omniRuntime.streamOmniSessionEvents({
          connection,
          sessionId: context.omniSessionId,
          signal: context.abortController.signal,
        }),
      ).pipe(
        Effect.flatMap((frames) =>
          frames.pipe(
            Stream.runForEach((frame) => handleStreamEvent(context, frame.event, frame.data)),
          ),
        ),
        Effect.exit,
        Effect.flatMap((exit) =>
          Effect.gen(function* () {
            if (context.abortController.signal.aborted || (yield* Ref.get(context.stopped))) {
              return;
            }
            if (Exit.isFailure(exit)) {
              const detail = omniRuntimeErrorDetail(Cause.squash(exit.cause));
              yield* publish(
                makeOmniRuntimeErrorEvent({
                  stamp: yield* makeEventStamp(),
                  provider: PROVIDER,
                  threadId: context.threadId,
                  turnId: context.activeTurnId,
                  message: detail,
                }),
              );
              yield* publish(
                makeOmniSessionExitedEvent({
                  stamp: yield* makeEventStamp(),
                  provider: PROVIDER,
                  threadId: context.threadId,
                  reason: `Omnigent session stream ended unexpectedly: ${detail}`,
                  recoverable: true,
                  exitKind: "error",
                }),
              );
            }
          }),
        ),
        Effect.forkIn(context.sessionScope),
      );
    });

    const startSession: OmniAdapterShape["startSession"] = Effect.fn("startSession")(function* (
      input,
    ) {
      const agentId = resolveAgentId(omniSettings, input.modelSelection?.model);
      if (agentId.length === 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "startSession",
          issue:
            "No Omnigent agent is configured. Set a default agent ID in Omni settings or pick an agent model.",
        });
      }

      const hostId = omniSettings.hostId.trim() || undefined;
      const workspace = input.cwd ?? serverConfig.cwd;

      const snapshot = yield* withHttp(
        omniRuntime.createOmniSession({
          connection,
          agentId,
          hostId,
          workspace,
          title: input.threadId,
        }),
      ).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "startSession",
              detail: omniRuntimeErrorDetail(cause),
              cause,
            }),
        ),
      );

      // Scope + abort controller are created only after the session exists, so
      // a failed createOmniSession cannot leak an unclosed scope.
      const sessionScope = yield* Scope.make();
      const abortController = new AbortController();

      const createdAt = DateTime.formatIso(yield* DateTime.now);
      const session: ProviderSession = {
        provider: PROVIDER,
        status: "ready",
        runtimeMode: input.runtimeMode,
        cwd: workspace,
        ...(input.modelSelection ? { model: input.modelSelection.model } : {}),
        threadId: input.threadId,
        createdAt,
        updatedAt: createdAt,
      };

      const context: OmniSessionContext = {
        threadId: input.threadId,
        omniSessionId: snapshot.id,
        agentId,
        session,
        sessionScope,
        abortController,
        activeTurnId: undefined,
        assistantTextByItemId: new Map(),
        pendingApprovals: new Map(),
        turns: [],
        stopped: yield* Ref.make(false),
      };

      yield* startStreamPump(context);

      yield* Ref.update(sessionsRef, (sessions) => {
        const next = new Map(sessions);
        next.set(input.threadId, context);
        return next;
      });

      yield* publish(
        makeOmniSessionStartedEvent({
          stamp: yield* makeEventStamp(),
          provider: PROVIDER,
          threadId: input.threadId,
          message: "Omnigent session started",
        }),
      );
      yield* publish(
        makeOmniThreadStartedEvent({
          stamp: yield* makeEventStamp(),
          provider: PROVIDER,
          threadId: input.threadId,
          providerThreadId: snapshot.id,
        }),
      );

      return session;
    });

    const sendTurn: OmniAdapterShape["sendTurn"] = Effect.fn("sendTurn")(function* (input) {
      const context = yield* getContext(input.threadId);

      // Validate before mutating turn state so a rejected turn cannot leave a
      // dangling activeTurnId / phantom turn entry behind.
      if (omniSettings.hostId.trim().length === 0) {
        return yield* new ProviderAdapterValidationError({
          provider: PROVIDER,
          operation: "sendTurn",
          issue:
            "Omni host is not configured. Register a host with `omni host` and set Host ID in Omni settings.",
        });
      }

      const turnId = TurnId.make(yield* randomUUIDv4);
      context.activeTurnId = turnId;
      context.turns.push({ id: turnId, items: [] });

      yield* publish(
        makeOmniTurnStartedEvent({
          stamp: yield* makeEventStamp(),
          provider: PROVIDER,
          threadId: input.threadId,
          turnId,
          model: context.agentId,
        }),
      );

      yield* withHttp(
        omniRuntime.postOmniSessionEvent({
          connection,
          sessionId: context.omniSessionId,
          type: "message",
          data: {
            role: "user",
            content: [{ type: "input_text", text: input.input ?? "" }],
          },
        }),
      ).pipe(
        Effect.mapError(
          (cause) =>
            new ProviderAdapterRequestError({
              provider: PROVIDER,
              method: "sendTurn",
              detail: omniRuntimeErrorDetail(cause),
              cause,
            }),
        ),
      );

      return { threadId: input.threadId, turnId };
    });

    const interruptTurn: OmniAdapterShape["interruptTurn"] = Effect.fn("interruptTurn")(
      function* (threadId) {
        const context = yield* getContext(threadId);
        yield* withHttp(
          omniRuntime.postOmniSessionEvent({
            connection,
            sessionId: context.omniSessionId,
            type: "interrupt",
            data: {},
          }),
        ).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "interruptTurn",
                detail: omniRuntimeErrorDetail(cause),
                cause,
              }),
          ),
        );
      },
    );

    const respondToRequest: OmniAdapterShape["respondToRequest"] = Effect.fn("respondToRequest")(
      function* (threadId, requestId, decision) {
        const context = yield* getContext(threadId);
        const pending = context.pendingApprovals.get(requestId);
        if (!pending) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "respondToRequest",
            detail: "Unknown Omnigent approval request.",
          });
        }
        const action =
          decision === "accept" || decision === "acceptForSession"
            ? "accept"
            : decision === "cancel"
              ? "cancel"
              : "decline";
        yield* withHttp(
          omniRuntime.resolveOmniElicitation({
            connection,
            sessionId: context.omniSessionId,
            elicitationId: pending.elicitationId,
            action,
          }),
        ).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: "respondToRequest",
                detail: omniRuntimeErrorDetail(cause),
                cause,
              }),
          ),
        );
        context.pendingApprovals.delete(requestId);
        const stamp = yield* makeEventStamp();
        yield* publish(
          makeOmniRequestResolvedEvent({
            stamp,
            provider: PROVIDER,
            threadId,
            turnId: context.activeTurnId,
            requestId: pending.requestId,
            decision,
          }),
        );
      },
    );

    const respondToUserInput: OmniAdapterShape["respondToUserInput"] = Effect.fn(
      "respondToUserInput",
    )(function* () {
      return yield* new ProviderAdapterRequestError({
        provider: PROVIDER,
        method: "respondToUserInput",
        detail: "Structured user-input prompts are not supported for Omni yet.",
      });
    });

    const stopSession: OmniAdapterShape["stopSession"] = Effect.fn("stopSession")(function* (
      threadId,
    ) {
      const sessions = yield* Ref.get(sessionsRef);
      const context = sessions.get(threadId);
      if (!context) return;
      yield* withHttp(
        omniRuntime.postOmniSessionEvent({
          connection,
          sessionId: context.omniSessionId,
          type: "stop_session",
          data: {},
        }),
      ).pipe(Effect.catch(() => Effect.void));
      yield* stopContext(context);
      yield* publish(
        makeOmniSessionExitedEvent({
          stamp: yield* makeEventStamp(),
          provider: PROVIDER,
          threadId,
          reason: "Omnigent session stopped.",
          recoverable: false,
          exitKind: "graceful",
        }),
      );
      yield* Ref.update(sessionsRef, (current) => {
        const next = new Map(current);
        next.delete(threadId);
        return next;
      });
    });

    const listSessions: OmniAdapterShape["listSessions"] = Effect.fn("listSessions")(function* () {
      const sessions = yield* Ref.get(sessionsRef);
      return [...sessions.values()]
        .filter((context) => !Ref.getUnsafe(context.stopped))
        .map((context) => context.session);
    });

    const hasSession: OmniAdapterShape["hasSession"] = Effect.fn("hasSession")(function* (
      threadId,
    ) {
      const sessions = yield* Ref.get(sessionsRef);
      const context = sessions.get(threadId);
      return context !== undefined && !Ref.getUnsafe(context.stopped);
    });

    const readThread: OmniAdapterShape["readThread"] = Effect.fn("readThread")(function* (
      threadId,
    ) {
      const context = yield* getContext(threadId);
      return {
        threadId,
        turns: context.turns.map((turn) => ({ id: turn.id, items: [...turn.items] })),
      };
    });

    const rollbackThread: OmniAdapterShape["rollbackThread"] = Effect.fn("rollbackThread")(
      function* () {
        return yield* new ProviderAdapterRequestError({
          provider: PROVIDER,
          method: "rollbackThread",
          detail: "Thread rollback is not supported for Omni yet.",
        });
      },
    );

    const stopAll: OmniAdapterShape["stopAll"] = Effect.fn("stopAll")(function* () {
      const sessions = yield* Ref.get(sessionsRef);
      yield* Effect.forEach([...sessions.values()], (context) => stopContext(context), {
        concurrency: "unbounded",
      });
      yield* Ref.set(sessionsRef, new Map());
    });

    // Tear every live session (forked SSE pumps + AbortControllers) down and
    // shut the event PubSub when the adapter's layer scope closes — otherwise
    // rebuilding the provider instance would leak open SSE connections.
    yield* Effect.addFinalizer(() =>
      Effect.ignore(stopAll()).pipe(Effect.tap(() => PubSub.shutdown(eventPubSub))),
    );

    const streamEvents = Stream.fromPubSub(eventPubSub);

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "unsupported" },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      streamEvents,
    } satisfies OmniAdapterShape;
  });
}
