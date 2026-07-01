import {
  type ProviderApprovalDecision,
  type ProviderDriverKind,
  type ProviderRuntimeEvent,
  RuntimeItemId,
  type RuntimeRequestId,
  type ThreadId,
  type TurnId,
} from "@t3tools/contracts";

type OmniEventStamp = {
  readonly eventId: ProviderRuntimeEvent["eventId"];
  readonly createdAt: string;
};

const omniRaw = (payload: unknown): ProviderRuntimeEvent["raw"] => ({
  source: "omni.sessions.sse",
  payload,
});

export function makeOmniContentDeltaEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly itemId: string | undefined;
  readonly delta: string;
  readonly streamKind: "assistant_text" | "reasoning_text";
  readonly raw: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "content.delta",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    turnId: input.turnId,
    ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
    payload: {
      streamKind: input.streamKind,
      delta: input.delta,
    },
    raw: omniRaw(input.raw),
  };
}

export function makeOmniAssistantItemEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly itemId: string | undefined;
  readonly detail?: string;
  readonly raw: unknown;
}): ProviderRuntimeEvent {
  const detail = input.detail?.trim();
  return {
    type: "item.completed",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    turnId: input.turnId,
    ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
    payload: {
      itemType: "assistant_message",
      status: "completed",
      title: "Assistant message",
      ...(detail && detail.length > 0 ? { detail } : {}),
    },
    raw: omniRaw(input.raw),
  };
}

export function makeOmniTurnCompletedEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly state?: "completed" | "failed" | "interrupted" | "cancelled";
  readonly errorMessage?: string;
  readonly raw: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "turn.completed",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    turnId: input.turnId,
    payload: {
      state: input.state ?? "completed",
      ...(input.errorMessage ? { errorMessage: input.errorMessage } : {}),
    },
    raw: omniRaw(input.raw),
  };
}

export function makeOmniRuntimeErrorEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly message: string;
  readonly raw?: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "runtime.error",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    turnId: input.turnId,
    payload: {
      message: input.message,
      class: "provider_error",
      ...(input.raw !== undefined ? { detail: input.raw } : {}),
    },
    raw: omniRaw(input.raw ?? { message: input.message }),
  };
}

export function makeOmniRequestOpenedEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly requestId: RuntimeRequestId;
  readonly detail: string;
  readonly raw: unknown;
}): ProviderRuntimeEvent {
  return {
    type: "request.opened",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    turnId: input.turnId,
    requestId: input.requestId,
    payload: {
      requestType: "unknown",
      detail: input.detail,
      args: input.raw,
    },
    raw: omniRaw(input.raw),
  };
}

export function makeOmniRequestResolvedEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly requestId: RuntimeRequestId;
  readonly decision: ProviderApprovalDecision;
}): ProviderRuntimeEvent {
  return {
    type: "request.resolved",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    turnId: input.turnId,
    requestId: input.requestId,
    payload: {
      requestType: "unknown",
      decision: input.decision,
    },
    raw: omniRaw({ decision: input.decision }),
  };
}

export function makeOmniToolCallEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly itemId: string | undefined;
  readonly toolName: string;
  readonly status: "inProgress" | "completed" | "failed";
  readonly detail: string | undefined;
  readonly raw: unknown;
}): ProviderRuntimeEvent {
  return {
    type: input.status === "completed" || input.status === "failed" ? "item.completed" : "item.updated",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    turnId: input.turnId,
    itemId: RuntimeItemId.make(input.itemId ?? `omni-tool:${input.toolName}`),
    payload: {
      itemType: "dynamic_tool_call",
      status: input.status,
      title: input.toolName,
      ...(input.detail ? { detail: input.detail } : {}),
    },
    raw: omniRaw(input.raw),
  };
}

export function makeOmniSessionStartedEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly message?: string;
  readonly raw?: unknown;
}): ProviderRuntimeEvent {
  const message = input.message?.trim();
  return {
    type: "session.started",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    payload: {
      ...(message && message.length > 0 ? { message } : {}),
    },
    raw: omniRaw(input.raw ?? {}),
  };
}

export function makeOmniThreadStartedEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly providerThreadId: string | undefined;
  readonly raw?: unknown;
}): ProviderRuntimeEvent {
  const providerThreadId = input.providerThreadId?.trim();
  return {
    type: "thread.started",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    payload: {
      ...(providerThreadId && providerThreadId.length > 0 ? { providerThreadId } : {}),
    },
    raw: omniRaw(input.raw ?? {}),
  };
}

export function makeOmniSessionExitedEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly reason?: string;
  readonly recoverable?: boolean;
  readonly exitKind?: "graceful" | "error";
  readonly raw?: unknown;
}): ProviderRuntimeEvent {
  const reason = input.reason?.trim();
  return {
    type: "session.exited",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    payload: {
      ...(reason && reason.length > 0 ? { reason } : {}),
      ...(input.recoverable !== undefined ? { recoverable: input.recoverable } : {}),
      ...(input.exitKind ? { exitKind: input.exitKind } : {}),
    },
    raw: omniRaw(input.raw ?? {}),
  };
}

export function makeOmniTurnStartedEvent(input: {
  readonly stamp: OmniEventStamp;
  readonly provider: ProviderDriverKind;
  readonly threadId: ThreadId;
  readonly turnId: TurnId | undefined;
  readonly model?: string;
  readonly raw?: unknown;
}): ProviderRuntimeEvent {
  const model = input.model?.trim();
  return {
    type: "turn.started",
    ...input.stamp,
    provider: input.provider,
    threadId: input.threadId,
    turnId: input.turnId,
    payload: {
      ...(model && model.length > 0 ? { model } : {}),
    },
    raw: omniRaw(input.raw ?? {}),
  };
}
