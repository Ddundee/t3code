import * as Effect from "effect/Effect";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { HttpClient } from "effect/unstable/http";

import { TextGenerationError, type ModelSelection, type OmniSettings } from "@t3tools/contracts";
import { sanitizeBranchFragment, sanitizeFeatureBranchName } from "@t3tools/shared/git";
import { extractJsonObject } from "@t3tools/shared/schemaJson";

import * as TextGeneration from "./TextGeneration.ts";
import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";
import {
  omniRuntimeErrorDetail,
  OmniRuntime,
  omniServerConnectionFromSettings,
  resolveOmniDefaultAgentId,
} from "../provider/omniRuntime.ts";

const OMNI_TIMEOUT_MS = 180_000;

// Terminal Responses-API events that signal the model turn is finished. The
// session event stream stays open across turns, so the consumer must stop on
// one of these rather than waiting for the stream itself to close (otherwise
// every generation would block until the timeout below).
const OMNI_TERMINAL_EVENTS = new Set([
  "response.completed",
  "response.failed",
  "response.incomplete",
  "response.error",
  "session.idle",
]);

const omniFrameEventType = (frame: { readonly event: string; readonly data: unknown }): string => {
  const data = frame.data && typeof frame.data === "object" ? (frame.data as Record<string, unknown>) : {};
  return typeof data.type === "string" ? data.type : frame.event;
};

export const makeOmniTextGeneration = Effect.fn("makeOmniTextGeneration")(function* (
  omniSettings: OmniSettings,
  _environment: NodeJS.ProcessEnv = process.env,
) {
  const omniRuntime = yield* OmniRuntime;
  const httpClient = yield* HttpClient.HttpClient;
  const connection = omniServerConnectionFromSettings(omniSettings);

  const withHttp = <A, E>(effect: Effect.Effect<A, E, HttpClient.HttpClient>) =>
    effect.pipe(Effect.provideService(HttpClient.HttpClient, httpClient));

  const runOmniJson = <S extends Schema.Top>({
    operation,
    cwd,
    prompt,
    outputSchemaJson,
    modelSelection,
  }: {
    operation:
      | "generateCommitMessage"
      | "generatePrContent"
      | "generateBranchName"
      | "generateThreadTitle";
    cwd: string;
    prompt: string;
    outputSchemaJson: S;
    modelSelection: ModelSelection;
  }): Effect.Effect<S["Type"], TextGenerationError, S["DecodingServices"]> =>
    Effect.gen(function* () {
      const agents = yield* withHttp(omniRuntime.listOmniAgents(connection)).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: omniRuntimeErrorDetail(cause),
              cause,
            }),
        ),
      );
      const agentId =
        modelSelection.model.trim() || resolveOmniDefaultAgentId(omniSettings, agents) || "";
      if (agentId.length === 0) {
        return yield* new TextGenerationError({
          operation,
          detail: "No Omnigent agent is configured for text generation.",
        });
      }

      const hostId = omniSettings.hostId.trim() || undefined;
      const session = yield* withHttp(
        omniRuntime.createOmniSession({
          connection,
          agentId,
          hostId,
          workspace: cwd,
          title: `t3-code:${operation}`,
        }),
      ).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: omniRuntimeErrorDetail(cause),
              cause,
            }),
        ),
      );

      const outputRef = yield* Ref.make("");
      const abortController = new AbortController();
      const frames = yield* withHttp(
        omniRuntime.streamOmniSessionEvents({
          connection,
          sessionId: session.id,
          signal: abortController.signal,
        }),
      ).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: omniRuntimeErrorDetail(cause),
              cause,
            }),
        ),
      );

      // Consume the SSE stream (accumulating output_text deltas until a
      // terminal event) while concurrently posting the user message that
      // triggers the response. Both must finish within the timeout.
      const consumeStream = frames.pipe(
        Stream.takeUntil((frame) => OMNI_TERMINAL_EVENTS.has(omniFrameEventType(frame))),
        Stream.runForEach((frame) =>
          Effect.gen(function* () {
            const data =
              frame.data && typeof frame.data === "object"
                ? (frame.data as Record<string, unknown>)
                : {};
            const payload =
              data.data && typeof data.data === "object"
                ? (data.data as Record<string, unknown>)
                : data;
            if (
              omniFrameEventType(frame) === "response.output_text.delta" &&
              typeof payload.delta === "string"
            ) {
              yield* Ref.update(outputRef, (current) => current + payload.delta);
            }
          }),
        ),
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: omniRuntimeErrorDetail(cause),
              cause,
            }),
        ),
      );

      const postMessage = withHttp(
        omniRuntime.postOmniSessionEvent({
          connection,
          sessionId: session.id,
          type: "message",
          data: {
            role: "user",
            content: [{ type: "input_text", text: prompt }],
          },
        }),
      ).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: omniRuntimeErrorDetail(cause),
              cause,
            }),
        ),
      );

      yield* Effect.all([consumeStream, postMessage], {
        concurrency: "unbounded",
        discard: true,
      }).pipe(
        Effect.timeoutOrElse({
          duration: OMNI_TIMEOUT_MS,
          orElse: () =>
            Effect.fail(
              new TextGenerationError({
                operation,
                detail: `Omni text generation timed out after ${OMNI_TIMEOUT_MS}ms.`,
              }),
            ),
        }),
        Effect.ensuring(Effect.sync(() => abortController.abort())),
      );

      const raw = yield* Ref.get(outputRef);
      const decodeOutput = Schema.decodeEffect(Schema.fromJsonString(outputSchemaJson));
      return yield* decodeOutput(extractJsonObject(raw)).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation,
              detail: `Failed to decode Omni JSON output: ${String(cause)}`,
              cause,
            }),
        ),
      );
    });

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("OmniTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt, outputSchema } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
      });
      const generated = yield* runOmniJson({
        operation: "generateCommitMessage",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        subject: sanitizeCommitSubject(generated.subject),
        body: generated.body.trim(),
        ...("branch" in generated && typeof generated.branch === "string"
          ? { branch: sanitizeFeatureBranchName(generated.branch) }
          : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("OmniTextGeneration.generatePrContent")(function* (input) {
      const { prompt, outputSchema } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
      });
      const generated = yield* runOmniJson({
        operation: "generatePrContent",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizePrTitle(generated.title),
        body: generated.body.trim(),
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("OmniTextGeneration.generateBranchName")(function* (input) {
      const { prompt, outputSchema } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });
      const generated = yield* runOmniJson({
        operation: "generateBranchName",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        branch: sanitizeBranchFragment(generated.branch),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("OmniTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt, outputSchema } = buildThreadTitlePrompt({
        message: input.message,
        attachments: input.attachments,
      });
      const generated = yield* runOmniJson({
        operation: "generateThreadTitle",
        cwd: input.cwd,
        prompt,
        outputSchemaJson: outputSchema,
        modelSelection: input.modelSelection,
      });
      return {
        title: sanitizeThreadTitle(generated.title),
      } satisfies TextGeneration.ThreadTitleGenerationResult;
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
