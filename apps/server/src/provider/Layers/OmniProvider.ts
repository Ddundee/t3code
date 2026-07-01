import {
  type OmniSettings,
  ProviderDriverKind,
  type ModelCapabilities,
  type ServerProviderModel,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Result from "effect/Result";
import { HttpClient } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { createModelCapabilities } from "@t3tools/shared/model";
import { resolveSpawnCommand } from "@t3tools/shared/shell";

import {
  buildServerProvider,
  isCommandMissingCause,
  providerModelsFromSettings,
  spawnAndCollect,
  type ServerProviderDraft,
} from "../providerSnapshot.ts";
import {
  formatOmniServerProbeError,
  omniAgentById,
  omniAgentsOrEmpty,
  omniModelNameFromAgent,
  omniModelSlugFromAgent,
  omniRuntimeErrorDetail,
  OmniRuntime,
  type OmniAgentSummary,
  omniServerConnectionFromSettings,
  omniServerUrlLabel,
  OMNI_VERSION_PROBE_TIMEOUT_MS,
  parseOmniCliVersion,
  resolveOmniDefaultAgentId,
} from "../omniRuntime.ts";

const PROVIDER = ProviderDriverKind.make("omni");
const OMNI_PRESENTATION = {
  displayName: "Omni",
  badgeLabel: "Early Access",
  showInteractionModeToggle: false,
  requiresNewThreadForModelChange: true,
} as const;
const EMPTY_CAPABILITIES: ModelCapabilities = createModelCapabilities({
  optionDescriptors: [],
});

function omniModelsFromSettings(
  customModels: ReadonlyArray<string> | undefined,
  discovered: ReadonlyArray<ServerProviderModel> = [],
): ReadonlyArray<ServerProviderModel> {
  if (discovered.length > 0) {
    return providerModelsFromSettings(
      discovered,
      PROVIDER,
      customModels ?? [],
      EMPTY_CAPABILITIES,
    );
  }
  return providerModelsFromSettings([], PROVIDER, customModels ?? [], EMPTY_CAPABILITIES);
}

function agentsToModels(agents: ReadonlyArray<OmniAgentSummary>) {
  return agents.map(
    (agent): ServerProviderModel => ({
      slug: omniModelSlugFromAgent(agent),
      name: omniModelNameFromAgent(agent),
      isCustom: false,
      capabilities: EMPTY_CAPABILITIES,
    }),
  );
}

export function buildInitialOmniProviderSnapshot(
  omniSettings: OmniSettings,
): Effect.Effect<ServerProviderDraft> {
  return Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);
    const models = omniModelsFromSettings(omniSettings.customModels);

    if (!omniSettings.enabled) {
      return buildServerProvider({
        presentation: OMNI_PRESENTATION,
        enabled: false,
        checkedAt,
        models,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message: "Omni is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: OMNI_PRESENTATION,
      enabled: true,
      checkedAt,
      models,
      probe: {
        installed: true,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Checking Omnigent server availability...",
      },
    });
  });
}

const runOmniVersionCommand = (
  omniSettings: OmniSettings,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.gen(function* () {
    const command = omniSettings.binaryPath || "omni";
    const spawnCommand = yield* resolveSpawnCommand(command, ["--version"], {
      env: environment,
    });
    return yield* spawnAndCollect(
      command,
      ChildProcess.make(spawnCommand.command, spawnCommand.args, {
        env: environment,
        shell: spawnCommand.shell,
      }),
    );
  });

export const checkOmniProviderStatus = Effect.fn("checkOmniProviderStatus")(function* (
  omniSettings: OmniSettings,
  environment: NodeJS.ProcessEnv = process.env,
): Effect.fn.Return<
  ServerProviderDraft,
  never,
  ChildProcessSpawner.ChildProcessSpawner | HttpClient.HttpClient | OmniRuntime
> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const fallbackModels = omniModelsFromSettings(omniSettings.customModels);
  const serverUrl = omniServerUrlLabel(omniSettings);

  if (!omniSettings.enabled) {
    return buildServerProvider({
      presentation: OMNI_PRESENTATION,
      enabled: false,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Omni is disabled in T3 Code settings.",
      },
    });
  }

  const omniRuntime = yield* OmniRuntime;
  const connection = omniServerConnectionFromSettings(omniSettings);

  const versionResult = yield* runOmniVersionCommand(omniSettings, environment).pipe(
    Effect.timeoutOption(OMNI_VERSION_PROBE_TIMEOUT_MS),
    Effect.result,
  );

  let cliInstalled = false;
  let version: string | null = null;

  if (Result.isFailure(versionResult)) {
    cliInstalled = !isCommandMissingCause(versionResult.failure);
  } else if (Option.isNone(versionResult.success)) {
    cliInstalled = true;
  } else {
    cliInstalled = true;
    const versionOutput = versionResult.success.value;
    version = parseOmniCliVersion(`${versionOutput.stdout}\n${versionOutput.stderr}`);
  }

  const healthResult = yield* omniRuntime.probeOmniServerHealth(connection).pipe(Effect.result);
  if (Result.isFailure(healthResult)) {
    const message = formatOmniServerProbeError({
      cause: healthResult.failure,
      serverUrl,
    });
    return buildServerProvider({
      presentation: OMNI_PRESENTATION,
      enabled: true,
      checkedAt,
      models: fallbackModels,
      probe: {
        installed: cliInstalled,
        version,
        status: "error",
        auth: { status: "unknown" },
        message,
      },
    });
  }

  const agentsResult = yield* omniRuntime.listOmniAgents(connection).pipe(Effect.result);
  const agents = Result.isSuccess(agentsResult) ? omniAgentsOrEmpty(agentsResult.success) : [];
  const models = omniModelsFromSettings(omniSettings.customModels, agentsToModels(agents));

  const defaultAgentId = resolveOmniDefaultAgentId(omniSettings, agents);
  const defaultAgent = defaultAgentId ? omniAgentById(agents, defaultAgentId) : undefined;

  let message = `Connected to Omnigent server at ${serverUrl}.`;
  if (agents.length === 0) {
    message = `Connected to Omnigent at ${serverUrl}, but no agents are registered yet.`;
  } else if (!defaultAgent) {
    message = `Connected to Omnigent at ${serverUrl}. Configure a default agent ID in settings.`;
  } else if (omniSettings.hostId.trim().length === 0) {
    message = `Connected to Omnigent at ${serverUrl}. Register a host with \`omni host\` and set Host ID in settings to run turns.`;
  }

  return buildServerProvider({
    presentation: OMNI_PRESENTATION,
    enabled: true,
    checkedAt,
    models,
    probe: {
      installed: cliInstalled,
      version,
      status: "ready",
      auth: { status: "authenticated" },
      message,
    },
  });
});

export function omniProbeDetailFromCause(cause: unknown): string {
  return omniRuntimeErrorDetail(cause);
}

export function isOmniCliMissingCause(cause: unknown): boolean {
  return isCommandMissingCause(cause);
}
