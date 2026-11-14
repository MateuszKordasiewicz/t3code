import {
  CodexSettings,
  ProviderInstanceId,
  ProjectId,
  resolveProjectProviderInstanceEnabled,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Option from "effect/Option";
import * as PlatformError from "effect/PlatformError";
import * as Stream from "effect/Stream";
import * as Sink from "effect/Sink";
import * as Path from "effect/Path";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import { stableStringify } from "@t3tools/shared/relaySigning";
import { expandHomePath } from "@t3tools/provider-core/server/pathExpansion";
import * as Store from "../workflowRuns/WorkflowRunStore.ts";
import * as Settings from "../serverSettings.ts";
import { codexAppServerEnvironment } from "../provider/CodexProvider.ts";
import { codexAppServerArgs, resolveCodexLaunchArgs } from "../provider/codexLaunchArgs.ts";
import { resolveCodexHomeLayout } from "../provider/Drivers/CodexHomeLayout.ts";
import * as Workspace from "./ThreadLaunchService.ts";

export class NativeCodexTransportError extends Schema.TaggedError<NativeCodexTransportError>()(
  "NativeCodexTransportError",
  { reason: Schema.Literals(["profile", "stale", "boundary", "closed", "failed"]) },
) {}
const decodeCodexSettings = Schema.decodeEffect(CodexSettings);
const isNativeTransportError = Schema.is(NativeCodexTransportError);
const failure = (reason: NativeCodexTransportError["reason"]) =>
  new NativeCodexTransportError({ reason });
const unavailable = (method: string) =>
  PlatformError.systemError({
    _tag: "Unknown",
    module: "NativeCodexTransport",
    method,
    description: "Native transport unavailable",
  });

export const makeNativeCodexTransport = Effect.fn("WorkflowNativeCodexTransport.make")(function* (
  coordinator: Parameters<typeof Workspace.makeNativeWorkspacePreparation>[0],
) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* Store.WorkflowRunStore;
  const workspace = yield* Workspace.makeNativeWorkspacePreparation(coordinator);
  const settings = yield* Settings.ServerSettingsService;
  const environment = Object.freeze({ ...(yield* HostProcess.Environment) });
  const home = yield* HostProcess.HomeDirectory;
  const platform = yield* HostProcess.Platform;
  const resolver = yield* SpawnExecutableResolution;
  const path = yield* Path.Path;
  const serviceScope = yield* Effect.scope;
  const entries = new Set<{
    fiber?: Fiber.Fiber<unknown, unknown>;
    owner?: Effect.Success<ReturnType<typeof store.consumeNativeCodexTransportOwner>>;
  }>();
  let closed = false;
  const stop = Effect.fn("WorkflowNativeCodexTransport.stopEntry")(function* (
    entry: typeof entries extends Set<infer A> ? A : never,
  ) {
    if (entry.fiber && entry.fiber.pollUnsafe() === undefined) entry.fiber.interruptUnsafe();
    const result = entry.owner
      ? yield* entry.owner.close()
      : { kind: "closed" as const, transportId: "" };
    if (entry.fiber) yield* Fiber.await(entry.fiber);
    return result;
  });
  yield* Scope.addFinalizer(
    serviceScope,
    Effect.gen(function* () {
      closed = true;
      yield* Effect.forEach(entries, (entry) => stop(entry).pipe(Effect.ignore), {
        concurrency: "unbounded",
        discard: true,
      });
    }),
  );
  const create = Effect.fn("WorkflowNativeCodexTransport.createReservedTransport")(
    function* (preparation: ReturnType<typeof workspace.createReservedWorkspace>) {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* failure("boundary");
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (closed || serviceScope.state._tag === "Closed" || platform !== "linux")
            return yield* failure("closed");
          const entry: {
            fiber?: Fiber.Fiber<unknown, unknown>;
            owner?: Effect.Success<ReturnType<typeof store.consumeNativeCodexTransportOwner>>;
          } = {};
          entries.add(entry);
          const body = Effect.gen(function* () {
            const prepared = yield* preparation;
            if (prepared.preparation.kind !== "retired") return yield* failure("stale");
            const job = prepared.preparation.job;
            const current = job.input.request.current as Record<string, unknown>;
            if (current.driver !== "codex" || typeof current.providerInstanceId !== "string")
              return yield* failure("profile");
            const instanceId = ProviderInstanceId.make(current.providerInstanceId);
            const snapshot = yield* settings.getSettings;
            const instance = snapshot.providerInstances[instanceId];
            if (
              !instance ||
              instance.driver !== "codex" ||
              (instance.environment?.length ?? 0) !== 0 ||
              !resolveProjectProviderInstanceEnabled(
                snapshot,
                ProjectId.make(job.input.projectId),
                instanceId,
              )
            )
              return yield* failure("profile");
            const config = yield* decodeCodexSettings(instance.config ?? {}).pipe(
              Effect.mapError(() => failure("profile")),
            );
            if (config.setupMode !== "existing" || config.shadowHomePath.trim())
              return yield* failure("profile");
            const layout = yield* resolveCodexHomeLayout(config).pipe(
              Effect.provideService(Path.Path, path),
              Effect.provideService(HostProcess.HomeDirectory, home),
            );
            if (layout.mode !== "direct") return yield* failure("profile");
            const env = Object.fromEntries(
              Object.entries({
                ...codexAppServerEnvironment(environment),
                ...(layout.effectiveHomePath ? { CODEX_HOME: layout.effectiveHomePath } : {}),
              }).filter((entry): entry is [string, string] => entry[1] !== undefined),
            );
            const executable = resolver(
              expandHomePath(config.binaryPath || "codex", home),
              "linux",
              env,
            );
            if (!executable || !path.isAbsolute(executable)) return yield* failure("profile");
            const owner = yield* store.consumeNativeCodexTransportOwner(prepared.preparation.job, {
              command: executable,
              args: codexAppServerArgs(resolveCodexLaunchArgs(config.launchArgs, env)),
              cwd: prepared.worktree.path,
              env,
            });
            entry.owner = owner;
            if (stableStringify(owner.executionClaim) !== stableStringify(prepared.executionClaim))
              return yield* failure("stale");
            yield* owner.captureConstructor();
            const io = yield* owner.open();
            const client = yield* CodexClient.makeControlledProcessProtocolClient(
              {
                pid: ChildProcessSpawner.ProcessId(io.pid),
                stdin: io.stdin.pipe(Sink.mapError(() => unavailable("stdin"))),
                stdout: io.stdout.pipe(Stream.mapError(() => unavailable("stdout"))),
                stderr: io.stderr.pipe(Stream.mapError(() => unavailable("stderr"))),
                exitCode: io.exitCode.pipe(
                  Effect.map(ChildProcessSpawner.ExitCode),
                  Effect.mapError(() => unavailable("exit")),
                ),
              },
              {
                wrapper: () =>
                  Effect.fail(
                    new CodexErrors.CodexAppServerFrameSubmissionError({
                      outcome: "not_submitted",
                      reason: "wrapper",
                    }),
                  ),
                onOutcome: () => Effect.void,
              },
            ).pipe(Effect.provideService(Scope.Scope, owner.scope));
            return Object.freeze({
              kind: "transport-created" as const,
              runState: "preparing" as const,
              native: prepared.native,
              executionClaim: prepared.executionClaim,
              preparation: prepared.preparation,
              transportId: owner.transportId,
              client: CodexClient.CodexAppServerControlledClient.of({
                ...client,
                ownedProcess: Object.freeze({
                  pid: ChildProcessSpawner.ProcessId(io.pid),
                  isRunning: io.isRunning.pipe(Effect.mapError(() => unavailable("isRunning"))),
                  awaitExit: io.exitCode.pipe(
                    Effect.map(ChildProcessSpawner.ExitCode),
                    Effect.mapError(() => unavailable("exit")),
                  ),
                }),
              }),
              close: Effect.fn("WorkflowNativeCodexTransport.close")(function* () {
                const result = yield* owner.close();
                entries.delete(entry);
                return result;
              }),
            });
          }).pipe(Effect.scoped, Effect.interruptible);
          const childScope = yield* Scope.make("sequential");
          yield* Scope.addFinalizer(serviceScope, Scope.close(childScope, Exit.void));
          const fiber = yield* body.pipe(Effect.forkIn(childScope));
          entry.fiber = fiber;
          const exit = yield* restore(Fiber.await(fiber)).pipe(
            Effect.onInterrupt(() => stop(entry).pipe(Effect.ignore)),
          );
          if (Exit.isFailure(exit)) {
            yield* stop(entry).pipe(Effect.ignore);
            entries.delete(entry);
            return yield* Effect.failCause(exit.cause);
          }
          return exit.value;
        }),
      );
    },
    Effect.mapError((error) => (isNativeTransportError(error) ? error : failure("failed"))),
    Effect.interruptible,
  );
  return Object.freeze({
    createReservedTransport(selection: Parameters<typeof workspace.createReservedWorkspace>[0]) {
      return create(workspace.createReservedWorkspace(selection));
    },
  });
});
