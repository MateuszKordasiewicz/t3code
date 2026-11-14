import {
  CodexSettings,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSessionId,
  ProjectId,
  resolveProjectProviderInstanceEnabled,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexErrors from "effect-codex-app-server/errors";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Cause from "effect/Cause";
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
import * as PreparationOwner from "../preparation/PreparationProcessOwner.ts";
import * as Settings from "../serverSettings.ts";
import { codexAppServerEnvironment } from "../provider/CodexProvider.ts";
import { codexAppServerArgs, resolveCodexLaunchArgs } from "../provider/codexLaunchArgs.ts";
import { resolveCodexHomeLayout } from "../provider/Drivers/CodexHomeLayout.ts";
import * as Workspace from "./ThreadLaunchService.ts";
import * as Manager from "./ProviderSessionManager.ts";
import * as EventSink from "./EventSink.ts";
import * as ProviderInstances from "../provider/ProviderInstanceRegistry.ts";
import * as CodexInstallation from "../provider/CodexInstallation.ts";
import * as RuntimePolicy from "./RuntimePolicy.ts";
import * as CodexAdapter from "./Adapters/CodexAdapterV2.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as Continuations from "@t3tools/provider-core/server/ProviderContinuationRequests";
import * as FileSystem from "effect/FileSystem";
import * as Digest from "effect/crypto/Digest";
import { ServerConfig } from "../config.ts";
import type * as CodexSchema from "effect-codex-app-server/schema";

export class NativeCodexTransportError extends Schema.TaggedError<NativeCodexTransportError>()(
  "NativeCodexTransportError",
  { reason: Schema.Literals(["profile", "stale", "boundary", "closed", "failed"]) },
) {}
const decodeCodexSettings = Schema.decodeEffect(CodexSettings);
const isNativeTransportError = Schema.is(NativeCodexTransportError);
const isFrameSubmissionError = Schema.is(CodexErrors.CodexAppServerFrameSubmissionError);
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
  const manager = yield* Manager.ProviderSessionManagerV2;
  const eventSink = yield* EventSink.EventSinkV2;
  const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
  yield* Manager.assertNativeManagerReceiver(manager, eventSink, mcpSessions);
  const workspace = yield* Workspace.makeNativeWorkspacePreparation(coordinator);
  const settings = yield* Settings.ServerSettingsService;
  const instances = yield* ProviderInstances.ProviderInstanceRegistry;
  const policy = yield* RuntimePolicy.RuntimePolicyV2;
  const installation = yield* CodexInstallation.CodexInstallation;
  const fileSystem = yield* FileSystem.FileSystem;
  const digest = yield* Digest.Digest;
  const idAllocator = yield* IdAllocator.IdAllocatorV2;
  const continuationRequests = yield* Continuations.ProviderContinuationRequests;
  const serverConfig = yield* ServerConfig;
  const environment = Object.freeze({ ...(yield* HostProcess.Environment) });
  const home = yield* HostProcess.HomeDirectory;
  const platform = yield* HostProcess.Platform;
  const resolver = yield* SpawnExecutableResolution;
  const path = yield* Path.Path;
  const serviceScope = yield* Effect.scope;
  type Owner = Effect.Success<ReturnType<typeof store.consumeNativeCodexTransportOwner>>;
  type Entry = {
    fiber?: Fiber.Fiber<unknown, unknown>;
    bootstrapFiber?: Fiber.Fiber<unknown, unknown>;
    adapterFiber?: Fiber.Fiber<unknown, unknown>;
    managerAttempted?: boolean;
    client?: typeof CodexClient.CodexAppServerControlledClient.Service;
    terminated?: Fiber.Fiber<unknown, unknown>;
    runtime?: ProviderAdapter.ProviderAdapterV2SessionRuntime;
    construction?: {
      input: Effect.Success<ReturnType<Owner["captureAdapterConstruction"]>>;
      policy: ProviderAdapter.ProviderAdapterV2RuntimePolicy;
    };
    owner?: Owner;
    preparationOwner?: PreparationOwner.Owner;
    preparationFiber?: Fiber.Fiber<
      Effect.Success<ReturnType<typeof workspace.createReservedWorkspace>>,
      Effect.Error<ReturnType<typeof workspace.createReservedWorkspace>>
    >;
  };
  const entries = new Set<Entry>();
  let closed = false;
  const stop = Effect.fn("WorkflowNativeCodexTransport.stopEntry")(function* (entry: Entry) {
    if (entry.preparationOwner) yield* PreparationOwner.assertCanStop(entry.preparationOwner);
    if (entry.fiber && entry.fiber.pollUnsafe() === undefined) entry.fiber.interruptUnsafe();
    if (entry.bootstrapFiber && entry.bootstrapFiber.pollUnsafe() === undefined)
      entry.bootstrapFiber.interruptUnsafe();
    if (entry.adapterFiber && entry.adapterFiber.pollUnsafe() === undefined)
      entry.adapterFiber.interruptUnsafe();
    if (entry.preparationFiber && entry.preparationFiber.pollUnsafe() === undefined)
      entry.preparationFiber.interruptUnsafe();
    const prepared = entry.preparationFiber
      ? yield* Fiber.await(entry.preparationFiber)
      : undefined;
    if (
      !entry.preparationOwner &&
      prepared &&
      Exit.isSuccess(prepared) &&
      prepared.value.preparation.kind === "retired"
    )
      entry.preparationOwner = yield* Store.readNativeManagerWorkspaceOwner(
        store,
        prepared.value.preparation.job,
        sql,
      );
    const selected = entry.owner ?? entry.preparationOwner;
    const result = selected
      ? yield* Manager.closeNativeManagerRuntime(manager, selected).pipe(
          Effect.catchIf(
            (error) =>
              error.reason === "unregistered" &&
              entry.preparationOwner !== undefined &&
              selected !== entry.preparationOwner,
            () => Manager.closeNativeManagerRuntime(manager, entry.preparationOwner!),
          ),
          Effect.catch((error) =>
            error.reason === "unregistered"
              ? entry.preparationOwner
                ? entry.preparationOwner.stop().pipe(
                    Effect.map((inventory) => ({
                      kind:
                        inventory.inventory === "all-retired"
                          ? ("closed" as const)
                          : ("held" as const),
                      transportId: entry.owner?.transportId ?? "",
                    })),
                  )
                : entry.owner
                  ? entry.owner.close()
                  : Effect.succeed({ kind: "closed" as const, transportId: "" })
              : Effect.succeed({
                  kind: "held" as const,
                  transportId: entry.owner?.transportId ?? "",
                }),
          ),
        )
      : { kind: "closed" as const, transportId: "" };
    if (entry.fiber) yield* Fiber.await(entry.fiber);
    if (entry.bootstrapFiber) yield* Fiber.await(entry.bootstrapFiber);
    if (entry.adapterFiber) yield* Fiber.await(entry.adapterFiber);
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
  const readProfile = Effect.fn("WorkflowNativeCodexTransport.readExistingProfile")(function* (
    projectId: ProjectId,
    instanceId: ProviderInstanceId,
  ) {
    const snapshot = yield* settings.getSettings;
    const instance = snapshot.providerInstances[instanceId];
    const selected = yield* instances.getInstance(instanceId);
    if (
      !selected ||
      selected.instanceId !== instanceId ||
      selected.driverKind !== "codex" ||
      !selected.enabled ||
      selected.auth !== undefined
    )
      return yield* failure("profile");
    if (
      !instance ||
      instance.driver !== "codex" ||
      (instance.environment?.length ?? 0) !== 0 ||
      !resolveProjectProviderInstanceEnabled(snapshot, projectId, instanceId)
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
    const executable = resolver(expandHomePath(config.binaryPath || "codex", home), "linux", env);
    if (!executable || !path.isAbsolute(executable)) return yield* failure("profile");
    const args = codexAppServerArgs(resolveCodexLaunchArgs(config.launchArgs, env));
    const provider = yield* selected.snapshot.getSnapshot;
    const key = stableStringify({
      config,
      env,
      executable,
      args,
      supportedModes: provider.supportedRuntimeModes ?? null,
    });
    return { instance: selected, config, env, executable, args, key };
  });
  const create = Effect.fn("WorkflowNativeCodexTransport.createReservedTransport")(
    function* (preparation: ReturnType<typeof workspace.createReservedWorkspace>) {
      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
        return yield* failure("boundary");
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          if (closed || serviceScope.state._tag === "Closed" || platform !== "linux")
            return yield* failure("closed");
          const entry: Entry = {};
          const control = yield* Effect.withFiber(Effect.succeed);
          entries.add(entry);
          const body = Effect.gen(function* () {
            const preparationFiber = yield* preparation.pipe(Effect.forkIn(childScope));
            entry.preparationFiber = preparationFiber;
            const preparationResult = yield* Fiber.await(preparationFiber);
            if (Exit.isFailure(preparationResult))
              return yield* Effect.failCause(preparationResult.cause);
            const prepared = preparationResult.value;
            if (prepared.preparation.kind !== "retired") return yield* failure("stale");
            const job = prepared.preparation.job;
            const retiredPreparation = prepared.preparation;
            const originalPreparationOwner = yield* Store.readNativeManagerWorkspaceOwner(
              store,
              job,
              sql,
            );
            entry.preparationOwner = originalPreparationOwner;
            const current = job.input.request.current as Record<string, unknown>;
            if (current.driver !== "codex" || typeof current.providerInstanceId !== "string")
              return yield* failure("profile");
            const instanceId = ProviderInstanceId.make(current.providerInstanceId);
            const profile = yield* readProfile(ProjectId.make(job.input.projectId), instanceId);
            const { config, env, executable, args } = profile;
            const checkProfile = Effect.fn("WorkflowNativeCodexTransport.checkExistingProfile")(
              function* () {
                const fresh = yield* readProfile(ProjectId.make(job.input.projectId), instanceId);
                if (fresh.instance !== profile.instance || fresh.key !== profile.key)
                  return yield* failure("profile");
                yield* Manager.checkNativeManagerMcp(
                  manager,
                  originalPreparationOwner,
                  mcpSessions,
                );
              },
            );
            const owner = yield* store.consumeNativeCodexTransportOwner(prepared.preparation.job, {
              command: executable,
              args,
              cwd: prepared.worktree.path,
              env,
            });
            return yield* PreparationOwner.withOperation(
              originalPreparationOwner,
              Effect.gen(function* () {
                entry.owner = owner;
                if (
                  stableStringify(owner.executionClaim) !== stableStringify(prepared.executionClaim)
                )
                  return yield* failure("stale");
                yield* owner.captureConstructor();
                entry.managerAttempted = true;
                yield* Manager.reserveNativeManagerRuntime(manager, {
                  mcp: mcpSessions,
                  store,
                  owner,
                  sql,
                  sink: eventSink,
                  control,
                  onCleanup: () => {
                    entries.delete(entry);
                  },
                  checkProfile: Effect.fn("WorkflowNativeCodexTransport.checkPublicationProfile")(
                    function* () {
                      yield* checkProfile();
                      if (
                        closed ||
                        serviceScope.state._tag === "Closed" ||
                        !entry.client ||
                        !entry.runtime ||
                        !entry.terminated ||
                        !(yield* entry.client.ownedProcess.isRunning) ||
                        entry.terminated.pollUnsafe() !== undefined
                      )
                        return yield* failure("closed");
                    },
                  ),
                });
                yield* checkProfile();
                const io = yield* owner.open();
                let bootstrapSelection: string | undefined;
                let bootstrapMethod: "initialize" | "initialized" | undefined;
                let bootstrapAttempted = false;
                let adapterAttempted = false;
                let initializeResponse: CodexSchema.V1InitializeResponse | undefined;
                let observer:
                  | CodexClient.CodexAppServerControlledClientOptions<CodexErrors.CodexAppServerFrameSubmissionError>["onOutcome"]
                  | undefined;
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
                    wrapper: (frame, submit) =>
                      bootstrapSelection !== undefined &&
                      frame.selection === bootstrapSelection &&
                      frame.method === bootstrapMethod
                        ? submit()
                        : Effect.fail(
                            new CodexErrors.CodexAppServerFrameSubmissionError({
                              outcome: "not_submitted",
                              reason: "wrapper",
                            }),
                          ),
                    onOutcome: (outcome) => (observer ? observer(outcome) : Effect.void),
                  },
                ).pipe(Effect.provideService(Scope.Scope, owner.scope));
                const terminated = yield* client.awaitTermination.pipe(
                  Effect.exit,
                  Effect.forkIn(owner.scope),
                );
                const retained = Object.freeze(
                  CodexClient.CodexAppServerControlledClient.of({
                    ...client,
                    raw: Object.freeze({ ...client.raw }),
                    ownedProcess: Object.freeze({
                      pid: ChildProcessSpawner.ProcessId(io.pid),
                      isRunning: io.isRunning.pipe(Effect.mapError(() => unavailable("isRunning"))),
                      awaitExit: io.exitCode.pipe(
                        Effect.map(ChildProcessSpawner.ExitCode),
                        Effect.mapError(() => unavailable("exit")),
                      ),
                    }),
                  }),
                );
                entry.client = retained;
                entry.terminated = terminated;
                const bootstrap = Effect.fn("WorkflowNativeCodexTransport.bootstrap")(
                  function* () {
                    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
                      return yield* failure("boundary");
                    return yield* Effect.uninterruptibleMask((resume) =>
                      Effect.gen(function* () {
                        if (bootstrapAttempted || closed || serviceScope.state._tag === "Closed")
                          return yield* failure("closed");
                        bootstrapAttempted = true;
                        const scope = yield* Scope.make("sequential");
                        yield* Scope.addFinalizer(serviceScope, Scope.close(scope, Exit.void));
                        const work = Effect.gen(function* () {
                          yield* checkProfile();
                          const profile = yield* owner.captureBootstrap();
                          bootstrapSelection = profile.selection;
                          bootstrapMethod = "initialize";
                          const response = yield* client
                            .request("initialize", profile.params, profile.selection)
                            .pipe(Effect.timeout("10 seconds"));
                          yield* owner.confirmInitialize();
                          bootstrapMethod = "initialized";
                          yield* client.notify("initialized", undefined, profile.selection);
                          return Object.freeze({
                            kind: "protocol-initialized" as const,
                            transportId: owner.transportId,
                            response,
                          });
                        }).pipe(
                          Effect.interruptible,
                          (work) =>
                            PreparationOwner.withOperation(
                              originalPreparationOwner,
                              work,
                              "native",
                            ),
                          Effect.ensuring(
                            Effect.sync(() => {
                              bootstrapSelection = undefined;
                              bootstrapMethod = undefined;
                            }),
                          ),
                        );
                        const fiber = yield* work.pipe(Effect.forkIn(scope));
                        entry.bootstrapFiber = fiber;
                        const result = yield* resume(Fiber.await(fiber)).pipe(
                          Effect.onInterrupt(() => stop(entry).pipe(Effect.ignore)),
                        );
                        if (Exit.isFailure(result)) {
                          yield* stop(entry).pipe(Effect.ignore);
                          entries.delete(entry);
                          return yield* Effect.failCause(result.cause);
                        }
                        initializeResponse = Object.freeze({ ...result.value.response });
                        return result.value;
                      }),
                    );
                  },
                  Effect.mapError(() => failure("failed")),
                  Effect.interruptible,
                );
                const createAdapterRuntime = Effect.fn(
                  "WorkflowNativeCodexTransport.createAdapterRuntime",
                )(
                  function* () {
                    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
                      return yield* failure("boundary");
                    return yield* Effect.uninterruptibleMask((resume) =>
                      Effect.gen(function* () {
                        if (
                          adapterAttempted ||
                          !initializeResponse ||
                          closed ||
                          serviceScope.state._tag === "Closed"
                        )
                          return yield* failure("closed");
                        adapterAttempted = true;
                        const response = initializeResponse;
                        const scope = yield* Scope.make("sequential");
                        yield* Scope.addFinalizer(serviceScope, Scope.close(scope, Exit.void));
                        const work = Effect.gen(function* () {
                          const input = yield* owner.captureAdapterConstruction();
                          yield* checkProfile();
                          if (
                            !(yield* retained.ownedProcess.isRunning) ||
                            terminated.pollUnsafe() !== undefined
                          )
                            return yield* failure("closed");
                          const runtimePolicy = yield* policy.resolve(input);
                          if (runtimePolicy.cwd !== prepared.worktree.path)
                            return yield* failure("profile");
                          yield* owner.confirmAdapterConstruction();
                          yield* checkProfile();
                          const expected = {
                            instanceId,
                            threadId: input.thread.id,
                            providerSessionId: ProviderSessionId.make(
                              owner.current.providerSessionId,
                            ),
                            runtimePolicy,
                            settings: config,
                            environment: env,
                          };
                          let acquired = false;
                          const clientFactory: CodexAdapter.CodexAppServerClientFactoryShape = {
                            open: () => Effect.die("Native adoption cannot open another client"),
                            adoptInitializedControlled: Effect.fn(
                              "WorkflowNativeCodexTransport.adoptInitializedControlled",
                            )(
                              function* (
                                actual: Parameters<
                                  NonNullable<
                                    CodexAdapter.CodexAppServerClientFactoryShape["adoptInitializedControlled"]
                                  >
                                >[0],
                              ) {
                                if (
                                  acquired ||
                                  observer !== undefined ||
                                  stableStringify({
                                    instanceId: actual.instanceId,
                                    threadId: actual.threadId,
                                    providerSessionId: actual.providerSessionId,
                                    runtimePolicy: actual.runtimePolicy,
                                    settings: actual.settings,
                                    environment: actual.environment,
                                  }) !== stableStringify(expected)
                                )
                                  return yield* failure("stale");
                                acquired = true;
                                yield* owner.confirmAdapterConstruction();
                                yield* checkProfile();
                                observer = actual.onOutcome;
                                return { client: retained, initializeResponse: response };
                              },
                              (work, actual) =>
                                work.pipe(
                                  Effect.mapError(
                                    (cause) =>
                                      new ProviderAdapter.ProviderAdapterOpenSessionError({
                                        driver: ProviderDriverKind.make("codex"),
                                        providerSessionId: actual.providerSessionId,
                                        cause,
                                      }),
                                  ),
                                ),
                            ),
                          };
                          const adapter = yield* CodexAdapter.makeCodexAdapterV2({
                            instanceId,
                            settings: config,
                            environment: env,
                            clientFactory,
                            digest,
                            fileSystem,
                            idAllocator,
                            serverConfig,
                            onUsageLimits: profile.instance.snapshot.applyUsageLimits,
                            compatibility: installation.compatibility,
                            continuationRequests,
                          }).pipe(
                            Effect.provideService(
                              McpProviderSessions.McpProviderSessions,
                              mcpSessions,
                            ),
                            Effect.provideService(Scope.Scope, owner.scope),
                          );
                          const runtime = yield* adapter
                            .openSession({
                              threadId: input.thread.id,
                              providerSessionId: expected.providerSessionId,
                              modelSelection: input.modelSelection,
                              runtimePolicy,
                            })
                            .pipe(Effect.provideService(Scope.Scope, owner.scope));
                          yield* checkProfile();
                          const freshPolicy = yield* policy.resolve(input);
                          if (stableStringify(freshPolicy) !== stableStringify(runtimePolicy))
                            return yield* failure("profile");
                          if (
                            !(yield* retained.ownedProcess.isRunning) ||
                            terminated.pollUnsafe() !== undefined
                          )
                            return yield* failure("closed");
                          yield* owner.confirmAdapterConstruction();
                          Object.freeze(runtime.providerSession);
                          return Object.freeze({
                            kind: "adapter-runtime-created" as const,
                            transportId: owner.transportId,
                            runtime: Object.freeze(runtime),
                            policy: runtimePolicy,
                            runState: "preparing" as const,
                            input,
                          });
                        }).pipe(Effect.interruptible, (work) =>
                          PreparationOwner.withOperation(originalPreparationOwner, work, "native"),
                        );
                        const fiber = yield* work.pipe(
                          Effect.forkIn(scope, { startImmediately: true }),
                        );
                        entry.adapterFiber = fiber;
                        const result = yield* Effect.exit(
                          resume(
                            Effect.gen(function* () {
                              const constructed = yield* Fiber.await(fiber);
                              if (Exit.isFailure(constructed))
                                return yield* Effect.failCause(constructed.cause);
                              yield* checkProfile();
                              yield* owner.confirmAdapterConstruction();
                              return constructed.value;
                            }).pipe(Effect.timeout("10 seconds")),
                          ),
                        );
                        if (Exit.isFailure(result)) {
                          yield* stop(entry).pipe(Effect.ignore);
                          entries.delete(entry);
                          return yield* Effect.failCause(result.cause);
                        }
                        entry.runtime = result.value.runtime;
                        entry.construction = {
                          input: result.value.input,
                          policy: result.value.policy,
                        };
                        return Object.freeze({
                          kind: result.value.kind,
                          transportId: result.value.transportId,
                          runtime: result.value.runtime,
                          runState: result.value.runState,
                        });
                      }),
                    );
                  },
                  Effect.mapError((error) =>
                    isNativeTransportError(error) ? error : failure("failed"),
                  ),
                  Effect.interruptible,
                );
                return Object.freeze({
                  kind: "transport-created" as const,
                  runState: "preparing" as const,
                  native: prepared.native,
                  executionClaim: prepared.executionClaim,
                  preparation: retiredPreparation,
                  transportId: owner.transportId,
                  bootstrap,
                  client: retained,
                  createAdapterRuntime,
                  publishRuntime: Effect.fn("WorkflowNativeCodexTransport.publishRuntime")(
                    function* () {
                      if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
                        return yield* failure("boundary");
                      if (closed || serviceScope.state._tag === "Closed" || !entry.runtime)
                        return yield* failure("closed");
                      const runtime = yield* Manager.publishNativeManagerRuntime(manager, owner);
                      return Object.freeze({
                        kind: "runtime-published" as const,
                        transportId: owner.transportId,
                        runtime,
                        runState: "preparing" as const,
                      });
                    },
                    Effect.mapError(() => failure("failed")),
                    Effect.interruptible,
                  ),
                  close: Effect.fn("WorkflowNativeCodexTransport.close")(function* () {
                    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
                      return yield* failure("boundary");
                    return yield* Effect.uninterruptibleMask((resume) =>
                      Effect.gen(function* () {
                        yield* retained.assertCanQuiesceOutput;
                        yield* PreparationOwner.assertCanStop(originalPreparationOwner);
                        const quiet = yield* Effect.exit(resume(client.quiesceOutput));
                        if (Exit.isFailure(quiet)) {
                          const error = Cause.findErrorOption(quiet.cause);
                          if (
                            Option.isSome(error) &&
                            isFrameSubmissionError(error.value) &&
                            error.value.reason === "reentrant"
                          )
                            return yield* Effect.failCause(quiet.cause);
                        }
                        const result = yield* stop(entry);
                        entries.delete(entry);
                        return result;
                      }),
                    );
                  }),
                });
              }),
              "native",
            );
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
