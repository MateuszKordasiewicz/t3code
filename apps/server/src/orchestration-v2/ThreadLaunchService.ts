import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveProjectScripts, setupProjectScript } from "@t3tools/shared/projectScripts";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProjectStore from "./ProjectStore.ts";
import * as WorkflowNativeLaunch from "./WorkflowNativeLaunchService.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as PreparationOwner from "../preparation/PreparationProcessOwner.ts";
import { decodeNativeReservationLaunch } from "../workflowRuns/WorkflowNativeReservationMaterial.ts";
import * as WorktreeSetupTracker from "../project/WorktreeSetupTracker.ts";
import * as ProjectCloneTracker from "../project/ProjectCloneTracker.ts";
import * as TerminalManager from "../terminal/Manager.ts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import {
  CommandId,
  type ChatAttachment,
  type MessageId,
  type ModelSelection,
  type OrchestrationV2Actor,
  type OrchestrationV2CreationSource,
  type OrchestrationV2DelegatedFrom,
  type OrchestrationV2LinkOrigin,
  type OrchestrationV2ProviderThreadNativeMetadata,
  type OrchestrationV2ThreadProjection,
  type ProviderDriverKind,
  type ProviderInteractionMode,
  ProjectId,
  type RunId,
  type RuntimeMode,
  type ScheduledTaskId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import {
  buildTemporaryWorktreeBranchName,
  flattenTemporaryWorktreeBranchName,
  isTemporaryWorktreeBranch,
  WORKTREE_BRANCH_PREFIX,
} from "@t3tools/shared/git";

import * as GitWorkflow from "../git/GitWorkflowService.ts";
import * as ProjectService from "../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as TextGeneration from "../textGeneration/TextGeneration.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "./Orchestrator.ts";
import * as WorkflowRunStore from "../workflowRuns/WorkflowRunStore.ts";
import * as PreparationJob from "../preparation/PreparationJob.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";
import { makeProviderFailure } from "@t3tools/provider-core/server/failure";
import { randomUuidV4 } from "@t3tools/provider-core/server/randomUuid";
import { DispatchModeLimit } from "./DispatchModeLimit.ts";
import { buildClaimCommand, buildInitialMessageCommand } from "./ThreadLaunchCommands.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

export type ThreadLaunchWorkspaceStrategy =
  | { readonly type: "root"; readonly branch?: string | undefined }
  | {
      readonly type: "existing_worktree";
      readonly worktreePath: string;
      readonly branch?: string | undefined;
    }
  | {
      readonly type: "worktree";
      readonly baseRef: string;
      readonly branch?: string | undefined;
      readonly startFromOrigin?: boolean | undefined;
    };

export interface ThreadLaunchInitialMessage {
  readonly messageId?: MessageId;
  readonly scheduledTaskId?: ScheduledTaskId;
  readonly senderThreadId?: ThreadId;
  readonly text: string;
  readonly attachments: ReadonlyArray<ChatAttachment>;
  readonly context?: import("@t3tools/contracts").OrchestrationMessageContext | undefined;
}

export interface ThreadLaunchInput {
  readonly commandId: CommandId;
  readonly threadId?: ThreadId;
  readonly reuseExistingThread?: boolean;
  readonly projectId: ProjectId;
  readonly title: string;
  readonly generateTitle?: boolean;
  readonly modelSelection: ModelSelection;
  readonly runtimeMode: RuntimeMode;
  readonly interactionMode: ProviderInteractionMode;
  readonly workspaceStrategy: ThreadLaunchWorkspaceStrategy;
  readonly initialMessage?: ThreadLaunchInitialMessage;
  readonly importedNativeThread?: {
    readonly ref: {
      readonly driver: ProviderDriverKind;
      readonly nativeId: string;
      readonly strength: "strong";
    };
    readonly metadata?: OrchestrationV2ProviderThreadNativeMetadata;
  };
  readonly createdBy: OrchestrationV2Actor;
  readonly creationSource: OrchestrationV2CreationSource;
  /** The linked environment this launch is for, if one asked for it. */
  readonly linkOrigin?: OrchestrationV2LinkOrigin;
  /** The parent there, when that environment's agent delegated this as a task. */
  readonly delegatedFrom?: OrchestrationV2DelegatedFrom;
}

type PreparationInput = Pick<
  ThreadLaunchInput,
  "commandId" | "projectId" | "workspaceStrategy" | "initialMessage"
> & {
  readonly reusedWorktree?: { readonly baseRef: string };
};

export interface ThreadLaunchPreparationInput {
  readonly commandId: CommandId;
  readonly threadId: ThreadId;
  readonly runId: RunId;
}

export interface ThreadLaunchResult {
  readonly threadId: ThreadId;
  readonly projection: OrchestrationV2ThreadProjection;
  /** The launch's last write (the initial message, else the thread claim) was accepted before this call. */
  readonly resumed: boolean;
  readonly sequence?: number;
}

export class ThreadLaunchError extends Schema.TaggedError<ThreadLaunchError>()(
  "ThreadLaunchError",
  {
    operation: Schema.Literals([
      "resolve-project",
      "read-receipt",
      "generate-metadata",
      "provision-worktree",
      "run-setup-script",
      "create-thread",
      "update-thread",
      "dispatch-message",
      "release-run",
      "fail-run",
    ]),
    commandId: CommandId,
    projectId: ProjectId,
    threadId: Schema.optional(ThreadId),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Thread launch ${this.commandId} failed during ${this.operation}.`;
  }
}

export class ThreadLaunchBaseRefError extends Schema.TaggedError<ThreadLaunchBaseRefError>()(
  "ThreadLaunchBaseRefError",
  {
    projectId: ProjectId,
    projectTitle: Schema.String,
    workspaceRoot: Schema.String,
    baseRef: Schema.String,
  },
) {
  override get message(): string {
    return `Base ref "${this.baseRef}" does not resolve to a commit in project "${this.projectTitle}" (${this.workspaceRoot}).`;
  }
}

export class ThreadLaunchService extends Context.Service<
  ThreadLaunchService,
  {
    readonly launch: (
      input: ThreadLaunchInput,
    ) => Effect.Effect<
      ThreadLaunchResult,
      ThreadLaunchError | ThreadManagement.ThreadManagementProviderInstanceDisabledError
    >;
    /** Check before creation for early errors; launch otherwise accepts and reports base failures inside the thread. */
    readonly checkWorktreeBase: (
      input: Pick<ThreadLaunchInput, "projectId" | "workspaceStrategy">,
    ) => Effect.Effect<void, ThreadLaunchBaseRefError>;
    readonly retryPreparation: (
      input: ThreadLaunchPreparationInput,
    ) => Effect.Effect<
      Orchestrator.OrchestratorV2DispatchResult,
      | Orchestrator.OrchestratorV2Error
      | ThreadManagement.ThreadManagementProviderInstanceDisabledError
    >;
    readonly prepareRun: (input: ThreadLaunchPreparationInput) => Effect.Effect<void>;
  }
>()("t3/orchestration-v2/ThreadLaunchService") {}

const isThreadLaunchError = Schema.is(ThreadLaunchError);

function failureDetail(error: unknown): string {
  if (isThreadLaunchError(error)) {
    const cause = error.cause;
    const detail = cause instanceof Error ? cause.message : String(cause);
    return `Workspace preparation failed during ${error.operation.replaceAll("-", " ")}: ${detail}`;
  }
  return `Workspace preparation failed: ${error instanceof Error ? error.message : String(error)}`;
}

function ownOrdinaryInput<A>(input: A): A {
  const ancestors = new Set<object>();
  const copy = (value: unknown): unknown => {
    if (value === null || typeof value === "boolean") return value;
    if (typeof value === "string" && value.isWellFormed()) return value;
    if (
      typeof value === "number" &&
      Number.isFinite(value) &&
      Math.abs(value) <= Number.MAX_SAFE_INTEGER &&
      !Object.is(value, -0)
    )
      return value;
    if (typeof value !== "object" || value === null || ancestors.has(value))
      throw new Error("Invalid preparation input");
    const array = Array.isArray(value);
    const prototype = Object.getPrototypeOf(value);
    if (
      array ? prototype !== Array.prototype : prototype !== Object.prototype && prototype !== null
    )
      throw new Error("Invalid preparation input");
    const keys = Reflect.ownKeys(value);
    if (array && keys.length !== value.length + 1) throw new Error("Invalid preparation input");
    ancestors.add(value);
    const entries: [string, unknown][] = [];
    for (const key of keys) {
      if (array && key === "length") continue;
      if (
        typeof key !== "string" ||
        !key.isWellFormed() ||
        (array && (!/^(0|[1-9]\d*)$/.test(key) || Number(key) >= value.length))
      )
        throw new Error("Invalid preparation input");
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor || !descriptor.enumerable || !("value" in descriptor))
        throw new Error("Invalid preparation input");
      if (descriptor.value === undefined && !array) continue;
      entries.push([key, copy(descriptor.value)]);
    }
    ancestors.delete(value);
    return Object.freeze(array ? entries.map(([, item]) => item) : Object.fromEntries(entries));
  };
  const owned = copy(input);
  WorkflowCanonicalHash.assertCanonical(owned);
  return owned as A;
}
type OwnedPreparationJob = Effect.Success<
  ReturnType<WorkflowRunStore.WorkflowRunStore["Service"]["closePreparationCode"]>
>;
type LaunchFailure = Effect.Error<ReturnType<ThreadLaunchService["Service"]["launch"]>>;

const make = Effect.gen(function* () {
  const projects = yield* ProjectService.ProjectService;
  const setupTracker = yield* WorktreeSetupTracker.WorktreeSetupTracker;
  const cloneTracker = yield* ProjectCloneTracker.ProjectCloneTracker;
  const terminals = yield* TerminalManager.TerminalManager;
  const git = yield* GitWorkflow.GitWorkflowService;
  const setupScripts = yield* ProjectSetupScriptRunner.ProjectSetupScriptRunner;
  const providerRegistry = yield* ProviderRegistry.ProviderRegistry;
  const serverSettings = yield* ServerSettings.ServerSettingsService;
  const textGeneration = yield* TextGeneration.TextGeneration;
  const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
  const workflowRuns = yield* WorkflowRunStore.WorkflowRunStore;
  const environmentId = yield* (yield* ServerEnvironment.ServerEnvironmentIdentity)
    .getEnvironmentId;
  const serverBootId = yield* randomUuidV4;
  const ids = yield* IdAllocator.IdAllocatorV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const managedFolders = yield* ManagedProjectFolders.ManagedProjectFolders;
  const preparationScope = yield* Scope.make("sequential");
  const scheduledLaunches = yield* Ref.make<ReadonlySet<CommandId>>(new Set());
  const launching = new Map<
    CommandId,
    { hash: string; result: Deferred.Deferred<ThreadLaunchResult, LaunchFailure> }
  >();
  yield* Effect.addFinalizer(() => Scope.close(preparationScope, Exit.void));

  const mapError =
    (input: PreparationInput, operation: ThreadLaunchError["operation"], threadId?: ThreadId) =>
    (cause: unknown) =>
      new ThreadLaunchError({
        operation,
        commandId: input.commandId,
        projectId: input.projectId,
        ...(threadId === undefined ? {} : { threadId }),
        cause,
      });

  const finishPreparationJob = Effect.fn("ThreadLaunchService.finishPreparationJob")(
    function* (job: OwnedPreparationJob) {
      const unknown = yield* workflowRuns.quiescePreparationJob(
        PreparationJob.selection(job),
        "unknown",
      );
      yield* workflowRuns.closePreparationCode(PreparationJob.selection(unknown));
    },
    Effect.catchCause((cause) =>
      Effect.logWarning("Preparation job code closure remains uncertain", { cause }),
    ),
  );
  const acquirePreparationJob = Effect.fn("ThreadLaunchService.acquirePreparationJob")(function* (
    phase: "thread-launch" | "thread-prepare",
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId | null,
    request: object,
    nativeSelection: PreparationJob.NativeSelection | null = null,
  ) {
    const result = yield* workflowRuns.beginOrdinaryPreparation({
      jobId: `${phase}:${input.commandId}`,
      entryKey: `${phase}:${input.commandId}`,
      kind: "launch",
      environmentId,
      projectId: input.projectId,
      threadId,
      runId,
      originatingLaunchJobId: null,
      parentSelection: null,
      nativeSelection,
      serverBootId,
      request: ownOrdinaryInput({ phase, processEvidenceMode: "untracked-preparation", request }),
    });
    if (result.kind !== "acquired") return result;
    const exposed = yield* workflowRuns.markPreparationProcessExposed(
      PreparationJob.selection(result.job),
    );
    return Object.freeze({ kind: "acquired" as const, job: exposed });
  });
  const requireOrdinaryThread = Effect.fn("ThreadLaunchService.requireOrdinaryThread")(function* (
    threadId: ThreadId,
  ) {
    const custody = yield* workflowRuns.readThreadCustody({ threadId });
    if (custody.kind === "owned")
      return yield* Effect.fail("Thread has workflow or planning custody.");
  });

  const readReceipt = (input: ThreadLaunchInput, commandId: CommandId) =>
    receipts
      .getByCommandId(commandId)
      .pipe(Effect.mapError(mapError(input, "read-receipt", input.threadId)));

  const validateReusableThread = Effect.fn("ThreadLaunchService.validateReusableThread")(function* (
    input: ThreadLaunchInput,
    threadId: ThreadId,
  ) {
    const projection = yield* threads
      .getThreadRecords(threadId, ["runs"])
      .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
    if (
      projection.thread.projectId !== input.projectId ||
      projection.thread.archivedAt !== null ||
      projection.thread.deletedAt !== null ||
      (yield* threads
        .getMessageCount(threadId)
        .pipe(Effect.mapError(mapError(input, "update-thread", threadId)))) > 0 ||
      projection.runs.length > 0
    ) {
      return yield* mapError(
        input,
        "update-thread",
        threadId,
      )("Only an empty active thread in the target project can change workspace during launch.");
    }
  });

  const checkWorktreeBase: ThreadLaunchService["Service"]["checkWorktreeBase"] = Effect.fn(
    "ThreadLaunchService.checkWorktreeBase",
  )(function* (input) {
    const strategy = input.workspaceStrategy;
    // `:/text` is a commit search, which cannot be asked for a commit type.
    if (strategy.type !== "worktree" || strategy.baseRef.startsWith(":")) return;
    const { baseRef } = strategy;
    const unresolved = yield* Effect.gen(function* () {
      const project = Option.getOrUndefined(yield* projects.getById(input.projectId));
      if (project === undefined) return undefined;
      const cwd = project.workspaceRoot;
      // `git worktree add` reads `-` as the previous checkout.
      if (yield* git.hasCommit({ cwd, refName: baseRef === "-" ? "@{-1}" : baseRef }))
        return undefined;
      if (
        strategy.startFromOrigin === true &&
        (yield* git.resolveWorktreeBaseRemote({ cwd, refName: baseRef })) !== null
      )
        return undefined;
      // git worktree add accepts remote-tracking branches; let Git decide when any matching ref exists.
      if (yield* git.hasRefNamed({ cwd, refName: baseRef })) return undefined;
      return (yield* git.hasCommit({ cwd, refName: "HEAD" })) ? project : undefined;
    }).pipe(Effect.orElseSucceed(() => undefined));
    if (unresolved !== undefined)
      return yield* new ThreadLaunchBaseRefError({
        projectId: unresolved.id,
        projectTitle: unresolved.title,
        workspaceRoot: unresolved.workspaceRoot,
        baseRef,
      });
  });

  const prepareInBackground = Effect.fn("ThreadLaunchService.prepareInBackground")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId | null,
  ) {
    const project = yield* projects.getById(input.projectId).pipe(
      Effect.mapError(mapError(input, "resolve-project", threadId)),
      Effect.flatMap(
        Option.match({
          onNone: () =>
            Effect.fail(mapError(input, "resolve-project", threadId)("Project no longer exists.")),
          onSome: Effect.succeed,
        }),
      ),
    );

    const reused = input.reusedWorktree;
    const tracked = input.workspaceStrategy.type === "worktree" || reused !== undefined;
    let createdWorktreePath: string | null = null;
    let createdBranch: string | null = null;
    let setupTerminalId: string | null = null;
    let workspaceRecorded = false;
    let runReleased = false;
    // Stop background renames before forgetting worktrees; completed renames record them again.
    let branchRename: Fiber.Fiber<unknown> | null = null;
    if (input.workspaceStrategy.type === "worktree") {
      yield* setupTracker.begin({
        threadId,
        branch: input.workspaceStrategy.branch ?? null,
        baseRef: input.workspaceStrategy.baseRef,
        stages: ["fetch", "checkout", "setup-script", "agent"],
        fiber: yield* Effect.fiber,
      });
    } else if (reused !== undefined) {
      yield* setupTracker.begin({
        threadId,
        branch: input.workspaceStrategy.branch ?? null,
        baseRef: reused.baseRef,
        stages: ["setup-script", "agent"],
        fiber: yield* Effect.fiber,
      });
    }
    yield* Effect.gen(function* () {
      const initialMessage = input.initialMessage;
      const generateBranchNameFor = Effect.fn("ThreadLaunchService.generateBranchNameFor")(
        function* (cwd: string, message: ThreadLaunchInitialMessage) {
          const settings = resolveProjectSettings(
            yield* serverSettings.getSettings,
            input.projectId,
          ).settings;
          const modelSelection =
            settings.sourceControlWriterModelSelection === null
              ? settings.textGenerationModelSelection
              : ServerSettings.resolveSourceControlWriterModelSelection(
                  settings,
                  yield* providerRegistry.getProviders,
                  input.projectId,
                );
          return yield* textGeneration
            .generateBranchName({
              naming: {
                mode: settings.branchNamingMode,
                prefix: settings.branchNamePrefix,
                instructions: settings.branchNameInstructions,
              },
              cwd,
              message: message.text,
              attachments: message.attachments,
              ...(message.context ? { context: message.context } : {}),
              modelSelection,
              projectId: input.projectId,
            })
            .pipe(
              Effect.map((result) => ({
                branch: result.branch,
                exactName: settings.branchNamingMode === "custom",
              })),
            );
        },
      );

      const requestedBranch = input.workspaceStrategy.branch;
      let branch: string | null;
      if (input.workspaceStrategy.type === "worktree" && requestedBranch === undefined) {
        const uuid = yield* randomUuidV4;
        branch = buildTemporaryWorktreeBranchName(() => uuid.replaceAll("-", ""));
      } else {
        branch = requestedBranch ?? null;
      }
      let worktreePath =
        input.workspaceStrategy.type === "existing_worktree"
          ? input.workspaceStrategy.worktreePath
          : null;
      if (input.workspaceStrategy.type === "worktree") {
        if (runId !== null) {
          yield* threads
            .dispatch({
              type: "prepared-run.progress",
              commandId: CommandId.make(`${input.commandId}:progress:worktree`),
              threadId,
              runId,
              phase: "worktree",
            })
            .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
        }
        let startRef = input.workspaceStrategy.baseRef;
        let baseRefName = input.workspaceStrategy.baseRef;
        const remote =
          input.workspaceStrategy.startFromOrigin === true
            ? yield* git
                .resolveWorktreeBaseRemote({
                  cwd: project.workspaceRoot,
                  refName: input.workspaceStrategy.baseRef,
                })
                .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)))
            : null;
        const shouldFetch = remote !== null;
        yield* setupTracker.stageStatus(threadId, "fetch", shouldFetch ? "running" : "skipped");
        if (remote !== null) {
          const remoteBase = yield* git
            .fetchWorktreeBaseCommit({
              cwd: project.workspaceRoot,
              refName: input.workspaceStrategy.baseRef,
              remote,
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
          if (remoteBase !== null) {
            startRef = remoteBase.commitSha;
            baseRefName = `refs/remotes/${remoteBase.remoteRefName}`;
          }
        }
        if (shouldFetch) yield* setupTracker.stageStatus(threadId, "fetch", "done");
        if (
          branch !== null &&
          isTemporaryWorktreeBranch(branch) &&
          (yield* git
            .hasCommit({
              cwd: project.workspaceRoot,
              refName: `refs/heads/${WORKTREE_BRANCH_PREFIX}`,
            })
            .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId))))
        ) {
          branch = flattenTemporaryWorktreeBranchName(branch);
        }
        yield* setupTracker.stageStatus(threadId, "checkout", "running");
        const worktree = yield* git
          .createWorktree(
            {
              cwd: project.workspaceRoot,
              refName: startRef,
              newRefName: branch!,
              baseRefName,
              path: null,
            },
            {
              progress: {
                onWorktreeClaimed: (path) =>
                  Effect.sync(() => {
                    createdWorktreePath = path;
                    createdBranch = branch;
                  }),
                onCheckoutProgress: (progress) =>
                  setupTracker.stage(threadId, "checkout", { percent: progress.percent }),
              },
            },
          )
          .pipe(Effect.mapError(mapError(input, "provision-worktree", threadId)));
        worktreePath = worktree.worktree.path;
        branch = worktree.worktree.refName;
        createdWorktreePath = worktreePath;
        createdBranch = branch;
        yield* setupTracker.update(threadId, (snapshot) => ({ ...snapshot, worktreePath, branch }));
        yield* setupTracker.stageStatus(threadId, "checkout", "done");
      }

      // Do not rewrite reused worktrees; that could undo the original branch rename.
      if (reused === undefined) {
        yield* threads
          .dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make(`${input.commandId}:workspace`),
            threadId,
            branch,
            worktreePath,
          })
          .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
      }
      workspaceRecorded = true;

      if (
        reused === undefined &&
        worktreePath !== null &&
        branch !== null &&
        initialMessage !== undefined &&
        isTemporaryWorktreeBranch(branch)
      ) {
        const oldBranch = branch;
        const worktreeCwd = worktreePath;
        branchRename = yield* generateBranchNameFor(worktreeCwd, initialMessage).pipe(
          Effect.flatMap(({ branch: newBranch, exactName }) =>
            git
              .renameBranch({
                cwd: worktreeCwd,
                oldBranch,
                newBranch,
                ...(exactName ? { exactName: true } : {}),
              })
              .pipe(
                // A cancel must see the renamed branch, or its cleanup deletes the old name.
                Effect.tap((renamed) =>
                  Effect.sync(() => {
                    createdBranch = renamed.branch;
                  }),
                ),
                Effect.uninterruptible,
              ),
          ),
          Effect.flatMap((renamed) =>
            Effect.gen(function* () {
              yield* threads
                .dispatch({
                  type: "thread.metadata.update",
                  commandId: CommandId.make(`${input.commandId}:branch-rename`),
                  threadId,
                  branch: renamed.branch,
                  worktreePath: worktreeCwd,
                  expectedBranch: oldBranch,
                  expectedWorktreePath: worktreeCwd,
                })
                .pipe(
                  Effect.catchCause((cause) =>
                    Effect.logWarning("Parent thread worktree branch update failed", {
                      threadId,
                      cause,
                    }),
                  ),
                );
              // Update parents first so children inherit names; reread each pass for newly updated descendants.
              const visited = new Set([threadId]);
              while (true) {
                const snapshot = yield* threads.getShellSnapshot({ worktreePath: worktreeCwd });
                const sharing = [...snapshot.threads, ...snapshot.archivedThreads].filter(
                  (thread) => !visited.has(thread.id) && thread.branch === oldBranch,
                );
                if (sharing.length === 0) break;
                for (const thread of sharing) {
                  visited.add(thread.id);
                  yield* threads
                    .dispatch({
                      type: "thread.metadata.update",
                      commandId: CommandId.make(`${input.commandId}:branch-rename:${thread.id}`),
                      threadId: thread.id,
                      branch: renamed.branch,
                      expectedBranch: oldBranch,
                      expectedWorktreePath: worktreeCwd,
                    })
                    .pipe(
                      Effect.catchCause((cause) =>
                        Effect.logWarning("Shared thread worktree branch update failed", {
                          threadId: thread.id,
                          cause,
                        }),
                      ),
                    );
                }
              }
            }),
          ),
          Effect.catchCause((cause) =>
            Effect.logWarning("Thread worktree branch rename failed", {
              commandId: input.commandId,
              threadId,
              oldBranch,
              cause,
            }),
          ),
          Effect.forkIn(preparationScope),
        );
      }

      if (runId !== null && worktreePath !== null) {
        yield* threads
          .dispatch({
            type: "prepared-run.progress",
            commandId: CommandId.make(`${input.commandId}:progress:setup`),
            threadId,
            runId,
            phase: "setup",
          })
          .pipe(Effect.mapError(mapError(input, "update-thread", threadId)));
      }
      yield* setupTracker.stageStatus(threadId, "setup-script", "running");
      const setup =
        worktreePath === null
          ? ({ status: "no-script" } as const)
          : yield* setupScripts
              .runForThread({
                threadId,
                projectId: input.projectId,
                projectCwd: project.workspaceRoot,
                worktreePath,
                ...(tracked
                  ? {
                      observeCompletion: {
                        onOutputLine: (line: string) =>
                          setupTracker.appendTail(threadId, "setup-script", line),
                      },
                    }
                  : {}),
                project: {
                  id: project.id,
                  workspaceRoot: project.workspaceRoot,
                  scripts: project.scripts,
                },
              })
              .pipe(Effect.mapError(mapError(input, "run-setup-script", threadId)));

      let awaitAsyncSetup = Effect.void;
      if (setup.status === "started") {
        setupTerminalId = setup.terminalId;
        yield* setupTracker.update(threadId, (snapshot) => ({
          ...snapshot,
          setupScript: {
            name: setup.scriptName,
            command: setup.scriptCommand,
            terminalId: setup.terminalId,
          },
        }));
        if (setup.completion) {
          const awaitCompletion = Effect.gen(function* () {
            const completion = yield* setup.completion!;
            yield* setupTracker.stage(threadId, "setup-script", {
              status: completion.exitCode === 0 ? "done" : "failed",
              detail: `exited with ${completion.exitCode ?? "no exit code"}`,
            });
            if (completion.exitCode !== 0 && !setup.async)
              return yield* mapError(
                input,
                "run-setup-script",
                threadId,
              )(`Setup script exited with ${completion.exitCode ?? "no exit code"}.`);
          });
          if (setup.async) {
            awaitAsyncSetup = awaitCompletion.pipe(
              Effect.catchCause((cause) =>
                setupTracker.stage(threadId, "setup-script", {
                  status: "failed",
                  detail: failureDetail(Cause.squash(cause)),
                }),
              ),
            );
          } else {
            yield* awaitCompletion;
          }
        } else {
          yield* setupTracker.stageStatus(threadId, "setup-script", "done");
        }
      } else {
        yield* setupTracker.stageStatus(threadId, "setup-script", "skipped");
      }
      yield* setupTracker.markUncancellable(threadId);
      yield* setupTracker.stageStatus(threadId, "agent", "running");
      if (runId !== null) {
        yield* threads
          .dispatch({
            type: "prepared-run.release",
            commandId: CommandId.make(`${input.commandId}:release`),
            threadId,
            runId,
          })
          .pipe(
            Effect.mapError(mapError(input, "release-run", threadId)),
            Effect.tap(() =>
              Effect.sync(() => {
                runReleased = true;
              }),
            ),
            Effect.uninterruptible,
          );
      }
      yield* setupTracker.stageStatus(threadId, "agent", "done");
      yield* awaitAsyncSetup;
      yield* setupTracker.finish(threadId, "done");
    }).pipe(
      Effect.onError((cause) =>
        Effect.gen(function* () {
          const cancelled = Cause.hasInterruptsOnly(cause);
          yield* setupTracker.finish(
            threadId,
            cancelled ? "cancelled" : "failed",
            cancelled ? null : failureDetail(Cause.squash(cause)),
          );
          if (cancelled && setupTerminalId)
            yield* terminals
              .close({ threadId, terminalId: setupTerminalId, deleteHistory: true })
              .pipe(Effect.ignore);
          if (
            tracked &&
            createdWorktreePath &&
            ((cancelled && !runReleased) || !workspaceRecorded)
          ) {
            if (branchRename !== null) yield* Fiber.interrupt(branchRename);
            const removedPath = createdWorktreePath;
            const removedBranch = createdBranch;
            // Forget worktrees only after removal succeeds; failed removals must not reuse partially written checkouts.
            yield* git
              .removeWorktree({ cwd: project.workspaceRoot, path: removedPath, force: true })
              .pipe(
                Effect.andThen(
                  removedBranch === null
                    ? Effect.void
                    : git
                        .deleteLocalBranch({
                          cwd: project.workspaceRoot,
                          refName: removedBranch,
                          force: true,
                        })
                        .pipe(Effect.ignoreCause({ log: true })),
                ),
                Effect.andThen(
                  threads
                    .dispatch({
                      type: "thread.metadata.update",
                      commandId: CommandId.make(`${input.commandId}:cancel-workspace`),
                      threadId,
                      worktreePath: null,
                      branch: null,
                    })
                    .pipe(Effect.ignore),
                ),
                Effect.catchCause((removeCause) =>
                  Effect.logWarning("Failed to remove an abandoned thread worktree", {
                    commandId: input.commandId,
                    threadId,
                    path: removedPath,
                    cause: removeCause,
                  }),
                ),
              );
          }
        }),
      ),
      Effect.onExit((exit) => {
        const rename = branchRename;
        if (rename === null) return Effect.void;
        if (Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause))
          return Fiber.interrupt(rename).pipe(Effect.asVoid);
        return Fiber.await(rename).pipe(
          Effect.asVoid,
          Effect.interruptible,
          Effect.onInterrupt(() => Fiber.interrupt(rename).pipe(Effect.asVoid)),
        );
      }),
    );
  });

  const failPreparedRun = (
    input: Pick<PreparationInput, "commandId">,
    threadId: ThreadId,
    runId: RunId | null,
    cause: unknown,
  ) =>
    runId === null
      ? Effect.logWarning("Thread workspace preparation failed", {
          commandId: input.commandId,
          threadId,
          cause,
        })
      : threads
          .dispatch({
            type: "prepared-run.fail",
            commandId: CommandId.make(`${input.commandId}:fail`),
            threadId,
            runId,
            failure: makeProviderFailure({
              cause,
              message: failureDetail(cause),
              class: "validation_error",
              retryable: false,
            }),
          })
          .pipe(
            Effect.catchCause((persistCause) =>
              Effect.logWarning("Failed to persist thread workspace preparation failure", {
                commandId: input.commandId,
                threadId,
                cause,
                persistCause,
              }),
            ),
          );

  const reservePreparation = (commandId: CommandId) =>
    Ref.modify(scheduledLaunches, (scheduled) => {
      if (scheduled.has(commandId)) return [false, scheduled] as const;
      const next = new Set(scheduled);
      next.add(commandId);
      return [true, next] as const;
    });

  const releasePreparation = (commandId: CommandId) =>
    Ref.update(scheduledLaunches, (scheduled) => {
      const next = new Set(scheduled);
      next.delete(commandId);
      return next;
    });

  const schedulePreparation = Effect.fn("ThreadLaunchService.schedulePreparation")(function* (
    input: PreparationInput,
    threadId: ThreadId,
    runId: RunId | null,
    ownedJob: OwnedPreparationJob,
  ) {
    const guardedPreparation = Effect.gen(function* () {
      const preflight = yield* Effect.gen(function* () {
        if (
          runId === null ||
          (input.workspaceStrategy.type !== "worktree" && input.reusedWorktree === undefined)
        )
          return { kind: "ready" as const, interruption: null };
        const afterSequence = yield* threads.getThreadEventSequence(threadId);
        const projection = yield* threads.getThreadRecords(threadId, ["runs"]);
        if (projection.runs.find((run) => run.id === runId)?.status !== "preparing") {
          return { kind: "skip" as const };
        }
        const interruption = threads
          .streamStoredEventsFrom({ threadId, afterSequence, eventType: "run.updated" })
          .pipe(
            Stream.filter(
              (stored) =>
                stored.event.type === "run.updated" &&
                stored.event.payload.id === runId &&
                (stored.event.payload.status === "interrupted" ||
                  stored.event.payload.status === "cancelled"),
            ),
            Stream.runHead,
            Effect.flatMap((event) => (Option.isSome(event) ? Effect.interrupt : Effect.never)),
          );
        return { kind: "ready" as const, interruption };
      }).pipe(
        Effect.onError((cause) =>
          Effect.gen(function* () {
            const custody = yield* Effect.result(requireOrdinaryThread(threadId));
            if (Result.isFailure(custody)) return;
            yield* failPreparedRun(
              input,
              threadId,
              runId,
              Cause.hasInterruptsOnly(cause) ? "Worktree setup cancelled." : Cause.squash(cause),
            );
          }),
        ),
        Effect.exit,
      );
      if (Exit.isFailure(preflight)) return;
      if (preflight.value.kind === "skip") return;
      const custody = yield* Effect.result(requireOrdinaryThread(threadId));
      if (Result.isFailure(custody)) return;
      const prepare = prepareInBackground(input, threadId, runId);
      yield* (
        preflight.value.interruption === null
          ? prepare
          : Effect.raceFirst(prepare, preflight.value.interruption)
      ).pipe(
        Effect.onError((cause) =>
          failPreparedRun(
            input,
            threadId,
            runId,
            Cause.hasInterruptsOnly(cause) ? "Worktree setup cancelled." : Cause.squash(cause),
          ),
        ),
        Effect.ignoreCause,
      );
    });
    yield* guardedPreparation.pipe(
      Effect.ensuring(releasePreparation(input.commandId)),
      Effect.ensuring(finishPreparationJob(ownedJob)),
      Effect.forkIn(preparationScope),
    );
  });

  const replayLaunch = Effect.fn("ThreadLaunchService.replayLaunch")(function* (
    input: ThreadLaunchInput,
    receipt: CommandReceiptStore.CommandReceiptV2,
  ) {
    const expected =
      input.reuseExistingThread === true ? "thread.metadata.update" : "thread.create";
    if (
      receipt.status !== "accepted" ||
      receipt.commandType !== expected ||
      (input.threadId !== undefined && input.threadId !== receipt.threadId)
    )
      return yield* mapError(
        input,
        "create-thread",
        input.threadId,
      )("Command cannot be replayed for this thread.");
    const threadId = receipt.threadId;
    const shell = yield* threads
      .getThreadShell(threadId)
      .pipe(Effect.mapError(mapError(input, "create-thread", threadId)));
    if (shell === null || shell.projectId !== input.projectId)
      return yield* mapError(
        input,
        "resolve-project",
        threadId,
      )("Project identity changed or thread no longer exists.");
    let sequence: number | undefined;
    if (input.initialMessage !== undefined) {
      const message = yield* readReceipt(
        input,
        CommandId.make(`${input.commandId}:initial-message`),
      );
      if (
        Option.isNone(message) ||
        message.value.status !== "accepted" ||
        message.value.commandType !== "message.dispatch" ||
        message.value.threadId !== threadId
      )
        return yield* mapError(
          input,
          "dispatch-message",
          threadId,
        )(
          "Launch is incomplete; its old command cannot start more work. Use a fresh command after reconciliation.",
        );
      sequence = message.value.resultSequence;
    }
    const projection = yield* threads
      .getThreadProjection(threadId)
      .pipe(Effect.mapError(mapError(input, "create-thread", threadId)));
    return { threadId, projection, resumed: true, ...(sequence === undefined ? {} : { sequence }) };
  });
  const performLaunch = Effect.fn("ThreadLaunchService.performLaunch")(function* (
    request: ThreadLaunchInput,
  ) {
    const launchReceipt = yield* readReceipt(request, request.commandId);
    if (Option.isSome(launchReceipt)) return yield* replayLaunch(request, launchReceipt.value);
    const candidateThreadId =
      request.threadId ??
      (yield* ids.allocate
        .thread({ projectId: request.projectId })
        .pipe(Effect.mapError(mapError(request, "create-thread"))));
    let handedOff = false;
    return yield* Effect.acquireUseRelease(
      acquirePreparationJob(
        "thread-launch",
        request,
        candidateThreadId,
        null,
        request,
        request.importedNativeThread === undefined
          ? null
          : {
              driver: request.importedNativeThread.ref.driver,
              providerInstanceId: request.modelSelection.instanceId,
              nativeId: request.importedNativeThread.ref.nativeId,
            },
      ).pipe(
        Effect.mapError(mapError(request, "update-thread", candidateThreadId)),
        Effect.flatMap((result) =>
          result.kind === "acquired"
            ? Effect.succeed(result.job)
            : Effect.fail(
                mapError(
                  request,
                  result.kind === "denied" ? "update-thread" : "create-thread",
                  candidateThreadId,
                )(
                  result.kind === "denied"
                    ? "Thread has workflow or planning custody."
                    : "Command cannot be replayed to start preparation.",
                ),
              ),
        ),
      ),
      (ownedJob) =>
        Effect.gen(function* () {
          const input: ThreadLaunchInput =
            request.workspaceStrategy.type === "worktree" &&
            (yield* managedFolders.isScratchProject(request.projectId))
              ? { ...request, workspaceStrategy: { type: "root" } }
              : request;
          const project = yield* projects.getById(input.projectId).pipe(
            Effect.mapError(mapError(input, "resolve-project")),
            Effect.flatMap(
              Option.match({
                onNone: () => Effect.fail(mapError(input, "resolve-project")("Project not found.")),
                onSome: Effect.succeed,
              }),
            ),
          );
          yield* requireOrdinaryThread(candidateThreadId).pipe(
            Effect.mapError(mapError(input, "update-thread", candidateThreadId)),
          );

          if (input.reuseExistingThread === true && Option.isNone(launchReceipt)) {
            yield* validateReusableThread(input, candidateThreadId);
          }

          const workspaceStrategy: ThreadLaunchWorkspaceStrategy =
            input.workspaceStrategy.type === "root" && Option.isNone(launchReceipt)
              ? Option.match(
                  yield* managedFolders
                    .folderForThread({
                      projectId: input.projectId,
                      threadId: candidateThreadId,
                      text: input.initialMessage?.text ?? input.title,
                    })
                    .pipe(
                      Effect.mapError(mapError(input, "provision-worktree", candidateThreadId)),
                    ),
                  {
                    onNone: () => input.workspaceStrategy,
                    onSome: (worktreePath) => ({ type: "existing_worktree", worktreePath }),
                  },
                )
              : input.workspaceStrategy;
          const claimDispatch = threads.dispatch(
            buildClaimCommand(input, candidateThreadId, workspaceStrategy),
          );
          const claimed = yield* claimDispatch.pipe(
            Effect.mapError(
              mapError(
                input,
                input.reuseExistingThread === true ? "update-thread" : "create-thread",
                candidateThreadId,
              ),
            ),
          );
          const threadId =
            claimed.storedEvents.find((stored) => stored.event.type.startsWith("thread."))?.event
              .threadId ?? candidateThreadId;
          if (project.id !== input.projectId) {
            return yield* mapError(input, "resolve-project", threadId)("Project identity changed.");
          }
          let runId: RunId | null = null;
          let messageSequence: number | undefined;
          let messageWasAlreadyAccepted = false;
          if (input.initialMessage !== undefined) {
            const messageCommandId = CommandId.make(`${input.commandId}:initial-message`);
            const messageReceipt = yield* readReceipt(input, messageCommandId);
            messageWasAlreadyAccepted = Option.isSome(messageReceipt);
            const messageId =
              input.initialMessage.messageId ??
              (yield* ids.allocate
                .message({ threadId, ordinal: 1 })
                .pipe(Effect.mapError(mapError(input, "dispatch-message", threadId))));
            const dispatched = yield* threads
              .dispatch(
                buildInitialMessageCommand(
                  input,
                  input.initialMessage,
                  messageCommandId,
                  threadId,
                  messageId,
                  workspaceStrategy,
                ),
              )
              .pipe(
                // An agent's launch can land on a thread that a replay, or a concurrent
                // launch with the same key, created with broader modes. The orchestrator
                // refuses the message under the thread's lock; the user's own clients
                // launch into their own drafts, whatever modes those hold.
                input.createdBy === "agent"
                  ? Effect.provideService(DispatchModeLimit, {
                      runtimeMode: input.runtimeMode,
                      interactionMode: input.interactionMode,
                    })
                  : (effect) => effect,
                Effect.mapError(mapError(input, "dispatch-message", threadId)),
              );
            // A concurrent retry can read no receipt and still replay the dispatch.
            messageWasAlreadyAccepted ||= dispatched.replayed === true;
            messageSequence = dispatched.sequence;
            const runCreated = dispatched.storedEvents.find(
              (stored) => stored.event.type === "run.created",
            );
            runId = runCreated?.event.type === "run.created" ? runCreated.event.payload.id : null;
            if (runId === null) {
              return yield* mapError(
                input,
                "dispatch-message",
                threadId,
              )("Initial message was accepted without a durable run.");
            }
          }

          const projection = yield* threads
            .getThreadProjection(threadId)
            .pipe(Effect.mapError(mapError(input, "create-thread", threadId)));
          const runIsPreparing =
            runId !== null &&
            projection.runs.some((run) => run.id === runId && run.status === "preparing");
          const shouldSchedule = runId === null ? Option.isNone(launchReceipt) : runIsPreparing;
          const boundWorktreePath = projection.thread.worktreePath;
          const preparationStrategy: ThreadLaunchWorkspaceStrategy =
            Option.isSome(launchReceipt) &&
            workspaceStrategy.type === "root" &&
            boundWorktreePath !== null
              ? {
                  type: "existing_worktree",
                  worktreePath: boundWorktreePath,
                  branch: workspaceStrategy.branch,
                }
              : workspaceStrategy;
          if (shouldSchedule) {
            const ownsPreparation = yield* reservePreparation(input.commandId);
            if (ownsPreparation) {
              yield* Effect.gen(function* () {
                const preparationStillRequired =
                  runId === null
                    ? true
                    : yield* threads.getThreadRecords(threadId, ["runs"], { runIds: [runId] }).pipe(
                        Effect.map((current) =>
                          current.runs.some(
                            (run) => run.id === runId && run.status === "preparing",
                          ),
                        ),
                        Effect.mapError(mapError(input, "update-thread", threadId)),
                      );
                if (preparationStillRequired) {
                  yield* requireOrdinaryThread(threadId).pipe(
                    Effect.mapError(mapError(input, "update-thread", threadId)),
                  );
                  yield* schedulePreparation(
                    { ...input, workspaceStrategy: preparationStrategy },
                    threadId,
                    runId,
                    ownedJob,
                  ).pipe(
                    Effect.tap(() =>
                      Effect.sync(() => {
                        handedOff = true;
                      }),
                    ),
                    Effect.uninterruptible,
                  );
                } else {
                  yield* releasePreparation(input.commandId);
                }
              }).pipe(Effect.onError(() => releasePreparation(input.commandId)));
            }
          }

          return {
            threadId,
            projection,
            // An attempt that failed after claiming the thread recorded nothing, so
            // only a repeat of the final write counts as resumed.
            resumed:
              input.initialMessage === undefined
                ? Option.isSome(launchReceipt) || claimed.replayed === true
                : messageWasAlreadyAccepted,
            ...(messageSequence === undefined ? {} : { sequence: messageSequence }),
          };
        }),
      (ownedJob) => (handedOff ? Effect.void : finishPreparationJob(ownedJob)),
    );
  });
  const launchOwned = Effect.fn("ThreadLaunchService.launch")(function* (input: ThreadLaunchInput) {
    const hash = WorkflowCanonicalHash.tagged("t3.ordinary-launch-request.v1", input);
    const waitFor = Effect.fn("ThreadLaunchService.awaitLaunchOwner")(
      (entry: { hash: string; result: Deferred.Deferred<ThreadLaunchResult, LaunchFailure> }) =>
        entry.hash !== hash
          ? Effect.fail(
              mapError(
                input,
                "create-thread",
                input.threadId,
              )("Command cannot be replayed with different launch input."),
            )
          : Deferred.await(entry.result).pipe(
              Effect.map((result) => ({ ...result, resumed: true })),
              Effect.interruptible,
            ),
    );
    const earlier = launching.get(input.commandId);
    if (earlier) return yield* waitFor(earlier);
    yield* ProjectCloneTracker.rejectCommandsDuringClone(cloneTracker, {
      type: "thread.create",
      projectId: input.projectId,
    }).pipe(Effect.mapError(mapError(input, "resolve-project")));
    const project = yield* projects
      .getById(input.projectId)
      .pipe(Effect.mapError(mapError(input, "resolve-project")));
    if (Option.isNone(project))
      return yield* mapError(input, "resolve-project")("Project not found.");
    if (input.reuseExistingThread === true && input.threadId === undefined)
      return yield* mapError(
        input,
        "update-thread",
      )("Reusing an existing thread requires a thread id.");
    const receipt = yield* readReceipt(input, input.commandId);
    const currentOwner = launching.get(input.commandId);
    if (currentOwner) return yield* waitFor(currentOwner);
    if (Option.isSome(receipt)) return yield* replayLaunch(input, receipt.value);
    yield* threads.assertProviderInstanceEnabled({
      projectId: input.projectId,
      commandId: input.commandId,
      instanceId: input.modelSelection.instanceId,
    });
    if (input.reuseExistingThread === true) {
      const valid = yield* Effect.exit(validateReusableThread(input, input.threadId!));
      if (Exit.isFailure(valid)) {
        const current = launching.get(input.commandId);
        if (current) return yield* waitFor(current);
        const committed = yield* readReceipt(input, input.commandId);
        if (Option.isSome(committed)) return yield* replayLaunch(input, committed.value);
        return yield* Effect.failCause(valid.cause);
      }
    }
    const result = yield* Deferred.make<ThreadLaunchResult, LaunchFailure>();
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const selected = yield* Effect.sync(() => {
          const current = launching.get(input.commandId);
          if (current) return { owner: false as const, entry: current };
          const entry = { hash, result };
          launching.set(input.commandId, entry);
          return { owner: true as const, entry };
        });
        if (!selected.owner) return yield* restore(waitFor(selected.entry));
        return yield* restore(performLaunch(input)).pipe(
          Effect.onExit((exit) =>
            Deferred.done(result, exit).pipe(
              Effect.andThen(
                Effect.sync(() => {
                  if (launching.get(input.commandId) === selected.entry)
                    launching.delete(input.commandId);
                }),
              ),
            ),
          ),
        );
      }),
    );
  });
  const launch: ThreadLaunchService["Service"]["launch"] = (request) => {
    try {
      return launchOwned(ownOrdinaryInput(request));
    } catch {
      return Effect.fail(
        new ThreadLaunchError({
          operation: "create-thread",
          commandId: CommandId.make("invalid-launch"),
          projectId: ProjectId.make("invalid-project"),
          cause: "Invalid launch input.",
        }),
      );
    }
  };

  const preparationTarget = Effect.fn("ThreadLaunchService.preparationTarget")(function* (
    input: ThreadLaunchPreparationInput,
  ) {
    const { thread, runs } = yield* threads.getThreadRecords(input.threadId, ["runs"], {
      runIds: [input.runId],
    });
    const run = runs.find((candidate) => candidate.id === input.runId);
    if (run === undefined || run.workspacePreparation === undefined) return null;
    return { thread, run, workspace: run.workspacePreparation };
  });
  const scheduleOwnedPreparation = Effect.fn("ThreadLaunchService.scheduleOwnedPreparation")(
    function* (input: ThreadLaunchPreparationInput, ownedJob: OwnedPreparationJob) {
      const target = yield* preparationTarget(input);
      if (target === null || target.run.status !== "preparing") return false;
      if (!(yield* reservePreparation(input.commandId))) return false;
      yield* scheduleRunPreparation(
        input,
        target.thread,
        target.run,
        target.workspace,
        ownedJob,
      ).pipe(Effect.onError(() => releasePreparation(input.commandId)));
      return true;
    },
  );
  const prepareRunOwnedInput = Effect.fn("ThreadLaunchService.prepareRun")(
    function* (input: ThreadLaunchPreparationInput) {
      const custody = yield* Effect.result(requireOrdinaryThread(input.threadId));
      if (Result.isFailure(custody)) return;
      const target = yield* preparationTarget(input);
      if (target === null || target.run.status !== "preparing") return;
      const request = ownOrdinaryInput({ input, workspace: target.workspace });
      const preparationInput = {
        commandId: input.commandId,
        projectId: target.thread.projectId,
        workspaceStrategy: target.workspace,
      };
      let handedOff = false;
      yield* Effect.acquireUseRelease(
        acquirePreparationJob(
          "thread-prepare",
          preparationInput,
          input.threadId,
          input.runId,
          request,
        ),
        (result) =>
          result.kind !== "acquired"
            ? Effect.void
            : scheduleOwnedPreparation(input, result.job).pipe(
                Effect.tap((scheduled) =>
                  Effect.sync(() => {
                    handedOff = scheduled;
                  }),
                ),
                Effect.uninterruptible,
                Effect.catchCause((cause) =>
                  failPreparedRun(input, input.threadId, input.runId, Cause.squash(cause)),
                ),
              ),
        (result) =>
          result.kind === "acquired" && !handedOff ? finishPreparationJob(result.job) : Effect.void,
      );
    },
    Effect.catchCause((cause) =>
      Effect.logWarning("Preparation entry was refused or unavailable", { cause }),
    ),
  );
  const prepareRun: ThreadLaunchService["Service"]["prepareRun"] = (request) => {
    try {
      return prepareRunOwnedInput(ownOrdinaryInput(request));
    } catch {
      return Effect.logWarning("Invalid preparation input");
    }
  };
  const retryOwnedInput = Effect.fn("ThreadLaunchService.retryPreparation")(function* (
    input: ThreadLaunchPreparationInput,
  ) {
    const dispatchError = (cause: unknown) =>
      new Orchestrator.OrchestratorDispatchError({
        commandId: input.commandId,
        commandType: "prepared-run.retry",
        cause,
      });
    const receipt = yield* receipts
      .getByCommandId(input.commandId)
      .pipe(Effect.mapError(dispatchError));
    if (Option.isSome(receipt)) {
      if (
        receipt.value.status !== "accepted" ||
        receipt.value.commandType !== "prepared-run.retry" ||
        receipt.value.threadId !== input.threadId
      )
        return yield* dispatchError("Command cannot be replayed for this preparation.");
      return { sequence: receipt.value.resultSequence, storedEvents: [], replayed: true as const };
    }
    yield* requireOrdinaryThread(input.threadId).pipe(Effect.mapError(dispatchError));
    const target = yield* preparationTarget(input);
    if (target === null) return yield* dispatchError("Run has no workspace preparation.");
    const preparationInput = {
      commandId: input.commandId,
      projectId: target.thread.projectId,
      workspaceStrategy: target.workspace,
    };
    const request = ownOrdinaryInput({ input, workspace: target.workspace });
    let handedOff = false;
    return yield* Effect.acquireUseRelease(
      acquirePreparationJob(
        "thread-prepare",
        preparationInput,
        input.threadId,
        input.runId,
        request,
      ).pipe(
        Effect.mapError(dispatchError),
        Effect.flatMap((result) =>
          result.kind === "acquired"
            ? Effect.succeed(result.job)
            : Effect.fail(
                dispatchError(
                  result.kind === "denied"
                    ? "Thread has workflow or planning custody."
                    : "Preparation command already has retained custody.",
                ),
              ),
        ),
      ),
      (ownedJob) =>
        Effect.gen(function* () {
          const dispatched = yield* threads.dispatch({
            type: "prepared-run.retry",
            commandId: input.commandId,
            threadId: input.threadId,
            runId: input.runId,
          });
          yield* scheduleOwnedPreparation(input, ownedJob).pipe(
            Effect.tap((scheduled) =>
              Effect.sync(() => {
                handedOff = scheduled;
              }),
            ),
            Effect.uninterruptible,
          );
          return dispatched;
        }),
      (ownedJob) => (handedOff ? Effect.void : finishPreparationJob(ownedJob)),
    );
  });
  const retryPreparation: ThreadLaunchService["Service"]["retryPreparation"] = (request) => {
    try {
      return retryOwnedInput(ownOrdinaryInput(request));
    } catch {
      return Effect.fail(
        new Orchestrator.OrchestratorDispatchError({
          commandId: CommandId.make("invalid-preparation"),
          commandType: "prepared-run.retry",
          cause: "Invalid preparation input.",
        }),
      );
    }
  };

  const scheduleRunPreparation = Effect.fn("ThreadLaunchService.scheduleRunPreparation")(function* (
    input: ThreadLaunchPreparationInput,
    thread: OrchestrationV2ThreadProjection["thread"],
    run: OrchestrationV2ThreadProjection["runs"][number],
    workspacePreparation: ThreadLaunchWorkspaceStrategy,
    ownedJob: OwnedPreparationJob,
  ) {
    const { messages } = yield* threads.getThreadRecords(input.threadId, ["messages"], {
      messageIds: [run.userMessageId],
    });
    const message = messages.find((candidate) => candidate.id === run.userMessageId);
    const reuse =
      workspacePreparation.type === "worktree" &&
      thread.worktreePath !== null &&
      thread.branch !== null
        ? {
            strategy: {
              type: "existing_worktree" as const,
              worktreePath: thread.worktreePath,
              branch: thread.branch,
            },
            reusedWorktree: { baseRef: workspacePreparation.baseRef },
          }
        : null;
    return yield* schedulePreparation(
      {
        commandId: input.commandId,
        projectId: thread.projectId,
        workspaceStrategy: reuse?.strategy ?? workspacePreparation,
        ...(reuse === null ? {} : { reusedWorktree: reuse.reusedWorktree }),
        ...(message === undefined
          ? {}
          : {
              initialMessage: {
                text: message.text,
                attachments: message.attachments,
                ...(message.context ? { context: message.context } : {}),
              },
            }),
      },
      input.threadId,
      run.id,
      ownedJob,
    );
  });

  return ThreadLaunchService.of({ launch, checkWorktreeBase, retryPreparation, prepareRun });
});

export const layer = Layer.effect(ThreadLaunchService, make);

export class NativeWorkspacePreparationError extends Schema.TaggedError<NativeWorkspacePreparationError>()(
  "NativeWorkspacePreparationError",
  {
    reason: Schema.Literals(["invalid", "boundary", "profile", "stale", "workspace", "closed"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}
const isNativeWorkspaceError = Schema.is(NativeWorkspacePreparationError);
const nativeWorkspaceError = (reason: NativeWorkspacePreparationError["reason"]) =>
  new NativeWorkspacePreparationError({ reason });
const makeNativeWorkspacePreparationOwned = Effect.fn(
  "ThreadLaunchService.makeNativeWorkspacePreparation",
)(function* (coordinator: Parameters<typeof WorkflowNativeLaunch.make>[0]) {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* WorkflowRunStore.WorkflowRunStore;
  const producer = yield* WorkflowNativeLaunch.make(coordinator);
  const core = yield* GitVcsDriver.GitVcsDriver;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projects = yield* ProjectStore.make;
  const settings = yield* ServerSettings.ServerSettingsService;
  const environment = Object.freeze({ ...(yield* HostProcess.Environment) });
  const cwd = yield* HostProcess.WorkingDirectory;
  const home = yield* HostProcess.HomeDirectory;
  const platform = yield* HostProcess.Platform;
  const path = yield* Path.Path;
  const serviceScope = yield* Effect.scope;
  const entryScope = yield* Scope.fork(serviceScope, "sequential");
  type Entry = { fiber?: Fiber.Fiber<unknown, unknown>; owner?: PreparationOwner.Owner };
  const entries = new Set<Entry>();
  let closed = false;
  const stopEntry = Effect.fn("ThreadLaunchService.stopNativeWorkspaceEntry")(function* (
    entry: Entry,
  ) {
    if (entry.fiber) {
      entry.fiber.interruptUnsafe();
      yield* Fiber.await(entry.fiber);
    }
    if (entry.owner) yield* entry.owner.stop().pipe(Effect.ignore);
  });
  yield* Scope.addFinalizer(
    serviceScope,
    Effect.gen(function* () {
      closed = true;
      yield* Effect.forEach(entries, stopEntry, { concurrency: "unbounded", discard: true });
    }),
  );
  const create = Effect.fn("ThreadLaunchService.createReservedWorkspace")(function* (
    claim: ReturnType<
      Effect.Success<ReturnType<typeof WorkflowNativeLaunch.make>>["claimReservedIntent"]
    >,
  ) {
    if (Option.isSome(yield* Effect.serviceOption(sql.transactionService)))
      return yield* nativeWorkspaceError("boundary");
    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        if (closed || serviceScope.state._tag === "Closed")
          return yield* nativeWorkspaceError("closed");
        const entry: Entry = {};
        entries.add(entry);
        const body = Effect.gen(function* () {
          const selected = yield* claim;
          const owner = selected.preparation.owner;
          entry.owner = owner;
          yield* owner.captureWorkspaceEntry();
          const job = selected.preparation.job;
          const decoded = yield* decodeNativeReservationLaunch(job.input.request.launch);
          const launch = decoded.launch;
          const strategy = launch.workspaceStrategy;
          if (
            !PreparationOwner.isOwner(owner) ||
            owner.jobId !== job.input.jobId ||
            owner.threadId !== selected.native.threadId ||
            owner.runId !== selected.native.runId ||
            decoded.launchHash !== job.input.request.launchHash ||
            platform !== "linux"
          )
            return yield* nativeWorkspaceError("invalid");
          if (
            launch.reuseExistingThread === true ||
            strategy.type !== "worktree" ||
            !strategy.branch ||
            isTemporaryWorktreeBranch(strategy.branch) ||
            strategy.startFromOrigin === true ||
            launch.generateTitle === true ||
            launch.initialMessage.attachments.length > 0
          )
            return yield* nativeWorkspaceError("profile");
          const project = yield* sql
            .withTransaction(
              Effect.gen(function* () {
                const current = yield* store.verifyClaimedNativeOperationInTransaction(
                  selected.executionClaim,
                );
                if (
                  current.kind !== "current" ||
                  current.stage !== "preparing" ||
                  current.threadId !== selected.native.threadId ||
                  current.runId !== selected.native.runId
                )
                  return yield* nativeWorkspaceError("stale");
                const value = yield* projects.get(launch.projectId);
                if (
                  Option.isNone(value) ||
                  !path.isAbsolute(value.value.workspaceRoot) ||
                  job.input.projectId !== launch.projectId
                )
                  return yield* nativeWorkspaceError("stale");
                return value.value;
              }),
            )
            .pipe(Effect.interruptible);
          const currentSettings = yield* settings.getSettings;
          if (
            setupProjectScript(
              resolveProjectScripts(currentSettings, {
                id: project.projectId,
                scripts: project.scripts,
              }),
            )
          )
            return yield* nativeWorkspaceError("profile");
          const result = yield* core
            .createWorktree(
              {
                cwd: project.workspaceRoot,
                refName: strategy.baseRef,
                newRefName: strategy.branch,
                path: null,
              },
              { fastCheckout: false, submodules: "none" },
            )
            .pipe(
              Effect.provideService(PreparationOwner.Current, owner),
              Effect.provideService(HostProcess.Environment, environment),
              Effect.provideService(HostProcess.WorkingDirectory, cwd),
              Effect.provideService(HostProcess.HomeDirectory, home),
              Effect.provideService(HostProcess.Platform, platform),
            );
          yield* orchestrator.dispatch({
            type: "thread.metadata.update",
            commandId: CommandId.make(`${launch.commandId}:native-workspace`),
            threadId: launch.threadId,
            branch: result.worktree.refName,
            worktreePath: result.worktree.path,
            expectedBranch: strategy.branch,
            expectedWorktreePath: null,
          });
          return Object.freeze({
            kind: "worktree-created" as const,
            state: "preparing-held" as const,
            native: selected.native,
            worktree: Object.freeze({ ...result.worktree }),
          });
        }).pipe(Effect.scoped, Effect.interruptible);
        const fiber = yield* Effect.forkIn(body, entryScope);
        entry.fiber = fiber;
        return yield* restore(Fiber.await(fiber)).pipe(
          Effect.onInterrupt(() => stopEntry(entry)),
          Effect.flatMap((exit) =>
            Effect.gen(function* () {
              if (Exit.isFailure(exit)) {
                if (entry.owner) yield* entry.owner.stop().pipe(Effect.ignore);
                return yield* Effect.failCause(exit.cause);
              }
              if (!entry.owner) return yield* nativeWorkspaceError("invalid");
              const inventory = yield* entry.owner.seal();
              return Object.freeze({ ...exit.value, inventory });
            }),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              entries.delete(entry);
            }),
          ),
        );
      }),
    );
  }, Effect.interruptible);
  return Object.freeze({
    createReservedWorkspace(selection: Parameters<typeof producer.claimReservedIntent>[0]) {
      return create(producer.claimReservedIntent(selection)).pipe(
        Effect.mapError((cause) =>
          isNativeWorkspaceError(cause)
            ? cause
            : new NativeWorkspacePreparationError({ reason: "workspace", cause }),
        ),
      );
    },
  });
});
export function makeNativeWorkspacePreparation(
  coordinator: Parameters<typeof WorkflowNativeLaunch.make>[0],
) {
  const owned = Result.try({
    try: () => {
      WorkflowCanonicalHash.assertCanonical(coordinator);
      const value = Object.freeze({ ...coordinator });
      if (
        Object.keys(value).sort().join() !== "bootId,workerId" ||
        ![value.bootId, value.workerId].every(
          (id) => typeof id === "string" && id.trim().length > 0,
        )
      )
        throw nativeWorkspaceError("invalid");
      return value;
    },
    catch: () => nativeWorkspaceError("invalid"),
  });
  return Effect.fromResult(owned).pipe(Effect.flatMap(makeNativeWorkspacePreparationOwned));
}
