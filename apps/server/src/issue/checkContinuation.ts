import {
  isLiveSubagentShell,
  threadIssueKeysEqual,
  type IssueContinuationResult,
  type IssueContinuationSource,
  type OrchestrationV2ThreadShell,
  type OrchestrationV2ThreadShellSnapshot,
  type ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as TaskFolders from "../workspace/TaskFolders.ts";

interface ContinuationLookup<ThreadError, ProjectError, SnapshotError> {
  readonly getThread: (
    id: ThreadId,
  ) => Effect.Effect<OrchestrationV2ThreadShell | null, ThreadError>;
  readonly getProjectRoot: (id: ProjectId) => Effect.Effect<string | null, ProjectError>;
  readonly getWorktreeThreads: (
    worktreePath: string,
  ) => Effect.Effect<OrchestrationV2ThreadShellSnapshot, SnapshotError>;
}

export const issueContinuationCheck = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const taskFolders = yield* TaskFolders.TaskFolders;

  const probe = (cwd: string, args: ReadonlyArray<string>) =>
    git
      .execute({ operation: "IssueContinuation.checkWorkspace", cwd, args, timeoutMs: 5_000 })
      .pipe(Effect.map(({ stdout }) => stdout.replace(/\r?\n$/, "")));

  const samePath = (left: string, right: string) => path.relative(left, right) === "";
  const realDirectory = Effect.fn("IssueContinuation.realDirectory")(function* (target: string) {
    const [real, parent, info] = yield* Effect.all(
      [fs.realPath(target), fs.realPath(path.dirname(target)), fs.stat(target)],
      { concurrency: 3 },
    );
    return info.type === "Directory" && samePath(real, path.join(parent, path.basename(target)))
      ? real
      : null;
  });

  const verifyCheckout = Effect.fn("IssueContinuation.verifyCheckout")(function* (
    repositoryPath: string,
    worktreePath: string,
    branch: string,
  ) {
    const [realRepository, realWorktree, listed] = yield* Effect.all(
      [
        fs.realPath(repositoryPath),
        realDirectory(worktreePath),
        git.listWorktreePaths(repositoryPath),
      ],
      { concurrency: 3 },
    );
    if (
      realWorktree === null ||
      samePath(realWorktree, realRepository) ||
      !listed.some((entry) => samePath(entry, realWorktree))
    ) {
      return null;
    }
    const [repositoryTop, worktreeTop, repositoryMetadata, worktreeMetadata, actualBranch] =
      yield* Effect.all(
        [
          probe(realRepository, ["rev-parse", "--show-toplevel"]),
          probe(realWorktree, ["rev-parse", "--show-toplevel"]),
          probe(realRepository, ["rev-parse", "--git-common-dir"]),
          probe(realWorktree, ["rev-parse", "--git-common-dir"]),
          probe(realWorktree, ["symbolic-ref", "--quiet", "HEAD"]),
        ],
        { concurrency: 5 },
      );
    const [realRepositoryTop, realWorktreeTop, realRepositoryMetadata, realWorktreeMetadata] =
      yield* Effect.all(
        [
          fs.realPath(repositoryTop),
          fs.realPath(worktreeTop),
          fs.realPath(path.resolve(realRepository, repositoryMetadata)),
          fs.realPath(path.resolve(realWorktree, worktreeMetadata)),
        ],
        { concurrency: 4 },
      );
    return !samePath(realWorktree, realRepositoryTop) &&
      samePath(realWorktree, realWorktreeTop) &&
      samePath(realRepositoryMetadata, realWorktreeMetadata) &&
      actualBranch === `refs/heads/${branch}`
      ? realWorktree
      : null;
  });

  const verifyWorkspace = Effect.fn("IssueContinuation.verifyWorkspace")(
    function* (projectId: ProjectId, projectRoot: string, worktreePath: string, branch: string) {
      const taskFolder = yield* taskFolders.find(worktreePath);
      if (taskFolder === null) return yield* verifyCheckout(projectRoot, worktreePath, branch);
      if (taskFolder.projectId !== projectId || !samePath(taskFolder.projectRoot, projectRoot))
        return null;

      const [realTaskRoot, realWorktree] = yield* Effect.all([
        realDirectory(taskFolder.root),
        taskFolder.projectIsWorktree
          ? verifyCheckout(projectRoot, worktreePath, branch)
          : realDirectory(worktreePath),
      ]);
      if (
        realTaskRoot === null ||
        realWorktree === null ||
        !samePath(
          realWorktree,
          path.resolve(realTaskRoot, path.relative(taskFolder.root, taskFolder.projectPath)),
        )
      ) {
        return null;
      }
      if (!taskFolder.projectIsWorktree) {
        const realProject = yield* fs.realPath(projectRoot);
        if (
          samePath(realProject, realWorktree) ||
          taskFolder.repositories.length === 0 ||
          (yield* fs.exists(path.join(realWorktree, ".git")))
        ) {
          return null;
        }
      }
      const repositories = yield* Effect.forEach(
        taskFolder.repositories,
        ({ repositoryPath, worktreePath }) => verifyCheckout(repositoryPath, worktreePath, branch),
        { concurrency: 4 },
      );
      return repositories.includes(null) ? null : realWorktree;
    },
    Effect.orElseSucceed(() => null),
  );

  return Effect.fn("IssueContinuation.check")(function* <ThreadError, ProjectError, SnapshotError>(
    lookup: ContinuationLookup<ThreadError, ProjectError, SnapshotError>,
    input: IssueContinuationSource,
    expected?: {
      readonly projectId: ProjectId;
      readonly worktreePath: string | null;
      readonly branch: string | null;
      readonly targetThreadId?: ThreadId;
    },
  ): Effect.fn.Return<IssueContinuationResult, ThreadError | ProjectError | SnapshotError> {
    const source = yield* lookup.getThread(input.threadId);
    if (
      source === null ||
      source.deletedAt !== null ||
      !source.issues?.some((link) => threadIssueKeysEqual(link, input.issue))
    ) {
      return { status: "source-unavailable" };
    }
    if (source.pendingRuntimeRequest !== null || source.hasActionableProposedPlan) {
      return { status: "answer-needed" };
    }
    if (source.activeRunId !== null || isLiveSubagentShell(source)) {
      return { status: "thread-busy" };
    }

    const { branch, projectId, worktreePath } = source;
    if (
      worktreePath === null ||
      branch === null ||
      !path.isAbsolute(worktreePath) ||
      (expected !== undefined &&
        (expected.projectId !== projectId ||
          expected.worktreePath !== worktreePath ||
          expected.branch !== branch))
    ) {
      return { status: "worktree-unavailable" };
    }
    const projectRoot = yield* lookup.getProjectRoot(projectId);
    if (projectRoot === null) return { status: "worktree-unavailable" };

    const realWorktree = yield* verifyWorkspace(projectId, projectRoot, worktreePath, branch);
    if (realWorktree === null) return { status: "worktree-unavailable" };

    const snapshots = yield* Effect.forEach(
      worktreePath === realWorktree ? [worktreePath] : [worktreePath, realWorktree],
      lookup.getWorktreeThreads,
    );
    if (
      snapshots.some(({ threads, archivedThreads }) =>
        [...threads, ...archivedThreads].some(
          (thread) =>
            thread.id !== source.id &&
            thread.id !== expected?.targetThreadId &&
            (thread.activeRunId !== null || isLiveSubagentShell(thread)),
        ),
      )
    ) {
      return { status: "worktree-busy" };
    }
    return { status: "ready", projectId, worktreePath, branch };
  });
});

export const issueContinuationBlockMessage: Record<
  Exclude<IssueContinuationResult["status"], "ready">,
  string
> = {
  "source-unavailable": "The original thread is no longer linked to this issue.",
  "answer-needed": "The original thread has a request that needs an answer.",
  "thread-busy": "The original thread is still working.",
  "worktree-unavailable": "The original Git worktree or branch could not be verified.",
  "worktree-busy": "Another thread is using the original worktree.",
};

export class IssueContinuationError extends Schema.TaggedError<IssueContinuationError>()(
  "IssueContinuationError",
  { message: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}
