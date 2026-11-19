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

  const probe = (cwd: string, args: ReadonlyArray<string>) =>
    git
      .execute({ operation: "IssueContinuation.checkWorkspace", cwd, args, timeoutMs: 5_000 })
      .pipe(Effect.map(({ stdout }) => stdout.replace(/\r?\n$/, "")));

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

    const samePath = (left: string, right: string) => path.relative(left, right) === "";
    const realWorktree = yield* Effect.gen(function* () {
      const [realProject, realWorktree, realParent, listed] = yield* Effect.all(
        [
          fs.realPath(projectRoot),
          fs.realPath(worktreePath),
          fs.realPath(path.dirname(worktreePath)),
          git.listWorktreePaths(projectRoot),
        ],
        { concurrency: 4 },
      );
      if (
        samePath(realWorktree, realProject) ||
        !samePath(realWorktree, path.join(realParent, path.basename(worktreePath))) ||
        !listed.some((entry) => samePath(entry, realWorktree))
      ) {
        return null;
      }
      const [projectTop, worktreeTop, projectMetadata, worktreeMetadata, actualBranch] =
        yield* Effect.all(
          [
            probe(realProject, ["rev-parse", "--show-toplevel"]),
            probe(realWorktree, ["rev-parse", "--show-toplevel"]),
            probe(realProject, ["rev-parse", "--git-common-dir"]),
            probe(realWorktree, ["rev-parse", "--git-common-dir"]),
            probe(realWorktree, ["symbolic-ref", "--quiet", "HEAD"]),
          ],
          { concurrency: 5 },
        );
      const [realProjectTop, realWorktreeTop, realProjectMetadata, realWorktreeMetadata] =
        yield* Effect.all(
          [
            fs.realPath(projectTop),
            fs.realPath(worktreeTop),
            fs.realPath(path.resolve(realProject, projectMetadata)),
            fs.realPath(path.resolve(realWorktree, worktreeMetadata)),
          ],
          { concurrency: 4 },
        );
      if (
        samePath(realWorktree, realProjectTop) ||
        !samePath(realWorktree, realWorktreeTop) ||
        !samePath(realProjectMetadata, realWorktreeMetadata) ||
        actualBranch !== `refs/heads/${branch}`
      ) {
        return null;
      }
      return realWorktree;
    }).pipe(Effect.orElseSucceed(() => null));
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
