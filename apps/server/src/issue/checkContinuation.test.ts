import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  IssueTrackerKind,
  ProviderInstanceId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2ThreadShell,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import * as ServerConfig from "../config.ts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as TaskFolders from "../workspace/TaskFolders.ts";
import {
  layerTaskFoldersWithGit,
  taskFoldersTestProjectId,
  type TaskFoldersTestState,
} from "../workspace/TaskFolders.testkit.ts";
import { issueContinuationCheck } from "./checkContinuation.ts";

const layer = GitVcsDriver.layer.pipe(
  Layer.provideMerge(
    ServerConfig.layerTest(process.cwd(), { prefix: "t3-issue-continuation-config-" }),
  ),
  Layer.provideMerge(VcsProcess.layer),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const layerLegacy = Layer.merge(
  layer,
  Layer.mock(TaskFolders.TaskFolders)({ find: () => Effect.succeed(null) }),
);
const threadId = ThreadId.make("original");
const projectId = taskFoldersTestProjectId;
const providerInstanceId = ProviderInstanceId.make("codex");
const branch = "work-in-progress";
const timestamp = "2026-11-18T00:00:00.000Z";
const at = DateTime.makeUnsafe(timestamp);
const issue = {
  tracker: IssueTrackerKind.make("beads"),
  host: "beads",
  scope: "example",
  key: "example-1",
};

function shell(worktreePath: string): OrchestrationV2ThreadShell {
  return {
    id: threadId,
    projectId,
    title: "Original thread",
    providerInstanceId,
    modelSelection: { instanceId: providerInstanceId, model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    branch,
    worktreePath,
    issues: [
      {
        ...issue,
        projectId,
        url: "beads://example/example-1",
        displayKey: issue.key,
        title: "Continue work",
        source: "manual",
        linkedAt: timestamp,
      },
    ],
    lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
    forkedFrom: null,
    activeProviderThreadId: null,
    latestRunId: null,
    activeRunId: null,
    status: "interrupted",
    pendingRuntimeRequest: null,
    latestVisibleMessage: null,
    latestUserMessageAt: null,
    hasActionableProposedPlan: false,
    itemCount: 0,
    visibleItemCount: 0,
    createdBy: "user",
    creationSource: "web",
    createdAt: at,
    updatedAt: at,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    deletedAt: null,
  };
}

const setup = Effect.fn("test.setupIssueContinuation")(function* (state?: TaskFoldersTestState) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const git = yield* GitVcsDriver.GitVcsDriver;
  const check = yield* issueContinuationCheck;
  const sandbox = yield* fs.makeTempDirectoryScoped({ prefix: "t3-issue-continuation-" });
  const projectRoot = path.join(sandbox, "project");
  const runGit = (cwd: string, args: ReadonlyArray<string>) =>
    git.execute({ operation: "test.issueContinuation.git", cwd, args, timeoutMs: 5_000 });
  const initRepository = Effect.fn("test.initContinuationRepository")(function* (cwd: string) {
    yield* fs.makeDirectory(cwd, { recursive: true });
    yield* runGit(cwd, ["init", "-b", "main"]);
    yield* fs.writeFileString(path.join(cwd, "edit.txt"), "base\n");
    yield* runGit(cwd, ["add", "edit.txt"]);
    yield* runGit(cwd, [
      "-c",
      "user.name=Test",
      "-c",
      "user.email=test@example.com",
      "-c",
      "commit.gpgsign=false",
      "commit",
      "-m",
      "Initial",
    ]);
  });
  const taskFolder = yield* Effect.gen(function* () {
    if (state === undefined) return null;
    state.workspaceRoot = projectRoot;
    state.trackedFolders = [
      { name: "api", path: path.join(projectRoot, "api") },
      { name: "web", path: path.join(sandbox, "web") },
    ];
    for (const repository of state.trackedFolders) yield* initRepository(repository.path);
    const taskFolders = yield* TaskFolders.TaskFolders;
    const plan = yield* taskFolders.plan({ projectId });
    if (plan === null) throw new Error("Expected a task folder plan");
    const created = yield* taskFolders.create({
      plan,
      branch,
      projectStart: { refName: "main", baseRefName: "main" },
      startFromOrigin: false,
    });
    return created.taskFolder;
  });
  const worktreePath = taskFolder?.projectPath ?? path.join(sandbox, "linked checkout");
  if (taskFolder === null) {
    yield* initRepository(projectRoot);
    yield* runGit(projectRoot, ["worktree", "add", "-b", branch, worktreePath]);
  }
  const source = shell(worktreePath);
  const continuation = (
    original: OrchestrationV2ThreadShell = source,
    archivedThreads: ReadonlyArray<OrchestrationV2ThreadShell> = [],
    expected?: Parameters<typeof check>[2],
  ) =>
    check(
      {
        getThread: () => Effect.succeed(original),
        getProjectRoot: () => Effect.succeed(projectRoot),
        getWorktreeThreads: () =>
          Effect.succeed({
            schemaVersion: 1,
            snapshotSequence: 0,
            threads: [original],
            archivedThreads,
          }),
      },
      { threadId, issue },
      expected,
    );
  return { fs, path, source, projectRoot, worktreePath, runGit, continuation, taskFolder };
});

it.live("accepts the exact linked checkout and leaves its partial edit unchanged", () =>
  Effect.gen(function* () {
    const { fs, path, worktreePath, continuation } = yield* setup();
    const edit = path.join(worktreePath, "edit.txt");
    yield* fs.writeFileString(edit, "partial edit\n");

    expect(yield* continuation()).toEqual({ status: "ready", projectId, worktreePath, branch });
    expect(yield* fs.readFileString(edit)).toBe("partial edit\n");
  }).pipe(Effect.provide(layerLegacy)),
);

it.live("validates the recorded checkouts of a task folder with a plain project root", () => {
  const state: TaskFoldersTestState = { workspaceRoot: "/unset", trackedFolders: [], thread: null };
  return Effect.gen(function* () {
    const { fs, path, source, worktreePath, runGit, continuation, taskFolder } =
      yield* setup(state);
    const api = taskFolder!.repositories.find((repo) => repo.name === "api")!;
    const web = taskFolder!.repositories.find((repo) => repo.name === "web")!;
    const edit = path.join(api.worktreePath, "edit.txt");
    yield* fs.writeFileString(edit, "partial edit\n");
    state.trackedFolders = [];

    expect(yield* continuation()).toEqual({ status: "ready", projectId, worktreePath, branch });
    expect(yield* fs.readFileString(edit)).toBe("partial edit\n");

    const unavailable = { status: "worktree-unavailable" };
    expect(yield* continuation({ ...source, worktreePath: api.worktreePath })).toEqual(unavailable);
    yield* runGit(api.worktreePath, ["branch", "-m", "different-branch"]);
    expect(yield* continuation()).toEqual(unavailable);
    yield* runGit(api.worktreePath, ["branch", "-m", branch]);

    const moved = `${web.worktreePath}-moved`;
    yield* runGit(web.repositoryPath, ["worktree", "move", web.worktreePath, moved]);
    expect(yield* continuation()).toEqual(unavailable);
    expect(yield* fs.exists(web.worktreePath)).toBe(false);
    yield* fs.symlink(moved, web.worktreePath);
    expect(yield* continuation()).toEqual(unavailable);
    yield* fs.remove(web.worktreePath);
    yield* runGit(web.repositoryPath, ["worktree", "move", moved, web.worktreePath]);

    yield* fs.rename(worktreePath, `${worktreePath}-moved`);
    expect(yield* continuation()).toEqual(unavailable);
    expect(yield* fs.exists(worktreePath)).toBe(false);
  }).pipe(Effect.provide(layerTaskFoldersWithGit(state).pipe(Layer.provideMerge(layer))));
});

it.live(
  "blocks a pending question, a busy source, or another active thread in the same checkout",
  () =>
    Effect.gen(function* () {
      const { source, continuation } = yield* setup();
      const pendingRuntimeRequest = {
        id: RuntimeRequestId.make("question"),
        kind: "user_input" as const,
        createdAt: at,
      };
      expect(yield* continuation({ ...source, status: "waiting", pendingRuntimeRequest })).toEqual({
        status: "answer-needed",
      });
      expect(yield* continuation({ ...source, status: "running" })).toEqual({
        status: "thread-busy",
      });

      const otherThread = {
        ...source,
        id: ThreadId.make("other"),
        status: "running" as const,
        archivedAt: at,
      };
      expect(yield* continuation(source, [otherThread])).toEqual({ status: "worktree-busy" });
      expect(
        yield* continuation(source, [otherThread], {
          projectId,
          worktreePath: source.worktreePath,
          branch,
          targetThreadId: otherThread.id,
        }),
      ).toEqual({ status: "ready", projectId, worktreePath: source.worktreePath, branch });
    }).pipe(Effect.provide(layerLegacy)),
);

it.live("rejects the project root, a changed branch, and a missing or replaced checkout", () =>
  Effect.gen(function* () {
    const { fs, source, projectRoot, worktreePath, continuation, runGit } = yield* setup();
    const unavailable = { status: "worktree-unavailable" };
    expect(yield* continuation({ ...source, worktreePath: projectRoot })).toEqual(unavailable);

    yield* runGit(worktreePath, ["branch", "-m", "another-branch"]);
    expect(yield* continuation()).toEqual(unavailable);
    expect(
      yield* continuation({ ...source, branch: "another-branch" }, [], {
        projectId,
        worktreePath,
        branch,
      }),
    ).toEqual(unavailable);

    yield* fs.remove(worktreePath, { recursive: true });
    expect(yield* continuation()).toEqual(unavailable);
    expect(yield* fs.exists(worktreePath)).toBe(false);

    yield* fs.makeDirectory(worktreePath);
    yield* runGit(worktreePath, ["init", "-b", branch]);
    expect(yield* continuation()).toEqual(unavailable);
  }).pipe(Effect.provide(layerLegacy)),
);
