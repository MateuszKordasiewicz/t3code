import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as TestClock from "effect/testing/TestClock";
import { VcsProcessSpawnError } from "@t3tools/contracts";
import type { SourceControlProcessInput } from "@t3tools/source-control-core/server/SourceControlHost";
import * as TestSourceControlHost from "@t3tools/source-control-testing/TestSourceControlHost";

import * as BeadsIssueTracker from "./BeadsIssueTracker.ts";
// Recorded from bd 1.3.4 against a scratch database.
import listAll from "./fixtures/list-all.json" with { type: "json" };
import showEpic from "./fixtures/show-epic.json" with { type: "json" };
import showSlice from "./fixtures/show-slice.json" with { type: "json" };

const FIXTURES = {
  "list-all.json": listAll,
  "show-epic.json": showEpic,
  "show-slice.json": showSlice,
};
const fixture = (name: keyof typeof FIXTURES) => JSON.stringify(FIXTURES[name]);

const DATABASE_ID = "d6e880e0-e62e-488a-92f2-c7c32424f1ec";
const SCOPE = "demo-d6e880e0";
const OTHER_SCOPE = "demo-11111111";
const contextFor = (id: string) =>
  JSON.stringify({ project_id: id, repo_root: "/home/dev/demo", schema_version: 1 });

type Answer = string | { readonly stdout: string; readonly exitCode: number };

/** Answers each bd run by its subcommand, recording every run. */
function tracker(answers: {
  readonly context?: (cwd: string) => Answer;
  readonly list?: (args: ReadonlyArray<string>) => Answer;
  readonly show?: (args: ReadonlyArray<string>) => Answer;
  readonly spawnFails?: () => boolean;
}) {
  const runs: SourceControlProcessInput[] = [];
  const layer = TestSourceControlHost.layer({
    process: {
      run: (input) =>
        Effect.suspend(() => {
          runs.push(input);
          if (answers.spawnFails?.() === true) {
            return Effect.fail(
              new VcsProcessSpawnError({
                operation: input.operation,
                command: input.command,
                cwd: input.cwd,
                cause: new Error("spawn bd ENOENT"),
              }),
            );
          }
          const [subcommand] = input.args;
          const answer =
            subcommand === "context"
              ? (answers.context?.(input.cwd) ?? contextFor(DATABASE_ID))
              : subcommand === "list"
                ? (answers.list?.(input.args) ?? fixture("list-all.json"))
                : (answers.show?.(input.args) ?? fixture("show-slice.json"));
          const { stdout, exitCode } =
            typeof answer === "string" ? { stdout: answer, exitCode: 0 } : answer;
          return Effect.succeed(
            TestSourceControlHost.processOutput(stdout, {
              exitCode: ChildProcessSpawner.ExitCode(exitCode),
            }),
          );
        }),
    },
    git: { readConfigValue: () => Effect.succeed("Fixture Agent") },
  });
  return { api: BeadsIssueTracker.make.pipe(Effect.provide(layer)), runs };
}

const listInput = {
  cwd: "/repo",
  host: BeadsIssueTracker.HOST,
  scope: SCOPE,
  state: "all",
  involvement: "all",
  viewer: "Fixture Agent",
  limit: 50,
  sort: "updated",
} as const;

describe("BeadsIssueTracker", () => {
  it.effect("claims a directory by the database bd finds there, once per cache window", () =>
    Effect.gen(function* () {
      const { api, runs } = tracker({});
      const beads = yield* api;
      const first = yield* beads.discoverProject!({ cwd: "/repo/worktree" });
      const second = yield* beads.discoverProject!({ cwd: "/repo/worktree" });
      assert.deepStrictEqual(first, { host: "beads", scope: SCOPE });
      assert.deepStrictEqual(second, first);
      assert.strictEqual(runs.length, 1);
      assert.deepStrictEqual(runs[0]!.args, ["context", "--json", "--readonly"]);
      // An inherited BEADS_DIR would make every project read one database.
      assert.deepStrictEqual(runs[0]!.env, { BEADS_DIR: undefined, BEADS_DB: undefined });
    }),
  );

  it.effect("claims nothing where bd finds no database, or where there is no bd", () =>
    Effect.gen(function* () {
      const none = yield* tracker({
        context: () => ({
          stdout: JSON.stringify({
            error: "cannot resolve repo context: no .beads directory found",
          }),
          exitCode: 1,
        }),
      }).api;
      assert.isNull(yield* none.discoverProject!({ cwd: "/elsewhere" }));
      const missing = yield* tracker({ spawnFails: () => true }).api;
      assert.isNull(yield* missing.discoverProject!({ cwd: "/repo" }));
    }),
  );

  it.effect("maps bd's list rows and pages them with an opaque cursor until exhausted", () =>
    Effect.gen(function* () {
      const beads = yield* tracker({}).api;
      const first = yield* beads.listIssues({ ...listInput, limit: 3 });
      assert.deepStrictEqual(
        first.items.map((item) => item.key),
        ["demo-0b4", "demo-dvm", "demo-ku6.1"],
      );
      assert.strictEqual(first.nextCursor, "3");
      const slice = first.items.find((item) => item.key === "demo-ku6.1")!;
      assert.deepStrictEqual(slice, {
        key: "demo-ku6.1",
        displayKey: "demo-ku6.1",
        title: "First slice",
        url: `beads://${SCOPE}/demo-ku6.1`,
        author: { login: "Fixture Agent", name: null, avatarUrl: null },
        state: { category: "open", name: "Open" },
        createdAt: "2026-11-17T16:26:59Z",
        updatedAt: "2026-11-17T16:27:00Z",
        closedAt: null,
        assignees: [{ login: "Fixture Agent", name: null, avatarUrl: null }],
        labels: [
          { name: "qa", color: null },
          { name: "ui", color: null },
        ],
        commentCount: 1,
      });
      const rest = yield* beads.listIssues({ ...listInput, limit: 3, cursor: "3" });
      assert.deepStrictEqual(
        rest.items.map((item) => [item.key, item.state]),
        [
          ["demo-ku6.2", { category: "closed", name: "Closed" }],
          ["demo-ku6", { category: "open", name: "Open" }],
        ],
      );
      assert.isNull(rest.nextCursor);
    }),
  );

  it.effect("passes state, label and assignee filters to bd and searches keys and titles", () =>
    Effect.gen(function* () {
      const { api, runs } = tracker({});
      const beads = yield* api;
      yield* beads.listIssues({
        ...listInput,
        state: "open",
        involvement: "assigned",
        labels: ["qa", "human"],
      });
      yield* beads.listIssues({ ...listInput, state: "closed" });
      const lists = runs.filter((run) => run.args[0] === "list").map((run) => run.args);
      assert.deepStrictEqual(lists[0], [
        "list",
        "--brief",
        "--flat",
        "--limit",
        "0",
        "--label",
        "qa",
        "--label",
        "human",
        "--assignee",
        "Fixture Agent",
        "--json",
        "--readonly",
      ]);
      assert.deepStrictEqual(lists[1]?.slice(5, 7), ["--status", "closed"]);
      assert.deepStrictEqual(yield* beads.listIssues({ ...listInput, involvement: "mentioned" }), {
        items: [],
        nextCursor: null,
      });
      const found = yield* beads.listIssues({ ...listInput, query: "WHICH" });
      assert.deepStrictEqual(
        found.items.map((item) => item.key),
        ["demo-ku6.2"],
      );
    }),
  );

  it.effect("refuses a scope the project's directory does not hold", () =>
    Effect.gen(function* () {
      const beads = yield* tracker({}).api;
      const error = yield* beads.listIssues({ ...listInput, scope: OTHER_SCOPE }).pipe(Effect.flip);
      assert.strictEqual(error.reason, "not-found");
      const detail = yield* beads
        .getIssue({ cwd: "/repo", host: "beads", scope: OTHER_SCOPE, key: "demo-ku6.1" })
        .pipe(Effect.flip);
      assert.strictEqual(detail.reason, "not-found");
    }),
  );

  it.effect("reads a bead read-only, with its type, priority and graph written into the body", () =>
    Effect.gen(function* () {
      const beads = yield* tracker({}).api;
      const detail = yield* beads.getIssue({
        cwd: "/repo",
        host: "beads",
        scope: SCOPE,
        key: "demo-ku6.1",
      });
      assert.deepStrictEqual(detail.states, []);
      assert.isTrue(Object.values(detail.viewerPermissions).every((granted) => !granted));
      assert.isUndefined(detail.reactions);
      assert.deepStrictEqual(detail.comments, [
        {
          id: "40330eb9-ed41-5ede-a520-0ef573b86085",
          author: { login: "Fixture Agent", name: null, avatarUrl: null },
          body: "First comment",
          createdAt: "2026-11-17T16:27:00Z",
          url: null,
        },
      ]);
      assert.strictEqual(
        detail.body,
        [
          "Slice body",
          "## Acceptance criteria\n\nTry it: run the thing",
          "## Design\n\nDesign note",
          "## Notes\n\nA note",
          [
            "## Details",
            "",
            "- Type: feature",
            "- Priority: P1",
            "- Status: Open",
            "- Parent: demo-ku6 · Demo epic (open)",
            "- Depends on:",
            "  - demo-ku6.2 · Q: which way? (closed)",
          ].join("\n"),
        ].join("\n\n"),
      );
      const epic = yield* (yield* tracker({ show: () => fixture("show-epic.json") }).api).getIssue({
        cwd: "/repo",
        host: "beads",
        scope: SCOPE,
        key: "demo-ku6",
      });
      assert.include(
        epic.body,
        "- Children:\n  - demo-ku6.1 · First slice (open)\n  - demo-ku6.2 · Q: which way? (closed)",
      );
      // Only a blocking edge is a dependency; any other link keeps its own name.
      const [slice] = JSON.parse(fixture("show-slice.json"));
      const [parent, blocker] = slice.dependencies;
      const { dependency_type: _untyped, ...untypedBlocker } = blocker;
      const linked = yield* (yield* tracker({
        show: () =>
          JSON.stringify([
            {
              ...slice,
              dependencies: [parent, untypedBlocker, { ...blocker, dependency_type: "related" }],
              dependents: [{ ...blocker, id: "demo-ku6.3", dependency_type: "tracks" }],
            },
          ]),
      }).api).getIssue({ cwd: "/repo", host: "beads", scope: SCOPE, key: "demo-ku6.1" });
      assert.notInclude(linked.body, "Required by");
      assert.include(
        linked.body,
        [
          "- Depends on:",
          "  - demo-ku6.2 · Q: which way? (closed)",
          "- Links to:",
          "  - related · demo-ku6.2 · Q: which way? (closed)",
          "- Linked from:",
          "  - tracks · demo-ku6.3 · Q: which way? (closed)",
        ].join("\n"),
      );
    }),
  );

  it.effect("reports a missing bead as not found", () =>
    Effect.gen(function* () {
      const beads = yield* tracker({
        show: () => ({
          stdout: JSON.stringify({ error: "no issues found matching the provided IDs" }),
          exitCode: 1,
        }),
      }).api;
      const error = yield* beads
        .getIssue({ cwd: "/repo", host: "beads", scope: SCOPE, key: "demo-404" })
        .pipe(Effect.flip);
      assert.strictEqual(error.reason, "not-found");
    }),
  );

  it.effect("names a bd that went missing after discovery as a missing tool", () =>
    Effect.gen(function* () {
      let installed = true;
      const beads = yield* tracker({ spawnFails: () => !installed }).api;
      yield* beads.discoverProject!({ cwd: "/repo" });
      installed = false;
      const error = yield* beads
        .getIssue({ cwd: "/repo", host: "beads", scope: SCOPE, key: "demo-1" })
        .pipe(Effect.flip);
      assert.strictEqual(error.reason, "missing-tool");
      assert.match(error.detail, /bd \(beads\) was not found/);
      const viewer = yield* beads.getViewer({ cwd: "/repo", host: "beads" });
      assert.strictEqual(viewer, "Fixture Agent");
    }),
  );

  it.effect("waits out a busy database a few times, then says it is busy", () =>
    Effect.gen(function* () {
      const gateBusy = {
        stdout: JSON.stringify({
          error:
            "a maintenance operation is running on this workspace; retry when it completes: workspace gate busy",
        }),
        exitCode: 1,
      };
      let attempts = 0;
      const recovers = yield* tracker({
        list: () => (++attempts < 3 ? gateBusy : fixture("list-all.json")),
      }).api;
      const fiber = yield* recovers.listIssues(listInput).pipe(Effect.forkChild);
      yield* TestClock.adjust("2 seconds");
      const page = yield* Fiber.join(fiber);
      assert.strictEqual(page.items.length, 5);
      assert.strictEqual(attempts, 3);

      const stuck = yield* tracker({ list: () => gateBusy }).api;
      const failing = yield* stuck.listIssues(listInput).pipe(Effect.flip, Effect.forkChild);
      yield* TestClock.adjust("5 seconds");
      const error = yield* Fiber.join(failing);
      assert.strictEqual(error.reason, "failed");
      assert.match(error.detail, /beads database is busy/);
    }),
  );

  it("scopes a database by its repository folder and its id", () => {
    const main = { project_id: DATABASE_ID, repo_root: "/home/dev/My Repo" };
    assert.strictEqual(BeadsIssueTracker.databaseScope(main), "my-repo-d6e880e0");
    assert.notStrictEqual(
      BeadsIssueTracker.databaseScope({
        ...main,
        project_id: "0badc0de-0000-0000-0000-000000000000",
      }),
      "my-repo-d6e880e0",
    );
    assert.strictEqual(BeadsIssueTracker.databaseScope({ project_id: DATABASE_ID }), "d6e880e0");
  });

  it("links issues by database and key, and reads its own links back", () => {
    const url = BeadsIssueTracker.issueUrl(SCOPE, "t3-dxh.1");
    assert.strictEqual(url, `beads://${SCOPE}/t3-dxh.1`);
    assert.deepStrictEqual(BeadsIssueTracker.parseIssueUrl(new URL(url)), {
      host: "beads",
      scope: SCOPE,
      key: "t3-dxh.1",
    });
    assert.isNull(BeadsIssueTracker.parseIssueUrl(new URL("https://github.com/a/b/issues/1")));
    assert.isNull(BeadsIssueTracker.parseIssueUrl(new URL(`beads://${SCOPE}/`)));
    assert.isNull(BeadsIssueTracker.parseIssueUrl(new URL(`beads://${SCOPE}/%`)));
  });
});
