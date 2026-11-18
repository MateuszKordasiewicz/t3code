import { IssueTrackerKind, type ThreadIssueKey } from "@t3tools/contracts";
import { assert, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import { listLinkedIssueThreads } from "./linkedThreads.ts";

const encodePayload = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

const issueKey = (tracker: string, host: string, scope: string, key: string): ThreadIssueKey => ({
  tracker: IssueTrackerKind.make(tracker),
  host,
  scope,
  key,
});

it.effect(
  "finds active, settled, and archived threads for exactly one issue, excluding deleted and side chat links",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const at = (day: number) => `2026-11-${String(day).padStart(2, "0")}T00:00:00.000Z`;
      // What a `beads://t3code-01da14b2/t3-dxh.5` link stores.
      const bead = issueKey("beads", "beads", "t3code-01da14b2", "t3-dxh.5");
      const otherScopeBead = issueKey("beads", "beads", "other-0000", "t3-dxh.5");
      const repoA = issueKey("github", "github.com", "owner/a", "7");
      const repoB = issueKey("github", "github.com", "owner/b", "7");
      const fixtures: ReadonlyArray<{
        id: string;
        issues: ReadonlyArray<ThreadIssueKey>;
        day: number;
        archived?: boolean;
        settled?: "override" | "auto";
        deleted?: boolean;
        sideChat?: boolean;
      }> = [
        { id: "archived", issues: [bead], day: 9, archived: true },
        { id: "active", issues: [bead], day: 8 },
        { id: "settled", issues: [bead], day: 7, settled: "override" },
        { id: "auto-settled", issues: [bead], day: 6, settled: "auto" },
        {
          id: "mixed-case",
          issues: [issueKey("beads", "Beads", "T3Code-01DA14B2", "t3-dxh.5")],
          day: 5,
        },
        { id: "two-issues", issues: [repoA, bead], day: 4 },
        { id: "deleted", issues: [bead], day: 10, deleted: true },
        { id: "side-chat", issues: [bead], day: 10, sideChat: true },
        { id: "other-scope", issues: [otherScopeBead], day: 10 },
        {
          id: "other-tracker",
          issues: [issueKey("linear", "beads", "t3code-01da14b2", "t3-dxh.5")],
          day: 10,
        },
        {
          id: "other-key",
          issues: [issueKey("beads", "beads", "t3code-01da14b2", "t3-dxh.6")],
          day: 10,
        },
        { id: "repo-a", issues: [repoA], day: 11 },
        { id: "repo-b", issues: [repoB], day: 12 },
      ];
      for (const fixture of fixtures) {
        const payload = yield* encodePayload({
          linkedPullRequest: null,
          settledOverride: fixture.settled === "override" ? "settled" : null,
          settledAt: fixture.settled === "auto" ? at(fixture.day) : null,
          ...(fixture.sideChat === true ? { lineage: { sideChat: true } } : {}),
          pullRequests: [],
          issues: fixture.issues.map((key) => ({
            ...key,
            projectId: "project-1",
            url: `https://${key.host}/${key.scope}/${key.key}`,
            source: "manual",
            linkedAt: at(1),
            displayKey: key.key,
            title: `Issue ${key.key}`,
          })),
        });
        yield* sql`
          INSERT INTO orchestration_v2_projection_threads (
            thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
            created_at, updated_at, archived_at, deleted_at, payload_json
          ) VALUES (
            ${fixture.id}, 'project-1', ${fixture.id}, 'codex', 'full-access', 'default',
            ${at(1)}, ${at(fixture.day)},
            ${fixture.archived === true ? at(fixture.day) : null},
            ${fixture.deleted === true ? at(fixture.day) : null}, ${payload}
          )
        `;
      }
      const ids = (key: ThreadIssueKey) =>
        listLinkedIssueThreads(key).pipe(
          Effect.map((result) => result.threads.map((thread) => thread.id)),
        );

      expect(yield* listLinkedIssueThreads(bead)).toEqual({
        threads: [
          {
            id: "archived",
            projectId: "project-1",
            title: "archived",
            archivedAt: at(9),
            settled: false,
          },
          {
            id: "active",
            projectId: "project-1",
            title: "active",
            archivedAt: null,
            settled: false,
          },
          {
            id: "settled",
            projectId: "project-1",
            title: "settled",
            archivedAt: null,
            settled: true,
          },
          {
            id: "auto-settled",
            projectId: "project-1",
            title: "auto-settled",
            archivedAt: null,
            settled: true,
          },
          {
            id: "mixed-case",
            projectId: "project-1",
            title: "mixed-case",
            archivedAt: null,
            settled: false,
          },
          {
            id: "two-issues",
            projectId: "project-1",
            title: "two-issues",
            archivedAt: null,
            settled: false,
          },
        ],
      });
      expect(yield* ids(issueKey("beads", "BEADS", "T3CODE-01da14b2", "t3-dxh.5"))).toEqual([
        "archived",
        "active",
        "settled",
        "auto-settled",
        "mixed-case",
        "two-issues",
      ]);
      expect(yield* ids(otherScopeBead)).toEqual(["other-scope"]);
      expect(yield* ids(issueKey("github", "GitHub.com", "Owner/A", "7"))).toEqual([
        "repo-a",
        "two-issues",
      ]);
      expect(yield* ids(repoB)).toEqual(["repo-b"]);
      assert.deepStrictEqual(
        yield* listLinkedIssueThreads(issueKey("beads", "beads", "t3code-01da14b2", "t3-zzz")),
        { threads: [] },
      );
    }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);
