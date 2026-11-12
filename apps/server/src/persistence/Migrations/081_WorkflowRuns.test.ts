import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "../Migrations.ts";

const layer = NodeSqliteClient.layer({ filename: ":memory:" });
const hash = "a".repeat(64);
const otherHash = "b".repeat(64);

it.effect("upgrades an 080 database without changing ordinary rows or history", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* runMigrations({ toMigrationInclusive: 80 });
    yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at)
      VALUES ('project', 'Project', '/project', '[]', 'created', 'updated')`;
    yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, created_at, updated_at)
      VALUES ('thread', 'project', 'Thread', '{}', 'full-access', 'default', 'created', 'updated')`;
    yield* sql`INSERT INTO scheduled_tasks ${sql.insert({
      task_id: "task",
      title: "Task",
      prompt: "Run",
      enabled: 1,
      schedule_json: "{}",
      project_id: "project",
      thread_id: "thread",
      workspace_strategy_json: "{}",
      model_selection_json: "{}",
      runtime_mode: "full-access",
      interaction_mode: "default",
      created_by: "user",
      creation_source: "web",
      created_at: "created",
      updated_at: "updated",
      next_run_at: null,
      last_run_at: null,
      last_run_status: "never",
      last_run_error: null,
      run_count: 0,
      thread_mode: "continue",
    })}`;
    const projects = yield* sql`SELECT * FROM projection_projects`;
    const threads = yield* sql`SELECT * FROM projection_threads`;
    const tasks = yield* sql`SELECT * FROM scheduled_tasks`;
    const history =
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`;
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 81 }), [
      [81, "WorkflowRuns"],
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM projection_projects`, projects);
    assert.deepStrictEqual(yield* sql`SELECT * FROM projection_threads`, threads);
    assert.deepStrictEqual(yield* sql`SELECT * FROM scheduled_tasks`, tasks);
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
      [...history, { migration_id: 81, name: "WorkflowRuns" }],
    );
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "enforces workflow identity, ownership, lifecycle and value constraints on fresh SQLite",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
      yield* runMigrations();
      const history = yield* sql<{
        migration_id: number;
        name: string;
      }>`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`;
      assert.deepStrictEqual(
        history.map(({ migration_id, name }) => [migration_id, name] as const),
        migrationManifest,
      );
      yield* sql`INSERT INTO workflow_plan_identities VALUES ('p1'), ('p2')`;
      yield* sql`INSERT INTO workflow_revision_identities VALUES ('p1', 'rev', ${hash}, ${hash}), ('p2', 'rev', ${hash}, ${hash})`;
      yield* sql`INSERT INTO workflow_planning_sessions VALUES ('s1', 'p1', NULL, 'active', 0, 0, '{}'), ('s2', 'p2', NULL, 'paused', 0, 0, '{}')`;
      yield* sql`INSERT INTO workflow_runs VALUES ('r1', 'p1', 'rev', ${hash}, ${hash}, ${hash}, 'active', 0, 0, '{}'), ('r2', 'p2', 'rev', ${hash}, ${hash}, ${hash}, 'active', 0, 0, '{}')`;
      yield* sql`INSERT INTO workflow_operations VALUES ('o1', 'r1', NULL, 'check', ${hash}, 'pending', 1, 0, 0, NULL, '{}'), ('o2', NULL, 's1', 'planning_pass', ${hash}, 'pending', 0, 0, 0, NULL, '{}'), ('o3', 'r2', NULL, 'check', ${hash}, 'pending', 0, 0, 0, NULL, '{}'), ('o4', NULL, 's2', 'planning_pass', ${hash}, 'pending', 0, 0, 0, NULL, '{}')`;
      yield* sql`INSERT INTO workflow_planning_passes VALUES ('pass1', 's1', 'p1', 'rev', ${hash}, ${hash}, 'o2', 'thread', 'run', '{}')`;
      yield* sql`INSERT INTO workflow_attempts VALUES ('attempt1', 'r1', 'o1', 'item', 0)`;
      yield* sql`INSERT INTO workflow_role_results VALUES ('role1', 'r1', 'o1', '{}', 0)`;
      yield* sql`INSERT INTO workflow_acceptances VALUES ('r1', 'accept', '{}', 0)`;
      yield* sql`INSERT INTO workflow_command_receipts VALUES ('c1', 'r1', NULL, ${hash}, 'receipt1', 'o1', '{}'), ('c2', NULL, 's1', ${hash}, 'receipt2', 'o2', '{}')`;

      const invalid = [
        [
          "missing operation owner",
          `INSERT INTO workflow_operations VALUES ('x', NULL, NULL, 'check', '${otherHash}', 'pending', 0, 0, 0, NULL, '{}')`,
        ],
        [
          "mixed operation owners",
          `INSERT INTO workflow_operations VALUES ('x', 'r1', 's1', 'check', '${otherHash}', 'pending', 0, 0, 0, NULL, '{}')`,
        ],
        [
          "run planning kind",
          `INSERT INTO workflow_operations VALUES ('x', 'r1', NULL, 'planning_pass', '${otherHash}', 'pending', 0, 0, 0, NULL, '{}')`,
        ],
        [
          "session run kind",
          `INSERT INTO workflow_operations VALUES ('x', NULL, 's1', 'check', '${otherHash}', 'pending', 0, 0, 0, NULL, '{}')`,
        ],
        [
          "foreign operation owner",
          `INSERT INTO workflow_operations VALUES ('x', 'missing', NULL, 'check', '${otherHash}', 'pending', 0, 0, 0, NULL, '{}')`,
        ],
        [
          "run fingerprint duplicate",
          `INSERT INTO workflow_operations VALUES ('x', 'r1', NULL, 'check', '${hash}', 'pending', 0, 0, 0, NULL, '{}')`,
        ],
        [
          "session fingerprint duplicate",
          `INSERT INTO workflow_operations VALUES ('x', NULL, 's1', 'planning_pass', '${hash}', 'pending', 0, 0, 0, NULL, '{}')`,
        ],
        [
          "bad operation hash",
          `INSERT INTO workflow_operations VALUES ('x', 'r1', NULL, 'check', 'bad', 'pending', 0, 0, 0, NULL, '{}')`,
        ],
        [
          "uppercase hash",
          `INSERT INTO workflow_operations VALUES ('x', 'r1', NULL, 'check', '${"A".repeat(64)}', 'pending', 0, 0, 0, NULL, '{}')`,
        ],
        [
          "held claimed",
          `INSERT INTO workflow_operations VALUES ('x', 'r1', NULL, 'check', '${otherHash}', 'claimed', 1, 0, 0, NULL, '{}')`,
        ],
        [
          "invalid boolean",
          `INSERT INTO workflow_operations VALUES ('x', 'r1', NULL, 'check', '${otherHash}', 'pending', 2, 0, 0, NULL, '{}')`,
        ],
        [
          "invalid status",
          `INSERT INTO workflow_operations VALUES ('x', 'r1', NULL, 'check', '${otherHash}', 'bad', 0, 0, 0, NULL, '{}')`,
        ],
        [
          "negative generation",
          `INSERT INTO workflow_operations VALUES ('x', 'r1', NULL, 'check', '${otherHash}', 'pending', 0, 0, -1, NULL, '{}')`,
        ],
        [
          "invalid json",
          `INSERT INTO workflow_operations VALUES ('x', 'r1', NULL, 'check', '${otherHash}', 'pending', 0, 0, 0, NULL, 'bad')`,
        ],
        [
          "second unsettled session",
          "INSERT INTO workflow_planning_sessions VALUES ('x', 'p1', NULL, 'needs_attention', 0, 0, '{}')",
        ],
        [
          "foreign plan",
          "INSERT INTO workflow_planning_sessions VALUES ('x', 'missing', NULL, 'active', 0, 0, '{}')",
        ],
        [
          "foreign predecessor",
          "INSERT INTO workflow_planning_sessions VALUES ('x', 'p1', 'missing', 'stopped', 0, 0, '{}')",
        ],
        ["empty identity", "INSERT INTO workflow_plan_identities VALUES ('')"],
        [
          "duplicate identity",
          `INSERT INTO workflow_revision_identities VALUES ('p1', 'rev', '${hash}', '${hash}')`,
        ],
        [
          "invalid revision hash",
          `INSERT INTO workflow_revision_identities VALUES ('p1', 'bad', '${hash}', 'bad')`,
        ],
        [
          "run revision binding",
          `INSERT INTO workflow_runs VALUES ('x', 'p1', 'rev', '${otherHash}', '${hash}', '${hash}', 'active', 0, 0, '{}')`,
        ],
        [
          "pass wrong session plan",
          `INSERT INTO workflow_planning_passes VALUES ('x', 's1', 'p2', 'rev', '${hash}', '${hash}', 'o2', NULL, NULL, '{}')`,
        ],
        [
          "pass cross owner operation",
          `INSERT INTO workflow_planning_passes VALUES ('x', 's1', 'p1', 'rev', '${hash}', '${hash}', 'o4', NULL, NULL, '{}')`,
        ],
        [
          "partial native binding",
          `INSERT INTO workflow_planning_passes VALUES ('x', 's2', 'p2', 'rev', '${hash}', '${hash}', 'o4', 'thread', NULL, '{}')`,
        ],
        [
          "duplicate native binding",
          `INSERT INTO workflow_planning_passes VALUES ('x', 's2', 'p2', 'rev', '${hash}', '${hash}', 'o4', 'thread', 'run', '{}')`,
        ],
        [
          "attempt cross owner",
          "INSERT INTO workflow_attempts VALUES ('x', 'r1', 'o3', 'item', 0)",
        ],
        [
          "result cross owner",
          "INSERT INTO workflow_role_results VALUES ('x', 'r1', 'o3', '{}', 0)",
        ],
        [
          "acceptance foreign run",
          "INSERT INTO workflow_acceptances VALUES ('missing', 'x', '{}', 0)",
        ],
        [
          "receipt missing owner",
          `INSERT INTO workflow_command_receipts VALUES ('x', NULL, NULL, '${hash}', 'x', NULL, '{}')`,
        ],
        [
          "receipt mixed owners",
          `INSERT INTO workflow_command_receipts VALUES ('x', 'r1', 's1', '${hash}', 'x', NULL, '{}')`,
        ],
        [
          "receipt cross run",
          `INSERT INTO workflow_command_receipts VALUES ('x', 'r1', NULL, '${hash}', 'x', 'o3', '{}')`,
        ],
        [
          "receipt cross session",
          `INSERT INTO workflow_command_receipts VALUES ('x', NULL, 's1', '${hash}', 'x', 'o4', '{}')`,
        ],
        [
          "receipt foreign owner",
          `INSERT INTO workflow_command_receipts VALUES ('x', 'missing', NULL, '${hash}', 'x', NULL, '{}')`,
        ],
      ] as const;
      for (const [label, statement] of invalid) {
        const result = yield* Effect.exit(sql.unsafe(statement));
        assert.isTrue(Exit.isFailure(result), label);
        assert.include(String(result), "constraint", label);
      }
      yield* sql`UPDATE workflow_planning_sessions SET status = 'stopped' WHERE id = 's1'`;
      yield* sql`INSERT INTO workflow_planning_sessions VALUES ('s3', 'p1', 's1', 'active', 0, 0, '{}')`;
      yield* sql`INSERT INTO workflow_planning_passes VALUES ('pass2', 's2', 'p2', 'rev', ${hash}, ${hash}, 'o4', NULL, NULL, '{}')`;
      assert.strictEqual((yield* sql`SELECT id FROM workflow_planning_passes`).length, 2);
    }).pipe(Effect.provide(layer)),
);
