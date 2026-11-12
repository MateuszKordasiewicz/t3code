// @effect-diagnostics nodeBuiltinImport:off -- Temporary SQLite files require Node filesystem paths.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const hash = "a".repeat(64);
const otherHash = "b".repeat(64);
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO workflow_plan_identities VALUES ('plan')`;
  yield* sql`INSERT INTO development_plans VALUES ('plan', 'env', 'project', 'Plan', NULL, 2, 'created')`;
  yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan', 'revision', ${hash}, ${otherHash})`;
  yield* sql`INSERT INTO workflow_planning_sessions VALUES ('session', 'plan', NULL, 'active', 2, 3, '{ "session": "original" }')`;
  yield* sql`INSERT INTO workflow_operations VALUES ('op1', NULL, 'session', 'planning_pass', ${hash}, 'settled', 0, 0, 3, NULL, '{ "operation": 1 }'), ('op2', NULL, 'session', 'planning_pass', ${otherHash}, 'pending', 0, 0, 3, NULL, '{ "operation": 2 }')`;
  yield* sql`INSERT INTO workflow_planning_passes VALUES ('pass1', 'session', 'plan', 'revision', ${hash}, ${otherHash}, 'op1', 'thread1', 'run1', '{ "pass": 1 }'), ('pass2', 'session', 'plan', 'revision', ${hash}, ${otherHash}, 'op2', 'thread2', 'run2', '{ "pass": 2 }')`;
});

it.effect("upgrades an 082 database preserving rows and persists evidence across reopen", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const dir = yield* Effect.acquireRelease(
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "planning-evidence-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(dir, "test.sqlite");
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA foreign_keys = ON`;
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
        yield* runMigrations({ toMigrationInclusive: 82 });
        yield* seed;
        const plans = yield* sql`SELECT * FROM development_plans`;
        const sessions = yield* sql`SELECT * FROM workflow_planning_sessions`;
        const operations = yield* sql`SELECT * FROM workflow_operations ORDER BY id`;
        const passes = yield* sql`SELECT * FROM workflow_planning_passes ORDER BY id`;
        const history =
          yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`;
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 83 }), [
          [83, "PlanningEvidence"],
        ]);
        assert.deepStrictEqual(yield* sql`SELECT * FROM development_plans`, plans);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, sessions);
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM workflow_operations ORDER BY id`,
          operations,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM workflow_planning_passes ORDER BY id`,
          passes,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
          [...history, { migration_id: 83, name: "PlanningEvidence" }],
        );
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_observations`, []);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_dispositions`, []);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_settlements`, []);
        yield* sql`INSERT INTO workflow_planning_observations VALUES ('pass1', 'op1', 'thread1', 'run1', 'message1', ${hash}, '{ "observed": true }')`;
        yield* sql`INSERT INTO workflow_planning_dispositions VALUES ('pass1', 'finding', 'pass2', '{ "disposition": true }')`;
        yield* sql`INSERT INTO workflow_planning_settlements VALUES ('op1', ${otherHash}, '{ "result": true }')`;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA foreign_keys = ON`;
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_observations`, [
          {
            pass_id: "pass1",
            operation_id: "op1",
            native_thread_id: "thread1",
            native_run_id: "run1",
            terminal_message_id: "message1",
            manifest_hash: hash,
            observation_json: '{ "observed": true }',
          },
        ]);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_dispositions`, [
          {
            review_pass_id: "pass1",
            finding_id: "finding",
            repair_pass_id: "pass2",
            disposition_json: '{ "disposition": true }',
          },
        ]);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_settlements`, [
          { operation_id: "op1", fingerprint: otherHash, result_json: '{ "result": true }' },
        ]);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    }),
  ),
);

it.effect("enforces evidence identities, references and record constraints on SQLite", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
    yield* runMigrations({ toMigrationInclusive: 83 });
    yield* seed;
    yield* sql`INSERT INTO workflow_planning_observations VALUES ('pass1', 'op1', 'thread1', 'run1', 'message1', ${hash}, '{}')`;
    yield* sql`INSERT INTO workflow_planning_dispositions VALUES ('pass1', 'finding', 'pass2', '{}')`;
    yield* sql`INSERT INTO workflow_planning_settlements VALUES ('op1', ${hash}, '{}')`;
    const invalid = [
      [
        "observation missing pass",
        `INSERT INTO workflow_planning_observations VALUES ('missing', 'op2', 't', 'r', 'm', '${hash}', '{}')`,
      ],
      [
        "observation missing operation",
        `INSERT INTO workflow_planning_observations VALUES ('pass2', 'missing', 't', 'r', 'm', '${hash}', '{}')`,
      ],
      [
        "duplicate observation pass",
        `INSERT INTO workflow_planning_observations VALUES ('pass1', 'op2', 't', 'r', 'm', '${hash}', '{}')`,
      ],
      [
        "duplicate observation operation",
        `INSERT INTO workflow_planning_observations VALUES ('pass2', 'op1', 't', 'r', 'm', '${hash}', '{}')`,
      ],
      [
        "duplicate native terminal",
        `INSERT INTO workflow_planning_observations VALUES ('pass2', 'op2', 'thread1', 'run1', 'message1', '${hash}', '{}')`,
      ],
      [
        "bad manifest hash",
        "INSERT INTO workflow_planning_observations VALUES ('pass2', 'op2', 't', 'r', 'm', 'bad', '{}')",
      ],
      [
        "uppercase manifest hash",
        `INSERT INTO workflow_planning_observations VALUES ('pass2', 'op2', 't', 'r', 'm', '${"A".repeat(64)}', '{}')`,
      ],
      [
        "invalid observation json",
        `INSERT INTO workflow_planning_observations VALUES ('pass2', 'op2', 't', 'r', 'm', '${hash}', 'bad')`,
      ],
      [
        "disposition missing review",
        "INSERT INTO workflow_planning_dispositions VALUES ('missing', 'other', 'pass2', '{}')",
      ],
      [
        "disposition missing repair",
        "INSERT INTO workflow_planning_dispositions VALUES ('pass1', 'other', 'missing', '{}')",
      ],
      [
        "duplicate finding",
        "INSERT INTO workflow_planning_dispositions VALUES ('pass1', 'finding', 'pass2', '{}')",
      ],
      [
        "empty finding",
        "INSERT INTO workflow_planning_dispositions VALUES ('pass1', '', 'pass2', '{}')",
      ],
      [
        "invalid disposition json",
        "INSERT INTO workflow_planning_dispositions VALUES ('pass1', 'other', 'pass2', 'bad')",
      ],
      [
        "settlement missing operation",
        `INSERT INTO workflow_planning_settlements VALUES ('missing', '${hash}', '{}')`,
      ],
      [
        "duplicate settlement",
        `INSERT INTO workflow_planning_settlements VALUES ('op1', '${hash}', '{}')`,
      ],
      [
        "bad settlement hash",
        "INSERT INTO workflow_planning_settlements VALUES ('op2', 'bad', '{}')",
      ],
      [
        "uppercase settlement hash",
        `INSERT INTO workflow_planning_settlements VALUES ('op2', '${"A".repeat(64)}', '{}')`,
      ],
      [
        "invalid settlement json",
        `INSERT INTO workflow_planning_settlements VALUES ('op2', '${hash}', 'bad')`,
      ],
    ] as const;
    for (const [label, statement] of invalid) {
      const result = yield* Effect.exit(sql.unsafe(statement));
      assert.isTrue(Exit.isFailure(result), label);
      assert.include(String(result), "constraint", label);
    }
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
