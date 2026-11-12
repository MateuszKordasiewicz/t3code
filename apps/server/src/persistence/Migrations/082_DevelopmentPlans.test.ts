import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "../Migrations.ts";

const layer = NodeSqliteClient.layer({ filename: ":memory:" });
const hash = "a".repeat(64);
const otherHash = "b".repeat(64);

it.effect("upgrades an 081 database without changing workflow rows or history", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* runMigrations({ toMigrationInclusive: 81 });
    yield* sql`INSERT INTO workflow_plan_identities VALUES ('p1')`;
    yield* sql`INSERT INTO workflow_revision_identities VALUES ('p1', 'rev', ${hash}, ${otherHash})`;
    yield* sql`INSERT INTO workflow_runs VALUES ('run', 'p1', 'rev', ${hash}, ${otherHash}, ${hash}, 'active', 3, 4, '{"key":"value"}')`;
    const plans = yield* sql`SELECT * FROM workflow_plan_identities`;
    const revisions = yield* sql`SELECT * FROM workflow_revision_identities`;
    const runs = yield* sql`SELECT * FROM workflow_runs`;
    const history =
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`;
    assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 82 }), [
      [82, "DevelopmentPlans"],
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_plan_identities`, plans);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_revision_identities`, revisions);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, runs);
    assert.deepStrictEqual(
      yield* sql`SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id`,
      [...history, { migration_id: 82, name: "DevelopmentPlans" }],
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("enforces development plan bindings and constraints on fresh SQLite", () =>
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
    yield* sql`BEGIN`;
    yield* sql`INSERT INTO workflow_plan_identities VALUES ('p1'), ('p2')`;
    yield* sql`INSERT INTO development_plans VALUES ('p1', 'env', 'project', 'Plan', NULL, 0, 'created'), ('p2', 'env', 'project', 'Plan 2', NULL, 0, 'created')`;
    yield* sql`INSERT INTO development_plan_source_packets VALUES ('p1', ${hash}, '{}'), ('p2', ${otherHash}, '{}')`;
    yield* sql`INSERT INTO workflow_revision_identities VALUES ('p1', 'rev', ${hash}, ${hash}), ('p1', 'missing', ${hash}, ${hash}), ('p1', 'cross', ${hash}, ${otherHash})`;
    yield* sql`INSERT INTO development_plan_revisions VALUES ('p1', 'rev', ${hash}, ${hash}, '{}', 'created')`;
    yield* sql`UPDATE development_plans SET current_revision_id = 'rev' WHERE plan_id = 'p1'`;
    yield* sql`COMMIT`;
    yield* sql`INSERT INTO development_plan_threads VALUES ('p1', 'thread')`;
    yield* sql`INSERT INTO development_plan_commands VALUES ('command', 'p1', ${hash}, '{}')`;
    assert.deepStrictEqual(
      yield* sql`SELECT current_revision_id FROM development_plans WHERE plan_id = 'p1'`,
      [{ current_revision_id: "rev" }],
    );

    const invalid = [
      [
        "current identity without payload",
        "UPDATE development_plans SET current_revision_id = 'missing' WHERE plan_id = 'p1'",
      ],
      [
        "current revision from another plan",
        "UPDATE development_plans SET current_revision_id = 'rev' WHERE plan_id = 'p2'",
      ],
      [
        "payload identity hashes mismatch",
        `INSERT INTO development_plan_revisions VALUES ('p1', 'missing', '${otherHash}', '${hash}', '{}', 'created')`,
      ],
      [
        "payload packet from another plan",
        `INSERT INTO development_plan_revisions VALUES ('p1', 'cross', '${hash}', '${otherHash}', '{}', 'created')`,
      ],
      [
        "payload missing identity",
        `INSERT INTO development_plan_revisions VALUES ('p1', 'absent', '${hash}', '${hash}', '{}', 'created')`,
      ],
      [
        "plan missing identity",
        "INSERT INTO development_plans VALUES ('absent', 'env', 'project', 'Plan', NULL, 0, 'created')",
      ],
      [
        "duplicate plan",
        "INSERT INTO development_plans VALUES ('p1', 'env', 'project', 'Plan', NULL, 0, 'created')",
      ],
      [
        "duplicate packet",
        `INSERT INTO development_plan_source_packets VALUES ('p1', '${hash}', '{}')`,
      ],
      [
        "duplicate revision",
        `INSERT INTO development_plan_revisions VALUES ('p1', 'rev', '${hash}', '${hash}', '{}', 'created')`,
      ],
      ["duplicate thread", "INSERT INTO development_plan_threads VALUES ('p1', 'thread')"],
      [
        "duplicate command",
        `INSERT INTO development_plan_commands VALUES ('command', 'p1', '${hash}', '{}')`,
      ],
      ["bad packet hash", "INSERT INTO development_plan_source_packets VALUES ('p1', 'bad', '{}')"],
      [
        "uppercase command hash",
        `INSERT INTO development_plan_commands VALUES ('bad', 'p1', '${"A".repeat(64)}', '{}')`,
      ],
      [
        "invalid packet json",
        `INSERT INTO development_plan_source_packets VALUES ('p1', '${otherHash}', 'bad')`,
      ],
      [
        "invalid revision json",
        `INSERT INTO development_plan_revisions VALUES ('p1', 'missing', '${hash}', '${hash}', 'bad', 'created')`,
      ],
      [
        "invalid command json",
        `INSERT INTO development_plan_commands VALUES ('bad', 'p1', '${hash}', 'bad')`,
      ],
      ["negative version", "UPDATE development_plans SET version = -1 WHERE plan_id = 'p1'"],
      ["delete referenced plan", "DELETE FROM development_plans WHERE plan_id = 'p1'"],
    ] as const;
    for (const [label, statement] of invalid) {
      const result = yield* Effect.exit(sql.unsafe(statement));
      assert.isTrue(Exit.isFailure(result), label);
      assert.include(String(result), "constraint", label);
    }
    assert.strictEqual(
      (yield* sql`SELECT * FROM development_plans WHERE plan_id = 'p1'`).length,
      1,
    );
    assert.strictEqual(
      (yield* sql`SELECT * FROM development_plan_revisions WHERE plan_id = 'p1'`).length,
      1,
    );
  }).pipe(Effect.provide(layer)),
);
