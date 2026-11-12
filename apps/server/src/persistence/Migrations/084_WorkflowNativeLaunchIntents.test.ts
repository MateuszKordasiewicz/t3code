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
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO workflow_plan_identities VALUES ('plan')`;
  yield* sql`INSERT INTO development_plans VALUES ('plan', 'env', 'project', 'Plan', NULL, 7, 'created')`;
  yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan', 'revision', ${hash}, ${hash})`;
  yield* sql`INSERT INTO workflow_runs VALUES ('run', 'plan', 'revision', ${hash}, ${hash}, ${hash}, 'active', 12, 9, '{ "attempts": 4, "repairs": 2 }')`;
  yield* sql`INSERT INTO workflow_planning_sessions VALUES ('session', 'plan', NULL, 'active', 5, 6, '{ "passes": 3 }')`;
  yield* sql`INSERT INTO workflow_operations VALUES ('op-final', 'run', NULL, 'stage_launch', ${hash}, 'pending', 0, 0, 9, NULL, '{ "native": "old" }')`;
  yield* sql`INSERT INTO workflow_command_receipts VALUES ('old-command', 'run', NULL, ${hash}, 'old-receipt', 'op-final', '{ "old": true }')`;
  yield* sql`INSERT INTO workflow_attempts VALUES ('old-attempt', 'run', 'op-final', 'old-item', 1)`;
  yield* sql`INSERT INTO workflow_operations VALUES ('planning-op', NULL, 'session', 'planning_pass', ${hash}, 'settled', 0, 0, 6, NULL, '{ "planning": true }')`;
  yield* sql`INSERT INTO workflow_planning_passes VALUES ('old-pass', 'session', 'plan', 'revision', ${hash}, ${hash}, 'planning-op', 'old-thread', 'old-run', '{ "pass": true }')`;
  yield* sql`INSERT INTO workflow_planning_observations VALUES ('old-pass', 'planning-op', 'old-thread', 'old-run', 'old-message', ${hash}, '{ "evidence": true }')`;
  yield* sql`INSERT INTO workflow_planning_settlements VALUES ('planning-op', ${hash}, '{ "settled": true }')`;
});

const row = (id: string, overrides: Record<string, string | number | null> = {}) => ({
  intent_id: id,
  workflow_run_id: "run",
  planning_session_id: null,
  operation_id: `op-${id}`,
  attempt_id: `attempt-${id}`,
  pass_id: null,
  item_id: `item-${id}`,
  charge_attempt: 1,
  repair: 0,
  role: "implementation",
  environment_id: "env",
  project_id: "project",
  launch_command_id: `launch-${id}`,
  thread_id: "thread",
  message_id: `message-${id}`,
  fingerprint: hash,
  draft_json: '{ "draft": true }',
  generation: 9,
  state: "reserved",
  held: 0,
  boot_id: null,
  worker_id: null,
  native_run_id: null,
  finalized_operation_id: null,
  ...overrides,
});

const insert = (values: ReturnType<typeof row>) => {
  const literal = (value: string | number | null) =>
    value === null
      ? "NULL"
      : typeof value === "number"
        ? String(value)
        : `'${value.replaceAll("'", "''")}'`;
  return `INSERT INTO workflow_native_launch_intents (${Object.keys(values).join(",")}) VALUES (${Object.values(values).map(literal).join(",")})`;
};

it.effect("upgrades seeded 083 without changing legacy rows, replays, and survives reopen", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const dir = yield* Effect.acquireRelease(
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-intents-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(dir, "test.sqlite");
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA foreign_keys = ON`;
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
        yield* runMigrations({ toMigrationInclusive: 83 });
        yield* seed;
        const runs = yield* sql`SELECT * FROM workflow_runs`;
        const sessions = yield* sql`SELECT * FROM workflow_planning_sessions`;
        const operations = yield* sql`SELECT * FROM workflow_operations ORDER BY id`;
        const attempts = yield* sql`SELECT * FROM workflow_attempts`;
        const passes = yield* sql`SELECT * FROM workflow_planning_passes`;
        const observations = yield* sql`SELECT * FROM workflow_planning_observations`;
        const settlements = yield* sql`SELECT * FROM workflow_planning_settlements`;
        const receipts = yield* sql`SELECT * FROM workflow_command_receipts`;
        const plans = yield* sql`SELECT * FROM development_plans`;
        assert.deepStrictEqual(yield* runMigrations(), [[84, "WorkflowNativeLaunchIntents"]]);
        assert.deepStrictEqual(yield* runMigrations(), []);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, runs);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, sessions);
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM workflow_operations ORDER BY id`,
          operations,
        );
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_attempts`, attempts);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_passes`, passes);
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM workflow_planning_observations`,
          observations,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM workflow_planning_settlements`,
          settlements,
        );
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_command_receipts`, receipts);
        assert.deepStrictEqual(yield* sql`SELECT * FROM development_plans`, plans);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
        yield* sql.unsafe(insert(row("one")));
        yield* sql`INSERT INTO workflow_native_intent_receipts VALUES ('command', 'receipt', 'one', ${hash}, '{ "reserved": true }')`;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`PRAGMA foreign_keys = ON`;
        assert.deepStrictEqual(yield* runMigrations(), []);
        assert.deepStrictEqual(
          yield* sql`SELECT record_json, record_version, control_generation FROM workflow_runs`,
          [
            {
              record_json: '{ "attempts": 4, "repairs": 2 }',
              record_version: 12,
              control_generation: 9,
            },
          ],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT intent_id, draft_json FROM workflow_native_launch_intents`,
          [{ intent_id: "one", draft_json: '{ "draft": true }' }],
        );
        assert.deepStrictEqual(
          yield* sql`SELECT result_json FROM workflow_native_intent_receipts`,
          [{ result_json: '{ "reserved": true }' }],
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    }),
  ),
);

it.effect(
  "enforces binding and receipt constraints while allowing valid owners and reused threads",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_keys`, [{ foreign_keys: 1 }]);
      yield* runMigrations();
      yield* seed;
      const valid = [
        row("one", { native_run_id: "native-one" }),
        row("two", { native_run_id: "native-two" }),
        row("review", { attempt_id: "attempt-one", item_id: "item-one", charge_attempt: 0 }),
        row("supervision", { attempt_id: null, item_id: null, charge_attempt: 0 }),
        row("planning", {
          workflow_run_id: null,
          planning_session_id: "session",
          attempt_id: null,
          item_id: null,
          pass_id: "pass",
          charge_attempt: 0,
          repair: 1,
        }),
        row("final", {
          operation_id: "op-final",
          state: "finalized",
          native_run_id: "native-final",
          finalized_operation_id: "op-final",
        }),
        row("held", { held: 1 }),
        row("dispatch", { state: "dispatching", boot_id: "boot", worker_id: "worker" }),
        row("unknown", { state: "unknown", boot_id: "boot", worker_id: "worker" }),
      ];
      for (const value of valid) yield* sql.unsafe(insert(value));
      yield* sql`INSERT INTO workflow_native_intent_receipts VALUES ('command', 'receipt', 'planning', ${hash}, '{}')`;
      const invalid: Array<[string, Record<string, string | number | null>]> = [
        ["both owners", { planning_session_id: "session" }],
        ["no owner", { workflow_run_id: null }],
        ["missing workflow", { workflow_run_id: "missing" }],
        [
          "missing session",
          {
            workflow_run_id: null,
            planning_session_id: "missing",
            attempt_id: null,
            item_id: null,
            pass_id: "other",
            charge_attempt: 0,
          },
        ],
        ["workflow pass", { pass_id: "pass-other" }],
        ["partial attempt", { item_id: null }],
        ["empty attempt", { attempt_id: "" }],
        ["charge without attempt", { attempt_id: null, item_id: null }],
        ["noncharging repair", { charge_attempt: 0, repair: 1 }],
        [
          "planning attempt",
          {
            workflow_run_id: null,
            planning_session_id: "session",
            pass_id: "new-pass",
            charge_attempt: 0,
          },
        ],
        [
          "planning no pass",
          {
            workflow_run_id: null,
            planning_session_id: "session",
            attempt_id: null,
            item_id: null,
            charge_attempt: 0,
          },
        ],
        [
          "duplicate pass",
          {
            workflow_run_id: null,
            planning_session_id: "session",
            attempt_id: null,
            item_id: null,
            pass_id: "pass",
            charge_attempt: 0,
          },
        ],
        ["duplicate intent", { intent_id: "one" }],
        ["duplicate operation", { operation_id: "op-one" }],
        ["duplicate charged attempt", { attempt_id: "attempt-one" }],
        ["duplicate launch", { launch_command_id: "launch-one" }],
        ["duplicate thread message", { message_id: "message-one" }],
        ["duplicate native run", { native_run_id: "native-one" }],
        ["empty intent", { intent_id: "" }],
        ["empty operation", { operation_id: "" }],
        ["empty role", { role: "" }],
        ["empty environment", { environment_id: "" }],
        ["empty project", { project_id: "" }],
        ["empty launch", { launch_command_id: "" }],
        ["empty thread", { thread_id: "" }],
        ["empty message", { message_id: "" }],
        ["empty native", { native_run_id: "" }],
        ["short fingerprint", { fingerprint: "bad" }],
        ["uppercase fingerprint", { fingerprint: "A".repeat(64) }],
        ["malformed draft", { draft_json: "bad" }],
        ["array draft", { draft_json: "[]" }],
        ["negative generation", { generation: -1 }],
        ["fractional generation", { generation: 1.5 }],
        ["unsafe generation", { generation: 9007199254740992 }],
        ["bad state", { state: "bad" }],
        ["bad repair", { repair: 2 }],
        ["bad held", { held: 2 }],
        ["held dispatch", { held: 1, state: "dispatching", boot_id: "boot", worker_id: "worker" }],
        ["dispatch without worker", { state: "dispatching" }],
        ["unknown without worker", { state: "unknown" }],
        ["unpaired worker", { boot_id: "boot" }],
        ["empty worker", { boot_id: "boot", worker_id: "" }],
        ["final without both", { state: "finalized" }],
        [
          "final without run",
          {
            state: "finalized",
            operation_id: "planning-op",
            finalized_operation_id: "planning-op",
          },
        ],
        ["final without operation", { state: "finalized", native_run_id: "unique-native" }],
        [
          "final mismatched operation",
          {
            state: "finalized",
            native_run_id: "unique-native",
            finalized_operation_id: "planning-op",
          },
        ],
        [
          "final missing operation FK",
          {
            state: "finalized",
            operation_id: "missing",
            finalized_operation_id: "missing",
            native_run_id: "unique-native",
          },
        ],
        ["nonfinal operation", { finalized_operation_id: "op-final" }],
      ];
      for (const [index, [label, overrides]] of invalid.entries()) {
        const result = yield* Effect.exit(sql.unsafe(insert(row(`invalid-${index}`, overrides))));
        assert.isTrue(Exit.isFailure(result), label);
      }
      const invalidReceipts = [
        `('command','other','one','${hash}','{}')`,
        `('other','receipt','one','${hash}','{}')`,
        `('other','other','planning','${hash}','{}')`,
        `('other','other','missing','${hash}','{}')`,
        `('','other','one','${hash}','{}')`,
        `('other','','one','${hash}','{}')`,
        `('other','other','one','bad','{}')`,
        `('other','other','one','${"A".repeat(64)}','{}')`,
        `('other','other','one','${hash}','[]')`,
        `('other','other','one','${hash}','bad')`,
      ];
      for (const values of invalidReceipts) {
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              sql.unsafe(`INSERT INTO workflow_native_intent_receipts VALUES ${values}`),
            ),
          ),
          values,
        );
      }
      assert.strictEqual(
        (yield* sql`SELECT * FROM workflow_native_launch_intents`).length,
        valid.length,
      );
      assert.strictEqual((yield* sql`SELECT * FROM workflow_native_intent_receipts`).length, 1);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
