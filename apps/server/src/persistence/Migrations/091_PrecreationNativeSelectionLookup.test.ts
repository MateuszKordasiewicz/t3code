import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../Migrations.ts";

const at = "2026-11-13T00:00:00.000Z";
const hash = "a".repeat(64);
const names = ["preparation_jobs_native_history", "workflow_native_material_native_history"];
const tuple = (driver = "codex", providerInstanceId = "instance", nativeId = "target") => ({
  driver,
  providerInstanceId,
  nativeId,
});
const seed = Effect.fn("test.seedPrecreationLookup")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO workflow_plan_identities VALUES ('plan')`;
  yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan','revision',${hash},${hash})`;
  yield* sql`INSERT INTO workflow_runs VALUES ('run','plan','revision',${hash},${hash},${hash},'active',0,0,'{}')`;
  yield* sql`INSERT INTO workflow_planning_sessions VALUES ('session','plan',NULL,'active',0,0,'{}')`;
  for (const [id, environment, selection, state] of [
    ["job-a", "environment", tuple(), "active"],
    ["job-b", "environment", tuple(), "unknown"],
    ["job-c", "environment", tuple(), "settled"],
    ["job-env", "foreign-environment", tuple(), "active"],
    ["job-driver", "environment", tuple("other-driver"), "active"],
    ["job-instance", "environment", tuple("codex", "other-instance"), "active"],
    ["job-native", "environment", tuple("codex", "instance", "other-native"), "active"],
    ["job-null", "environment", null, "active"],
    ["job-v1", "environment", undefined, "active"],
  ] as const) {
    const identity = selection === undefined ? { v: 1 } : { v: 2, nativeSelection: selection };
    yield* sql`INSERT INTO orchestration_v2_preparation_jobs(format_version,job_id,entry_key,kind,environment_id,project_id,thread_id,server_boot_id,request_identity_json,request_identity_hash,created_at) VALUES (1,${id},${id},'launch',${environment},'project',${`thread-${id}`},'boot',${JSON.stringify(identity)},${hash},${at})`;
    if (state === "unknown")
      yield* sql`UPDATE orchestration_v2_preparation_jobs SET state='unknown',record_version=1 WHERE job_id=${id}`;
    if (state === "settled")
      yield* sql`UPDATE orchestration_v2_preparation_jobs SET state='settled',record_version=1,code_closed_at=${at},settlement_json=${JSON.stringify({ v: 1, jobId: id, entryKey: id, priorVersion: 0, kind: "code_only" })},settlement_hash=${hash},settled_at=${at} WHERE job_id=${id}`;
  }
  for (const [id, owner, environment, selection, state, version] of [
    ["material-a", "workflow", "environment", tuple(), "reserved", 2],
    ["material-b", "planning", "environment", tuple(), "cancelled", 2],
    ["material-c", "workflow", "environment", tuple(), "finalized", 2],
    ["material-p", "planning", "environment", tuple(), "finalized", 2],
    ["material-env", "planning", "foreign-environment", tuple(), "reserved", 2],
    ["material-driver", "workflow", "environment", tuple("other-driver"), "reserved", 2],
    [
      "material-instance",
      "planning",
      "environment",
      tuple("codex", "other-instance"),
      "reserved",
      2,
    ],
    [
      "material-native",
      "workflow",
      "environment",
      tuple("codex", "instance", "other-native"),
      "reserved",
      2,
    ],
    ["material-null", "planning", "environment", null, "reserved", 2],
    ["material-v1", "workflow", "environment", tuple(), "cancelled", 1],
  ] as const) {
    const workflow = owner === "workflow";
    const operation = `operation-${id}`;
    if (state === "finalized")
      yield* sql`INSERT INTO workflow_operations(id,workflow_run_id,planning_session_id,kind,fingerprint,status,generation,record_json) VALUES (${operation},${workflow ? "run" : null},${workflow ? null : "session"},${workflow ? "supervision" : "planning_pass"},${hash},'settled',0,'{}')`;
    yield* sql`INSERT INTO workflow_native_launch_intents(intent_id,workflow_run_id,planning_session_id,operation_id,pass_id,charge_attempt,repair,role,environment_id,project_id,launch_command_id,thread_id,message_id,fingerprint,draft_json,generation,state,native_run_id,finalized_operation_id) VALUES (${id},${workflow ? "run" : null},${workflow ? null : "session"},${operation},${workflow ? null : `pass-${id}`},0,0,${workflow ? "supervisor" : "author"},${environment},'project',${`launch-${id}`},${`thread-${id}`},${`message-${id}`},${hash},'{}',0,${state},${state === "finalized" ? `native-${id}` : null},${state === "finalized" ? operation : null})`;
    const launch =
      selection === null
        ? { modelSelection: { instanceId: "instance" } }
        : {
            modelSelection: { instanceId: selection.providerInstanceId },
            importedNativeThread: {
              ref: { driver: selection.driver, nativeId: selection.nativeId, strength: "strong" },
            },
          };
    yield* sql`INSERT INTO workflow_native_reservation_material VALUES (${id},${JSON.stringify({ version, launch })},${hash})`;
  }
});
const snapshotRows = Effect.fn("test.snapshotPrecreationLookupRows")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql<{
    name: string;
  }>`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'effect_sql_migrations' ORDER BY name`;
  const result: Record<string, readonly string[]> = {};
  for (const table of tables)
    result[table.name] = (yield* sql`SELECT * FROM ${sql(table.name)}`).map(stableStringify).sort();
  return result;
});
const snapshotSchema = Effect.fn("test.snapshotPrecreationLookupSchema")(function* () {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<{
    type: string;
    name: string;
    sql: string | null;
  }>`SELECT type,name,sql FROM sqlite_master ORDER BY name`).filter(
    (row) => !names.includes(row.name),
  );
});
const lookups = Effect.fn("test.precreationNativeLookups")(function* (
  environment = "environment",
  selection = tuple(),
  after = "",
) {
  const sql = yield* SqlClient.SqlClient;
  const queries = [
    sql`SELECT * FROM orchestration_v2_preparation_jobs WHERE environment_id=${environment} AND CASE WHEN json_valid(request_identity_json) THEN json_extract(request_identity_json,'$.nativeSelection.driver') END=${selection.driver} AND CASE WHEN json_valid(request_identity_json) THEN json_extract(request_identity_json,'$.nativeSelection.providerInstanceId') END=${selection.providerInstanceId} AND CASE WHEN json_valid(request_identity_json) THEN json_extract(request_identity_json,'$.nativeSelection.nativeId') END=${selection.nativeId} AND job_id>${after} ORDER BY job_id LIMIT 2`,
    sql`SELECT m.*,i.environment_id FROM workflow_native_reservation_material m JOIN workflow_native_launch_intents i ON i.intent_id=m.intent_id WHERE CASE WHEN json_valid(m.material_json) THEN json_extract(m.material_json,'$.launch.importedNativeThread.ref.driver') END=${selection.driver} AND CASE WHEN json_valid(m.material_json) THEN json_extract(m.material_json,'$.launch.modelSelection.instanceId') END=${selection.providerInstanceId} AND CASE WHEN json_valid(m.material_json) THEN json_extract(m.material_json,'$.launch.importedNativeThread.ref.nativeId') END=${selection.nativeId} AND i.environment_id=${environment} AND m.intent_id>${after} ORDER BY m.intent_id LIMIT 2`,
  ];
  return yield* Effect.forEach(queries, (query) =>
    Effect.gen(function* () {
      const plan = yield* sql<{ detail: string }>`EXPLAIN QUERY PLAN ${query}`;
      return { rows: yield* query, plan: plan.map((row) => row.detail) };
    }),
  );
});
const pages = Effect.fn("test.precreationLookupPages")(function* (
  environment = "environment",
  selection = tuple(),
) {
  const result: unknown[][] = [[], []];
  const cursors = ["", ""];
  const done = [false, false];
  while (done.some((value) => !value)) {
    for (let index = 0; index < 2; index++) {
      if (done[index]) continue;
      const query = (yield* lookups(environment, selection, cursors[index]))[index]!;
      result[index]!.push(...query.rows);
      if (query.rows.length < 2) done[index] = true;
      else {
        const row = query.rows.at(-1)!;
        const next = index === 0 ? row.job_id : row.intent_id;
        assert.isString(next);
        assert.notStrictEqual(next, cursors[index]);
        cursors[index] = next as string;
      }
    }
  }
  return result;
});
it.effect("indexes precreation selection history through version90 upgrade replay and reopen", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-precreation-lookup-" });
    const filename = path.join(directory, "state.sqlite");
    const preserved = yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 90 });
      yield* seed();
      const before = {
        rows: yield* snapshotRows(),
        schema: yield* snapshotSchema(),
        pages: yield* pages(),
      };
      for (const query of yield* lookups())
        assert.isFalse(query.plan.some((detail) => names.some((name) => detail.includes(name))));
      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 91 }), [
        [91, "PrecreationNativeSelectionLookup"],
      ]);
      assert.deepStrictEqual(yield* snapshotRows(), before.rows);
      assert.deepStrictEqual(yield* snapshotSchema(), before.schema);
      assert.deepStrictEqual(yield* pages(), before.pages);
      assert.deepStrictEqual(
        before.pages.map((rows) =>
          rows.map(
            (row) =>
              (row as Record<string, unknown>).job_id ?? (row as Record<string, unknown>).intent_id,
          ),
        ),
        [
          ["job-a", "job-b", "job-c"],
          ["material-a", "material-b", "material-c", "material-p", "material-v1"],
        ],
      );
      for (const [environment, selection, expected] of [
        [
          "environment",
          tuple(),
          [
            ["job-a", "job-b", "job-c"],
            ["material-a", "material-b", "material-c", "material-p", "material-v1"],
          ],
        ],
        ["foreign-environment", tuple(), [["job-env"], ["material-env"]]],
        ["environment", tuple("other-driver"), [["job-driver"], ["material-driver"]]],
        [
          "environment",
          tuple("codex", "other-instance"),
          [["job-instance"], ["material-instance"]],
        ],
        [
          "environment",
          tuple("codex", "instance", "other-native"),
          [["job-native"], ["material-native"]],
        ],
        ["environment", tuple("codex", "instance", "absent"), [[], []]],
        ["absent-environment", tuple(), [[], []]],
      ] as const) {
        const found = yield* pages(environment, selection);
        assert.deepStrictEqual(
          found.map((rows) =>
            rows.map(
              (row) =>
                (row as Record<string, unknown>).job_id ??
                (row as Record<string, unknown>).intent_id,
            ),
          ),
          expected.map((group) => [...group]),
        );
        for (const after of ["", "job-a", "material-b"]) {
          const queries = yield* lookups(environment, selection, after);
          for (const [index, query] of queries.entries()) {
            assert.isTrue(
              query.plan.some(
                (detail) => detail.includes("SEARCH") && detail.includes(names[index]!),
              ),
              query.plan.join("\n"),
            );
            assert.isFalse(
              query.plan.some((detail) => /SCAN|TEMP B-TREE/.test(detail)),
              query.plan.join("\n"),
            );
          }
          assert.isTrue(
            queries[1]!.plan.some(
              (detail) =>
                detail.includes("workflow_native_launch_intents") && detail.includes("intent_id=?"),
            ),
            queries[1]!.plan.join("\n"),
          );
        }
      }
      const indexes = yield* sql<{
        name: string;
        sql: string;
      }>`SELECT name,sql FROM sqlite_master WHERE type='index' ORDER BY name`;
      for (const name of names) {
        const index = indexes.find((row) => row.name === name);
        assert.isDefined(index);
        assert.isFalse(/\bWHERE\b/i.test(index!.sql));
      }
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 91 }), []);
      return before;
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    yield* Effect.gen(function* () {
      yield* runMigrations({ toMigrationInclusive: 91 });
      assert.deepStrictEqual(yield* snapshotRows(), preserved.rows);
      assert.deepStrictEqual(yield* snapshotSchema(), preserved.schema);
      assert.deepStrictEqual(yield* pages(), preserved.pages);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
