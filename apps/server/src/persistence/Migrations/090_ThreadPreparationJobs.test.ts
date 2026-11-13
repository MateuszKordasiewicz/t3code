import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const table = "orchestration_v2_preparation_jobs";
const at = "2026-11-13T00:00:00.000Z";
const hash = "a".repeat(64);
const job = (
  id: string,
  kind: "launch" | "setup" | "title" | "release" = "launch",
  parent: string | null = null,
) => ({
  format_version: 1,
  job_id: id,
  entry_key: `entry-${id}`,
  kind,
  environment_id: "environment",
  project_id: "project",
  thread_id: "thread",
  run_id: null,
  originating_launch_job_id: parent,
  server_boot_id: "server-boot",
  request_identity_json: JSON.stringify({ id, kind }),
  request_identity_hash: hash,
  created_at: at,
});
const settlement = (id: string, priorVersion: number, kind = "code_only") => ({
  v: 1,
  jobId: id,
  entryKey: `entry-${id}`,
  priorVersion,
  kind,
});
const settle = Effect.fn("test.settlePreparationStorage")(function* (
  id: string,
  priorVersion: number,
  kind = "code_only",
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`UPDATE ${sql(table)} SET state='settled',record_version=${priorVersion + 1},code_closed_at=${at},settlement_json=${JSON.stringify(settlement(id, priorVersion, kind))},settlement_hash=${hash},settled_at=${at} WHERE job_id=${id}`;
});
const rows = Effect.fn("test.preparationStorageRows")(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT * FROM ${sql(table)} ORDER BY job_id`;
});
const reject = Effect.fn("test.rejectPreparationStorageMutation")(function* <A, E, R>(
  statement: Effect.Effect<A, E, R>,
) {
  const before = yield* rows();
  assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
  assert.deepStrictEqual(yield* rows(), before);
});
const oldRows = Effect.fn("test.snapshotPrePreparationRows")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql<{
    name: string;
  }>`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'effect_sql_migrations' AND name<>${table} ORDER BY name`;
  const result: Record<string, readonly string[]> = {};
  for (const entry of tables)
    result[entry.name] = (yield* sql`SELECT * FROM ${sql(entry.name)}`).map(stableStringify).sort();
  return result;
});
const oldSchema = Effect.fn("test.snapshotPrePreparationSchema")(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT type,name,sql FROM sqlite_master WHERE tbl_name<>${table} ORDER BY name`;
});

it.effect("adds preparation jobs without changing retained version89 rows", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-preparation-upgrade-" });
    const filename = path.join(directory, "state.sqlite");
    const preserved = yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 89 });
      yield* sql`INSERT INTO workflow_plan_identities VALUES ('old-plan')`;
      yield* sql`INSERT INTO workflow_revision_identities VALUES ('old-plan','old-revision',${hash},${hash})`;
      yield* sql`INSERT INTO workflow_runs VALUES ('old-run','old-plan','old-revision',${hash},${hash},${hash},'active',7,3,'{"retained":true}')`;
      yield* sql`INSERT INTO workflow_native_launch_intents(intent_id,workflow_run_id,operation_id,attempt_id,item_id,charge_attempt,repair,role,environment_id,project_id,launch_command_id,thread_id,message_id,fingerprint,draft_json,generation,state) VALUES ('old-intent','old-run','old-operation','old-attempt','old-item',1,0,'writer','environment','project','old-launch','thread','message',${hash},'{}',3,'reserved')`;
      yield* sql`INSERT INTO workflow_native_intent_receipts VALUES ('old-request','old-receipt','old-intent',${hash},'{}')`;
      yield* sql`INSERT INTO workflow_native_reservation_material VALUES ('old-intent','{}',${hash})`;
      yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status) VALUES ('old-command','thread','thread',${at},1,'accepted')`;
      yield* sql`INSERT INTO workflow_native_command_proofs VALUES ('old-command','old-intent','claim','{}',${hash})`;
      yield* sql`INSERT INTO orchestration_v2_projection_provider_threads(provider_thread_id,thread_id,provider,status,updated_at,payload_json) VALUES ('old-provider','thread','codex','not_loaded',${at},'{}')`;
      yield* sql`INSERT INTO orchestration_v2_effect_outbox(effect_id,command_id,thread_id,effect_type,payload_json,status,available_at,created_at,updated_at) VALUES ('old-effect','old-command','thread','thread-title.generate','{}','pending',${at},${at},${at})`;
      yield* sql`INSERT INTO orchestration_v2_submission_transports(format_version,transport_id,environment_id,project_id,opening_thread_id,driver,provider_instance_id,provider_session_id,host_boot_id,scope_namespace,proposed_unit,creation_context_json,creation_context_hash,creation_input_hash,created_at) VALUES(1,'old-transport','environment','project','thread','codex','instance','session','boot','scope.slice','old-unit.scope','{}',${hash},${hash},${at})`;
      const before = { rows: yield* oldRows(), schema: yield* oldSchema() };
      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 90 }), [
        [90, "ThreadPreparationJobs"],
      ]);
      assert.deepStrictEqual(yield* rows(), []);
      assert.deepStrictEqual(yield* oldRows(), before.rows);
      assert.deepStrictEqual(yield* oldSchema(), before.schema);
      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 90 }), []);
      return before;
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    yield* Effect.gen(function* () {
      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 90 }), []);
      assert.deepStrictEqual(yield* oldRows(), preserved.rows);
      assert.deepStrictEqual(yield* oldSchema(), preserved.schema);
      assert.deepStrictEqual(yield* rows(), []);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("retains preparation identity and monotonic evidence through legal transitions", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-preparation-history-" });
    const filename = path.join(directory, "state.sqlite");
    const retained = yield* Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 90 });
      for (const row of [
        job("launch"),
        job("setup", "setup", "launch"),
        job("title", "title"),
        job("release", "release"),
      ])
        yield* sql`INSERT INTO ${sql(table)} ${sql.insert(row)}`;
      yield* settle("title", 0);
      yield* sql`UPDATE ${sql(table)} SET process_exposed=1,record_version=1 WHERE job_id='setup'`;
      yield* sql`UPDATE ${sql(table)} SET state='quiescing',record_version=2 WHERE job_id='setup'`;
      yield* sql`UPDATE ${sql(table)} SET state='unknown',record_version=3 WHERE job_id='setup'`;
      yield* sql`UPDATE ${sql(table)} SET code_closed_at=${at},record_version=4 WHERE job_id='setup'`;
      yield* settle("setup", 4, "owned_resources");
      yield* sql`UPDATE ${sql(table)} SET state='unknown',record_version=1 WHERE job_id='release'`;
      const result = yield* rows();
      assert.deepStrictEqual(
        result.map((row) => [row.job_id, row.state]),
        [
          ["launch", "active"],
          ["release", "unknown"],
          ["setup", "settled"],
          ["title", "settled"],
        ],
      );
      return result;
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    yield* Effect.gen(function* () {
      yield* runMigrations({ toMigrationInclusive: 90 });
      assert.deepStrictEqual(yield* rows(), retained);
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("rejects malformed, replaced and prematurely settled preparation history", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 90 });
    yield* sql`PRAGMA recursive_triggers=OFF`;
    yield* sql`INSERT INTO ${sql(table)} ${sql.insert(job("parent"))}`;
    yield* sql`INSERT INTO ${sql(table)} ${sql.insert(job("guard"))}`;
    yield* sql`INSERT INTO ${sql(table)} ${sql.insert(job("nonlaunch", "title"))}`;
    for (const patch of [
      { format_version: 2 },
      { job_id: "" },
      { entry_key: "" },
      { kind: "bad" },
      { environment_id: "" },
      { run_id: "" },
      { request_identity_json: "[]" },
      { request_identity_json: "{bad" },
      { request_identity_hash: "A".repeat(64) },
      { record_version: 1 },
      { state: "unknown" },
      { process_exposed: 1 },
      { code_closed_at: at },
      { kind: "setup", originating_launch_job_id: "missing" },
      { kind: "setup", originating_launch_job_id: "nonlaunch" },
      { kind: "setup", originating_launch_job_id: "parent", project_id: "foreign" },
      { originating_launch_job_id: "parent" },
    ])
      yield* reject(sql`INSERT INTO ${sql(table)} ${sql.insert({ ...job("invalid"), ...patch })}`);
    for (const column of [
      "job_id",
      "entry_key",
      "kind",
      "environment_id",
      "project_id",
      "thread_id",
      "run_id",
      "originating_launch_job_id",
      "server_boot_id",
      "request_identity_json",
      "request_identity_hash",
      "created_at",
    ]) {
      const value =
        column === "request_identity_hash"
          ? hash
          : column === "request_identity_json"
            ? "{}"
            : column === "kind"
              ? "launch"
              : column === "originating_launch_job_id"
                ? "parent"
                : "blob-value";
      yield* reject(
        sql`INSERT INTO ${sql(table)} ${sql.insert({ ...job("blob"), [column]: new TextEncoder().encode(value) })}`,
      );
    }
    for (const column of [
      "job_id",
      "entry_key",
      "kind",
      "environment_id",
      "project_id",
      "thread_id",
      "run_id",
      "originating_launch_job_id",
      "server_boot_id",
      "request_identity_json",
      "request_identity_hash",
      "created_at",
    ])
      yield* reject(
        sql`UPDATE ${sql(table)} SET ${sql(column)}='changed',record_version=1 WHERE job_id='guard'`,
      );
    for (const patch of [
      { record_version: -1 },
      { record_version: 0.5 },
      { record_version: 9007199254740992 },
      { record_version: 2 },
      { state: "quiescing" },
      { state: "settled", record_version: 1 },
      { record_version: 1, settlement_json: "{}" },
      { record_version: 1, settlement_hash: hash },
      { record_version: 1, settled_at: at },
    ])
      yield* reject(sql`UPDATE ${sql(table)} SET ${sql.update(patch)} WHERE job_id='guard'`);
    const valid = {
      state: "settled",
      record_version: 1,
      code_closed_at: at,
      settlement_json: JSON.stringify(settlement("guard", 0)),
      settlement_hash: hash,
      settled_at: at,
    };
    for (const column of [
      "code_closed_at",
      "settlement_json",
      "settlement_hash",
      "settled_at",
    ] as const)
      yield* reject(
        sql`UPDATE ${sql(table)} SET ${sql.update({ ...valid, [column]: new TextEncoder().encode(valid[column]) })} WHERE job_id='guard'`,
      );
    for (const wrong of [
      { ...settlement("guard", 0), v: 2 },
      { ...settlement("guard", 0), jobId: "other" },
      { ...settlement("guard", 0), entryKey: "other" },
      { ...settlement("guard", 0), priorVersion: 1 },
      { ...settlement("guard", 0), priorVersion: 0.0 + 0.5 },
      { ...settlement("guard", 0), kind: "unknown" },
    ])
      yield* reject(
        sql`UPDATE ${sql(table)} SET ${sql.update({ ...valid, settlement_json: JSON.stringify(wrong) })} WHERE job_id='guard'`,
      );
    yield* sql`UPDATE ${sql(table)} SET process_exposed=1,record_version=1 WHERE job_id='guard'`;
    yield* sql`UPDATE ${sql(table)} SET code_closed_at=${at},record_version=2 WHERE job_id='guard'`;
    for (const patch of [
      { process_exposed: 0 },
      { code_closed_at: null },
      { code_closed_at: "changed" },
      { state: "active", process_exposed: -1 },
    ])
      yield* reject(
        sql`UPDATE ${sql(table)} SET ${sql.update({ ...patch, record_version: 3 })} WHERE job_id='guard'`,
      );
    yield* reject(settle("guard", 2));
    yield* sql`UPDATE ${sql(table)} SET state='unknown',record_version=3 WHERE job_id='guard'`;
    yield* reject(
      sql`UPDATE ${sql(table)} SET state='active',record_version=4 WHERE job_id='guard'`,
    );
    yield* settle("guard", 3, "owned_resources");
    for (const statement of [
      sql`UPDATE ${sql(table)} SET record_version=5 WHERE job_id='guard'`,
      sql`DELETE FROM ${sql(table)} WHERE job_id='guard'`,
      sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert(job("guard"))}`,
      sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert({ ...job("replacement"), entry_key: "entry-guard" })}`,
      sql`INSERT INTO ${sql(table)} ${sql.insert(job("guard"))} ON CONFLICT(job_id) DO UPDATE SET state='active'`,
      sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert({ rowid: 1, ...job("rowid-alias") })}`,
    ])
      yield* reject(statement);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("uses preparation history indexes for exact resource lookup", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 90 });
    yield* sql`INSERT INTO ${sql(table)} ${sql.insert(job("parent"))}`;
    for (const id of ["child-a", "child-b", "child-c"])
      yield* sql`INSERT INTO ${sql(table)} ${sql.insert(job(id, "setup", "parent"))}`;
    yield* sql`UPDATE ${sql(table)} SET state='unknown',record_version=1 WHERE job_id='child-b'`;
    yield* settle("child-c", 0);
    for (const key of ["thread", "absent"]) {
      const query = sql`SELECT * FROM ${sql(table)} WHERE environment_id='environment' AND thread_id=${key} AND job_id>'' ORDER BY job_id LIMIT 2`;
      const plan = yield* sql<{ detail: string }>`EXPLAIN QUERY PLAN ${query}`;
      assert.isTrue(
        plan.some(
          (row) =>
            row.detail.includes("SEARCH") && row.detail.includes("preparation_jobs_thread_history"),
        ),
      );
      assert.isFalse(plan.some((row) => /SCAN|TEMP B-TREE/.test(row.detail)));
      assert.deepStrictEqual(
        (yield* query).map((row) => row.job_id),
        key === "thread" ? ["child-a", "child-b"] : [],
      );
    }
    for (const parent of ["parent", "absent"]) {
      const query = sql`SELECT * FROM ${sql(table)} WHERE originating_launch_job_id=${parent} AND job_id>'child-a' ORDER BY job_id`;
      const plan = yield* sql<{ detail: string }>`EXPLAIN QUERY PLAN ${query}`;
      assert.isTrue(
        plan.some(
          (row) =>
            row.detail.includes("SEARCH") && row.detail.includes("preparation_jobs_origin_history"),
        ),
      );
      assert.isFalse(plan.some((row) => /SCAN|TEMP B-TREE/.test(row.detail)));
      assert.deepStrictEqual(
        (yield* query).map((row) => row.job_id),
        parent === "parent" ? ["child-b", "child-c"] : [],
      );
    }
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
