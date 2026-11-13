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

const at = "2026-11-13T18:00:00.000Z";
const hash = "a".repeat(64);
const names = [
  "submission_transports_thread_history",
  "submission_transports_session_history",
  "submission_attempts_thread_history",
  "submission_attempts_provider_thread_history",
  "submission_attempts_native_history",
  "orchestration_v2_provider_threads_native_lookup",
];
const transport = (id: string, environment = "environment", instance = "instance") => ({
  format_version: 1,
  transport_id: id,
  environment_id: environment,
  project_id: "project",
  opening_thread_id: "target",
  driver: "codex",
  provider_instance_id: instance,
  provider_session_id: "target",
  host_boot_id: "boot",
  scope_namespace: "scope.slice",
  proposed_unit: `${id}.scope`,
  creation_context_json: "{}",
  creation_context_hash: hash,
  creation_input_hash: hash,
  created_at: at,
});
const binding = (id: string) => ({
  unit: `${id}.scope`,
  invocationId: "b".repeat(32),
  controlGroup: `/fixture/${id}.scope`,
  pid: 101,
  pidStartTicks: "9007199254740993",
});
const attempt = (id: string, parent: ReturnType<typeof transport>) => ({
  format_version: 1,
  attempt_id: id,
  transport_id: parent.transport_id,
  environment_id: parent.environment_id,
  project_id: "project",
  thread_id: "target",
  run_id: `run-${id}`,
  run_ordinal: 1,
  root_node_id: `root-${id}`,
  provider_turn_ordinal: 1,
  driver: parent.driver,
  provider_instance_id: parent.provider_instance_id,
  provider_session_id: parent.provider_session_id,
  provider_thread_id: "target",
  native_thread_id: "target",
  method: "turn/start",
  selection_json: "{}",
  selection_hash: hash,
  input_hash: hash,
  frame_hash: hash,
  request_id_json: '"request"',
  created_at: at,
});
const seed = Effect.fn("test.seedSubmissionLookup")(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [id, environment, instance] of [
    ["transport-a", "environment", "instance"],
    ["transport-b", "environment", "instance"],
    ["transport-c", "environment", "instance"],
    ["transport-d", "foreign-environment", "instance"],
    ["transport-e", "environment", "foreign-instance"],
  ] as const) {
    const row = transport(id, environment, instance);
    yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
    if (id === "transport-a") continue;
    yield* sql`UPDATE orchestration_v2_submission_transports SET binding_json=${JSON.stringify(binding(id))},binding_hash=${hash},bound_at=${at} WHERE transport_id=${id}`;
    const child = attempt(`attempt-${id.slice(-1)}1`, row);
    yield* sql`INSERT INTO orchestration_v2_submission_attempts ${sql.insert(child)}`;
    if (id === "transport-b") {
      yield* sql`INSERT INTO orchestration_v2_submission_attempts ${sql.insert(attempt("attempt-b2", row))}`;
      yield* sql`UPDATE orchestration_v2_submission_attempts SET local_outcome='not_submitted',local_reason='wrapper',local_observed_at=${at} WHERE attempt_id='attempt-b2'`;
    }
    if (id === "transport-c") {
      yield* sql`UPDATE orchestration_v2_submission_attempts SET local_outcome='unknown',local_reason='sink',local_observed_at=${at} WHERE attempt_id=${child.attempt_id}`;
      yield* sql`UPDATE orchestration_v2_submission_transports SET settlement_json=${JSON.stringify({ binding: binding(id), observation: "removed" })},settlement_hash=${hash},settled_at=${at} WHERE transport_id=${id}`;
    }
    if (id === "transport-d")
      yield* sql`UPDATE orchestration_v2_submission_attempts SET local_outcome='submitted',local_reason='completed',local_observed_at=${at} WHERE attempt_id=${child.attempt_id}`;
  }
  for (const [id, payload] of [
    [
      "provider-a",
      '{ "nativeThreadRef": { "nativeId": "target", "driver": "codex", "strength": "strong" }, "driver": "codex", "providerInstanceId": "instance" }',
    ],
    [
      "provider-b",
      '{"nativeThreadRef":{"nativeId":"target","driver":"codex","strength":"strong"},"driver":"codex","providerInstanceId":"foreign-instance"}',
    ],
    ["provider-c", '{"nativeThreadRef":{"nativeId":"other","driver":"codex","strength":"strong"}}'],
    ["provider-malformed", "{malformed JSON"],
    ["provider-null", "null"],
    ["provider-array", "[]"],
  ]) {
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads(provider_thread_id,thread_id,owner_node_id,provider,provider_session_id,status,updated_at,payload_json) VALUES (${id},'target',NULL,'codex','target','loaded',${at},${payload})`;
  }
  yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status) VALUES ('old-command','thread','target',${at},7,'accepted')`;
  yield* sql`INSERT INTO workflow_plan_identities(plan_id) VALUES ('old-plan')`;
  yield* sql`INSERT INTO workflow_revision_identities(plan_id,revision_id,revision_hash,source_packet_hash) VALUES ('old-plan','old-revision',${hash},${hash})`;
  yield* sql`INSERT INTO workflow_runs(id,plan_id,revision_id,revision_hash,source_packet_hash,spec_hash,status,record_version,control_generation,record_json) VALUES ('old-run','old-plan','old-revision',${hash},${hash},${hash},'active',7,3,'{ "retained": true }')`;
});

const snapshotRows = Effect.fn("test.snapshotSubmissionLookupRows")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql<{
    name: string;
  }>`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name<>'effect_sql_migrations' ORDER BY name`;
  const rows: Record<string, readonly string[]> = {};
  for (const table of tables)
    rows[table.name] = (yield* sql`SELECT * FROM ${sql(table.name)}`).map(stableStringify).sort();
  return rows;
});
const snapshotSchema = Effect.fn("test.snapshotSubmissionLookupSchema")(function* () {
  const sql = yield* SqlClient.SqlClient;
  return (yield* sql<{
    name: string;
    sql: string | null;
  }>`SELECT name,sql FROM sqlite_master ORDER BY name`).filter((row) => !names.includes(row.name));
});
const lookups = Effect.fn("test.submissionLookupQueries")(function* (key: string, after = "") {
  const sql = yield* SqlClient.SqlClient;
  const queries = [
    sql`SELECT * FROM orchestration_v2_submission_transports WHERE environment_id='environment' AND opening_thread_id=${key} AND transport_id>${after} ORDER BY transport_id LIMIT 2`,
    sql`SELECT * FROM orchestration_v2_submission_transports WHERE environment_id='environment' AND provider_session_id=${key} AND transport_id>${after} ORDER BY transport_id LIMIT 2`,
    sql`SELECT * FROM orchestration_v2_submission_attempts WHERE environment_id='environment' AND thread_id=${key} AND attempt_id>${after} ORDER BY attempt_id LIMIT 2`,
    sql`SELECT * FROM orchestration_v2_submission_attempts WHERE environment_id='environment' AND provider_thread_id=${key} AND attempt_id>${after} ORDER BY attempt_id LIMIT 2`,
    sql`SELECT * FROM orchestration_v2_submission_attempts WHERE environment_id='environment' AND driver='codex' AND provider_instance_id='instance' AND native_thread_id=${key} AND attempt_id>${after} ORDER BY attempt_id LIMIT 2`,
    sql`SELECT * FROM orchestration_v2_projection_provider_threads WHERE CASE WHEN json_valid(payload_json) THEN json_extract(payload_json,'$.nativeThreadRef.nativeId') END=${key} AND provider_thread_id>${after} ORDER BY provider_thread_id LIMIT 2`,
  ];
  return yield* Effect.forEach(queries, (query) =>
    Effect.gen(function* () {
      const plan = yield* sql<{ detail: string }>`EXPLAIN QUERY PLAN ${query}`;
      return { rows: yield* query, plan: plan.map((row) => row.detail) };
    }),
  );
});
const allPages = Effect.fn("test.submissionLookupPages")(function* (key: string) {
  const results: unknown[][] = names.map(() => []);
  const cursors = names.map(() => "");
  const done = names.map(() => false);
  while (done.some((finished) => !finished)) {
    for (let i = 0; i < names.length; i++) {
      if (done[i]) continue;
      const result = (yield* lookups(key, cursors[i]))[i]!;
      results[i]!.push(...result.rows);
      const last = result.rows.at(-1);
      if (!last || result.rows.length < 2) done[i] = true;
      else {
        const next = i < 2 ? last.transport_id : i < 5 ? last.attempt_id : last.provider_thread_id;
        assert.isString(next);
        assert.notStrictEqual(next, cursors[i]);
        cursors[i] = next as string;
      }
    }
  }
  return results;
});

it.effect(
  "indexes complete historical custody and native aliases through upgrade replay and reopen",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-submission-lookup-" });
      const filename = path.join(directory, "state.sqlite");
      const preserved = yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 88 });
        yield* seed();
        const rows = yield* snapshotRows();
        const schema = yield* snapshotSchema();
        const pages = yield* allPages("target");
        const before = yield* lookups("target");
        for (const [i, entry] of before.entries()) {
          assert.isFalse(
            entry.plan.some((detail) => detail.includes(i < 5 ? "environment_id=?" : "<expr>=?")),
            entry.plan.join("\n"),
          );
        }
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 89 }), [
          [89, "ProviderSubmissionLookup"],
        ]);
        assert.deepStrictEqual(yield* snapshotRows(), rows);
        assert.deepStrictEqual(yield* snapshotSchema(), schema);
        assert.deepStrictEqual(yield* allPages("target"), pages);
        assert.strictEqual(pages[0]!.length, 4);
        assert.strictEqual(pages[2]!.length, 4);
        assert.strictEqual(pages[4]!.length, 3);
        assert.strictEqual(pages[5]!.length, 2);
        for (const key of ["target", "absent"]) {
          for (const after of ["", "attempt-b1", "provider-a", "transport-b"]) {
            const queries = yield* lookups(key, after);
            for (const [i, entry] of queries.entries()) {
              assert.isTrue(
                entry.plan.some(
                  (detail) => detail.includes("SEARCH") && detail.includes(names[i]!),
                ),
                entry.plan.join("\n"),
              );
              assert.isFalse(
                entry.plan.some(
                  (detail) => detail.includes("SCAN") || detail.includes("TEMP B-TREE"),
                ),
                entry.plan.join("\n"),
              );
              if (key === "absent") assert.deepStrictEqual(entry.rows, []);
            }
          }
        }
        const indexes = yield* sql<{
          name: string;
          sql: string;
        }>`SELECT name,sql FROM sqlite_master WHERE type='index' ORDER BY name`;
        assert.strictEqual(indexes.filter((row) => names.includes(row.name)).length, 6);
        assert.isTrue(
          indexes
            .filter((row) => names.includes(row.name))
            .every((row) => !/\bWHERE\b/i.test(row.sql)),
        );
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 89 }), []);
        return { rows, schema, pages };
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 89 }), []);
        assert.deepStrictEqual(yield* snapshotRows(), preserved.rows);
        assert.deepStrictEqual(yield* snapshotSchema(), preserved.schema);
        assert.deepStrictEqual(yield* allPages("target"), preserved.pages);
        for (const statement of [
          sql`DELETE FROM orchestration_v2_submission_transports WHERE transport_id='transport-a'`,
          sql`UPDATE orchestration_v2_submission_attempts SET local_outcome=NULL,local_reason=NULL,local_observed_at=NULL WHERE attempt_id='attempt-b2'`,
          sql`INSERT OR REPLACE INTO orchestration_v2_submission_transports ${sql.insert(transport("transport-a"))}`,
        ])
          assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
        assert.deepStrictEqual(yield* snapshotRows(), preserved.rows);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
