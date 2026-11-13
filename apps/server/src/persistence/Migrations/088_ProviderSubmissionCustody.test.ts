import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const transports = "orchestration_v2_submission_transports";
const attempts = "orchestration_v2_submission_attempts";
const hash = "a".repeat(64);
const at = "2026-11-13T00:00:00.000Z";
const transportRow = (id = "transport-a") => ({
  format_version: 1,
  transport_id: id,
  environment_id: "environment-a",
  project_id: "project-a",
  opening_thread_id: "thread-a",
  driver: "codex",
  provider_instance_id: "codex-instance-a",
  provider_session_id: "session-a",
  host_boot_id: "host-boot-a",
  scope_namespace: "app-t3code-agents-fixture.slice",
  proposed_unit: `unit-${id}.scope`,
  creation_context_json: "{}",
  creation_context_hash: hash,
  creation_input_hash: hash,
  created_at: at,
});
const attemptRow = (id = "native-attempt-a", parent = "transport-a") => ({
  format_version: 1,
  attempt_id: id,
  transport_id: parent,
  environment_id: "environment-a",
  project_id: "project-a",
  thread_id: "thread-a",
  run_id: "run-a",
  run_ordinal: 1,
  root_node_id: "root-a",
  provider_turn_ordinal: 1,
  driver: "codex",
  provider_instance_id: "codex-instance-a",
  provider_session_id: "session-a",
  provider_thread_id: "provider-thread-a",
  native_thread_id: "native-thread-a",
  method: "turn/start",
  selection_json: "{}",
  selection_hash: hash,
  input_hash: hash,
  frame_hash: hash,
  request_id_json: "1",
  created_at: at,
});
const bindingFor = (unit: string) => ({
  unit,
  invocationId: "1".repeat(32),
  controlGroup: `/fixture/${unit}`,
  pid: 101,
  pidStartTicks: "9007199254740993",
});
const bindTransport = Effect.fn("test.bindTransportStorage")(function* (id = "transport-a") {
  const sql = yield* SqlClient.SqlClient;
  const binding = bindingFor(transportRow(id).proposed_unit);
  yield* sql`UPDATE ${sql(transports)} SET ${sql.update({ binding_json: JSON.stringify(binding), binding_hash: hash, bound_at: at })} WHERE transport_id=${id}`;
  return binding;
});

it.effect.each(["transport", "attempt"] as const)(
  "preserves permanent %s identity against hidden rowid replacement",
  (kind) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 88 });
      yield* sql`PRAGMA recursive_triggers=OFF`;
      yield* sql`INSERT INTO ${sql(transports)} ${sql.insert(transportRow())}`;
      yield* bindTransport();
      yield* sql`INSERT INTO ${sql(attempts)} ${sql.insert(attemptRow())}`;
      const table = kind === "transport" ? transports : attempts;
      if (kind === "transport")
        yield* sql`INSERT INTO ${sql(transports)} ${sql.insert(transportRow("transport-free"))}`;
      const columns = yield* sql<{
        readonly wr: number;
      }>`SELECT wr FROM pragma_table_list WHERE name=${table}`;
      if (columns[0]?.wr === 1) {
        const replacement =
          kind === "transport"
            ? { rowid: 2, ...transportRow("replacement") }
            : { rowid: 1, ...attemptRow("replacement") };
        const exit = yield* Effect.exit(
          sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert(replacement)}`,
        );
        assert.isTrue(Exit.isFailure(exit));
        assert.include(String(exit), "no column named rowid");
      } else {
        const [existing] = yield* sql<{
          readonly rowid: number;
        }>`SELECT rowid FROM ${sql(table)} WHERE ${kind === "transport" ? sql`transport_id='transport-free'` : sql`attempt_id='native-attempt-a'`}`;
        const replacement =
          kind === "transport"
            ? { rowid: existing!.rowid, ...transportRow("replacement") }
            : { rowid: existing!.rowid, ...attemptRow("replacement") };
        const exit = yield* Effect.exit(
          sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert(replacement)}`,
        );
        assert.isTrue(Exit.isFailure(exit));
      }
      assert.strictEqual(
        (yield* sql`SELECT * FROM ${sql(attempts)} WHERE attempt_id='native-attempt-a'`).length,
        1,
      );
      if (kind === "transport")
        assert.strictEqual(
          (yield* sql`SELECT * FROM ${sql(transports)} WHERE transport_id='transport-free'`).length,
          1,
        );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

const rejectMutation = Effect.fn("test.rejectCustodyMutation")(function* <A, E, R>(
  query: Effect.Effect<A, E, R>,
  expected: RegExp,
) {
  const exit = yield* Effect.exit(query);
  assert.isTrue(Exit.isFailure(exit));
  assert.match(String(exit), expected);
});
const settleTransport = Effect.fn("test.settleTransportStorage")(function* (id = "transport-a") {
  const sql = yield* SqlClient.SqlClient;
  const settlement = {
    binding: bindingFor(transportRow(id).proposed_unit),
    observation: "removed",
  };
  yield* sql`UPDATE ${sql(transports)} SET ${sql.update({ settlement_json: JSON.stringify(settlement), settlement_hash: hash, settled_at: at })} WHERE transport_id=${id}`;
  return settlement;
});

const oldTables = [
  "orchestration_events",
  "orchestration_command_receipts",
  "orchestration_v2_events",
  "orchestration_v2_projection_threads",
  "orchestration_v2_projection_runs",
  "orchestration_v2_projection_run_attempts",
  "workflow_plan_identities",
  "workflow_revision_identities",
  "workflow_runs",
  "workflow_operations",
  "workflow_attempts",
  "workflow_native_launch_intents",
  "workflow_native_reservation_material",
  "workflow_native_command_proofs",
] as const;
const snapshotOld = Effect.fn("test.snapshotOldCustodyRows")(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* Effect.forEach(oldTables, (table) => sql`SELECT * FROM ${sql(table)}`);
});
const seedOld = Effect.fn("test.seedOldCustodyRows")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO workflow_plan_identities(plan_id) VALUES ('plan-a')`;
  yield* sql`INSERT INTO workflow_revision_identities(plan_id,revision_id,revision_hash,source_packet_hash) VALUES ('plan-a','revision-a',${hash},${hash})`;
  yield* sql`INSERT INTO workflow_runs(id,plan_id,revision_id,revision_hash,source_packet_hash,spec_hash,status,record_version,control_generation,record_json) VALUES ('workflow-run-a','plan-a','revision-a',${hash},${hash},${hash},'active',0,0,'{}')`;
  yield* sql`INSERT INTO workflow_operations(id,workflow_run_id,kind,fingerprint,status,generation,record_json) VALUES ('operation-a','workflow-run-a','stage_launch',${hash},'pending',0,'{}')`;
  yield* sql`INSERT INTO workflow_attempts(id,workflow_run_id,operation_id,item_id,repair) VALUES ('workflow-attempt-a','workflow-run-a','operation-a','item-a',0)`;
  yield* sql`INSERT INTO workflow_native_launch_intents(intent_id,workflow_run_id,operation_id,attempt_id,item_id,charge_attempt,repair,role,environment_id,project_id,launch_command_id,thread_id,message_id,fingerprint,draft_json,generation,state) VALUES ('intent-a','workflow-run-a','operation-a','workflow-attempt-a','item-a',1,0,'writer','environment-a','project-a','command-a','thread-a','message-a',${hash},'{}',0,'reserved')`;
  yield* sql`INSERT INTO workflow_native_reservation_material(intent_id,material_json,material_hash) VALUES ('intent-a','{}',${hash})`;
  yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status) VALUES ('command-a','thread','thread-a',${at},1,'accepted')`;
  yield* sql`INSERT INTO workflow_native_command_proofs(command_id,intent_id,phase,proof_json,proof_hash) VALUES ('command-a','intent-a','claim','{}',${hash})`;
  yield* sql`INSERT INTO orchestration_events(event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json) VALUES ('event-a','thread','thread-a',1,'thread.created',${at},'command-a','agent','{}','{}')`;
  yield* sql`INSERT INTO orchestration_v2_events(event_id,command_id,thread_id,run_id,node_id,event_type,occurred_at,payload_json) VALUES ('v2-event-a','command-a','thread-a','run-a','root-a','run.requested',${at},'{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES ('thread-a','project-a','fixture','codex','full-access','default',${at},${at},'{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,provider_thread_id,status,requested_at,payload_json) VALUES ('run-a','thread-a',1,'codex','provider-thread-a','pending',${at},'{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_run_attempts(attempt_id,thread_id,run_id,attempt_ordinal,root_node_id,provider,provider_thread_id,status,payload_json) VALUES ('native-attempt-a','thread-a','run-a',1,'root-a','codex','provider-thread-a','pending','{}')`;
});

it.effect(
  "preserves old journals and permanent custody across file-backed upgrade replay and reopen",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-submission-custody-" });
      const filename = path.join(directory, "state.sqlite");
      const old = yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 87 });
        yield* seedOld();
        const before = yield* snapshotOld();
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 88 }), [
          [88, "ProviderSubmissionCustody"],
        ]);
        assert.deepStrictEqual(yield* snapshotOld(), before);
        assert.deepStrictEqual(yield* sql`SELECT * FROM ${sql(transports)}`, []);
        assert.deepStrictEqual(yield* sql`SELECT * FROM ${sql(attempts)}`, []);
        yield* sql`INSERT INTO ${sql(transports)} ${sql.insert(transportRow())}`;
        yield* bindTransport();
        yield* sql`INSERT INTO ${sql(attempts)} ${sql.insert(attemptRow())}`;
        yield* sql`UPDATE ${sql(attempts)} SET local_outcome='not_submitted',local_reason='wrapper',local_observed_at=${at} WHERE attempt_id='native-attempt-a'`;
        assert.strictEqual(
          (yield* sql`SELECT transport_id FROM ${sql(transports)} WHERE settlement_json IS NULL`)
            .length,
          1,
        );
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 88 }), []);
        return before;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 88 }), []);
        assert.deepStrictEqual(yield* snapshotOld(), old);
        const [row] = yield* sql<{
          readonly local_outcome: string;
        }>`SELECT local_outcome FROM ${sql(attempts)} WHERE attempt_id='native-attempt-a'`;
        assert.strictEqual(row?.local_outcome, "not_submitted");
        assert.strictEqual(
          (yield* sql`SELECT transport_id FROM ${sql(transports)} WHERE settlement_json IS NULL`)
            .length,
          1,
        );
        yield* rejectMutation(
          sql`INSERT INTO ${sql(attempts)} ${sql.insert(attemptRow())}`,
          /fresh immutable identity/,
        );
        yield* settleTransport();
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(yield* snapshotOld(), old);
        assert.strictEqual(
          (yield* sql`SELECT transport_id FROM ${sql(transports)} WHERE settlement_json IS NULL`)
            .length,
          0,
        );
        yield* rejectMutation(
          sql`INSERT INTO ${sql(attempts)} ${sql.insert(attemptRow())}`,
          /fresh immutable identity/,
        );
        yield* sql`DELETE FROM orchestration_v2_projection_run_attempts WHERE attempt_id='native-attempt-a'`;
        assert.strictEqual(
          (yield* sql`SELECT attempt_id FROM ${sql(attempts)} WHERE attempt_id='native-attempt-a'`)
            .length,
          1,
        );
        assert.strictEqual(
          (yield* sql`SELECT id FROM workflow_attempts WHERE id='workflow-attempt-a'`).length,
          1,
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("requires fresh complete evidence groups and preserves every populated group", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 88 });
    const row = transportRow();
    const binding = bindingFor(row.proposed_unit);
    const bound = { binding_json: JSON.stringify(binding), binding_hash: hash, bound_at: at };
    const settlement = {
      settlement_json: JSON.stringify({ binding, observation: "removed" }),
      settlement_hash: hash,
      settled_at: at,
    };
    yield* rejectMutation(
      sql`INSERT INTO ${sql(transports)} ${sql.insert({ ...row, ...bound })}`,
      /fresh immutable identity/,
    );
    yield* sql`INSERT INTO ${sql(transports)} ${sql.insert(row)}`;
    for (const patch of [
      { binding_json: bound.binding_json },
      { binding_hash: hash },
      { bound_at: at },
      { binding_json: "[]", binding_hash: hash, bound_at: at },
      { ...bound, binding_hash: "bad" },
      { ...bound, binding_json: JSON.stringify({ ...binding, unit: "wrong.scope" }) },
    ]) {
      yield* rejectMutation(
        sql`UPDATE ${sql(transports)} SET ${sql.update(patch)} WHERE transport_id=${row.transport_id}`,
        /CHECK constraint|malformed JSON/,
      );
    }
    yield* rejectMutation(
      sql`UPDATE ${sql(transports)} SET ${sql.update({ ...bound, ...settlement })} WHERE transport_id=${row.transport_id}`,
      /identity or evidence is immutable/,
    );
    yield* bindTransport();
    for (const field of ["unit", "invocationId", "controlGroup", "pid", "pidStartTicks"] as const) {
      const wrong = { ...binding, [field]: field === "pid" ? 102 : "wrong" };
      const absent = Object.fromEntries(Object.entries(binding).filter(([key]) => key !== field));
      for (const other of [wrong, absent])
        yield* rejectMutation(
          sql`UPDATE ${sql(transports)} SET ${sql.update({ ...settlement, settlement_json: JSON.stringify({ binding: other, observation: "removed" }) })} WHERE transport_id=${row.transport_id}`,
          /CHECK constraint/,
        );
    }
    yield* settleTransport();
    for (const patch of [
      { binding_json: null, binding_hash: null, bound_at: null },
      { ...bound, bound_at: "later" },
      { settlement_json: null, settlement_hash: null, settled_at: null },
      { ...settlement, settlement_hash: "b".repeat(64) },
    ]) {
      yield* rejectMutation(
        sql`UPDATE ${sql(transports)} SET ${sql.update(patch)} WHERE transport_id=${row.transport_id}`,
        /identity or evidence is immutable/,
      );
    }
    yield* sql`UPDATE ${sql(transports)} SET ${sql.update({ ...bound, ...settlement })} WHERE transport_id=${row.transport_id}`;
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "forbids identity edits deletes named replacements and upserts with recursive triggers off",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 88 });
      yield* sql`PRAGMA recursive_triggers=OFF`;
      const transport = transportRow();
      const attempt = attemptRow();
      yield* sql`INSERT INTO ${sql(transports)} ${sql.insert(transport)}`;
      yield* bindTransport();
      yield* sql`INSERT INTO ${sql(attempts)} ${sql.insert(attempt)}`;
      for (const [table, row, key] of [
        [transports, transport, "transport_id"],
        [attempts, attempt, "attempt_id"],
      ] as const) {
        const recordId = "attempt_id" in row ? row.attempt_id : row.transport_id;
        for (const [field, value] of Object.entries(row)) {
          const changed =
            typeof value === "number"
              ? value + 1
              : field.endsWith("_hash")
                ? "b".repeat(64)
                : field.endsWith("_json")
                  ? '{"changed":true}'
                  : value + "-changed";
          yield* rejectMutation(
            sql`UPDATE ${sql(table)} SET ${sql.update({ [field]: changed })} WHERE ${sql(key)}=${recordId}`,
            /identity or evidence is immutable/,
          );
        }
        yield* rejectMutation(
          sql`DELETE FROM ${sql(table)} WHERE ${sql(key)}=${recordId}`,
          /tombstone is permanent/,
        );
        yield* rejectMutation(
          sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert(row)}`,
          /fresh immutable identity/,
        );
        yield* rejectMutation(
          sql`INSERT INTO ${sql(table)} ${sql.insert(row)} ON CONFLICT(${sql(key)}) DO UPDATE SET created_at=excluded.created_at`,
          /fresh immutable identity/,
        );
      }
      yield* rejectMutation(
        sql`INSERT OR REPLACE INTO ${sql(transports)} ${sql.insert({ ...transportRow("other"), proposed_unit: transport.proposed_unit })}`,
        /fresh immutable identity/,
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("admits only a bound unsettled matching parent and an immutable local outcome", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 88 });
    const attempt = attemptRow();
    yield* rejectMutation(
      sql`INSERT INTO ${sql(attempts)} ${sql.insert(attempt)}`,
      /fresh immutable identity/,
    );
    yield* sql`INSERT INTO ${sql(transports)} ${sql.insert(transportRow())}`;
    yield* rejectMutation(
      sql`INSERT INTO ${sql(attempts)} ${sql.insert(attempt)}`,
      /fresh immutable identity/,
    );
    yield* bindTransport();
    for (const field of [
      "environment_id",
      "driver",
      "provider_instance_id",
      "provider_session_id",
    ] as const) {
      yield* rejectMutation(
        sql`INSERT INTO ${sql(attempts)} ${sql.insert({ ...attempt, [field]: "foreign" })}`,
        /fresh immutable identity/,
      );
    }
    for (const patch of [
      { method: "thread/compact/start" },
      { run_ordinal: 0 },
      { provider_turn_ordinal: 0 },
      { selection_json: "[]" },
      { selection_hash: "bad" },
      { request_id_json: "null" },
      { request_id_json: "{}" },
    ]) {
      yield* rejectMutation(
        sql`INSERT INTO ${sql(attempts)} ${sql.insert({ ...attempt, ...patch })}`,
        /CHECK constraint/,
      );
    }
    yield* rejectMutation(
      sql`INSERT INTO ${sql(attempts)} ${sql.insert({ ...attempt, local_outcome: "submitted", local_reason: "completed", local_observed_at: at })}`,
      /fresh immutable identity/,
    );
    yield* sql`INSERT INTO ${sql(attempts)} ${sql.insert(attempt)}`;
    yield* sql`INSERT INTO ${sql(attempts)} ${sql.insert({ ...attemptRow("shared-attempt"), project_id: "other-project", thread_id: "other-thread" })}`;
    for (const patch of [
      { local_outcome: "unknown" },
      { local_reason: "wrapper" },
      { local_outcome: "submitted", local_reason: "wrapper", local_observed_at: at },
      { local_outcome: "unknown", local_reason: "completed", local_observed_at: at },
    ]) {
      yield* rejectMutation(
        sql`UPDATE ${sql(attempts)} SET ${sql.update(patch)} WHERE attempt_id=${attempt.attempt_id}`,
        /CHECK constraint/,
      );
    }
    const outcome = { local_outcome: "unknown", local_reason: "sink", local_observed_at: at };
    yield* sql`UPDATE ${sql(attempts)} SET ${sql.update(outcome)} WHERE attempt_id=${attempt.attempt_id}`;
    for (const patch of [
      { local_outcome: "not_submitted", local_reason: "wrapper", local_observed_at: at },
      { local_outcome: null, local_reason: null, local_observed_at: null },
      { ...outcome, local_observed_at: "later" },
    ]) {
      yield* rejectMutation(
        sql`UPDATE ${sql(attempts)} SET ${sql.update(patch)} WHERE attempt_id=${attempt.attempt_id}`,
        /identity or evidence is immutable/,
      );
    }
    yield* sql`UPDATE ${sql(attempts)} SET ${sql.update(outcome)} WHERE attempt_id=${attempt.attempt_id}`;
    yield* settleTransport();
    yield* rejectMutation(
      sql`INSERT INTO ${sql(attempts)} ${sql.insert(attemptRow("late-attempt"))}`,
      /fresh immutable identity/,
    );
    const keys = yield* sql<{
      readonly table: string;
    }>`PRAGMA foreign_key_list(orchestration_v2_submission_attempts)`;
    assert.deepStrictEqual(
      keys.map((row) => row.table),
      [transports],
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("uses retained resource indexes for both present and absent identities", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 88 });
    yield* sql`INSERT INTO ${sql(transports)} ${sql.insert(transportRow())}`;
    yield* bindTransport();
    yield* sql`INSERT INTO ${sql(attempts)} ${sql.insert(attemptRow())}`;
    const queries = [
      [
        "submission_transports_thread",
        "SELECT transport_id FROM orchestration_v2_submission_transports WHERE settlement_json IS NULL AND environment_id=? AND opening_thread_id=?",
        ["environment-a", "thread-a"],
      ],
      [
        "submission_transports_session",
        "SELECT transport_id FROM orchestration_v2_submission_transports WHERE settlement_json IS NULL AND environment_id=? AND provider_session_id=?",
        ["environment-a", "session-a"],
      ],
      [
        "submission_transports_instance",
        "SELECT transport_id FROM orchestration_v2_submission_transports WHERE settlement_json IS NULL AND environment_id=? AND driver=? AND provider_instance_id=?",
        ["environment-a", "codex", "codex-instance-a"],
      ],
      [
        "submission_attempts_thread",
        "SELECT attempt_id FROM orchestration_v2_submission_attempts WHERE (local_outcome IS NULL OR local_outcome<>'not_submitted') AND environment_id=? AND thread_id=?",
        ["environment-a", "thread-a"],
      ],
      [
        "submission_attempts_provider_thread",
        "SELECT attempt_id FROM orchestration_v2_submission_attempts WHERE (local_outcome IS NULL OR local_outcome<>'not_submitted') AND environment_id=? AND provider_thread_id=?",
        ["environment-a", "provider-thread-a"],
      ],
      [
        "submission_attempts_native",
        "SELECT attempt_id FROM orchestration_v2_submission_attempts WHERE (local_outcome IS NULL OR local_outcome<>'not_submitted') AND environment_id=? AND driver=? AND provider_instance_id=? AND native_thread_id=?",
        ["environment-a", "codex", "codex-instance-a", "native-thread-a"],
      ],
      [
        "submission_attempts_session",
        "SELECT attempt_id FROM orchestration_v2_submission_attempts WHERE (local_outcome IS NULL OR local_outcome<>'not_submitted') AND environment_id=? AND provider_session_id=?",
        ["environment-a", "session-a"],
      ],
      [
        "submission_attempts_transport",
        "SELECT attempt_id FROM orchestration_v2_submission_attempts WHERE transport_id=?",
        ["transport-a"],
      ],
    ] as const;
    for (const [index, query, parameters] of queries) {
      for (const missing of [false, true]) {
        const input = missing ? [...parameters.slice(0, -1), "absent"] : parameters;
        const plan = yield* sql.unsafe<{ readonly detail: string }>(
          "EXPLAIN QUERY PLAN " + query,
          input,
        );
        assert.isTrue(
          plan.some((row) => row.detail.includes(index) && row.detail.includes("SEARCH")),
          JSON.stringify(plan),
        );
        assert.strictEqual((yield* sql.unsafe(query, input)).length, missing ? 0 : 1);
      }
    }
    yield* sql`UPDATE ${sql(attempts)} SET local_outcome='not_submitted',local_reason='wrapper',local_observed_at=${at} WHERE attempt_id='native-attempt-a'`;
    assert.strictEqual((yield* sql.unsafe(queries[3][1], queries[3][2])).length, 0);
    assert.strictEqual((yield* sql.unsafe(queries[0][1], queries[0][2])).length, 1);
    yield* settleTransport();
    assert.strictEqual((yield* sql.unsafe(queries[0][1], queries[0][2])).length, 0);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
