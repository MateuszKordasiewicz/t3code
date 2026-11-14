import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const hash = "a".repeat(64);
const at = "2026-11-14T16:00:00.000Z";
const indexName = "native_conversations_native_history";
const seed = Effect.fn("test.seedNativeConversationLookup")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const expectedClaim = {
    operationId: "operation",
    expectedFingerprint: hash,
    expectedGeneration: 0,
    bootId: "boot",
    workerId: "worker",
  };
  for (const [id, environment, instance, nativeId] of [
    ["a", "environment", "instance", "target"],
    ["b", "environment", "instance", "target"],
    ["c", "environment", "instance", "other-native"],
    ["d", "other-environment", "instance", "target"],
    ["e", "environment", "other-instance", "target"],
  ] as const) {
    const transportId = `transport-${id}`;
    const selected = {
      v: 1,
      kind: "fresh-conversation",
      transportId,
      environmentId: environment,
      projectId: "project",
      threadId: `thread-${id}`,
      runId: `run-${id}`,
      runOrdinal: 1,
      attemptId: `attempt-${id}`,
      rootNodeId: `root-${id}`,
      intentId: `intent-${id}`,
      driver: "codex",
      providerInstanceId: instance,
      providerSessionId: `session-${id}`,
      providerThreadId: `selected-${id}`,
      invocation: 1,
      expectedClaim,
    };
    const binding = {
      unit: `${id}.scope`,
      invocationId: "1".repeat(32),
      controlGroup: `/fixture/${id}.scope`,
      pid: 101,
      pidStartTicks: "123456",
    };
    yield* sql`INSERT INTO orchestration_v2_submission_transports(format_version,transport_id,environment_id,project_id,opening_thread_id,driver,provider_instance_id,provider_session_id,host_boot_id,scope_namespace,proposed_unit,creation_context_json,creation_context_hash,creation_input_hash,created_at) VALUES (1,${transportId},${environment},'project',${selected.threadId},'codex',${instance},${selected.providerSessionId},'host','scope.slice',${binding.unit},${stableStringify({ expectedClaim })},${hash},${hash},${at})`;
    yield* sql`UPDATE orchestration_v2_submission_transports SET binding_json=${stableStringify(binding)},binding_hash=${hash},bound_at=${at} WHERE transport_id=${transportId}`;
    yield* sql`INSERT INTO orchestration_v2_native_conversations(format_version,transport_id,environment_id,project_id,thread_id,run_id,run_ordinal,attempt_id,root_node_id,intent_id,driver,provider_instance_id,provider_session_id,provider_thread_id,method,selection_json,selection_hash,input_json,input_hash,request_id_json,created_at) VALUES (1,${transportId},${environment},'project',${selected.threadId},${selected.runId},1,${selected.attemptId},${selected.rootNodeId},${selected.intentId},'codex',${instance},${selected.providerSessionId},${selected.providerThreadId},'thread/start',${stableStringify(selected)},${hash},'{}',${hash},'2',${at})`;
    yield* sql`UPDATE orchestration_v2_native_conversations SET local_outcome='submitted',local_reason='completed',local_observed_at=${at} WHERE transport_id=${transportId}`;
    yield* sql`UPDATE orchestration_v2_native_conversations SET response_json=${stableStringify({ v: 1, nativeThreadId: nativeId, returnedProviderThreadId: `derived-${id}`, driver: "codex", providerInstanceId: instance, providerSessionId: selected.providerSessionId, threadId: selected.threadId })},response_hash=${hash},response_observed_at=${at} WHERE transport_id=${transportId}`;
    if (id === "b")
      yield* sql`UPDATE orchestration_v2_submission_transports SET settlement_json=${stableStringify({ binding, observation: "empty" })},settlement_hash=${hash},settled_at=${at} WHERE transport_id=${transportId}`;
  }
});
const lookup = Effect.fn("test.nativeConversationLookup")(function* (nativeId: string) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT * FROM orchestration_v2_native_conversations WHERE environment_id='environment' AND driver='codex' AND provider_instance_id='instance' AND json_extract(response_json,'$.nativeThreadId')=${nativeId} AND transport_id>'' ORDER BY transport_id LIMIT 128`;
});
const plan = Effect.fn("test.nativeConversationLookupPlan")(function* (nativeId: string) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    readonly detail: string;
  }>`EXPLAIN QUERY PLAN SELECT * FROM orchestration_v2_native_conversations WHERE environment_id='environment' AND driver='codex' AND provider_instance_id='instance' AND json_extract(response_json,'$.nativeThreadId')=${nativeId} AND transport_id>'' ORDER BY transport_id LIMIT 128`;
});

const providerPlan = Effect.fn("test.nativeConversationProviderPlan")(function* (
  providerThreadId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql<{
    readonly detail: string;
  }>`EXPLAIN QUERY PLAN SELECT 1 FROM orchestration_v2_native_conversations WHERE environment_id='environment' AND provider_thread_id=${providerThreadId} LIMIT 1`;
});

it.effect(
  "indexes complete native conversation history without changing rows through upgrade and reopen",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped();
      const filename = path.join(directory, "lookup.sqlite");
      const expected = yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 93 });
        yield* seed();
        const rows =
          yield* sql`SELECT * FROM orchestration_v2_native_conversations ORDER BY transport_id`;
        const parents =
          yield* sql`SELECT * FROM orchestration_v2_submission_transports ORDER BY transport_id`;
        const found = yield* lookup("target");
        assert.deepStrictEqual(
          found.map((row) => row.transport_id),
          ["transport-a", "transport-b"],
        );
        for (const id of ["target", "absent"])
          assert.isFalse((yield* plan(id)).some((row) => row.detail.includes(indexName)));
        for (const selected of ["selected-b", "absent"])
          assert.isFalse(
            (yield* providerPlan(selected)).some((row) =>
              row.detail.includes("native_conversations_provider_history"),
            ),
          );
        yield* runMigrations({ toMigrationInclusive: 94 });
        for (const selected of ["selected-b", "absent"])
          assert.isTrue(
            (yield* providerPlan(selected)).some(
              (row) =>
                row.detail.includes("native_conversations_provider_history") &&
                row.detail.includes("provider_thread_id=?"),
            ),
          );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM orchestration_v2_native_conversations ORDER BY transport_id`,
          rows,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM orchestration_v2_submission_transports ORDER BY transport_id`,
          parents,
        );
        assert.deepStrictEqual(yield* lookup("target"), found);
        assert.deepStrictEqual(yield* lookup("absent"), []);
        for (const id of ["target", "absent"]) {
          const actual = yield* plan(id);
          assert.isTrue(
            actual.some((row) => row.detail.includes(indexName) && row.detail.includes("<expr>=?")),
          );
          assert.isFalse(actual.some((row) => row.detail.includes("TEMP B-TREE")));
        }
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 94 }), []);
        return { rows, parents, found };
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })), Effect.scoped);
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(yield* lookup("target"), expected.found);
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM orchestration_v2_native_conversations ORDER BY transport_id`,
          expected.rows,
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM orchestration_v2_submission_transports ORDER BY transport_id`,
          expected.parents,
        );
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
        assert.isTrue(
          (yield* providerPlan("selected-b")).some((row) =>
            row.detail.includes("native_conversations_provider_history"),
          ),
        );
        assert.deepStrictEqual(
          yield* sql`SELECT transport_id FROM orchestration_v2_native_conversations WHERE environment_id='environment' AND provider_thread_id='selected-b'`,
          [{ transport_id: "transport-b" }],
        );
        assert.isTrue((yield* plan("absent")).some((row) => row.detail.includes(indexName)));
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })), Effect.scoped);
    }).pipe(Effect.provide(NodeServices.layer)),
);
