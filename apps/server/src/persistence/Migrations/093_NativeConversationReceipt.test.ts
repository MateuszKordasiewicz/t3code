import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Codec from "../../orchestration-v2/ProviderSubmissionCodec.ts";
import * as SqlClient from "effect/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

it.effect("adds empty native conversation receipt storage without native delivery", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations({ toMigrationInclusive: 93 });
    assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_v2_native_conversations`, []);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

const at = "2026-11-14T15:00:00.000Z";
const hash = "a".repeat(64);
const transportId = "11111111-2222-3333-4444-555555555555";
const table = "orchestration_v2_native_conversations";
const transports = "orchestration_v2_submission_transports";
function fixture() {
  const expectedClaim = {
    operationId: "operation",
    expectedFingerprint: hash,
    expectedGeneration: 0,
    bootId: "boot",
    workerId: "worker",
  };
  const selected = {
    v: 1,
    kind: "fresh-conversation",
    transportId,
    environmentId: "environment",
    projectId: "project",
    threadId: "thread",
    runId: "run",
    runOrdinal: 1,
    attemptId: "attempt",
    rootNodeId: "root",
    intentId: "intent",
    driver: "codex",
    providerInstanceId: "instance",
    providerSessionId: "session",
    providerThreadId: "selected-row",
    invocation: 1,
    expectedClaim,
  };
  const params = {
    model: "fixture-model",
    cwd: "/fixture/workspace",
    config: {
      "tools.update_plan.enabled": true,
      mcp_optional_startup_grace_ms: 10000,
      "features.concurrent_reasoning_summaries": true,
      "shell_environment_policy.set.T3CODE_THREAD_ID": "thread",
    },
  };
  const encoded = JSON.stringify({ id: 3, method: "thread/start", params }) + "\n";
  const row = Codec.snapshotConversationFrame(
    {
      transportId,
      environmentId: "environment",
      projectId: "project",
      driver: "codex",
      providerInstanceId: "instance",
      providerSessionId: "session",
      expectedSelection: selected,
    },
    {
      encoded,
      byteLength: new TextEncoder().encode(encoded).byteLength,
      method: "thread/start",
      requestId: 3,
      selection: stableStringify(selected),
    },
    params,
    { mode: "disabled" },
    at,
  );
  const parent = Codec.encodeSubmissionTransportData({
    context: {
      v: 1,
      environmentId: "environment",
      projectId: "project",
      openingThreadId: "thread",
      driver: "codex",
      providerInstanceId: "instance",
      providerSessionId: "session",
      scopeNamespace: "app-t3code-agents-123456abcdef.slice",
      hostBootId: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
      expectedClaim,
    },
    transportId,
    proposedUnit: "t3code-codex-aaaaaaaa-1111-2222-3333-444444444444.scope",
    creationInputHash: hash,
    createdAt: at,
  });
  const binding = {
    unit: parent.proposed_unit,
    invocationId: "1".repeat(32),
    controlGroup: `/fixture/${parent.scope_namespace}/${parent.proposed_unit}`,
    pid: 101,
    pidStartTicks: "123456",
  };
  const response = {
    v: 1,
    nativeThreadId: "original-native-thread",
    returnedProviderThreadId: "derived-row",
    driver: "codex",
    providerInstanceId: "instance",
    providerSessionId: "session",
    threadId: "thread",
  };
  return { row, parent, binding, selected, response };
}
const seedParent = Effect.fn("test.seedConversationParent")(function* (bound = true) {
  const sql = yield* SqlClient.SqlClient;
  const value = fixture();
  yield* sql`INSERT INTO ${sql(transports)} ${sql.insert(value.parent)}`;
  if (bound) {
    const binding = Codec.encodeScopeBindingData(value.binding, value.parent);
    yield* sql`UPDATE ${sql(transports)} SET binding_json=${binding.json},binding_hash=${binding.hash},bound_at=${at} WHERE transport_id=${transportId}`;
  }
  return value;
});
const reject = Effect.fn("test.rejectConversationMutation")(function* <A, E, R>(
  query: Effect.Effect<A, E, R>,
) {
  assert.isTrue(Exit.isFailure(yield* Effect.exit(query)));
});
const settleParent = Effect.fn("test.settleConversationParent")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const settlement = Codec.encodeScopeSettlementData(
    { binding: fixture().binding, observation: "empty" },
    fixture().binding,
  );
  yield* sql`UPDATE ${sql(transports)} SET settlement_json=${settlement.json},settlement_hash=${settlement.hash},settled_at=${at} WHERE transport_id=${transportId}`;
});

it.effect(
  "requires the original bound unsettled parent and refuses fabricated initial observations",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 93 });
      const value = yield* seedParent(false);
      yield* reject(sql`INSERT INTO ${sql(table)} ${sql.insert(value.row)}`);
      const binding = Codec.encodeScopeBindingData(value.binding, value.parent);
      yield* sql`UPDATE ${sql(transports)} SET binding_json=${binding.json},binding_hash=${binding.hash},bound_at=${at} WHERE transport_id=${transportId}`;
      const shadows = {
        environment_id: "environmentId",
        project_id: "projectId",
        thread_id: "threadId",
        provider_instance_id: "providerInstanceId",
        provider_session_id: "providerSessionId",
      };
      for (const [column, field] of Object.entries(shadows)) {
        const row = {
          ...value.row,
          [column]: "foreign",
          selection_json: stableStringify({ ...value.selected, [field]: "foreign" }),
        };
        yield* reject(sql`INSERT INTO ${sql(table)} ${sql.insert(row)}`);
      }
      for (const [key, changed] of Object.entries({
        operationId: "foreign-operation",
        expectedFingerprint: "b".repeat(64),
        expectedGeneration: 1,
        bootId: "foreign-boot",
        workerId: "foreign-worker",
      })) {
        const row = {
          ...value.row,
          selection_json: stableStringify({
            ...value.selected,
            expectedClaim: { ...value.selected.expectedClaim, [key]: changed },
          }),
        };
        yield* reject(sql`INSERT INTO ${sql(table)} ${sql.insert(row)}`);
      }
      const submitted = Codec.encodeConversationOutcomeData(
        value.row,
        { kind: "submitted", reason: "completed" },
        at,
      );
      yield* reject(sql`INSERT INTO ${sql(table)} ${sql.insert(submitted)}`);
      const opened = Codec.encodeConversationResponseData(submitted, value.response, at, {
        mode: "disabled",
      });
      yield* reject(sql`INSERT INTO ${sql(table)} ${sql.insert(opened)}`);
      yield* settleParent();
      yield* reject(sql`INSERT INTO ${sql(table)} ${sql.insert(value.row)}`);
      assert.deepStrictEqual(yield* sql`SELECT * FROM ${sql(table)}`, []);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "preserves one permanent conversation identity against update, replacement and upsert",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 93 });
      yield* sql`PRAGMA recursive_triggers=OFF`;
      const { row } = yield* seedParent();
      yield* sql`INSERT INTO ${sql(table)} ${sql.insert(row)}`;
      const evidence = new Set([
        "local_outcome",
        "local_reason",
        "local_observed_at",
        "response_json",
        "response_hash",
        "response_observed_at",
      ]);
      for (const [column, value] of Object.entries(row)) {
        if (evidence.has(column)) continue;
        const changed = typeof value === "number" ? value + 1 : `${String(value)}-changed`;
        yield* reject(
          sql`UPDATE ${sql(table)} SET ${sql.update({ [column]: changed })} WHERE transport_id=${transportId}`,
        );
      }
      yield* reject(sql`DELETE FROM ${sql(table)} WHERE transport_id=${transportId}`);
      yield* reject(sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert(row)}`);
      yield* reject(
        sql`INSERT INTO ${sql(table)} ${sql.insert(row)} ON CONFLICT(transport_id) DO UPDATE SET input_hash=${"b".repeat(64)}`,
      );
      yield* reject(sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert({ rowid: 1, ...row })}`);
      assert.deepStrictEqual(yield* sql`SELECT * FROM ${sql(table)}`, [row]);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect.each(["submitted", "unknown", "not_submitted"] as const)(
  "retains the original %s local outcome and accepts a later response only after submitted",
  (kind) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 93 });
      const { row, response } = yield* seedParent();
      yield* sql`INSERT INTO ${sql(table)} ${sql.insert(row)}`;
      const observed = Codec.encodeConversationOutcomeData(
        row,
        { kind, reason: kind === "submitted" ? "completed" : "sink" },
        at,
      );
      const submitted = Codec.encodeConversationOutcomeData(
        row,
        { kind: "submitted", reason: "completed" },
        at,
      );
      const opened = Codec.encodeConversationResponseData(submitted, response, at, {
        mode: "disabled",
      });
      const responseUpdate = {
        response_json: opened.response_json,
        response_hash: opened.response_hash,
        response_observed_at: at,
      };
      for (const patch of [
        { local_outcome: kind },
        { local_outcome: "unknown", local_reason: "completed", local_observed_at: at },
        responseUpdate,
        {
          ...responseUpdate,
          local_outcome: "submitted",
          local_reason: "completed",
          local_observed_at: at,
        },
      ])
        yield* reject(
          sql`UPDATE ${sql(table)} SET ${sql.update(patch)} WHERE transport_id=${transportId}`,
        );
      yield* sql`UPDATE ${sql(table)} SET local_outcome=${observed.local_outcome},local_reason=${observed.local_reason},local_observed_at=${observed.local_observed_at} WHERE transport_id=${transportId}`;
      yield* reject(
        sql`UPDATE ${sql(table)} SET local_outcome=NULL,local_reason=NULL,local_observed_at=NULL WHERE transport_id=${transportId}`,
      );
      yield* reject(
        sql`UPDATE ${sql(table)} SET local_reason='timeout' WHERE transport_id=${transportId}`,
      );
      yield* settleParent();
      if (kind === "submitted") {
        yield* reject(
          sql`UPDATE ${sql(table)} SET response_json=${stableStringify({ ...response, nativeThreadId: "x".repeat(16384) })},response_hash=${hash},response_observed_at=${at} WHERE transport_id=${transportId}`,
        );
        yield* sql`UPDATE ${sql(table)} SET ${sql.update(responseUpdate)} WHERE transport_id=${transportId}`;
        for (const patch of [
          { response_json: null, response_hash: null, response_observed_at: null },
          { response_hash: "b".repeat(64) },
          { response_json: stableStringify({ ...response, nativeThreadId: "substituted" }) },
          { response_observed_at: "2026-11-15T00:00:00.000Z" },
        ])
          yield* reject(
            sql`UPDATE ${sql(table)} SET ${sql.update(patch)} WHERE transport_id=${transportId}`,
          );
        assert.deepStrictEqual(
          Codec.decodeConversationReceiptData((yield* sql`SELECT * FROM ${sql(table)}`)[0]),
          opened,
        );
      } else {
        yield* reject(
          sql`UPDATE ${sql(table)} SET ${sql.update(responseUpdate)} WHERE transport_id=${transportId}`,
        );
        yield* reject(
          sql`UPDATE ${sql(table)} SET local_outcome='submitted',local_reason='completed' WHERE transport_id=${transportId}`,
        );
        assert.deepStrictEqual(
          Codec.decodeConversationReceiptData((yield* sql`SELECT * FROM ${sql(table)}`)[0]),
          observed,
        );
      }
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "preserves old transport data and new conversation tombstones across upgrade, replay and reopen",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped();
      const filename = path.join(directory, "conversations.sqlite");
      const expected = yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 92 });
        const { row, response } = yield* seedParent();
        const before = yield* sql`SELECT * FROM ${sql(transports)}`;
        yield* runMigrations({ toMigrationInclusive: 93 });
        assert.deepStrictEqual(yield* sql`SELECT * FROM ${sql(transports)}`, before);
        yield* sql`INSERT INTO ${sql(table)} ${sql.insert(row)}`;
        const observed = Codec.encodeConversationOutcomeData(
          row,
          { kind: "submitted", reason: "completed" },
          at,
        );
        yield* sql`UPDATE ${sql(table)} SET local_outcome=${observed.local_outcome},local_reason=${observed.local_reason},local_observed_at=${at} WHERE transport_id=${transportId}`;
        const opened = Codec.encodeConversationResponseData(observed, response, at, {
          mode: "disabled",
        });
        yield* settleParent();
        yield* sql`UPDATE ${sql(table)} SET response_json=${opened.response_json},response_hash=${opened.response_hash},response_observed_at=${at} WHERE transport_id=${transportId}`;
        assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 93 }), []);
        return opened;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })), Effect.scoped);
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(
          Codec.decodeConversationReceiptData((yield* sql`SELECT * FROM ${sql(table)}`)[0]),
          expected,
        );
        yield* sql`PRAGMA recursive_triggers=OFF`;
        yield* reject(sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert(fixture().row)}`);
        yield* reject(sql`DELETE FROM ${sql(table)}`);
        const indexes = yield* sql<{
          readonly name: string;
        }>`SELECT name FROM pragma_index_list(${table})`;
        assert.isTrue(indexes.some((row) => row.name === "native_conversations_thread_unresolved"));
        assert.isTrue(indexes.some((row) => row.name === "native_conversations_run"));
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })), Effect.scoped);
    }).pipe(Effect.provide(NodeServices.layer)),
);
