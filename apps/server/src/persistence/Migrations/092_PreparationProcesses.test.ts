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
import * as PreparationJob from "../../preparation/PreparationJob.ts";

const table = "orchestration_v2_preparation_process_resources";
const jobs = "orchestration_v2_preparation_jobs";
const at = "2026-11-14T00:00:00.000Z";
const hash = "a".repeat(64);
const claim = {
  operationId: "operation",
  expectedFingerprint: hash,
  expectedGeneration: 2,
  bootId: "boot",
  workerId: "worker",
};
const current = {
  driver: "codex",
  providerInstanceId: "instance",
  providerThreadId: "provider-thread",
  providerSessionId: null,
  nativeThreadId: null,
};
const parent = (id = "parent", native = false) => {
  const input = PreparationJob.ownInput({
    jobId: id,
    entryKey: `entry-${id}`,
    kind: "launch",
    environmentId: "environment",
    projectId: "project",
    threadId: "thread",
    runId: native ? "run" : null,
    originatingLaunchJobId: null,
    parentSelection: null,
    serverBootId: "boot",
    request: native ? { current } : {},
    nativeSelection: null,
    ...(native
      ? {
          nativeOwner: {
            intentId: "intent",
            owner: { kind: "workflow_run", workflowRunId: "workflow" },
            executionClaim: claim,
          },
        }
      : {}),
  });
  const identity = PreparationJob.identity(input);
  return {
    format_version: 1,
    job_id: id,
    entry_key: input.entryKey,
    kind: input.kind,
    environment_id: input.environmentId,
    project_id: input.projectId,
    thread_id: input.threadId,
    run_id: input.runId,
    server_boot_id: input.serverBootId,
    request_identity_json: identity.text,
    request_identity_hash: identity.hash,
    created_at: at,
  };
};
const resource = (id = "child", native = false, jobId = "parent") => {
  const job = parent(jobId, native);
  const context = {
    v: 1,
    profile: "linux-node-no-stdin-v1",
    jobSelection: {
      jobId,
      entryKey: job.entry_key,
      requestIdentityHash: job.request_identity_hash,
      serverBootId: job.server_boot_id,
      expectedVersion: 1,
    },
    environmentId: job.environment_id,
    projectId: job.project_id,
    threadId: job.thread_id,
    runId: job.run_id,
    executionClaim: native ? claim : null,
    current: native ? current : null,
    host: {
      hostBootId: "12345678-1234-4234-8234-123456789abc",
      scopeNamespace: "app-t3code-agents-123456789abc.slice",
    },
    proposedUnit: `t3code-preparation-${id}.scope`,
    creationInputHash: hash,
  };
  return {
    format_version: 1,
    resource_id: id,
    job_id: jobId,
    job_request_identity_hash: job.request_identity_hash,
    environment_id: job.environment_id,
    project_id: job.project_id,
    thread_id: job.thread_id,
    run_id: job.run_id,
    server_boot_id: job.server_boot_id,
    host_boot_id: context.host.hostBootId,
    scope_namespace: context.host.scopeNamespace,
    proposed_unit: context.proposedUnit,
    creation_input_hash: hash,
    creation_context_json: JSON.stringify(context),
    creation_context_hash: hash,
    created_at: at,
  };
};
const binding = (id = "child") => ({
  unit: resource(id).proposed_unit,
  invocationId: "1".repeat(32),
  controlGroup: `/fixture/${resource(id).proposed_unit}`,
  pid: 123,
  pidStartTicks: "9007199254740993",
});
const settlement = (id: string, priorVersion: number) => ({
  v: 1,
  resourceId: id,
  contextHash: hash,
  priorVersion,
  childExit: { pid: 123, exitCode: 0, signal: null },
  scope: { binding: binding(id), observation: "removed" },
});
const seedParent = Effect.fn("test.seedProcessParent")(function* (id = "parent", native = false) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO ${sql(jobs)} ${sql.insert(parent(id, native))}`;
  yield* sql`UPDATE ${sql(jobs)} SET process_exposed=1,record_version=1 WHERE job_id=${id}`;
});
const rows = Effect.fn("test.preparationProcessRows")(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT * FROM ${sql(table)} ORDER BY resource_id`;
});
const reject = Effect.fn("test.rejectProcessMutation")(function* <A, E, R>(
  statement: Effect.Effect<A, E, R>,
) {
  const before = yield* rows();
  assert.isTrue(Exit.isFailure(yield* Effect.exit(statement)));
  assert.deepStrictEqual(yield* rows(), before);
});
const bind = Effect.fn("test.bindPreparationProcess")(function* (id = "child") {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`UPDATE ${sql(table)} SET record_version=1,creation_started_at=${at},child_pid=123 WHERE resource_id=${id}`;
  yield* sql`UPDATE ${sql(table)} SET record_version=2,binding_json=${JSON.stringify(binding(id))},binding_hash=${hash},bound_at=${at} WHERE resource_id=${id}`;
});
const snapshot = Effect.fn("test.snapshotPreProcessStorage")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql<{
    name: string;
  }>`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT IN ('effect_sql_migrations',${table}) ORDER BY name`;
  const retained: Record<string, readonly string[]> = {};
  for (const entry of tables)
    retained[entry.name] = (yield* sql`SELECT * FROM ${sql(entry.name)}`)
      .map(stableStringify)
      .sort();
  const schema =
    yield* sql`SELECT type,name,sql FROM sqlite_master WHERE tbl_name<>${table} ORDER BY name`;
  return { retained, schema };
});

it.effect(
  "adds permanent process storage without changing version91 history through replay and reopen",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-preparation-process-upgrade-",
      });
      const filename = path.join(directory, "state.sqlite");
      const retained = yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 91 });
        yield* seedParent();
        yield* sql`INSERT INTO workflow_plan_identities VALUES ('plan')`;
        yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan','revision',${hash},${hash})`;
        yield* sql`INSERT INTO workflow_runs VALUES ('run','plan','revision',${hash},${hash},${hash},'active',0,0,'{}')`;
        yield* sql`INSERT INTO workflow_native_launch_intents(intent_id,workflow_run_id,operation_id,charge_attempt,repair,role,environment_id,project_id,launch_command_id,thread_id,message_id,fingerprint,draft_json,generation,state) VALUES ('intent','run','operation',0,0,'supervisor','environment','project','launch','thread','message',${hash},'{}',0,'reserved')`;
        yield* sql`INSERT INTO workflow_native_reservation_material VALUES ('intent','{}',${hash})`;
        yield* sql`INSERT INTO orchestration_v2_projection_provider_threads(provider_thread_id,thread_id,provider,status,updated_at,payload_json) VALUES ('provider','thread','codex','not_loaded',${at},'{}')`;
        const before = yield* snapshot();
        assert.deepStrictEqual(yield* runMigrations(), [[92, "PreparationProcesses"]]);
        assert.deepStrictEqual(yield* snapshot(), before);
        assert.deepStrictEqual(yield* rows(), []);
        assert.deepStrictEqual(yield* runMigrations(), []);
        for (const id of ["child", "sibling"])
          yield* sql`INSERT INTO ${sql(table)} ${sql.insert(resource(id))}`;
        yield* bind();
        yield* sql`UPDATE ${sql(table)} SET record_version=3,release_started_at=${at} WHERE resource_id='child'`;
        yield* sql`UPDATE ${sql(table)} SET record_version=4,state='settled',settlement_json=${JSON.stringify(settlement("child", 3))},settlement_hash=${hash},settled_at=${at} WHERE resource_id='child'`;
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
        const plan = yield* sql<{
          detail: string;
        }>`EXPLAIN QUERY PLAN SELECT * FROM ${sql(table)} WHERE job_id='parent' AND resource_id>'' ORDER BY resource_id`;
        assert.isTrue(
          plan.some((row) => row.detail.includes("preparation_process_resources_job_history")),
        );
        return { before, resources: yield* rows() };
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(yield* runMigrations(), []);
        assert.deepStrictEqual(yield* snapshot(), retained.before);
        assert.deepStrictEqual(yield* rows(), retained.resources);
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
        assert.deepStrictEqual(
          (yield* rows()).map((row) => [row.resource_id, row.state]),
          [
            ["child", "settled"],
            ["sibling", "active"],
          ],
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "requires exact exposed open V2 or V3 parent admission and preserves rollback custody",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* seedParent();
      yield* seedParent("native", true);
      for (const [id, native, jobId] of [
        ["child", false, "parent"],
        ["native-child", true, "native"],
      ] as const)
        yield* sql`INSERT INTO ${sql(table)} ${sql.insert(resource(id, native, jobId))}`;
      const baseline = yield* rows();
      const aborted = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`UPDATE ${sql(table)} SET record_version=1,creation_started_at=${at} WHERE resource_id='child'`;
            return yield* Effect.fail("fixture rollback after possible creation");
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(aborted));
      assert.deepStrictEqual(yield* rows(), baseline);
      for (const patch of [
        { job_id: "missing" },
        { environment_id: "foreign" },
        { project_id: "foreign" },
        { thread_id: "foreign" },
        { run_id: "foreign" },
        { server_boot_id: "foreign" },
        { job_request_identity_hash: "b".repeat(64) },
      ])
        yield* reject(
          sql`INSERT INTO ${sql(table)} ${sql.insert({ ...resource("bad"), ...patch })}`,
        );
      for (const [path, value] of [
        ["jobSelection.entryKey", "foreign"],
        ["jobSelection.expectedVersion", 0],
        ["jobSelection.expectedVersion", 2],
        ["executionClaim", claim],
        ["current", current],
      ] as const)
        yield* reject(
          sql`INSERT INTO ${sql(table)} ${sql.insert({ ...resource("bad"), creation_context_json: JSON.stringify(change(JSON.parse(resource("bad").creation_context_json), path, value)) })}`,
        );
      for (const [path, value] of [
        ["executionClaim.operationId", "foreign"],
        ["executionClaim.expectedFingerprint", "b".repeat(64)],
        ["executionClaim.expectedGeneration", 3],
        ["executionClaim.bootId", "foreign"],
        ["executionClaim.workerId", "foreign"],
        ["current.driver", "other"],
        ["current.providerInstanceId", "other"],
        ["current.providerThreadId", "other"],
        ["current.providerSessionId", "other"],
        ["current.nativeThreadId", "other"],
        ["current", null],
        ["executionClaim", null],
      ] as const) {
        const row = resource("bad", true, "native");
        yield* reject(
          sql`INSERT INTO ${sql(table)} ${sql.insert({ ...row, creation_context_json: JSON.stringify(change(JSON.parse(row.creation_context_json), path, value)) })}`,
        );
      }
      for (const [id, patch] of [
        ["closed", { code_closed_at: at }],
        ["quiescing", { state: "quiescing" }],
        ["unknown", { state: "unknown" }],
        [
          "settled",
          {
            state: "settled",
            code_closed_at: at,
            settlement_json: JSON.stringify({
              v: 1,
              jobId: "settled",
              entryKey: "entry-settled",
              priorVersion: 1,
              kind: "owned_resources",
            }),
            settlement_hash: hash,
            settled_at: at,
          },
        ],
      ] as const) {
        yield* seedParent(id);
        yield* sql`UPDATE ${sql(jobs)} SET ${sql.update({ ...patch, record_version: 2 })} WHERE job_id=${id}`;
        const blocked = resource("bad", false, id);
        yield* reject(
          sql`INSERT INTO ${sql(table)} ${sql.insert({ ...blocked, creation_context_json: JSON.stringify(change(JSON.parse(blocked.creation_context_json), "jobSelection.expectedVersion", 2)) })}`,
        );
      }
      const unexposed = parent("unexposed");
      yield* sql`INSERT INTO ${sql(jobs)} ${sql.insert(unexposed)}`;
      const row = resource("bad", false, "unexposed");
      yield* reject(
        sql`INSERT INTO ${sql(table)} ${sql.insert({ ...row, creation_context_json: JSON.stringify(change(JSON.parse(row.creation_context_json), "jobSelection.expectedVersion", 0)) })}`,
      );
      const legacy = parent("legacy");
      yield* sql`INSERT INTO ${sql(jobs)} ${sql.insert({ ...legacy, request_identity_json: '{"v":1}' })}`;
      yield* sql`UPDATE ${sql(jobs)} SET process_exposed=1,record_version=1 WHERE job_id='legacy'`;
      yield* reject(sql`INSERT INTO ${sql(table)} ${sql.insert(resource("bad", false, "legacy"))}`);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

function change(value: unknown, path: string, replacement: unknown): unknown {
  const result = structuredClone(value) as Record<string, unknown>;
  const keys = path.split(".");
  let node = result;
  for (const key of keys.slice(0, -1)) node = node[key] as Record<string, unknown>;
  const key = keys.at(-1)!;
  if (replacement === undefined) delete node[key];
  else node[key] = replacement;
  return result;
}

it.effect(
  "rejects missing or mistyped context and BLOB shadows instead of admitting ambiguous identity",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* seedParent();
      yield* seedParent("native", true);
      for (const native of [false, true]) {
        const row = resource("bad", native, native ? "native" : "parent");
        const context: unknown = JSON.parse(row.creation_context_json);
        const required = [
          "v",
          "profile",
          "jobSelection",
          "jobSelection.jobId",
          "jobSelection.entryKey",
          "jobSelection.requestIdentityHash",
          "jobSelection.serverBootId",
          "jobSelection.expectedVersion",
          "environmentId",
          "projectId",
          "threadId",
          "runId",
          "host",
          "host.hostBootId",
          "host.scopeNamespace",
          "proposedUnit",
          "creationInputHash",
          "executionClaim",
          "current",
        ];
        if (native)
          required.push(
            ...claimFields.map((field) => `executionClaim.${field}`),
            ...currentFields.map((field) => `current.${field}`),
          );
        for (const path of required) {
          yield* reject(
            sql`INSERT INTO ${sql(table)} ${sql.insert({ ...row, creation_context_json: JSON.stringify(change(context, path, undefined)) })}`,
          );
          for (const bad of [[], {}, true])
            yield* reject(
              sql`INSERT INTO ${sql(table)} ${sql.insert({ ...row, creation_context_json: JSON.stringify(change(context, path, bad)) })}`,
            );
        }
        for (const path of [
          "v",
          "profile",
          "jobSelection",
          "jobSelection.expectedVersion",
          "host",
          "host.hostBootId",
          ...(native
            ? ["executionClaim.operationId", "executionClaim.expectedGeneration", "current.driver"]
            : []),
        ])
          yield* reject(
            sql`INSERT INTO ${sql(table)} ${sql.insert({ ...row, creation_context_json: JSON.stringify(change(context, path, null)) })}`,
          );
      }
      const required = { ...resource("blob"), state: "active" };
      for (const [column, value] of Object.entries(required)) {
        if (typeof value !== "string") continue;
        yield* reject(
          sql`INSERT INTO ${sql(table)} ${sql.insert({ ...required, [column]: new TextEncoder().encode(value) })}`,
        );
      }
      const nativeRow = resource("blob-native", true, "native");
      yield* reject(
        sql`INSERT INTO ${sql(table)} ${sql.insert({ ...nativeRow, run_id: new TextEncoder().encode(nativeRow.run_id!) })}`,
      );
      for (const patch of [
        { format_version: 2 },
        { record_version: 1 },
        { record_version: 0.5 },
        { record_version: 9007199254740992 },
        { state: "unknown" },
        { creation_started_at: at },
        { child_pid: 123 },
        { release_started_at: at },
        { creation_context_json: "[]" },
        { creation_context_json: "{bad" },
        { creation_input_hash: "A".repeat(64) },
      ])
        yield* reject(
          sql`INSERT INTO ${sql(table)} ${sql.insert({ ...resource("bad"), ...patch })}`,
        );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

const claimFields = [
  "operationId",
  "expectedFingerprint",
  "expectedGeneration",
  "bootId",
  "workerId",
];
const currentFields = [
  "driver",
  "providerInstanceId",
  "providerThreadId",
  "providerSessionId",
  "nativeThreadId",
];

it.effect("retains a signal terminal event without fabricating a numeric exit code", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedParent();
    yield* sql`INSERT INTO ${sql(table)} ${sql.insert(resource())}`;
    yield* bind();
    const evidence = {
      ...settlement("child", 2),
      childExit: { pid: 123, exitCode: null, signal: "SIGTERM" },
    };
    for (const patch of [
      { exitCode: null, signal: null },
      { exitCode: 0, signal: "SIGTERM" },
      { exitCode: null, signal: "" },
      { exitCode: null, signal: 15 },
      { exitCode: null, signal: [] },
      { exitCode: null, signal: {} },
      { exitCode: "0", signal: null },
      { exitCode: true, signal: null },
    ])
      yield* reject(
        sql`UPDATE ${sql(table)} SET record_version=3,state='settled',settlement_json=${JSON.stringify({ ...evidence, childExit: { pid: 123, ...patch } })},settlement_hash=${hash},settled_at=${at} WHERE resource_id='child'`,
      );
    for (const path of ["childExit.exitCode", "childExit.signal"])
      yield* reject(
        sql`UPDATE ${sql(table)} SET record_version=3,state='settled',settlement_json=${JSON.stringify(change(evidence, path, undefined))},settlement_hash=${hash},settled_at=${at} WHERE resource_id='child'`,
      );
    yield* sql`UPDATE ${sql(table)} SET record_version=3,state='settled',settlement_json=${JSON.stringify(evidence)},settlement_hash=${hash},settled_at=${at} WHERE resource_id='child'`;
    const [retained] = yield* rows();
    assert.deepStrictEqual(JSON.parse(retained!.settlement_json as string), evidence);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("retains exact immutable history against replacements, stale versions and rearm", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedParent();
    yield* sql`INSERT INTO ${sql(table)} ${sql.insert(resource())}`;
    for (const recursive of [false, true]) {
      yield* sql.unsafe(`PRAGMA recursive_triggers=${recursive ? "ON" : "OFF"}`);
      for (const row of [
        resource(),
        { ...resource("other"), proposed_unit: resource().proposed_unit },
      ]) {
        yield* reject(sql`INSERT OR REPLACE INTO ${sql(table)} ${sql.insert(row)}`);
        yield* reject(
          sql`INSERT INTO ${sql(table)} ${sql.insert(row)} ON CONFLICT DO UPDATE SET record_version=record_version+1`,
        );
      }
    }
    yield* reject(sql`DELETE FROM ${sql(table)} WHERE resource_id='child'`);
    for (const [column, value] of Object.entries(resource())) {
      const replacement =
        typeof value === "number" ? 2 : value === null ? "run" : `${value}-changed`;
      yield* reject(
        sql`UPDATE ${sql(table)} SET ${sql.update({ [column]: replacement, record_version: 1 })} WHERE resource_id='child'`,
      );
    }
    for (const version of [0, 2, -1, 0.5, 9007199254740992])
      yield* reject(
        sql`UPDATE ${sql(table)} SET record_version=${version} WHERE resource_id='child'`,
      );
    yield* bind();
    for (const patch of [
      { creation_started_at: "later" },
      { creation_started_at: null },
      { child_pid: 124 },
      { child_pid: null },
      { binding_json: null, binding_hash: null, bound_at: null },
      { binding_hash: "b".repeat(64) },
      { bound_at: "later" },
    ])
      yield* reject(
        sql`UPDATE ${sql(table)} SET ${sql.update({ ...patch, record_version: 3 })} WHERE resource_id='child'`,
      );
    yield* sql`UPDATE ${sql(table)} SET record_version=3,release_started_at=${at} WHERE resource_id='child'`;
    yield* reject(
      sql`UPDATE ${sql(table)} SET record_version=4,release_started_at=NULL WHERE resource_id='child'`,
    );
    yield* sql`UPDATE ${sql(table)} SET record_version=4,state='quiescing' WHERE resource_id='child'`;
    yield* reject(
      sql`UPDATE ${sql(table)} SET record_version=5,state='active' WHERE resource_id='child'`,
    );
    yield* sql`UPDATE ${sql(table)} SET record_version=5,state='unknown' WHERE resource_id='child'`;
    yield* reject(
      sql`UPDATE ${sql(table)} SET record_version=6,state='quiescing' WHERE resource_id='child'`,
    );
    yield* sql`UPDATE ${sql(table)} SET record_version=6,state='settled',settlement_json=${JSON.stringify(settlement("child", 5))},settlement_hash=${hash},settled_at=${at} WHERE resource_id='child'`;
    yield* reject(sql`UPDATE ${sql(table)} SET record_version=7 WHERE resource_id='child'`);
    assert.deepStrictEqual(yield* sql`SELECT state FROM ${sql(jobs)}`, [{ state: "active" }]);
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect(
  "requires original binding and complete retirement while allowing late cleanup after parent closure",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* seedParent();
      for (const id of ["child", "late", "blocked"])
        yield* sql`INSERT INTO ${sql(table)} ${sql.insert(resource(id))}`;
      yield* reject(
        sql`UPDATE ${sql(table)} SET record_version=1,release_started_at=${at} WHERE resource_id='child'`,
      );
      yield* sql`UPDATE ${sql(table)} SET record_version=1,creation_started_at=${at},child_pid=123 WHERE resource_id='child'`;
      for (const [path, bad] of [
        ["unit", "foreign"],
        ["pid", 124],
        ["pid", "123"],
        ["pid", 0],
        ["invocationId", "0".repeat(32)],
        ["invocationId", "A".repeat(32)],
        ["controlGroup", "relative"],
        ["pidStartTicks", "01"],
        ["pidStartTicks", "0"],
        ["pidStartTicks", 123],
      ] as const)
        yield* reject(
          sql`UPDATE ${sql(table)} SET record_version=2,binding_json=${JSON.stringify(change(binding(), path, bad))},binding_hash=${hash},bound_at=${at} WHERE resource_id='child'`,
        );
      for (const path of ["unit", "invocationId", "controlGroup", "pid", "pidStartTicks"])
        for (const bad of [undefined, null, [], {}])
          yield* reject(
            sql`UPDATE ${sql(table)} SET record_version=2,binding_json=${JSON.stringify(change(binding(), path, bad))},binding_hash=${hash},bound_at=${at} WHERE resource_id='child'`,
          );
      for (const patch of [
        { binding_json: JSON.stringify(binding()) },
        { binding_hash: hash, bound_at: at },
        { child_pid: 0 },
        { child_pid: 1.5 },
        { child_pid: 9007199254740992 },
      ])
        yield* reject(
          sql`UPDATE ${sql(table)} SET ${sql.update({ ...patch, record_version: 2 })} WHERE resource_id='child'`,
        );
      yield* sql`UPDATE ${sql(table)} SET record_version=2,binding_json=${JSON.stringify(binding())},binding_hash=${hash},bound_at=${at} WHERE resource_id='child'`;
      const valid = settlement("child", 2);
      for (const path of [
        "v",
        "resourceId",
        "contextHash",
        "priorVersion",
        "childExit",
        "childExit.pid",
        "childExit.exitCode",
        "scope",
        "scope.binding",
        "scope.observation",
        ...Object.keys(binding()).map((key) => `scope.binding.${key}`),
      ])
        for (const bad of [undefined, null, [], {}])
          yield* reject(
            sql`UPDATE ${sql(table)} SET record_version=3,state='settled',settlement_json=${JSON.stringify(change(valid, path, bad))},settlement_hash=${hash},settled_at=${at} WHERE resource_id='child'`,
          );
      for (const [path, bad] of [
        ["resourceId", "other"],
        ["contextHash", "b".repeat(64)],
        ["priorVersion", 1],
        ["childExit.pid", 124],
        ["childExit.exitCode", -1],
        ["childExit.exitCode", 0.5],
        ["scope.observation", "not_found"],
        ["scope.binding.unit", "foreign"],
        ["scope.binding.invocationId", "2".repeat(32)],
        ["scope.binding.pidStartTicks", "other"],
        ["scope.binding.controlGroup", "/foreign"],
        ["scope.binding.pid", 124],
      ] as const)
        yield* reject(
          sql`UPDATE ${sql(table)} SET record_version=3,state='settled',settlement_json=${JSON.stringify(change(valid, path, bad))},settlement_hash=${hash},settled_at=${at} WHERE resource_id='child'`,
        );
      yield* reject(
        sql`UPDATE ${sql(table)} SET record_version=1,state='settled',creation_started_at=${at},child_pid=123,binding_json=${JSON.stringify(binding("late"))},binding_hash=${hash},bound_at=${at},settlement_json=${JSON.stringify(settlement("late", 0))},settlement_hash=${hash},settled_at=${at} WHERE resource_id='late'`,
      );
      yield* sql`UPDATE ${sql(jobs)} SET record_version=2,state='unknown',code_closed_at=${at} WHERE job_id='parent'`;
      yield* reject(
        sql`UPDATE ${sql(table)} SET record_version=1,creation_started_at=${at} WHERE resource_id='blocked'`,
      );
      yield* reject(
        sql`UPDATE ${sql(table)} SET record_version=3,release_started_at=${at} WHERE resource_id='child'`,
      );
      yield* sql`UPDATE ${sql(table)} SET record_version=1,state='unknown' WHERE resource_id='late'`;
      yield* sql`UPDATE ${sql(table)} SET record_version=2,creation_started_at=${at},child_pid=123,binding_json=${JSON.stringify(binding("late"))},binding_hash=${hash},bound_at=${at} WHERE resource_id='late'`;
      yield* reject(
        sql`UPDATE ${sql(table)} SET record_version=3,state='active',release_started_at=${at} WHERE resource_id='late'`,
      );
      yield* sql`UPDATE ${sql(table)} SET record_version=3,state='settled',settlement_json=${JSON.stringify(settlement("late", 2))},settlement_hash=${hash},settled_at=${at} WHERE resource_id='late'`;
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);

it.effect("rejects BLOB evidence with coherent companions and refuses safe-version overflow", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    yield* seedParent();
    yield* sql`INSERT INTO ${sql(table)} ${sql.insert(resource())}`;
    const encoding = new TextEncoder();
    yield* reject(
      sql`UPDATE ${sql(table)} SET record_version=1,creation_started_at=${encoding.encode(at)} WHERE resource_id='child'`,
    );
    yield* sql`UPDATE ${sql(table)} SET record_version=1,creation_started_at=${at},child_pid=123 WHERE resource_id='child'`;
    const bound = { binding_json: JSON.stringify(binding()), binding_hash: hash, bound_at: at };
    for (const [column, value] of Object.entries(bound))
      yield* reject(
        sql`UPDATE ${sql(table)} SET ${sql.update({ ...bound, [column]: encoding.encode(value), record_version: 2 })} WHERE resource_id='child'`,
      );
    yield* sql`UPDATE ${sql(table)} SET ${sql.update({ ...bound, record_version: 2 })} WHERE resource_id='child'`;
    yield* reject(
      sql`UPDATE ${sql(table)} SET record_version=3,release_started_at=${encoding.encode(at)} WHERE resource_id='child'`,
    );
    const retired = {
      settlement_json: JSON.stringify(settlement("child", 2)),
      settlement_hash: hash,
      settled_at: at,
    };
    for (const [column, value] of Object.entries(retired))
      yield* reject(
        sql`UPDATE ${sql(table)} SET ${sql.update({ ...retired, [column]: encoding.encode(value), state: "settled", record_version: 3 })} WHERE resource_id='child'`,
      );
    const [trigger] = yield* sql<{
      sql: string;
    }>`SELECT sql FROM sqlite_master WHERE name='preparation_process_resources_update_guard'`;
    yield* sql`DROP TRIGGER preparation_process_resources_update_guard`;
    yield* sql`UPDATE ${sql(table)} SET record_version=9007199254740990 WHERE resource_id='child'`;
    yield* sql.unsafe(trigger!.sql);
    yield* sql`UPDATE ${sql(table)} SET record_version=9007199254740991 WHERE resource_id='child'`;
    yield* reject(
      sql`UPDATE ${sql(table)} SET record_version=record_version+1 WHERE resource_id='child'`,
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
