import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../persistence/Migrations.ts";
import * as Job from "./PreparationJob.ts";
import * as Resource from "./PreparationProcessResource.ts";

const resourceId = "11111111-1111-4111-8111-111111111111";
const unitId = "22222222-2222-4222-8222-222222222222";
const at = "2026-11-14T00:00:00.000Z";
const hash = "a".repeat(64);
const claim = {
  operationId: "operation",
  expectedFingerprint: hash,
  expectedGeneration: 0,
  bootId: "server-boot",
  workerId: "worker",
};
const current = {
  driver: "codex",
  providerInstanceId: "instance",
  providerThreadId: "provider-thread",
  providerSessionId: null,
  nativeThreadId: null,
};
const jobInput = Job.ownInput({
  jobId: "job",
  entryKey: "entry",
  kind: "launch",
  environmentId: "environment",
  projectId: "project",
  threadId: "thread",
  runId: "run",
  serverBootId: "server-boot",
  originatingLaunchJobId: null,
  parentSelection: null,
  nativeSelection: null,
  nativeOwner: {
    intentId: "intent",
    owner: { kind: "workflow_run", workflowRunId: "workflow" },
    executionClaim: claim,
  },
  request: { current },
});
const jobIdentity = Job.identity(jobInput);
const context = () =>
  Resource.ownContext({
    v: 1,
    profile: "linux-node-no-stdin-v1",
    jobSelection: {
      jobId: "job",
      entryKey: "entry",
      requestIdentityHash: jobIdentity.hash,
      serverBootId: "server-boot",
      expectedVersion: 1,
    },
    environmentId: "environment",
    projectId: "project",
    threadId: "thread",
    runId: "run",
    executionClaim: claim,
    current,
    host: {
      hostBootId: "33333333-3333-4333-8333-333333333333",
      scopeNamespace: "app-t3code-agents-123456789abc.slice",
    },
    proposedUnit: `t3code-preparation-${unitId}.scope`,
    creationInputHash: hash,
  });
const binding = () =>
  Resource.ownBinding(
    {
      unit: context().proposedUnit,
      invocationId: "1".repeat(32),
      controlGroup: `/app.slice/${context().host.scopeNamespace}/${context().proposedUnit}`,
      pid: 123,
      pidStartTicks: "9007199254740993123456789",
    },
    context(),
  );
const settlement = (signal: string | null = null, priorVersion = 1) => ({
  v: 1,
  resourceId,
  contextHash: Resource.encodeContext(context()).hash,
  priorVersion,
  childExit: { pid: 123, exitCode: signal === null ? 0 : null, signal },
  scope: { binding: binding(), observation: "removed" },
});
const row = () => {
  const value = context();
  const encoded = Resource.encodeContext(value);
  return {
    format_version: 1,
    resource_id: resourceId,
    job_id: value.jobSelection.jobId,
    job_request_identity_hash: value.jobSelection.requestIdentityHash,
    environment_id: value.environmentId,
    project_id: value.projectId,
    thread_id: value.threadId,
    run_id: value.runId,
    server_boot_id: value.jobSelection.serverBootId,
    host_boot_id: value.host.hostBootId,
    scope_namespace: value.host.scopeNamespace,
    proposed_unit: value.proposedUnit,
    creation_input_hash: value.creationInputHash,
    creation_context_json: encoded.text,
    creation_context_hash: encoded.hash,
    created_at: at,
    record_version: 0,
    state: "active",
    creation_started_at: null,
    child_pid: null,
    binding_json: null,
    binding_hash: null,
    bound_at: null,
    release_started_at: null,
    settlement_json: null,
    settlement_hash: null,
    settled_at: null,
  };
};
const boundRow = () => {
  const encoded = Resource.encodeBinding(resourceId, context(), binding());
  return {
    ...row(),
    record_version: 1,
    creation_started_at: at,
    child_pid: 123,
    binding_json: encoded.text,
    binding_hash: encoded.hash,
    bound_at: at,
  };
};
const retiredRow = (signal: string | null = null) => {
  const encoded = Resource.encodeSettlement(
    resourceId,
    context(),
    binding(),
    1,
    settlement(signal),
  );
  return {
    ...boundRow(),
    record_version: 2,
    state: "settled",
    settlement_json: encoded.text,
    settlement_hash: encoded.hash,
    settled_at: at,
  };
};
function change(value: unknown, path: string, replacement: unknown) {
  const result = structuredClone(value) as Record<string, unknown>;
  const keys = path.split(".");
  let node = result;
  for (const key of keys.slice(0, -1)) node = node[key] as Record<string, unknown>;
  const key = keys.at(-1)!;
  if (replacement === undefined) delete node[key];
  else node[key] = replacement;
  return result;
}

it("owns context and physical evidence without rounding ticks or conflating null identity", () => {
  const mutable = { ...context(), host: { ...context().host } };
  const owned = Resource.ownContext(mutable);
  mutable.host.hostBootId = "44444444-4444-4444-8444-444444444444";
  assert.notStrictEqual(owned.host.hostBootId, mutable.host.hostBootId);
  assert.isTrue(Object.isFrozen(owned));
  assert.isTrue(Object.isFrozen(owned.host));
  assert.isTrue(Object.isFrozen(owned.current));
  assert.isTrue(Object.isFrozen(owned.executionClaim));
  assert.isTrue(Object.isFrozen(owned.jobSelection));
  assert.strictEqual(binding().pidStartTicks, "9007199254740993123456789");
  const ordinary = Resource.ownContext({
    ...context(),
    executionClaim: null,
    current: null,
    runId: null,
  });
  assert.strictEqual(ordinary.current, null);
  assert.strictEqual(ordinary.executionClaim, null);
  assert.throws(() => Resource.ownContext({ ...ordinary, current }));
  assert.throws(() => Resource.ownContext({ ...ordinary, executionClaim: claim }));
  const encoded = Resource.encodeContext(owned);
  assert.strictEqual(encoded.text, stableStringify(owned));
  assert.deepStrictEqual(Resource.decodeRow(row()).context, owned);
  assert.deepStrictEqual(Resource.selection(Resource.decodeRow(row())), {
    resourceId,
    contextHash: encoded.hash,
    expectedVersion: 0,
  });
});

it("rejects malformed nested context, getters and noncanonical inputs before hashing", () => {
  for (const [path, value] of [
    ["v", 2],
    ["profile", "other"],
    ["host.hostBootId", "00000000-0000-0000-0000-000000000000"],
    ["host.hostBootId", "UPPERCASE"],
    ["host.scopeNamespace", "app-t3code-agents.slice"],
    ["proposedUnit", "t3code-preparation-foreign.scope"],
    ["creationInputHash", "A".repeat(64)],
    ["jobSelection.expectedVersion", -0],
    ["jobSelection.expectedVersion", 0.5],
    ["jobSelection.jobId", "bad\0id"],
    ["jobSelection.entryKey", "\ud800"],
    ["executionClaim.bootId", "foreign"],
    ["executionClaim.expectedGeneration", -1],
    ["executionClaim.workerId", "\0"],
    ["current.providerThreadId", ""],
    ["current.providerSessionId", undefined],
    ["current.nativeThreadId", 0],
    ["current", null],
    ["runId", null],
  ] as const)
    assert.throws(() => Resource.ownContext(change(context(), path, value)));
  const original = context();
  for (const path of ["host", "jobSelection", "executionClaim", "current"] as const) {
    const nested = original[path];
    if (nested === null) throw new Error("Expected native fixture context");
    assert.throws(() => Resource.ownContext(change(original, path, { ...nested, extra: true })));
  }
  let reads = 0;
  const getter = Object.defineProperty({ ...context() }, "environmentId", {
    enumerable: true,
    get: () => {
      reads++;
      return "environment";
    },
  });
  for (const value of [
    getter,
    { ...context(), extra: true },
    { ...context(), [Symbol("extra")]: true },
    new Map(),
    Object.create(context()),
    { ...context(), threadId: undefined },
  ])
    assert.throws(() => Resource.encodeContext(value));
  assert.strictEqual(reads, 0);
});

it("binds exact scope namespace, unit, PID and all original binding fields", () => {
  for (const [field, value] of [
    ["unit", `t3code-preparation-${resourceId}.scope`],
    ["invocationId", "0".repeat(32)],
    ["invocationId", "A".repeat(32)],
    ["pid", 0],
    ["pid", 0.5],
    ["pid", Number.MAX_SAFE_INTEGER + 1],
    ["pidStartTicks", 123],
    ["pidStartTicks", "01"],
    ["controlGroup", `/other.slice/${context().proposedUnit}`],
    ["controlGroup", `/app.slice/${context().host.scopeNamespace}/../${context().proposedUnit}`],
    ["controlGroup", `/app.slice//${context().host.scopeNamespace}/${context().proposedUnit}`],
    ["controlGroup", `/app.slice/${context().host.scopeNamespace}/${context().proposedUnit}/`],
  ] as const)
    assert.throws(() => Resource.ownBinding(change(binding(), field, value), context()));
  for (const field of Object.keys(binding()))
    assert.throws(() => Resource.ownBinding(change(binding(), field, undefined), context()));
  const original = Resource.encodeBinding(resourceId, context(), binding());
  const other = Resource.encodeBinding(
    "55555555-5555-4555-8555-555555555555",
    context(),
    binding(),
  );
  assert.notStrictEqual(original.hash, other.hash);
  assert.notStrictEqual(
    original.hash,
    Resource.encodeBinding(
      resourceId,
      { ...context(), creationInputHash: "b".repeat(64) },
      binding(),
    ).hash,
  );
});

it.each([null, "SIGTERM", "SIGKILL", "SIGIOT", "SIGPOLL"])(
  "retains real terminal code-or-signal evidence: %s",
  (signal) => {
    const value = Resource.decodeRow(retiredRow(signal));
    assert.strictEqual(value.state, "settled");
    assert.deepStrictEqual(value.settlement?.childExit, {
      pid: 123,
      exitCode: signal === null ? 0 : null,
      signal,
    });
    assert.isTrue(Object.isFrozen(value.settlement?.scope.binding));
  },
);

it("refuses incomplete, ambiguous and mismatched physical settlement evidence", () => {
  const original = settlement();
  for (const [path, value] of [
    ["v", 2],
    ["resourceId", unitId],
    ["contextHash", "b".repeat(64)],
    ["priorVersion", 0],
    ["childExit.pid", 124],
    ["childExit.exitCode", -1],
    ["childExit.exitCode", 0.5],
    ["childExit.signal", "SIGTERM"],
    ["childExit.exitCode", null],
    ["childExit.signal", undefined],
    ["scope.observation", "not_found"],
    ["scope.binding.invocationId", "2".repeat(32)],
    ["scope.binding.pidStartTicks", "123"],
    ["scope.binding.pid", 124],
  ] as const)
    assert.throws(() =>
      Resource.ownSettlement(change(original, path, value), context(), resourceId, binding(), 1),
    );
  for (const signal of ["SIGBREAK", "SIGUNKNOWN", "SIGCLD", "", 15])
    assert.throws(() =>
      Resource.ownSettlement(
        change(settlement("SIGTERM"), "childExit.signal", signal),
        context(),
        resourceId,
        binding(),
        1,
      ),
    );
  for (const path of [
    "scope",
    "scope.binding",
    "scope.observation",
    "childExit",
    "childExit.pid",
    "childExit.exitCode",
    "childExit.signal",
    "priorVersion",
  ])
    assert.throws(() =>
      Resource.ownSettlement(
        change(original, path, undefined),
        context(),
        resourceId,
        binding(),
        1,
      ),
    );
  const encoded = Resource.encodeSettlement(resourceId, context(), binding(), 1, original);
  assert.strictEqual(encoded.text, stableStringify(original));
  assert.notStrictEqual(
    encoded.hash,
    Resource.encodeBinding(resourceId, context(), binding()).hash,
  );
});

it("checks every SQL shadow and canonical stored JSON or hash independently", () => {
  const initial = row();
  for (const [column, value] of [
    ["format_version", "1"],
    ["format_version", 2],
    ["resource_id", "invalid"],
    ["job_id", "foreign"],
    ["job_request_identity_hash", "b".repeat(64)],
    ["environment_id", "foreign"],
    ["project_id", "foreign"],
    ["thread_id", "foreign"],
    ["run_id", null],
    ["server_boot_id", "foreign"],
    ["host_boot_id", unitId],
    ["scope_namespace", "app-t3code-agents-987654321abc.slice"],
    ["proposed_unit", `t3code-preparation-${resourceId}.scope`],
    ["creation_input_hash", "b".repeat(64)],
    ["creation_context_hash", "b".repeat(64)],
    ["creation_context_json", " " + initial.creation_context_json],
    ["creation_context_json", "null"],
    ["created_at", "2026-11-14T00:00:00Z"],
    ["created_at", "2026-11-14T01:00:00.000+01:00"],
  ] as const)
    assert.throws(() => Resource.decodeRow({ ...initial, [column]: value }));
  for (const [source, column] of [
    [boundRow(), "binding_json"],
    [retiredRow(), "settlement_json"],
  ] as const)
    assert.throws(() => Resource.decodeRow({ ...source, [column]: " " + source[column] }));
  assert.throws(() => Resource.decodeRow({ ...boundRow(), binding_hash: "b".repeat(64) }));
  assert.throws(() => Resource.decodeRow({ ...retiredRow(), settlement_hash: "b".repeat(64) }));
  for (const column of Object.keys(initial)) {
    const missing: Record<string, unknown> = { ...initial };
    delete missing[column];
    assert.throws(() => Resource.decodeRow(missing));
  }
  assert.throws(() => Resource.decodeRow({ ...initial, extra: true }));
});

it("requires coherent versioned phase evidence and never interprets blank markers as noncreation", () => {
  const empty = Resource.decodeRow(row());
  assert.strictEqual(empty.state, "active");
  assert.strictEqual(empty.creationStartedAt, null);
  assert.strictEqual(empty.settlement, null);
  assert.strictEqual(
    Resource.decodeRow({ ...row(), record_version: 1, state: "unknown" }).state,
    "unknown",
  );
  for (const patch of [
    { record_version: -0 },
    { record_version: -1 },
    { record_version: 0.5 },
    { record_version: Number.MAX_SAFE_INTEGER + 1 },
    { state: "bad" },
    { state: "quiescing" },
    { creation_started_at: at },
    { child_pid: 123 },
    { release_started_at: at },
    { settled_at: at },
    { settlement_hash: hash },
  ])
    assert.throws(() => Resource.decodeRow({ ...row(), ...patch }));
  for (const patch of [
    { record_version: 0 },
    { creation_started_at: null },
    { child_pid: null },
    { child_pid: 124 },
    { binding_json: null },
    { binding_hash: null },
    { bound_at: null },
    { release_started_at: at },
  ])
    assert.throws(() => Resource.decodeRow({ ...boundRow(), ...patch }));
  for (const patch of [
    { record_version: 1 },
    { state: "active" },
    { binding_json: null, binding_hash: null, bound_at: null },
    { settlement_json: null },
    { settlement_hash: null },
    { settled_at: null },
    { release_started_at: at },
  ])
    assert.throws(() => Resource.decodeRow({ ...retiredRow(), ...patch }));
  assert.strictEqual(
    Resource.decodeRow({ ...row(), record_version: Number.MAX_SAFE_INTEGER }).recordVersion,
    Number.MAX_SAFE_INTEGER,
  );
  assert.strictEqual(
    Resource.decodeRow({ ...boundRow(), record_version: 2, release_started_at: at })
      .releaseStartedAt,
    at,
  );
});

it("rejects binary SQL values in all required and nullable text columns", () => {
  const sources = [
    row(),
    boundRow(),
    retiredRow(),
    { ...boundRow(), record_version: 2, release_started_at: at },
  ];
  for (const source of sources)
    for (const [column, value] of Object.entries(source))
      if (typeof value === "string")
        assert.throws(() =>
          Resource.decodeRow({ ...source, [column]: new TextEncoder().encode(value) }),
        );
});

it.effect(
  "roundtrips admitted, bound and normal-or-signal retired rows through migrated SQLite and reopen",
  () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "t3-preparation-resource-codec-",
      });
      const filename = path.join(directory, "state.sqlite");
      const retained = yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations();
        yield* sql`INSERT INTO orchestration_v2_preparation_jobs ${sql.insert({
          format_version: 1,
          job_id: jobInput.jobId,
          entry_key: jobInput.entryKey,
          kind: jobInput.kind,
          environment_id: jobInput.environmentId,
          project_id: jobInput.projectId,
          thread_id: jobInput.threadId,
          run_id: jobInput.runId,
          server_boot_id: jobInput.serverBootId,
          request_identity_json: jobIdentity.text,
          request_identity_hash: jobIdentity.hash,
          created_at: at,
        })}`;
        yield* sql`UPDATE orchestration_v2_preparation_jobs SET process_exposed=1,record_version=1 WHERE job_id='job'`;
        const [admitted] =
          yield* sql`INSERT INTO orchestration_v2_preparation_process_resources ${sql.insert(row())} RETURNING *`;
        assert.deepStrictEqual(Resource.decodeRow(admitted), Resource.decodeRow(row()));
        const [bound] =
          yield* sql`UPDATE orchestration_v2_preparation_process_resources SET ${sql.update(boundRow())} WHERE resource_id=${resourceId} RETURNING *`;
        assert.deepStrictEqual(Resource.decodeRow(bound), Resource.decodeRow(boundRow()));
        const [retired] =
          yield* sql`UPDATE orchestration_v2_preparation_process_resources SET ${sql.update(retiredRow("SIGTERM"))} WHERE resource_id=${resourceId} RETURNING *`;
        assert.deepStrictEqual(
          Resource.decodeRow(retired),
          Resource.decodeRow(retiredRow("SIGTERM")),
        );
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
        return retired;
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        assert.deepStrictEqual(yield* runMigrations(), []);
        const [loaded] =
          yield* sql`SELECT * FROM orchestration_v2_preparation_process_resources WHERE resource_id=${resourceId}`;
        assert.deepStrictEqual(loaded, retained);
        assert.deepStrictEqual(
          Resource.decodeRow(loaded),
          Resource.decodeRow(retiredRow("SIGTERM")),
        );
        assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename })));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
