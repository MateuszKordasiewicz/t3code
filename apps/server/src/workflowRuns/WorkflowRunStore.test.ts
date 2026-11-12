import { assert, it } from "@effect/vitest";
import { PlanningSessionV1, WorkflowRunV1 } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/081_WorkflowRuns.ts";
import * as Store from "./WorkflowRunStore.ts";
import { WorkflowRunPolicy } from "./WorkflowRunPolicy.ts";

const hash = "a".repeat(64);
const nextHash = "b".repeat(64);
const run = Schema.decodeSync(WorkflowRunV1)({
  schemaVersion: 1,
  id: "run",
  spec: {
    ref: { kind: "configuration", id: "spec", sha256: hash },
    plan: {
      planId: "plan",
      revisionId: "revision",
      revisionSchemaVersion: 1,
      revisionHash: hash,
      sourcePacketHash: hash,
    },
  },
  mode: "ordinary",
  origin: { kind: "human", id: "human" },
  status: "active",
  controlGeneration: 0,
  recordVersion: 0,
  allowance: {
    maxAttempts: 2,
    maxRepairRounds: 1,
    maxActiveWriters: 1,
    attemptsConsumed: 0,
    repairRoundsConsumed: 0,
    deadline: "2026-12-01T00:00:00Z",
  },
  amendmentSequence: 0,
  createdAt: "2026-11-01T00:00:00Z",
  access: { origins: [] },
});
const session = Schema.decodeSync(PlanningSessionV1)({
  schemaVersion: 1,
  id: "session",
  environmentId: "environment",
  projectId: "project",
  planId: "plan",
  origin: { kind: "human", id: "human" },
  request: "Plan the work",
  mode: "ordinary",
  allowedRoles: ["author"],
  allowedProviders: [{ providerInstanceId: "provider", model: "model" }],
  limits: { maxPasses: 2, maxRepairRounds: 1, deadline: "2026-12-01T00:00:00Z" },
  consumed: { passes: 0, repairRounds: 0 },
  controlGeneration: 0,
  recordVersion: 0,
  status: "active",
  createdAt: "2026-11-01T00:00:00Z",
  access: { origins: [] },
});
type Input = Parameters<typeof WorkflowRunPolicy.decide>[0];
const facts: Input["facts"] = {
  now: "2026-11-12T00:00:00Z",
  authority: true,
  custody: "none",
  activeWriterCount: 0,
  heldOperations: [],
  capabilitiesCurrent: true,
  compatibilityCurrent: true,
  fingerprintCurrent: true,
  originGrantCurrent: true,
};
const command: Input["command"] = {
  kind: "pause",
  commandId: "command",
  fingerprint: hash,
  expectedVersion: 0,
  expectedGeneration: 0,
  expectedSpecHash: hash,
};
const json = (value: unknown) => JSON.stringify(value);
const layer = Store.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* migration;
  yield* sql`INSERT INTO workflow_plan_identities VALUES ('plan')`;
  yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan', 'revision', ${hash}, ${hash})`;
  yield* sql`INSERT INTO workflow_runs VALUES ('run', 'plan', 'revision', ${hash}, ${hash}, ${hash}, 'active', 0, 0, ${json(run)})`;
});

it.effect("creates runs and planning sessions with their identities", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* migration;
    const store = yield* Store.WorkflowRunStore;
    assert.deepStrictEqual(yield* store.createRun(run), run);
    assert.deepStrictEqual(yield* store.createPlanningSession(session), session);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_plan_identities`, [
      { plan_id: "plan" },
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_revision_identities`, [
      { plan_id: "plan", revision_id: "revision", revision_hash: hash, source_packet_hash: hash },
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, [
      {
        id: run.id,
        plan_id: run.spec.plan.planId,
        revision_id: run.spec.plan.revisionId,
        revision_hash: hash,
        source_packet_hash: hash,
        spec_hash: hash,
        status: run.status,
        record_version: 0,
        control_generation: 0,
        record_json: json(run),
      },
    ]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, [
      {
        id: session.id,
        plan_id: session.planId,
        predecessor_id: null,
        status: session.status,
        record_version: 0,
        control_generation: 0,
        record_json: json(session),
      },
    ]);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["run status", "run", { ...run, status: "paused" }],
  ["run version", "run", { ...run, recordVersion: 1 }],
  ["run generation", "run", { ...run, controlGeneration: 1 }],
  ["run amendment", "run", { ...run, amendmentSequence: 1, priorSpecHash: hash }],
  ["run prior hash", "run", { ...run, priorSpecHash: hash }],
  ["run attempts", "run", { ...run, allowance: { ...run.allowance, attemptsConsumed: 1 } }],
  [
    "run repairs",
    "run",
    { ...run, allowance: { ...run.allowance, attemptsConsumed: 1, repairRoundsConsumed: 1 } },
  ],
  ["session status", "session", { ...session, status: "paused" }],
  ["session version", "session", { ...session, recordVersion: 1 }],
  ["session generation", "session", { ...session, controlGeneration: 1 }],
  ["session passes", "session", { ...session, consumed: { passes: 1, repairRounds: 0 } }],
  ["session repairs", "session", { ...session, consumed: { passes: 1, repairRounds: 1 } }],
  ["invalid run", "run", {}],
  ["invalid session", "session", {}],
] as const)("rejects %s without writes", ([_name, kind, record]) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* migration;
    const store = yield* Store.WorkflowRunStore;
    const error = yield* kind === "run"
      ? Effect.flip(store.createRun(record))
      : Effect.flip(store.createPlanningSession(record));
    assert.strictEqual(error.reason, "integrity");
    assert.strictEqual((yield* sql`SELECT * FROM workflow_plan_identities`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_revision_identities`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_runs`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_planning_sessions`).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("rejects conflicting revision hashes and duplicate run IDs atomically", () =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    for (const record of [
      {
        ...run,
        id: "other",
        spec: { ...run.spec, plan: { ...run.spec.plan, revisionHash: nextHash } },
      },
      {
        ...run,
        id: "other",
        spec: { ...run.spec, plan: { ...run.spec.plan, sourcePacketHash: nextHash } },
      },
      { ...run, spec: { ...run.spec, plan: { ...run.spec.plan, planId: "new-plan" } } },
    ]) {
      assert.strictEqual((yield* Effect.flip(store.createRun(record))).reason, "integrity");
    }
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_plan_identities`, [
      { plan_id: "plan" },
    ]);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_revision_identities`).length, 1);
    assert.deepStrictEqual(yield* sql`SELECT record_json FROM workflow_runs`, [
      { record_json: json(run) },
    ]);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "rejects an unsettled or duplicate session and permits a successor to a stopped session",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA foreign_keys = ON`;
      yield* migration;
      const store = yield* Store.WorkflowRunStore;
      yield* store.createPlanningSession(session);
      assert.strictEqual(
        (yield* Effect.flip(store.createPlanningSession({ ...session, id: "other" }))).reason,
        "integrity",
      );
      assert.strictEqual(
        (yield* Effect.flip(store.createPlanningSession({ ...session, planId: "new-plan" })))
          .reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_plan_identities`, [
        { plan_id: "plan" },
      ]);
      yield* sql`UPDATE workflow_planning_sessions SET status = 'stopped' WHERE id = ${session.id}`;
      const successor = { ...session, id: "other", predecessorSessionId: session.id };
      assert.deepStrictEqual<unknown>(yield* store.createPlanningSession(successor), successor);
      assert.deepStrictEqual(
        yield* sql`SELECT id, predecessor_id, status FROM workflow_planning_sessions ORDER BY id`,
        [
          { id: "other", predecessor_id: "session", status: "active" },
          { id: "session", predecessor_id: null, status: "stopped" },
        ],
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("replays identical commands and rejects changed identities and stale versions", () =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const input = { runId: run.id, command, externalFacts: facts, receiptId: "receipt" };
    assert.deepStrictEqual(yield* store.commitTransition(input), {
      kind: "accepted",
      receiptId: "receipt",
    });
    assert.deepStrictEqual(yield* store.commitTransition(input), {
      kind: "replay",
      receiptId: "receipt",
    });
    for (const changed of [
      { ...input, command: { ...command, kind: "stop" as const } },
      { ...input, runId: "foreign" },
    ]) {
      assert.isTrue(Exit.isFailure(yield* Effect.exit(store.commitTransition(changed))));
    }
    assert.deepStrictEqual(
      yield* store.commitTransition({ ...input, command: { ...command, commandId: "stale" } }),
      { kind: "rejected", reason: "stale" },
    );
    assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, 1);
    assert.deepStrictEqual(
      yield* sql`SELECT record_version, control_generation, status FROM workflow_runs`,
      [{ record_version: 1, control_generation: 1, status: "paused" }],
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("rolls back a failure after the run update", () =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* sql`CREATE TRIGGER reject_receipt BEFORE INSERT ON workflow_command_receipts BEGIN SELECT RAISE(ABORT, 'failure'); END`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          store.commitTransition({
            runId: run.id,
            command,
            externalFacts: facts,
            receiptId: "receipt",
          }),
        ),
      ),
    );
    assert.deepStrictEqual(
      yield* sql`SELECT record_version, control_generation, status FROM workflow_runs`,
      [{ record_version: 0, control_generation: 0, status: "active" }],
    );
    assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("serializes decisions against the same version", () =>
  Effect.gen(function* () {
    yield* setup;
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const results = yield* Effect.forEach(
      ["one", "two"],
      (id) =>
        store.commitTransition({
          runId: run.id,
          command: { ...command, commandId: id },
          externalFacts: facts,
          receiptId: id,
        }),
      { concurrency: "unbounded" },
    );
    assert.strictEqual(results.filter((r) => r.kind === "accepted").length, 1);
    assert.strictEqual(
      results.filter((r) => r.kind === "rejected" && r.reason === "stale").length,
      1,
    );
    assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, 1);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["all", "unknown"],
  ["selected", ["evidence"]],
] as const)("amendment invalidation %s", ([scope, affected]) =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* sql`INSERT INTO workflow_operations VALUES ('op', 'run', NULL, 'check', ${hash}, 'pending', 0, 0, 0, NULL, '{}')`;
    yield* sql`INSERT INTO workflow_role_results VALUES ('result', 'run', 'op', '{}', 0)`;
    yield* sql`INSERT INTO workflow_acceptances VALUES ('run', 'item:item', '{}', 0)`;
    const amendment: Input["command"] = {
      ...command,
      kind: "approveAmendment",
      proposal: {
        proposalHash: "proposal",
        previousSpecHash: hash,
        newSpecRef: { ...run.spec.ref, sha256: nextHash },
        newPlanBinding: run.spec.plan,
        limits: {
          maxAttempts: 2,
          maxRepairRounds: 1,
          maxActiveWriters: 1,
          deadline: run.allowance.deadline,
        },
        affectedEvidence: affected,
      },
      actor: { kind: "human", id: "reviewer" },
      approvedProposalHash: "proposal",
    };
    const approval: Input["facts"] = {
      ...facts,
      implementerId: "implementer",
      amendmentApproval: {
        actor: { kind: "human", id: "reviewer" },
        approvedProposalHash: "proposal",
        runId: run.id,
        recordVersion: 0,
        previousSpecHash: hash,
        newSpecHash: nextHash,
      },
    };
    const result = yield* Effect.exit(
      store.commitTransition({
        runId: run.id,
        command: amendment,
        externalFacts: approval,
        receiptId: "receipt",
      }),
    );
    assert.strictEqual(Exit.isSuccess(result), scope === "all");
    const expected = scope === "all" ? 1 : 0;
    assert.deepStrictEqual(yield* sql`SELECT invalidated FROM workflow_role_results`, [
      { invalidated: expected },
    ]);
    assert.deepStrictEqual(yield* sql`SELECT invalidated FROM workflow_acceptances`, [
      { invalidated: expected },
    ]);
    assert.deepStrictEqual(yield* sql`SELECT record_version FROM workflow_runs`, [
      { record_version: expected },
    ]);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, expected);
  }).pipe(Effect.provide(layer)),
);
