// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import { assert, it } from "@effect/vitest";
import { PlanningSessionV1, WorkflowOperationV1, WorkflowRunV1 } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/081_WorkflowRuns.ts";
import planMigration from "../persistence/Migrations/082_DevelopmentPlans.ts";
import * as Store from "./WorkflowRunStore.ts";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
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
const context = {
  actor: session.origin,
  destination: { environmentId: session.environmentId, projectId: session.projectId },
};
const begin = (store: Store.WorkflowRunStore["Service"], record: unknown) => {
  const id = json(record);
  return store
    .beginPlanning({
      session: record as typeof session,
      now: "2026-11-12T00:00:00Z",
      context,
      command: { commandId: id, receiptId: id },
    })
    .pipe(Effect.map((result) => result.value));
};
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
const parse = (value: string): unknown => JSON.parse(value);
const layer = Store.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const seedPlan = (level: "identity" | "plan" | "complete" = "complete") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO workflow_plan_identities VALUES ('plan')`;
    if (level === "identity") {
      yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan', 'revision', ${hash}, ${hash})`;
      return;
    }
    yield* sql`INSERT INTO development_plans VALUES ('plan', 'environment', 'project', 'Plan', NULL, 0, '2026-11-01T00:00:00Z')`;
    if (level === "plan") return;
    yield* sql`INSERT INTO development_plan_source_packets VALUES ('plan', ${hash}, '{}')`;
    yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan', 'revision', ${hash}, ${hash})`;
    yield* sql`INSERT INTO development_plan_revisions VALUES ('plan', 'revision', ${hash}, ${hash}, '{}', '2026-11-01T00:00:00Z')`;
    yield* sql`UPDATE development_plans SET current_revision_id = 'revision' WHERE plan_id = 'plan'`;
  });
const setup = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* migration;
  yield* planMigration;
  yield* seedPlan();
  yield* sql`INSERT INTO workflow_runs VALUES ('run', 'plan', 'revision', ${hash}, ${hash}, ${hash}, 'active', 0, 0, ${json(run)})`;
});

it.effect("creates runs and planning sessions with existing plan payloads", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* migration;
    yield* planMigration;
    yield* seedPlan();
    const store = yield* Store.WorkflowRunStore;
    assert.deepStrictEqual(yield* store.createRun(run), run);
    assert.deepStrictEqual(yield* begin(store, session), session);
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

it.effect("preserves an admitted run's plan binding after reopening", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "workflow-admission-")),
        ),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "test.sqlite");
      const fileLayer = Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA foreign_keys = ON`;
          yield* migration;
          yield* planMigration;
          yield* seedPlan();
          const store = yield* Store.WorkflowRunStore;
          yield* store.createRun(run);
        }).pipe(Effect.provide(fileLayer)),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.deepStrictEqual(
            yield* sql`SELECT plan_id, revision_id, revision_hash, source_packet_hash FROM workflow_runs`,
            [
              {
                plan_id: "plan",
                revision_id: "revision",
                revision_hash: hash,
                source_packet_hash: hash,
              },
            ],
          );
        }).pipe(Effect.provide(fileLayer)),
      );
    }),
  ),
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
    yield* planMigration;
    const store = yield* Store.WorkflowRunStore;
    const error = yield* kind === "run"
      ? Effect.flip(store.createRun(record))
      : Effect.flip(begin(store, record));
    assert.strictEqual(error.reason, kind === "run" ? "integrity" : "invalid");
    assert.strictEqual((yield* sql`SELECT * FROM workflow_plan_identities`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_revision_identities`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_runs`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_planning_sessions`).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["missing plan", "none", run],
  ["missing payload", "identity", run],
  [
    "revision hash mismatch",
    "complete",
    { ...run, spec: { ...run.spec, plan: { ...run.spec.plan, revisionHash: nextHash } } },
  ],
  [
    "source packet hash mismatch",
    "complete",
    { ...run, spec: { ...run.spec, plan: { ...run.spec.plan, sourcePacketHash: nextHash } } },
  ],
] as const)("rejects run with %s without writes or hints", ([_name, fixture, record]) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* migration;
    yield* planMigration;
    if (fixture !== "none") yield* seedPlan(fixture);
    const identities = yield* sql`SELECT * FROM workflow_plan_identities`;
    const revisions = yield* sql`SELECT * FROM workflow_revision_identities`;
    const store = yield* Store.WorkflowRunStore;
    const subscription = yield* store.subscribeChanges;
    assert.strictEqual((yield* Effect.flip(store.createRun(record))).reason, "integrity");
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_plan_identities`, identities);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_revision_identities`, revisions);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_runs`).length, 0);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("rejects a planning session without a current payload without writes or hints", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* migration;
    yield* planMigration;
    yield* seedPlan("plan");
    const store = yield* Store.WorkflowRunStore;
    const subscription = yield* store.subscribeChanges;
    assert.strictEqual((yield* Effect.flip(begin(store, session))).reason, "integrity");
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_plan_identities`, [
      { plan_id: "plan" },
    ]);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_planning_sessions`).length, 0);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
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
      yield* planMigration;
      yield* seedPlan();
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      assert.strictEqual(
        (yield* Effect.flip(begin(store, { ...session, id: "other" }))).reason,
        "custody",
      );
      assert.strictEqual(
        (yield* Effect.flip(begin(store, { ...session, planId: "new-plan" }))).reason,
        "not-found",
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_plan_identities`, [
        { plan_id: "plan" },
      ]);
      yield* sql`UPDATE workflow_planning_sessions SET status = 'stopped', record_json = ${json({ ...session, status: "stopped" })} WHERE id = ${session.id}`;
      const successor = { ...session, id: "other", predecessorSessionId: session.id };
      assert.deepStrictEqual<unknown>(yield* begin(store, successor), successor);
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

const decodeOperation = Schema.decodeSync(WorkflowOperationV1);
const destination = { environmentId: "environment", projectId: "project" };
const operationFor = (id: string, attemptId: string) => {
  const value = decodeOperation({
    schemaVersion: 1,
    id,
    owner: { kind: "workflow_run", workflowRunId: run.id },
    kind: "stage_launch",
    parameters: {
      kind: "stage_launch",
      stage: "implementation",
      context: {
        specHash: hash,
        policyHash: hash,
        itemId: "item",
        attemptId,
        candidateHash: hash,
        configurationHash: hash,
      },
      provider: { providerInstanceId: "provider", model: "model" },
      nativeThreadId: "thread",
      nativeRunId: id,
    },
    fingerprint: hash,
    origin: run.origin,
    destination,
    controlGeneration: 0,
    status: "pending",
    access: { origins: [] },
  });
  return { ...value, fingerprint: WorkflowCanonicalHash.operation(value) };
};
const admission = (
  operation: WorkflowOperationV1,
  attemptId: string,
  repair = false,
): Extract<Input["command"], { kind: "admitAttempt" }> => ({
  ...command,
  kind: "admitAttempt",
  commandId: attemptId,
  operationId: operation.id,
  operationFingerprint: operation.fingerprint,
  attemptId,
  itemId: "item",
  repair,
});
const admissionFacts = { ...facts, orderedItemIds: ["item"] };
const setupAdmission = Effect.gen(function* () {
  yield* setup;
  const sql = yield* SqlClient.SqlClient;
  const updated = { ...run, access: { origins: [{ ...destination, threadId: "thread" }] } };
  yield* sql`UPDATE workflow_runs SET record_json = ${json(updated)} WHERE id = ${run.id}`;
});

it.effect("admits, replays, blocks a concurrent attempt, and permits a repair after failure", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const first = operationFor("op-1", "attempt-1");
    const input = {
      runId: run.id,
      command: admission(first, "attempt-1"),
      externalFacts: admissionFacts,
      operation: first,
      receiptId: "receipt-1",
    };
    assert.deepStrictEqual(yield* store.commitTransition(input), {
      kind: "accepted",
      receiptId: "receipt-1",
    });
    assert.deepStrictEqual(
      yield* sql`SELECT id, operation_id, item_id, repair FROM workflow_attempts`,
      [{ id: "attempt-1", operation_id: "op-1", item_id: "item", repair: 0 }],
    );
    assert.strictEqual((yield* sql`SELECT id FROM workflow_operations`).length, 1);
    const stored = yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`;
    assert.strictEqual((parse(stored[0]!.record_json) as typeof run).allowance.attemptsConsumed, 1);
    assert.deepStrictEqual(
      yield* store.commitTransition({
        ...input,
        command: { ...input.command, commandId: "replay" },
        receiptId: "other",
      }),
      { kind: "replay", receiptId: "receipt-1" },
    );
    assert.deepStrictEqual(
      yield* store.commitTransition({
        ...input,
        command: {
          ...admission(first, "attempt-1"),
          commandId: "different",
          operationFingerprint: nextHash,
        },
        receiptId: "different",
      }),
      { kind: "rejected", reason: "integrity" },
    );
    const second = operationFor("op-2", "attempt-2");
    const next = {
      runId: run.id,
      command: { ...admission(second, "attempt-2", true), expectedVersion: 1 },
      externalFacts: admissionFacts,
      operation: second,
      receiptId: "receipt-2",
    };
    assert.deepStrictEqual(yield* store.commitTransition(next), {
      kind: "rejected",
      reason: "custody",
    });
    const failed = {
      ...first,
      status: "settled",
      outcome: { kind: "failed", summary: "Failed", evidence: [] },
    };
    yield* sql`UPDATE workflow_operations SET status = 'settled', record_json = ${json(failed)} WHERE id = ${first.id}`;
    assert.deepStrictEqual(yield* store.commitTransition(next), {
      kind: "accepted",
      receiptId: "receipt-2",
    });
    const final = yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`;
    assert.deepStrictEqual((parse(final[0]!.record_json) as typeof run).allowance, {
      ...run.allowance,
      attemptsConsumed: 2,
      repairRoundsConsumed: 1,
    });
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["claimed", "custody"],
  ["unknown", "custody"],
  ["exhausted", "exhausted"],
  ["receipted held", "custody"],
] as const)("resume refuses %s", ([scenario, reason]) =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const paused = {
      ...run,
      status: "paused",
      allowance: { ...run.allowance, attemptsConsumed: scenario === "exhausted" ? 2 : 0 },
    };
    yield* sql`UPDATE workflow_runs SET status = 'paused', record_json = ${json(paused)}`;
    const op = operationFor("op", "attempt");
    if (scenario !== "exhausted") {
      const status = scenario === "claimed" || scenario === "unknown" ? scenario : "pending";
      const held = status === "pending" ? 1 : 0;
      yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, held, generation, record_json) VALUES (${op.id}, ${run.id}, ${op.kind}, ${op.fingerprint}, ${status}, ${held}, 0, ${json(op)})`;
      if (scenario === "receipted held")
        yield* sql`INSERT INTO workflow_command_receipts VALUES ('old', 'run', NULL, ${hash}, 'old-receipt', ${op.id}, '{}')`;
    }
    assert.deepStrictEqual(
      yield* store.commitTransition({
        runId: run.id,
        command: { ...command, kind: "resume" },
        externalFacts: { ...facts, custody: "none" },
        receiptId: "receipt",
      }),
      { kind: "rejected", reason },
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("pause then resume rearms a provably unstarted held operation", () =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const op = operationFor("op", "attempt");
    yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, held, generation, record_json) VALUES (${op.id}, ${run.id}, ${op.kind}, ${op.fingerprint}, 'pending', 0, 0, ${json(op)})`;
    assert.deepStrictEqual(
      yield* store.commitTransition({
        runId: run.id,
        command,
        externalFacts: facts,
        receiptId: "pause",
      }),
      { kind: "accepted", receiptId: "pause" },
    );
    assert.deepStrictEqual(yield* sql`SELECT held, generation FROM workflow_operations`, [
      { held: 1, generation: 0 },
    ]);
    assert.deepStrictEqual(
      yield* store.commitTransition({
        runId: run.id,
        command: {
          ...command,
          kind: "resume",
          commandId: "resume",
          expectedVersion: 1,
          expectedGeneration: 1,
        },
        externalFacts: facts,
        receiptId: "resume",
      }),
      { kind: "accepted", receiptId: "resume" },
    );
    assert.deepStrictEqual(yield* sql`SELECT held, generation FROM workflow_operations`, [
      { held: 0, generation: 2 },
    ]);
  }).pipe(Effect.provide(layer)),
);

it.effect("rolls back allowance and operation if attempt insertion fails", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const operation = operationFor("op", "attempt");
    yield* sql`CREATE TRIGGER reject_attempt BEFORE INSERT ON workflow_attempts BEGIN SELECT RAISE(ABORT, 'failure'); END`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          store.commitTransition({
            runId: run.id,
            command: admission(operation, "attempt"),
            externalFacts: admissionFacts,
            operation,
            receiptId: "receipt",
          }),
        ),
      ),
    );
    assert.strictEqual((yield* sql`SELECT * FROM workflow_operations`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_attempts`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, 0);
    const rows = yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`;
    assert.strictEqual((parse(rows[0]!.record_json) as typeof run).allowance.attemptsConsumed, 0);
  }).pipe(Effect.provide(layer)),
);

const itemBinding = {
  specHash: hash,
  policyHash: hash,
  candidateHash: hash,
  configurationHash: hash,
  itemId: "item",
  attemptId: "attempt",
};
const finalBinding = {
  specHash: hash,
  policyHash: hash,
  candidateHash: hash,
  configurationHash: hash,
};
const ref = (id: string) => ({ kind: "report", id, sha256: hash });
const roleRecord = (
  id: string,
  operationId: string,
  binding: typeof finalBinding | typeof itemBinding,
  result: unknown,
) => ({
  schemaVersion: 1,
  id,
  workflowRunId: "run",
  operationId,
  binding,
  result,
  access: { origins: [] },
});
const reviewResult = {
  kind: "code_review",
  verdict: "pass",
  findings: [],
  output: { ref: ref("output"), byteLength: 1, availability: "available", access: { origins: [] } },
};
const rollupResult = (verdict = "accepted", resultId = "review") => ({
  kind: "rollup",
  verdict,
  resultRefs: [ref(resultId)],
  summary: "Review",
});
const settled = (op: WorkflowOperationV1) => ({
  ...op,
  status: "settled",
  outcome: { kind: "succeeded", summary: "Done", evidence: [] },
});
const reviewOperation = (id: string, final = false, sameProvider = false) => {
  const op = operationFor(id, "attempt");
  return {
    ...settled(op),
    parameters: {
      kind: "stage_launch",
      stage: "code_review",
      context: final ? finalBinding : itemBinding,
      provider: { providerInstanceId: sameProvider ? "provider" : "reviewer", model: "model" },
      nativeThreadId: "thread",
      nativeRunId: id,
    },
  };
};
const insertOperation = (id: string, kind: string, record: unknown, status = "settled") =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const fingerprint = WorkflowCanonicalHash.command(id, {});
    yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, generation, record_json) VALUES (${id}, 'run', ${kind}, ${fingerprint}, ${status}, 0, ${json(record)})`;
  });
const setupEvidence = (sameProvider = false) =>
  Effect.gen(function* () {
    yield* setup;
    const sql = yield* SqlClient.SqlClient;
    yield* insertOperation("impl", "stage_launch", settled(operationFor("impl", "attempt")));
    yield* sql`INSERT INTO workflow_attempts VALUES ('attempt', 'run', 'impl', 'item', 0)`;
    yield* insertOperation(
      "review-op",
      "stage_launch",
      reviewOperation("review-op", false, sameProvider),
    );
  });
const advance = (
  binding: typeof finalBinding | typeof itemBinding,
  finalCombined = false,
  version = 0,
): Input["command"] => ({
  ...command,
  commandId: finalCombined ? "final" : "advance",
  expectedVersion: version,
  kind: "advance",
  binding,
  finalCombined,
});

it.effect.each([
  "happy",
  "binding mismatch",
  "foreign operation",
  "unsettled operation",
  "duplicate id",
] as const)("records role result: %s", (scenario) =>
  Effect.gen(function* () {
    yield* setupEvidence();
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const record = roleRecord(
      "review",
      "review-op",
      scenario === "binding mismatch" ? { ...itemBinding, policyHash: nextHash } : itemBinding,
      reviewResult,
    );
    if (scenario === "unsettled operation")
      yield* sql`UPDATE workflow_operations SET status = 'pending' WHERE id = 'review-op'`;
    if (scenario === "duplicate id")
      yield* sql`INSERT INTO workflow_role_results VALUES ('review', 'run', 'review-op', '{}', 0)`;
    const before = yield* sql`SELECT * FROM workflow_role_results`;
    const input = {
      runId: scenario === "foreign operation" ? "other" : "run",
      operationId: "review-op",
      resultId: "review",
      record: scenario === "foreign operation" ? { ...record, workflowRunId: "other" } : record,
    };
    const exit = yield* Effect.exit(store.recordRoleResult(input));
    assert.strictEqual(Exit.isSuccess(exit), scenario === "happy");
    const after = yield* sql`SELECT * FROM workflow_role_results`;
    assert.strictEqual(after.length, before.length + (scenario === "happy" ? 1 : 0));
    if (scenario !== "happy") assert.deepStrictEqual(after, before);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "accepted",
  "same provider",
  "missing result",
  "rejected",
  "invalidated review",
] as const)("item advance: %s", (scenario) =>
  Effect.gen(function* () {
    yield* setupEvidence(scenario === "same provider");
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* store.recordRoleResult({
      runId: "run",
      operationId: "review-op",
      resultId: "review",
      record: roleRecord("review", "review-op", itemBinding, reviewResult),
    });
    yield* store.recordRoleResult({
      runId: "run",
      operationId: "impl",
      resultId: "rollup",
      record: roleRecord(
        "rollup",
        "impl",
        itemBinding,
        rollupResult(
          scenario === "rejected" ? "rejected" : "accepted",
          scenario === "missing result" ? "missing" : "review",
        ),
      ),
    });
    if (scenario === "invalidated review")
      yield* sql`UPDATE workflow_role_results SET invalidated = 1 WHERE id = 'review'`;
    const result = yield* store.commitTransition({
      runId: "run",
      command: advance(itemBinding),
      externalFacts: admissionFacts,
      receiptId: "receipt",
    });
    assert.deepStrictEqual(
      result,
      scenario === "accepted"
        ? { kind: "accepted", receiptId: "receipt" }
        : { kind: "rejected", reason: "evidence" },
    );
    assert.strictEqual(
      (yield* sql`SELECT * FROM workflow_acceptances WHERE acceptance_key = 'item:item'`).length,
      scenario === "accepted" ? 1 : 0,
    );
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "accepts a final combined review and completes only for matching published and CI heads",
  () =>
    Effect.gen(function* () {
      yield* setupEvidence();
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      assert.deepStrictEqual(
        yield* store.commitTransition({
          runId: "run",
          command: { ...command, kind: "complete", commandId: "premature" },
          externalFacts: {
            ...facts,
            finalAccepted: true,
            publicationSettled: true,
            requiredCiPassed: true,
            publishedHead: "head",
            ciHead: "head",
          },
          receiptId: "premature",
        }),
        { kind: "rejected", reason: "evidence" },
      );
      yield* sql`INSERT INTO workflow_acceptances VALUES ('run', 'item:item', ${json(itemBinding)}, 0)`;
      yield* insertOperation(
        "final-review-op",
        "stage_launch",
        reviewOperation("final-review-op", true),
      );
      yield* store.recordRoleResult({
        runId: "run",
        operationId: "final-review-op",
        resultId: "final-review",
        record: roleRecord("final-review", "final-review-op", finalBinding, reviewResult),
      });
      yield* store.recordRoleResult({
        runId: "run",
        operationId: "impl",
        resultId: "final-rollup",
        record: roleRecord(
          "final-rollup",
          "impl",
          finalBinding,
          rollupResult("accepted", "final-review"),
        ),
      });
      assert.deepStrictEqual(
        yield* store.commitTransition({
          runId: "run",
          command: advance(finalBinding, true),
          externalFacts: admissionFacts,
          receiptId: "final-receipt",
        }),
        { kind: "accepted", receiptId: "final-receipt" },
      );
      assert.strictEqual(
        (yield* sql`SELECT * FROM workflow_acceptances WHERE acceptance_key = 'final'`).length,
        1,
      );
      const completion: Input["command"] = {
        ...command,
        kind: "complete",
        commandId: "complete",
        expectedVersion: 1,
      };
      const complete = () =>
        store.commitTransition({
          runId: "run",
          command: completion,
          externalFacts: {
            ...facts,
            publicationSettled: true,
            requiredCiPassed: true,
            publishedHead: "fake",
            ciHead: "fake",
          },
          receiptId: "completed",
        });
      assert.deepStrictEqual(yield* complete(), { kind: "rejected", reason: "evidence" });
      const op = settled(operationFor("publication", "attempt"));
      yield* insertOperation("publication", "publication", {
        ...op,
        kind: "publication",
        parameters: {
          kind: "publication",
          context: finalBinding,
          repositoryId: "repo",
          baseRef: "main",
          headRef: "branch",
          expectedRemoteHead: "old",
          candidateCommit: "head",
        },
      });
      yield* insertOperation("ci", "ci_observation", {
        ...op,
        id: "ci",
        kind: "ci_observation",
        parameters: {
          kind: "ci_observation",
          context: finalBinding,
          repositoryId: "repo",
          pullRequestId: "pr",
          headCommit: "other",
          requiredChecks: [],
        },
      });
      assert.deepStrictEqual(yield* complete(), { kind: "rejected", reason: "evidence" });
      yield* sql`UPDATE workflow_operations SET record_json = json_set(record_json, '$.parameters.headCommit', 'head') WHERE id = 'ci'`;
      assert.deepStrictEqual(yield* complete(), { kind: "accepted", receiptId: "completed" });
    }).pipe(Effect.provide(layer)),
);

it.effect("publishes only committed transitions", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const subscription = yield* store.subscribeChanges;
      const op = operationFor("notify-op", "notify-attempt");
      const input = {
        runId: run.id,
        command: admission(op, "notify-attempt"),
        externalFacts: admissionFacts,
        operation: op,
        receiptId: "notify-receipt",
      };
      yield* sql`CREATE TRIGGER fail_receipt BEFORE INSERT ON workflow_command_receipts BEGIN SELECT RAISE(ABORT, 'fault'); END`;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(store.commitTransition(input))));
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      assert.strictEqual((yield* sql`SELECT * FROM workflow_operations`).length, 0);
      assert.strictEqual((yield* sql`SELECT * FROM workflow_attempts`).length, 0);
      assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, 0);
      assert.deepStrictEqual(yield* sql`SELECT record_version FROM workflow_runs`, [
        { record_version: 0 },
      ]);
      yield* sql`DROP TRIGGER fail_receipt`;
      yield* store.commitTransition(input);
      yield* PubSub.take(subscription);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      yield* store.commitTransition(input);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }),
  ).pipe(Effect.provide(layer)),
);

const independentRows = (filename: string) => {
  const db = new NodeSqlite.DatabaseSync(filename);
  try {
    db.exec("PRAGMA foreign_keys = ON");
    return {
      run: db.prepare("SELECT record_json FROM workflow_runs WHERE id = 'run'").get(),
      operation: db
        .prepare("SELECT fingerprint, record_json FROM workflow_operations WHERE id = 'lost-op'")
        .get(),
      receipt: db
        .prepare(
          "SELECT fingerprint, operation_id FROM workflow_command_receipts WHERE receipt_id = 'lost-receipt'",
        )
        .get(),
      attempts: db.prepare("SELECT id, operation_id FROM workflow_attempts").all(),
    };
  } finally {
    db.close();
  }
};

it.effect("recovers durable pending work after a post-commit notification fault", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "workflow-store-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "test.sqlite");
      const faultLayer = Store.layerWithPublishFault(Effect.die("notification fault")).pipe(
        Layer.provideMerge(NodeSqliteClient.layer({ filename })),
      );
      yield* Effect.gen(function* () {
        yield* setupAdmission;
        const store = yield* Store.WorkflowRunStore;
        const subscription = yield* store.subscribeChanges;
        const op = operationFor("lost-op", "lost-attempt");
        const input = {
          runId: run.id,
          command: admission(op, "lost-attempt"),
          externalFacts: admissionFacts,
          operation: op,
          receiptId: "lost-receipt",
        };
        assert.isTrue(Exit.isFailure(yield* Effect.exit(store.commitTransition(input))));
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        const rows = yield* Effect.sync(() => independentRows(filename));
        assert.strictEqual(
          (parse(String(rows.run?.record_json)) as typeof run).allowance.attemptsConsumed,
          1,
        );
        assert.strictEqual(rows.operation?.fingerprint, op.fingerprint);
        assert.deepStrictEqual(parse(String(rows.operation?.record_json)), op);
        assert.strictEqual(
          rows.receipt?.fingerprint,
          WorkflowCanonicalHash.command(run.id, input.command),
        );
        assert.strictEqual(rows.receipt?.operation_id, op.id);
        assert.strictEqual(rows.attempts.length, 1);
        assert.deepStrictEqual(yield* store.listPending(), [op]);
      }).pipe(Effect.provide(faultLayer));
    }),
  ),
);

it.effect.each(["eligible", "held", "non-pending", "untrusted", "stale-owner"] as const)(
  "claim: %s",
  (scenario) =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setupAdmission;
        const sql = yield* SqlClient.SqlClient;
        const store = yield* Store.WorkflowRunStore;
        const op = operationFor("claim-op", "claim-attempt");
        yield* store.commitTransition({
          runId: run.id,
          command: admission(op, "claim-attempt"),
          externalFacts: admissionFacts,
          operation: op,
          receiptId: "claim-receipt",
        });
        if (scenario === "held")
          yield* sql`UPDATE workflow_operations SET held = 1 WHERE id = ${op.id}`;
        if (scenario === "non-pending") {
          const cancelled = {
            ...op,
            status: "cancelled",
            outcome: { kind: "cancelled", summary: "cancelled", evidence: [] },
          };
          yield* sql`UPDATE workflow_operations SET status = 'cancelled', record_json = ${json(cancelled)} WHERE id = ${op.id}`;
        }
        const before = yield* sql`SELECT * FROM workflow_operations`;
        const input = {
          operationId: op.id,
          expectedOwnerVersion: scenario === "stale-owner" ? 0 : 1,
          expectedGeneration: 0,
          bootId: "boot",
          workerId: "worker",
          now: "2026-11-12T00:00:00Z",
          ...(scenario === "untrusted" ? {} : { trustedEligibility: true }),
        };
        const result = yield* store.claim(input);
        assert.strictEqual(
          result.kind,
          scenario === "eligible" ? "claimed" : scenario === "stale-owner" ? "stale" : "denied",
        );
        if (scenario === "eligible") {
          assert.strictEqual((yield* store.claim(input)).kind, "stale");
          assert.deepStrictEqual(yield* store.listPending(), []);
        } else assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, before);
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect("claim loses to a committed pause of its owner", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const op = operationFor("claim-op", "claim-attempt");
      yield* store.commitTransition({
        runId: run.id,
        command: admission(op, "claim-attempt"),
        externalFacts: admissionFacts,
        operation: op,
        receiptId: "claim-receipt",
      });
      yield* store.commitTransition({
        runId: run.id,
        command: { ...command, commandId: "pause", expectedVersion: 1, expectedGeneration: 0 },
        externalFacts: facts,
        receiptId: "pause",
      });
      const before = yield* sql`SELECT * FROM workflow_operations`;
      const claim = {
        operationId: op.id,
        bootId: "boot",
        workerId: "worker",
        now: "2026-11-12T00:00:00Z",
        trustedEligibility: true,
      };
      assert.strictEqual(
        (yield* store.claim({ ...claim, expectedOwnerVersion: 1, expectedGeneration: 0 })).kind,
        "stale",
      );
      assert.strictEqual(
        (yield* store.claim({ ...claim, expectedOwnerVersion: 2, expectedGeneration: 1 })).kind,
        "stale",
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, before);
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect(
  "planning controls replay and preserve pending reservations through pause, resume and stop",
  () =>
    Effect.gen(function* () {
      yield* setup;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const created = yield* store.beginPlanning({
        session,
        now: facts.now,
        context,
        command: { commandId: "begin", receiptId: "begin-receipt" },
      });
      const binding = {
        planId: session.planId,
        revisionId: "revision",
        revisionSchemaVersion: 1 as const,
        revisionHash: hash,
        sourcePacketHash: hash,
      };
      const pass = {
        schemaVersion: 1,
        id: "pass",
        planningSessionId: session.id,
        role: "author",
        provider: session.allowedProviders[0],
        binding,
        operationId: "planning-op",
        inputManifest: {
          sources: [{ kind: "source", id: "source", sha256: hash }],
          references: [],
          additionalContext: [],
          access: { origins: [] },
        },
        status: "pending",
        access: { origins: [] },
      };
      const operation = decodeOperation({
        schemaVersion: 1,
        id: "planning-op",
        owner: { kind: "planning_session", planningSessionId: session.id },
        kind: "planning_pass",
        parameters: {
          kind: "planning_pass",
          passId: "pass",
          binding,
          role: "author",
          provider: session.allowedProviders[0]!,
          limits: session.limits,
          inputManifestHash: hash,
        },
        fingerprint: hash,
        origin: session.origin,
        destination: context.destination,
        controlGeneration: 0,
        status: "pending",
        access: { origins: [] },
      });
      yield* sql`INSERT INTO workflow_operations (id, planning_session_id, kind, fingerprint, status, generation, record_json) VALUES (${operation.id}, ${session.id}, 'planning_pass', ${hash}, 'pending', 0, ${json(operation)})`;
      yield* sql`INSERT INTO workflow_planning_passes VALUES ('pass', ${session.id}, ${session.planId}, 'revision', ${hash}, ${hash}, ${operation.id}, NULL, NULL, ${json(pass)})`;
      const pause = {
        sessionId: session.id,
        action: "pause" as const,
        expectedVersion: 0,
        now: facts.now,
        context,
        command: { commandId: "pause-planning", receiptId: "pause-receipt" },
      };
      const paused = yield* store.controlPlanning(pause);
      assert.strictEqual(paused.value.status, "paused");
      assert.deepStrictEqual(yield* sql`SELECT held, generation FROM workflow_operations`, [
        { held: 1, generation: 0 },
      ]);
      const resumed = yield* store.controlPlanning({
        ...pause,
        action: "resume",
        expectedVersion: 1,
        command: { commandId: "resume-planning", receiptId: "resume-receipt" },
      });
      assert.strictEqual(resumed.value.status, "active");
      assert.deepStrictEqual(resumed.value.consumed, session.consumed);
      assert.deepStrictEqual(yield* sql`SELECT held, generation FROM workflow_operations`, [
        { held: 0, generation: 2 },
      ]);
      assert.deepStrictEqual(
        yield* store.controlPlanning({ ...pause, now: "2027-01-01T00:00:00Z" }),
        { ...paused, kind: "replay" },
      );
      assert.deepStrictEqual(
        yield* store.beginPlanning({
          session,
          now: "2027-01-01T00:00:00Z",
          context,
          command: { commandId: "begin", receiptId: "other" },
        }),
        { ...created, kind: "replay" },
      );
      assert.strictEqual(
        (yield* Effect.flip(store.controlPlanning({ ...pause, expectedVersion: 1 }))).reason,
        "conflict",
      );
      const stopped = yield* store.controlPlanning({
        ...pause,
        action: "stop",
        expectedVersion: 2,
        command: { commandId: "stop-planning", receiptId: "stop-receipt" },
      });
      assert.strictEqual(stopped.value.status, "stopped");
      assert.deepStrictEqual(
        yield* sql`SELECT status, held, cancel_requested FROM workflow_operations`,
        [{ status: "cancelled", held: 0, cancel_requested: 1 }],
      );
      const records = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM workflow_planning_passes`;
      assert.strictEqual(
        (parse(records[0]!.record_json) as { status: string }).status,
        "cancelled",
      );
      assert.strictEqual(
        (yield* Effect.flip(
          store.controlPlanning({
            ...pause,
            action: "resume",
            expectedVersion: 3,
            command: { commandId: "bad-resume", receiptId: "bad-resume" },
          }),
        )).reason,
        "ineligible",
      );
      assert.strictEqual((yield* store.getPlanning({ sessionId: session.id })).status, "stopped");
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["stale", "paused", 0, "2026-11-12T00:00:00Z", 1, "conflict"],
  ["deadline", "paused", 0, "2026-12-01T00:00:00Z", 0, "deadline"],
  ["limit", "paused", 2, "2026-11-12T00:00:00Z", 0, "limit"],
  ["pausing", "pausing", 0, "2026-11-12T00:00:00Z", 0, "ineligible"],
] as const)(
  "planning resume rejects %s without writes",
  ([_name, status, passes, now, version, reason]) =>
    Effect.gen(function* () {
      yield* setup;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      const current = { ...session, status, consumed: { ...session.consumed, passes } };
      yield* sql`UPDATE workflow_planning_sessions SET status = ${status}, record_json = ${json(current)} WHERE id = ${session.id}`;
      const before = yield* sql`SELECT * FROM workflow_planning_sessions`;
      const error = yield* Effect.flip(
        store.controlPlanning({
          sessionId: session.id,
          action: "resume",
          expectedVersion: version,
          now,
          context,
          command: { commandId: "resume", receiptId: "resume" },
        }),
      );
      assert.strictEqual(error.reason, reason);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before);
    }).pipe(Effect.provide(layer)),
);

it.effect("planning receipts survive database reopen and corruption is rejected", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "planning-store-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "test.sqlite");
      const fileLayer = Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
      const input = {
        session,
        now: facts.now,
        context,
        command: { commandId: "begin", receiptId: "receipt" },
      };
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA foreign_keys = ON`;
          yield* migration;
          yield* planMigration;
          yield* seedPlan();
          const store = yield* Store.WorkflowRunStore;
          yield* store.beginPlanning(input);
          yield* store.controlPlanning({
            sessionId: session.id,
            action: "pause",
            expectedVersion: 0,
            now: facts.now,
            context,
            command: { commandId: "pause", receiptId: "pause" },
          });
        }).pipe(Effect.provide(fileLayer)),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const store = yield* Store.WorkflowRunStore;
          assert.deepStrictEqual(
            yield* store.beginPlanning({ ...input, now: "2027-01-01T00:00:00Z" }),
            { kind: "replay", receiptId: "receipt", value: session },
          );
          yield* sql`UPDATE workflow_planning_sessions SET control_generation = 9 WHERE id = ${session.id}`;
          assert.strictEqual(
            (yield* Effect.flip(store.getPlanning({ sessionId: session.id }))).reason,
            "integrity",
          );
        }).pipe(Effect.provide(fileLayer)),
      );
    }),
  ),
);
