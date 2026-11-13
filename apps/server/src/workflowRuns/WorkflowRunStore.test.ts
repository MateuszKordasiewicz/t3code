// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import { assert, it } from "@effect/vitest";
import {
  CommandId,
  RunId,
  RunAttemptId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  ThreadId,
  PlanningPassV1,
  PlanningSessionV1,
  WorkflowOperationV1,
  WorkflowRunV1,
  upgradeDevelopmentPlanDocumentV1,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Digest from "effect/crypto/Digest";
import * as Ref from "effect/Ref";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import migration from "../persistence/Migrations/081_WorkflowRuns.ts";
import planMigration from "../persistence/Migrations/082_DevelopmentPlans.ts";
import intentMigration from "../persistence/Migrations/084_WorkflowNativeLaunchIntents.ts";
import { runMigrations } from "../persistence/Migrations.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as EffectWorker from "../orchestration-v2/EffectWorker.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { DevelopmentPlanHash } from "../developmentPlans/DevelopmentPlanHash.ts";
import * as Store from "./WorkflowRunStore.ts";
import * as Recovery from "./WorkflowRunRecovery.ts";
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
  yield* runMigrations();
  yield* seedPlan();
  yield* sql`INSERT INTO workflow_runs VALUES ('run', 'plan', 'revision', ${hash}, ${hash}, ${hash}, 'active', 0, 0, ${json(run)})`;
});

it.effect("creates runs and planning sessions with existing plan payloads", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`PRAGMA foreign_keys = ON`;
    yield* migration;
    yield* planMigration;
    yield* intentMigration;
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
          yield* intentMigration;
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
    yield* intentMigration;
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
    yield* intentMigration;
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
    yield* intentMigration;
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
      yield* intentMigration;
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
const decodeWorkflowRunId = Schema.decodeSync(WorkflowRunV1.fields.id);
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

const reservation = (operation: WorkflowOperationV1, attemptId: string) => {
  if (operation.parameters.kind !== "stage_launch") throw new Error("expected stage launch");
  const {
    fingerprint: _fingerprint,
    controlGeneration: _generation,
    status: _status,
    ...rest
  } = operation;
  const { nativeRunId: _nativeRunId, ...parameters } = operation.parameters;
  const {
    fingerprint: _commandHash,
    operationFingerprint: _operationHash,
    ...command
  } = admission(operation, attemptId);
  return {
    intentId: `intent-${attemptId}`,
    runId: run.id,
    operationId: operation.id,
    attemptId,
    itemId: "item",
    repair: false,
    environmentId: destination.environmentId,
    projectId: destination.projectId,
    launchCommandId: `launch-${attemptId}`,
    threadId: parameters.nativeThreadId,
    messageId: `message-${attemptId}`,
    receiptId: `receipt-${attemptId}`,
    generation: 0,
    draft: {
      version: 1 as const,
      kind: "stage_launch" as const,
      operation: { ...rest, parameters },
      admission: { command },
    },
    externalFacts: admissionFacts,
  };
};

it.effect.each(["dispatching", "unknown"] as const)(
  "workflow pause retains native %s custody",
  (state) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const input = reservation(operationFor("native-op", "native-attempt"), "native-attempt");
      yield* store.reserveWorkflowNativeIntent(input);
      yield* sql`UPDATE workflow_native_launch_intents SET state = ${state}, boot_id = 'boot', worker_id = 'worker' WHERE intent_id = ${input.intentId}`;
      const paused = yield* store.commitTransition({
        runId: run.id,
        command: { ...command, commandId: "pause-native", expectedVersion: 1 },
        externalFacts: facts,
        receiptId: "pause-native",
      });
      assert.strictEqual(paused.kind, "accepted");
      const rows = yield* sql<{ status: string }>`SELECT status FROM workflow_runs`;
      assert.strictEqual(rows[0]!.status, state === "unknown" ? "needs_attention" : "pausing");
      assert.deepStrictEqual(yield* sql`SELECT state, held FROM workflow_native_launch_intents`, [
        { state, held: 0 },
      ]);
      const stopped = yield* store.commitTransition({
        runId: run.id,
        command: {
          ...command,
          kind: "stop",
          commandId: "stop-native",
          expectedVersion: 2,
          expectedGeneration: 1,
        },
        externalFacts: facts,
        receiptId: "stop-native",
      });
      assert.strictEqual(stopped.kind, "accepted");
      assert.deepStrictEqual(yield* sql`SELECT status FROM workflow_runs`, [
        { status: "stopping" },
      ]);
      const stoppingRow = (yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM workflow_runs`)[0]!;
      const stoppingRun = parse(stoppingRow.record_json) as typeof run;
      yield* sql`UPDATE workflow_runs SET status = 'paused', record_json = ${json({ ...stoppingRun, status: "paused" })}`;
      assert.deepStrictEqual(
        yield* store.commitTransition({
          runId: run.id,
          command: {
            ...command,
            kind: "resume",
            commandId: "resume-custody",
            expectedVersion: 3,
            expectedGeneration: 2,
          },
          externalFacts: admissionFacts,
          receiptId: "resume-custody",
        }),
        { kind: "rejected", reason: "custody" },
      );
      yield* sql`UPDATE workflow_runs SET status = 'stopping', record_json = ${stoppingRow.record_json}`;
      assert.deepStrictEqual(
        yield* store.commitTransition({
          runId: run.id,
          command: {
            ...command,
            kind: "custodySettled",
            commandId: "settle-native",
            expectedVersion: 3,
            expectedGeneration: 2,
          },
          externalFacts: { ...facts, now: "2099-01-01T00:00:00Z" },
          receiptId: "settle-native",
        }),
        { kind: "rejected", reason: "custody" },
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["reserved", "dispatching", "unknown"] as const)(
  "workflow %s intent blocks advancement",
  (state) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const input = reservation(operationFor("advance-op", "advance-attempt"), "advance-attempt");
      yield* store.reserveWorkflowNativeIntent(input);
      if (state !== "reserved")
        yield* sql`UPDATE workflow_native_launch_intents SET state = ${state}, boot_id = 'boot', worker_id = 'worker'`;
      const before = yield* sql`SELECT * FROM workflow_native_launch_intents`;
      assert.deepStrictEqual(
        yield* store.commitTransition({
          runId: run.id,
          command: advance(itemBinding, false, 1),
          externalFacts: admissionFacts,
          receiptId: "advance-blocked",
        }),
        { kind: "rejected", reason: "custody" },
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, before);
    }).pipe(Effect.provide(layer)),
);

it.effect("workflow controls hold and rearm a charged native reservation", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const input = reservation(operationFor("held-op", "held-attempt"), "held-attempt");
    const reserved = yield* store.reserveWorkflowNativeIntent(input);
    const before = yield* sql`SELECT * FROM workflow_native_launch_intents`;
    const receipts = yield* sql`SELECT * FROM workflow_native_intent_receipts`;
    assert.strictEqual(
      (yield* store.commitTransition({
        runId: run.id,
        command: { ...command, commandId: "hold", expectedVersion: 1 },
        externalFacts: admissionFacts,
        receiptId: "hold",
      })).kind,
      "accepted",
    );
    assert.deepStrictEqual(yield* sql`SELECT state, held FROM workflow_native_launch_intents`, [
      { state: "reserved", held: 1 },
    ]);
    assert.strictEqual(
      (yield* store.commitTransition({
        runId: run.id,
        command: {
          ...command,
          kind: "resume",
          commandId: "rearm",
          expectedVersion: 2,
          expectedGeneration: 1,
        },
        externalFacts: admissionFacts,
        receiptId: "rearm",
      })).kind,
      "accepted",
    );
    const after = yield* sql`SELECT * FROM workflow_native_launch_intents`;
    assert.deepStrictEqual(after, [{ ...before[0], generation: 2 }]);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, receipts);
    assert.deepStrictEqual(yield* store.reserveWorkflowNativeIntent(input), {
      ...reserved,
      kind: "replay",
    });
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["resume", "stop"] as const)(
  "workflow %s retains the original charge and receipt",
  (action) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const row = (yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`)[0]!;
      const original = parse(row.record_json) as typeof run;
      yield* sql`UPDATE workflow_runs SET record_json = ${json({ ...original, allowance: { ...original.allowance, maxAttempts: 1 } })}`;
      const input = reservation(operationFor("last-op", "last-attempt"), "last-attempt");
      const reserved = yield* store.reserveWorkflowNativeIntent(input);
      const intent = (yield* sql`SELECT * FROM workflow_native_launch_intents`)[0]!;
      const receipt = yield* sql`SELECT * FROM workflow_native_intent_receipts`;
      const pause = {
        runId: run.id,
        command: { ...command, commandId: "last-pause", expectedVersion: 1 },
        externalFacts: admissionFacts,
        receiptId: "last-pause",
      };
      assert.strictEqual((yield* store.commitTransition(pause)).kind, "accepted");
      const next = {
        ...pause,
        command: {
          ...command,
          kind: action,
          commandId: "last-control",
          expectedVersion: 2,
          expectedGeneration: 1,
        },
        receiptId: "last-control",
      };
      assert.strictEqual((yield* store.commitTransition(next)).kind, "accepted");
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, [
        {
          ...intent,
          state: action === "stop" ? "cancelled" : "reserved",
          generation: action === "resume" ? 2 : 0,
        },
      ]);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, receipt);
      assert.deepStrictEqual(yield* store.reserveWorkflowNativeIntent(input), {
        ...reserved,
        kind: "replay",
      });
      const owner = parse(
        (yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`)[0]!
          .record_json,
      ) as typeof run;
      assert.strictEqual(owner.allowance.attemptsConsumed, 1);
      assert.strictEqual(owner.allowance.deadline, original.allowance.deadline);
      assert.deepStrictEqual(yield* store.commitTransition(next), {
        kind: "replay",
        receiptId: "last-control",
      });
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["fingerprint", "receipt", "future", "cas", "receipt-cas"] as const)(
  "workflow control rejects %s and rolls back",
  (scenario) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const input = reservation(operationFor("bad-op", "bad-attempt"), "bad-attempt");
      yield* store.reserveWorkflowNativeIntent(input);
      if (scenario === "fingerprint")
        yield* sql`UPDATE workflow_native_launch_intents SET fingerprint = ${nextHash}`;
      if (scenario === "receipt")
        yield* sql`UPDATE workflow_native_intent_receipts SET result_json = '{}'`;
      if (scenario === "future")
        yield* sql`UPDATE workflow_native_launch_intents SET generation = 5`;
      if (scenario === "cas")
        yield* sql`CREATE TRIGGER reject_intent_hold BEFORE UPDATE ON workflow_native_launch_intents BEGIN SELECT RAISE(IGNORE); END`;
      if (scenario === "receipt-cas")
        yield* sql`CREATE TRIGGER mutate_intent_receipt AFTER UPDATE ON workflow_native_launch_intents BEGIN UPDATE workflow_native_intent_receipts SET result_json = '{}'; END`;
      const beforeRun = yield* sql`SELECT * FROM workflow_runs`;
      const beforeIntent = yield* sql`SELECT * FROM workflow_native_launch_intents`;
      const beforeReceipt = yield* sql`SELECT * FROM workflow_native_intent_receipts`;
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            store.commitTransition({
              runId: run.id,
              command: { ...command, commandId: "bad-pause", expectedVersion: 1 },
              externalFacts: admissionFacts,
              receiptId: "bad-pause",
            }),
          ),
        ),
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, beforeRun);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_launch_intents`,
        beforeIntent,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        beforeReceipt,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_command_receipts`, []);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["owner", "receipt"] as const)(
  "workflow control %s failure rolls back and replay does not notify",
  (fault) =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setupAdmission;
        const sql = yield* SqlClient.SqlClient;
        const store = yield* Store.WorkflowRunStore;
        yield* store.reserveWorkflowNativeIntent(
          reservation(operationFor("fault-op", "fault-attempt"), "fault-attempt"),
        );
        const subscription = yield* store.subscribeChanges;
        if (fault === "owner")
          yield* sql`CREATE TRIGGER fail_control BEFORE UPDATE ON workflow_runs BEGIN SELECT RAISE(IGNORE); END`;
        else
          yield* sql`CREATE TRIGGER fail_control BEFORE INSERT ON workflow_command_receipts BEGIN SELECT RAISE(ABORT, 'fault'); END`;
        const before = {
          owner: yield* sql`SELECT * FROM workflow_runs`,
          intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
          receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
        };
        const control = {
          runId: run.id,
          command: { ...command, commandId: "fault-control", expectedVersion: 1 },
          externalFacts: admissionFacts,
          receiptId: "fault-control",
        };
        assert.isTrue(Exit.isFailure(yield* Effect.exit(store.commitTransition(control))));
        assert.deepStrictEqual(
          {
            owner: yield* sql`SELECT * FROM workflow_runs`,
            intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
            receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
          },
          before,
        );
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        yield* sql`DROP TRIGGER fail_control`;
        yield* store.commitTransition(control);
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 1);
        assert.strictEqual((yield* store.commitTransition(control)).kind, "replay");
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect.each(["origin", "flags", "ordering", "older", "writer"] as const)(
  "workflow rearm checks %s",
  (scenario) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* store.reserveWorkflowNativeIntent(
        reservation(operationFor("elig-op", "elig-attempt"), "elig-attempt"),
      );
      yield* store.commitTransition({
        runId: run.id,
        command: { ...command, commandId: "elig-pause", expectedVersion: 1 },
        externalFacts: admissionFacts,
        receiptId: "elig-pause",
      });
      const owner = parse(
        (yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`)[0]!
          .record_json,
      ) as typeof run;
      if (scenario === "origin")
        yield* sql`UPDATE workflow_runs SET record_json = ${json({ ...owner, access: { origins: [] } })}`;
      if (scenario === "older")
        yield* sql`UPDATE workflow_runs SET control_generation = 3, record_version = 4, record_json = ${json({ ...owner, controlGeneration: 3, recordVersion: 4 })}`;
      if (scenario === "writer") {
        const writer = operationFor("other-writer", "other-attempt");
        yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, held, generation, record_json) VALUES (${writer.id}, ${run.id}, ${writer.kind}, ${writer.fingerprint}, 'pending', 0, 0, ${json(writer)})`;
      }
      const before = {
        owner: yield* sql`SELECT * FROM workflow_runs`,
        intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
        receipt: yield* sql`SELECT * FROM workflow_command_receipts`,
      };
      const result = yield* store.commitTransition({
        runId: run.id,
        command: {
          ...command,
          kind: "resume",
          commandId: "elig-resume",
          expectedVersion: scenario === "older" ? 4 : 2,
          expectedGeneration: scenario === "older" ? 3 : 1,
        },
        externalFacts: {
          ...admissionFacts,
          ...(scenario === "flags" ? { originGrantCurrent: false } : {}),
          ...(scenario === "ordering" ? { orderedItemIds: ["other"] } : {}),
        },
        receiptId: "elig-resume",
      });
      if (scenario === "older") {
        assert.strictEqual(result.kind, "accepted");
        assert.deepStrictEqual(
          yield* sql`SELECT held, generation FROM workflow_native_launch_intents`,
          [{ held: 0, generation: 4 }],
        );
      } else {
        assert.deepStrictEqual(result, {
          kind: "rejected",
          reason:
            scenario === "ordering" ? "ordering" : scenario === "writer" ? "custody" : "freshness",
        });
        assert.deepStrictEqual(
          {
            owner: yield* sql`SELECT * FROM workflow_runs`,
            intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
            receipt: yield* sql`SELECT * FROM workflow_command_receipts`,
          },
          before,
        );
      }
    }).pipe(Effect.provide(layer)),
);

it.effect("cancelled workflow reservation and original receipt survive reopening", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "workflow-retained-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "test.sqlite");
      const fileLayer = Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
      const saved = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* setupAdmission;
          const store = yield* Store.WorkflowRunStore;
          const input = reservation(operationFor("reopen-op", "reopen-attempt"), "reopen-attempt");
          const reserved = yield* store.reserveWorkflowNativeIntent(input);
          yield* store.commitTransition({
            runId: run.id,
            command: { ...command, kind: "stop", commandId: "reopen-stop", expectedVersion: 1 },
            externalFacts: admissionFacts,
            receiptId: "reopen-stop",
          });
          return { input, reserved };
        }).pipe(Effect.provide(fileLayer)),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* Store.WorkflowRunStore;
          const sql = yield* SqlClient.SqlClient;
          assert.deepStrictEqual(
            yield* store.reserveWorkflowNativeIntent({
              ...saved.input,
              externalFacts: { ...admissionFacts, now: "2099-01-01T00:00:00Z" },
            }),
            { ...saved.reserved, kind: "replay" },
          );
          assert.deepStrictEqual(
            yield* sql`SELECT state, held FROM workflow_native_launch_intents`,
            [{ state: "cancelled", held: 0 }],
          );
          const owner = parse(
            (yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`)[0]!
              .record_json,
          ) as typeof run;
          assert.strictEqual(owner.allowance.attemptsConsumed, 1);
          assert.strictEqual(owner.allowance.deadline, run.allowance.deadline);
        }).pipe(Effect.provide(fileLayer)),
      );
    }),
  ),
);

it.effect("reserves once, replays after expiry and prevents legacy admission", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const operation = operationFor("reservation-op", "reservation-attempt");
    const input = reservation(operation, "reservation-attempt");
    const first = yield* store.reserveWorkflowNativeIntent(input);
    assert.strictEqual(first.kind, "reserved");
    if (first.kind !== "reserved") return;
    assert.strictEqual(
      first.fingerprint,
      WorkflowCanonicalHash.tagged("t3.workflow-native-launch-intent.v2", {
        intentId: input.intentId,
        workflowRunId: input.runId,
        planningSessionId: null,
        operationId: input.operationId,
        attemptId: input.attemptId,
        passId: null,
        itemId: input.itemId,
        chargeAttempt: 1,
        repair: input.repair,
        role: "implementation",
        environmentId: input.environmentId,
        projectId: input.projectId,
        launchCommandId: input.launchCommandId,
        threadId: input.threadId,
        messageId: input.messageId,
        draft: input.draft,
        receiptId: input.receiptId,
      }),
    );
    for (const changed of [
      { ...input, receiptId: "changed" },
      { ...input, intentId: "changed" },
      { ...input, messageId: "changed" },
      {
        ...input,
        draft: {
          ...input.draft,
          admission: { command: { ...input.draft.admission.command, expectedVersion: 1 } },
        },
      },
    ])
      assert.strictEqual(
        (yield* Effect.flip(store.reserveWorkflowNativeIntent(changed))).reason,
        "integrity",
      );
    const rowsBefore = yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`;
    const changedRun = {
      ...(parse(rowsBefore[0]!.record_json) as typeof run),
      status: "paused",
      recordVersion: 2,
      controlGeneration: 1,
    };
    yield* sql`UPDATE workflow_runs SET status = 'paused', record_version = 2, control_generation = 1, record_json = ${json(changedRun)} WHERE id = ${run.id}`;
    const replay = yield* store.reserveWorkflowNativeIntent({
      ...input,
      externalFacts: { ...admissionFacts, now: "2030-01-01T00:00:00Z" },
    });
    assert.deepStrictEqual(replay, { ...first, kind: "replay" });
    const legacy = yield* store.commitTransition({
      runId: run.id,
      command: admission(operation, "reservation-attempt"),
      operation,
      receiptId: "legacy",
      externalFacts: admissionFacts,
    });
    assert.deepStrictEqual(legacy, { kind: "rejected", reason: "custody" });
    const rows = yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`;
    assert.strictEqual((parse(rows[0]!.record_json) as typeof run).allowance.attemptsConsumed, 1);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_operations`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_attempts`).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("finalized workflow intent preserves exact V1 receipt replay", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const operation = operationFor("finalized-op", "finalized-attempt");
    const input = {
      runId: run.id,
      command: admission(operation, "finalized-attempt"),
      operation,
      receiptId: "finalized-receipt",
      externalFacts: admissionFacts,
    };
    assert.strictEqual((yield* store.commitTransition(input)).kind, "accepted");
    yield* sql`INSERT INTO workflow_native_launch_intents (intent_id, workflow_run_id, operation_id, attempt_id, item_id, charge_attempt, repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, generation, state, native_run_id, finalized_operation_id) VALUES ('finalized-intent', ${run.id}, ${operation.id}, 'finalized-attempt', 'item', 1, 0, 'implementation', ${destination.environmentId}, ${destination.projectId}, 'finalized-launch', 'thread', 'finalized-message', ${hash}, '{}', 0, 'finalized', 'finalized-op', ${operation.id})`;
    const before = yield* sql`SELECT * FROM workflow_runs`;
    assert.deepStrictEqual(yield* store.commitTransition(input), {
      kind: "replay",
      receiptId: input.receiptId,
    });
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before);
  }).pipe(Effect.provide(layer)),
);

it.effect("a cancelled native attempt identity cannot be charged by legacy admission", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const operation = operationFor("cancelled-op", "cancelled-attempt");
    const input = reservation(operation, "cancelled-attempt");
    yield* store.reserveWorkflowNativeIntent(input);
    yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled' WHERE intent_id = ${input.intentId}`;
    const before = yield* sql`SELECT record_json FROM workflow_runs`;
    const result = yield* Effect.result(
      store.commitTransition({
        runId: run.id,
        command: {
          ...admission(operation, "cancelled-attempt"),
          commandId: "legacy-after-cancel",
          expectedVersion: 1,
        },
        operation,
        receiptId: "legacy-after-cancel",
        externalFacts: admissionFacts,
      }),
    );
    assert.isFalse(result._tag === "Success" && result.success.kind === "accepted");
    assert.deepStrictEqual(yield* sql`SELECT record_json FROM workflow_runs`, before);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "unknown field",
  "native run",
  "wrong context",
  "bad time",
  "deadline",
  "stale",
  "authority",
  "freshness",
  "ordering",
] as const)("reservation rejects %s without writes", (scenario) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const original = reservation(operationFor("op", "attempt"), "attempt");
    const draft =
      scenario === "unknown field"
        ? { ...original.draft, extra: true }
        : scenario === "native run"
          ? {
              ...original.draft,
              operation: {
                ...original.draft.operation,
                parameters: { ...original.draft.operation.parameters, nativeRunId: "fake" },
              },
            }
          : scenario === "wrong context"
            ? {
                ...original.draft,
                operation: {
                  ...original.draft.operation,
                  parameters: {
                    ...original.draft.operation.parameters,
                    context: { ...original.draft.operation.parameters.context, itemId: "wrong" },
                  },
                },
              }
            : original.draft;
    const input = {
      ...original,
      draft,
      externalFacts: {
        ...original.externalFacts,
        ...(scenario === "deadline" ? { now: run.allowance.deadline } : {}),
        ...(scenario === "bad time" ? { now: "invalid" } : {}),
        ...(scenario === "authority" ? { authority: false } : {}),
        ...(scenario === "freshness" ? { originGrantCurrent: false } : {}),
        ...(scenario === "ordering" ? { orderedItemIds: ["other"] } : {}),
      },
      ...(scenario === "stale" ? { generation: 1 } : {}),
    };
    const before = yield* sql`SELECT * FROM workflow_runs`;
    const result = yield* Effect.result(
      store.reserveWorkflowNativeIntent(input as typeof original),
    );
    if (
      scenario === "unknown field" ||
      scenario === "native run" ||
      scenario === "wrong context" ||
      scenario === "bad time"
    ) {
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "integrity");
    } else {
      assert.strictEqual(result._tag, "Success");
      if (result._tag === "Success")
        assert.deepStrictEqual(result.success, {
          kind: "rejected",
          reason: scenario === "authority" ? "unauthorized" : scenario,
        });
    }
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_native_launch_intents`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_native_intent_receipts`).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("rolls back reservation when receipt insertion fails", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const before = yield* sql`SELECT * FROM workflow_runs`;
    yield* sql`CREATE TRIGGER reject_intent_receipt BEFORE INSERT ON workflow_native_intent_receipts BEGIN SELECT RAISE(ABORT, 'failure'); END`;
    assert.isTrue(
      Exit.isFailure(
        yield* Effect.exit(
          store.reserveWorkflowNativeIntent(reservation(operationFor("op", "attempt"), "attempt")),
        ),
      ),
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_native_launch_intents`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_native_intent_receipts`).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["intent", "legacy"] as const)(
  "rejects a %s attempt reused by another run on a distinct thread",
  (source) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const first = reservation(operationFor("first-op", "attempt"), "attempt");
      if (source === "intent") {
        assert.strictEqual((yield* store.reserveWorkflowNativeIntent(first)).kind, "reserved");
      } else {
        assert.strictEqual(
          (yield* store.commitTransition({
            runId: run.id,
            command: admission(operationFor("first-op", "attempt"), "attempt"),
            operation: operationFor("first-op", "attempt"),
            externalFacts: admissionFacts,
            receiptId: "legacy-receipt",
          })).kind,
          "accepted",
        );
      }
      const secondRun = {
        ...run,
        id: "other-run",
        access: { origins: [{ ...destination, threadId: "other-thread" }] },
      };
      yield* sql`INSERT INTO workflow_runs VALUES ('other-run', 'plan', 'revision', ${hash}, ${hash}, ${hash}, 'active', 0, 0, ${json(secondRun)})`;
      const op = operationFor("other-op", "attempt");
      if (op.parameters.kind !== "stage_launch") return;
      const secondOp = decodeOperation({
        ...op,
        owner: { kind: "workflow_run", workflowRunId: "other-run" },
        parameters: { ...op.parameters, nativeThreadId: "other-thread" },
      });
      const second = {
        ...reservation(secondOp, "attempt"),
        runId: "other-run",
        intentId: "other-intent",
        receiptId: "other-receipt",
        launchCommandId: "other-launch",
        threadId: "other-thread",
        messageId: "other-message",
        draft: {
          ...reservation(secondOp, "attempt").draft,
          admission: {
            command: {
              ...reservation(secondOp, "attempt").draft.admission.command,
              commandId: "other-command",
            },
          },
        },
      };
      const before = yield* sql`SELECT * FROM workflow_runs ORDER BY id`;
      assert.strictEqual(
        (yield* Effect.flip(store.reserveWorkflowNativeIntent(second))).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs ORDER BY id`, before);
      assert.strictEqual(
        (yield* sql`SELECT * FROM workflow_native_launch_intents`).length,
        source === "intent" ? 1 : 0,
      );
      assert.strictEqual(
        (yield* sql`SELECT * FROM workflow_native_intent_receipts`).length,
        source === "intent" ? 1 : 0,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["reservation", "settled"],
  ["legacy", "settled"],
  ["reservation", "pending"],
  ["legacy", "pending"],
  ["reservation", "unknown"],
  ["legacy", "unknown"],
] as const)("checks next-item state and global custody through %s", ([path, priorStatus]) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const previous = operationFor("previous-operation", "previous-attempt");
    assert.strictEqual(
      (yield* store.commitTransition({
        runId: run.id,
        command: admission(previous, "previous-attempt"),
        operation: previous,
        externalFacts: admissionFacts,
        receiptId: "previous-receipt",
      })).kind,
      "accepted",
    );
    const settled = {
      ...previous,
      status: priorStatus,
      ...(priorStatus === "pending"
        ? {}
        : {
            outcome: {
              kind: priorStatus === "settled" ? "succeeded" : "unknown",
              summary: "observed",
              evidence: [],
            },
          }),
    };
    yield* sql`UPDATE workflow_operations SET status = ${priorStatus}, record_json = ${json(settled)} WHERE id = ${previous.id}`;
    yield* sql`INSERT INTO workflow_acceptances VALUES (${run.id}, 'item:item', '{}', 0)`;
    const nextBase = operationFor("next-operation", "next-attempt");
    if (nextBase.parameters.kind !== "stage_launch") throw new Error("expected stage launch");
    const next = decodeOperation({
      ...nextBase,
      parameters: {
        ...nextBase.parameters,
        context: { ...nextBase.parameters.context, itemId: "next" },
      },
    });
    const operation = { ...next, fingerprint: WorkflowCanonicalHash.operation(next) };
    const command = {
      ...admission(operation, "next-attempt"),
      expectedVersion: 1,
      itemId: "next",
    };
    const externalFacts = { ...admissionFacts, orderedItemIds: ["item", "next"] };
    const input = reservation(operation, "next-attempt");
    const {
      fingerprint: _commandHash,
      operationFingerprint: _operationHash,
      ...retainedCommand
    } = command;
    const result =
      path === "legacy"
        ? yield* store.commitTransition({
            runId: run.id,
            command,
            operation,
            externalFacts,
            receiptId: "next-receipt",
          })
        : yield* store.reserveWorkflowNativeIntent({
            ...input,
            itemId: "next",
            externalFacts,
            draft: { ...input.draft, admission: { command: retainedCommand } },
          });
    if (priorStatus === "settled") {
      assert.strictEqual(result.kind, path === "legacy" ? "accepted" : "reserved");
    } else {
      assert.deepStrictEqual(result, { kind: "rejected", reason: "custody" });
    }
    const rows = yield* sql<{
      record_json: string;
    }>`SELECT record_json FROM workflow_runs WHERE id = ${run.id}`;
    assert.strictEqual(
      (parse(rows[0]!.record_json) as typeof run).allowance.attemptsConsumed,
      priorStatus === "settled" ? 2 : 1,
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("independent connections contend for the last workflow attempt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "workflow-reserve-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "test.sqlite");
      const fileLayer = () =>
        Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* setupAdmission;
          const sql = yield* SqlClient.SqlClient;
          const limited = {
            ...run,
            access: { origins: [{ ...destination, threadId: "thread" }] },
            allowance: { ...run.allowance, maxAttempts: 1 },
          };
          yield* sql`UPDATE workflow_runs SET record_json = ${json(limited)} WHERE id = ${run.id}`;
        }).pipe(Effect.provide(fileLayer())),
      );
      const barrier = yield* Deferred.make<void>();
      const arrivals = yield* Ref.make(0);
      const attempt = (id: string) =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            const count = yield* Ref.updateAndGet(arrivals, (value) => value + 1);
            if (count === 2) yield* Deferred.succeed(barrier, undefined);
            yield* Deferred.await(barrier);
            return yield* Effect.result(
              store.reserveWorkflowNativeIntent(reservation(operationFor(`op-${id}`, id), id)),
            );
          }).pipe(Effect.provide(fileLayer())),
        );
      const results = yield* Effect.all([attempt("one"), attempt("two")], { concurrency: 2 });
      assert.strictEqual(
        results.filter((result) => result._tag === "Success" && result.success.kind === "reserved")
          .length,
        1,
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const rows = yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`;
          assert.strictEqual(
            (parse(rows[0]!.record_json) as typeof run).allowance.attemptsConsumed,
            1,
          );
          assert.strictEqual((yield* sql`SELECT * FROM workflow_native_launch_intents`).length, 1);
          assert.strictEqual((yield* sql`SELECT * FROM workflow_native_intent_receipts`).length, 1);
        }).pipe(Effect.provide(fileLayer())),
      );
    }),
  ),
);

it.effect("admits, replays, blocks a concurrent attempt, and permits a repair after failure", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const legacy = decodeOperation({
      ...operationFor("op-1", "attempt-1"),
      origin: { ...run.origin, id: "legacy-actor" },
    });
    const first = { ...legacy, fingerprint: WorkflowCanonicalHash.operation(legacy) };
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
    assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: "thread" }), {
      kind: "owned",
      owner: { kind: "workflow_run", workflowRunId: run.id },
    });
    const stored = yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`;
    assert.strictEqual((parse(stored[0]!.record_json) as typeof run).allowance.attemptsConsumed, 1);
    assert.deepStrictEqual(
      yield* store
        .commitTransition({
          ...input,
          operation: decodeOperation({
            ...first,
            access: { origins: [{ ...destination, threadId: "other" }] },
          }),
        })
        .pipe(Effect.result),
      yield* Effect.fail(new Store.WorkflowRunStoreError({ reason: "integrity" })).pipe(
        Effect.result,
      ),
    );
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
          yield* intentMigration;
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

const seedAdmission = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`PRAGMA foreign_keys = ON`;
  yield* runMigrations();
  const example = {
    schemaVersion: 1,
    planId: "plan",
    revisionId: "revision",
    environmentId: "environment",
    projectId: "project",
    title: "Example",
    brief: "Outcome",
    linkedThreadIds: [],
    sources: [
      {
        id: "source",
        kind: "thread_message",
        environmentId: "environment",
        projectId: "project",
        threadId: "thread",
        messageId: "message",
        capturedAt: "2026-11-01T00:00:00Z",
        attribution: "user",
        bytesBase64: "WA==",
        byteLength: 1,
        sha256: "4b68ab3847feda7d6c62c1fbcbeebfa35eab7351ed5e78f4ddadea5df64b8015",
      },
    ],
    sourceStatements: [
      { id: "statement", sourceId: "source", startByte: 0, endByte: 1, text: "X" },
    ],
    requirements: [],
  };
  const { native } = yield* upgradeDevelopmentPlanDocumentV1({
    bytes: new TextEncoder().encode(json(example)),
    hashSourcePacket: DevelopmentPlanHash.sourcePacket,
  });
  const sourcePacketHash = DevelopmentPlanHash.sourcePacket(native.sourcePacket);
  const revisionHash = DevelopmentPlanHash.revision(native.revision);
  yield* sql`INSERT INTO workflow_plan_identities VALUES ('plan')`;
  yield* sql`INSERT INTO development_plans VALUES ('plan', 'environment', 'project', 'Plan', NULL, 0, '2026-11-01T00:00:00Z')`;
  yield* sql`INSERT INTO development_plan_source_packets VALUES ('plan', ${sourcePacketHash}, ${json(native.sourcePacket)})`;
  yield* sql`INSERT INTO workflow_revision_identities VALUES ('plan', 'revision', ${revisionHash}, ${sourcePacketHash})`;
  yield* sql`INSERT INTO development_plan_revisions VALUES ('plan', 'revision', ${revisionHash}, ${sourcePacketHash}, ${json(native.revision)}, '2026-11-01T00:00:00Z')`;
  yield* sql`UPDATE development_plans SET current_revision_id = 'revision' WHERE plan_id = 'plan'`;
  return {
    planId: session.planId,
    revisionId: "revision",
    revisionSchemaVersion: 1 as const,
    revisionHash,
    sourcePacketHash,
  };
});
const decodePassFixture = Schema.decodeSync(PlanningPassV1);
const decodeSessionFixture = Schema.decodeSync(PlanningSessionV1);
const decodeActorFixture = Schema.decodeSync(PlanningSessionV1.fields.origin);
const decodeTerminalId = Schema.decodeSync(
  PlanningPassV1.fields.native.schema.fields.terminalMessageId.schema,
);
const decodeThreadId = Schema.decodeSync(PlanningPassV1.fields.native.schema.fields.threadId);
const decodeNativeRunId = Schema.decodeSync(PlanningPassV1.fields.native.schema.fields.runId);
const decodePassId = Schema.decodeSync(PlanningPassV1.fields.id);
const decodeOperationFixture = Schema.decodeSync(WorkflowOperationV1);
const admissionInput = (
  binding: Awaited<Effect.Success<typeof seedAdmission>>,
  id: string,
  owner = session,
) => {
  const pass = decodePassFixture({
    schemaVersion: 1,
    id: `pass-${id}`,
    planningSessionId: owner.id,
    role: "author",
    provider: { providerInstanceId: "provider", model: "model" },
    binding,
    operationId: `operation-${id}`,
    inputManifest: {
      sources: [{ kind: "source", id: "source", sha256: binding.sourcePacketHash }],
      references: [],
      additionalContext: [],
      access: { origins: [] },
    },
    status: "pending",
    native: { threadId: `thread-${id}`, runId: `run-${id}` },
    access: { origins: [] },
  });
  const operation = decodeOperationFixture({
    schemaVersion: 1,
    id: pass.operationId,
    owner: { kind: "planning_session", planningSessionId: owner.id },
    kind: "planning_pass",
    parameters: {
      kind: "planning_pass",
      passId: pass.id,
      binding,
      role: pass.role,
      provider: pass.provider,
      limits: owner.limits,
      inputManifestHash: WorkflowCanonicalHash.tagged(
        "t3.planning-input-manifest.v1",
        pass.inputManifest,
      ),
    },
    fingerprint: hash,
    origin: owner.origin,
    destination: context.destination,
    controlGeneration: owner.controlGeneration,
    status: "pending",
    access: { origins: [] },
  });
  return {
    sessionId: owner.id,
    expectedVersion: 0,
    pass,
    operation: { ...operation, fingerprint: WorkflowCanonicalHash.operation(operation) },
    now: facts.now,
    context,
    command: { commandId: `command-${id}`, receiptId: `receipt-${id}` },
  };
};

const planningReservation = (admission: ReturnType<typeof admissionInput>) => {
  const { native: _native, ...pass } = admission.pass;
  const {
    fingerprint: _fingerprint,
    controlGeneration: _generation,
    status: _status,
    claim: _claim,
    outcome: _outcome,
    nextDueAt: _nextDueAt,
    ...operation
  } = admission.operation;
  return {
    intentId: `intent-${pass.id}`,
    sessionId: admission.sessionId,
    operationId: operation.id,
    passId: pass.id,
    role: pass.role,
    environmentId: admission.context.destination.environmentId,
    projectId: admission.context.destination.projectId,
    launchCommandId: `launch-${pass.id}`,
    threadId: admission.pass.native!.threadId,
    messageId: `message-${pass.id}`,
    generation: admission.operation.controlGeneration,
    draft: {
      version: 1 as const,
      kind: "planning_pass" as const,
      operation,
      pass,
      admission: {
        sessionId: admission.sessionId,
        expectedVersion: admission.expectedVersion,
        context: admission.context,
        command: admission.command,
      },
    },
    now: admission.now,
  };
};

const layerWithOutbox = Layer.merge(Store.layer, EffectOutbox.layer).pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);

const nativeRequests = [
  { type: "provider-turn.start", runId: RunId.make("native-run") },
  {
    type: "provider-turn.restart",
    runId: RunId.make("native-run"),
    interruptedAttemptId: RunAttemptId.make("attempt"),
    providerSessionId: ProviderSessionId.make("session"),
    providerThreadId: ProviderThreadId.make("provider-thread"),
    providerTurnId: ProviderTurnId.make("turn"),
  },
  { type: "provider-runtime.continue", sourceRunId: RunId.make("native-run") },
] as const;
const rawReservationState = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return {
    runs: yield* sql`SELECT * FROM workflow_runs`,
    sessions: yield* sql`SELECT * FROM workflow_planning_sessions`,
    intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
    receipts: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
    commands: yield* sql`SELECT * FROM workflow_command_receipts`,
    operations: yield* sql`SELECT * FROM workflow_operations`,
    attempts: yield* sql`SELECT * FROM workflow_attempts`,
    passes: yield* sql`SELECT * FROM workflow_planning_passes`,
    outbox: yield* sql`SELECT * FROM orchestration_v2_effect_outbox`,
  };
});

it.effect.each(["workflow", "planning"] as const)(
  "legacy %s admission refuses a running native effect",
  (owner) =>
    Effect.gen(function* () {
      const store = yield* Store.WorkflowRunStore;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      let threadId: string;
      let workflowInput: Parameters<typeof store.commitTransition>[0] | undefined;
      let planningInput: ReturnType<typeof admissionInput> | undefined;
      if (owner === "workflow") {
        yield* setupAdmission;
        const operation = operationFor("legacy-running-op", "legacy-running-attempt");
        workflowInput = {
          runId: run.id,
          command: admission(operation, "legacy-running-attempt"),
          externalFacts: admissionFacts,
          operation,
          receiptId: "legacy-running-receipt",
        };
        threadId = "thread";
      } else {
        const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
        yield* begin(store, session);
        planningInput = admissionInput(binding, "legacy-running");
        threadId = planningInput.pass.native!.threadId;
      }
      yield* outbox.enqueue([
        {
          id: "legacy-running-effect",
          commandId: CommandId.make("legacy-running-command"),
          threadId: ThreadId.make(threadId),
          request: nativeRequests[0],
        },
      ]);
      assert.isTrue(Option.isSome(yield* outbox.claimNext({ workerId: "worker" })));
      const before = yield* rawReservationState;
      if (workflowInput)
        assert.deepStrictEqual(yield* store.commitTransition(workflowInput), {
          kind: "rejected",
          reason: "custody",
        });
      if (planningInput)
        assert.strictEqual(
          (yield* Effect.flip(store.admitPlanningPass(planningInput))).reason,
          "custody",
        );
      assert.deepStrictEqual(yield* rawReservationState, before);
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each([
  ["workflow", "cancelled"],
  ["planning", "cancelled"],
  ["workflow", "malformed"],
  ["planning", "malformed"],
] as const)("legacy %s admission rejects %s foreign association", ([target, state]) =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const owner = {
      ...run,
      spec: { ...run.spec, plan: binding },
      access: { origins: [{ ...destination, threadId: "thread" }] },
    };
    yield* sql`INSERT INTO workflow_runs VALUES (${owner.id}, ${binding.planId}, ${binding.revisionId}, ${binding.revisionHash}, ${binding.sourcePacketHash}, ${hash}, 'active', 0, 0, ${json(owner)})`;
    const operation = operationFor("foreign-workflow-op", "foreign-workflow-attempt");
    const workflowInput = {
      runId: owner.id,
      command: admission(operation, "foreign-workflow-attempt"),
      externalFacts: admissionFacts,
      operation,
      receiptId: "foreign-workflow-receipt",
    };
    const planningBase = admissionInput(binding, "foreign-planning");
    const planningInput = {
      ...planningBase,
      pass: {
        ...planningBase.pass,
        native: { ...planningBase.pass.native!, threadId: decodeThreadId("thread") },
      },
    };
    if (target === "workflow") {
      const reservationInput = planningReservation(planningInput);
      yield* store.reservePlanningNativeIntent(reservationInput);
      if (state === "cancelled")
        yield* store.controlPlanning({
          sessionId: session.id,
          action: "stop",
          expectedVersion: 1,
          now: facts.now,
          context,
          command: { commandId: "stop-foreign", receiptId: "stop-foreign" },
        });
      else
        yield* sql`UPDATE workflow_native_intent_receipts SET result_json = '{}' WHERE intent_id = ${reservationInput.intentId}`;
    } else {
      const reservationInput = reservation(operation, "foreign-workflow-attempt");
      yield* store.reserveWorkflowNativeIntent(reservationInput);
      if (state === "cancelled")
        yield* store.commitTransition({
          runId: owner.id,
          command: { ...command, kind: "stop", commandId: "stop-foreign", expectedVersion: 1 },
          externalFacts: facts,
          receiptId: "stop-foreign",
        });
      else
        yield* sql`UPDATE workflow_native_intent_receipts SET result_json = '{}' WHERE intent_id = ${reservationInput.intentId}`;
    }
    const before = yield* rawReservationState;
    if (target === "workflow") {
      if (state === "malformed")
        assert.strictEqual(
          (yield* Effect.flip(store.commitTransition(workflowInput))).reason,
          "integrity",
        );
      else
        assert.deepStrictEqual(yield* store.commitTransition(workflowInput), {
          kind: "rejected",
          reason: "custody",
        });
    } else {
      assert.strictEqual(
        (yield* Effect.flip(store.admitPlanningPass(planningInput))).reason,
        state === "malformed" ? "integrity" : "custody",
      );
    }
    assert.deepStrictEqual(yield* rawReservationState, before);
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each(["workflow", "planning"] as const)(
  "legacy %s admission fails closed on a missing native lookup",
  (owner) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      let workflowInput: Parameters<typeof store.commitTransition>[0] | undefined;
      let planningInput: ReturnType<typeof admissionInput> | undefined;
      if (owner === "workflow") {
        yield* setupAdmission;
        const operation = operationFor("missing-op", "missing-attempt");
        workflowInput = {
          runId: run.id,
          command: admission(operation, "missing-attempt"),
          externalFacts: admissionFacts,
          operation,
          receiptId: "missing-receipt",
        };
      } else {
        const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
        yield* begin(store, session);
        planningInput = admissionInput(binding, "missing");
      }
      const before = yield* rawReservationState;
      yield* sql`DROP TABLE orchestration_v2_effect_outbox`;
      if (workflowInput)
        assert.strictEqual(
          (yield* Effect.flip(store.commitTransition(workflowInput))).reason,
          "persistence",
        );
      if (planningInput)
        assert.strictEqual(
          (yield* Effect.flip(store.admitPlanningPass(planningInput))).reason,
          "storage",
        );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before.runs);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before.sessions);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_command_receipts`, before.commands);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, before.operations);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_attempts`, before.attempts);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_passes`, before.passes);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_launch_intents`,
        before.intents,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        before.receipts,
      );
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each([
  ["workflow", "legacy"],
  ["workflow", "native"],
  ["planning", "legacy"],
  ["planning", "native"],
] as const)(
  "independent legacy %s and native writers preserve custody when %s commits first",
  ([legacyOwner, winner]) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "legacy-custody-race-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        const fileLayer = () =>
          Layer.merge(Store.layer, EffectOutbox.layer).pipe(
            Layer.provideMerge(NodeSqliteClient.layer({ filename })),
          );
        const inputs = yield* Effect.scoped(
          Effect.gen(function* () {
            const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
            const sql = yield* SqlClient.SqlClient;
            const store = yield* Store.WorkflowRunStore;
            yield* begin(store, session);
            const owner = {
              ...run,
              spec: { ...run.spec, plan: binding },
              access: { origins: [{ ...destination, threadId: "thread" }] },
            };
            yield* sql`INSERT INTO workflow_runs VALUES (${owner.id}, ${binding.planId}, ${binding.revisionId}, ${binding.revisionHash}, ${binding.sourcePacketHash}, ${hash}, 'active', 0, 0, ${json(owner)})`;
            const operation = operationFor("race-legacy-op", "race-legacy-attempt");
            const workflow = {
              runId: owner.id,
              command: admission(operation, "race-legacy-attempt"),
              externalFacts: admissionFacts,
              operation,
              receiptId: "race-legacy-receipt",
            };
            const base = admissionInput(binding, "race-planning");
            const planning = {
              ...base,
              pass: {
                ...base.pass,
                native: { ...base.pass.native!, threadId: decodeThreadId("thread") },
              },
            };
            return {
              workflow,
              planning,
              nativeWorkflow: reservation(operation, "race-legacy-attempt"),
              nativePlanning: planningReservation(planning),
            };
          }).pipe(Effect.provide(fileLayer())),
        );
        const ready = yield* Deferred.make<void>();
        const firstCommitted = yield* Deferred.make<void>();
        const arrivals = yield* Ref.make(0);
        const contender = (kind: "legacy" | "native") =>
          Effect.scoped(
            Effect.gen(function* () {
              const store = yield* Store.WorkflowRunStore;
              if ((yield* Ref.updateAndGet(arrivals, (n) => n + 1)) === 2)
                yield* Deferred.succeed(ready, undefined);
              yield* Deferred.await(ready);
              if (kind !== winner) yield* Deferred.await(firstCommitted);
              const before = yield* rawReservationState;
              const result =
                kind === "legacy"
                  ? legacyOwner === "workflow"
                    ? yield* Effect.result(store.commitTransition(inputs.workflow))
                    : yield* Effect.result(store.admitPlanningPass(inputs.planning))
                  : legacyOwner === "workflow"
                    ? yield* Effect.result(store.reservePlanningNativeIntent(inputs.nativePlanning))
                    : yield* Effect.result(
                        store.reserveWorkflowNativeIntent(inputs.nativeWorkflow),
                      );
              const after = yield* rawReservationState;
              if (kind === winner) yield* Deferred.succeed(firstCommitted, undefined);
              return { result, before, after };
            }).pipe(Effect.provide(fileLayer())),
          );
        const [legacy, native] = yield* Effect.all([contender("legacy"), contender("native")], {
          concurrency: 2,
        });
        const first = winner === "legacy" ? legacy : native;
        const second = winner === "legacy" ? native : legacy;
        assert.strictEqual(first.result._tag, "Success");
        if (first.result._tag === "Success")
          assert.isTrue(["accepted", "applied", "reserved"].includes(first.result.success.kind));
        if (second.result._tag === "Failure")
          assert.strictEqual(second.result.failure.reason, "custody");
        else assert.deepStrictEqual(second.result.success, { kind: "rejected", reason: "custody" });
        assert.deepStrictEqual(second.before, first.after);
        assert.deepStrictEqual(second.after, second.before);
      }),
    ),
);

it.effect.each(["workflow", "planning"] as const)(
  "independent native claim commits before legacy %s admission",
  (owner) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "legacy-claim-race-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        const fileLayer = () =>
          Layer.merge(Store.layer, EffectOutbox.layer).pipe(
            Layer.provideMerge(NodeSqliteClient.layer({ filename })),
          );
        const inputs = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            let workflow: Parameters<typeof store.commitTransition>[0] | undefined;
            let planning: ReturnType<typeof admissionInput> | undefined;
            let threadId: string;
            if (owner === "workflow") {
              yield* setupAdmission;
              const operation = operationFor("claim-legacy-op", "claim-legacy-attempt");
              workflow = {
                runId: run.id,
                command: admission(operation, "claim-legacy-attempt"),
                externalFacts: admissionFacts,
                operation,
                receiptId: "claim-legacy-receipt",
              };
              threadId = "thread";
            } else {
              const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
              yield* begin(store, session);
              planning = admissionInput(binding, "claim-legacy");
              threadId = planning.pass.native!.threadId;
            }
            yield* outbox.enqueue([
              {
                id: "claim-legacy-effect",
                commandId: CommandId.make("claim-legacy-command"),
                threadId: ThreadId.make(threadId),
                request: nativeRequests[0],
              },
            ]);
            return { workflow, planning };
          }).pipe(Effect.provide(fileLayer())),
        );
        const claimed = yield* Deferred.make<void>();
        const completed = yield* Deferred.make<void>();
        const claim = Effect.scoped(
          Effect.gen(function* () {
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            assert.isTrue(
              Option.isSome(yield* outbox.claimNext({ workerId: "legacy-claim-worker" })),
            );
            const after = yield* rawReservationState;
            yield* Deferred.succeed(claimed, undefined);
            yield* Deferred.await(completed);
            return after;
          }).pipe(Effect.provide(fileLayer())),
        );
        const attemptAdmission = Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            yield* Deferred.await(claimed);
            const before = yield* rawReservationState;
            if (inputs.workflow)
              assert.deepStrictEqual(yield* store.commitTransition(inputs.workflow), {
                kind: "rejected",
                reason: "custody",
              });
            if (inputs.planning)
              assert.strictEqual(
                (yield* Effect.flip(store.admitPlanningPass(inputs.planning))).reason,
                "custody",
              );
            const after = yield* rawReservationState;
            yield* Deferred.succeed(completed, undefined);
            return { before, after };
          }).pipe(Effect.provide(fileLayer())),
        );
        const [claimState, admissionState] = yield* Effect.all([claim, attemptAdmission], {
          concurrency: 2,
        });
        assert.deepStrictEqual(admissionState.before, claimState);
        assert.deepStrictEqual(admissionState.after, admissionState.before);
      }),
    ),
);

it.effect.each(["same", "new"] as const)(
  "admission %s command replay validates immutable identity",
  (path) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const store = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      const operation = operationFor("identity-op", "identity-attempt");
      const input = {
        runId: run.id,
        command: admission(operation, "identity-attempt"),
        externalFacts: admissionFacts,
        operation,
        receiptId: "identity-receipt",
      };
      assert.strictEqual((yield* store.commitTransition(input)).kind, "accepted");
      const before = yield* rawReservationState;
      const commandFor = (changes: Partial<typeof input.command> = {}) => ({
        ...input.command,
        ...changes,
        commandId: path === "same" ? input.command.commandId : "new-command",
      });
      if (operation.parameters.kind !== "stage_launch") throw new Error("expected stage launch");
      const changedOperations = [
        {} as WorkflowOperationV1,
        decodeOperation({ ...operation, id: "other-id" }),
        decodeOperation({
          ...operation,
          owner: { kind: "workflow_run", workflowRunId: "other-run" },
        }),
        decodeOperation({
          ...operation,
          access: { origins: [{ ...destination, threadId: "other" }] },
        }),
        decodeOperation({
          ...operation,
          parameters: { ...operation.parameters, nativeThreadId: "other-thread" },
        }),
      ];
      for (const changed of changedOperations) {
        const result = yield* Effect.result(
          store.commitTransition({ ...input, command: commandFor(), operation: changed }),
        );
        if (path === "same") {
          assert.strictEqual(result._tag, "Failure");
          if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "integrity");
        } else {
          assert.deepStrictEqual(
            result,
            yield* Effect.succeed({ kind: "rejected" as const, reason: "integrity" as const }).pipe(
              Effect.result,
            ),
          );
        }
      }
      for (const changes of [
        { attemptId: "other-attempt" },
        { itemId: "other-item" },
        { repair: true },
      ]) {
        const result = yield* Effect.result(
          store.commitTransition({ ...input, command: commandFor(changes) }),
        );
        if (path === "same") assert.strictEqual(result._tag, "Failure");
        else
          assert.deepStrictEqual(
            result,
            yield* Effect.succeed({ kind: "rejected" as const, reason: "integrity" as const }).pipe(
              Effect.result,
            ),
          );
      }
      assert.deepStrictEqual(yield* rawReservationState, before);
      assert.strictEqual(
        (yield* store.commitTransition({
          runId: run.id,
          command: { ...command, commandId: "later-pause", expectedVersion: 1 },
          externalFacts: facts,
          receiptId: "later-pause",
        })).kind,
        "accepted",
      );
      const settled = {
        ...operation,
        status: "cancelled",
        outcome: { kind: "cancelled", summary: "cancelled", evidence: [] },
      };
      yield* sql`UPDATE workflow_operations SET status = 'cancelled', held = 0, record_json = ${json(settled)} WHERE id = ${operation.id}`;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      yield* outbox.enqueue([
        {
          id: "replay-later-effect",
          commandId: CommandId.make("replay-later-effect"),
          threadId: ThreadId.make("thread"),
          request: nativeRequests[0],
        },
      ]);
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'running', attempt_count = 1, lease_owner = 'worker' WHERE effect_id = 'replay-later-effect'`;
      const replay = {
        ...input,
        command: commandFor(),
        externalFacts: { ...admissionFacts, now: "2027-01-01T00:00:00Z" },
      };
      const { operation: _omitted, ...withoutOperation } = replay;
      const afterActivity = yield* rawReservationState;
      assert.deepStrictEqual(yield* store.commitTransition(replay), {
        kind: "replay",
        receiptId: input.receiptId,
      });
      assert.deepStrictEqual(yield* store.commitTransition(withoutOperation), {
        kind: "replay",
        receiptId: input.receiptId,
      });
      assert.deepStrictEqual(yield* rawReservationState, afterActivity);
      yield* sql`DROP TABLE orchestration_v2_effect_outbox`;
      assert.deepStrictEqual(yield* store.commitTransition(replay), {
        kind: "replay",
        receiptId: input.receiptId,
      });
      assert.deepStrictEqual(yield* store.commitTransition(withoutOperation), {
        kind: "replay",
        receiptId: input.receiptId,
      });
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, afterActivity.runs);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_operations`,
        afterActivity.operations,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_attempts`, afterActivity.attempts);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_command_receipts`,
        afterActivity.commands,
      );
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each(["operation", "attempt", "receipt", "unsafe receipt"] as const)(
  "admission replay refuses malformed stored %s",
  (tamper) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const operation = operationFor("tamper-op", "tamper-attempt");
      const input = {
        runId: run.id,
        command: admission(operation, "tamper-attempt"),
        externalFacts: admissionFacts,
        operation,
        receiptId: "tamper-receipt",
      };
      yield* store.commitTransition(input);
      if (tamper === "operation")
        yield* sql`UPDATE workflow_operations SET record_json = '{}' WHERE id = ${operation.id}`;
      if (tamper === "attempt")
        yield* sql`UPDATE workflow_attempts SET item_id = 'other' WHERE operation_id = ${operation.id}`;
      if (tamper === "receipt")
        yield* sql`UPDATE workflow_command_receipts SET result_json = '{}' WHERE operation_id = ${operation.id}`;
      if (tamper === "unsafe receipt")
        yield* sql`UPDATE workflow_command_receipts SET result_json = json_set(result_json, '$.extra', 9007199254740992) WHERE operation_id = ${operation.id}`;
      const before = yield* rawReservationState;
      for (const commandId of [input.command.commandId, "another-command"]) {
        const error = yield* Effect.flip(
          store.commitTransition({ ...input, command: { ...input.command, commandId } }),
        );
        assert.strictEqual(error.reason, "integrity");
      }
      assert.deepStrictEqual(yield* rawReservationState, before);
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each(
  (["workflow", "planning"] as const).flatMap((owner) =>
    nativeRequests.map((request) => ({ owner, request })),
  ),
)("refuses a new $owner reservation after a $request.type claim", ({ owner, request }) =>
  Effect.gen(function* () {
    const store = yield* Store.WorkflowRunStore;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    let input;
    if (owner === "workflow") {
      yield* setupAdmission;
      input = reservation(operationFor("running-op", "running-attempt"), "running-attempt");
    } else {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      yield* begin(store, session);
      input = planningReservation(admissionInput(binding, "running"));
    }
    yield* outbox.enqueue([
      {
        id: "running-effect",
        commandId: CommandId.make("running-effect-command"),
        threadId: ThreadId.make(input.threadId),
        request,
      },
    ]);
    assert.isTrue(Option.isSome(yield* outbox.claimNext({ workerId: "worker" })));
    const before = yield* rawReservationState;
    if (owner === "workflow") {
      assert.deepStrictEqual(
        yield* store.reserveWorkflowNativeIntent(input as ReturnType<typeof reservation>),
        {
          kind: "rejected",
          reason: "custody",
        },
      );
    } else {
      assert.strictEqual(
        (yield* Effect.flip(
          store.reservePlanningNativeIntent(input as ReturnType<typeof planningReservation>),
        )).reason,
        "custody",
      );
    }
    assert.deepStrictEqual(yield* rawReservationState, before);
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each([
  ["workflow", "pending"],
  ["planning", "pending"],
  ["workflow", "succeeded"],
  ["planning", "succeeded"],
  ["workflow", "other"],
  ["planning", "other"],
] as const)("%s reservation allows %s outbox effects", ([owner, state]) =>
  Effect.gen(function* () {
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const outbox = yield* EffectOutbox.EffectOutboxV2;
    let input;
    if (owner === "workflow") {
      yield* setupAdmission;
      input = reservation(operationFor("allowed-op", "allowed-attempt"), "allowed-attempt");
    } else {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      yield* begin(store, session);
      input = planningReservation(admissionInput(binding, "allowed"));
    }
    yield* outbox.enqueue([
      {
        id: "allowed-effect",
        commandId: CommandId.make("allowed-command"),
        threadId: ThreadId.make(input.threadId),
        request: state === "other" ? { type: "terminal.cleanup" } : nativeRequests[0],
      },
    ]);
    if (state === "other")
      assert.isTrue(Option.isSome(yield* outbox.claimNext({ workerId: "worker" })));
    if (state === "succeeded")
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'succeeded' WHERE effect_id = 'allowed-effect'`;
    const result =
      owner === "workflow"
        ? yield* store.reserveWorkflowNativeIntent(input as ReturnType<typeof reservation>)
        : yield* store.reservePlanningNativeIntent(input as ReturnType<typeof planningReservation>);
    assert.strictEqual(result.kind, "reserved");
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each(["workflow", "planning"] as const)(
  "%s replay bypasses running and missing outbox; new reservations fail closed",
  (owner) =>
    Effect.gen(function* () {
      const store = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      const outbox = yield* EffectOutbox.EffectOutboxV2;
      let first;
      let next;
      if (owner === "workflow") {
        yield* setupAdmission;
        first = reservation(operationFor("replay-op", "replay-attempt"), "replay-attempt");
        next = reservation(operationFor("next-op", "next-attempt"), "next-attempt");
      } else {
        const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
        yield* begin(store, session);
        first = planningReservation(admissionInput(binding, "replay"));
        next = planningReservation(admissionInput(binding, "next"));
      }
      const reserve = (input: typeof first) =>
        Effect.gen(function* () {
          if (owner === "workflow")
            return yield* store.reserveWorkflowNativeIntent(
              input as ReturnType<typeof reservation>,
            );
          return yield* store.reservePlanningNativeIntent(
            input as ReturnType<typeof planningReservation>,
          );
        });
      const initial = yield* reserve(first);
      assert.strictEqual(initial.kind, "reserved");
      yield* outbox.enqueue([
        {
          id: "replay-effect",
          commandId: CommandId.make("replay-effect-command"),
          threadId: ThreadId.make(first.threadId),
          request: nativeRequests[0],
        },
      ]);
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status = 'running', attempt_count = 1, lease_owner = 'worker' WHERE effect_id = 'replay-effect'`;
      const before = yield* rawReservationState;
      assert.deepStrictEqual(yield* reserve(first), { ...initial, kind: "replay" });
      assert.deepStrictEqual(yield* rawReservationState, before);
      yield* sql`DROP TABLE orchestration_v2_effect_outbox`;
      assert.deepStrictEqual(yield* reserve(first), { ...initial, kind: "replay" });
      const failure = yield* Effect.flip(reserve(next));
      assert.strictEqual(failure.reason, owner === "workflow" ? "persistence" : "storage");
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_launch_intents`,
        before.intents,
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        before.receipts,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before.runs);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before.sessions);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_command_receipts`, before.commands);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, before.operations);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_attempts`, before.attempts);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_passes`, before.passes);
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each(["workflow", "planning"] as const)(
  "%s reservation maps a writer lock failure without writes",
  (owner) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-locked-"))),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        const fileLayer = Store.layer.pipe(
          Layer.provideMerge(NodeSqliteClient.layer({ filename })),
        );
        yield* Effect.gen(function* () {
          const store = yield* Store.WorkflowRunStore;
          const sql = yield* SqlClient.SqlClient;
          let input;
          if (owner === "workflow") {
            yield* setupAdmission;
            input = reservation(operationFor("locked-op", "locked-attempt"), "locked-attempt");
          } else {
            const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
            yield* begin(store, session);
            input = planningReservation(admissionInput(binding, "locked"));
          }
          const before = yield* rawReservationState;
          yield* sql`PRAGMA busy_timeout = 0`;
          const other = yield* Effect.acquireRelease(
            Effect.sync(() => {
              const database = new NodeSqlite.DatabaseSync(filename);
              database.exec("BEGIN IMMEDIATE");
              return database;
            }),
            (database) => Effect.sync(() => database.close()),
          );
          const failure =
            owner === "workflow"
              ? yield* Effect.flip(
                  store.reserveWorkflowNativeIntent(input as ReturnType<typeof reservation>),
                )
              : yield* Effect.flip(
                  store.reservePlanningNativeIntent(
                    input as ReturnType<typeof planningReservation>,
                  ),
                );
          assert.strictEqual(failure.reason, owner === "workflow" ? "persistence" : "storage");
          yield* Effect.sync(() => other.exec("ROLLBACK"));
          assert.deepStrictEqual(yield* rawReservationState, before);
        }).pipe(Effect.provide(fileLayer));
      }),
    ),
);

it.effect.each([
  ["workflow", "reservation"],
  ["workflow", "claim"],
  ["planning", "reservation"],
  ["planning", "claim"],
] as const)(
  "independent connections preserve $0 custody when $1 commits first",
  ([owner, winner]) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-commit-order-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        const fileLayer = () =>
          Layer.merge(Store.layer, EffectOutbox.layer).pipe(
            Layer.provideMerge(NodeSqliteClient.layer({ filename })),
          );
        const prepared = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            let input;
            if (owner === "workflow") {
              yield* setupAdmission;
              input = reservation(operationFor("race-op", "race-attempt"), "race-attempt");
            } else {
              const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
              yield* begin(store, session);
              input = planningReservation(admissionInput(binding, "race"));
            }
            yield* outbox.enqueue([
              {
                id: "race-effect",
                commandId: CommandId.make("race-command"),
                threadId: ThreadId.make(input.threadId),
                request: nativeRequests[0],
              },
            ]);
            return { input, before: yield* rawReservationState };
          }).pipe(Effect.provide(fileLayer())),
        );
        const ready = yield* Deferred.make<void>();
        const arrivals = yield* Ref.make(0);
        const firstCommitted = yield* Deferred.make<void>();
        const secondFinished = yield* Deferred.make<void>();
        const proofFinished = yield* Deferred.make<void>();
        const rendezvous = Effect.gen(function* () {
          if ((yield* Ref.updateAndGet(arrivals, (n) => n + 1)) === 2)
            yield* Deferred.succeed(ready, undefined);
          yield* Deferred.await(ready);
        });
        const reserve = Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            yield* rendezvous;
            if (winner === "claim") yield* Deferred.await(firstCommitted);
            const before = yield* rawReservationState;
            const result =
              owner === "workflow"
                ? yield* Effect.result(
                    store.reserveWorkflowNativeIntent(
                      prepared.input as ReturnType<typeof reservation>,
                    ),
                  )
                : yield* Effect.result(
                    store.reservePlanningNativeIntent(
                      prepared.input as ReturnType<typeof planningReservation>,
                    ),
                  );
            const after = yield* rawReservationState;
            if (winner === "reservation") {
              yield* Deferred.succeed(firstCommitted, undefined);
              yield* Deferred.await(secondFinished);
            } else {
              yield* Deferred.succeed(secondFinished, undefined);
            }
            const final = yield* rawReservationState;
            const replay =
              winner === "reservation"
                ? owner === "workflow"
                  ? yield* Effect.result(
                      store.reserveWorkflowNativeIntent(
                        prepared.input as ReturnType<typeof reservation>,
                      ),
                    )
                  : yield* Effect.result(
                      store.reservePlanningNativeIntent(
                        prepared.input as ReturnType<typeof planningReservation>,
                      ),
                    )
                : undefined;
            const afterReplay = yield* rawReservationState;
            if (winner === "reservation") yield* Deferred.succeed(proofFinished, undefined);
            else yield* Deferred.await(proofFinished);
            return { before, result, after, final, replay, afterReplay };
          }).pipe(Effect.provide(fileLayer())),
        );
        const claim = Effect.scoped(
          Effect.gen(function* () {
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            yield* rendezvous;
            if (winner === "reservation") yield* Deferred.await(firstCommitted);
            const before = yield* rawReservationState;
            const result = yield* outbox.claimNext({ workerId: "race-worker" });
            const executed = yield* Ref.make<Array<string>>([]);
            const executor = Layer.succeed(
              EffectWorker.OrchestrationEffectExecutorV2,
              EffectWorker.OrchestrationEffectExecutorV2.of({
                execute: (effect) => Ref.update(executed, (ids) => [...ids, effect.id]),
              }),
            );
            const workerLayer = EffectWorker.layerWithOptions({ workerId: "proof-worker" }).pipe(
              Layer.provide(
                Layer.merge(Layer.succeed(EffectOutbox.EffectOutboxV2, outbox), executor),
              ),
            );
            const worked =
              winner === "reservation"
                ? yield* Effect.gen(function* () {
                    const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
                    return yield* worker.runOnce;
                  }).pipe(Effect.provide(workerLayer))
                : undefined;
            const executedIds = yield* Ref.get(executed);
            const after = yield* rawReservationState;
            if (winner === "claim") {
              yield* Deferred.succeed(firstCommitted, undefined);
              yield* Deferred.await(secondFinished);
            } else {
              yield* Deferred.succeed(secondFinished, undefined);
            }
            const final = yield* rawReservationState;
            if (winner === "claim") yield* Deferred.succeed(proofFinished, undefined);
            else yield* Deferred.await(proofFinished);
            return { before, result, after, final, worked, executedIds };
          }).pipe(Effect.provide(fileLayer())),
        );
        const [reservationResult, claimResult] = yield* Effect.all([reserve, claim], {
          concurrency: 2,
        });
        if (winner === "reservation") {
          assert.strictEqual(reservationResult.result._tag, "Success");
          if (reservationResult.result._tag !== "Success") throw new Error("reservation failed");
          assert.strictEqual(reservationResult.result.success.kind, "reserved");
          assert.deepStrictEqual(claimResult.before, reservationResult.after);
          assert.isTrue(Option.isNone(claimResult.result));
          assert.isFalse(claimResult.worked);
          assert.deepStrictEqual(claimResult.executedIds, []);
          assert.deepStrictEqual(claimResult.after, claimResult.before);
          assert.strictEqual(reservationResult.replay?._tag, "Success");
          if (reservationResult.replay?._tag !== "Success") throw new Error("replay failed");
          assert.deepStrictEqual(reservationResult.replay.success, {
            ...reservationResult.result.success,
            kind: "replay",
          });
          assert.deepStrictEqual(reservationResult.final, reservationResult.after);
          assert.deepStrictEqual(reservationResult.afterReplay, reservationResult.after);
        } else {
          assert.isTrue(Option.isSome(claimResult.result));
          assert.deepStrictEqual(claimResult.before, prepared.before);
          assert.deepStrictEqual(reservationResult.before, claimResult.after);
          assert.deepStrictEqual(reservationResult.after, reservationResult.before);
          assert.deepStrictEqual(claimResult.final, claimResult.after);
          if (owner === "workflow") {
            assert.strictEqual(reservationResult.result._tag, "Success");
            if (reservationResult.result._tag !== "Success")
              throw new Error("workflow reservation failed");
            assert.deepStrictEqual(reservationResult.result.success, {
              kind: "rejected",
              reason: "custody",
            });
          } else {
            assert.strictEqual(reservationResult.result._tag, "Failure");
            if (reservationResult.result._tag !== "Failure")
              throw new Error("planning reservation succeeded");
            assert.strictEqual(reservationResult.result.failure.reason, "custody");
          }
          assert.strictEqual(claimResult.after.outbox[0]?.status, "running");
          assert.strictEqual(claimResult.after.outbox[0]?.attempt_count, 1);
          assert.strictEqual(claimResult.after.outbox[0]?.lease_owner, "race-worker");
        }
        yield* Effect.scoped(
          Effect.gen(function* () {
            const outbox = yield* EffectOutbox.EffectOutboxV2;
            yield* outbox.enqueue([
              {
                id: "ordinary-effect",
                commandId: CommandId.make("ordinary-command"),
                threadId: ThreadId.make("ordinary-thread"),
                request: nativeRequests[0],
              },
            ]);
            const ordinary = yield* outbox.claimNext({ workerId: "ordinary-worker" });
            assert.strictEqual(Option.getOrThrow(ordinary).id, "ordinary-effect");
          }).pipe(Effect.provide(fileLayer())),
        );
      }),
    ),
);

it.effect("recovery custody reads all selected associations without changing state", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const input = reservation(
      operationFor("recovery-custody", "recovery-custody"),
      "recovery-custody",
    );
    yield* store.reserveWorkflowNativeIntent(input);
    yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled' WHERE intent_id = ${input.intentId}`;
    const payload = json({
      id: "shared",
      driver: "codex",
      providerInstanceId: "provider",
      status: "running",
      cwd: "/workspace",
      model: "model",
      capabilities: CodexProviderCapabilitiesV2,
      createdAt: "2026-11-01T00:00:00Z",
      updatedAt: "2026-11-01T00:00:00Z",
      lastError: null,
    });
    yield* sql`INSERT INTO orchestration_v2_projection_provider_sessions (provider_session_id, thread_id, provider, driver, provider_instance_id, status, updated_at, payload_json) VALUES ('shared', NULL, 'provider', 'codex', 'provider', 'running', '2026-11-01T00:00:00Z', ${payload})`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings VALUES ('shared', 'ordinary'), ('shared', ${input.threadId})`;
    const subscription = yield* store.subscribeChanges;
    const snapshot = Effect.gen(function* () {
      return {
        workflow: yield* rawReservationState,
        nativeSessions: yield* sql`SELECT * FROM orchestration_v2_projection_provider_sessions`,
        bindings: yield* sql`SELECT * FROM orchestration_v2_projection_provider_session_bindings`,
        events: yield* sql`SELECT * FROM orchestration_v2_events`,
      };
    });
    const before = yield* snapshot;
    const read = (threadId: string, providerSessionIds: readonly string[]) =>
      sql.withTransaction(store.readRecoveryCustodyInTransaction({ threadId, providerSessionIds }));
    assert.deepStrictEqual(yield* read("unrelated", []), { kind: "ordinary" });
    assert.deepStrictEqual(yield* read(input.threadId, []), { kind: "owned" });
    assert.deepStrictEqual(yield* read("ordinary", []), { kind: "owned" });
    assert.deepStrictEqual(yield* read("unrelated", ["shared", "shared"]), { kind: "owned" });
    assert.deepStrictEqual(yield* snapshot, before);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);

    yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings WHERE thread_id = 'ordinary'`;
    yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET thread_id = 'ordinary' WHERE provider_session_id = 'shared'`;
    assert.deepStrictEqual(yield* read("ordinary", []), { kind: "owned" });
    yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings VALUES ('shared', 'ordinary')`;
    yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings WHERE thread_id = ${input.threadId}`;
    yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET thread_id = ${input.threadId} WHERE provider_session_id = 'shared'`;
    assert.deepStrictEqual(yield* read("ordinary", []), { kind: "owned" });
    assert.deepStrictEqual(yield* read(input.threadId, []), { kind: "owned" });
    yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET thread_id = NULL WHERE provider_session_id = 'shared'`;
    assert.deepStrictEqual(yield* read("ordinary", []), { kind: "ordinary" });
    yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET thread_id = ${input.threadId} WHERE provider_session_id = 'shared'`;
    yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, generation, record_json) VALUES ('malformed-peer', ${run.id}, 'stage_launch', ${hash}, 'pending', 0, '{"parameters":{"nativeThreadId":"ordinary"}}')`;
    assert.strictEqual((yield* Effect.flip(read(input.threadId, []))).reason, "integrity");
    yield* sql`DELETE FROM workflow_operations WHERE id = 'malformed-peer'`;
    yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled', fingerprint = ${nextHash} WHERE intent_id = ${input.intentId}`;
    const malformedBefore = yield* snapshot;
    assert.strictEqual((yield* Effect.flip(read("ordinary", []))).reason, "integrity");
    assert.deepStrictEqual(yield* read("unrelated", []), { kind: "ordinary" });
    assert.deepStrictEqual(yield* snapshot, malformedBefore);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("recovery custody validates allocated sessions and follows every allocation owner", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const read = sql.withTransaction(
      store.readRecoveryCustodyInTransaction({
        threadId: "ordinary",
        providerSessionIds: ["allocated"],
      }),
    );
    const allocation = {
      id: "allocated-thread",
      driver: "codex",
      providerInstanceId: "provider",
      providerSessionId: "allocated",
      appThreadId: "ordinary",
      ownerNodeId: null,
      nativeThreadRef: null,
      nativeConversationHeadRef: null,
      status: "not_loaded",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      createdAt: "2026-11-01T00:00:00Z",
      updatedAt: "2026-11-01T00:00:00Z",
    };
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads (provider_thread_id, thread_id, owner_node_id, provider, driver, provider_instance_id, provider_session_id, status, updated_at, payload_json) VALUES ('allocated-thread', 'ordinary', NULL, 'provider', 'codex', 'provider', 'allocated', 'not_loaded', '2026-11-01T00:00:00Z', ${json(allocation)})`;
    assert.deepStrictEqual(yield* read, { kind: "ordinary" });
    const input = reservation(
      operationFor("allocated-owned", "allocated-owned"),
      "allocated-owned",
    );
    yield* store.reserveWorkflowNativeIntent(input);
    const owned = { ...allocation, appThreadId: input.threadId };
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads (provider_thread_id, thread_id, owner_node_id, provider, driver, provider_instance_id, provider_session_id, status, updated_at, payload_json) VALUES ('owned-allocation', ${input.threadId}, NULL, 'provider', 'codex', 'provider', 'allocated', 'not_loaded', '2026-11-01T00:00:00Z', ${json({ ...owned, id: "owned-allocation" })})`;
    const before = yield* rawReservationState;
    const referencesBefore = yield* sql`SELECT * FROM orchestration_v2_projection_provider_threads`;
    assert.deepStrictEqual(yield* read, { kind: "owned" });
    assert.deepStrictEqual(yield* rawReservationState, before);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM orchestration_v2_projection_provider_threads`,
      referencesBefore,
    );
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET status = 'active', payload_json = ${json({ ...owned, id: "owned-allocation", status: "active" })} WHERE provider_thread_id = 'owned-allocation'`;
    assert.strictEqual((yield* Effect.flip(read)).reason, "integrity");
    yield* sql`DELETE FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id = 'owned-allocation'`;
    yield* sql`INSERT INTO orchestration_v2_projection_nodes (node_id, thread_id, root_node_id, kind, status, payload_json) VALUES ('owner-node', ${input.threadId}, 'owner-node', 'agent', 'running', '{}')`;
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET thread_id = NULL, owner_node_id = 'owner-node', payload_json = ${json({ ...allocation, appThreadId: null, ownerNodeId: "owner-node" })}`;
    assert.deepStrictEqual(yield* read, { kind: "owned" });
    yield* sql`DELETE FROM orchestration_v2_projection_nodes`;
    assert.strictEqual((yield* Effect.flip(read)).reason, "integrity");
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET owner_node_id = NULL, payload_json = ${json({ ...allocation, appThreadId: null })}`;
    assert.strictEqual((yield* Effect.flip(read)).reason, "integrity");
    yield* sql`INSERT INTO orchestration_v2_projection_subagents (subagent_id, thread_id, parent_node_id, provider, provider_thread_id, origin, status, updated_at, payload_json) VALUES ('subagent', ${input.threadId}, 'parent', 'provider', 'allocated-thread', 'native', 'running', '2026-11-01T00:00:00Z', '{}')`;
    assert.deepStrictEqual(yield* read, { kind: "owned" });
    yield* sql`DELETE FROM orchestration_v2_projection_subagents`;
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET thread_id = 'ordinary', payload_json = ${json(allocation)}`;
    for (const invalid of [
      { ...allocation, status: "active" },
      {
        ...allocation,
        nativeThreadRef: { driver: "codex", nativeId: "existing", strength: "strong" },
      },
      {
        ...allocation,
        nativeConversationHeadRef: { driver: "codex", nativeId: "head", strength: "strong" },
      },
      { ...allocation, pendingBackgroundTasks: [{ kind: "command", taskId: "background" }] },
      { ...allocation, id: "mismatch" },
      { ...allocation, appThreadId: "mismatch" },
      { ...allocation, providerSessionId: "mismatch" },
      { ...allocation, driver: "claude" },
      { ...allocation, providerInstanceId: "mismatch" },
      {},
    ]) {
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET status = ${"status" in invalid ? invalid.status : "not_loaded"}, payload_json = ${json(invalid)}`;
      assert.strictEqual((yield* Effect.flip(read)).reason, "integrity");
    }
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET status = 'not_loaded', payload_json = ${json(allocation)}`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings VALUES ('allocated', 'unrelated')`;
    assert.strictEqual((yield* Effect.flip(read)).reason, "integrity");
    yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings`;
    assert.deepStrictEqual(yield* read, { kind: "ordinary" });
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "recovery custody requires its captured transaction and fails closed on invalid associations",
  () =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const store = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      const read = (threadId: string, providerSessionIds: readonly string[]) =>
        sql.withTransaction(
          store.readRecoveryCustodyInTransaction({ threadId, providerSessionIds }),
        );
      assert.strictEqual(
        (yield* Effect.flip(
          store.readRecoveryCustodyInTransaction({ threadId: "thread", providerSessionIds: [] }),
        )).reason,
        "integrity",
      );
      assert.strictEqual((yield* Effect.flip(read("", []))).reason, "integrity");
      assert.strictEqual((yield* Effect.flip(read("thread", [""]))).reason, "integrity");
      assert.strictEqual((yield* Effect.flip(read("thread", ["missing"]))).reason, "integrity");
      yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings VALUES ('missing', 'thread')`;
      assert.strictEqual((yield* Effect.flip(read("thread", []))).reason, "integrity");
      yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings`;
      yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings VALUES ('', 'thread')`;
      assert.strictEqual((yield* Effect.flip(read("thread", []))).reason, "integrity");
      yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings`;
      yield* sql`INSERT INTO orchestration_v2_projection_provider_sessions (provider_session_id, thread_id, provider, status, updated_at, payload_json) VALUES ('bad-peer', NULL, 'provider', 'running', '2026-11-01T00:00:00Z', '{}')`;
      yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings VALUES ('bad-peer', '')`;
      assert.strictEqual((yield* Effect.flip(read("thread", ["bad-peer"]))).reason, "integrity");
      yield* sql`DELETE FROM orchestration_v2_projection_provider_session_bindings`;
      yield* sql`DROP TABLE orchestration_v2_projection_provider_session_bindings`;
      const wrongClient = yield* Effect.gen(function* () {
        const other = yield* SqlClient.SqlClient;
        return yield* Effect.flip(
          other.withTransaction(
            store.readRecoveryCustodyInTransaction({ threadId: "thread", providerSessionIds: [] }),
          ),
        );
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" })));
      assert.strictEqual(wrongClient.reason, "integrity");
      assert.strictEqual((yield* Effect.flip(read("thread", []))).reason, "persistence");
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["reserved", "dispatching", "unknown", "cancelled"] as const)(
  "thread custody recognizes workflow intent in %s state and ignores unrelated malformed records",
  (state) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const store = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: "unassociated" }), {
        kind: "ordinary",
      });
      assert.strictEqual(
        (yield* Effect.flip(store.readThreadCustody({ threadId: "" }))).reason,
        "integrity",
      );
      const input = reservation(operationFor("custody-op", "custody-attempt"), "custody-attempt");
      yield* store.reserveWorkflowNativeIntent(input);
      if (state === "dispatching" || state === "unknown")
        yield* sql`UPDATE workflow_native_launch_intents SET state = ${state}, boot_id = 'boot', worker_id = 'worker' WHERE intent_id = ${input.intentId}`;
      if (state === "cancelled")
        yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled' WHERE intent_id = ${input.intentId}`;
      yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, generation, record_json) VALUES ('unrelated', ${run.id}, 'stage_launch', ${hash}, 'pending', 0, '{"destination":{"projectId":"other-project"},"parameters":{"nativeThreadId":"elsewhere"}}')`;
      assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: "unassociated" }), {
        kind: "ordinary",
      });
      assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), {
        kind: "owned",
        owner: { kind: "workflow_run", workflowRunId: run.id },
      });
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "draft",
  "column",
  "receipt",
  "result",
  "owner",
  "state",
  "generation",
  "hash",
  "receipt-id",
] as const)("selected %s tampering fails closed before workflow reservation writes", (tamper) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const first = reservation(operationFor("first-custody", "first-custody"), "first-custody");
    yield* store.reserveWorkflowNativeIntent(first);
    if (tamper === "draft")
      yield* sql`UPDATE workflow_native_launch_intents SET draft_json = '{}' WHERE intent_id = ${first.intentId}`;
    if (tamper === "column")
      yield* sql`UPDATE workflow_native_launch_intents SET message_id = 'tampered' WHERE intent_id = ${first.intentId}`;
    if (tamper === "receipt")
      yield* sql`DELETE FROM workflow_native_intent_receipts WHERE intent_id = ${first.intentId}`;
    if (tamper === "result")
      yield* sql`UPDATE workflow_native_intent_receipts SET result_json = '{}' WHERE intent_id = ${first.intentId}`;
    if (tamper === "owner")
      yield* sql`UPDATE workflow_runs SET record_json = '{}' WHERE id = ${run.id}`;
    if (tamper === "hash")
      yield* sql`UPDATE workflow_native_launch_intents SET fingerprint = ${nextHash} WHERE intent_id = ${first.intentId}`;
    if (tamper === "receipt-id")
      yield* sql`UPDATE workflow_native_intent_receipts SET receipt_id = 'substituted' WHERE intent_id = ${first.intentId}`;
    if (tamper === "generation") {
      yield* sql`PRAGMA ignore_check_constraints = ON`;
      yield* sql`UPDATE workflow_native_launch_intents SET generation = 9007199254740992 WHERE intent_id = ${first.intentId}`;
      yield* sql`PRAGMA ignore_check_constraints = OFF`;
    }
    if (tamper === "state") {
      yield* sql`PRAGMA ignore_check_constraints = ON`;
      yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled', boot_id = 'boot', worker_id = 'worker' WHERE intent_id = ${first.intentId}`;
      yield* sql`PRAGMA ignore_check_constraints = OFF`;
    }
    const before = {
      owner: yield* sql`SELECT * FROM workflow_runs`,
      intents:
        yield* sql`SELECT intent_id, workflow_run_id, planning_session_id, operation_id, attempt_id, pass_id, item_id, charge_attempt, repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, CAST(generation AS TEXT) AS generation, state, held, boot_id, worker_id, native_run_id, finalized_operation_id FROM workflow_native_launch_intents`,
      receipts: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
      operations: yield* sql`SELECT * FROM workflow_operations`,
    };
    const next = reservation(operationFor("next-custody", "next-custody"), "next-custody");
    assert.strictEqual(
      (yield* Effect.flip(store.readThreadCustody({ threadId: first.threadId }))).reason,
      "integrity",
    );
    assert.strictEqual(
      (yield* Effect.flip(store.reserveWorkflowNativeIntent(next))).reason,
      "integrity",
    );
    assert.deepStrictEqual(
      {
        owner: yield* sql`SELECT * FROM workflow_runs`,
        intents:
          yield* sql`SELECT intent_id, workflow_run_id, planning_session_id, operation_id, attempt_id, pass_id, item_id, charge_attempt, repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, CAST(generation AS TEXT) AS generation, state, held, boot_id, worker_id, native_run_id, finalized_operation_id FROM workflow_native_launch_intents`,
        receipts: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        operations: yield* sql`SELECT * FROM workflow_operations`,
      },
      before,
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("thread custody recognizes planning intents and finalized immutable links", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    yield* begin(store, session);
    const admission = admissionInput(binding, "custody-planning");
    const input = planningReservation(admission);
    yield* store.reservePlanningNativeIntent(input);
    const owned = {
      kind: "owned",
      owner: { kind: "planning_session", planningSessionId: session.id },
    } as const;
    for (const state of ["reserved", "dispatching", "unknown", "cancelled"] as const) {
      if (state === "dispatching" || state === "unknown")
        yield* sql`UPDATE workflow_native_launch_intents SET state = ${state}, boot_id = 'boot', worker_id = 'worker' WHERE intent_id = ${input.intentId}`;
      if (state === "cancelled")
        yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled', boot_id = NULL, worker_id = NULL WHERE intent_id = ${input.intentId}`;
      assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), owned);
      assert.deepStrictEqual(
        yield* sql.withTransaction(
          store.readRecoveryCustodyInTransaction({
            threadId: input.threadId,
            providerSessionIds: [],
          }),
        ),
        { kind: "owned" },
      );
    }
    const operation = admission.operation;
    const pass = admission.pass;
    yield* sql`INSERT INTO workflow_operations (id, planning_session_id, kind, fingerprint, status, generation, record_json) VALUES (${operation.id}, ${session.id}, 'planning_pass', ${operation.fingerprint}, 'pending', 0, ${json(operation)})`;
    yield* sql`INSERT INTO workflow_planning_passes VALUES (${pass.id}, ${session.id}, ${binding.planId}, ${binding.revisionId}, ${binding.revisionHash}, ${binding.sourcePacketHash}, ${operation.id}, ${input.threadId}, ${pass.native!.runId}, ${json(pass)})`;
    yield* sql`UPDATE workflow_native_launch_intents SET state = 'finalized', native_run_id = ${pass.native!.runId}, finalized_operation_id = ${operation.id} WHERE intent_id = ${input.intentId}`;
    assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), owned);
    const terminal = {
      ...operation,
      status: "cancelled",
      outcome: { kind: "cancelled", summary: "cancelled", evidence: [] },
    };
    const terminalPass = { ...pass, status: "cancelled" };
    yield* sql`UPDATE workflow_operations SET status = 'cancelled', record_json = ${json(terminal)} WHERE id = ${operation.id}`;
    yield* sql`UPDATE workflow_planning_passes SET record_json = ${json(terminalPass)} WHERE id = ${pass.id}`;
    assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), owned);
    yield* sql`UPDATE workflow_planning_passes SET native_run_id = 'substituted' WHERE id = ${pass.id}`;
    assert.strictEqual(
      (yield* Effect.flip(store.readThreadCustody({ threadId: input.threadId }))).reason,
      "integrity",
    );
    yield* sql`DELETE FROM workflow_planning_passes WHERE id = ${pass.id}`;
    assert.strictEqual(
      (yield* Effect.flip(store.readThreadCustody({ threadId: input.threadId }))).reason,
      "integrity",
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("independent owners cannot reserve the same thread on separate connections", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "workflow-thread-race-")),
        ),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "test.sqlite");
      const fileLayer = () =>
        Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* setupAdmission;
          const sql = yield* SqlClient.SqlClient;
          const other = {
            ...run,
            id: "other-run",
            access: { origins: [{ ...destination, threadId: "thread" }] },
          };
          yield* sql`INSERT INTO workflow_runs VALUES (${other.id}, 'plan', 'revision', ${hash}, ${hash}, ${hash}, 'active', 0, 0, ${json(other)})`;
        }).pipe(Effect.provide(fileLayer())),
      );
      const barrier = yield* Deferred.make<void>();
      const arrivals = yield* Ref.make(0);
      const attempt = (id: string) =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            const base = operationFor(`race-${id}`, `attempt-${id}`);
            const ownerId = id === "one" ? run.id : decodeWorkflowRunId("other-run");
            const operation = {
              ...base,
              owner: { kind: "workflow_run" as const, workflowRunId: ownerId },
            };
            const input = { ...reservation(operation, `attempt-${id}`), runId: ownerId };
            const count = yield* Ref.updateAndGet(arrivals, (value) => value + 1);
            if (count === 2) yield* Deferred.succeed(barrier, undefined);
            yield* Deferred.await(barrier);
            return yield* Effect.result(store.reserveWorkflowNativeIntent(input));
          }).pipe(Effect.provide(fileLayer())),
        );
      const results = yield* Effect.all([attempt("one"), attempt("two")], { concurrency: 2 });
      assert.strictEqual(
        results.filter((r) => r._tag === "Success" && r.success.kind === "reserved").length,
        1,
      );
      assert.strictEqual(
        results.filter(
          (r) =>
            (r._tag === "Success" &&
              r.success.kind === "rejected" &&
              r.success.reason === "custody") ||
            (r._tag === "Failure" && r.failure.reason === "persistence"),
        ).length,
        1,
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          const owners = yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`;
          assert.strictEqual(
            owners.reduce(
              (sum, row) => sum + (parse(row.record_json) as typeof run).allowance.attemptsConsumed,
              0,
            ),
            1,
          );
          assert.strictEqual((yield* sql`SELECT * FROM workflow_native_launch_intents`).length, 1);
          assert.strictEqual((yield* sql`SELECT * FROM workflow_native_intent_receipts`).length, 1);
        }).pipe(Effect.provide(fileLayer())),
      );
    }),
  ),
);

it.effect(
  "workflow legacy and finalized associations retain ownership and validate native identity",
  () =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const operation = operationFor("linked-op", "linked-attempt");
      const input = reservation(operation, "linked-attempt");
      yield* store.reserveWorkflowNativeIntent(input);
      yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, generation, record_json) VALUES (${operation.id}, ${run.id}, 'stage_launch', ${operation.fingerprint}, 'pending', 0, ${json(operation)})`;
      yield* sql`UPDATE workflow_native_launch_intents SET state = 'finalized', native_run_id = ${operation.id}, finalized_operation_id = ${operation.id} WHERE intent_id = ${input.intentId}`;
      const owned = {
        kind: "owned",
        owner: { kind: "workflow_run", workflowRunId: run.id },
      } as const;
      assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), owned);
      const other = operationFor("same-owner-op", "same-owner-attempt");
      yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, generation, record_json) VALUES (${other.id}, ${run.id}, 'stage_launch', ${other.fingerprint}, 'pending', 0, ${json(other)})`;
      if (other.parameters.kind !== "stage_launch") throw new Error("expected stage launch");
      const supervision = decodeOperation({
        ...other,
        id: "supervision-op",
        kind: "supervision",
        parameters: {
          kind: "supervision",
          context: other.parameters.context,
          incidentId: "incident",
          provider: other.parameters.provider,
          nativeThreadId: input.threadId,
          nativeRunId: "supervision-run",
        },
      });
      const supervised = {
        ...supervision,
        fingerprint: WorkflowCanonicalHash.operation(supervision),
      };
      yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, generation, record_json) VALUES (${supervised.id}, ${run.id}, 'supervision', ${supervised.fingerprint}, 'pending', 0, ${json(supervised)})`;
      assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), owned);
      yield* sql`UPDATE workflow_native_launch_intents SET native_run_id = 'wrong-native' WHERE intent_id = ${input.intentId}`;
      assert.strictEqual(
        (yield* Effect.flip(store.readThreadCustody({ threadId: input.threadId }))).reason,
        "integrity",
      );
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "both reservations fail closed for foreign, conflicting, and malformed selected owners",
  () =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      const first = reservation(operationFor("owner-op", "owner-attempt"), "owner-attempt");
      yield* store.reserveWorkflowNativeIntent(first);
      const binding = {
        planId: session.planId,
        revisionId: "revision",
        revisionSchemaVersion: 1 as const,
        revisionHash: hash,
        sourcePacketHash: hash,
      };
      const planning = {
        ...planningReservation(admissionInput(binding, "foreign-owner")),
        threadId: first.threadId,
      };
      const snapshot = () =>
        Effect.gen(function* () {
          return {
            runs: yield* sql`SELECT * FROM workflow_runs`,
            sessions: yield* sql`SELECT * FROM workflow_planning_sessions`,
            intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
            receipts: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
            operations: yield* sql`SELECT * FROM workflow_operations`,
            passes: yield* sql`SELECT * FROM workflow_planning_passes`,
          };
        });
      const before = yield* snapshot();
      assert.strictEqual(
        (yield* Effect.flip(store.reservePlanningNativeIntent(planning))).reason,
        "custody",
      );
      assert.deepStrictEqual(yield* snapshot(), before);
      const otherRun = {
        ...run,
        id: decodeWorkflowRunId("foreign-run"),
        access: { origins: [{ ...destination, threadId: "thread" }] },
      };
      yield* sql`INSERT INTO workflow_runs VALUES (${otherRun.id}, 'plan', 'revision', ${hash}, ${hash}, ${hash}, 'active', 0, 0, ${json(otherRun)})`;
      const raw = operationFor("foreign-op", "foreign-attempt");
      const operation = {
        ...raw,
        owner: { kind: "workflow_run" as const, workflowRunId: otherRun.id },
      };
      const canonical = { ...operation, fingerprint: WorkflowCanonicalHash.operation(operation) };
      yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, generation, record_json) VALUES (${canonical.id}, ${otherRun.id}, 'stage_launch', ${canonical.fingerprint}, 'pending', 0, ${json(canonical)})`;
      const conflict = yield* snapshot();
      assert.strictEqual(
        (yield* Effect.flip(store.readThreadCustody({ threadId: first.threadId }))).reason,
        "integrity",
      );
      assert.strictEqual(
        (yield* Effect.flip(store.reservePlanningNativeIntent(planning))).reason,
        "integrity",
      );
      const next = reservation(
        operationFor("next-owner-op", "next-owner-attempt"),
        "next-owner-attempt",
      );
      assert.strictEqual(
        (yield* Effect.flip(store.reserveWorkflowNativeIntent(next))).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* snapshot(), conflict);
      yield* sql`DELETE FROM workflow_operations WHERE id = ${canonical.id}`;
      yield* sql`UPDATE workflow_native_intent_receipts SET result_json = '{}' WHERE intent_id = ${first.intentId}`;
      const corrupt = yield* snapshot();
      assert.strictEqual(
        (yield* Effect.flip(store.reservePlanningNativeIntent(planning))).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* snapshot(), corrupt);
    }).pipe(Effect.provide(layer)),
);

it.effect("workflow reservation refuses a planning-owned thread and maps failed lookups", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const planning = {
      ...planningReservation(admissionInput(binding, "planning-owner")),
      threadId: "thread",
    };
    yield* store.reservePlanningNativeIntent(planning);
    const owner = {
      ...run,
      spec: { ...run.spec, plan: binding },
      access: { origins: [{ ...destination, threadId: "thread" }] },
    };
    yield* sql`INSERT INTO workflow_runs VALUES (${owner.id}, ${binding.planId}, ${binding.revisionId}, ${binding.revisionHash}, ${binding.sourcePacketHash}, ${hash}, 'active', 0, 0, ${json(owner)})`;
    const input = reservation(
      operationFor("foreign-workflow", "foreign-workflow"),
      "foreign-workflow",
    );
    const before = yield* sql`SELECT * FROM workflow_runs`;
    assert.deepStrictEqual(yield* store.reserveWorkflowNativeIntent(input), {
      kind: "rejected",
      reason: "custody",
    });
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before);
    yield* sql`DROP TABLE workflow_planning_passes`;
    assert.strictEqual(
      (yield* Effect.flip(store.readThreadCustody({ threadId: "empty" }))).reason,
      "persistence",
    );
    assert.strictEqual(
      (yield* Effect.flip(
        store.reserveWorkflowNativeIntent({
          ...input,
          threadId: "empty",
          draft: {
            ...input.draft,
            operation: {
              ...input.draft.operation,
              parameters: {
                ...input.draft.operation.parameters,
                nativeThreadId: decodeThreadId("empty"),
              },
            },
          },
        }),
      )).reason,
      "persistence",
    );
    const next = planningReservation(admissionInput(binding, "lookup-failure"));
    assert.strictEqual(
      (yield* Effect.flip(store.reservePlanningNativeIntent(next))).reason,
      "storage",
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["dispatching", "unknown"] as const)(
  "planning pause retains native %s custody",
  (state) =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      const input = planningReservation(admissionInput(binding, "native-custody"));
      yield* store.reservePlanningNativeIntent(input);
      yield* sql`UPDATE workflow_native_launch_intents SET state = ${state}, boot_id = 'boot', worker_id = 'worker' WHERE intent_id = ${input.intentId}`;
      const paused = yield* store.controlPlanning({
        sessionId: session.id,
        action: "pause",
        expectedVersion: 1,
        now: facts.now,
        context,
        command: { commandId: "pause-custody", receiptId: "pause-custody" },
      });
      assert.strictEqual(paused.value.status, state === "unknown" ? "needs_attention" : "pausing");
      assert.deepStrictEqual(yield* sql`SELECT state, held FROM workflow_native_launch_intents`, [
        { state, held: 0 },
      ]);
      const custodyBefore = yield* sql`SELECT * FROM workflow_native_launch_intents`;
      const syntheticPaused = { ...paused.value, status: "paused" };
      yield* sql`UPDATE workflow_planning_sessions SET status = 'paused', record_json = ${json(syntheticPaused)} WHERE id = ${session.id}`;
      const denied = yield* Effect.flip(
        store.controlPlanning({
          sessionId: session.id,
          action: "resume",
          expectedVersion: 2,
          now: facts.now,
          context,
          command: { commandId: "resume-custody", receiptId: "resume-custody" },
        }),
      );
      assert.strictEqual(denied.reason, "custody");
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_launch_intents`,
        custodyBefore,
      );
      yield* sql`UPDATE workflow_planning_sessions SET status = ${paused.value.status}, record_json = ${json(paused.value)} WHERE id = ${session.id}`;
      const stopped = yield* store.controlPlanning({
        sessionId: session.id,
        action: "stop",
        expectedVersion: 2,
        now: "2099-01-01T00:00:00Z",
        context,
        command: { commandId: "stop-custody", receiptId: "stop-custody" },
      });
      assert.strictEqual(stopped.value.status, "stopping");
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_launch_intents`,
        custodyBefore,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("planning retained reservation resumes at the last charged slot", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const owner = { ...session, limits: { ...session.limits, maxPasses: 1 } };
    yield* begin(store, owner);
    const input = planningReservation(admissionInput(binding, "last-slot", owner));
    yield* store.reservePlanningNativeIntent(input);
    const before = yield* sql`SELECT * FROM workflow_native_launch_intents`;
    const pause = {
      sessionId: owner.id,
      action: "pause" as const,
      expectedVersion: 1,
      now: facts.now,
      context,
      command: { commandId: "last-pause", receiptId: "last-pause" },
    };
    yield* store.controlPlanning(pause);
    const result = yield* store.controlPlanning({
      ...pause,
      action: "resume",
      expectedVersion: 2,
      command: { commandId: "last-resume", receiptId: "last-resume" },
    });
    const after = yield* sql`SELECT * FROM workflow_native_launch_intents`;
    assert.deepStrictEqual(after, [{ ...before[0], generation: result.value.controlGeneration }]);
    assert.deepStrictEqual(result.value.consumed, { passes: 1, repairRounds: 0 });
    assert.strictEqual(result.value.limits.deadline, owner.limits.deadline);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([false, true])(
  "planning stop cancels retained reservation (previously held: %s) and keeps its receipt",
  (held) =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      const admission = admissionInput(binding, "cancelled");
      const input = planningReservation(admission);
      const reserved = yield* store.reservePlanningNativeIntent(input);
      if (held)
        yield* store.controlPlanning({
          sessionId: session.id,
          action: "pause",
          expectedVersion: 1,
          now: facts.now,
          context,
          command: { commandId: "pause-before-stop", receiptId: "pause-before-stop" },
        });
      const before = yield* sql`SELECT * FROM workflow_native_launch_intents`;
      const receipt = yield* sql`SELECT * FROM workflow_native_intent_receipts`;
      const stopped = yield* store.controlPlanning({
        sessionId: session.id,
        action: "stop",
        expectedVersion: held ? 2 : 1,
        now: facts.now,
        context,
        command: { commandId: "stop-reserved", receiptId: "stop-reserved" },
      });
      assert.strictEqual(stopped.value.status, "stopped");
      assert.deepStrictEqual(stopped.value.consumed, { passes: 1, repairRounds: 0 });
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, [
        { ...before[0], state: "cancelled", held: 0 },
      ]);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, receipt);
      assert.deepStrictEqual(
        yield* store.reservePlanningNativeIntent({ ...input, now: "2099-01-01T00:00:00Z" }),
        { ...reserved, kind: "replay" },
      );
      assert.strictEqual(
        (yield* Effect.flip(store.admitPlanningPass(admission))).reason,
        "conflict",
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["draft", "fingerprint", "receipt", "custody", "generation"] as const)(
  "planning control rejects tampered %s before writes",
  (tamper) =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      const input = planningReservation(admissionInput(binding, "tamper"));
      yield* store.reservePlanningNativeIntent(input);
      if (tamper === "draft")
        yield* sql`UPDATE workflow_native_launch_intents SET draft_json = json_set(draft_json, '$.pass.inputManifest.sources[0].sha256', ${hash})`;
      if (tamper === "fingerprint")
        yield* sql`UPDATE workflow_native_launch_intents SET fingerprint = ${hash}`;
      if (tamper === "receipt")
        yield* sql`UPDATE workflow_native_intent_receipts SET result_json = '{}'`;
      if (tamper === "custody")
        yield* sql`UPDATE workflow_native_launch_intents SET boot_id = 'boot', worker_id = 'worker'`;
      if (tamper === "generation")
        yield* sql`UPDATE workflow_native_launch_intents SET generation = generation + 1`;
      const before = {
        owner: yield* sql`SELECT * FROM workflow_planning_sessions`,
        intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
        receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
      };
      const error = yield* Effect.flip(
        store.controlPlanning({
          sessionId: session.id,
          action: "pause",
          expectedVersion: 1,
          now: facts.now,
          context,
          command: { commandId: "tamper-pause", receiptId: "tamper-pause" },
        }),
      );
      assert.strictEqual(error.reason, "integrity");
      assert.deepStrictEqual(
        {
          owner: yield* sql`SELECT * FROM workflow_planning_sessions`,
          intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
          receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
        },
        before,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "provider",
  "role",
  "revision",
  "writer",
  "unauthorized",
  "stale",
  "unheld",
  "uncharged",
  "generation",
] as const)("planning rearm rejects %s without writes", (condition) =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const input = planningReservation(admissionInput(binding, "rearm-reject"));
    yield* store.reservePlanningNativeIntent(input);
    const pause = {
      sessionId: session.id,
      action: "pause" as const,
      expectedVersion: 1,
      now: facts.now,
      context,
      command: { commandId: "reject-pause", receiptId: "reject-pause" },
    };
    const paused = yield* store.controlPlanning(pause);
    if (condition === "provider" || condition === "role") {
      const altered =
        condition === "provider"
          ? {
              ...paused.value,
              allowedProviders: [{ providerInstanceId: "other", model: "other" }],
            }
          : { ...paused.value, allowedRoles: ["review"] };
      yield* sql`UPDATE workflow_planning_sessions SET record_json = ${json(altered)} WHERE id = ${session.id}`;
    }
    if (condition === "unheld") yield* sql`UPDATE workflow_native_launch_intents SET held = 0`;
    if (condition === "uncharged")
      yield* sql`UPDATE workflow_planning_sessions SET record_json = ${json({ ...paused.value, consumed: { passes: 0, repairRounds: 0 } })} WHERE id = ${session.id}`;
    if (condition === "revision")
      yield* sql`UPDATE development_plan_source_packets SET packet_json = '{}'`;
    if (condition === "writer") {
      const admission = admissionInput(binding, "competing");
      yield* sql`INSERT INTO workflow_operations (id, planning_session_id, kind, fingerprint, status, generation, record_json) VALUES (${admission.operation.id}, ${session.id}, 'planning_pass', ${admission.operation.fingerprint}, 'pending', 0, ${json(admission.operation)})`;
      yield* sql`INSERT INTO workflow_planning_passes (id, planning_session_id, plan_id, revision_id, revision_hash, source_packet_hash, operation_id, record_json) VALUES (${admission.pass.id}, ${session.id}, ${session.planId}, ${binding.revisionId}, ${binding.revisionHash}, ${binding.sourcePacketHash}, ${admission.operation.id}, ${json(admission.pass)})`;
    }
    if (condition === "generation")
      yield* sql`UPDATE workflow_native_launch_intents SET generation = ${paused.value.controlGeneration}`;
    const before = {
      owner: yield* sql`SELECT * FROM workflow_planning_sessions`,
      intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
      receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
    };
    const error = yield* Effect.flip(
      store.controlPlanning({
        ...pause,
        action: "resume",
        expectedVersion: condition === "stale" ? 1 : 2,
        context:
          condition === "unauthorized"
            ? { ...context, actor: decodeActorFixture({ ...context.actor, id: "other" }) }
            : context,
        command: { commandId: "reject-resume", receiptId: "reject-resume" },
      }),
    );
    assert.strictEqual(
      error.reason,
      condition === "writer"
        ? "custody"
        : condition === "unauthorized"
          ? "unauthorized"
          : condition === "stale"
            ? "conflict"
            : condition === "revision" ||
                condition === "unheld" ||
                condition === "uncharged" ||
                condition === "generation"
              ? "integrity"
              : "ineligible",
    );
    assert.deepStrictEqual(
      {
        owner: yield* sql`SELECT * FROM workflow_planning_sessions`,
        intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
        receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
      },
      before,
    );
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["receipt", "intent-cas", "owner-cas"] as const)(
  "planning control rolls back %s and publishes only after commit",
  (fault) =>
    Effect.scoped(
      Effect.gen(function* () {
        const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
        const sql = yield* SqlClient.SqlClient;
        const store = yield* Store.WorkflowRunStore;
        yield* begin(store, session);
        yield* store.reservePlanningNativeIntent(
          planningReservation(admissionInput(binding, "fault")),
        );
        const subscription = yield* store.subscribeChanges;
        if (fault === "receipt")
          yield* sql`CREATE TRIGGER fail_control BEFORE INSERT ON workflow_command_receipts BEGIN SELECT RAISE(ABORT, 'fault'); END`;
        if (fault === "intent-cas")
          yield* sql`CREATE TRIGGER fail_control BEFORE UPDATE ON workflow_native_launch_intents BEGIN SELECT RAISE(IGNORE); END`;
        if (fault === "owner-cas")
          yield* sql`CREATE TRIGGER fail_control BEFORE UPDATE ON workflow_planning_sessions BEGIN SELECT RAISE(IGNORE); END`;
        const before = {
          owner: yield* sql`SELECT * FROM workflow_planning_sessions`,
          intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
          receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
        };
        const input = {
          sessionId: session.id,
          action: "pause" as const,
          expectedVersion: 1,
          now: facts.now,
          context,
          command: { commandId: "fault-pause", receiptId: "fault-pause" },
        };
        assert.strictEqual(
          (yield* Effect.flip(store.controlPlanning(input))).reason,
          fault === "receipt" ? "storage" : fault === "intent-cas" ? "integrity" : "conflict",
        );
        assert.deepStrictEqual(
          {
            owner: yield* sql`SELECT * FROM workflow_planning_sessions`,
            intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
            receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
          },
          before,
        );
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        yield* sql`DROP TRIGGER fail_control`;
        const applied = yield* store.controlPlanning(input);
        yield* PubSub.take(subscription);
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        assert.deepStrictEqual(yield* store.controlPlanning(input), { ...applied, kind: "replay" });
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      }),
    ).pipe(Effect.provide(layer)),
);

it.effect("planning review and writer reservations coexist through pause and resume", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const owner = { ...session, allowedRoles: ["author", "review"] as const };
    yield* begin(store, owner);
    yield* store.reservePlanningNativeIntent(
      planningReservation(admissionInput(binding, "coexist-writer", owner)),
    );
    const next = admissionInput(binding, "coexist-review", owner);
    if (next.operation.parameters.kind !== "planning_pass")
      throw new Error("expected planning pass");
    yield* store.reservePlanningNativeIntent(
      planningReservation({
        ...next,
        expectedVersion: 1,
        pass: { ...next.pass, role: "review" },
        operation: {
          ...next.operation,
          parameters: { ...next.operation.parameters, role: "review" },
        },
      }),
    );
    const pause = {
      sessionId: owner.id,
      action: "pause" as const,
      expectedVersion: 2,
      now: facts.now,
      context,
      command: { commandId: "coexist-pause", receiptId: "coexist-pause" },
    };
    yield* store.controlPlanning(pause);
    assert.deepStrictEqual(
      yield* sql`SELECT held FROM workflow_native_launch_intents ORDER BY role`,
      [{ held: 1 }, { held: 1 }],
    );
    const resumed = yield* store.controlPlanning({
      ...pause,
      action: "resume",
      expectedVersion: 3,
      command: { commandId: "coexist-resume", receiptId: "coexist-resume" },
    });
    assert.strictEqual(resumed.value.status, "active");
    assert.deepStrictEqual(resumed.value.consumed, { passes: 2, repairRounds: 0 });
    assert.deepStrictEqual(
      yield* sql`SELECT held, generation FROM workflow_native_launch_intents ORDER BY role`,
      [
        { held: 0, generation: 2 },
        { held: 0, generation: 2 },
      ],
    );
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["pause", "stop"] as const)(
  "planning %s preserves control when retained eligibility changes",
  (action) =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      yield* store.reservePlanningNativeIntent(
        planningReservation(admissionInput(binding, "eligibility-control")),
      );
      const current = yield* store.getPlanning({ sessionId: session.id });
      const changed = {
        ...current,
        allowedRoles: ["review"],
        allowedProviders: [{ providerInstanceId: "other", model: "other" }],
      };
      yield* sql`UPDATE workflow_planning_sessions SET record_json = ${json(changed)} WHERE id = ${session.id}`;
      yield* sql`UPDATE development_plan_source_packets SET packet_json = '{}'`;
      const result = yield* store.controlPlanning({
        sessionId: session.id,
        action,
        expectedVersion: 1,
        now: "2099-01-01T00:00:00Z",
        context,
        command: { commandId: "changed-control", receiptId: "changed-control" },
      });
      assert.strictEqual(result.value.status, action === "pause" ? "paused" : "stopped");
      assert.deepStrictEqual(yield* sql`SELECT state, held FROM workflow_native_launch_intents`, [
        action === "pause" ? { state: "reserved", held: 1 } : { state: "cancelled", held: 0 },
      ]);
    }).pipe(Effect.provide(layer)),
);

it.effect("cancelled planning reservation replays after reopening the database", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "planning-retained-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "test.sqlite");
      const fileLayer = Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
      const saved = yield* Effect.scoped(
        Effect.gen(function* () {
          const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
          const store = yield* Store.WorkflowRunStore;
          yield* begin(store, session);
          const admission = admissionInput(binding, "reopen-cancel");
          const input = planningReservation(admission);
          const reserved = yield* store.reservePlanningNativeIntent(input);
          yield* store.controlPlanning({
            sessionId: session.id,
            action: "stop",
            expectedVersion: 1,
            now: facts.now,
            context,
            command: { commandId: "reopen-stop", receiptId: "reopen-stop" },
          });
          return { admission, input, reserved };
        }).pipe(Effect.provide(fileLayer)),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* Store.WorkflowRunStore;
          const sql = yield* SqlClient.SqlClient;
          assert.deepStrictEqual(
            yield* store.reservePlanningNativeIntent({
              ...saved.input,
              now: "2099-01-01T00:00:00Z",
            }),
            { ...saved.reserved, kind: "replay" },
          );
          assert.strictEqual(
            (yield* Effect.flip(store.admitPlanningPass(saved.admission))).reason,
            "conflict",
          );
          assert.deepStrictEqual(
            yield* sql`SELECT state, held FROM workflow_native_launch_intents`,
            [{ state: "cancelled", held: 0 }],
          );
          assert.strictEqual(
            (yield* store.getPlanning({ sessionId: session.id })).consumed.passes,
            1,
          );
        }).pipe(Effect.provide(fileLayer)),
      );
    }),
  ),
);

it.effect("planning reservation charges once and replays after its deadline", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const input = planningReservation(admissionInput(binding, "intent"));
    const first = yield* store.reservePlanningNativeIntent(input);
    assert.strictEqual(first.kind, "reserved");
    assert.strictEqual(
      (yield* Effect.flip(
        store.reservePlanningNativeIntent({
          ...input,
          draft: {
            ...input.draft,
            admission: {
              ...input.draft.admission,
              command: { ...input.draft.admission.command, receiptId: "changed-receipt" },
            },
          },
        }),
      )).reason,
      "conflict",
    );
    assert.strictEqual((yield* sql`SELECT * FROM workflow_native_intent_receipts`).length, 1);
    yield* store.controlPlanning({
      sessionId: session.id,
      action: "pause",
      expectedVersion: 1,
      now: facts.now,
      context,
      command: { commandId: "pause-intent", receiptId: "pause-intent-receipt" },
    });
    const held =
      yield* sql`SELECT held, generation, draft_json, fingerprint FROM workflow_native_launch_intents`;
    assert.strictEqual(held[0]!.held, 1);
    const resumed = yield* store.controlPlanning({
      sessionId: session.id,
      action: "resume",
      expectedVersion: 2,
      now: facts.now,
      context,
      command: { commandId: "resume-intent", receiptId: "resume-intent-receipt" },
    });
    const rearmed =
      yield* sql`SELECT held, generation, draft_json, fingerprint FROM workflow_native_launch_intents`;
    assert.deepStrictEqual(rearmed, [
      { ...held[0], held: 0, generation: resumed.value.controlGeneration },
    ]);
    assert.deepStrictEqual(resumed.value.consumed, { passes: 1, repairRounds: 0 });
    assert.strictEqual(resumed.value.limits.deadline, session.limits.deadline);
    const replay = yield* store.reservePlanningNativeIntent({
      ...input,
      generation: 1,
      now: "2099-01-01T00:00:00Z",
    });
    assert.deepStrictEqual(replay, { ...first, kind: "replay" });
    const current = yield* store.getPlanning({ sessionId: session.id });
    assert.strictEqual(current.consumed.passes, 1);
    assert.strictEqual(current.consumed.repairRounds, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_planning_passes`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_operations`).length, 0);
    const forged = {
      ...input,
      draft: {
        ...input.draft,
        pass: { ...input.draft.pass, native: { threadId: input.threadId, runId: "forged" } },
      },
    };
    assert.strictEqual(
      (yield* Effect.flip(store.reservePlanningNativeIntent(forged))).reason,
      "integrity",
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("finalized planning intent preserves exact V1 receipt replay", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const input = admissionInput(binding, "finalized-replay");
    const first = yield* store.admitPlanningPass(input);
    yield* sql`INSERT INTO workflow_native_launch_intents (intent_id, planning_session_id, operation_id, pass_id, charge_attempt, repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, generation, state, native_run_id, finalized_operation_id) VALUES ('finalized-intent', ${session.id}, ${input.operation.id}, ${input.pass.id}, 0, 0, 'author', ${session.environmentId}, ${session.projectId}, 'finalized-launch', ${input.pass.native!.threadId}, 'finalized-message', ${hash}, '{}', 0, 'finalized', ${input.pass.native!.runId}, ${input.operation.id})`;
    const before = yield* sql`SELECT * FROM workflow_planning_sessions`;
    assert.deepStrictEqual(yield* store.admitPlanningPass(input), { ...first, kind: "replay" });
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before);
  }).pipe(Effect.provide(layer)),
);

it.effect("a cancelled planning intent identity cannot be charged by legacy admission", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const admission = admissionInput(binding, "cancelled-intent");
    const input = planningReservation(admission);
    yield* store.reservePlanningNativeIntent(input);
    yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled' WHERE intent_id = ${input.intentId}`;
    const before = yield* sql`SELECT record_json FROM workflow_planning_sessions`;
    const result = yield* Effect.result(
      store.admitPlanningPass({
        ...admission,
        expectedVersion: 1,
        command: { commandId: "legacy-after-cancel", receiptId: "legacy-after-cancel" },
      }),
    );
    assert.strictEqual(result._tag, "Failure");
    assert.deepStrictEqual(yield* sql`SELECT record_json FROM workflow_planning_sessions`, before);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["author", "review", "repair", "readback"] as const)(
  "planning reservation admits eligible %s and charges its allowance",
  (role) =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const store = yield* Store.WorkflowRunStore;
      const owner = { ...session, allowedRoles: [role] };
      yield* begin(store, owner);
      const base = admissionInput(binding, role, owner);
      const pass = { ...base.pass, role };
      if (base.operation.parameters.kind !== "planning_pass") return;
      const operation = { ...base.operation, parameters: { ...base.operation.parameters, role } };
      const input = planningReservation({ ...base, pass, operation });
      assert.strictEqual((yield* store.reservePlanningNativeIntent(input)).kind, "reserved");
      const current = yield* store.getPlanning({ sessionId: owner.id });
      assert.deepStrictEqual(current.consumed, {
        passes: 1,
        repairRounds: role === "repair" ? 1 : 0,
      });
    }).pipe(Effect.provide(layer)),
);

it.effect("planning reservation enforces plan-wide writer custody across sessions", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const second = decodeSessionFixture({
      ...session,
      id: "second-session",
      predecessorSessionId: session.id,
      allowedRoles: ["author", "review"],
    });
    yield* begin(store, session);
    const firstIntent = planningReservation(admissionInput(binding, "first-writer"));
    yield* store.reservePlanningNativeIntent(firstIntent);
    const first = yield* store.getPlanning({ sessionId: session.id });
    yield* sql`UPDATE workflow_planning_sessions SET status = 'stopped', record_json = ${json({ ...first, status: "stopped" })} WHERE id = ${session.id}`;
    yield* begin(store, second);
    const next = planningReservation(admissionInput(binding, "second-writer", second));
    assert.strictEqual(
      (yield* Effect.flip(store.reservePlanningNativeIntent(next))).reason,
      "custody",
    );
    assert.strictEqual(
      (yield* Effect.flip(
        store.admitPlanningPass(admissionInput(binding, "legacy-writer", second)),
      )).reason,
      "custody",
    );
    const review = admissionInput(binding, "foreign-thread", second);
    if (review.operation.parameters.kind !== "planning_pass") return;
    const reviewInput = planningReservation({
      ...review,
      pass: { ...review.pass, role: "review" },
      operation: {
        ...review.operation,
        parameters: { ...review.operation.parameters, role: "review" },
      },
    });
    assert.strictEqual(
      (yield* Effect.flip(
        store.reservePlanningNativeIntent({ ...reviewInput, threadId: firstIntent.threadId }),
      )).reason,
      "custody",
    );
    yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled' WHERE intent_id = ${firstIntent.intentId}`;
    assert.strictEqual(
      (yield* Effect.flip(
        store.reservePlanningNativeIntent({ ...reviewInput, threadId: firstIntent.threadId }),
      )).reason,
      "custody",
    );
    assert.strictEqual((yield* store.getPlanning({ sessionId: second.id })).consumed.passes, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["operation", "pass", "command", "receipt", "launch", "message"] as const)(
  "planning reservation rejects global %s collision without charging",
  (collision) =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      const first = planningReservation(admissionInput(binding, "collision-one"));
      yield* store.reservePlanningNativeIntent(first);
      const second = planningReservation(admissionInput(binding, "collision-two"));
      const input = {
        ...second,
        operationId: collision === "operation" ? first.operationId : second.operationId,
        passId: collision === "pass" ? first.passId : second.passId,
        launchCommandId: collision === "launch" ? first.launchCommandId : second.launchCommandId,
        threadId: collision === "message" ? first.threadId : second.threadId,
        messageId: collision === "message" ? first.messageId : second.messageId,
        draft: {
          ...second.draft,
          operation:
            collision === "operation"
              ? { ...second.draft.operation, id: first.operationId }
              : second.draft.operation,
          pass: {
            ...second.draft.pass,
            ...(collision === "pass" ? { id: first.passId } : {}),
            ...(collision === "operation" ? { operationId: first.operationId } : {}),
          },
          admission: {
            ...second.draft.admission,
            expectedVersion: 1,
            command: {
              commandId:
                collision === "command"
                  ? first.draft.admission.command.commandId
                  : second.draft.admission.command.commandId,
              receiptId:
                collision === "receipt"
                  ? first.draft.admission.command.receiptId
                  : second.draft.admission.command.receiptId,
            },
          },
        },
      };
      if (input.draft.operation.parameters.kind !== "planning_pass") return;
      const adjusted = {
        ...input,
        draft: {
          ...input.draft,
          operation: {
            ...input.draft.operation,
            parameters: { ...input.draft.operation.parameters, passId: input.passId },
          },
        },
      };
      assert.strictEqual(
        (yield* Effect.flip(store.reservePlanningNativeIntent(adjusted))).reason,
        "conflict",
      );
      assert.strictEqual((yield* store.getPlanning({ sessionId: session.id })).consumed.passes, 1);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "deadline",
  "generation",
  "version",
  "provider",
  "manifest",
  "status",
  "claim",
] as const)("planning reservation rejects %s before charging", (scenario) =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    yield* begin(store, session);
    const base = planningReservation(admissionInput(binding, "invalid"));
    if (base.draft.operation.parameters.kind !== "planning_pass") return;
    const operation = {
      ...base.draft.operation,
      ...(scenario === "status" ? { status: "claimed" } : {}),
      ...(scenario === "claim" ? { claim: { bootId: "forged", workerId: "forged" } } : {}),
      parameters: {
        ...base.draft.operation.parameters,
        ...(scenario === "manifest" ? { inputManifestHash: hash } : {}),
        ...(scenario === "provider"
          ? { provider: { ...base.draft.pass.provider, model: "other" } }
          : {}),
      },
    };
    const input = {
      ...base,
      now: scenario === "deadline" ? session.limits.deadline : base.now,
      generation: scenario === "generation" ? 1 : base.generation,
      draft: {
        ...base.draft,
        operation,
        admission: { ...base.draft.admission, expectedVersion: scenario === "version" ? 1 : 0 },
      },
    };
    const reason =
      scenario === "deadline"
        ? "deadline"
        : scenario === "version"
          ? "conflict"
          : scenario === "claim"
            ? "invalid"
            : "integrity";
    assert.strictEqual(
      (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
      reason,
    );
    assert.strictEqual((yield* store.getPlanning({ sessionId: session.id })).consumed.passes, 0);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
  }).pipe(Effect.provide(layer)),
);

it.effect("legacy planning admits valid same-owner history on its native thread", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    const owner = { ...session, allowedRoles: ["author", "review"] as const };
    yield* begin(store, owner);
    const first = admissionInput(binding, "same-owner-first", owner);
    yield* store.admitPlanningPass(first);
    const next = admissionInput(binding, "same-owner-review", owner);
    if (next.operation.parameters.kind !== "planning_pass")
      throw new Error("expected planning pass");
    const operation = {
      ...next.operation,
      parameters: { ...next.operation.parameters, role: "review" as const },
    };
    const review = {
      ...next,
      expectedVersion: 1,
      pass: {
        ...next.pass,
        role: "review" as const,
        native: { ...next.pass.native!, threadId: first.pass.native!.threadId },
      },
      operation: { ...operation, fingerprint: WorkflowCanonicalHash.operation(operation) },
    };
    const result = yield* store.admitPlanningPass(review);
    assert.strictEqual(result.kind, "applied");
    assert.strictEqual(result.value.session.consumed.passes, 2);
  }).pipe(Effect.provide(layer)),
);

it.effect("planning reservation rejects an ambiguous legacy thread association", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const owner = { ...session, allowedRoles: ["author", "review"] as const };
    yield* begin(store, owner);
    const legacy = admissionInput(binding, "legacy-thread", owner);
    yield* store.admitPlanningPass(legacy);
    assert.deepStrictEqual(
      yield* store.readThreadCustody({ threadId: legacy.pass.native!.threadId }),
      {
        kind: "owned",
        owner: { kind: "planning_session", planningSessionId: session.id },
      },
    );
    yield* sql`UPDATE workflow_planning_passes SET native_run_id = 'different-run' WHERE id = ${legacy.pass.id}`;
    const next = admissionInput(binding, "new-review", owner);
    if (next.operation.parameters.kind !== "planning_pass") return;
    const review = planningReservation({
      ...next,
      pass: { ...next.pass, role: "review" },
      operation: {
        ...next.operation,
        parameters: { ...next.operation.parameters, role: "review" },
      },
    });
    const input = {
      ...review,
      threadId: legacy.pass.native!.threadId,
      draft: { ...review.draft, admission: { ...review.draft.admission, expectedVersion: 1 } },
    };
    assert.strictEqual(
      (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
      "integrity",
    );
    assert.strictEqual((yield* store.getPlanning({ sessionId: owner.id })).consumed.passes, 1);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "mixed actor",
  "unauthorized actor",
  "exhausted passes",
  "tampered revision",
] as const)("planning reservation rejects %s without writing", (scenario) =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    yield* begin(store, session);
    const base = planningReservation(admissionInput(binding, "eligibility"));
    if (scenario === "exhausted passes") {
      const exhausted = {
        ...session,
        consumed: { ...session.consumed, passes: session.limits.maxPasses },
      };
      yield* sql`UPDATE workflow_planning_sessions SET record_json = ${json(exhausted)} WHERE id = ${session.id}`;
    }
    if (scenario === "tampered revision")
      yield* sql`UPDATE development_plan_revisions SET revision_json = '{}' WHERE plan_id = ${session.planId}`;
    const actor = decodeActorFixture({ ...context.actor, id: "other-actor" });
    const input = {
      ...base,
      draft: {
        ...base.draft,
        operation:
          scenario === "unauthorized actor"
            ? { ...base.draft.operation, origin: actor }
            : base.draft.operation,
        admission: {
          ...base.draft.admission,
          context:
            scenario === "mixed actor" || scenario === "unauthorized actor"
              ? { ...context, actor }
              : context,
        },
      },
    };
    const before = yield* sql`SELECT * FROM workflow_planning_sessions`;
    const expected =
      scenario === "unauthorized actor"
        ? "unauthorized"
        : scenario === "exhausted passes"
          ? "limit"
          : "integrity";
    assert.strictEqual(
      (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
      expected,
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["reservation", "legacy"] as const)(
  "rejects planning version overflow through %s without writes",
  (path) =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const store = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      yield* begin(store, session);
      const maxVersion = { ...session, recordVersion: Number.MAX_SAFE_INTEGER };
      yield* sql`UPDATE workflow_planning_sessions SET record_version = ${Number.MAX_SAFE_INTEGER}, record_json = ${json(maxVersion)} WHERE id = ${session.id}`;
      const admission = {
        ...admissionInput(binding, "max-version"),
        expectedVersion: Number.MAX_SAFE_INTEGER,
      };
      const before = yield* sql`SELECT * FROM workflow_planning_sessions`;
      const receiptsBefore = yield* sql`SELECT * FROM workflow_command_receipts`;
      const error =
        path === "reservation"
          ? yield* Effect.flip(store.reservePlanningNativeIntent(planningReservation(admission)))
          : yield* Effect.flip(store.admitPlanningPass(admission));
      assert.strictEqual(error.reason, "integrity");
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_passes`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_command_receipts`, receiptsBefore);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, []);
    }).pipe(Effect.provide(layer)),
);

it.effect("planning reservation receipt failure rolls back the charge and intent", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    yield* begin(store, session);
    yield* sql`CREATE TRIGGER reject_planning_intent_receipt BEFORE INSERT ON workflow_native_intent_receipts BEGIN SELECT RAISE(ABORT, 'failure'); END`;
    const input = planningReservation(admissionInput(binding, "rollback"));
    assert.strictEqual(
      (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
      "storage",
    );
    assert.strictEqual((yield* store.getPlanning({ sessionId: session.id })).consumed.passes, 0);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, []);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "planning reservations compete for the last slot across connections and replay on reopen",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "planning-reserve-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "test.sqlite");
        const fileLayer = () =>
          Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
        const limited = { ...session, limits: { ...session.limits, maxPasses: 1 } };
        const binding = yield* Effect.scoped(
          Effect.gen(function* () {
            const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
            yield* begin(yield* Store.WorkflowRunStore, limited);
            return binding;
          }).pipe(Effect.provide(fileLayer())),
        );
        const barrier = yield* Deferred.make<void>();
        const arrivals = yield* Ref.make(0);
        const attempt = (id: string) =>
          Effect.scoped(
            Effect.gen(function* () {
              const store = yield* Store.WorkflowRunStore;
              assert.strictEqual(
                (yield* store.getPlanning({ sessionId: limited.id })).consumed.passes,
                0,
              );
              if ((yield* Ref.updateAndGet(arrivals, (n) => n + 1)) === 2)
                yield* Deferred.succeed(barrier, undefined);
              yield* Deferred.await(barrier);
              const input = planningReservation(admissionInput(binding, id, limited));
              return {
                input,
                result: yield* Effect.result(store.reservePlanningNativeIntent(input)),
              };
            }).pipe(Effect.provide(fileLayer())),
          );
        const results = yield* Effect.all([attempt("one"), attempt("two")], { concurrency: 2 });
        const winner = results.find((r) => r.result._tag === "Success");
        const loser = results.find((r) => r.result._tag === "Failure");
        assert.isDefined(winner);
        assert.isDefined(loser);
        if (
          !winner ||
          winner.result._tag !== "Success" ||
          !loser ||
          loser.result._tag !== "Failure"
        )
          return;
        assert.include(["conflict", "limit", "storage"], loser.result.failure.reason);
        const won = winner.result.success;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            assert.deepStrictEqual(
              yield* store.reservePlanningNativeIntent({
                ...winner.input,
                now: "2099-01-01T00:00:00Z",
              }),
              { ...won, kind: "replay" },
            );
            assert.strictEqual(
              (yield* store.getPlanning({ sessionId: limited.id })).consumed.passes,
              1,
            );
          }).pipe(Effect.provide(fileLayer())),
        );
      }),
    ),
);

it.effect("planning admission fails closed for an outstanding intent", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const input = admissionInput(binding, "reserved");
    yield* sql`INSERT INTO workflow_native_launch_intents (intent_id, planning_session_id, operation_id, pass_id, charge_attempt, repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, generation, state) VALUES ('planning-intent', ${session.id}, ${input.operation.id}, ${input.pass.id}, 0, 0, 'author', 'environment', 'project', 'launch', 'thread', 'message', ${hash}, '{}', 0, 'reserved')`;
    yield* sql`INSERT INTO workflow_native_intent_receipts VALUES (${input.command.commandId}, 'reserved-receipt', 'planning-intent', ${hash}, '{}')`;
    const before = yield* sql`SELECT * FROM workflow_planning_sessions`;
    assert.strictEqual((yield* Effect.flip(store.admitPlanningPass(input))).reason, "conflict");
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_planning_passes`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM workflow_operations`).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["bad timestamp", "invalid"],
  ["bad command", "invalid"],
  ["owner mismatch", "integrity"],
  ["role mismatch", "integrity"],
  ["provider mismatch", "integrity"],
  ["operation limits mismatch", "integrity"],
  ["manifest hash mismatch", "integrity"],
  ["wrong actor", "unauthorized"],
  ["wrong destination", "unauthorized"],
  ["stale version", "conflict"],
  ["inactive session", "ineligible"],
  ["deadline", "deadline"],
  ["after deadline", "deadline"],
  ["disallowed role", "ineligible"],
  ["disallowed provider", "ineligible"],
  ["passes exhausted", "limit"],
  ["repair rounds exhausted", "limit"],
  ["pending writer", "custody"],
  ["claimed writer", "custody"],
  ["unknown writer", "custody"],
  ["missing revision", "integrity"],
  ["tampered revision", "integrity"],
  ["tampered source packet", "integrity"],
  ["changed command fingerprint", "conflict"],
] as const)("planning admission rejects %s without writes", ([scenario, reason]) =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const repair = scenario === "disallowed role" || scenario === "repair rounds exhausted";
    const owner = {
      ...session,
      allowedRoles:
        scenario === "repair rounds exhausted"
          ? (["author", "repair"] as const)
          : session.allowedRoles,
      consumed: {
        passes:
          scenario === "passes exhausted"
            ? session.limits.maxPasses
            : scenario === "repair rounds exhausted"
              ? 1
              : 0,
        repairRounds: scenario === "repair rounds exhausted" ? session.limits.maxRepairRounds : 0,
      },
    };
    yield* begin(store, { ...owner, consumed: session.consumed });
    if (scenario === "passes exhausted" || scenario === "repair rounds exhausted")
      yield* sql`UPDATE workflow_planning_sessions SET record_json = ${json(owner)} WHERE id = ${owner.id}`;
    let input = admissionInput(binding, "test", owner);
    if (repair || scenario === "disallowed provider") {
      const pass = decodePassFixture({
        ...input.pass,
        ...(repair
          ? { role: "repair" }
          : { provider: { providerInstanceId: "other", model: "model" } }),
      });
      if (input.operation.parameters.kind !== "planning_pass")
        throw new Error("expected planning pass");
      const operation = decodeOperationFixture({
        ...input.operation,
        parameters: { ...input.operation.parameters, role: pass.role, provider: pass.provider },
      });
      input = {
        ...input,
        pass,
        operation: { ...operation, fingerprint: WorkflowCanonicalHash.operation(operation) },
      };
    }
    if (scenario === "inactive session") {
      yield* sql`UPDATE workflow_planning_sessions SET status = 'paused', record_json = ${json({ ...owner, status: "paused" })} WHERE id = ${owner.id}`;
    }
    if (scenario === "missing revision") {
      yield* sql`UPDATE development_plans SET current_revision_id = NULL WHERE plan_id = 'plan'`;
      yield* sql`DELETE FROM development_plan_revisions WHERE plan_id = 'plan'`;
    }
    if (scenario === "tampered revision")
      yield* sql`UPDATE development_plan_revisions SET revision_json = '{}' WHERE plan_id = 'plan'`;
    if (scenario === "tampered source packet")
      yield* sql`UPDATE development_plan_source_packets SET packet_json = '{}' WHERE plan_id = 'plan'`;
    if (
      scenario === "pending writer" ||
      scenario === "claimed writer" ||
      scenario === "unknown writer" ||
      scenario === "changed command fingerprint"
    ) {
      const first = admissionInput(binding, "first", owner);
      yield* store.admitPlanningPass(first);
      if (scenario === "changed command fingerprint") input = { ...input, command: first.command };
      else {
        const status =
          scenario === "claimed writer"
            ? "claimed"
            : scenario === "unknown writer"
              ? "unknown"
              : "pending";
        yield* sql`UPDATE workflow_operations SET status = ${status} WHERE id = ${first.operation.id}`;
        input = { ...input, expectedVersion: 1 };
      }
    }
    if (scenario === "bad timestamp") input = { ...input, now: "bad" };
    if (scenario === "bad command")
      input = { ...input, command: { ...input.command, commandId: "" } };
    if (scenario === "stale version") input = { ...input, expectedVersion: 1 };
    if (scenario === "deadline" || scenario === "after deadline")
      input = {
        ...input,
        now: scenario === "deadline" ? owner.limits.deadline : "2026-12-02T00:00:00Z",
      };
    if (scenario === "wrong actor" || scenario === "wrong destination") {
      const changedContext =
        scenario === "wrong actor"
          ? { ...context, actor: decodeActorFixture({ ...context.actor, id: "other" }) }
          : { ...context, destination: { ...context.destination, projectId: "other" } };
      const operation = decodeOperationFixture({
        ...input.operation,
        origin: changedContext.actor,
        destination: changedContext.destination,
      });
      input = {
        ...input,
        context: { actor: operation.origin, destination: operation.destination },
        operation: { ...operation, fingerprint: WorkflowCanonicalHash.operation(operation) },
      };
    }
    if (
      [
        "owner mismatch",
        "role mismatch",
        "provider mismatch",
        "operation limits mismatch",
        "manifest hash mismatch",
      ].includes(scenario)
    ) {
      const params = input.operation.parameters;
      const operation = decodeOperationFixture({
        ...input.operation,
        ...(scenario === "owner mismatch"
          ? { owner: { kind: "planning_session", planningSessionId: "other" } }
          : {}),
        parameters: {
          ...params,
          ...(scenario === "role mismatch" ? { role: "repair" } : {}),
          ...(scenario === "provider mismatch"
            ? { provider: { providerInstanceId: "other", model: "model" } }
            : {}),
          ...(scenario === "operation limits mismatch"
            ? { limits: { ...owner.limits, maxPasses: 9 } }
            : {}),
          ...(scenario === "manifest hash mismatch" ? { inputManifestHash: hash } : {}),
        },
      });
      input = {
        ...input,
        operation: { ...operation, fingerprint: WorkflowCanonicalHash.operation(operation) },
      };
    }
    const snapshot = Effect.gen(function* () {
      return {
        sessions: yield* sql`SELECT * FROM workflow_planning_sessions`,
        operations: yield* sql`SELECT * FROM workflow_operations`,
        passes: yield* sql`SELECT * FROM workflow_planning_passes`,
        receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
      };
    });
    const before = yield* snapshot;
    const error = yield* Effect.flip(store.admitPlanningPass(input));
    assert.strictEqual(error.reason, reason);
    if (scenario === "stale version") assert.strictEqual(error.currentVersion, 0);
    assert.deepStrictEqual(yield* snapshot, before);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["deadline", "binding", "stale generation", "untrusted"] as const)(
  "planning claim rejects %s without writes",
  (scenario) =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      const input = admissionInput(binding, "claim");
      yield* store.admitPlanningPass(input);
      if (scenario === "binding")
        yield* sql`UPDATE workflow_planning_passes SET record_json = ${json({ ...input.pass, binding: { ...input.pass.binding, revisionHash: hash } })} WHERE id = ${input.pass.id}`;
      const operations = yield* sql`SELECT * FROM workflow_operations`;
      const passes = yield* sql`SELECT * FROM workflow_planning_passes`;
      const claim = store.claim({
        operationId: input.operation.id,
        expectedOwnerVersion: 1,
        expectedGeneration: scenario === "stale generation" ? 1 : 0,
        bootId: "boot",
        workerId: "worker",
        now: scenario === "deadline" ? session.limits.deadline : facts.now,
        trustedEligibility: scenario !== "untrusted",
      });
      if (scenario === "binding")
        assert.strictEqual((yield* Effect.flip(claim)).reason, "integrity");
      else
        assert.strictEqual(
          (yield* claim).kind,
          scenario === "stale generation" ? "stale" : "denied",
        );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, operations);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_passes`, passes);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "competing independent admissions charge the last pass once and replay after reopening",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "planning-admit-"))),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "test.sqlite");
        const fileLayer = () =>
          Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
        const limited = { ...session, limits: { ...session.limits, maxPasses: 1 } };
        const binding = yield* Effect.scoped(
          Effect.gen(function* () {
            const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
            const store = yield* Store.WorkflowRunStore;
            yield* begin(store, limited);
            return binding;
          }).pipe(Effect.provide(fileLayer())),
        );
        const barrier = yield* Deferred.make<void>();
        const arrivals = yield* Ref.make(0);
        const attempt = (id: string) =>
          Effect.scoped(
            Effect.gen(function* () {
              const store = yield* Store.WorkflowRunStore;
              const snapshot = yield* store.getPlanning({ sessionId: limited.id });
              assert.strictEqual(snapshot.consumed.passes, 0);
              const count = yield* Ref.updateAndGet(arrivals, (value) => value + 1);
              if (count === 2) yield* Deferred.succeed(barrier, undefined);
              yield* Deferred.await(barrier);
              const input = admissionInput(binding, id, limited);
              return { input, result: yield* Effect.result(store.admitPlanningPass(input)) };
            }).pipe(Effect.provide(fileLayer())),
          );
        const results = yield* Effect.all([attempt("one"), attempt("two")], { concurrency: 2 });
        const winner = results.find((item) => item.result._tag === "Success");
        const loser = results.find((item) => item.result._tag === "Failure");
        assert.isDefined(winner);
        assert.isDefined(loser);
        if (
          !winner ||
          winner.result._tag !== "Success" ||
          !loser ||
          loser.result._tag !== "Failure"
        )
          return;
        assert.include(["conflict", "limit"], loser.result.failure.reason);
        const won = winner.result.success;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            const sql = yield* SqlClient.SqlClient;
            assert.deepStrictEqual(yield* store.admitPlanningPass(winner.input), {
              ...won,
              kind: "replay",
            });
            assert.strictEqual(
              (yield* store.getPlanning({ sessionId: limited.id })).consumed.passes,
              1,
            );
            assert.deepStrictEqual(yield* sql`SELECT id FROM workflow_operations`, [
              { id: winner.input.operation.id },
            ]);
            assert.deepStrictEqual(yield* sql`SELECT id FROM workflow_planning_passes`, [
              { id: winner.input.pass.id },
            ]);
            assert.deepStrictEqual(
              yield* sql`SELECT operation_id FROM workflow_command_receipts WHERE operation_id IS NOT NULL`,
              [{ operation_id: winner.input.operation.id }],
            );
            const claim = yield* store.claim({
              operationId: winner.input.operation.id,
              expectedOwnerVersion: 1,
              expectedGeneration: 0,
              bootId: "boot",
              workerId: "worker",
              now: facts.now,
              trustedEligibility: true,
            });
            assert.strictEqual(claim.kind, "claimed");
            const rows = yield* sql<{
              record_json: string;
            }>`SELECT record_json FROM workflow_planning_passes`;
            assert.strictEqual(
              (parse(rows[0]!.record_json) as { status: string }).status,
              "claimed",
            );
          }).pipe(Effect.provide(fileLayer())),
        );
      }),
    ),
);

it.effect.each([
  ["thread", "integrity"],
  ["run", "integrity"],
  ["operation", "integrity"],
  ["terminal", "integrity"],
  ["pass", "integrity"],
  ["manifest", "integrity"],
  ["role", "integrity"],
  ["review revision hash", "integrity"],
  ["review source hash", "integrity"],
  ["malformed", "integrity"],
  ["pending", "integrity"],
  ["failed", "ineligible"],
  ["cancelled", "ineligible"],
  ["replay observation", "integrity"],
  ["replay report", "integrity"],
  ["version", "conflict"],
] as const)("planning settlement rejects %s without writes", ([scenario, reason]) =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const review = scenario === "review revision hash" || scenario === "review source hash";
    const owner = review ? { ...session, allowedRoles: ["review"] as const } : session;
    yield* begin(store, owner);
    const base = admissionInput(binding, "settle", owner);
    const pass = review ? { ...base.pass, role: "review" as const } : base.pass;
    if (base.operation.parameters.kind !== "planning_pass") throw new Error("wrong operation");
    const operation = {
      ...base.operation,
      parameters: { ...base.operation.parameters, role: pass.role },
    };
    const admission = {
      ...base,
      pass,
      operation: { ...operation, fingerprint: WorkflowCanonicalHash.operation(operation) },
    };
    yield* store.admitPlanningPass(admission);
    if (scenario !== "pending")
      yield* store.claim({
        operationId: admission.operation.id,
        expectedOwnerVersion: 1,
        expectedGeneration: 0,
        bootId: "boot",
        workerId: "worker",
        now: facts.now,
        trustedEligibility: true,
      });
    const native = admission.pass.native;
    if (!native) throw new Error("missing native");
    const observation = {
      kind: "terminal" as const,
      operationId: admission.operation.id,
      passId: admission.pass.id,
      native: {
        threadId: native.threadId,
        runId: native.runId,
        terminalMessageId: decodeTerminalId("terminal"),
      },
      outcome: "succeeded" as const,
      actualInputManifest: admission.pass.inputManifest,
      inputManifestHash: WorkflowCanonicalHash.tagged(
        "t3.planning-input-manifest.v1",
        admission.pass.inputManifest,
      ),
      freshConversation: true,
      sourcePacketDelivered: true,
      forbiddenContextDelivered: false,
      restrictedProfileQualified: false,
    };
    const input = {
      sessionId: session.id,
      passId: admission.pass.id,
      operationId: admission.operation.id,
      expectedVersion: scenario === "version" ? 0 : 1,
      observation: {
        ...observation,
        operationId:
          scenario === "operation"
            ? decodeOperationFixture({ ...admission.operation, id: "other" }).id
            : observation.operationId,
        passId: scenario === "pass" ? decodePassId("other") : observation.passId,
        native: {
          ...observation.native,
          terminalMessageId:
            scenario === "terminal"
              ? (undefined as unknown as typeof observation.native.terminalMessageId)
              : observation.native.terminalMessageId,
          threadId: scenario === "thread" ? decodeThreadId("other") : native.threadId,
          runId: scenario === "run" ? decodeNativeRunId("other") : native.runId,
        },
        inputManifestHash: scenario === "manifest" ? hash : observation.inputManifestHash,
        outcome:
          scenario === "failed"
            ? ("failed" as const)
            : scenario === "cancelled"
              ? ("cancelled" as const)
              : observation.outcome,
      },
      report: review
        ? {
            kind: "review" as const,
            verdict: "pass" as const,
            assessment: {
              revisionHash: scenario === "review revision hash" ? hash : binding.revisionHash,
              sourcePacketHash: scenario === "review source hash" ? hash : binding.sourcePacketHash,
              inventory: [],
            },
            findings: [],
          }
        : scenario === "malformed"
          ? { kind: "author" as const, summary: "", outputs: [] }
          : scenario === "role"
            ? { kind: "repair" as const, summary: "repair", dispositions: [] }
            : { kind: "author" as const, summary: "done", outputs: [] },
    };
    if (scenario === "replay observation" || scenario === "replay report") {
      yield* store.settlePlanningPass(input);
    }
    const rejected =
      scenario === "replay observation"
        ? { ...input, observation: { ...input.observation, sourcePacketDelivered: false } }
        : scenario === "replay report"
          ? { ...input, report: { kind: "author" as const, summary: "changed", outputs: [] } }
          : input;
    const snapshot = Effect.gen(function* () {
      return {
        receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
        sessions: yield* sql`SELECT * FROM workflow_planning_sessions`,
        passes: yield* sql`SELECT * FROM workflow_planning_passes`,
        operations: yield* sql`SELECT * FROM workflow_operations`,
        observations: yield* sql`SELECT * FROM workflow_planning_observations`,
        settlements: yield* sql`SELECT * FROM workflow_planning_settlements`,
        dispositions: yield* sql`SELECT * FROM workflow_planning_dispositions`,
      };
    });
    const before = yield* snapshot;
    assert.strictEqual((yield* Effect.flip(store.settlePlanningPass(rejected))).reason, reason);
    assert.deepStrictEqual(yield* snapshot, before);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "planning recovery settles unknown custody after restart without reopening a stopped session",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "planning-settle-"))),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "test.sqlite");
        const fileLayer = () =>
          Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
        const first = yield* Effect.scoped(
          Effect.gen(function* () {
            const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
            const store = yield* Store.WorkflowRunStore;
            yield* begin(store, session);
            const admission = admissionInput(binding, "persist");
            yield* store.admitPlanningPass(admission);
            yield* store.claim({
              operationId: admission.operation.id,
              expectedOwnerVersion: 1,
              expectedGeneration: 0,
              bootId: "boot",
              workerId: "worker",
              now: facts.now,
              trustedEligibility: true,
            });
            const native = admission.pass.native;
            if (!native) throw new Error("missing native");
            const input = {
              sessionId: session.id,
              passId: admission.pass.id,
              operationId: admission.operation.id,
              expectedVersion: 1,
              observation: {
                kind: "terminal" as const,
                operationId: admission.operation.id,
                passId: admission.pass.id,
                native: {
                  threadId: native.threadId,
                  runId: native.runId,
                  terminalMessageId: decodeTerminalId("terminal"),
                },
                outcome: "succeeded" as const,
                actualInputManifest: admission.pass.inputManifest,
                inputManifestHash: WorkflowCanonicalHash.tagged(
                  "t3.planning-input-manifest.v1",
                  admission.pass.inputManifest,
                ),
                freshConversation: true,
                sourcePacketDelivered: true,
                forbiddenContextDelivered: false,
                restrictedProfileQualified: false,
              },
              report: { kind: "author" as const, summary: "done", outputs: [] },
            };
            return input;
          }).pipe(Effect.provide(fileLayer())),
        );
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            const recovery = yield* Recovery.WorkflowRunRecovery;
            const sql = yield* SqlClient.SqlClient;
            assert.strictEqual(
              (yield* recovery.reconcile({ operationId: first.operationId })).kind,
              "unknown",
            );
            assert.strictEqual(
              (yield* store.readRecovery(first.operationId))?.operation.status,
              "unknown",
            );
            const current = yield* store.getPlanning({ sessionId: session.id });
            yield* sql`UPDATE workflow_planning_sessions SET status = 'stopped', record_json = ${json({ ...current, status: "stopped" })} WHERE id = ${session.id}`;
            const input = { ...first, expectedVersion: current.recordVersion };
            const settled = yield* store.settlePlanningPass(input);
            assert.strictEqual(settled.kind, "applied");
            assert.strictEqual(settled.value.session.status, "stopped");
            assert.deepStrictEqual(settled.value.session.consumed, { passes: 1, repairRounds: 0 });
            return { input, settled };
          }).pipe(
            Effect.provide(
              Recovery.layer.pipe(
                Layer.provideMerge(
                  Layer.merge(
                    fileLayer(),
                    Layer.succeed(Recovery.WorkflowRunObservation, {
                      observe: () => Effect.succeed({ kind: "unknown" as const }),
                    }),
                  ),
                ),
              ),
            ),
          ),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            const sql = yield* SqlClient.SqlClient;
            assert.deepStrictEqual(yield* store.settlePlanningPass(result.input), {
              ...result.settled,
              kind: "replay",
            });
            assert.strictEqual(
              (yield* Effect.flip(
                store.settlePlanningPass({
                  ...result.input,
                  report: { ...result.input.report, summary: "changed" },
                }),
              )).reason,
              "integrity",
            );
            assert.strictEqual(
              (yield* sql`SELECT * FROM workflow_planning_observations`).length,
              1,
            );
            assert.strictEqual((yield* sql`SELECT * FROM workflow_planning_settlements`).length, 1);
          }).pipe(Effect.provide(fileLayer())),
        );
      }),
    ),
);

it.effect.each([
  "accept",
  "defer",
  "foreign review",
  "missing finding",
  "affected objects",
  "conflict",
] as const)("planning repair disposition: %s", (scenario) =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const owner = { ...session, allowedRoles: ["review", "repair"] as const };
    yield* begin(store, owner);
    const admissionFor = (id: string, role: "review" | "repair", expectedVersion: number) => {
      const base = admissionInput(binding, id, owner);
      const pass = { ...base.pass, role };
      if (base.operation.parameters.kind !== "planning_pass") throw new Error("wrong operation");
      const operation = { ...base.operation, parameters: { ...base.operation.parameters, role } };
      return {
        ...base,
        expectedVersion,
        pass,
        operation: { ...operation, fingerprint: WorkflowCanonicalHash.operation(operation) },
      };
    };
    const review = admissionFor("review", "review", 0);
    yield* store.admitPlanningPass(review);
    yield* store.claim({
      operationId: review.operation.id,
      expectedOwnerVersion: 1,
      expectedGeneration: 0,
      bootId: "boot",
      workerId: "worker",
      now: facts.now,
      trustedEligibility: true,
    });
    const observationFor = (admission: typeof review) => {
      const native = admission.pass.native;
      if (!native) throw new Error("missing native");
      return {
        kind: "terminal" as const,
        operationId: admission.operation.id,
        passId: admission.pass.id,
        native: {
          threadId: native.threadId,
          runId: native.runId,
          terminalMessageId: decodeTerminalId("terminal"),
        },
        outcome: "succeeded" as const,
        actualInputManifest: admission.pass.inputManifest,
        inputManifestHash: WorkflowCanonicalHash.tagged(
          "t3.planning-input-manifest.v1",
          admission.pass.inputManifest,
        ),
        freshConversation: true,
        sourcePacketDelivered: true,
        forbiddenContextDelivered: false,
        restrictedProfileQualified: false,
      };
    };
    const reviewPass = decodePassFixture({
      schemaVersion: review.pass.schemaVersion,
      id: review.pass.id,
      planningSessionId: review.pass.planningSessionId,
      role: review.pass.role,
      provider: review.pass.provider,
      binding: review.pass.binding,
      operationId: review.pass.operationId,
      inputManifest: review.pass.inputManifest,
      access: review.pass.access,
      status: "settled",
      native: observationFor(review).native,
      report: {
        kind: "review",
        verdict: "changes_requested",
        assessment: {
          revisionHash: binding.revisionHash,
          sourcePacketHash: binding.sourcePacketHash,
          inventory: [],
        },
        findings: [
          {
            id: "finding",
            severity: "blocking",
            description: "Problem",
            affectedObjectIds: ["object"],
            evidence: [],
            proposedRemedy: "Repair",
          },
        ],
      },
    });
    if (!reviewPass.report) throw new Error("missing report");
    yield* store.settlePlanningPass({
      sessionId: owner.id,
      passId: review.pass.id,
      operationId: review.operation.id,
      expectedVersion: 1,
      observation: observationFor(review),
      report: reviewPass.report,
    });
    const repair = admissionFor("repair", "repair", 2);
    yield* store.admitPlanningPass(repair);
    yield* store.claim({
      operationId: repair.operation.id,
      expectedOwnerVersion: 3,
      expectedGeneration: 0,
      bootId: "boot",
      workerId: "worker",
      now: facts.now,
      trustedEligibility: true,
    });
    const disposition = {
      reportId: scenario === "foreign review" ? "missing" : review.pass.id,
      findingId: scenario === "missing finding" ? "missing" : "finding",
      decision: scenario === "defer" ? ("defer" as const) : ("accept" as const),
      rationale: "Reason",
      affectedObjectIds: scenario === "affected objects" ? ["other"] : ["object"],
      ...(scenario === "defer" ? { deferTrigger: "After review" } : {}),
    };
    const repairPass = decodePassFixture({
      schemaVersion: repair.pass.schemaVersion,
      id: repair.pass.id,
      planningSessionId: repair.pass.planningSessionId,
      role: repair.pass.role,
      provider: repair.pass.provider,
      binding: repair.pass.binding,
      operationId: repair.pass.operationId,
      inputManifest: repair.pass.inputManifest,
      access: repair.pass.access,
      status: "settled",
      native: observationFor(repair).native,
      report: { kind: "repair", summary: "Repaired", dispositions: [disposition] },
    });
    if (repairPass.report?.kind !== "repair") throw new Error("missing report");
    const item = repairPass.report.dispositions[0];
    if (!item) throw new Error("missing disposition");
    if (scenario === "conflict")
      yield* sql`INSERT INTO workflow_planning_dispositions VALUES (${review.pass.id}, ${item.findingId}, ${review.pass.id}, ${json({ ...item, rationale: "Different" })})`;
    const before = {
      sessions: yield* sql`SELECT * FROM workflow_planning_sessions`,
      operations: yield* sql`SELECT * FROM workflow_operations`,
      passes: yield* sql`SELECT * FROM workflow_planning_passes`,
      observations: yield* sql`SELECT * FROM workflow_planning_observations`,
      settlements: yield* sql`SELECT * FROM workflow_planning_settlements`,
      dispositions: yield* sql`SELECT * FROM workflow_planning_dispositions`,
    };
    const action = store.settlePlanningPass({
      sessionId: owner.id,
      passId: repair.pass.id,
      operationId: repair.operation.id,
      expectedVersion: 3,
      observation: observationFor(repair),
      report: repairPass.report,
    });
    if (scenario === "accept" || scenario === "defer") {
      const result = yield* action;
      assert.strictEqual(result.kind, "applied");
      const rows = yield* sql<{
        disposition_json: string;
      }>`SELECT disposition_json FROM workflow_planning_dispositions`;
      assert.strictEqual(rows.length, 1);
      const row = rows[0];
      if (!row) throw new Error("missing disposition");
      assert.deepStrictEqual(parse(row.disposition_json), item);
      const input = {
        sessionId: owner.id,
        reviewPassId: review.pass.id,
        findingId: item.findingId,
        repairPassId: repair.pass.id,
        disposition: item,
        expectedVersion: 4,
        context,
        command: { commandId: "disposition", receiptId: "disposition-receipt" },
      };
      const receipt = yield* store.recordPlanningDisposition(input);
      assert.deepStrictEqual(yield* store.recordPlanningDisposition(input), {
        ...receipt,
        kind: "replay",
      });
    } else {
      assert.strictEqual(
        (yield* Effect.flip(action)).reason,
        scenario === "conflict" ? "conflict" : "integrity",
      );
      assert.deepStrictEqual(
        {
          sessions: yield* sql`SELECT * FROM workflow_planning_sessions`,
          operations: yield* sql`SELECT * FROM workflow_operations`,
          passes: yield* sql`SELECT * FROM workflow_planning_passes`,
          observations: yield* sql`SELECT * FROM workflow_planning_observations`,
          settlements: yield* sql`SELECT * FROM workflow_planning_settlements`,
          dispositions: yield* sql`SELECT * FROM workflow_planning_dispositions`,
        },
        before,
      );
    }
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["active", "active", true, "none"],
  ["pausing", "paused", true, "none"],
  ["stopping", "stopped", false, "none"],
  ["stopped", "stopped", false, "none"],
  ["needs_attention", "needs_attention", false, "none"],
  ["pausing", "pausing", true, "dispatching"],
  ["pausing", "pausing", true, "unknown"],
  ["stopping", "stopping", false, "dispatching"],
  ["stopping", "stopping", false, "unknown"],
] as const)(
  "planning settlement preserves %s history",
  ([initial, expected, eligible, intentState]) =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const store = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      const owner = { ...session, allowedRoles: ["author", "review"] as const };
      yield* begin(store, owner);
      const admission = admissionInput(binding, "late", owner);
      yield* store.admitPlanningPass(admission);
      if (intentState !== "none") {
        const review = admissionInput(binding, "remaining", owner);
        if (review.operation.parameters.kind !== "planning_pass")
          throw new Error("expected planning pass");
        const nativeInput = planningReservation({
          ...review,
          expectedVersion: 1,
          pass: { ...review.pass, role: "review" },
          operation: {
            ...review.operation,
            parameters: { ...review.operation.parameters, role: "review" },
          },
        });
        yield* store.reservePlanningNativeIntent(nativeInput);
        yield* sql`UPDATE workflow_native_launch_intents SET state = ${intentState}, boot_id = 'boot', worker_id = 'worker' WHERE intent_id = ${nativeInput.intentId}`;
      }
      yield* store.claim({
        operationId: admission.operation.id,
        expectedOwnerVersion: intentState === "none" ? 1 : 2,
        expectedGeneration: 0,
        bootId: "boot",
        workerId: "worker",
        now: facts.now,
        trustedEligibility: true,
      });
      const current = yield* store.getPlanning({ sessionId: session.id });
      yield* sql`UPDATE workflow_planning_sessions SET status = ${initial}, record_json = ${json({ ...current, status: initial })} WHERE id = ${session.id}`;
      const native = admission.pass.native;
      if (!native) throw new Error("missing native");
      const result = yield* store.settlePlanningPass({
        sessionId: session.id,
        passId: admission.pass.id,
        operationId: admission.operation.id,
        expectedVersion: current.recordVersion,
        observation: {
          kind: "terminal",
          operationId: admission.operation.id,
          passId: admission.pass.id,
          native: {
            threadId: native.threadId,
            runId: native.runId,
            terminalMessageId: decodeTerminalId("terminal"),
          },
          outcome: "succeeded",
          actualInputManifest: admission.pass.inputManifest,
          inputManifestHash: WorkflowCanonicalHash.tagged(
            "t3.planning-input-manifest.v1",
            admission.pass.inputManifest,
          ),
          freshConversation: true,
          sourcePacketDelivered: true,
          forbiddenContextDelivered: false,
          restrictedProfileQualified: false,
        },
        report: { kind: "author", summary: "done", outputs: [] },
      });
      assert.strictEqual(result.value.session.status, expected);
      assert.deepStrictEqual(result.value.session.consumed, {
        passes: intentState === "none" ? 1 : 2,
        repairRounds: 0,
      });
      const rows = yield* sql<{
        observation_json: string;
      }>`SELECT observation_json FROM workflow_planning_observations`;
      const row = rows[0];
      if (!row) throw new Error("missing observation");
      assert.strictEqual(
        (parse(row.observation_json) as { eligibleForReadiness: boolean }).eligibleForReadiness,
        eligible,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("planning recovery preserves unknown custody and rejects independent settlement", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    yield* begin(store, session);
    const admission = admissionInput(binding, "recover");
    yield* store.admitPlanningPass(admission);
    yield* store.claim({
      operationId: admission.operation.id,
      expectedOwnerVersion: 1,
      expectedGeneration: 0,
      bootId: "boot",
      workerId: "worker",
      now: facts.now,
      trustedEligibility: true,
    });
    const snapshot = yield* store.readRecovery(admission.operation.id);
    if (!snapshot) throw new Error("missing operation");
    const input = {
      operationId: admission.operation.id,
      expectedOwnerVersion: snapshot.owner.recordVersion,
      expectedGeneration: 0,
      expectedFingerprint: snapshot.operation.fingerprint,
      observed: {
        operation: snapshot.operation,
        ownerGeneration: snapshot.owner.controlGeneration,
        native: snapshot.native,
        kind: "settled" as const,
        outcome: { kind: "succeeded" as const, summary: "done", evidence: [] },
      },
    };
    const before = yield* sql`SELECT * FROM workflow_operations`;
    assert.strictEqual(
      (yield* Effect.flip(store.compareAndApplyRecovery(input))).reason,
      "integrity",
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, before);
    assert.strictEqual(
      (yield* store.compareAndApplyRecovery({
        ...input,
        observed: { ...input.observed, kind: "unknown" },
      })).kind,
      "updated",
    );
    const rows = yield* sql<{
      record_json: string;
    }>`SELECT record_json FROM workflow_planning_passes`;
    const row = rows[0];
    if (!row) throw new Error("missing pass");
    assert.strictEqual((parse(row.record_json) as { status: string }).status, "unknown");
    assert.strictEqual(
      (yield* store.getPlanning({ sessionId: session.id })).status,
      "needs_attention",
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("admission replays after pause and resume advance the control generation", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const admitted = yield* store.admitPlanningPass(admissionInput(binding, "one"));
    assert.strictEqual(admitted.kind, "applied");
    const control = {
      sessionId: session.id,
      now: facts.now,
      context,
    };
    yield* store.controlPlanning({
      ...control,
      action: "pause",
      expectedVersion: 1,
      command: { commandId: "pause-after-admit", receiptId: "pause-after-admit" },
    });
    yield* store.controlPlanning({
      ...control,
      action: "resume",
      expectedVersion: 2,
      command: { commandId: "resume-after-admit", receiptId: "resume-after-admit" },
    });
    const current = yield* store.getPlanning({ sessionId: session.id });
    assert.notStrictEqual(current.controlGeneration, session.controlGeneration);
    const replay = yield* store.admitPlanningPass(admissionInput(binding, "one", current));
    assert.deepStrictEqual(replay, { ...admitted, kind: "replay" });
    assert.deepStrictEqual(
      yield* store.getPlanningPass({ passId: admitted.value.pass.id }),
      admitted.value.pass,
    );
    assert.strictEqual(
      (yield* Effect.flip(
        store.getPlanningPass({
          passId: admitted.value.pass.id.replace("one", "missing") as typeof admitted.value.pass.id,
        }),
      )).reason,
      "not-found",
    );
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["reservation-version", "control-version", "control-generation"] as const)(
  "workflow rejects overflowing %s without writes and preserves replay",
  (scenario) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const input = reservation(
        operationFor("overflow-op", "overflow-attempt"),
        "overflow-attempt",
      );
      const prior = yield* store.reserveWorkflowNativeIntent(input);
      const row = (yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`)[0]!;
      const owner = parse(row.record_json) as typeof run;
      const version =
        scenario === "control-generation" ? owner.recordVersion : Number.MAX_SAFE_INTEGER;
      const generation =
        scenario === "control-generation" ? Number.MAX_SAFE_INTEGER : owner.controlGeneration;
      const changed = { ...owner, recordVersion: version, controlGeneration: generation };
      yield* sql`UPDATE workflow_runs SET record_version = ${version}, control_generation = ${generation}, record_json = ${json(changed)}`;
      if (scenario === "control-generation")
        yield* sql`UPDATE workflow_native_launch_intents SET generation = ${generation}`;
      const before = {
        owner: yield* sql`SELECT * FROM workflow_runs`,
        intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
        nativeReceipts: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
      };
      if (scenario === "reservation-version") {
        // Remove the active reservation only after retaining its receipt for exact replay.
        yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled'`;
        const cancelled = yield* sql`SELECT * FROM workflow_native_launch_intents`;
        const next = reservation(operationFor("overflow-next", "overflow-next"), "overflow-next");
        const candidate = {
          ...next,
          draft: {
            ...next.draft,
            admission: { command: { ...next.draft.admission.command, expectedVersion: version } },
          },
        };
        assert.strictEqual(
          (yield* Effect.flip(store.reserveWorkflowNativeIntent(candidate))).reason,
          "integrity",
        );
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, cancelled);
      } else {
        assert.strictEqual(
          (yield* Effect.flip(
            store.commitTransition({
              runId: run.id,
              command: {
                ...command,
                commandId: "overflow-control",
                expectedVersion: version,
                expectedGeneration: generation,
              },
              externalFacts: admissionFacts,
              receiptId: "overflow-control",
            }),
          )).reason,
          "integrity",
        );
        assert.deepStrictEqual(
          yield* sql`SELECT * FROM workflow_native_launch_intents`,
          before.intents,
        );
      }
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before.owner);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        before.nativeReceipts,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_command_receipts`, before.receipts);
      assert.deepStrictEqual(yield* store.reserveWorkflowNativeIntent(input), {
        ...prior,
        kind: "replay",
      });
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["pause", "resume", "stop"] as const)(
  "workflow %s rejects receipt ID substitution without writes",
  (kind) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const input = reservation(operationFor("anchor-op", "anchor-attempt"), "anchor-attempt");
      yield* store.reserveWorkflowNativeIntent(input);
      if (kind === "resume")
        yield* store.commitTransition({
          runId: run.id,
          command: { ...command, commandId: "anchor-pause", expectedVersion: 1 },
          externalFacts: admissionFacts,
          receiptId: "anchor-pause",
        });
      yield* sql`UPDATE workflow_native_intent_receipts SET receipt_id = 'substituted'`;
      const snapshot = {
        owner: yield* sql`SELECT * FROM workflow_runs`,
        intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
        nativeReceipt: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        controls: yield* sql`SELECT * FROM workflow_command_receipts`,
      };
      assert.strictEqual(
        (yield* Effect.flip(
          store.commitTransition({
            runId: run.id,
            command: {
              ...command,
              kind,
              commandId: "anchor-control",
              expectedVersion: kind === "resume" ? 2 : 1,
              expectedGeneration: kind === "resume" ? 1 : 0,
            },
            externalFacts: admissionFacts,
            receiptId: "anchor-control",
          }),
        )).reason,
        "integrity",
      );
      assert.deepStrictEqual(
        {
          owner: yield* sql`SELECT * FROM workflow_runs`,
          intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
          nativeReceipt: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
          controls: yield* sql`SELECT * FROM workflow_command_receipts`,
        },
        snapshot,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("legacy workflow reservation replays exactly but cannot authorize controls", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const input = reservation(operationFor("v1-op", "v1-attempt"), "v1-attempt");
    const legacy = WorkflowCanonicalHash.tagged("t3.workflow-native-launch-intent.v1", {
      intentId: input.intentId,
      workflowRunId: input.runId,
      planningSessionId: null,
      operationId: input.operationId,
      attemptId: input.attemptId,
      passId: null,
      itemId: input.itemId,
      chargeAttempt: 1,
      repair: input.repair,
      role: "implementation",
      environmentId: input.environmentId,
      projectId: input.projectId,
      launchCommandId: input.launchCommandId,
      threadId: input.threadId,
      messageId: input.messageId,
      draft: input.draft,
    });
    const owner = {
      ...run,
      access: { origins: [{ ...destination, threadId: "thread" }] },
      recordVersion: 1,
      allowance: { ...run.allowance, attemptsConsumed: 1 },
    };
    yield* sql`UPDATE workflow_runs SET record_version = 1, record_json = ${json(owner)}`;
    yield* sql`INSERT INTO workflow_native_launch_intents (intent_id, workflow_run_id, operation_id, attempt_id, item_id, charge_attempt, repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, generation, state, held) VALUES (${input.intentId}, ${input.runId}, ${input.operationId}, ${input.attemptId}, ${input.itemId}, 1, 0, 'implementation', ${input.environmentId}, ${input.projectId}, ${input.launchCommandId}, ${input.threadId}, ${input.messageId}, ${legacy}, ${json(input.draft)}, 0, 'reserved', 0)`;
    yield* sql`INSERT INTO workflow_native_intent_receipts VALUES (${input.draft.admission.command.commandId}, ${input.receiptId}, ${input.intentId}, ${legacy}, ${json({ intentId: input.intentId, fingerprint: legacy })})`;
    assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), {
      kind: "owned",
      owner: { kind: "workflow_run", workflowRunId: run.id },
    });
    const snapshot = {
      owner: yield* sql`SELECT * FROM workflow_runs`,
      intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
      nativeReceipt: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
      controls: yield* sql`SELECT * FROM workflow_command_receipts`,
    };
    assert.deepStrictEqual(
      yield* store.reserveWorkflowNativeIntent({
        ...input,
        externalFacts: { ...admissionFacts, now: "2099-01-01T00:00:00Z" },
      }),
      { kind: "replay", receiptId: input.receiptId, intentId: input.intentId, fingerprint: legacy },
    );
    for (const changed of [
      { ...input, receiptId: "changed" },
      { ...input, messageId: "changed" },
    ])
      assert.strictEqual(
        (yield* Effect.flip(store.reserveWorkflowNativeIntent(changed))).reason,
        "integrity",
      );
    assert.strictEqual(
      (yield* Effect.flip(
        store.commitTransition({
          runId: run.id,
          command: { ...command, commandId: "v1-pause", expectedVersion: 1 },
          externalFacts: admissionFacts,
          receiptId: "v1-pause",
        }),
      )).reason,
      "integrity",
    );
    assert.deepStrictEqual(
      {
        owner: yield* sql`SELECT * FROM workflow_runs`,
        intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
        nativeReceipt: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        controls: yield* sql`SELECT * FROM workflow_command_receipts`,
      },
      snapshot,
    );
    const paused = { ...owner, status: "paused", recordVersion: 2, controlGeneration: 1 };
    yield* sql`UPDATE workflow_runs SET status = 'paused', record_version = 2, control_generation = 1, record_json = ${json(paused)}`;
    yield* sql`UPDATE workflow_native_launch_intents SET held = 1`;
    assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), {
      kind: "owned",
      owner: { kind: "workflow_run", workflowRunId: run.id },
    });
    const held = {
      owner: yield* sql`SELECT * FROM workflow_runs`,
      intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
      nativeReceipt: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
      controls: yield* sql`SELECT * FROM workflow_command_receipts`,
    };
    assert.deepStrictEqual(
      yield* store.reserveWorkflowNativeIntent({
        ...input,
        externalFacts: { ...admissionFacts, now: "2099-01-01T00:00:00Z" },
      }),
      { kind: "replay", receiptId: input.receiptId, intentId: input.intentId, fingerprint: legacy },
    );
    for (const kind of ["resume", "stop"] as const)
      assert.strictEqual(
        (yield* Effect.flip(
          store.commitTransition({
            runId: run.id,
            command: {
              ...command,
              kind,
              commandId: `v1-${kind}`,
              expectedVersion: 2,
              expectedGeneration: 1,
            },
            externalFacts: admissionFacts,
            receiptId: `v1-${kind}`,
          }),
        )).reason,
        "integrity",
      );
    assert.deepStrictEqual(
      {
        owner: yield* sql`SELECT * FROM workflow_runs`,
        intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
        nativeReceipt: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        controls: yield* sql`SELECT * FROM workflow_command_receipts`,
      },
      held,
    );
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["reserved", "dispatching", "unknown"] as const)(
  "workflow deadline cancels held reservation and retains %s custody",
  (state) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const input = reservation(
        operationFor("deadline-op", "deadline-attempt"),
        "deadline-attempt",
      );
      yield* store.reserveWorkflowNativeIntent(input);
      if (state === "reserved")
        yield* store.commitTransition({
          runId: run.id,
          command: { ...command, commandId: "deadline-pause", expectedVersion: 1 },
          externalFacts: admissionFacts,
          receiptId: "deadline-pause",
        });
      else
        yield* sql`UPDATE workflow_native_launch_intents SET state = ${state}, boot_id = 'boot', worker_id = 'worker'`;
      const beforeReceipt = yield* sql`SELECT * FROM workflow_native_intent_receipts`;
      if (state === "reserved") {
        const before = {
          owner: yield* sql`SELECT * FROM workflow_runs`,
          intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
          receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
        };
        assert.deepStrictEqual(
          yield* store.commitTransition({
            runId: run.id,
            command: {
              ...command,
              kind: "resume",
              commandId: "deadline-resume",
              expectedVersion: 2,
              expectedGeneration: 1,
            },
            externalFacts: { ...admissionFacts, now: run.allowance.deadline },
            receiptId: "deadline-resume",
          }),
          { kind: "rejected", reason: "deadline" },
        );
        assert.deepStrictEqual(
          {
            owner: yield* sql`SELECT * FROM workflow_runs`,
            intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
            receipts: yield* sql`SELECT * FROM workflow_command_receipts`,
          },
          before,
        );
      }
      assert.strictEqual(
        (yield* store.commitTransition({
          runId: run.id,
          command: {
            ...command,
            kind: "deadline",
            commandId: "deadline-control",
            expectedVersion: state === "reserved" ? 2 : 1,
            expectedGeneration: state === "reserved" ? 1 : 0,
          },
          externalFacts: { ...admissionFacts, now: run.allowance.deadline },
          receiptId: "deadline-control",
        })).kind,
        "accepted",
      );
      assert.deepStrictEqual(yield* sql`SELECT state, held FROM workflow_native_launch_intents`, [
        { state: state === "reserved" ? "cancelled" : state, held: 0 },
      ]);
      assert.deepStrictEqual(yield* sql`SELECT status FROM workflow_runs`, [
        { status: state === "reserved" ? "stopped" : "stopping" },
      ]);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        beforeReceipt,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("competing workflow controls on separate connections have one complete winner", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "workflow-control-race-")),
        ),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "test.sqlite");
      const fileLayer = () =>
        Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
      yield* Effect.scoped(
        Effect.gen(function* () {
          yield* setupAdmission;
          const store = yield* Store.WorkflowRunStore;
          yield* store.reserveWorkflowNativeIntent(
            reservation(operationFor("race-op", "race-attempt"), "race-attempt"),
          );
        }).pipe(Effect.provide(fileLayer())),
      );
      const barrier = yield* Deferred.make<void>();
      const arrivals = yield* Ref.make(0);
      const control = (id: string) =>
        Effect.scoped(
          Effect.gen(function* () {
            const store = yield* Store.WorkflowRunStore;
            const count = yield* Ref.updateAndGet(arrivals, (value) => value + 1);
            if (count === 2) yield* Deferred.succeed(barrier, undefined);
            yield* Deferred.await(barrier);
            return yield* Effect.result(
              store.commitTransition({
                runId: run.id,
                command: { ...command, commandId: id, expectedVersion: 1 },
                externalFacts: admissionFacts,
                receiptId: id,
              }),
            );
          }).pipe(Effect.provide(fileLayer())),
        );
      const results = yield* Effect.all([control("race-one"), control("race-two")], {
        concurrency: 2,
      });
      assert.strictEqual(
        results.filter((r) => r._tag === "Success" && r.success.kind === "accepted").length,
        1,
      );
      assert.strictEqual(
        results.filter(
          (r) =>
            r._tag === "Success" && r.success.kind === "rejected" && r.success.reason === "stale",
        ).length,
        1,
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          assert.deepStrictEqual(
            yield* sql`SELECT status, record_version, control_generation FROM workflow_runs`,
            [{ status: "paused", record_version: 2, control_generation: 1 }],
          );
          assert.deepStrictEqual(
            yield* sql`SELECT state, held, generation FROM workflow_native_launch_intents`,
            [{ state: "reserved", held: 1, generation: 0 }],
          );
          assert.strictEqual((yield* sql`SELECT * FROM workflow_native_intent_receipts`).length, 1);
          assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, 1);
        }).pipe(Effect.provide(fileLayer())),
      );
    }),
  ),
);
