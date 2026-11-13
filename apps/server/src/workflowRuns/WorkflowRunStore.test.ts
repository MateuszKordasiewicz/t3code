// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";

import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  CodexSettings,
  MessageId,
  EventId,
  CheckpointScopeId,
  OrchestrationV2Command,
  ProviderInstanceId,
  ProviderDriverKind,
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
import * as DateTime from "effect/DateTime";
import * as Path from "effect/Path";
import * as FileSystem from "effect/FileSystem";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import {
  encodeSubmissionTransportData,
  encodeScopeBindingData,
  encodeScopeSettlementData,
  snapshotSubmissionFrame,
} from "../orchestration-v2/ProviderSubmissionCodec.ts";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as CodexAdapter from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexErrors from "effect-codex-app-server/errors";
import type * as CodexProtocol from "effect-codex-app-server/protocol";

import { ChildProcess, ChildProcessSpawner } from "effect/process";
import * as Sink from "effect/Sink";
import * as Stream from "effect/Stream";
import * as Queue from "effect/Queue";
import * as NativeProtocol from "./WorkflowNativeCommandProtocol.ts";
import * as Duration from "effect/Duration";
import * as TestClock from "effect/testing/TestClock";
import * as Context from "effect/Context";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as ServerConfig from "../config.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as AuthSessions from "../persistence/AuthSessions.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as ExecutionAuthority from "./WorkflowExecutionAuthority.ts";
import * as Deferred from "effect/Deferred";
import * as Digest from "effect/crypto/Digest";
import * as Ref from "effect/Ref";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Statement from "effect/sql/Statement";
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
import {
  decodeNativeReservationLaunch,
  decodeNativeReservationMaterial,
} from "./WorkflowNativeReservationMaterial.ts";
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
const decodePlanningFinalizationValue = Schema.decodeUnknownEffect(
  Schema.Struct({
    session: PlanningSessionV1,
    pass: PlanningPassV1,
    operation: WorkflowOperationV1,
  }),
);
const decodeStoredFinalizationOperation = Schema.decodeUnknownEffect(WorkflowOperationV1);
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

const legacyReservation = (operation: WorkflowOperationV1, attemptId: string) => {
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

const reservation = (operation: WorkflowOperationV1, attemptId: string) => {
  const input = legacyReservation(operation, attemptId);
  return {
    ...input,
    materialVersion: 1 as const,
    launch: {
      commandId: input.launchCommandId,
      threadId: input.threadId,
      projectId: input.projectId,
      title: "Work",
      modelSelection: { instanceId: "provider", model: "model" },
      runtimeMode: "full-access",
      interactionMode: "default",
      workspaceStrategy: { type: "root", branch: "main" },
      initialMessage: { messageId: input.messageId, text: "Do the work", attachments: [] },
      createdBy: "agent",
      creationSource: "mcp",
    },
  };
};

const seedHistoricalReservation = (input: ReturnType<typeof legacyReservation>, version: 1 | 2) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const envelope = {
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
    };
    const fingerprint =
      version === 1
        ? WorkflowCanonicalHash.tagged("t3.workflow-native-launch-intent.v1", envelope)
        : WorkflowCanonicalHash.tagged("t3.workflow-native-launch-intent.v2", {
            ...envelope,
            receiptId: input.receiptId,
          });
    const owner = {
      ...run,
      access: { origins: [{ ...destination, threadId: "thread" }] },
      recordVersion: 1,
      allowance: { ...run.allowance, attemptsConsumed: 1 },
    };
    yield* sql`UPDATE workflow_runs SET record_version = 1, record_json = ${json(owner)}`;
    yield* sql`INSERT INTO workflow_native_launch_intents (intent_id, workflow_run_id, operation_id, attempt_id, item_id, charge_attempt, repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, generation, state, held) VALUES (${input.intentId}, ${input.runId}, ${input.operationId}, ${input.attemptId}, ${input.itemId}, 1, 0, 'implementation', ${input.environmentId}, ${input.projectId}, ${input.launchCommandId}, ${input.threadId}, ${input.messageId}, ${fingerprint}, ${json(input.draft)}, 0, 'reserved', 0)`;
    yield* sql`INSERT INTO workflow_native_intent_receipts VALUES (${input.draft.admission.command.commandId}, ${input.receiptId}, ${input.intentId}, ${fingerprint}, ${json({ intentId: input.intentId, fingerprint })})`;
    return { fingerprint, owner };
  });

// Tamper with isolated stored evidence after a valid reservation.

const storedV3 = (
  input: ReturnType<typeof reservation>,
  changes: {
    launch?: Record<string, unknown>;
    material?: Record<string, unknown>;
    markerHash?: string;
  } = {},
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    assert.strictEqual((yield* store.reserveWorkflowNativeIntent(input)).kind, "reserved");
    const launch = { ...input.launch, ...changes.launch };
    const { launchHash } = yield* decodeNativeReservationLaunch(launch);
    const draft = {
      ...input.draft,
      version: 2,
      material: { version: 1, launchHash: changes.markerHash ?? launchHash },
    };
    const fingerprint = WorkflowCanonicalHash.tagged("t3.workflow-native-launch-intent.v3", {
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
      draft,
      receiptId: input.receiptId,
    });
    const material = {
      version: 1,
      intentId: input.intentId,
      receiptId: input.receiptId,
      intentFingerprint: fingerprint,
      launch,
      planningSession: null,
      ...changes.material,
    };
    const { materialHash } = yield* decodeNativeReservationMaterial(material);
    yield* sql`UPDATE workflow_native_launch_intents SET draft_json = ${json(draft)}, fingerprint = ${fingerprint} WHERE intent_id = ${input.intentId}`;
    yield* sql`UPDATE workflow_native_intent_receipts SET fingerprint = ${fingerprint}, result_json = ${json({ intentId: input.intentId, fingerprint, materialVersion: 1, materialHash })} WHERE intent_id = ${input.intentId}`;
    yield* sql`DROP TRIGGER workflow_native_reservation_material_update_guard`;
    yield* sql`UPDATE workflow_native_reservation_material SET material_json = ${json(material)}, material_hash = ${materialHash} WHERE intent_id = ${input.intentId}`;
    yield* sql`CREATE TRIGGER workflow_native_reservation_material_update_guard BEFORE UPDATE ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT, 'workflow native reservation material is immutable'); END`;
    return { draft, fingerprint, material, materialHash };
  });

it.effect("stored V3 workflow material provides custody and retains bytes through controls", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const input = reservation(operationFor("v3-op", "v3-attempt"), "v3-attempt");
    const reserved = yield* store.reserveWorkflowNativeIntent(input);
    assert.strictEqual(reserved.kind, "reserved");
    assert.deepStrictEqual(yield* store.reserveWorkflowNativeIntent(input), {
      ...reserved,
      kind: "replay",
    });
    assert.strictEqual(
      (yield* Effect.flip(store.readNativeReservationInTransaction({ intentId: input.intentId })))
        .reason,
      "integrity",
    );
    const read = () =>
      sql.withTransaction(store.readNativeReservationInTransaction({ intentId: input.intentId }));
    const first = yield* read();
    assert.deepStrictEqual(first.owner, { kind: "workflow_run", workflowRunId: run.id });
    assert.deepStrictEqual<unknown>(first.material.launch, input.launch);
    assert.strictEqual(first.receiptId, input.receiptId);
    assert.strictEqual(first.state, "reserved");
    const selected = { intentId: input.intentId };
    let selectionChanged = false;
    const mutateSelection: Statement.Transformer = (statement) => {
      const [query, parameters] = statement.compile();
      return Effect.sync(() => {
        if (
          !selectionChanged &&
          query.includes("SELECT * FROM workflow_native_launch_intents WHERE intent_id") &&
          parameters.includes(input.intentId)
        ) {
          selected.intentId = "missing";
          selectionChanged = true;
        }
        return statement;
      });
    };
    const captured = yield* sql
      .withTransaction(store.readNativeReservationInTransaction(selected))
      .pipe(Effect.provideService(Statement.CurrentTransformer, mutateSelection));
    assert.isTrue(selectionChanged);
    assert.strictEqual(captured.intentId, input.intentId);
    assert.strictEqual(captured.material.intentId, input.intentId);
    (first.material.launch.initialMessage.attachments as unknown[]).push({ altered: true });
    assert.deepStrictEqual<unknown>((yield* read()).material.launch, input.launch);
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(store.readNativeReservationInTransaction({ intentId: "missing" })),
      )).reason,
      "integrity",
    );
    const before = yield* sql`SELECT draft_json, fingerprint FROM workflow_native_launch_intents`;
    const receipt = yield* sql`SELECT * FROM workflow_native_intent_receipts`;
    const material = yield* sql`SELECT * FROM workflow_native_reservation_material`;
    const outbox = yield* sql`SELECT * FROM orchestration_v2_effect_outbox`;
    assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), {
      kind: "owned",
      owner: { kind: "workflow_run", workflowRunId: run.id },
    });
    for (const [kind, version, generation] of [
      ["pause", 1, 0],
      ["resume", 2, 1],
      ["stop", 3, 2],
    ] as const) {
      assert.strictEqual(
        (yield* store.commitTransition({
          runId: run.id,
          command: {
            ...command,
            kind,
            commandId: `v3-${kind}`,
            expectedVersion: version,
            expectedGeneration: generation,
          },
          externalFacts: admissionFacts,
          receiptId: `v3-${kind}`,
        })).kind,
        "accepted",
      );
    }
    assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), {
      kind: "owned",
      owner: { kind: "workflow_run", workflowRunId: run.id },
    });
    assert.deepStrictEqual(
      yield* sql`SELECT draft_json, fingerprint FROM workflow_native_launch_intents`,
      before,
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, receipt);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM workflow_native_reservation_material`,
      material,
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, outbox);
    assert.deepStrictEqual(
      yield* store.reserveWorkflowNativeIntent({
        ...input,
        externalFacts: { ...admissionFacts, now: "2099-01-01T00:00:00Z" },
      }),
      { ...reserved, kind: "replay" },
    );
    const owner = parse(
      (yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`)[0]!.record_json,
    ) as typeof run;
    assert.strictEqual(owner.allowance.attemptsConsumed, 1);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "missing",
  "malformed",
  "swapped",
  "material-hash",
  "marker-hash",
  "marker-removed",
  "intent-id",
  "receipt-id",
  "intent-fingerprint",
  "planning-owner",
  "launch-command",
  "launch-thread",
  "launch-message",
  "launch-project",
  "provider",
  "model",
  "receipt",
  "receipt-hash",
  "downgrade",
] as const)("stored V3 rejects %s before custody or control writes", (scenario) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const input = reservation(operationFor("invalid-v3-op", "invalid-v3"), "invalid-v3");
    const launch =
      scenario === "launch-command"
        ? { commandId: "other-command" }
        : scenario === "launch-thread"
          ? { threadId: "other-thread" }
          : scenario === "launch-message"
            ? {
                initialMessage: {
                  messageId: "other-message",
                  text: "Do the work",
                  attachments: [],
                },
              }
            : scenario === "launch-project"
              ? { projectId: "other-project" }
              : scenario === "provider"
                ? { modelSelection: { instanceId: "other", model: "model" } }
                : scenario === "model"
                  ? { modelSelection: { instanceId: "provider", model: "other" } }
                  : undefined;
    const material =
      scenario === "intent-id"
        ? { intentId: "other-intent" }
        : scenario === "receipt-id"
          ? { receiptId: "other-receipt" }
          : scenario === "intent-fingerprint"
            ? { intentFingerprint: hash }
            : scenario === "planning-owner"
              ? { planningSession: session }
              : undefined;
    const fixture = yield* storedV3(input, {
      ...(launch ? { launch } : {}),
      ...(material ? { material } : {}),
      ...(scenario === "marker-hash" ? { markerHash: hash } : {}),
    });
    if (scenario === "swapped") {
      const otherRunId = decodeWorkflowRunId("other-run");
      yield* store.createRun({
        ...run,
        id: otherRunId,
        access: { origins: [{ ...destination, threadId: "other-thread" }] },
      });
      const original = operationFor("other-v3-op", "other-v3");
      if (original.parameters.kind !== "stage_launch") throw new Error("expected stage launch");
      const otherOperation = decodeOperation({
        ...original,
        owner: { kind: "workflow_run", workflowRunId: otherRunId },
        parameters: { ...original.parameters, nativeThreadId: "other-thread" },
      });
      const otherInput = { ...reservation(otherOperation, "other-v3"), runId: otherRunId };
      yield* storedV3(otherInput);
      const foreign = (yield* sql<{
        material_json: string;
        material_hash: string;
      }>`SELECT material_json, material_hash FROM workflow_native_reservation_material WHERE intent_id = ${otherInput.intentId}`)[0]!;
      yield* sql`DROP TRIGGER workflow_native_reservation_material_update_guard`;
      yield* sql`UPDATE workflow_native_reservation_material SET material_json = ${foreign.material_json}, material_hash = ${foreign.material_hash} WHERE intent_id = ${input.intentId}`;
      yield* sql`CREATE TRIGGER workflow_native_reservation_material_update_guard BEFORE UPDATE ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT, 'workflow native reservation material is immutable'); END`;
    }
    if (scenario === "missing" || scenario === "malformed" || scenario === "material-hash") {
      yield* sql`DROP TRIGGER workflow_native_reservation_material_update_guard`;
      yield* sql`DROP TRIGGER workflow_native_reservation_material_delete_guard`;
      if (scenario === "missing") yield* sql`DELETE FROM workflow_native_reservation_material`;
      if (scenario === "malformed")
        yield* sql`UPDATE workflow_native_reservation_material SET material_json = '{}'`;
      if (scenario === "material-hash")
        yield* sql`UPDATE workflow_native_reservation_material SET material_hash = ${hash}`;
      yield* sql`CREATE TRIGGER workflow_native_reservation_material_update_guard BEFORE UPDATE ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT, 'workflow native reservation material is immutable'); END`;
      yield* sql`CREATE TRIGGER workflow_native_reservation_material_delete_guard BEFORE DELETE ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT, 'workflow native reservation material is immutable'); END`;
    }
    if (scenario === "receipt")
      yield* sql`UPDATE workflow_native_intent_receipts SET result_json = ${json({ intentId: input.intentId, fingerprint: fixture.fingerprint })}`;
    if (scenario === "receipt-hash")
      yield* sql`UPDATE workflow_native_intent_receipts SET result_json = ${json({ intentId: input.intentId, fingerprint: fixture.fingerprint, materialVersion: 1, materialHash: hash })}`;
    if (scenario === "marker-removed") {
      const { material: _marker, ...withoutMarker } = fixture.draft;
      yield* sql`UPDATE workflow_native_launch_intents SET draft_json = ${json(withoutMarker)}`;
    }
    if (scenario === "downgrade") {
      const legacy = WorkflowCanonicalHash.tagged("t3.workflow-native-launch-intent.v2", {
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
      });
      yield* sql`UPDATE workflow_native_launch_intents SET fingerprint = ${legacy}`;
      yield* sql`UPDATE workflow_native_intent_receipts SET fingerprint = ${legacy}, result_json = ${json({ intentId: input.intentId, fingerprint: legacy })}`;
    }
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(store.readNativeReservationInTransaction({ intentId: input.intentId })),
      )).reason,
      "integrity",
    );
    const beforeRun = yield* sql`SELECT * FROM workflow_runs`;
    const beforeIntent = yield* sql`SELECT * FROM workflow_native_launch_intents`;
    const beforeReceipt = yield* sql`SELECT * FROM workflow_native_intent_receipts`;
    const beforeMaterial = yield* sql`SELECT * FROM workflow_native_reservation_material`;
    assert.strictEqual(
      (yield* Effect.flip(store.readThreadCustody({ threadId: input.threadId }))).reason,
      "integrity",
    );
    assert.strictEqual(
      (yield* Effect.flip(
        store.commitTransition({
          runId: run.id,
          command: { ...command, commandId: "bad-v3-pause", expectedVersion: 1 },
          externalFacts: admissionFacts,
          receiptId: "bad-v3-pause",
        }),
      )).reason,
      "integrity",
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, beforeRun);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, beforeIntent);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM workflow_native_intent_receipts`,
      beforeReceipt,
    );
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM workflow_native_reservation_material`,
      beforeMaterial,
    );
    assert.strictEqual((yield* sql`SELECT * FROM workflow_command_receipts`).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("stored V3 unknown custody remains readable without control eligibility", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const input = reservation(operationFor("unknown-v3-op", "unknown-v3"), "unknown-v3");
    yield* storedV3(input);
    yield* sql`UPDATE workflow_native_launch_intents SET state = 'unknown', boot_id = 'boot', worker_id = 'worker'`;
    assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), {
      kind: "owned",
      owner: { kind: "workflow_run", workflowRunId: run.id },
    });
  }).pipe(Effect.provide(layer)),
);

it.effect("stored V3 material SQL failures map to persistence", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const input = reservation(operationFor("sql-v3-op", "sql-v3"), "sql-v3");
    yield* storedV3(input);
    yield* sql`DROP TABLE workflow_native_reservation_material`;
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(store.readNativeReservationInTransaction({ intentId: input.intentId })),
      )).reason,
      "persistence",
    );
    assert.strictEqual(
      (yield* Effect.flip(store.readThreadCustody({ threadId: input.threadId }))).reason,
      "persistence",
    );
    assert.strictEqual(
      (yield* Effect.flip(
        store.commitTransition({
          runId: run.id,
          command: { ...command, commandId: "sql-v3-pause", expectedVersion: 1 },
          externalFacts: admissionFacts,
          receiptId: "sql-v3-pause",
        }),
      )).reason,
      "persistence",
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("unanchored material does not promote a legacy workflow reservation", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const input = legacyReservation(
      operationFor("old-material-op", "old-material"),
      "old-material",
    );
    yield* seedHistoricalReservation(input, 2);
    yield* sql`INSERT INTO workflow_native_reservation_material VALUES (${input.intentId}, '{}', ${hash})`;
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(store.readNativeReservationInTransaction({ intentId: input.intentId })),
      )).reason,
      "integrity",
    );
    assert.strictEqual(
      (yield* store.readThreadCustody({ threadId: input.threadId })).kind,
      "owned",
    );
    assert.strictEqual(
      (yield* store.commitTransition({
        runId: run.id,
        command: { ...command, commandId: "old-material-pause", expectedVersion: 1 },
        externalFacts: admissionFacts,
        receiptId: "old-material-pause",
      })).kind,
      "accepted",
    );
  }).pipe(Effect.provide(layer)),
);

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
      WorkflowCanonicalHash.tagged("t3.workflow-native-launch-intent.v3", {
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
        draft: {
          ...input.draft,
          version: 2,
          material: {
            version: 1,
            launchHash: (yield* decodeNativeReservationLaunch(input.launch)).launchHash,
          },
        },
        receiptId: input.receiptId,
      }),
    );
    const snapshot = {
      owner: yield* sql`SELECT * FROM workflow_runs`,
      intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
      material: yield* sql`SELECT * FROM workflow_native_reservation_material`,
      receipt: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
    };
    assert.strictEqual(
      (yield* Effect.flip(
        store.reserveWorkflowNativeIntent(legacyReservation(operation, "reservation-attempt")),
      )).reason,
      "integrity",
    );
    for (const changed of [
      { ...input, launch: { ...input.launch, title: "Changed launch" } },
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
    assert.deepStrictEqual(
      {
        owner: yield* sql`SELECT * FROM workflow_runs`,
        intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
        material: yield* sql`SELECT * FROM workflow_native_reservation_material`,
        receipt: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
      },
      snapshot,
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

it.effect("freezes reservation material before SQL can yield to caller mutation", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const input = reservation(operationFor("snapshot-op", "snapshot"), "snapshot");
    const payload = { item: "original" };
    const launch = {
      ...input.launch,
      initialMessage: {
        ...input.launch.initialMessage,
        context: {
          version: 1,
          records: [
            { version: 1, contextId: "ctx", kind: "future-kind", label: "Context", payload },
          ],
        },
      },
    };
    let mutated = false;
    const mutate: Statement.Transformer = (statement) => {
      const [query] = statement.compile();
      return Effect.sync(() => {
        if (query.includes("UPDATE workflow_runs SET status")) {
          payload.item = "changed";
          mutated = true;
        }
        return statement;
      });
    };
    const result = yield* store
      .reserveWorkflowNativeIntent({ ...input, launch })
      .pipe(Effect.provideService(Statement.CurrentTransformer, mutate));
    assert.strictEqual(result.kind, "reserved");
    assert.isTrue(mutated);
    assert.strictEqual(payload.item, "changed");
    assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), {
      kind: "owned",
      owner: { kind: "workflow_run", workflowRunId: run.id },
    });
    const rows = yield* sql<{
      material_json: string;
      material_hash: string;
    }>`SELECT material_json, material_hash FROM workflow_native_reservation_material WHERE intent_id = ${input.intentId}`;
    const { material, materialHash } = yield* decodeNativeReservationMaterial(
      parse(rows[0]!.material_json),
    );
    assert.strictEqual(materialHash, rows[0]!.material_hash);
    assert.deepStrictEqual(
      material.launch.initialMessage.context?.records.map((record) =>
        "payload" in record ? record.payload : undefined,
      ),
      [{ item: "original" }],
    );
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "fresh legacy and partial material requests fail without writes; historical V2 replays",
  () =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const operation = operationFor("old-v2-op", "old-v2");
      const input = legacyReservation(operation, "old-v2");
      const before = yield* sql`SELECT * FROM workflow_runs`;
      for (const invalid of [
        input,
        { ...input, materialVersion: 1 },
        { ...input, launch: {} },
        { ...input, materialVersion: 2, launch: {} },
      ]) {
        assert.strictEqual(
          (yield* Effect.flip(
            store.reserveWorkflowNativeIntent(
              invalid as Parameters<typeof store.reserveWorkflowNativeIntent>[0],
            ),
          )).reason,
          "integrity",
        );
      }
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_reservation_material`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, []);
      const { fingerprint } = yield* seedHistoricalReservation(input, 2);
      const historical = {
        owner: yield* sql`SELECT * FROM workflow_runs`,
        intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
        material: yield* sql`SELECT * FROM workflow_native_reservation_material`,
        receipt: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
      };
      assert.deepStrictEqual(yield* store.reserveWorkflowNativeIntent(input), {
        kind: "replay",
        receiptId: input.receiptId,
        intentId: input.intentId,
        fingerprint,
      });
      assert.strictEqual(
        (yield* Effect.flip(store.reserveWorkflowNativeIntent(reservation(operation, "old-v2"))))
          .reason,
        "integrity",
      );
      assert.deepStrictEqual(
        {
          owner: yield* sql`SELECT * FROM workflow_runs`,
          intent: yield* sql`SELECT * FROM workflow_native_launch_intents`,
          material: yield* sql`SELECT * FROM workflow_native_reservation_material`,
          receipt: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        },
        historical,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["material", "receipt"] as const)(
  "reservation %s insertion failure rolls back and does not notify",
  (fault) =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setupAdmission;
        const sql = yield* SqlClient.SqlClient;
        const store = yield* Store.WorkflowRunStore;
        const subscription = yield* store.subscribeChanges;
        if (fault === "material")
          yield* sql`CREATE TRIGGER fail_reservation BEFORE INSERT ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT, 'fault'); END`;
        else
          yield* sql`CREATE TRIGGER fail_reservation BEFORE INSERT ON workflow_native_intent_receipts BEGIN SELECT RAISE(ABORT, 'fault'); END`;
        const before = yield* sql`SELECT * FROM workflow_runs`;
        const input = reservation(operationFor("rollback-op", "rollback"), "rollback");
        assert.strictEqual(
          (yield* Effect.flip(store.reserveWorkflowNativeIntent(input))).reason,
          "persistence",
        );
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_reservation_material`, []);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, []);
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        yield* sql`DROP TRIGGER fail_reservation`;
        const result = yield* store.reserveWorkflowNativeIntent(input);
        assert.strictEqual(result.kind, "reserved");
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 1);
        assert.strictEqual((yield* store.reserveWorkflowNativeIntent(input)).kind, "replay");
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      }),
    ).pipe(Effect.provide(layer)),
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
    materialVersion: 1 as const,
    launch: {
      commandId: `launch-${pass.id}`,
      threadId: admission.pass.native!.threadId,
      projectId: admission.context.destination.projectId,
      title: "Planning",
      modelSelection: { instanceId: pass.provider.providerInstanceId, model: pass.provider.model },
      runtimeMode: "full-access",
      interactionMode: "default",
      workspaceStrategy: { type: "root", branch: "main" },
      initialMessage: { messageId: `message-${pass.id}`, text: "Plan the work", attachments: [] },
      createdBy: "agent",
      creationSource: "mcp",
    },
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
    const selectedInput = {
      threadId: "unrelated",
      providerSessionIds: [],
      providerThreadIds: ["owned-allocation"],
    };
    assert.deepStrictEqual(
      yield* sql.withTransaction(store.readRecoveryCustodyInTransaction(selectedInput)),
      { kind: "owned" },
    );
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
    yield* seedPhysicalNode("owner-node", input.threadId);
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET thread_id = NULL, owner_node_id = 'owner-node', payload_json = ${json({ ...allocation, appThreadId: null, ownerNodeId: "owner-node" })}`;
    assert.deepStrictEqual(yield* read, { kind: "owned" });
    yield* sql`DELETE FROM orchestration_v2_projection_nodes`;
    assert.strictEqual((yield* Effect.flip(read)).reason, "integrity");
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET owner_node_id = NULL, payload_json = ${json({ ...allocation, appThreadId: null })}`;
    assert.strictEqual((yield* Effect.flip(read)).reason, "integrity");
    yield* seedPhysicalNode("parent", input.threadId);
    yield* seedPhysicalSubagent("subagent", input.threadId, "parent", "allocated-thread");
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

it.effect("recovery custody validates selected provider threads and follows their owners", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const input = reservation(operationFor("selected-owned", "selected-owned"), "selected-owned");
    yield* store.reserveWorkflowNativeIntent(input);
    const reference = {
      id: "selected",
      driver: "codex",
      providerInstanceId: "provider",
      providerSessionId: null,
      appThreadId: input.threadId,
      ownerNodeId: null,
      nativeThreadRef: { driver: "codex", nativeId: "existing", strength: "strong" },
      nativeConversationHeadRef: { driver: "codex", nativeId: "head", strength: "strong" },
      status: "active",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [{ kind: "command", taskId: "background" }],
      createdAt: "2026-11-01T00:00:00Z",
      updatedAt: "2026-11-01T00:00:00Z",
    };
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads (provider_thread_id, thread_id, owner_node_id, provider, driver, provider_instance_id, provider_session_id, status, updated_at, payload_json) VALUES ('selected', ${input.threadId}, NULL, 'provider', 'codex', 'provider', NULL, 'active', '2026-11-01T00:00:00Z', ${json(reference)})`;
    yield* sql`INSERT INTO orchestration_v2_projection_provider_threads (provider_thread_id, thread_id, owner_node_id, provider, status, updated_at, payload_json) VALUES ('unrelated-bad', NULL, NULL, 'provider', 'active', '2026-11-01T00:00:00Z', '{}')`;
    const read = (providerThreadIds: readonly string[]) => {
      const selection = { threadId: "unrelated", providerSessionIds: [], providerThreadIds };
      return sql.withTransaction(store.readRecoveryCustodyInTransaction(selection));
    };
    const subscription = yield* store.subscribeChanges;
    const snapshot = Effect.gen(function* () {
      return {
        workflow: yield* rawReservationState,
        references: yield* sql`SELECT * FROM orchestration_v2_projection_provider_threads`,
        nodes: yield* sql`SELECT * FROM orchestration_v2_projection_nodes`,
        subagents: yield* sql`SELECT * FROM orchestration_v2_projection_subagents`,
        sessions: yield* sql`SELECT * FROM orchestration_v2_projection_provider_sessions`,
        bindings: yield* sql`SELECT * FROM orchestration_v2_projection_provider_session_bindings`,
        events: yield* sql`SELECT * FROM orchestration_events`,
      };
    });
    const before = yield* snapshot;
    assert.deepStrictEqual(yield* read(["selected", "selected"]), { kind: "owned" });
    assert.deepStrictEqual(yield* read([]), { kind: "ordinary" });
    assert.deepStrictEqual(
      yield* sql.withTransaction(
        store.readRecoveryCustodyInTransaction({ threadId: "unrelated", providerSessionIds: [] }),
      ),
      { kind: "ordinary" },
    );
    for (const ids of [["missing"], [""], ["selected", "missing"], ["selected", "unrelated-bad"]]) {
      assert.strictEqual((yield* Effect.flip(read(ids))).reason, "integrity");
    }
    assert.deepStrictEqual(yield* snapshot, before);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);

    yield* seedPhysicalNode("owner-node", input.threadId);
    const byNode = { ...reference, appThreadId: null, ownerNodeId: "owner-node" };
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET thread_id = NULL, owner_node_id = 'owner-node', payload_json = ${json(byNode)} WHERE provider_thread_id = 'selected'`;
    assert.deepStrictEqual(yield* read(["selected"]), { kind: "owned" });
    yield* sql`DELETE FROM orchestration_v2_projection_nodes WHERE node_id = 'owner-node'`;
    assert.strictEqual((yield* Effect.flip(read(["selected"]))).reason, "integrity");
    const unattributed = { ...reference, appThreadId: null };
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET owner_node_id = NULL, payload_json = ${json(unattributed)} WHERE provider_thread_id = 'selected'`;
    assert.strictEqual((yield* Effect.flip(read(["selected"]))).reason, "integrity");
    yield* seedPhysicalNode("parent", input.threadId);
    yield* seedPhysicalSubagent("selected-subagent", input.threadId, "parent", "selected");
    assert.deepStrictEqual(yield* read(["selected"]), { kind: "owned" });
    yield* sql`DELETE FROM orchestration_v2_projection_subagents WHERE subagent_id = 'selected-subagent'`;
    const ordinary = { ...reference, appThreadId: "ordinary" };
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET thread_id = 'ordinary', payload_json = ${json(ordinary)} WHERE provider_thread_id = 'selected'`;
    assert.deepStrictEqual(yield* read(["selected"]), { kind: "ordinary" });
    yield* seedPhysicalSession("selected-session");
    yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings VALUES ('selected-session', ${input.threadId})`;
    const shared = { ...ordinary, providerSessionId: "selected-session" };
    yield* sql`UPDATE orchestration_v2_projection_provider_threads SET provider_session_id = 'selected-session', payload_json = ${json(shared)} WHERE provider_thread_id = 'selected'`;
    assert.deepStrictEqual(yield* read(["selected"]), { kind: "owned" });
    for (const invalid of [
      { ...shared, id: "mismatch" },
      { ...shared, appThreadId: "mismatch" },
      { ...shared, ownerNodeId: "mismatch" },
      { ...shared, providerSessionId: "mismatch" },
      { ...shared, status: "not_loaded" },
      { ...shared, driver: "claude" },
      { ...shared, providerInstanceId: "mismatch" },
      {},
    ]) {
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json = ${json(invalid)} WHERE provider_thread_id = 'selected'`;
      assert.strictEqual((yield* Effect.flip(read(["selected"]))).reason, "integrity");
    }
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
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

it.effect("planning material V3 remains unsupported by custody and controls", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    yield* begin(store, session);
    const input = planningReservation(admissionInput(binding, "unsupported-v3"));
    yield* store.reservePlanningNativeIntent(input);
    const marked = { ...input.draft, version: 2, material: { version: 1, launchHash: hash } };
    yield* sql`UPDATE workflow_native_launch_intents SET draft_json = ${json(marked)} WHERE intent_id = ${input.intentId}`;
    assert.strictEqual(
      (yield* Effect.flip(store.readThreadCustody({ threadId: input.threadId }))).reason,
      "integrity",
    );
    const before = yield* sql`SELECT * FROM workflow_planning_sessions`;
    assert.strictEqual(
      (yield* Effect.flip(
        store.controlPlanning({
          sessionId: session.id,
          expectedVersion: 1,
          action: "pause",
          now: facts.now,
          context,
          command: { commandId: "unsupported-v3-pause", receiptId: "unsupported-v3-pause" },
        }),
      )).reason,
      "integrity",
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before);
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
      const planningBase = planningReservation(admissionInput(binding, "foreign-owner"));
      const planning = {
        ...planningBase,
        threadId: first.threadId,
        launch: { ...planningBase.launch, threadId: first.threadId },
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
    const planningBase = planningReservation(admissionInput(binding, "planning-owner"));
    const planning = {
      ...planningBase,
      threadId: "thread",
      launch: { ...planningBase.launch, threadId: "thread" },
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
          launch: { ...input.launch, threadId: "empty" },
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

it.effect.each([
  "draft",
  "missing-command",
  "fingerprint",
  "receipt",
  "custody",
  "generation",
] as const)("planning control rejects tampered %s before writes", (tamper) =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const input = planningReservation(admissionInput(binding, "tamper"));
    yield* store.reservePlanningNativeIntent(input);
    if (tamper === "draft")
      yield* sql`UPDATE workflow_native_launch_intents SET draft_json = json_set(draft_json, '$.pass.inputManifest.sources[0].sha256', ${hash})`;
    if (tamper === "missing-command")
      yield* sql`UPDATE workflow_native_launch_intents SET draft_json = json_remove(draft_json, '$.admission.command')`;
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

it.effect("planning material retains the exact charged snapshot through controls and replay", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      const prior = { ...session, recordVersion: 1, consumed: { passes: 1, repairRounds: 0 } };
      yield* sql`UPDATE workflow_planning_sessions SET record_version = 1, record_json = ${json(prior)} WHERE id = ${session.id}`;
      const admission = { ...admissionInput(binding, "snapshot"), expectedVersion: 1 };
      const input = planningReservation(admission);
      const subscription = yield* store.subscribeChanges;
      const reserved = yield* store.reservePlanningNativeIntent(input);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 1);
      const owner = yield* store.getPlanning({ sessionId: session.id });
      const reservation = yield* sql.withTransaction(
        store.readNativeReservationInTransaction({ intentId: input.intentId }),
      );
      assert.deepStrictEqual(reservation.owner, {
        kind: "planning_session",
        planningSessionId: session.id,
      });
      assert.deepStrictEqual(reservation.material.planningSession, owner);
      const rows = yield* sql<{
        material_json: string;
        material_hash: string;
      }>`SELECT material_json, material_hash FROM workflow_native_reservation_material`;
      const decoded = yield* decodeNativeReservationMaterial(parse(rows[0]!.material_json));
      assert.deepStrictEqual(decoded.material.planningSession, owner);
      assert.deepStrictEqual(owner.consumed, { passes: 2, repairRounds: 0 });
      assert.strictEqual(decoded.materialHash, rows[0]!.material_hash);
      const before = yield* sql`SELECT * FROM workflow_native_reservation_material`;
      const receipt = yield* sql`SELECT * FROM workflow_native_intent_receipts`;
      for (const [action, version] of [
        ["pause", 2],
        ["resume", 3],
      ] as const) {
        yield* store.controlPlanning({
          sessionId: session.id,
          action,
          expectedVersion: version,
          now: facts.now,
          context,
          command: { commandId: `snapshot-${action}`, receiptId: `snapshot-${action}` },
        });
      }
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_reservation_material`,
        before,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, receipt);
      const current = yield* store.getPlanning({ sessionId: session.id });
      assert.strictEqual(current.controlGeneration, 2);
      assert.strictEqual(decoded.material.planningSession!.controlGeneration, 0);
      yield* PubSub.takeUpTo(subscription, 10);
      assert.deepStrictEqual(
        yield* store.reservePlanningNativeIntent({
          ...input,
          generation: 999,
          now: "2099-01-01T00:00:00Z",
        }),
        { ...reserved, kind: "replay" },
      );
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      assert.deepStrictEqual(yield* store.getPlanning({ sessionId: session.id }), current);
      assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), {
        kind: "owned",
        owner: { kind: "planning_session", planningSessionId: session.id },
      });
    }),
  ).pipe(Effect.provide(layer)),
);

it.effect("planning reservation snapshots caller context before the owner write", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const input = planningReservation(admissionInput(binding, "alias"));
    const payload = { item: "original" };
    const launch = {
      ...input.launch,
      initialMessage: {
        ...input.launch.initialMessage,
        context: {
          version: 1,
          records: [
            { version: 1, contextId: "ctx", kind: "future-kind", label: "Context", payload },
          ],
        },
      },
    };
    const originalLaunch = structuredClone(launch);
    let mutated = false;
    const mutate: Statement.Transformer = (statement) => {
      const [query] = statement.compile();
      return Effect.sync(() => {
        if (query.includes("UPDATE workflow_planning_sessions SET record_version")) {
          payload.item = "changed";
          mutated = true;
        }
        return statement;
      });
    };
    const reserved = yield* store
      .reservePlanningNativeIntent({ ...input, launch })
      .pipe(Effect.provideService(Statement.CurrentTransformer, mutate));
    assert.isTrue(mutated);
    assert.strictEqual(payload.item, "changed");
    const row = (yield* sql<{
      material_json: string;
    }>`SELECT material_json FROM workflow_native_reservation_material`)[0]!;
    const { material } = yield* decodeNativeReservationMaterial(parse(row.material_json));
    assert.deepStrictEqual(material.launch as unknown, originalLaunch);
    assert.deepStrictEqual(
      yield* store.reservePlanningNativeIntent({ ...input, launch: originalLaunch }),
      { ...reserved, kind: "replay" },
    );
  }).pipe(Effect.provide(layer)),
);

it.effect("historical planning V1 replays and controls without promotion to material", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const materialized = planningReservation(admissionInput(binding, "historical"));
    const { materialVersion: _version, launch: _launch, ...input } = materialized;
    const fingerprint = WorkflowCanonicalHash.tagged("t3.workflow-native-launch-intent.v1", {
      intentId: input.intentId,
      workflowRunId: null,
      planningSessionId: input.sessionId,
      operationId: input.operationId,
      attemptId: null,
      passId: input.passId,
      itemId: null,
      chargeAttempt: 0,
      repair: false,
      role: input.role,
      environmentId: input.environmentId,
      projectId: input.projectId,
      launchCommandId: input.launchCommandId,
      threadId: input.threadId,
      messageId: input.messageId,
      draft: input.draft,
    });
    const owner = { ...session, recordVersion: 1, consumed: { passes: 1, repairRounds: 0 } };
    yield* sql`UPDATE workflow_planning_sessions SET record_version = 1, record_json = ${json(owner)} WHERE id = ${session.id}`;
    yield* sql`INSERT INTO workflow_native_launch_intents (intent_id, planning_session_id, operation_id, pass_id, charge_attempt, repair, role, environment_id, project_id, launch_command_id, thread_id, message_id, fingerprint, draft_json, generation, state, held) VALUES (${input.intentId}, ${input.sessionId}, ${input.operationId}, ${input.passId}, 0, 0, ${input.role}, ${input.environmentId}, ${input.projectId}, ${input.launchCommandId}, ${input.threadId}, ${input.messageId}, ${fingerprint}, ${json(input.draft)}, 0, 'reserved', 0)`;
    yield* sql`INSERT INTO workflow_native_intent_receipts VALUES (${input.draft.admission.command.commandId}, ${input.draft.admission.command.receiptId}, ${input.intentId}, ${fingerprint}, ${json({ intentId: input.intentId, fingerprint })})`;
    const unanchored = yield* decodeNativeReservationMaterial({
      version: 1,
      intentId: input.intentId,
      receiptId: input.draft.admission.command.receiptId,
      intentFingerprint: fingerprint,
      launch: materialized.launch,
      planningSession: owner,
    });
    yield* sql`INSERT INTO workflow_native_reservation_material (intent_id, material_json, material_hash) VALUES (${input.intentId}, ${json(unanchored.material)}, ${unanchored.materialHash})`;
    const expected = {
      kind: "replay",
      receiptId: input.draft.admission.command.receiptId,
      intentId: input.intentId,
      fingerprint,
    };
    assert.deepStrictEqual(yield* store.reservePlanningNativeIntent(input), expected);
    assert.strictEqual(
      (yield* Effect.flip(store.reservePlanningNativeIntent(materialized))).reason,
      "conflict",
    );
    yield* store.controlPlanning({
      sessionId: session.id,
      action: "pause",
      expectedVersion: 1,
      now: facts.now,
      context,
      command: { commandId: "legacy-pause", receiptId: "legacy-pause" },
    });
    assert.deepStrictEqual(yield* store.reservePlanningNativeIntent(input), expected);
    assert.deepStrictEqual(yield* store.readThreadCustody({ threadId: input.threadId }), {
      kind: "owned",
      owner: { kind: "planning_session", planningSessionId: session.id },
    });
    assert.strictEqual((yield* sql`SELECT * FROM workflow_native_reservation_material`).length, 1);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "planning rejects fresh legacy, cross-format, malformed and changed launch without writes",
  () =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      const input = planningReservation(admissionInput(binding, "formats"));
      const { materialVersion: _version, launch: _launch, ...legacy } = input;
      const before = yield* sql`SELECT * FROM workflow_planning_sessions`;
      for (const candidate of [
        legacy,
        { ...legacy, materialVersion: 1 },
        { ...input, materialVersion: 2 },
        { ...legacy, launch: input.launch },
      ]) {
        assert.strictEqual(
          (yield* Effect.result(store.reservePlanningNativeIntent(candidate as typeof input)))._tag,
          "Failure",
        );
      }
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
      yield* store.reservePlanningNativeIntent(input);
      const saved = yield* sql`SELECT * FROM workflow_native_reservation_material`;
      assert.strictEqual(
        (yield* Effect.result(store.reservePlanningNativeIntent(legacy)))._tag,
        "Failure",
      );
      assert.strictEqual(
        (yield* Effect.result(
          store.reservePlanningNativeIntent({
            ...input,
            launch: { ...input.launch, title: "Changed" },
          }),
        ))._tag,
        "Failure",
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_reservation_material`, saved);
      assert.strictEqual((yield* store.getPlanning({ sessionId: session.id })).consumed.passes, 1);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "missing",
  "hash",
  "receipt",
  "snapshot-plan",
  "snapshot-null",
  "snapshot-actor",
  "snapshot-version",
  "snapshot-generation",
  "snapshot-count",
  "snapshot-role",
] as const)("planning %s material tampering fails replay, control and custody", (scenario) =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    yield* begin(store, session);
    const input = planningReservation(admissionInput(binding, "tamper-material"));
    yield* store.reservePlanningNativeIntent(input);
    if (scenario === "receipt")
      yield* sql`UPDATE workflow_native_intent_receipts SET result_json = '{}'`;
    else {
      yield* sql`DROP TRIGGER workflow_native_reservation_material_update_guard`;
      yield* sql`DROP TRIGGER workflow_native_reservation_material_delete_guard`;
      if (scenario === "missing") yield* sql`DELETE FROM workflow_native_reservation_material`;
      if (scenario === "hash")
        yield* sql`UPDATE workflow_native_reservation_material SET material_hash = ${hash}`;
      if (scenario.startsWith("snapshot-")) {
        const row = (yield* sql<{
          material_json: string;
        }>`SELECT material_json FROM workflow_native_reservation_material`)[0]!;
        const decoded = yield* decodeNativeReservationMaterial(parse(row.material_json));
        const snapshot = decoded.material.planningSession!;
        const changes =
          scenario === "snapshot-plan"
            ? { planId: "other-plan" }
            : scenario === "snapshot-actor"
              ? { origin: { ...snapshot.origin, id: "other" } }
              : scenario === "snapshot-version"
                ? { recordVersion: 2 }
                : scenario === "snapshot-generation"
                  ? { controlGeneration: 1 }
                  : scenario === "snapshot-count"
                    ? { consumed: { passes: 0, repairRounds: 0 } }
                    : scenario === "snapshot-role"
                      ? { allowedRoles: ["review"] }
                      : {};
        const altered = {
          ...decoded.material,
          planningSession: scenario === "snapshot-null" ? null : { ...snapshot, ...changes },
        };
        const { materialHash } = yield* decodeNativeReservationMaterial(altered);
        yield* sql`UPDATE workflow_native_reservation_material SET material_json = ${json(altered)}, material_hash = ${materialHash}`;
        yield* sql`UPDATE workflow_native_intent_receipts SET result_json = ${json({ intentId: input.intentId, fingerprint: decoded.material.intentFingerprint, materialVersion: 1, materialHash })}`;
      }
      yield* sql`CREATE TRIGGER workflow_native_reservation_material_update_guard BEFORE UPDATE ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT, 'workflow native reservation material is immutable'); END`;
      yield* sql`CREATE TRIGGER workflow_native_reservation_material_delete_guard BEFORE DELETE ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT, 'workflow native reservation material is immutable'); END`;
    }
    const before = yield* sql`SELECT * FROM workflow_planning_sessions`;
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(store.readNativeReservationInTransaction({ intentId: input.intentId })),
      )).reason,
      "integrity",
    );
    assert.strictEqual(
      (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
      "integrity",
    );
    assert.strictEqual(
      (yield* Effect.flip(store.readThreadCustody({ threadId: input.threadId }))).reason,
      "integrity",
    );
    assert.strictEqual(
      (yield* Effect.flip(
        store.controlPlanning({
          sessionId: session.id,
          action: "pause",
          expectedVersion: 1,
          now: facts.now,
          context,
          command: { commandId: "tamper-control", receiptId: "tamper-control" },
        }),
      )).reason,
      "integrity",
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "planning material SELECT errors retain storage and custody persistence classification",
  () =>
    Effect.gen(function* () {
      const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* begin(store, session);
      const input = planningReservation(admissionInput(binding, "material-sql"));
      yield* store.reservePlanningNativeIntent(input);
      yield* sql`ALTER TABLE workflow_native_reservation_material RENAME TO unavailable_material`;
      assert.strictEqual(
        (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
        "storage",
      );
      assert.strictEqual(
        (yield* Effect.flip(store.readThreadCustody({ threadId: input.threadId }))).reason,
        "persistence",
      );
      assert.strictEqual(
        (yield* Effect.flip(
          store.controlPlanning({
            sessionId: session.id,
            action: "pause",
            expectedVersion: 1,
            now: facts.now,
            context,
            command: { commandId: "sql-pause", receiptId: "sql-pause" },
          }),
        )).reason,
        "storage",
      );
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
          const charged = yield* store.getPlanning({ sessionId: session.id });
          const sql = yield* SqlClient.SqlClient;
          const material = yield* sql<{
            material_json: string;
            material_hash: string;
          }>`SELECT * FROM workflow_native_reservation_material`;
          const decoded = yield* decodeNativeReservationMaterial(parse(material[0]!.material_json));
          assert.deepStrictEqual(decoded.material.planningSession, charged);
          yield* store.controlPlanning({
            sessionId: session.id,
            action: "stop",
            expectedVersion: 1,
            now: facts.now,
            context,
            command: { commandId: "reopen-stop", receiptId: "reopen-stop" },
          });
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM workflow_native_reservation_material`,
            material,
          );
          return { admission, input, reserved, material, charged };
        }).pipe(Effect.provide(fileLayer)),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* Store.WorkflowRunStore;
          const sql = yield* SqlClient.SqlClient;
          assert.deepStrictEqual(
            yield* sql`SELECT * FROM workflow_native_reservation_material`,
            saved.material,
          );
          const decoded = yield* decodeNativeReservationMaterial(
            parse(saved.material[0]!.material_json),
          );
          assert.deepStrictEqual(decoded.material.planningSession, saved.charged);
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
        store.reservePlanningNativeIntent({
          ...reviewInput,
          threadId: firstIntent.threadId,
          launch: { ...reviewInput.launch, threadId: firstIntent.threadId },
        }),
      )).reason,
      "custody",
    );
    yield* sql`UPDATE workflow_native_launch_intents SET state = 'cancelled' WHERE intent_id = ${firstIntent.intentId}`;
    assert.strictEqual(
      (yield* Effect.flip(
        store.reservePlanningNativeIntent({
          ...reviewInput,
          threadId: firstIntent.threadId,
          launch: { ...reviewInput.launch, threadId: firstIntent.threadId },
        }),
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
        launch: {
          ...input.launch,
          commandId: input.launchCommandId,
          threadId: input.threadId,
          initialMessage: { ...input.launch.initialMessage, messageId: input.messageId },
        },
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

it.effect.each(["material", "receipt"] as const)(
  "planning %s insertion rolls back without notification",
  (fault) =>
    Effect.scoped(
      Effect.gen(function* () {
        const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
        const store = yield* Store.WorkflowRunStore;
        const sql = yield* SqlClient.SqlClient;
        yield* begin(store, session);
        const subscription = yield* store.subscribeChanges;
        if (fault === "material")
          yield* sql`CREATE TRIGGER reject_planning BEFORE INSERT ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT, 'failure'); END`;
        else
          yield* sql`CREATE TRIGGER reject_planning BEFORE INSERT ON workflow_native_intent_receipts BEGIN SELECT RAISE(ABORT, 'failure'); END`;
        const input = planningReservation(admissionInput(binding, "rollback"));
        const owner = yield* sql`SELECT * FROM workflow_planning_sessions`;
        assert.strictEqual(
          (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
          "storage",
        );
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, owner);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_reservation_material`, []);
        assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, []);
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        yield* sql`DROP TRIGGER reject_planning`;
        assert.strictEqual((yield* store.reservePlanningNativeIntent(input)).kind, "reserved");
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 1);
        assert.strictEqual((yield* store.reservePlanningNativeIntent(input)).kind, "replay");
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      }),
    ).pipe(Effect.provide(layer)),
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
    const input = legacyReservation(operationFor("v1-op", "v1-attempt"), "v1-attempt");
    const { fingerprint: legacy, owner } = yield* seedHistoricalReservation(input, 1);
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
    assert.strictEqual(
      (yield* Effect.flip(
        store.reserveWorkflowNativeIntent(
          reservation(operationFor("v1-op", "v1-attempt"), "v1-attempt"),
        ),
      )).reason,
      "integrity",
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

const authorizedStore = Effect.fn("test.authorizedStore")(function* (ttl?: Duration.Duration) {
  const sql = yield* SqlClient.SqlClient;
  const identity = {
    getEnvironmentId: Effect.succeed(EnvironmentId.make(destination.environmentId)),
  };
  const platform = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
  const base = Layer.mergeAll(
    Layer.succeed(SqlClient.SqlClient, sql),
    Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, identity),
    ManagedPolicy.layerTest(),
    ServerConfig.layerTest(process.cwd(), { prefix: "workflow-authorized-store-" }),
  ).pipe(Layer.provideMerge(platform));
  const scope = yield* Layer.build(
    Layer.fresh(
      SessionStore.layer.pipe(
        Layer.provideMerge(ServerSecretStore.layer.pipe(Layer.provideMerge(base))),
      ),
    ),
  );
  const sessions = Context.get(scope, SessionStore.SessionStore);
  const issued = yield* sessions.issue({
    ...(ttl === undefined ? {} : { ttl }),
    subject: "human",
    method: "bearer-access-token",
    scopes: ["orchestration:read", "orchestration:operate"],
  });
  const authority = ExecutionAuthority.captureLocalRpcExecutionAuthority(
    yield* sessions.verify(issued.token),
    EnvironmentId.make(destination.environmentId),
    "verified-session",
  );
  if (authority === undefined) throw new Error("expected authority");
  const store = yield* Store.makeCapturedAuthorizedWorkflowRunStore(
    sql,
    identity,
    Context.get(scope, ManagedPolicy.ManagedPolicy),
  );
  return {
    store,
    sessions,
    authority,
    identity,
    policy: Context.get(scope, ManagedPolicy.ManagedPolicy),
    config: Context.get(scope, ServerConfig.ServerConfig),
    path: Context.get(scope, Path.Path),
  };
});

const authorizedWorkflow = Effect.fn("test.authorizedWorkflow")(function* () {
  yield* TestClock.setTime(DateTime.makeUnsafe(facts.now).epochMilliseconds);
  yield* setupAdmission;
  const sql = yield* SqlClient.SqlClient;
  const current = { ...run, access: { origins: [destination] } };
  yield* sql`UPDATE workflow_runs SET record_json = ${json(current)} WHERE id = ${run.id}`;
  yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES ('project','Project','/work','[]',${facts.now},${facts.now})`;
  const { store, sessions, authority } = yield* authorizedStore();
  const input = {
    ...reservation(operationFor("authority-op", "authority-attempt"), "authority-attempt"),
    materialVersion: 2 as const,
    authority,
  };
  return { store, sessions, input, authority };
});

it.effect(
  "authorized workflow reservation retains V2 authority and replays without a second charge",
  () =>
    Effect.gen(function* () {
      const { store, input, authority } = yield* authorizedWorkflow();
      const sql = yield* SqlClient.SqlClient;
      const ordinary = yield* Store.WorkflowRunStore;
      assert.deepStrictEqual(yield* ordinary.reserveWorkflowNativeIntent(input), {
        kind: "rejected",
        reason: "unauthorized",
      });
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
      const first = yield* store.reserveWorkflowNativeIntent(input);
      assert.strictEqual(first.kind, "reserved");
      assert.deepStrictEqual(yield* ordinary.reserveWorkflowNativeIntent(input), {
        ...first,
        kind: "replay",
      });
      const retained = yield* sql.withTransaction(
        ordinary.readNativeReservationInTransaction({ intentId: input.intentId }),
      );
      assert.strictEqual(retained.material.version, 2);
      if (retained.material.version === 2)
        assert.deepStrictEqual(retained.material.authority, authority);
      assert.deepStrictEqual(yield* ordinary.readThreadCustody({ threadId: input.threadId }), {
        kind: "owned",
        owner: { kind: "workflow_run", workflowRunId: run.id },
      });
      const owner = parse(
        (yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`)[0]!
          .record_json,
      ) as typeof run;
      assert.strictEqual(owner.allowance.attemptsConsumed, 1);
      assert.strictEqual(owner.allowance.deadline, run.allowance.deadline);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_attempts`, []);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "authorized V2 workflow refuses revoked grants and writer-current deadline before charge",
  () =>
    Effect.gen(function* () {
      const { store, sessions, input } = yield* authorizedWorkflow();
      const sql = yield* SqlClient.SqlClient;
      const snapshot = Effect.gen(function* () {
        return {
          owner: yield* sql`SELECT * FROM workflow_runs`,
          intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
          material: yield* sql`SELECT * FROM workflow_native_reservation_material`,
          receipts: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        };
      });
      const before = yield* snapshot;
      yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      assert.deepStrictEqual(yield* store.reserveWorkflowNativeIntent(input), {
        kind: "rejected",
        reason: "deadline",
      });
      assert.deepStrictEqual(yield* snapshot, before);
      yield* TestClock.setTime(DateTime.makeUnsafe(facts.now).epochMilliseconds);
      yield* sessions.revoke(input.authority.sessionId);
      assert.deepStrictEqual(yield* store.reserveWorkflowNativeIntent(input), {
        kind: "rejected",
        reason: "unauthorized",
      });
      assert.deepStrictEqual(yield* snapshot, before);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "retained V2 workflow pause and stop survive revocation while rearm needs live authority",
  () =>
    Effect.gen(function* () {
      const { store, sessions, input } = yield* authorizedWorkflow();
      const ordinary = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      const reserved = yield* store.reserveWorkflowNativeIntent(input);
      const control = (kind: "pause" | "resume" | "stop", version: number, generation: number) => ({
        runId: run.id,
        command: {
          ...command,
          kind,
          commandId: `authority-${kind}-${version}`,
          expectedVersion: version,
          expectedGeneration: generation,
        },
        externalFacts: admissionFacts,
        receiptId: `authority-${kind}`,
      });
      assert.strictEqual(
        (yield* ordinary.commitTransition(control("pause", 1, 0))).kind,
        "accepted",
      );
      const before = yield* sql`SELECT * FROM workflow_native_launch_intents`;
      assert.deepStrictEqual(yield* ordinary.commitTransition(control("resume", 2, 1)), {
        kind: "rejected",
        reason: "unauthorized",
      });
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, before);
      assert.strictEqual((yield* store.commitTransition(control("resume", 2, 1))).kind, "accepted");
      assert.deepStrictEqual(
        yield* sql`SELECT held,generation FROM workflow_native_launch_intents`,
        [{ held: 0, generation: 2 }],
      );
      const pauseAgain = {
        ...control("pause", 3, 2),
        command: { ...control("pause", 3, 2).command, commandId: "second-pause" },
        receiptId: "second-pause",
      };
      yield* ordinary.commitTransition(pauseAgain);
      yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      assert.deepStrictEqual(yield* store.commitTransition(control("resume", 4, 3)), {
        kind: "rejected",
        reason: "deadline",
      });
      yield* TestClock.setTime(DateTime.makeUnsafe(facts.now).epochMilliseconds);
      yield* sessions.revoke(input.authority.sessionId);
      assert.deepStrictEqual(yield* store.commitTransition(control("resume", 4, 3)), {
        kind: "rejected",
        reason: "unauthorized",
      });
      assert.strictEqual(
        (yield* ordinary.commitTransition(control("stop", 4, 3))).kind,
        "accepted",
      );
      assert.deepStrictEqual(yield* ordinary.reserveWorkflowNativeIntent(input), {
        ...reserved,
        kind: "replay",
      });
      assert.strictEqual(
        (yield* sql.withTransaction(
          ordinary.readNativeReservationInTransaction({ intentId: input.intentId }),
        )).material.version,
        2,
      );
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "V2 workflow cannot borrow another principal, narrow scopes or a missing reuse target",
  () =>
    Effect.gen(function* () {
      const { store, sessions, input } = yield* authorizedWorkflow();
      const sql = yield* SqlClient.SqlClient;
      const other = yield* sessions.issue({
        subject: "other-human",
        method: "bearer-access-token",
        scopes: ["orchestration:read", "orchestration:operate"],
      });
      const otherRef = ExecutionAuthority.captureLocalRpcExecutionAuthority(
        yield* sessions.verify(other.token),
        EnvironmentId.make(destination.environmentId),
        "verified-session",
      );
      if (!otherRef) throw new Error("reference missing");
      for (const authority of [
        otherRef,
        { ...input.authority, observedScopes: ["orchestration:read" as const] },
        { ...input.authority, runtimeModeCeiling: "approval-required" as const },
      ]) {
        assert.deepStrictEqual(yield* store.reserveWorkflowNativeIntent({ ...input, authority }), {
          kind: "rejected",
          reason: "unauthorized",
        });
      }
      assert.deepStrictEqual(
        yield* store.reserveWorkflowNativeIntent({
          ...input,
          launch: { ...input.launch, reuseExistingThread: true },
        }),
        { kind: "rejected", reason: "unauthorized" },
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
      const owner = { ...run, access: { origins: [{ ...destination, threadId: input.threadId }] } };
      yield* sql`UPDATE workflow_runs SET record_json = ${json(owner)}`;
      assert.deepStrictEqual(yield* store.reserveWorkflowNativeIntent(input), {
        kind: "rejected",
        reason: "unauthorized",
      });
      const current = parse(
        (yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`)[0]!
          .record_json,
      ) as typeof run;
      assert.strictEqual(current.allowance.attemptsConsumed, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "V2 authority reservation rolls back all writes and owns the typed request before waiting",
  () =>
    Effect.gen(function* () {
      const { store, input } = yield* authorizedWorkflow();
      const sql = yield* SqlClient.SqlClient;
      const subscription = yield* store.subscribeChanges;
      const before = yield* sql`SELECT * FROM workflow_runs`;
      yield* sql`CREATE TRIGGER reject_authority_material BEFORE INSERT ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT,'injected V2 material failure'); END`;
      assert.strictEqual(
        (yield* Effect.flip(store.reserveWorkflowNativeIntent(input))).reason,
        "persistence",
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_runs`, before);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_reservation_material`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, []);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      yield* sql`DROP TRIGGER reject_authority_material`;
      const mutable = {
        ...input,
        authority: { ...input.authority, observedScopes: [...input.authority.observedScopes] },
        launch: { ...input.launch },
        externalFacts: { ...input.externalFacts, orderedItemIds: ["item"] },
      };
      const pending = store.reserveWorkflowNativeIntent(mutable);
      mutable.authority.observedScopes.length = 0;
      mutable.launch.title = "mutated";
      mutable.externalFacts.orderedItemIds.length = 0;
      assert.strictEqual((yield* pending).kind, "reserved");
      yield* PubSub.take(subscription);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      const read = yield* sql.withTransaction(
        store.readNativeReservationInTransaction({ intentId: input.intentId }),
      );
      assert.strictEqual(read.material.launch.title, input.launch.title);
      if (read.material.version !== 2) throw new Error("expected authority material");
      assert.deepStrictEqual(
        read.material.authority.observedScopes,
        input.authority.observedScopes,
      );
      yield* sql`DROP TRIGGER workflow_native_reservation_material_update_guard`;
      yield* sql`UPDATE workflow_native_reservation_material SET material_hash = ${hash}`;
      assert.strictEqual(
        (yield* Effect.flip(store.readThreadCustody({ threadId: input.threadId }))).reason,
        "integrity",
      );
    }).pipe(Effect.provide(layer)),
);

const authorizedPlanning = Effect.fn("test.authorizedPlanning")(function* () {
  yield* TestClock.setTime(DateTime.makeUnsafe(facts.now).epochMilliseconds);
  const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES ('project','Project','/work','[]',${facts.now},${facts.now})`;
  const auth = yield* authorizedStore();
  const owner = { ...session, limits: { ...session.limits, maxPasses: 1 } };
  yield* begin(auth.store, owner);
  const input = {
    ...planningReservation(admissionInput(binding, "authority-planning", owner)),
    materialVersion: 2 as const,
    authority: auth.authority,
  };
  return { ...auth, owner, input };
});

it.effect(
  "authorized planning V2 retains the exact charged session through controls and revoked replay",
  () =>
    Effect.gen(function* () {
      const { store, sessions, input, owner } = yield* authorizedPlanning();
      const ordinary = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      assert.strictEqual(
        (yield* Effect.flip(ordinary.reservePlanningNativeIntent(input))).reason,
        "unauthorized",
      );
      const first = yield* store.reservePlanningNativeIntent(input);
      assert.strictEqual(first.kind, "reserved");
      const retained = yield* sql.withTransaction(
        ordinary.readNativeReservationInTransaction({ intentId: input.intentId }),
      );
      assert.strictEqual(retained.material.version, 2);
      const historical = retained.material.planningSession;
      assert.deepStrictEqual(historical, {
        ...owner,
        recordVersion: 1,
        consumed: { passes: 1, repairRounds: 0 },
      });
      const control = (action: "pause" | "resume" | "stop", version: number) => ({
        sessionId: owner.id,
        action,
        expectedVersion: version,
        now: facts.now,
        context,
        command: {
          commandId: `auth-plan-${action}-${version}`,
          receiptId: `auth-plan-${action}-${version}`,
        },
      });
      yield* ordinary.controlPlanning(control("pause", 1));
      assert.strictEqual(
        (yield* Effect.flip(ordinary.controlPlanning(control("resume", 2)))).reason,
        "unauthorized",
      );
      const resumed = yield* store.controlPlanning(control("resume", 2));
      assert.deepStrictEqual(resumed.value.consumed, { passes: 1, repairRounds: 0 });
      yield* ordinary.controlPlanning(control("pause", 3));
      yield* sessions.revoke(input.authority.sessionId);
      assert.strictEqual(
        (yield* Effect.flip(store.controlPlanning(control("resume", 4)))).reason,
        "unauthorized",
      );
      assert.deepStrictEqual(yield* ordinary.reservePlanningNativeIntent(input), {
        ...first,
        kind: "replay",
      });
      yield* ordinary.controlPlanning(control("stop", 4));
      const after = yield* sql.withTransaction(
        ordinary.readNativeReservationInTransaction({ intentId: input.intentId }),
      );
      assert.deepStrictEqual(after.material, retained.material);
      assert.deepStrictEqual(yield* ordinary.reservePlanningNativeIntent(input), {
        ...first,
        kind: "replay",
      });
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "authorized planning V2 uses the writer clock for admission and rearm without changing history",
  () =>
    Effect.gen(function* () {
      const { store, sessions, input, owner } = yield* authorizedPlanning();
      const sql = yield* SqlClient.SqlClient;
      const snapshot = Effect.gen(function* () {
        return {
          owner: yield* sql`SELECT * FROM workflow_planning_sessions`,
          intents: yield* sql`SELECT * FROM workflow_native_launch_intents`,
          material: yield* sql`SELECT * FROM workflow_native_reservation_material`,
          receipts: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
          controls: yield* sql`SELECT * FROM workflow_command_receipts`,
        };
      });
      const before = yield* snapshot;
      yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      assert.strictEqual(
        (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
        "deadline",
      );
      assert.deepStrictEqual(yield* snapshot, before);
      yield* TestClock.setTime(DateTime.makeUnsafe(facts.now).epochMilliseconds);
      const first = yield* store.reservePlanningNativeIntent(input);
      const control = {
        sessionId: owner.id,
        action: "pause" as const,
        expectedVersion: 1,
        now: facts.now,
        context,
        command: { commandId: "clock-plan-pause", receiptId: "clock-plan-pause" },
      };
      yield* store.controlPlanning(control);
      const paused = yield* snapshot;
      yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      assert.strictEqual(
        (yield* Effect.flip(
          store.controlPlanning({
            ...control,
            action: "resume",
            expectedVersion: 2,
            command: { commandId: "clock-plan-resume", receiptId: "clock-plan-resume" },
          }),
        )).reason,
        "deadline",
      );
      assert.deepStrictEqual(yield* snapshot, paused);
      yield* sessions.revoke(input.authority.sessionId);
      assert.deepStrictEqual(yield* store.reservePlanningNativeIntent(input), {
        ...first,
        kind: "replay",
      });
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "authorized planning V2 rejects foreign identity, narrowed authority, reuse and missing origins before charge",
  () =>
    Effect.gen(function* () {
      const { store, sessions, input, owner } = yield* authorizedPlanning();
      const sql = yield* SqlClient.SqlClient;
      const other = yield* sessions.issue({
        subject: "other-human",
        method: "bearer-access-token",
        scopes: ["orchestration:read", "orchestration:operate"],
      });
      const narrow = yield* sessions.issue({
        subject: "human",
        method: "bearer-access-token",
        scopes: ["orchestration:read"],
      });
      const reference = (token: string) =>
        sessions
          .verify(token)
          .pipe(
            Effect.map((session) =>
              ExecutionAuthority.captureLocalRpcExecutionAuthority(
                session,
                EnvironmentId.make(destination.environmentId),
                "verified-session",
              ),
            ),
          );
      const otherRef = yield* reference(other.token);
      const narrowRef = yield* reference(narrow.token);
      if (!otherRef || !narrowRef) throw new Error("reference missing");
      for (const authority of [
        otherRef,
        { ...input.authority, environmentId: "foreign" },
        { ...input.authority, observedScopes: ["orchestration:read" as const] },
        { ...narrowRef, observedScopes: input.authority.observedScopes },
        { ...input.authority, runtimeModeCeiling: "approval-required" as const },
      ]) {
        assert.strictEqual(
          (yield* Effect.flip(store.reservePlanningNativeIntent({ ...input, authority }))).reason,
          "unauthorized",
        );
      }
      assert.strictEqual(
        (yield* Effect.flip(
          store.reservePlanningNativeIntent({
            ...input,
            launch: { ...input.launch, reuseExistingThread: true },
          }),
        )).reason,
        "unauthorized",
      );
      yield* sql`UPDATE workflow_planning_sessions SET record_json = ${json({ ...owner, access: { origins: [{ ...destination, threadId: input.threadId }] } })}`;
      assert.strictEqual(
        (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
        "unauthorized",
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
      assert.deepStrictEqual(
        (yield* store.getPlanning({ sessionId: owner.id })).consumed,
        owner.consumed,
      );
      yield* sql`UPDATE workflow_planning_sessions SET record_json = ${json(owner)}`;
      yield* sessions.revoke(input.authority.sessionId);
      assert.strictEqual(
        (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
        "unauthorized",
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_reservation_material`, []);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "authorized planning V2 rolls back atomically, owns inputs before waiting and detects retained tampering",
  () =>
    Effect.gen(function* () {
      const { store, input, owner } = yield* authorizedPlanning();
      const sql = yield* SqlClient.SqlClient;
      const subscription = yield* store.subscribeChanges;
      const before = yield* sql`SELECT * FROM workflow_planning_sessions`;
      yield* sql`CREATE TRIGGER reject_authority_material BEFORE INSERT ON workflow_native_reservation_material BEGIN SELECT RAISE(ABORT,'injected planning V2 material failure'); END`;
      assert.strictEqual(
        (yield* Effect.flip(store.reservePlanningNativeIntent(input))).reason,
        "storage",
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_sessions`, before);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_reservation_material`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_intent_receipts`, []);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      yield* sql`DROP TRIGGER reject_authority_material`;
      const mutable = {
        ...input,
        authority: { ...input.authority, observedScopes: [...input.authority.observedScopes] },
        launch: { ...input.launch },
        draft: {
          ...input.draft,
          admission: {
            ...input.draft.admission,
            context: {
              ...input.draft.admission.context,
              actor: { ...input.draft.admission.context.actor },
            },
          },
        },
      };
      const pending = store.reservePlanningNativeIntent(mutable);
      mutable.authority.observedScopes.length = 0;
      mutable.launch.title = "mutated";
      mutable.draft.admission.context.actor.id = decodeActorFixture({
        kind: "human",
        id: "mutated",
      }).id;
      assert.strictEqual((yield* pending).kind, "reserved");
      yield* PubSub.take(subscription);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      const retained = yield* sql.withTransaction(
        store.readNativeReservationInTransaction({ intentId: input.intentId }),
      );
      assert.strictEqual(retained.material.launch.title, input.launch.title);
      if (retained.material.version !== 2) throw new Error("authority missing");
      assert.deepStrictEqual(retained.material.authority, input.authority);
      assert.deepStrictEqual(retained.material.planningSession, {
        ...owner,
        recordVersion: 1,
        consumed: { passes: 1, repairRounds: 0 },
      });
      yield* sql`DROP TRIGGER workflow_native_reservation_material_update_guard`;
      yield* sql`UPDATE workflow_native_reservation_material SET material_hash = ${hash}`;
      assert.strictEqual(
        (yield* Effect.flip(
          store.controlPlanning({
            sessionId: owner.id,
            action: "pause",
            expectedVersion: 1,
            now: facts.now,
            context,
            command: { commandId: "tampered-plan-pause", receiptId: "tampered-plan-pause" },
          }),
        )).reason,
        "integrity",
      );
      assert.strictEqual(
        (yield* Effect.flip(store.readThreadCustody({ threadId: input.threadId }))).reason,
        "integrity",
      );
    }).pipe(Effect.provide(layer)),
);

const nativeClaimControl = Effect.fn("test.nativeClaimControl")(function* (
  kind: "workflow" | "planning",
  store: Store.WorkflowRunStore["Service"],
  action: "pause" | "stop",
) {
  const sql = yield* SqlClient.SqlClient;
  if (kind === "workflow") {
    const row = (yield* sql<{
      record_version: number;
      control_generation: number;
    }>`SELECT record_version,control_generation FROM workflow_runs`)[0]!;
    return yield* store
      .commitTransition({
        runId: run.id,
        command: {
          ...command,
          kind: action,
          commandId: `claim-${action}-${row.record_version}`,
          expectedVersion: row.record_version,
          expectedGeneration: row.control_generation,
        },
        externalFacts: admissionFacts,
        receiptId: `claim-control-${action}-${row.record_version}`,
      })
      .pipe(Effect.asVoid);
  }
  const row = (yield* sql<{
    record_version: number;
  }>`SELECT record_version FROM workflow_planning_sessions`)[0]!;
  yield* store.controlPlanning({
    sessionId: session.id,
    action,
    expectedVersion: row.record_version,
    now: facts.now,
    context,
    command: {
      commandId: `claim-${action}-${row.record_version}`,
      receiptId: `claim-${action}-${row.record_version}`,
    },
  });
});

const nativeClaimFixture = Effect.fn("test.nativeClaimFixture")(function* (
  kind: "workflow" | "planning",
  materialVersion: 1 | 2 = 2,
  ttl?: Duration.Duration,
) {
  yield* TestClock.setTime(DateTime.makeUnsafe(facts.now).epochMilliseconds);
  const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES ('project','Project','/work','[]',${facts.now},${facts.now})`;
  const auth = yield* authorizedStore(ttl);
  const material =
    materialVersion === 2
      ? { materialVersion: 2 as const, authority: auth.authority }
      : { materialVersion: 1 as const };
  let intentId: string;
  let fingerprint: string;
  if (kind === "workflow") {
    const owner = {
      ...run,
      spec: { ...run.spec, plan: binding },
      access: { origins: [destination] },
      allowance: { ...run.allowance, maxAttempts: 1 },
    };
    yield* auth.store.createRun(owner);
    const input = {
      ...reservation(operationFor("claim-op", "claim-attempt"), "claim-attempt"),
      ...material,
    };
    const receipt = yield* auth.store.reserveWorkflowNativeIntent(input);
    if (receipt.kind !== "reserved") throw new Error("expected reservation");
    intentId = input.intentId;
    fingerprint = receipt.fingerprint;
  } else {
    const owner = { ...session, limits: { ...session.limits, maxPasses: 1 } };
    yield* begin(auth.store, owner);
    const input = {
      ...planningReservation(admissionInput(binding, "claim", owner)),
      ...material,
    };
    const receipt = yield* auth.store.reservePlanningNativeIntent(input);
    intentId = input.intentId;
    fingerprint = receipt.fingerprint;
  }
  const control = (action: "pause" | "stop") => nativeClaimControl(kind, auth.store, action);
  return {
    ...auth,
    kind,
    control,
    claimInput: {
      intentId,
      expectedFingerprint: fingerprint,
      expectedOwnerVersion: 1,
      expectedGeneration: 0,
      bootId: "boot",
      workerId: "worker",
    },
  };
});

it.effect.each(["workflow", "planning"] as const)(
  "native intent claim owns %s custody once at the last charged slot",
  (kind) =>
    Effect.gen(function* () {
      const { store, claimInput } = yield* nativeClaimFixture(kind);
      const sql = yield* SqlClient.SqlClient;
      const before = {
        workflow: yield* sql`SELECT * FROM workflow_runs`,
        planning: yield* sql`SELECT * FROM workflow_planning_sessions`,
        material: yield* sql`SELECT * FROM workflow_native_reservation_material`,
        receipts: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
      };
      const subscription = yield* store.subscribeChanges;
      const first = yield* store.claimNativeIntent(claimInput);
      assert.strictEqual(first.kind, "claimed");
      assert.deepStrictEqual(
        yield* sql`SELECT state,held,boot_id,worker_id FROM workflow_native_launch_intents`,
        [{ state: "dispatching", held: 0, boot_id: "boot", worker_id: "worker" }],
      );
      yield* PubSub.take(subscription);
      for (const workerId of ["worker", "other-worker"])
        assert.notStrictEqual(
          (yield* store.claimNativeIntent({ ...claimInput, workerId })).kind,
          "claimed",
        );
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      assert.deepStrictEqual(
        {
          workflow: yield* sql`SELECT * FROM workflow_runs`,
          planning: yield* sql`SELECT * FROM workflow_planning_sessions`,
          material: yield* sql`SELECT * FROM workflow_native_reservation_material`,
          receipts: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
        },
        before,
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_passes`, []);
      yield* sql`UPDATE workflow_native_launch_intents SET state = 'unknown'`;
      const uncertain = yield* sql`SELECT * FROM workflow_native_launch_intents`;
      yield* TestClock.setTime(DateTime.makeUnsafe("2027-12-02T00:00:00Z").epochMilliseconds);
      assert.notStrictEqual(
        (yield* store.claimNativeIntent({ ...claimInput, bootId: "later-boot" })).kind,
        "claimed",
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_launch_intents`, uncertain);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "native intent claim rejects stale, missing authority and elapsed %s work without writes",
  (kind) =>
    Effect.gen(function* () {
      const { store, sessions, authority, claimInput } = yield* nativeClaimFixture(kind);
      const sql = yield* SqlClient.SqlClient;
      const ordinary = yield* Store.WorkflowRunStore;
      const subscription = yield* store.subscribeChanges;
      const before = yield* rawReservationState;
      assert.deepStrictEqual(yield* ordinary.claimNativeIntent(claimInput), {
        kind: "denied",
        reason: "authority",
      });
      for (const changes of [
        { expectedFingerprint: nextHash },
        { expectedGeneration: 1 },
        { expectedOwnerVersion: 2 },
      ])
        assert.deepStrictEqual(yield* store.claimNativeIntent({ ...claimInput, ...changes }), {
          kind: "stale",
        });
      yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      assert.deepStrictEqual(yield* store.claimNativeIntent(claimInput), {
        kind: "denied",
        reason: "deadline",
      });
      yield* TestClock.setTime(DateTime.makeUnsafe(facts.now).epochMilliseconds);
      yield* sessions.revoke(authority.sessionId);
      assert.deepStrictEqual(yield* store.claimNativeIntent(claimInput), {
        kind: "denied",
        reason: "authority",
      });
      assert.deepStrictEqual(yield* rawReservationState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      assert.strictEqual(
        (yield* Effect.flip(sql.withTransaction(store.claimNativeIntent(claimInput)))).reason,
        "integrity",
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "native intent claim refuses legacy %s material without rewriting it",
  (kind) =>
    Effect.gen(function* () {
      const { store, claimInput } = yield* nativeClaimFixture(kind, 1);
      const before = yield* rawReservationState;
      assert.deepStrictEqual(yield* store.claimNativeIntent(claimInput), {
        kind: "denied",
        reason: "authority",
      });
      assert.deepStrictEqual(yield* rawReservationState, before);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "native intent claim rolls back and snapshots %s custody input before execution",
  (kind) =>
    Effect.gen(function* () {
      const { store, claimInput } = yield* nativeClaimFixture(kind);
      const sql = yield* SqlClient.SqlClient;
      const subscription = yield* store.subscribeChanges;
      const before = yield* rawReservationState;
      yield* sql`CREATE TRIGGER reject_native_claim AFTER UPDATE OF state ON workflow_native_launch_intents WHEN NEW.state = 'dispatching' BEGIN SELECT RAISE(ABORT,'injected claim failure'); END`;
      assert.strictEqual(
        (yield* Effect.flip(store.claimNativeIntent(claimInput))).reason,
        "persistence",
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      yield* sql`DROP TRIGGER reject_native_claim`;
      const mutable = { ...claimInput };
      const pending = store.claimNativeIntent(mutable);
      mutable.workerId = "mutated";
      mutable.expectedFingerprint = nextHash;
      assert.strictEqual((yield* pending).kind, "claimed");
      assert.deepStrictEqual(
        yield* sql`SELECT boot_id,worker_id FROM workflow_native_launch_intents`,
        [{ boot_id: "boot", worker_id: "worker" }],
      );
      yield* PubSub.take(subscription);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "receipt"],
  ["planning", "receipt"],
  ["workflow", "event"],
  ["planning", "event"],
  ["workflow", "proof"],
  ["planning", "proof"],
] as const)(
  "native intent claim retains $0 reservation when existing $1 needs reconciliation",
  ([kind, evidence]) =>
    Effect.gen(function* () {
      const { store, claimInput } = yield* nativeClaimFixture(kind);
      const sql = yield* SqlClient.SqlClient;
      const native = (yield* sql<{
        launch_command_id: string;
        thread_id: string;
      }>`SELECT launch_command_id,thread_id FROM workflow_native_launch_intents`)[0]!;
      const commandId =
        evidence === "proof"
          ? "foreign-proof-command"
          : `${native.launch_command_id}:initial-message`;
      if (evidence === "receipt" || evidence === "proof")
        yield* sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status) VALUES (${commandId},'thread',${native.thread_id},${facts.now},0,'accepted')`;
      if (evidence === "event")
        yield* sql`INSERT INTO orchestration_events (event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json) VALUES ('orphan-native-event','thread',${native.thread_id},1,'run.created',${facts.now},${commandId},'system','{}','{}')`;
      if (evidence === "proof")
        yield* sql`INSERT INTO workflow_native_command_proofs (command_id,intent_id,phase,proof_json,proof_hash) VALUES (${commandId},${claimInput.intentId},'claim','{}',${hash})`;
      const before = yield* rawReservationState;
      assert.deepStrictEqual(yield* store.claimNativeIntent(claimInput), {
        kind: "denied",
        reason: "evidence",
      });
      assert.deepStrictEqual(yield* rawReservationState, before);
      assert.deepStrictEqual(
        yield* sql`SELECT state,boot_id,worker_id FROM workflow_native_launch_intents`,
        [{ state: "reserved", boot_id: null, worker_id: null }],
      );
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "revoke", "claim"],
  ["workflow", "revoke", "control"],
  ["planning", "revoke", "claim"],
  ["planning", "revoke", "control"],
  ["workflow", "pause", "claim"],
  ["workflow", "pause", "control"],
  ["planning", "pause", "claim"],
  ["planning", "pause", "control"],
  ["workflow", "stop", "claim"],
  ["workflow", "stop", "control"],
  ["planning", "stop", "claim"],
  ["planning", "stop", "control"],
] as const)(
  "independent native intent claim $0/$1 preserves $2-first commit order",
  ([kind, action, first]) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-authority-claim-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        const fileLayer = Store.layer.pipe(
          Layer.provideMerge(NodeSqliteClient.layer({ filename })),
        );
        yield* Effect.gen(function* () {
          const prepared = yield* nativeClaimFixture(kind);
          const sql = yield* SqlClient.SqlClient;
          yield* sql`PRAGMA journal_mode = WAL`;
          const contextB = yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename })));
          const sqlB = Context.get(contextB, SqlClient.SqlClient);
          const other = yield* authorizedStore().pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          );
          const control =
            action === "revoke"
              ? other.sessions.revoke(prepared.authority.sessionId).pipe(Effect.asVoid)
              : nativeClaimControl(kind, other.store, action).pipe(
                  Effect.provideService(SqlClient.SqlClient, sqlB),
                );
          const originals = {
            material: yield* sql`SELECT * FROM workflow_native_reservation_material`,
            receipts: yield* sql`SELECT * FROM workflow_native_intent_receipts`,
          };
          if (first === "control") {
            yield* control;
            const before = yield* rawReservationState;
            assert.notStrictEqual(
              (yield* prepared.store.claimNativeIntent(prepared.claimInput)).kind,
              "claimed",
            );
            assert.deepStrictEqual(yield* rawReservationState, before);
          } else {
            assert.strictEqual(
              (yield* prepared.store.claimNativeIntent(prepared.claimInput)).kind,
              "claimed",
            );
            yield* control;
            const before = yield* rawReservationState;
            assert.notStrictEqual(
              (yield* other.store.claimNativeIntent({
                ...prepared.claimInput,
                bootId: "replacement-boot",
                workerId: "replacement-worker",
              })).kind,
              "claimed",
            );
            assert.deepStrictEqual(yield* rawReservationState, before);
            assert.deepStrictEqual(
              yield* sqlB`SELECT state,boot_id,worker_id FROM workflow_native_launch_intents`,
              [{ state: "dispatching", boot_id: "boot", worker_id: "worker" }],
            );
            const status =
              kind === "workflow"
                ? yield* sqlB<{ status: string }>`SELECT status FROM workflow_runs`
                : yield* sqlB<{ status: string }>`SELECT status FROM workflow_planning_sessions`;
            assert.strictEqual(
              status[0]!.status,
              action === "revoke" ? "active" : action === "pause" ? "pausing" : "stopping",
            );
          }
          assert.deepStrictEqual(
            {
              material: yield* sqlB`SELECT * FROM workflow_native_reservation_material`,
              receipts: yield* sqlB`SELECT * FROM workflow_native_intent_receipts`,
            },
            originals,
          );
          const owners =
            kind === "workflow"
              ? yield* sqlB<{ record_json: string }>`SELECT record_json FROM workflow_runs`
              : yield* sqlB<{
                  record_json: string;
                }>`SELECT record_json FROM workflow_planning_sessions`;
          const record = parse(owners[0]!.record_json) as typeof run & typeof session;
          assert.strictEqual(
            kind === "workflow" ? record.allowance.attemptsConsumed : record.consumed.passes,
            1,
          );
        }).pipe(Effect.provide(fileLayer));
      }),
    ),
);

it.effect.each(["workflow", "planning"] as const)(
  "native intent claim rechecks current %s bindings, resources and running effects",
  (kind) =>
    Effect.gen(function* () {
      const { store, claimInput } = yield* nativeClaimFixture(kind);
      const sql = yield* SqlClient.SqlClient;
      const subscription = yield* store.subscribeChanges;
      const original = yield* rawReservationState;
      yield* sql`UPDATE projection_projects SET deleted_at = ${facts.now}`;
      assert.deepStrictEqual(yield* store.claimNativeIntent(claimInput), {
        kind: "denied",
        reason: "authority",
      });
      yield* sql`UPDATE projection_projects SET deleted_at = NULL`;
      if (kind === "workflow") {
        const rows = yield* sql<{ record_json: string }>`SELECT record_json FROM workflow_runs`;
        const owner = parse(rows[0]!.record_json) as typeof run;
        yield* sql`UPDATE workflow_runs SET spec_hash = ${nextHash}, record_json = ${json({ ...owner, spec: { ...owner.spec, ref: { ...owner.spec.ref, sha256: nextHash } } })}`;
        assert.strictEqual(
          (yield* Effect.flip(store.claimNativeIntent(claimInput))).reason,
          "integrity",
        );
        yield* sql`UPDATE workflow_runs SET spec_hash = ${hash},record_json = ${rows[0]!.record_json}`;
      } else {
        const rows = yield* sql<{
          record_json: string;
        }>`SELECT record_json FROM workflow_planning_sessions`;
        const owner = parse(rows[0]!.record_json) as typeof session;
        yield* sql`UPDATE workflow_planning_sessions SET record_json = ${json({ ...owner, allowedProviders: [{ providerInstanceId: "different", model: "different" }] })}`;
        assert.deepStrictEqual(yield* store.claimNativeIntent(claimInput), {
          kind: "denied",
          reason: "ineligible",
        });
        yield* sql`UPDATE workflow_planning_sessions SET record_json = ${rows[0]!.record_json}`;
      }
      const revisions = yield* sql<{
        revision_json: string;
      }>`SELECT revision_json FROM development_plan_revisions`;
      yield* sql`UPDATE development_plan_revisions SET revision_json = '{}'`;
      assert.strictEqual(
        (yield* Effect.flip(store.claimNativeIntent(claimInput))).reason,
        "integrity",
      );
      yield* sql`UPDATE development_plan_revisions SET revision_json = ${revisions[0]!.revision_json}`;
      yield* sql`ALTER TABLE development_plan_revisions RENAME TO unavailable_plan_revisions`;
      assert.strictEqual(
        (yield* Effect.flip(store.claimNativeIntent(claimInput))).reason,
        "persistence",
      );
      yield* sql`ALTER TABLE unavailable_plan_revisions RENAME TO development_plan_revisions`;
      const native = (yield* sql<{
        thread_id: string;
      }>`SELECT thread_id FROM workflow_native_launch_intents`)[0]!;
      yield* sql`INSERT INTO orchestration_v2_effect_outbox (effect_id,command_id,thread_id,effect_type,payload_json,status,available_at,created_at,updated_at) VALUES ('running-before-claim','native-command',${native.thread_id},'provider-turn.start',${json(nativeRequests[0])},'running',${facts.now},${facts.now},${facts.now})`;
      const before = yield* rawReservationState;
      assert.deepStrictEqual(yield* store.claimNativeIntent(claimInput), {
        kind: "denied",
        reason: "custody",
      });
      assert.deepStrictEqual(yield* rawReservationState, before);
      yield* sql`DELETE FROM orchestration_v2_effect_outbox WHERE effect_id = 'running-before-claim'`;
      assert.deepStrictEqual(yield* rawReservationState, original);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "claimed native intent verification observes current %s custody without writes",
  (kind) =>
    Effect.gen(function* () {
      const { store, claimInput } = yield* nativeClaimFixture(kind);
      const sql = yield* SqlClient.SqlClient;
      assert.strictEqual((yield* store.claimNativeIntent(claimInput)).kind, "claimed");
      const subscription = yield* store.subscribeChanges;
      const input = {
        intentId: claimInput.intentId,
        bootId: claimInput.bootId,
        workerId: claimInput.workerId,
      };
      const before = yield* rawReservationState;
      assert.deepStrictEqual(
        yield* sql.withTransaction(store.verifyClaimedNativeIntentInTransaction(input)),
        { kind: "current" },
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "claimed native intent verification rejects foreign transactions, labels and ordinary %s construction",
  (kind) =>
    Effect.gen(function* () {
      const { store, claimInput } = yield* nativeClaimFixture(kind);
      const sql = yield* SqlClient.SqlClient;
      const ordinary = yield* Store.WorkflowRunStore;
      const input = { intentId: claimInput.intentId, bootId: "boot", workerId: "worker" };
      const before = yield* rawReservationState;
      assert.deepStrictEqual(
        yield* sql.withTransaction(store.verifyClaimedNativeIntentInTransaction(input)),
        { kind: "denied", reason: "state" },
      );
      assert.strictEqual(
        (yield* Effect.flip(store.verifyClaimedNativeIntentInTransaction(input))).reason,
        "integrity",
      );
      const foreign = Context.get(
        yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename: ":memory:" }))),
        SqlClient.SqlClient,
      );
      assert.strictEqual(
        (yield* Effect.flip(
          foreign.withTransaction(store.verifyClaimedNativeIntentInTransaction(input)),
        )).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
      yield* store.claimNativeIntent(claimInput);
      const claimed = yield* rawReservationState;
      for (const changes of [{ bootId: "other-boot" }, { workerId: "other-worker" }])
        assert.deepStrictEqual(
          yield* sql.withTransaction(
            store.verifyClaimedNativeIntentInTransaction({ ...input, ...changes }),
          ),
          { kind: "stale" },
        );
      assert.deepStrictEqual(
        yield* sql.withTransaction(ordinary.verifyClaimedNativeIntentInTransaction(input)),
        { kind: "denied", reason: "authority" },
      );
      const mutable = { ...input };
      const pending = store.verifyClaimedNativeIntentInTransaction(mutable);
      mutable.workerId = "mutated";
      assert.deepStrictEqual(yield* sql.withTransaction(pending), { kind: "current" });
      assert.deepStrictEqual(yield* rawReservationState, claimed);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "claimed native intent verification refuses elapsed or revoked %s authority without writes",
  (kind) =>
    Effect.gen(function* () {
      const { store, sessions, authority, claimInput } = yield* nativeClaimFixture(
        kind,
        2,
        Duration.hours(1),
      );
      const sql = yield* SqlClient.SqlClient;
      yield* store.claimNativeIntent(claimInput);
      const input = { intentId: claimInput.intentId, bootId: "boot", workerId: "worker" };
      const before = yield* rawReservationState;
      yield* TestClock.setTime(DateTime.makeUnsafe("2026-11-12T02:00:00Z").epochMilliseconds);
      assert.deepStrictEqual(
        yield* sql.withTransaction(store.verifyClaimedNativeIntentInTransaction(input)),
        { kind: "denied", reason: "authority" },
      );
      yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      assert.deepStrictEqual(
        yield* sql.withTransaction(store.verifyClaimedNativeIntentInTransaction(input)),
        { kind: "denied", reason: "deadline" },
      );
      yield* TestClock.setTime(DateTime.makeUnsafe(facts.now).epochMilliseconds);
      yield* sessions.revoke(authority.sessionId);
      assert.deepStrictEqual(
        yield* sql.withTransaction(store.verifyClaimedNativeIntentInTransaction(input)),
        { kind: "denied", reason: "authority" },
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "pause"],
  ["workflow", "stop"],
  ["planning", "pause"],
  ["planning", "stop"],
] as const)(
  "claimed native intent verification observes $0/$1 control without renewed custody",
  ([kind, action]) =>
    Effect.gen(function* () {
      const { store, control, claimInput } = yield* nativeClaimFixture(kind);
      const sql = yield* SqlClient.SqlClient;
      yield* store.claimNativeIntent(claimInput);
      yield* control(action);
      const subscription = yield* store.subscribeChanges;
      const before = yield* rawReservationState;
      assert.notStrictEqual(
        (yield* sql.withTransaction(
          store.verifyClaimedNativeIntentInTransaction({
            intentId: claimInput.intentId,
            bootId: "boot",
            workerId: "worker",
          }),
        )).kind,
        "current",
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "claimed native intent verification retains %s unknown custody and rejects tampering",
  (kind) =>
    Effect.gen(function* () {
      const { store, claimInput } = yield* nativeClaimFixture(kind);
      const sql = yield* SqlClient.SqlClient;
      yield* store.claimNativeIntent(claimInput);
      const input = { intentId: claimInput.intentId, bootId: "boot", workerId: "worker" };
      yield* sql`UPDATE workflow_native_launch_intents SET state = 'unknown'`;
      const before = yield* rawReservationState;
      assert.deepStrictEqual(
        yield* sql.withTransaction(store.verifyClaimedNativeIntentInTransaction(input)),
        { kind: "denied", reason: "state" },
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
      yield* sql`UPDATE workflow_native_launch_intents SET state = 'dispatching'`;
      yield* sql`DROP TRIGGER workflow_native_reservation_material_update_guard`;
      yield* sql`UPDATE workflow_native_reservation_material SET material_hash = ${nextHash}`;
      const tampered = yield* rawReservationState;
      assert.strictEqual(
        (yield* Effect.flip(
          sql.withTransaction(store.verifyClaimedNativeIntentInTransaction(input)),
        )).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* rawReservationState, tampered);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "claimed native intent verification keeps %s protocol evidence separate from current authority",
  (kind) =>
    Effect.gen(function* () {
      const { store, claimInput } = yield* nativeClaimFixture(kind);
      const sql = yield* SqlClient.SqlClient;
      yield* store.claimNativeIntent(claimInput);
      const row = (yield* sql<{
        launch_command_id: string;
        thread_id: string;
      }>`SELECT launch_command_id,thread_id FROM workflow_native_launch_intents`)[0]!;
      yield* sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status) VALUES (${row.launch_command_id},'thread',${row.thread_id},${facts.now},0,'accepted')`;
      yield* sql`INSERT INTO workflow_native_command_proofs (command_id,intent_id,phase,proof_json,proof_hash) VALUES (${row.launch_command_id},${claimInput.intentId},'claim','{}',${hash})`;
      const input = { intentId: claimInput.intentId, bootId: "boot", workerId: "worker" };
      const before = yield* rawReservationState;
      assert.deepStrictEqual(
        yield* sql.withTransaction(store.verifyClaimedNativeIntentInTransaction(input)),
        { kind: "current" },
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
      yield* sql`UPDATE workflow_native_launch_intents SET native_run_id = 'observed-native-run'`;
      const bound = yield* rawReservationState;
      assert.deepStrictEqual(
        yield* sql.withTransaction(store.verifyClaimedNativeIntentInTransaction(input)),
        { kind: "denied", reason: "state" },
      );
      assert.deepStrictEqual(yield* rawReservationState, bound);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "claimed native intent verification cannot promote legacy %s rows through state flags",
  (kind) =>
    Effect.gen(function* () {
      const { store, claimInput } = yield* nativeClaimFixture(kind, 1);
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE workflow_native_launch_intents SET state = 'dispatching',boot_id = 'boot',worker_id = 'worker'`;
      const before = yield* rawReservationState;
      assert.deepStrictEqual(
        yield* sql.withTransaction(
          store.verifyClaimedNativeIntentInTransaction({
            intentId: claimInput.intentId,
            bootId: "boot",
            workerId: "worker",
          }),
        ),
        { kind: "denied", reason: "authority" },
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
    }).pipe(Effect.provide(layer)),
);

const nativeBindingState = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return {
    ...(yield* rawReservationState),
    material: yield* sql`SELECT * FROM workflow_native_reservation_material`,
    nativeProofs: yield* sql`SELECT * FROM workflow_native_command_proofs`,
    nativeReceipts: yield* sql`SELECT * FROM orchestration_command_receipts`,
    nativeEvents: yield* sql`SELECT * FROM orchestration_events`,
    nativeThreads: yield* sql`SELECT * FROM orchestration_v2_projection_threads`,
    nativeRuns: yield* sql`SELECT * FROM orchestration_v2_projection_runs`,
    nativeMessages: yield* sql`SELECT * FROM orchestration_v2_projection_messages`,
    nativeAttempts: yield* sql`SELECT * FROM orchestration_v2_projection_run_attempts`,
    nativeNodes: yield* sql`SELECT * FROM orchestration_v2_projection_nodes`,
    nativeProviders: yield* sql`SELECT * FROM orchestration_v2_projection_provider_threads`,
    nativeScopes: yield* sql`SELECT * FROM orchestration_v2_projection_checkpoint_scopes`,
  };
});
const decodeBindingCommand = Schema.decodeSync(OrchestrationV2Command);
const nativeBindingFixture = Effect.fn("test.nativeBindingFixture")(function* (
  kind: "workflow" | "planning",
  reuse = false,
  materialVersion: 1 | 2 = 2,
  claim = true,
  repair = false,
  maxPlanningPasses = 1,
) {
  yield* TestClock.setTime(DateTime.makeUnsafe(facts.now).epochMilliseconds);
  const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES ('project','Project','/work','[]',${facts.now},${facts.now})`;
  const auth = yield* authorizedStore();
  const coordinator = { bootId: "binding-boot", workerId: "binding-worker" };
  const adapter = {
    instanceId: ProviderInstanceId.make("provider"),
    driver: ProviderDriverKind.make("codex"),
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    openSession: () => Effect.die("Binding fixture must never open a provider session"),
  } as ProviderAdapter.ProviderAdapterV2["Service"];
  const runtime = yield* Layer.build(
    Layer.fresh(
      ProviderReplayHarness.layerWithRegistry(
        { name: "native-binding" },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
        {
          databaseLayer: Layer.succeed(SqlClient.SqlClient, sql),
          serverConfig: auth.config,
          nativeWorkflowAuthority: { identity: auth.identity, policy: auth.policy, coordinator },
          runEffectWorker: false,
          runContinuationWorker: false,
          recoverOnStartup: false,
        },
      ),
    ),
  );
  const store = Context.get(runtime, Store.WorkflowRunStore);
  assert.isFunction(store.bindNativeIntent);
  const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
  const planningOwner = { ...session, limits: { ...session.limits, maxPasses: maxPlanningPasses } };
  const material =
    materialVersion === 2
      ? { materialVersion: 2 as const, authority: auth.authority }
      : { materialVersion: 1 as const };
  const input =
    kind === "workflow"
      ? {
          ...reservation(operationFor("binding-operation", "binding-attempt"), "binding-attempt"),
          ...material,
          repair,
          draft: {
            ...reservation(operationFor("binding-operation", "binding-attempt"), "binding-attempt")
              .draft,
            admission: {
              command: {
                ...reservation(
                  operationFor("binding-operation", "binding-attempt"),
                  "binding-attempt",
                ).draft.admission.command,
                repair,
                expectedVersion: repair ? 1 : 0,
              },
            },
          },
          launch: {
            ...reservation(operationFor("binding-operation", "binding-attempt"), "binding-attempt")
              .launch,
            reuseExistingThread: reuse,
          },
        }
      : (() => {
          const base = planningReservation(admissionInput(binding, "binding", planningOwner));
          return { ...base, ...material, launch: { ...base.launch, reuseExistingThread: reuse } };
        })();
  if (reuse)
    yield* orchestrator.dispatch(
      decodeBindingCommand({
        type: "thread.create",
        commandId: "binding-existing-thread",
        threadId: input.threadId,
        projectId: input.projectId,
        title: "Existing",
        modelSelection: input.launch.modelSelection,
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: "main",
        worktreePath: null,
        createdBy: "agent",
        creationSource: "mcp",
      }),
    );
  let fingerprint: string;
  if (kind === "workflow" && "runId" in input) {
    yield* store.createRun({
      ...run,
      spec: { ...run.spec, plan: binding },
      allowance: { ...run.allowance, maxAttempts: repair ? 2 : 1 },
      access: { origins: [destination] },
    });
    if (repair) {
      const prior = operationFor("prior-operation", "prior-attempt");
      assert.strictEqual(
        (yield* store.commitTransition({
          runId: run.id,
          command: admission(prior, "prior-attempt"),
          operation: prior,
          externalFacts: admissionFacts,
          receiptId: "prior-receipt",
        })).kind,
        "accepted",
      );
      yield* sql`UPDATE workflow_operations SET status='settled',record_json=${json({ ...prior, status: "settled", outcome: { kind: "failed", summary: "Needs repair", evidence: [] } })} WHERE id=${prior.id}`;
    }
    const result = yield* store.reserveWorkflowNativeIntent(input);
    if (result.kind !== "reserved") throw new Error("expected reservation");
    fingerprint = result.fingerprint;
  } else if ("sessionId" in input) {
    yield* begin(store, planningOwner);
    fingerprint = (yield* store.reservePlanningNativeIntent(input)).fingerprint;
  } else throw new Error("wrong fixture owner");
  if (claim)
    assert.strictEqual(
      (yield* store.claimNativeIntent({
        intentId: input.intentId,
        expectedFingerprint: fingerprint,
        expectedOwnerVersion: repair ? 2 : 1,
        expectedGeneration: 0,
        ...coordinator,
      })).kind,
      "claimed",
    );
  const protocol = yield* NativeProtocol.makeNativeCommandProtocol(sql, auth.config, auth.path);
  const derived = yield* sql.withTransaction(
    Effect.gen(function* () {
      return yield* protocol.deriveInTransaction(
        yield* store.readNativeReservationInTransaction({ intentId: input.intentId }),
      );
    }),
  );
  const dispatch = (phase: "claim" | "message") =>
    orchestrator.dispatch(phase === "claim" ? derived.claimCommand : derived.messageCommand, {
      intentId: input.intentId,
      phase,
    });
  return {
    ...auth,
    store,
    sql,
    runtime,
    kind,
    input,
    coordinator,
    dispatch,
    selection: {
      intentId: input.intentId,
      expectedFingerprint: fingerprint,
      expectedGeneration: 0,
    },
  };
});

it.effect.each([
  ["workflow", false],
  ["workflow", true],
  ["planning", false],
  ["planning", true],
] as const)("native binding records verified $0 RunId once with reuse=$1", ([kind, reuse]) =>
  Effect.gen(function* () {
    const fixture = yield* nativeBindingFixture(kind, reuse);
    const { store, sql, selection, dispatch } = fixture;
    yield* dispatch("claim");
    const result = yield* dispatch("message");
    const runEvent = result.storedEvents.find(({ event }) => event.type === "run.created");
    if (runEvent?.event.type !== "run.created") throw new Error("actual run missing");
    const actualRunId = runEvent.event.payload.id;
    const subscription = yield* store.subscribeChanges;
    const before = yield* nativeBindingState;
    const material = yield* sql`SELECT * FROM workflow_native_reservation_material`;
    assert.deepStrictEqual(yield* store.bindNativeIntent(selection), {
      kind: "bound",
      nativeRunId: actualRunId,
      state: "dispatching",
    });
    yield* PubSub.take(subscription);
    const { intents: beforeIntents, ...beforeOther } = before;
    const { intents: afterIntents, ...afterOther } = yield* nativeBindingState;
    assert.deepStrictEqual(afterOther, beforeOther);
    assert.deepStrictEqual(afterIntents, [{ ...beforeIntents[0], native_run_id: actualRunId }]);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM workflow_native_reservation_material`,
      material,
    );
    assert.deepStrictEqual(yield* store.bindNativeIntent(selection), {
      kind: "replay",
      nativeRunId: actualRunId,
      state: "dispatching",
    });
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "revoke"],
  ["planning", "revoke"],
  ["workflow", "pause"],
  ["planning", "pause"],
  ["workflow", "stop"],
  ["planning", "stop"],
  ["workflow", "deadline"],
  ["planning", "deadline"],
  ["workflow", "unknown"],
  ["planning", "unknown"],
  ["workflow", "deleted-target"],
  ["planning", "deleted-target"],
] as const)(
  "native binding retains verified $0 run in unknown custody after $1",
  ([kind, change]) =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture(kind);
      const { store, sql, selection } = fixture;
      yield* fixture.dispatch("claim");
      const message = yield* fixture.dispatch("message");
      const created = message.storedEvents.find(({ event }) => event.type === "run.created");
      if (created?.event.type !== "run.created") throw new Error("actual run missing");
      if (change === "revoke") yield* fixture.sessions.revoke(fixture.authority.sessionId);
      else if (change === "pause" || change === "stop")
        yield* nativeClaimControl(kind, store, change);
      else if (change === "deadline")
        yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      else if (change === "unknown")
        yield* sql`UPDATE workflow_native_launch_intents SET state = 'unknown'`;
      else
        yield* sql`UPDATE orchestration_v2_projection_threads SET deleted_at = ${facts.now} WHERE thread_id = ${fixture.input.threadId}`;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.deepStrictEqual(yield* store.bindNativeIntent(selection), {
        kind: "bound",
        nativeRunId: created.event.payload.id,
        state: "unknown",
      });
      yield* PubSub.take(subscription);
      const { intents: original, ...others } = before;
      const { intents, ...afterOther } = yield* nativeBindingState;
      assert.deepStrictEqual(afterOther, others);
      assert.deepStrictEqual(intents, [
        { ...original[0], native_run_id: created.event.payload.id, state: "unknown" },
      ]);
      assert.deepStrictEqual(yield* store.bindNativeIntent(selection), {
        kind: "replay",
        nativeRunId: created.event.payload.id,
        state: "unknown",
      });
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", false],
  ["workflow", true],
  ["planning", false],
  ["planning", true],
] as const)(
  "native binding retains $0 custody when complete native evidence is absent (claim present=$1)",
  ([kind, claimPresent]) =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture(kind);
      const { store, sql, selection } = fixture;
      if (claimPresent) yield* fixture.dispatch("claim");
      const subscription = yield* store.subscribeChanges;
      const before = yield* nativeBindingState;
      assert.deepStrictEqual(yield* store.bindNativeIntent(selection), {
        kind: "reconciliation_required",
        state: "unknown",
      });
      yield* PubSub.take(subscription);
      const { intents: original, ...others } = before;
      const { intents, ...afterOther } = yield* nativeBindingState;
      assert.deepStrictEqual(afterOther, others);
      assert.deepStrictEqual(intents, [{ ...original[0], state: "unknown" }]);
      const stable = yield* nativeBindingState;
      assert.deepStrictEqual(yield* store.bindNativeIntent(selection), {
        kind: "reconciliation_required",
        state: "unknown",
      });
      assert.deepStrictEqual(yield* nativeBindingState, stable);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      assert.strictEqual(
        (yield* sql`SELECT * FROM workflow_native_launch_intents WHERE native_run_id IS NOT NULL`)
          .length,
        0,
      );
      assert.isTrue(
        Exit.isFailure(yield* Effect.exit(fixture.dispatch(claimPresent ? "message" : "claim"))),
      );
      assert.deepStrictEqual(yield* nativeBindingState, stable);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "native binding refuses wrong $0 selection, constructor and coordinator without writes",
  (kind) =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture(kind);
      const { store, sql, selection } = fixture;
      yield* fixture.dispatch("claim");
      yield* fixture.dispatch("message");
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      const ordinary = yield* Store.WorkflowRunStore;
      assert.deepStrictEqual(yield* ordinary.bindNativeIntent(selection), {
        kind: "denied",
        reason: "unavailable",
      });
      for (const change of [{ expectedFingerprint: nextHash }, { expectedGeneration: 1 }])
        assert.deepStrictEqual(yield* store.bindNativeIntent({ ...selection, ...change }), {
          kind: "stale",
        });
      const wrong = yield* Store.makeCapturedNativeWorkflowRunStore(
        sql,
        fixture.config,
        fixture.path,
        fixture.identity,
        fixture.policy,
        { ...fixture.coordinator, workerId: "wrong-worker" },
      );
      assert.deepStrictEqual(yield* wrong.bindNativeIntent(selection), { kind: "stale" });
      const forged = { ...selection, nativeRunId: "caller-invented-run" };
      assert.strictEqual((yield* Effect.flip(store.bindNativeIntent(forged))).reason, "integrity");
      assert.strictEqual(
        (yield* Effect.flip(sql.withTransaction(store.bindNativeIntent(selection)))).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "legacy"],
  ["workflow", "reserved"],
  ["workflow", "cancelled"],
  ["planning", "legacy"],
  ["planning", "reserved"],
  ["planning", "cancelled"],
] as const)("native binding cannot promote $0 $1 custody", ([kind, state]) =>
  Effect.gen(function* () {
    const fixture = yield* nativeBindingFixture(kind, false, state === "legacy" ? 1 : 2, false);
    if (state === "legacy")
      yield* fixture.sql`UPDATE workflow_native_launch_intents SET state = 'dispatching', boot_id = 'binding-boot', worker_id = 'binding-worker'`;
    if (state === "cancelled") yield* nativeClaimControl(kind, fixture.store, "stop");
    const before = yield* nativeBindingState;
    assert.strictEqual((yield* fixture.store.bindNativeIntent(fixture.selection)).kind, "denied");
    assert.deepStrictEqual(yield* nativeBindingState, before);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "material"],
  ["planning", "material"],
  ["workflow", "proof"],
  ["planning", "proof"],
  ["workflow", "partial"],
  ["planning", "partial"],
  ["workflow", "receipt"],
  ["planning", "receipt"],
  ["workflow", "normalization"],
  ["planning", "normalization"],
  ["workflow", "prebound"],
  ["planning", "prebound"],
] as const)(
  "native binding rejects $0 $1 conflict without altering retained evidence",
  ([kind, change]) =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture(kind);
      const { store, sql, selection } = fixture;
      yield* fixture.dispatch("claim");
      yield* fixture.dispatch("message");
      if (change === "material") {
        yield* sql`DROP TRIGGER workflow_native_reservation_material_update_guard`;
        yield* sql`UPDATE workflow_native_reservation_material SET material_hash = ${nextHash}`;
      } else if (change === "proof") {
        yield* sql`DROP TRIGGER workflow_native_command_proofs_update_guard`;
        yield* sql`UPDATE workflow_native_command_proofs SET proof_hash = ${nextHash} WHERE phase = 'message'`;
      } else if (change === "partial") {
        yield* sql`DROP TRIGGER workflow_native_command_proofs_delete_guard`;
        yield* sql`DELETE FROM workflow_native_command_proofs WHERE phase = 'message'`;
      } else if (change === "receipt")
        yield* sql`UPDATE orchestration_command_receipts SET status = 'rejected' WHERE command_id = ${fixture.input.launchCommandId + ":initial-message"}`;
      else if (change === "normalization")
        yield* sql`UPDATE projection_projects SET workspace_root = '/changed-workspace'`;
      else
        yield* sql`UPDATE workflow_native_launch_intents SET native_run_id = 'conflicting-native-run'`;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      const error = yield* Effect.flip(store.bindNativeIntent(selection));
      assert.strictEqual(error.reason, change === "normalization" ? "stale" : "integrity");
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "native binding rolls back $0 SQL failures and owns factory/request input",
  (kind) =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture(kind);
      const { store, sql, selection } = fixture;
      yield* fixture.dispatch("claim");
      yield* fixture.dispatch("message");
      const subscription = yield* store.subscribeChanges;
      const before = yield* nativeBindingState;
      yield* sql`ALTER TABLE auth_sessions RENAME TO binding_hidden_sessions`;
      assert.strictEqual(
        (yield* Effect.flip(store.bindNativeIntent(selection))).reason,
        "persistence",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      yield* sql`ALTER TABLE binding_hidden_sessions RENAME TO auth_sessions`;
      yield* sql`CREATE TRIGGER reject_native_binding AFTER UPDATE OF native_run_id ON workflow_native_launch_intents WHEN NEW.native_run_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'injected binding failure'); END`;
      assert.strictEqual(
        (yield* Effect.flip(store.bindNativeIntent(selection))).reason,
        "persistence",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      yield* sql`DROP TRIGGER reject_native_binding`;
      const config = { ...fixture.config };
      const coordinator = { ...fixture.coordinator };
      const pendingStore = Store.makeCapturedNativeWorkflowRunStore(
        sql,
        config,
        fixture.path,
        fixture.identity,
        fixture.policy,
        coordinator,
      );
      config.baseDir = "/mutated-binding-config";
      coordinator.workerId = "mutated-worker";
      const owned = yield* pendingStore;
      const ownSubscription = yield* owned.subscribeChanges;
      const input = { ...selection };
      const pending = owned.bindNativeIntent(input);
      input.intentId = "mutated-intent";
      input.expectedFingerprint = nextHash;
      input.expectedGeneration = 1;
      assert.strictEqual((yield* pending).kind, "bound");
      yield* PubSub.take(ownSubscription);
      assert.strictEqual((yield* PubSub.takeUpTo(ownSubscription, 10)).length, 0);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "revoke", "bind"],
  ["workflow", "revoke", "control"],
  ["planning", "revoke", "bind"],
  ["planning", "revoke", "control"],
  ["workflow", "pause", "bind"],
  ["workflow", "pause", "control"],
  ["planning", "pause", "bind"],
  ["planning", "pause", "control"],
  ["workflow", "stop", "bind"],
  ["workflow", "stop", "control"],
  ["planning", "stop", "bind"],
  ["planning", "stop", "control"],
] as const)(
  "independent native binding $0/$1 preserves $2-first commit order",
  ([kind, action, first]) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-binding-order-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        const fileLayer = Store.layer.pipe(
          Layer.provideMerge(NodeSqliteClient.layer({ filename })),
        );
        yield* Effect.gen(function* () {
          const fixture = yield* nativeBindingFixture(kind);
          yield* fixture.dispatch("claim");
          const message = yield* fixture.dispatch("message");
          const created = message.storedEvents.find(({ event }) => event.type === "run.created");
          if (created?.event.type !== "run.created") throw new Error("actual run missing");
          yield* fixture.sql`PRAGMA journal_mode = WAL`;
          const sqlB = Context.get(
            yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
            SqlClient.SqlClient,
          );
          const other = yield* Store.makeCapturedNativeWorkflowRunStore(
            sqlB,
            fixture.config,
            fixture.path,
            fixture.identity,
            fixture.policy,
            fixture.coordinator,
          );
          const sessionsB = yield* AuthSessions.make.pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          );
          const control =
            action === "revoke"
              ? sessionsB
                  .revoke({
                    sessionId: fixture.authority.sessionId,
                    revokedAt: yield* DateTime.now,
                  })
                  .pipe(Effect.asVoid)
              : nativeClaimControl(kind, other, action).pipe(
                  Effect.provideService(SqlClient.SqlClient, sqlB),
                );
          const subscription = yield* fixture.store.subscribeChanges;
          let outcome;
          if (first === "control") {
            yield* control;
            outcome = yield* fixture.store.bindNativeIntent(fixture.selection);
          } else {
            outcome = yield* fixture.store.bindNativeIntent(fixture.selection);
            yield* control;
          }
          assert.deepStrictEqual(outcome, {
            kind: "bound",
            nativeRunId: created.event.payload.id,
            state: first === "control" ? "unknown" : "dispatching",
          });
          yield* PubSub.take(subscription);
          const beforeReplay = yield* nativeBindingState;
          const otherSubscription = yield* other.subscribeChanges;
          assert.deepStrictEqual(yield* other.bindNativeIntent(fixture.selection), {
            ...outcome,
            kind: "replay",
          });
          assert.deepStrictEqual(yield* nativeBindingState, beforeReplay);
          assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
          assert.strictEqual((yield* PubSub.takeUpTo(otherSubscription, 10)).length, 0);
          assert.deepStrictEqual(
            yield* sqlB`SELECT native_run_id,boot_id,worker_id FROM workflow_native_launch_intents`,
            [
              {
                native_run_id: created.event.payload.id,
                boot_id: "binding-boot",
                worker_id: "binding-worker",
              },
            ],
          );
        }).pipe(Effect.provide(fileLayer));
      }),
    ),
);

it.effect.each([
  ["workflow", "first"],
  ["workflow", "second"],
  ["planning", "first"],
  ["planning", "second"],
] as const)("independent native binding $0 has one $1-client winner", ([kind, first]) =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-binding-winner-")),
        ),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      yield* Effect.gen(function* () {
        const fixture = yield* nativeBindingFixture(kind);
        yield* fixture.dispatch("claim");
        yield* fixture.dispatch("message");
        const sqlB = Context.get(
          yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
          SqlClient.SqlClient,
        );
        const other = yield* Store.makeCapturedNativeWorkflowRunStore(
          sqlB,
          fixture.config,
          fixture.path,
          fixture.identity,
          fixture.policy,
          fixture.coordinator,
        );
        const firstStore = first === "first" ? fixture.store : other;
        const secondStore = first === "first" ? other : fixture.store;
        const firstSubscription = yield* firstStore.subscribeChanges;
        const secondSubscription = yield* secondStore.subscribeChanges;
        const winner = yield* firstStore.bindNativeIntent(fixture.selection);
        assert.strictEqual(winner.kind, "bound");
        yield* PubSub.take(firstSubscription);
        const before = yield* nativeBindingState;
        assert.deepStrictEqual(yield* secondStore.bindNativeIntent(fixture.selection), {
          ...winner,
          kind: "replay",
        });
        assert.deepStrictEqual(yield* nativeBindingState, before);
        assert.strictEqual((yield* PubSub.takeUpTo(firstSubscription, 10)).length, 0);
        assert.strictEqual((yield* PubSub.takeUpTo(secondSubscription, 10)).length, 0);
      }).pipe(
        Effect.provide(Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })))),
      );
    }),
  ),
);

it.effect.each(["workflow", "planning"] as const)(
  "native binding aborts $0 under another connection writer without changing custody",
  (kind) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-binding-lock-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        yield* Effect.gen(function* () {
          const fixture = yield* nativeBindingFixture(kind);
          yield* fixture.dispatch("claim");
          yield* fixture.dispatch("message");
          yield* fixture.sql`PRAGMA busy_timeout = 0`;
          const before = yield* nativeBindingState;
          const database = yield* Effect.acquireRelease(
            Effect.sync(() => {
              const connection = new NodeSqlite.DatabaseSync(filename);
              connection.exec("BEGIN IMMEDIATE");
              return connection;
            }),
            (connection) => Effect.sync(() => connection.close()),
          );
          assert.strictEqual(
            (yield* Effect.flip(fixture.store.bindNativeIntent(fixture.selection))).reason,
            "persistence",
          );
          yield* Effect.sync(() => database.exec("ROLLBACK"));
          assert.deepStrictEqual(yield* nativeBindingState, before);
        }).pipe(
          Effect.provide(
            Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename }))),
          ),
        );
      }),
    ),
);

it.effect.each([false, true])(
  "workflow native finalization preserves charged V1 admission with reuse=%s",
  (reuse) =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture("workflow", reuse);
      const { store, sql, selection, input } = fixture;
      if (!("runId" in input)) throw new Error("expected workflow input");
      yield* fixture.dispatch("claim");
      yield* fixture.dispatch("message");
      const bound = yield* store.bindNativeIntent(selection);
      if (bound.kind !== "bound") throw new Error("expected actual binding");
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.deepStrictEqual(yield* store.finalizeWorkflowNativeIntent(selection), {
        kind: "finalized",
        operationId: input.operationId,
        receiptId: input.receiptId,
      });
      yield* PubSub.take(subscription);
      const rows = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM workflow_operations WHERE id = ${input.operationId}`;
      const operation = yield* decodeStoredFinalizationOperation(parse(rows[0]!.record_json));
      assert.strictEqual(operation.status, "pending");
      if (operation.parameters.kind !== "stage_launch") throw new Error("wrong operation kind");
      assert.strictEqual(String(operation.parameters.nativeRunId), String(bound.nativeRunId));
      assert.strictEqual(operation.parameters.nativeThreadId, input.threadId);
      assert.strictEqual(operation.fingerprint, WorkflowCanonicalHash.operation(operation));
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_attempts`, [
        {
          id: input.attemptId,
          workflow_run_id: input.runId,
          operation_id: input.operationId,
          item_id: input.itemId,
          repair: 0,
        },
      ]);
      const actualCommand = {
        ...input.draft.admission.command,
        operationFingerprint: operation.fingerprint,
      };
      assert.deepStrictEqual(
        yield* sql`SELECT command_id,workflow_run_id,planning_session_id,fingerprint,receipt_id,operation_id,result_json FROM workflow_command_receipts`,
        [
          {
            command_id: actualCommand.commandId,
            workflow_run_id: input.runId,
            planning_session_id: null,
            fingerprint: WorkflowCanonicalHash.command(input.runId, actualCommand),
            receipt_id: input.receiptId,
            operation_id: input.operationId,
            result_json: json({ kind: "accepted", receiptId: input.receiptId }),
          },
        ],
      );
      const after = yield* nativeBindingState;
      assert.deepStrictEqual(after.runs, before.runs);
      assert.deepStrictEqual(after.material, before.material);
      assert.deepStrictEqual(after.receipts, before.receipts);
      assert.deepStrictEqual(after.nativeProofs, before.nativeProofs);
      assert.deepStrictEqual(after.intents, [
        { ...before.intents[0], state: "finalized", finalized_operation_id: input.operationId },
      ]);
      assert.deepStrictEqual(yield* store.finalizeWorkflowNativeIntent(selection), {
        kind: "replay",
        operationId: input.operationId,
        receiptId: input.receiptId,
      });
      assert.deepStrictEqual(
        yield* store.commitTransition({
          runId: input.runId,
          command: {
            ...actualCommand,
            fingerprint: WorkflowCanonicalHash.command(input.runId, actualCommand),
          },
          operation,
          externalFacts: admissionFacts,
          receiptId: input.receiptId,
        }),
        { kind: "replay", receiptId: input.receiptId },
      );
      assert.deepStrictEqual(yield* nativeBindingState, after);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

const workflowFinalizationFixture = Effect.fn("test.workflowFinalizationFixture")(function* () {
  const fixture = yield* nativeBindingFixture("workflow");
  yield* fixture.dispatch("claim");
  yield* fixture.dispatch("message");
  const bound = yield* fixture.store.bindNativeIntent(fixture.selection);
  if (bound.kind !== "bound") throw new Error("expected binding");
  return { ...fixture, bound };
});

it.effect.each(["pause", "stop", "revoke", "deadline", "unknown", "unbound"] as const)(
  "workflow native finalization refuses fresh %s work without creating pending records",
  (change) =>
    Effect.gen(function* () {
      const fixture = yield* workflowFinalizationFixture();
      const { store, sql, selection } = fixture;
      if (change === "pause" || change === "stop")
        yield* nativeClaimControl("workflow", store, change);
      else if (change === "revoke") yield* fixture.sessions.revoke(fixture.authority.sessionId);
      else if (change === "deadline")
        yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      else if (change === "unknown")
        yield* sql`UPDATE workflow_native_launch_intents SET state = 'unknown'`;
      else yield* sql`UPDATE workflow_native_launch_intents SET native_run_id = NULL`;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.notStrictEqual(
        (yield* store.finalizeWorkflowNativeIntent(selection)).kind,
        "finalized",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_attempts`, []);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "run",
  "run-time",
  "attempt",
  "attempt-turn",
  "root",
  "root-time",
  "root-request",
  "root-checkpoint",
] as const)("workflow native finalization refuses progressed %s native work", (change) =>
  Effect.gen(function* () {
    const { store, sql, selection } = yield* workflowFinalizationFixture();
    if (change === "run")
      yield* sql`UPDATE orchestration_v2_projection_runs SET status='running',payload_json=json_set(payload_json,'$.status','running')`;
    else if (change === "run-time")
      yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json=json_set(payload_json,'$.workStartedAt',${facts.now})`;
    else if (change === "attempt")
      yield* sql`UPDATE orchestration_v2_projection_run_attempts SET status='running',payload_json=json_set(payload_json,'$.status','running')`;
    else if (change === "attempt-turn")
      yield* sql`UPDATE orchestration_v2_projection_run_attempts SET provider_turn_id='started-turn',payload_json=json_set(payload_json,'$.providerTurnId','started-turn')`;
    else if (change === "root")
      yield* sql`UPDATE orchestration_v2_projection_nodes SET status='running',payload_json=json_set(payload_json,'$.status','running')`;
    else if (change === "root-time")
      yield* sql`UPDATE orchestration_v2_projection_nodes SET started_at=${facts.now},payload_json=json_set(payload_json,'$.startedAt',${facts.now})`;
    else if (change === "root-request")
      yield* sql`UPDATE orchestration_v2_projection_nodes SET payload_json=json_set(payload_json,'$.runtimeRequestId','runtime-request')`;
    else
      yield* sql`UPDATE orchestration_v2_projection_nodes SET payload_json=json_set(payload_json,'$.checkpointScopeId','checkpoint')`;
    const before = yield* nativeBindingState;
    const subscription = yield* store.subscribeChanges;
    assert.deepStrictEqual(yield* store.finalizeWorkflowNativeIntent(selection), {
      kind: "denied",
      reason: "state",
    });
    assert.deepStrictEqual(yield* nativeBindingState, before);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "workflow native finalization owns selection and refuses wrong construction, labels and captured transactions",
  () =>
    Effect.gen(function* () {
      const fixture = yield* workflowFinalizationFixture();
      const { store, sql, selection } = fixture;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.deepStrictEqual(
        yield* (yield* Store.WorkflowRunStore).finalizeWorkflowNativeIntent(selection),
        { kind: "denied", reason: "unavailable" },
      );
      for (const change of [{ expectedFingerprint: nextHash }, { expectedGeneration: 1 }])
        assert.deepStrictEqual(
          yield* store.finalizeWorkflowNativeIntent({ ...selection, ...change }),
          { kind: "stale" },
        );
      const wrong = yield* Store.makeCapturedNativeWorkflowRunStore(
        sql,
        fixture.config,
        fixture.path,
        fixture.identity,
        fixture.policy,
        { ...fixture.coordinator, workerId: "other" },
      );
      assert.deepStrictEqual(yield* wrong.finalizeWorkflowNativeIntent(selection), {
        kind: "stale",
      });
      for (const forged of [
        { ...selection, nativeRunId: "invented" },
        { ...selection, expectedGeneration: -1 },
      ])
        assert.strictEqual(
          (yield* Effect.flip(store.finalizeWorkflowNativeIntent(forged))).reason,
          "integrity",
        );
      assert.strictEqual(
        (yield* Effect.flip(sql.withTransaction(store.finalizeWorkflowNativeIntent(selection))))
          .reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      const mutable = { ...selection };
      const pending = store.finalizeWorkflowNativeIntent(mutable);
      mutable.intentId = "changed";
      mutable.expectedFingerprint = nextHash;
      assert.strictEqual((yield* pending).kind, "finalized");
      yield* PubSub.take(subscription);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "operation",
  "attempt",
  "receipt",
  "cas",
  "corrupt-attempt",
  "corrupt-receipt",
  "corrupt-operation",
] as const)(
  "workflow native finalization atomically rolls back %s failure without publication",
  (stage) =>
    Effect.gen(function* () {
      const { store, sql, selection } = yield* workflowFinalizationFixture();
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      if (stage === "operation")
        yield* sql`CREATE TRIGGER reject_finalization AFTER INSERT ON workflow_operations BEGIN SELECT RAISE(ABORT,'finalization failure'); END`;
      else if (stage === "attempt")
        yield* sql`CREATE TRIGGER reject_finalization AFTER INSERT ON workflow_attempts BEGIN SELECT RAISE(ABORT,'finalization failure'); END`;
      else if (stage === "receipt")
        yield* sql`CREATE TRIGGER reject_finalization AFTER INSERT ON workflow_command_receipts BEGIN SELECT RAISE(ABORT,'finalization failure'); END`;
      else if (stage === "corrupt-attempt")
        yield* sql`CREATE TRIGGER reject_finalization AFTER INSERT ON workflow_attempts BEGIN UPDATE workflow_attempts SET item_id='silently-replaced' WHERE id=NEW.id; END`;
      else if (stage === "corrupt-receipt")
        yield* sql`CREATE TRIGGER reject_finalization AFTER INSERT ON workflow_command_receipts BEGIN UPDATE workflow_command_receipts SET result_json='{}' WHERE command_id=NEW.command_id; END`;
      else if (stage === "corrupt-operation")
        yield* sql`CREATE TRIGGER reject_finalization AFTER INSERT ON workflow_operations BEGIN UPDATE workflow_operations SET generation=1,record_json=json_set(record_json,'$.controlGeneration',1) WHERE id=NEW.id; END`;
      else
        yield* sql`CREATE TRIGGER reject_finalization AFTER UPDATE OF state ON workflow_native_launch_intents WHEN NEW.state='finalized' BEGIN SELECT RAISE(ABORT,'finalization failure'); END`;
      assert.strictEqual(
        (yield* Effect.flip(store.finalizeWorkflowNativeIntent(selection))).reason,
        stage.startsWith("corrupt-") ? "integrity" : "persistence",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      yield* sql`DROP TRIGGER reject_finalization`;
      assert.strictEqual((yield* store.finalizeWorkflowNativeIntent(selection)).kind, "finalized");
      yield* PubSub.take(subscription);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "binding",
  "proof",
  "native-receipt",
  "native-event",
  "operation-id",
  "operation-access",
  "attempt-item",
  "attempt-repair",
  "receipt-result",
  "receipt-hash",
  "receipt-operation",
  "missing-attempt",
  "missing-receipt",
] as const)(
  "workflow native finalization rejects finalized %s tampering without repairing it",
  (change) =>
    Effect.gen(function* () {
      const fixture = yield* workflowFinalizationFixture();
      const { store, sql, selection } = fixture;
      yield* store.finalizeWorkflowNativeIntent(selection);
      if (change === "binding")
        yield* sql`UPDATE workflow_native_launch_intents SET native_run_id='wrong-run'`;
      else if (change === "proof") {
        yield* sql`DROP TRIGGER workflow_native_command_proofs_update_guard`;
        yield* sql`UPDATE workflow_native_command_proofs SET proof_hash=${nextHash} WHERE phase='message'`;
      } else if (change === "native-receipt")
        yield* sql`UPDATE orchestration_command_receipts SET status='rejected' WHERE command_id=${fixture.input.launchCommandId + ":initial-message"}`;
      else if (change === "native-event")
        yield* sql`UPDATE orchestration_events SET payload_json='{}' WHERE event_type='run.created'`;
      else if (change === "operation-id")
        yield* sql`UPDATE workflow_operations SET record_json=json_set(record_json,'$.id','substituted')`;
      else if (change === "operation-access")
        yield* sql`UPDATE workflow_operations SET record_json=json_set(record_json,'$.access.origins[0].projectId','substituted')`;
      else if (change === "attempt-item")
        yield* sql`UPDATE workflow_attempts SET item_id='substituted'`;
      else if (change === "attempt-repair") yield* sql`UPDATE workflow_attempts SET repair=1`;
      else if (change === "receipt-result")
        yield* sql`UPDATE workflow_command_receipts SET result_json='{"kind":"accepted","receiptId":"substituted"}'`;
      else if (change === "receipt-hash")
        yield* sql`UPDATE workflow_command_receipts SET fingerprint=${nextHash}`;
      else if (change === "receipt-operation")
        yield* sql`UPDATE workflow_command_receipts SET operation_id=NULL`;
      else if (change === "missing-attempt") yield* sql`DELETE FROM workflow_attempts`;
      else yield* sql`DELETE FROM workflow_command_receipts`;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.strictEqual(
        (yield* Effect.flip(store.finalizeWorkflowNativeIntent(selection))).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["operation", "attempt", "receipt"] as const)(
  "workflow native finalization rejects nonfinalized orphan %s records",
  (change) =>
    Effect.gen(function* () {
      const { store, sql, selection } = yield* workflowFinalizationFixture();
      yield* store.finalizeWorkflowNativeIntent(selection);
      yield* sql`UPDATE workflow_native_launch_intents SET state='dispatching',finalized_operation_id=NULL`;
      if (change !== "attempt") yield* sql`DELETE FROM workflow_attempts`;
      if (change !== "receipt") yield* sql`DELETE FROM workflow_command_receipts`;
      if (change === "receipt") {
        yield* sql`UPDATE workflow_command_receipts SET operation_id=NULL`;
        yield* sql`DELETE FROM workflow_operations`;
      }
      const before = yield* nativeBindingState;
      assert.strictEqual(
        (yield* Effect.flip(store.finalizeWorkflowNativeIntent(selection))).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["pause", "stop", "revoke", "deadline", "progress"] as const)(
  "workflow native finalization replays history after %s without notification or another charge",
  (change) =>
    Effect.gen(function* () {
      const fixture = yield* workflowFinalizationFixture();
      const { store, sql, selection } = fixture;
      const result = yield* store.finalizeWorkflowNativeIntent(selection);
      if (change === "pause" || change === "stop")
        yield* nativeClaimControl("workflow", store, change);
      else if (change === "revoke") yield* fixture.sessions.revoke(fixture.authority.sessionId);
      else if (change === "deadline")
        yield* TestClock.setTime(DateTime.makeUnsafe("2027-01-01T00:00:00Z").epochMilliseconds);
      else {
        yield* sql`UPDATE orchestration_v2_projection_runs SET status='running',payload_json=json_set(payload_json,'$.status','running')`;
        yield* sql`UPDATE workflow_operations SET status='settled',record_json=json_set(record_json,'$.status','settled','$.outcome',json(${json({ kind: "succeeded", summary: "Done", evidence: [] })}))`;
      }
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.deepStrictEqual(yield* store.finalizeWorkflowNativeIntent(selection), {
        ...result,
        kind: "replay",
      });
      if (!("runId" in fixture.input)) throw new Error("expected workflow");
      const rows = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM workflow_operations WHERE id=${fixture.input.operationId}`;
      const operation = yield* decodeStoredFinalizationOperation(parse(rows[0]!.record_json));
      for (const commandId of [
        fixture.input.draft.admission.command.commandId,
        "alternate-legacy-admission",
      ]) {
        const actual = {
          ...fixture.input.draft.admission.command,
          commandId,
          operationFingerprint: operation.fingerprint,
        };
        assert.deepStrictEqual(
          yield* store.commitTransition({
            runId: fixture.input.runId,
            command: {
              ...actual,
              fingerprint: WorkflowCanonicalHash.command(fixture.input.runId, actual),
            },
            operation,
            receiptId: fixture.input.receiptId,
            externalFacts: admissionFacts,
          }),
          { kind: "replay", receiptId: fixture.input.receiptId },
        );
      }
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["repair", "rearm"] as const)(
  "workflow native finalization preserves %s reservation history",
  (change) =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture(
        "workflow",
        false,
        2,
        change === "repair",
        change === "repair",
      );
      const { store, sql, input } = fixture;
      if (!("runId" in input)) throw new Error("expected workflow");
      const selection = { ...fixture.selection };
      if (change === "rearm") {
        yield* nativeClaimControl("workflow", store, "pause");
        assert.strictEqual(
          (yield* store.commitTransition({
            runId: run.id,
            command: {
              ...command,
              kind: "resume",
              commandId: "before-claim-resume",
              expectedVersion: 2,
              expectedGeneration: 1,
            },
            externalFacts: admissionFacts,
            receiptId: "before-claim-resume",
          })).kind,
          "accepted",
        );
        selection.expectedGeneration = 2;
        assert.strictEqual(
          (yield* store.claimNativeIntent({
            ...selection,
            expectedOwnerVersion: 3,
            ...fixture.coordinator,
          })).kind,
          "claimed",
        );
      }
      yield* fixture.dispatch("claim");
      yield* fixture.dispatch("message");
      yield* store.bindNativeIntent(selection);
      const before = yield* nativeBindingState;
      assert.strictEqual((yield* store.finalizeWorkflowNativeIntent(selection)).kind, "finalized");
      const rows = yield* sql<{
        record_json: string;
      }>`SELECT record_json FROM workflow_operations WHERE id=${input.operationId}`;
      const operation = yield* decodeStoredFinalizationOperation(parse(rows[0]!.record_json));
      assert.strictEqual(operation.controlGeneration, change === "rearm" ? 2 : 0);
      assert.deepStrictEqual(
        yield* sql`SELECT repair FROM workflow_attempts WHERE id=${input.attemptId}`,
        [{ repair: change === "repair" ? 1 : 0 }],
      );
      const actualCommand = {
        ...input.draft.admission.command,
        operationFingerprint: operation.fingerprint,
      };
      assert.strictEqual(actualCommand.expectedVersion, change === "repair" ? 1 : 0);
      assert.strictEqual(actualCommand.expectedGeneration, 0);
      assert.deepStrictEqual(
        yield* store.commitTransition({
          runId: input.runId,
          command: {
            ...actualCommand,
            fingerprint: WorkflowCanonicalHash.command(input.runId, actualCommand),
          },
          operation,
          receiptId: input.receiptId,
          externalFacts: admissionFacts,
        }),
        { kind: "replay", receiptId: input.receiptId },
      );
      assert.deepStrictEqual((yield* nativeBindingState).runs, before.runs);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["revoke", "finalize"],
  ["revoke", "control"],
  ["pause", "finalize"],
  ["pause", "control"],
  ["stop", "finalize"],
  ["stop", "control"],
] as const)(
  "workflow native finalization preserves independent $0/$1-first commit order",
  ([action, first]) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-finalize-order-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        yield* Effect.gen(function* () {
          const fixture = yield* workflowFinalizationFixture();
          const { store, sql, selection } = fixture;
          yield* sql`PRAGMA journal_mode=WAL`;
          const sqlB = Context.get(
            yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
            SqlClient.SqlClient,
          );
          const other = yield* Store.makeCapturedNativeWorkflowRunStore(
            sqlB,
            fixture.config,
            fixture.path,
            fixture.identity,
            fixture.policy,
            fixture.coordinator,
          );
          const sessionsB = yield* AuthSessions.make.pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          );
          const control =
            action === "revoke"
              ? sessionsB
                  .revoke({
                    sessionId: fixture.authority.sessionId,
                    revokedAt: yield* DateTime.now,
                  })
                  .pipe(Effect.asVoid)
              : nativeClaimControl("workflow", other, action).pipe(
                  Effect.provideService(SqlClient.SqlClient, sqlB),
                );
          const subscription = yield* store.subscribeChanges;
          if (first === "control") {
            yield* control;
            const before = yield* nativeBindingState;
            assert.notStrictEqual(
              (yield* store.finalizeWorkflowNativeIntent(selection)).kind,
              "finalized",
            );
            assert.deepStrictEqual(yield* nativeBindingState, before);
            assert.deepStrictEqual(yield* sqlB`SELECT * FROM workflow_attempts`, []);
          } else {
            const result = yield* store.finalizeWorkflowNativeIntent(selection);
            assert.strictEqual(result.kind, "finalized");
            yield* PubSub.take(subscription);
            yield* control;
            const before = yield* nativeBindingState;
            const otherSubscription = yield* other.subscribeChanges;
            assert.deepStrictEqual(yield* other.finalizeWorkflowNativeIntent(selection), {
              ...result,
              kind: "replay",
            });
            assert.deepStrictEqual(yield* nativeBindingState, before);
            assert.strictEqual((yield* PubSub.takeUpTo(otherSubscription, 10)).length, 0);
            assert.strictEqual((yield* sqlB`SELECT * FROM workflow_attempts`).length, 1);
          }
          assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        }).pipe(
          Effect.provide(
            Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename }))),
          ),
        );
      }),
    ),
);

it.effect.each([false, true])(
  "workflow native finalization has one independent writer winner with second=%s",
  (second) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-finalize-winner-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        yield* Effect.gen(function* () {
          const fixture = yield* workflowFinalizationFixture();
          const sqlB = Context.get(
            yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
            SqlClient.SqlClient,
          );
          const other = yield* Store.makeCapturedNativeWorkflowRunStore(
            sqlB,
            fixture.config,
            fixture.path,
            fixture.identity,
            fixture.policy,
            fixture.coordinator,
          );
          const first = second ? other : fixture.store;
          const next = second ? fixture.store : other;
          const subscription = yield* first.subscribeChanges;
          const nextSubscription = yield* next.subscribeChanges;
          const result = yield* first.finalizeWorkflowNativeIntent(fixture.selection);
          assert.strictEqual(result.kind, "finalized");
          yield* PubSub.take(subscription);
          const before = yield* nativeBindingState;
          assert.deepStrictEqual(yield* next.finalizeWorkflowNativeIntent(fixture.selection), {
            ...result,
            kind: "replay",
          });
          assert.deepStrictEqual(yield* nativeBindingState, before);
          assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
          assert.strictEqual((yield* PubSub.takeUpTo(nextSubscription, 10)).length, 0);
        }).pipe(
          Effect.provide(
            Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename }))),
          ),
        );
      }),
    ),
);

it.effect.each([
  "attempt-missing",
  "root-missing",
  "attempt-identity",
  "root-identity",
  "run-identity",
  "run-row",
] as const)("workflow native finalization rejects invalid %s preparing snapshots", (change) =>
  Effect.gen(function* () {
    const { store, sql, selection } = yield* workflowFinalizationFixture();
    if (change === "attempt-missing")
      yield* sql`DELETE FROM orchestration_v2_projection_run_attempts`;
    else if (change === "root-missing") yield* sql`DELETE FROM orchestration_v2_projection_nodes`;
    else if (change === "attempt-identity")
      yield* sql`UPDATE orchestration_v2_projection_run_attempts SET payload_json=json_set(payload_json,'$.rootNodeId','foreign-root')`;
    else if (change === "root-identity")
      yield* sql`UPDATE orchestration_v2_projection_nodes SET payload_json=json_set(payload_json,'$.providerThreadId','foreign-provider-thread')`;
    else if (change === "run-identity")
      yield* sql`UPDATE orchestration_v2_projection_runs SET payload_json=json_set(payload_json,'$.activeAttemptId','foreign-attempt')`;
    else yield* sql`UPDATE orchestration_v2_projection_runs SET status='running'`;
    const before = yield* nativeBindingState;
    const subscription = yield* store.subscribeChanges;
    assert.strictEqual(
      (yield* Effect.flip(store.finalizeWorkflowNativeIntent(selection))).reason,
      "integrity",
    );
    assert.deepStrictEqual(yield* nativeBindingState, before);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("workflow native finalization retains binding on live authority storage failure", () =>
  Effect.gen(function* () {
    const { store, sql, selection } = yield* workflowFinalizationFixture();
    yield* sql`ALTER TABLE auth_sessions RENAME TO finalization_hidden_sessions`;
    const before = yield* nativeBindingState;
    const subscription = yield* store.subscribeChanges;
    assert.strictEqual(
      (yield* Effect.flip(store.finalizeWorkflowNativeIntent(selection))).reason,
      "persistence",
    );
    assert.deepStrictEqual(yield* nativeBindingState, before);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    yield* sql`ALTER TABLE finalization_hidden_sessions RENAME TO auth_sessions`;
  }).pipe(Effect.provide(layer)),
);

it.effect.each([false, true])(
  "planning native finalization preserves charged V1 admission with reuse=%s",
  (reuse) =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture("planning", reuse);
      const { store, sql, selection, input } = fixture;
      if (!("sessionId" in input)) throw new Error("expected planning input");
      yield* fixture.dispatch("claim");
      yield* fixture.dispatch("message");
      const bound = yield* store.bindNativeIntent(selection);
      if (bound.kind !== "bound") throw new Error("expected binding");
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.deepStrictEqual(yield* store.finalizePlanningNativeIntent(selection), {
        kind: "finalized",
        operationId: input.operationId,
        passId: input.passId,
        receiptId: input.draft.admission.command.receiptId,
      });
      yield* PubSub.take(subscription);
      const result = yield* sql<{
        result_json: string;
        fingerprint: string;
      }>`SELECT result_json,fingerprint FROM workflow_command_receipts WHERE operation_id=${input.operationId}`;
      const value = yield* decodePlanningFinalizationValue(parse(result[0]!.result_json));
      assert.strictEqual(String(value.pass.native?.runId), String(bound.nativeRunId));
      assert.strictEqual(value.pass.status, "pending");
      assert.strictEqual(value.operation.status, "pending");
      assert.strictEqual(
        value.operation.fingerprint,
        WorkflowCanonicalHash.operation(value.operation),
      );
      const material = yield* decodeNativeReservationMaterial(
        parse(
          (yield* sql<{
            material_json: string;
          }>`SELECT material_json FROM workflow_native_reservation_material`)[0]!.material_json,
        ),
      );
      assert.deepStrictEqual(value.session, material.material.planningSession);
      const actual = {
        ...input.draft.admission,
        pass: value.pass,
        operation: value.operation,
        now: facts.now,
      };
      const old = yield* store.admitPlanningPass(actual);
      assert.deepStrictEqual(old, {
        kind: "replay",
        receiptId: input.draft.admission.command.receiptId,
        value,
      });
      assert.deepStrictEqual(yield* store.finalizePlanningNativeIntent(selection), {
        kind: "replay",
        operationId: input.operationId,
        passId: input.passId,
        receiptId: input.draft.admission.command.receiptId,
      });
      const after = yield* nativeBindingState;
      assert.deepStrictEqual(after.sessions, before.sessions);
      assert.deepStrictEqual(after.material, before.material);
      assert.deepStrictEqual(after.receipts, before.receipts);
      assert.deepStrictEqual(after.nativeProofs, before.nativeProofs);
      assert.deepStrictEqual(after.intents, [
        { ...before.intents[0], state: "finalized", finalized_operation_id: input.operationId },
      ]);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

const planningFinalizationFixture = Effect.fn("test.planningFinalizationFixture")(function* () {
  const fixture = yield* nativeBindingFixture("planning");
  yield* fixture.dispatch("claim");
  yield* fixture.dispatch("message");
  const bound = yield* fixture.store.bindNativeIntent(fixture.selection);
  if (bound.kind !== "bound") throw new Error("expected binding");
  return { ...fixture, bound };
});

it.effect.each(["pause", "stop", "revoke", "deadline", "unknown", "unbound", "progress"] as const)(
  "planning native finalization refuses fresh %s without another charge",
  (change) =>
    Effect.gen(function* () {
      const fixture = yield* planningFinalizationFixture();
      const { store, sql, selection } = fixture;
      if (change === "pause" || change === "stop")
        yield* nativeClaimControl("planning", store, change);
      else if (change === "revoke") yield* fixture.sessions.revoke(fixture.authority.sessionId);
      else if (change === "deadline")
        yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      else if (change === "unknown")
        yield* sql`UPDATE workflow_native_launch_intents SET state='unknown'`;
      else if (change === "unbound")
        yield* sql`UPDATE workflow_native_launch_intents SET native_run_id=NULL`;
      else
        yield* sql`UPDATE orchestration_v2_projection_runs SET status='running',payload_json=json_set(payload_json,'$.status','running')`;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.notStrictEqual(
        (yield* store.finalizePlanningNativeIntent(selection)).kind,
        "finalized",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_planning_passes`, []);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, []);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "planning native finalization preserves historical generation-zero session for a rearmed generation-two operation",
  () =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture("planning", false, 2, false);
      const { store, sql, input } = fixture;
      if (!("sessionId" in input)) throw new Error("expected planning");
      yield* nativeClaimControl("planning", store, "pause");
      yield* store.controlPlanning({
        sessionId: input.sessionId,
        action: "resume",
        expectedVersion: 2,
        now: facts.now,
        context,
        command: { commandId: "finalization-resume", receiptId: "finalization-resume" },
      });
      const selection = { ...fixture.selection, expectedGeneration: 2 };
      assert.strictEqual(
        (yield* store.claimNativeIntent({
          ...selection,
          expectedOwnerVersion: 3,
          ...fixture.coordinator,
        })).kind,
        "claimed",
      );
      yield* fixture.dispatch("claim");
      yield* fixture.dispatch("message");
      yield* store.bindNativeIntent(selection);
      const before = yield* nativeBindingState;
      assert.strictEqual((yield* store.finalizePlanningNativeIntent(selection)).kind, "finalized");
      const receipts = yield* sql<{
        result_json: string;
      }>`SELECT result_json FROM workflow_command_receipts WHERE operation_id=${input.operationId}`;
      const value = yield* decodePlanningFinalizationValue(parse(receipts[0]!.result_json));
      assert.strictEqual(value.session.controlGeneration, 0);
      assert.strictEqual(value.session.recordVersion, 1);
      assert.strictEqual(value.session.consumed.passes, 1);
      assert.strictEqual(value.operation.controlGeneration, 2);
      const current = yield* store.getPlanning({ sessionId: input.sessionId });
      assert.strictEqual(current.controlGeneration, 2);
      assert.strictEqual(current.recordVersion, 3);
      assert.deepStrictEqual((yield* nativeBindingState).sessions, before.sessions);
      assert.deepStrictEqual(
        yield* store.admitPlanningPass({
          ...input.draft.admission,
          pass: value.pass,
          operation: value.operation,
          now: facts.now,
        }),
        { kind: "replay", receiptId: input.draft.admission.command.receiptId, value },
      );
      assert.strictEqual((yield* store.finalizePlanningNativeIntent(selection)).kind, "replay");
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["pause", "stop", "revoke", "deadline", "progress"] as const)(
  "planning native finalization replays exact saved V1 result after %s",
  (change) =>
    Effect.gen(function* () {
      const fixture = yield* planningFinalizationFixture();
      const { store, sql, selection, input } = fixture;
      if (!("sessionId" in input)) throw new Error("expected planning");
      const result = yield* store.finalizePlanningNativeIntent(selection);
      const receipts = yield* sql<{
        result_json: string;
      }>`SELECT result_json FROM workflow_command_receipts WHERE operation_id=${input.operationId}`;
      const value = yield* decodePlanningFinalizationValue(parse(receipts[0]!.result_json));
      if (change === "pause" || change === "stop")
        yield* nativeClaimControl("planning", store, change);
      else if (change === "revoke") yield* fixture.sessions.revoke(fixture.authority.sessionId);
      else if (change === "deadline")
        yield* TestClock.setTime(DateTime.makeUnsafe("2027-01-01T00:00:00Z").epochMilliseconds);
      else {
        yield* sql`UPDATE orchestration_v2_projection_runs SET status='running',payload_json=json_set(payload_json,'$.status','running')`;
        yield* sql`UPDATE workflow_planning_passes SET record_json=json_set(record_json,'$.status','settled','$.native.terminalMessageId','terminal','$.report',json(${json({ kind: "author", summary: "Done", outputs: [] })}))`;
        yield* sql`UPDATE workflow_operations SET status='settled',record_json=json_set(record_json,'$.status','settled','$.outcome',json(${json({ kind: "succeeded", summary: "Done", evidence: [] })}))`;
      }
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.deepStrictEqual(yield* store.finalizePlanningNativeIntent(selection), {
        ...result,
        kind: "replay",
      });
      assert.deepStrictEqual(
        yield* store.admitPlanningPass({
          ...input.draft.admission,
          pass: value.pass,
          operation: value.operation,
          now: facts.now,
        }),
        { kind: "replay", receiptId: input.draft.admission.command.receiptId, value },
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "planning native finalization owns selection and refuses wrong construction, labels and captured transactions",
  () =>
    Effect.gen(function* () {
      const fixture = yield* planningFinalizationFixture();
      const { store, sql, selection } = fixture;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.deepStrictEqual(
        yield* (yield* Store.WorkflowRunStore).finalizePlanningNativeIntent(selection),
        { kind: "denied", reason: "unavailable" },
      );
      for (const change of [{ expectedFingerprint: nextHash }, { expectedGeneration: 1 }])
        assert.deepStrictEqual(
          yield* store.finalizePlanningNativeIntent({ ...selection, ...change }),
          { kind: "stale" },
        );
      const wrong = yield* Store.makeCapturedNativeWorkflowRunStore(
        sql,
        fixture.config,
        fixture.path,
        fixture.identity,
        fixture.policy,
        { ...fixture.coordinator, workerId: "other" },
      );
      assert.deepStrictEqual(yield* wrong.finalizePlanningNativeIntent(selection), {
        kind: "stale",
      });
      const forged = { ...selection, nativeRunId: "invented" };
      assert.strictEqual(
        (yield* Effect.flip(store.finalizePlanningNativeIntent(forged))).reason,
        "integrity",
      );
      assert.strictEqual(
        (yield* Effect.flip(sql.withTransaction(store.finalizePlanningNativeIntent(selection))))
          .reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      const mutable = { ...selection };
      const pending = store.finalizePlanningNativeIntent(mutable);
      mutable.intentId = "mutated";
      assert.strictEqual((yield* pending).kind, "finalized");
      yield* PubSub.take(subscription);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "operation",
  "pass",
  "receipt",
  "cas",
  "corrupt-pass",
  "corrupt-receipt",
  "corrupt-operation",
] as const)(
  "planning native finalization atomically rolls back %s failure without publication",
  (stage) =>
    Effect.gen(function* () {
      const { store, sql, selection } = yield* planningFinalizationFixture();
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      if (stage === "operation")
        yield* sql`CREATE TRIGGER reject_planning_finalization AFTER INSERT ON workflow_operations BEGIN SELECT RAISE(ABORT,'finalization failure'); END`;
      else if (stage === "pass")
        yield* sql`CREATE TRIGGER reject_planning_finalization AFTER INSERT ON workflow_planning_passes BEGIN SELECT RAISE(ABORT,'finalization failure'); END`;
      else if (stage === "receipt")
        yield* sql`CREATE TRIGGER reject_planning_finalization AFTER INSERT ON workflow_command_receipts BEGIN SELECT RAISE(ABORT,'finalization failure'); END`;
      else if (stage === "corrupt-pass")
        yield* sql`CREATE TRIGGER reject_planning_finalization AFTER INSERT ON workflow_planning_passes BEGIN UPDATE workflow_planning_passes SET record_json=json_set(record_json,'$.status','claimed') WHERE id=NEW.id; END`;
      else if (stage === "corrupt-receipt")
        yield* sql`CREATE TRIGGER reject_planning_finalization AFTER INSERT ON workflow_command_receipts BEGIN UPDATE workflow_command_receipts SET result_json='{}' WHERE command_id=NEW.command_id; END`;
      else if (stage === "corrupt-operation")
        yield* sql`CREATE TRIGGER reject_planning_finalization AFTER INSERT ON workflow_operations BEGIN UPDATE workflow_operations SET generation=1,record_json=json_set(record_json,'$.controlGeneration',1) WHERE id=NEW.id; END`;
      else
        yield* sql`CREATE TRIGGER reject_planning_finalization AFTER UPDATE OF state ON workflow_native_launch_intents WHEN NEW.state='finalized' BEGIN SELECT RAISE(ABORT,'finalization failure'); END`;
      assert.strictEqual(
        (yield* Effect.flip(store.finalizePlanningNativeIntent(selection))).reason,
        stage.startsWith("corrupt-") ? "integrity" : "persistence",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      yield* sql`DROP TRIGGER reject_planning_finalization`;
      assert.strictEqual((yield* store.finalizePlanningNativeIntent(selection)).kind, "finalized");
      yield* PubSub.take(subscription);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  "binding",
  "material",
  "proof",
  "pass-id",
  "pass-access",
  "pass-native",
  "pass-provider",
  "operation-access",
  "receipt-session",
  "receipt-pass",
  "receipt-operation",
  "receipt-hash",
  "missing-pass",
  "missing-receipt",
] as const)(
  "planning native finalization rejects finalized %s tampering without repairing it",
  (change) =>
    Effect.gen(function* () {
      const fixture = yield* planningFinalizationFixture();
      const { store, sql, selection, input } = fixture;
      yield* store.finalizePlanningNativeIntent(selection);
      if (change === "binding")
        yield* sql`UPDATE workflow_native_launch_intents SET native_run_id='foreign-run'`;
      else if (change === "material") {
        yield* sql`DROP TRIGGER workflow_native_reservation_material_update_guard`;
        yield* sql`UPDATE workflow_native_reservation_material SET material_hash=${nextHash}`;
      } else if (change === "proof") {
        yield* sql`DROP TRIGGER workflow_native_command_proofs_update_guard`;
        yield* sql`UPDATE workflow_native_command_proofs SET proof_hash=${nextHash} WHERE phase='message'`;
      } else if (change === "pass-id")
        yield* sql`UPDATE workflow_planning_passes SET record_json=json_set(record_json,'$.id','substituted')`;
      else if (change === "pass-access")
        yield* sql`UPDATE workflow_planning_passes SET record_json=json_set(record_json,'$.access.origins',json('[{"environmentId":"environment","projectId":"foreign-project"}]'))`;
      else if (change === "pass-native")
        yield* sql`UPDATE workflow_planning_passes SET native_run_id='foreign-run',record_json=json_set(record_json,'$.native.runId','foreign-run')`;
      else if (change === "pass-provider")
        yield* sql`UPDATE workflow_planning_passes SET record_json=json_set(record_json,'$.provider.model','foreign-model')`;
      else if (change === "operation-access")
        yield* sql`UPDATE workflow_operations SET record_json=json_set(record_json,'$.access.origins',json('[{"environmentId":"environment","projectId":"foreign-project"}]'))`;
      else if (change === "receipt-session")
        yield* sql`UPDATE workflow_command_receipts SET result_json=json_set(result_json,'$.session.controlGeneration',1) WHERE operation_id=${input.operationId}`;
      else if (change === "receipt-pass")
        yield* sql`UPDATE workflow_command_receipts SET result_json=json_set(result_json,'$.pass.status','claimed') WHERE operation_id=${input.operationId}`;
      else if (change === "receipt-operation")
        yield* sql`UPDATE workflow_command_receipts SET result_json=json_set(result_json,'$.operation.controlGeneration',1) WHERE operation_id=${input.operationId}`;
      else if (change === "receipt-hash")
        yield* sql`UPDATE workflow_command_receipts SET fingerprint=${nextHash} WHERE operation_id=${input.operationId}`;
      else if (change === "missing-pass") yield* sql`DELETE FROM workflow_planning_passes`;
      else
        yield* sql`DELETE FROM workflow_command_receipts WHERE operation_id=${input.operationId}`;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.strictEqual(
        (yield* Effect.flip(store.finalizePlanningNativeIntent(selection))).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["operation", "pass", "receipt"] as const)(
  "planning native finalization rejects nonfinalized orphan %s records",
  (change) =>
    Effect.gen(function* () {
      const { store, sql, selection, input } = yield* planningFinalizationFixture();
      yield* store.finalizePlanningNativeIntent(selection);
      yield* sql`UPDATE workflow_native_launch_intents SET state='dispatching',finalized_operation_id=NULL`;
      if (change !== "pass") yield* sql`DELETE FROM workflow_planning_passes`;
      if (change !== "receipt")
        yield* sql`DELETE FROM workflow_command_receipts WHERE operation_id=${input.operationId}`;
      else {
        yield* sql`UPDATE workflow_command_receipts SET operation_id=NULL WHERE operation_id=${input.operationId}`;
        yield* sql`DELETE FROM workflow_operations`;
      }
      const before = yield* nativeBindingState;
      assert.strictEqual(
        (yield* Effect.flip(store.finalizePlanningNativeIntent(selection))).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["revoke", "finalize"],
  ["revoke", "control"],
  ["pause", "finalize"],
  ["pause", "control"],
  ["stop", "finalize"],
  ["stop", "control"],
] as const)(
  "planning native finalization preserves independent $0/$1-first commit order",
  ([action, first]) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-finalize-order-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        yield* Effect.gen(function* () {
          const fixture = yield* planningFinalizationFixture();
          const { store, sql, selection } = fixture;
          yield* sql`PRAGMA journal_mode=WAL`;
          const sqlB = Context.get(
            yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
            SqlClient.SqlClient,
          );
          const other = yield* Store.makeCapturedNativeWorkflowRunStore(
            sqlB,
            fixture.config,
            fixture.path,
            fixture.identity,
            fixture.policy,
            fixture.coordinator,
          );
          const sessionsB = yield* AuthSessions.make.pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          );
          const control =
            action === "revoke"
              ? sessionsB
                  .revoke({
                    sessionId: fixture.authority.sessionId,
                    revokedAt: yield* DateTime.now,
                  })
                  .pipe(Effect.asVoid)
              : nativeClaimControl("planning", other, action).pipe(
                  Effect.provideService(SqlClient.SqlClient, sqlB),
                );
          const subscription = yield* store.subscribeChanges;
          if (first === "control") {
            yield* control;
            const before = yield* nativeBindingState;
            assert.notStrictEqual(
              (yield* store.finalizePlanningNativeIntent(selection)).kind,
              "finalized",
            );
            assert.deepStrictEqual(yield* nativeBindingState, before);
            assert.deepStrictEqual(yield* sqlB`SELECT * FROM workflow_planning_passes`, []);
          } else {
            const result = yield* store.finalizePlanningNativeIntent(selection);
            assert.strictEqual(result.kind, "finalized");
            yield* PubSub.take(subscription);
            yield* control;
            const before = yield* nativeBindingState;
            const otherSubscription = yield* other.subscribeChanges;
            assert.deepStrictEqual(yield* other.finalizePlanningNativeIntent(selection), {
              ...result,
              kind: "replay",
            });
            assert.deepStrictEqual(yield* nativeBindingState, before);
            assert.strictEqual((yield* PubSub.takeUpTo(otherSubscription, 10)).length, 0);
            assert.strictEqual((yield* sqlB`SELECT * FROM workflow_planning_passes`).length, 1);
          }
          assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        }).pipe(
          Effect.provide(
            Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename }))),
          ),
        );
      }),
    ),
);

it.effect.each([false, true])(
  "planning native finalization has one independent writer winner with second=%s",
  (second) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-finalize-winner-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        yield* Effect.gen(function* () {
          const fixture = yield* planningFinalizationFixture();
          const sqlB = Context.get(
            yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
            SqlClient.SqlClient,
          );
          const other = yield* Store.makeCapturedNativeWorkflowRunStore(
            sqlB,
            fixture.config,
            fixture.path,
            fixture.identity,
            fixture.policy,
            fixture.coordinator,
          );
          const first = second ? other : fixture.store;
          const next = second ? fixture.store : other;
          const subscription = yield* first.subscribeChanges;
          const nextSubscription = yield* next.subscribeChanges;
          const result = yield* first.finalizePlanningNativeIntent(fixture.selection);
          assert.strictEqual(result.kind, "finalized");
          yield* PubSub.take(subscription);
          const before = yield* nativeBindingState;
          assert.deepStrictEqual(yield* next.finalizePlanningNativeIntent(fixture.selection), {
            ...result,
            kind: "replay",
          });
          assert.deepStrictEqual(yield* nativeBindingState, before);
          assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
          assert.strictEqual((yield* PubSub.takeUpTo(nextSubscription, 10)).length, 0);
        }).pipe(
          Effect.provide(
            Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename }))),
          ),
        );
      }),
    ),
);

it.effect.each([
  ["workflow", "revoke"],
  ["planning", "revoke"],
  ["workflow", "deadline"],
  ["planning", "deadline"],
] as const)(
  "native execution claim rejects $0 $1 despite trusted boolean and forged clock",
  ([kind, change]) =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture(kind);
      const { store, selection, input } = fixture;
      yield* fixture.dispatch("claim");
      yield* fixture.dispatch("message");
      yield* store.bindNativeIntent(selection);
      if (kind === "workflow") yield* store.finalizeWorkflowNativeIntent(selection);
      else yield* store.finalizePlanningNativeIntent(selection);
      if (change === "revoke") yield* fixture.sessions.revoke(fixture.authority.sessionId);
      else yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.strictEqual(
        (yield* store.claim({
          operationId: input.operationId,
          expectedOwnerVersion: 1,
          expectedGeneration: 0,
          ...fixture.coordinator,
          now: facts.now,
          trustedEligibility: true,
        })).kind,
        "denied",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

const nativeExecutionFixture = Effect.fn("test.nativeExecutionFixture")(function* (
  kind: "workflow" | "planning",
  rearm = false,
  maxPlanningPasses = 1,
) {
  const fixture = yield* nativeBindingFixture(kind, false, 2, true, false, maxPlanningPasses);
  const { store, selection, input } = fixture;
  yield* fixture.dispatch("claim");
  yield* fixture.dispatch("message");
  yield* store.bindNativeIntent(selection);
  if (kind === "workflow") yield* store.finalizeWorkflowNativeIntent(selection);
  else yield* store.finalizePlanningNativeIntent(selection);
  if (rearm) {
    yield* nativeClaimControl(kind, store, "pause");
    if (kind === "workflow")
      yield* store.commitTransition({
        runId: run.id,
        command: {
          ...command,
          kind: "resume",
          commandId: "execution-resume",
          expectedVersion: 2,
          expectedGeneration: 1,
        },
        receiptId: "execution-resume",
        externalFacts: admissionFacts,
      });
    else
      yield* store.controlPlanning({
        sessionId: session.id,
        action: "resume",
        expectedVersion: 2,
        now: facts.now,
        context,
        command: { commandId: "execution-resume", receiptId: "execution-resume" },
      });
  }
  const rows = yield* fixture.sql<{
    record_json: string;
  }>`SELECT record_json FROM workflow_operations WHERE id=${input.operationId}`;
  const pendingOperation = yield* decodeStoredFinalizationOperation(parse(rows[0]!.record_json));
  return {
    ...fixture,
    pendingOperation,
    claimInput: {
      operationId: input.operationId,
      expectedOwnerVersion: rearm ? 3 : 1,
      expectedGeneration: rearm ? 2 : 0,
      ...fixture.coordinator,
      now: "1900-01-01T00:00:00Z",
      trustedEligibility: true,
    },
  };
});

it.effect.each([
  ["workflow", false],
  ["planning", false],
  ["workflow", true],
  ["planning", true],
] as const)(
  "native execution claim verifies $0 current authority and rearm=$1 without charging",
  ([kind, rearm]) =>
    Effect.gen(function* () {
      const fixture = yield* nativeExecutionFixture(kind, rearm);
      const { store, sql, claimInput, input, pendingOperation } = fixture;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      const mutable = { ...claimInput };
      const pending = store.claim(mutable);
      mutable.expectedGeneration = 999;
      mutable.workerId = "mutated";
      const result = yield* pending;
      assert.strictEqual(result.kind, "claimed");
      if (result.kind !== "claimed") throw new Error("expected native operation claim");
      assert.deepStrictEqual(
        result.operation,
        decodeOperationFixture({
          ...pendingOperation,
          status: "claimed",
          claim: {
            ...fixture.coordinator,
            generation: rearm ? 2 : 0,
            claimedAt: DateTime.formatIso(DateTime.makeUnsafe(facts.now)),
          },
        }),
      );
      yield* PubSub.take(subscription);
      assert.strictEqual((yield* store.claim(claimInput)).kind, "stale");
      if (kind === "workflow" && "runId" in input) {
        assert.strictEqual(
          (yield* store.finalizeWorkflowNativeIntent(fixture.selection)).kind,
          "replay",
        );
        const actual = {
          ...input.draft.admission.command,
          operationFingerprint: pendingOperation.fingerprint,
        };
        assert.deepStrictEqual(
          yield* store.commitTransition({
            runId: input.runId,
            command: { ...actual, fingerprint: WorkflowCanonicalHash.command(input.runId, actual) },
            operation: pendingOperation,
            receiptId: input.receiptId,
            externalFacts: admissionFacts,
          }),
          { kind: "replay", receiptId: input.receiptId },
        );
      } else if ("sessionId" in input) {
        assert.strictEqual(
          (yield* store.finalizePlanningNativeIntent(fixture.selection)).kind,
          "replay",
        );
        const rows = yield* sql<{
          result_json: string;
        }>`SELECT result_json FROM workflow_command_receipts WHERE operation_id=${input.operationId}`;
        const value = yield* decodePlanningFinalizationValue(parse(rows[0]!.result_json));
        assert.strictEqual(value.session.controlGeneration, 0);
        assert.strictEqual(value.operation.controlGeneration, 0);
        assert.strictEqual(
          (yield* store.admitPlanningPass({
            ...input.draft.admission,
            pass: value.pass,
            operation: value.operation,
            now: facts.now,
          })).kind,
          "replay",
        );
      }
      const after = yield* nativeBindingState;
      assert.deepStrictEqual(after.runs, before.runs);
      assert.deepStrictEqual(after.sessions, before.sessions);
      assert.deepStrictEqual(after.intents, before.intents);
      assert.deepStrictEqual(after.receipts, before.receipts);
      assert.deepStrictEqual(after.commands, before.commands);
      assert.deepStrictEqual(after.material, before.material);
      assert.deepStrictEqual(after.nativeProofs, before.nativeProofs);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "plain"],
  ["planning", "plain"],
  ["workflow", "labels"],
  ["planning", "labels"],
  ["workflow", "nested"],
  ["planning", "nested"],
  ["workflow", "held"],
  ["planning", "held"],
  ["workflow", "stop"],
  ["planning", "stop"],
  ["workflow", "scope"],
  ["planning", "scope"],
  ["workflow", "expiry"],
  ["planning", "expiry"],
  ["workflow", "progress"],
  ["planning", "progress"],
  ["workflow", "cancel"],
  ["planning", "cancel"],
] as const)("native execution claim refuses $0 $1 without writes", ([kind, change]) =>
  Effect.gen(function* () {
    const fixture = yield* nativeExecutionFixture(kind);
    const { store, sql, claimInput } = fixture;
    let selected = store;
    const request = { ...claimInput };
    if (change === "plain") selected = yield* Store.WorkflowRunStore;
    else if (change === "labels") request.workerId = "foreign-worker";
    else if (change === "held") yield* nativeClaimControl(kind, store, "pause");
    else if (change === "stop") yield* nativeClaimControl(kind, store, "stop");
    else if (change === "scope")
      yield* sql`UPDATE auth_sessions SET scopes='["orchestration:read"]' WHERE session_id=${fixture.authority.sessionId}`;
    else if (change === "expiry")
      yield* sql`UPDATE auth_sessions SET expires_at=${facts.now} WHERE session_id=${fixture.authority.sessionId}`;
    else if (change === "progress")
      yield* sql`UPDATE orchestration_v2_projection_runs SET status='running',payload_json=json_set(payload_json,'$.status','running')`;
    else if (change === "cancel") yield* sql`UPDATE workflow_operations SET cancel_requested=1`;
    const before = yield* nativeBindingState;
    const subscription = yield* store.subscribeChanges;
    if (change === "nested")
      assert.strictEqual(
        (yield* Effect.flip(sql.withTransaction(store.claim(request)))).reason,
        "integrity",
      );
    else assert.notStrictEqual((yield* selected.claim(request)).kind, "claimed");
    assert.deepStrictEqual(yield* nativeBindingState, before);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect(
  "native execution claim preserves a manual legacy claim inside its existing transaction",
  () =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const store = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      const operation = operationFor("manual-operation", "manual-attempt");
      assert.strictEqual(
        (yield* store.commitTransition({
          runId: run.id,
          command: admission(operation, "manual-attempt"),
          operation,
          externalFacts: admissionFacts,
          receiptId: "manual-receipt",
        })).kind,
        "accepted",
      );
      const result = yield* sql.withTransaction(
        store.claim({
          operationId: operation.id,
          expectedOwnerVersion: 1,
          expectedGeneration: 0,
          bootId: "legacy-boot",
          workerId: "legacy-worker",
          now: facts.now,
          trustedEligibility: true,
        }),
      );
      assert.strictEqual(result.kind, "claimed");
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "revoke"],
  ["planning", "revoke"],
  ["workflow", "deadline"],
  ["planning", "deadline"],
  ["workflow", "cancel"],
  ["planning", "cancel"],
  ["workflow", "progress"],
  ["planning", "progress"],
  ["workflow", "plain"],
  ["planning", "plain"],
] as const)(
  "native execution rearm refuses $0 $1 and preserves its last charge",
  ([kind, change]) =>
    Effect.gen(function* () {
      const fixture = yield* nativeExecutionFixture(kind);
      const { store, sql } = fixture;
      yield* nativeClaimControl(kind, store, "pause");
      if (change === "revoke") yield* fixture.sessions.revoke(fixture.authority.sessionId);
      else if (change === "deadline")
        yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      else if (change === "cancel") yield* sql`UPDATE workflow_operations SET cancel_requested=1`;
      else if (change === "progress")
        yield* sql`UPDATE orchestration_v2_projection_runs SET status='running',payload_json=json_set(payload_json,'$.status','running')`;
      const selected = change === "plain" ? yield* Store.WorkflowRunStore : store;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      if (kind === "workflow") {
        const outcome = yield* selected.commitTransition({
          runId: run.id,
          command: {
            ...command,
            kind: "resume",
            commandId: "refused-resume",
            expectedVersion: 2,
            expectedGeneration: 1,
          },
          receiptId: "refused-resume",
          externalFacts: {
            ...admissionFacts,
            now: facts.now,
            verifiedNativeHeldOperations: [
              {
                operationId: fixture.input.operationId,
                fingerprint: fixture.pendingOperation.fingerprint,
              },
            ],
          },
        });
        assert.notStrictEqual(outcome.kind, "accepted");
      } else
        assert.isTrue(
          Exit.isFailure(
            yield* Effect.exit(
              selected.controlPlanning({
                sessionId: session.id,
                action: "resume",
                expectedVersion: 2,
                now: facts.now,
                context,
                command: { commandId: "refused-resume", receiptId: "refused-resume" },
              }),
            ),
          ),
        );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "receipt-hash"],
  ["planning", "receipt-hash"],
  ["workflow", "receipt-result"],
  ["planning", "receipt-result"],
  ["workflow", "owned-artifact"],
  ["planning", "owned-artifact"],
  ["workflow", "proof"],
  ["planning", "proof"],
  ["workflow", "unknown"],
  ["planning", "unknown"],
] as const)(
  "native execution claim rejects $0 $1 journal corruption without legacy fallback",
  ([kind, change]) =>
    Effect.gen(function* () {
      const fixture = yield* nativeExecutionFixture(kind);
      const { store, sql, input, claimInput } = fixture;
      if (change === "receipt-hash")
        yield* sql`UPDATE workflow_command_receipts SET fingerprint=${nextHash} WHERE operation_id=${input.operationId}`;
      else if (change === "receipt-result")
        yield* sql`UPDATE workflow_command_receipts SET result_json='{}' WHERE operation_id=${input.operationId}`;
      else if (change === "proof") {
        yield* sql`DROP TRIGGER workflow_native_command_proofs_update_guard`;
        yield* sql`UPDATE workflow_native_command_proofs SET proof_hash=${nextHash} WHERE phase='message'`;
      } else if (change === "unknown")
        yield* sql`UPDATE workflow_native_launch_intents SET state='unknown',finalized_operation_id=NULL`;
      else if (kind === "workflow") yield* sql`UPDATE workflow_attempts SET item_id='foreign-item'`;
      else
        yield* sql`UPDATE workflow_planning_passes SET record_json=json_set(record_json,'$.access.origins',json('[{"environmentId":"environment","projectId":"foreign-project"}]'))`;
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      assert.strictEqual((yield* Effect.flip(store.claim(claimInput))).reason, "integrity");
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "sql"],
  ["planning", "sql"],
  ["workflow", "claim-label"],
  ["planning", "claim-label"],
  ["workflow", "cancel-flag"],
  ["planning", "cancel-flag"],
  ["planning", "pass-sql"],
  ["planning", "pass-status"],
] as const)(
  "native execution claim rolls back $0 $1 failure without notification",
  ([kind, stage]) =>
    Effect.gen(function* () {
      const { store, sql, claimInput } = yield* nativeExecutionFixture(kind);
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      if (stage === "sql")
        yield* sql`CREATE TRIGGER reject_native_execution AFTER UPDATE OF status ON workflow_operations WHEN NEW.status='claimed' BEGIN SELECT RAISE(ABORT,'claim failure'); END`;
      else if (stage === "claim-label")
        yield* sql`CREATE TRIGGER reject_native_execution AFTER UPDATE OF status ON workflow_operations WHEN NEW.status='claimed' BEGIN UPDATE workflow_operations SET record_json=json_set(record_json,'$.claim.workerId','corrupted-worker') WHERE id=NEW.id; END`;
      else if (stage === "cancel-flag")
        yield* sql`CREATE TRIGGER reject_native_execution AFTER UPDATE OF status ON workflow_operations WHEN NEW.status='claimed' BEGIN UPDATE workflow_operations SET cancel_requested=1 WHERE id=NEW.id; END`;
      else if (stage === "pass-sql")
        yield* sql`CREATE TRIGGER reject_native_execution AFTER UPDATE OF record_json ON workflow_planning_passes WHEN json_extract(NEW.record_json,'$.status')='claimed' BEGIN SELECT RAISE(ABORT,'claim failure'); END`;
      else
        yield* sql`CREATE TRIGGER reject_native_execution AFTER UPDATE OF record_json ON workflow_planning_passes WHEN json_extract(NEW.record_json,'$.status')='claimed' BEGIN UPDATE workflow_planning_passes SET record_json=json_set(record_json,'$.status','pending') WHERE id=NEW.id; END`;
      assert.strictEqual(
        (yield* Effect.flip(store.claim(claimInput))).reason,
        stage === "sql" || stage === "pass-sql" ? "persistence" : "integrity",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      yield* sql`DROP TRIGGER reject_native_execution`;
      assert.strictEqual((yield* store.claim(claimInput)).kind, "claimed");
      yield* PubSub.take(subscription);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "native execution claim refuses a foreign captured coordinator for %s",
  (kind) =>
    Effect.gen(function* () {
      const fixture = yield* nativeExecutionFixture(kind);
      const coordinator = { ...fixture.coordinator, workerId: "foreign-worker" };
      const other = yield* Store.makeCapturedNativeWorkflowRunStore(
        fixture.sql,
        fixture.config,
        fixture.path,
        fixture.identity,
        fixture.policy,
        coordinator,
      );
      const before = yield* nativeBindingState;
      assert.strictEqual(
        (yield* other.claim({ ...fixture.claimInput, ...coordinator })).kind,
        "denied",
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
    }).pipe(Effect.provide(layer)),
);

it.effect("native execution claim cannot treat an alias of a bound native run as legacy", () =>
  Effect.gen(function* () {
    const { store, sql, pendingOperation, claimInput } = yield* nativeExecutionFixture("workflow");
    if (pendingOperation.parameters.kind !== "stage_launch")
      throw new Error("expected workflow operation");
    const aliasBase = decodeOperationFixture({
      ...pendingOperation,
      id: "alias-operation",
      parameters: {
        ...pendingOperation.parameters,
        context: { ...pendingOperation.parameters.context, attemptId: "alias-attempt" },
      },
    });
    const alias = { ...aliasBase, fingerprint: WorkflowCanonicalHash.operation(aliasBase) };
    yield* sql`INSERT INTO workflow_operations (id,workflow_run_id,kind,fingerprint,status,generation,record_json) VALUES (${alias.id},${run.id},${alias.kind},${alias.fingerprint},'pending',0,${json(alias)})`;
    const before = yield* nativeBindingState;
    assert.strictEqual(
      (yield* Effect.flip(store.claim({ ...claimInput, operationId: alias.id }))).reason,
      "integrity",
    );
    assert.deepStrictEqual(yield* nativeBindingState, before);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", "revoke", "claim"],
  ["workflow", "revoke", "control"],
  ["planning", "revoke", "claim"],
  ["planning", "revoke", "control"],
  ["workflow", "pause", "claim"],
  ["workflow", "pause", "control"],
  ["planning", "pause", "claim"],
  ["planning", "pause", "control"],
  ["workflow", "stop", "claim"],
  ["workflow", "stop", "control"],
  ["planning", "stop", "claim"],
  ["planning", "stop", "control"],
] as const)(
  "native execution claim preserves independent $0/$1/$2-first commit order",
  ([kind, action, first]) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-execution-order-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        yield* Effect.gen(function* () {
          const fixture = yield* nativeExecutionFixture(kind);
          const { store, sql, claimInput } = fixture;
          yield* sql`PRAGMA journal_mode=WAL`;
          const sqlB = Context.get(
            yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
            SqlClient.SqlClient,
          );
          const other = yield* Store.makeCapturedNativeWorkflowRunStore(
            sqlB,
            fixture.config,
            fixture.path,
            fixture.identity,
            fixture.policy,
            fixture.coordinator,
          );
          const sessionsB = yield* AuthSessions.make.pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          );
          const control =
            action === "revoke"
              ? sessionsB
                  .revoke({
                    sessionId: fixture.authority.sessionId,
                    revokedAt: yield* DateTime.now,
                  })
                  .pipe(Effect.asVoid)
              : nativeClaimControl(kind, other, action).pipe(
                  Effect.provideService(SqlClient.SqlClient, sqlB),
                );
          const subscription = yield* store.subscribeChanges;
          if (first === "control") {
            yield* control;
            const before = yield* nativeBindingState;
            assert.notStrictEqual((yield* store.claim(claimInput)).kind, "claimed");
            assert.deepStrictEqual(yield* nativeBindingState, before);
          } else {
            assert.strictEqual((yield* store.claim(claimInput)).kind, "claimed");
            yield* PubSub.take(subscription);
            yield* control;
            const before = yield* nativeBindingState;
            assert.strictEqual((yield* other.claim(claimInput)).kind, "stale");
            assert.deepStrictEqual(yield* nativeBindingState, before);
            assert.strictEqual(
              (yield* sqlB`SELECT * FROM workflow_operations WHERE status='claimed'`).length,
              1,
            );
          }
          assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        }).pipe(
          Effect.provide(
            Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename }))),
          ),
        );
      }),
    ),
);

it.effect.each([
  ["workflow", false],
  ["workflow", true],
  ["planning", false],
  ["planning", true],
] as const)("native execution claim has one $0 writer winner with second=$1", ([kind, second]) =>
  Effect.scoped(
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-execution-winner-")),
        ),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      yield* Effect.gen(function* () {
        const fixture = yield* nativeExecutionFixture(kind);
        const sqlB = Context.get(
          yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
          SqlClient.SqlClient,
        );
        const other = yield* Store.makeCapturedNativeWorkflowRunStore(
          sqlB,
          fixture.config,
          fixture.path,
          fixture.identity,
          fixture.policy,
          fixture.coordinator,
        );
        const first = second ? other : fixture.store;
        const next = second ? fixture.store : other;
        const subscription = yield* first.subscribeChanges;
        const nextSubscription = yield* next.subscribeChanges;
        assert.strictEqual((yield* first.claim(fixture.claimInput)).kind, "claimed");
        yield* PubSub.take(subscription);
        const before = yield* nativeBindingState;
        assert.strictEqual((yield* next.claim(fixture.claimInput)).kind, "stale");
        assert.deepStrictEqual(yield* nativeBindingState, before);
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        assert.strictEqual((yield* PubSub.takeUpTo(nextSubscription, 10)).length, 0);
      }).pipe(
        Effect.provide(Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })))),
      );
    }),
  ),
);

it.effect(
  "native execution rearm cannot misclassify a corrupted finalized planning link as legacy",
  () =>
    Effect.gen(function* () {
      const fixture = yield* nativeExecutionFixture("planning", false, 2);
      yield* nativeClaimControl("planning", fixture.store, "pause");
      if (fixture.pendingOperation.parameters.kind !== "planning_pass")
        throw new Error("expected planning operation");
      const foreignBase = decodeOperationFixture({
        ...fixture.pendingOperation,
        id: "foreign-finalized-operation",
        parameters: { ...fixture.pendingOperation.parameters, passId: "foreign-pass" },
        status: "settled",
        outcome: { kind: "succeeded", summary: "foreign fixture", evidence: [] },
      });
      const foreign = { ...foreignBase, fingerprint: WorkflowCanonicalHash.operation(foreignBase) };
      yield* fixture.sql`INSERT INTO workflow_operations (id,planning_session_id,kind,fingerprint,status,generation,record_json) VALUES (${foreign.id},${session.id},${foreign.kind},${foreign.fingerprint},'settled',0,${json(foreign)})`;
      yield* fixture.sql`UPDATE workflow_native_launch_intents SET operation_id=${foreign.id},finalized_operation_id=${foreign.id}`;
      yield* fixture.sessions.revoke(fixture.authority.sessionId);
      const before = yield* nativeBindingState;
      const subscription = yield* fixture.store.subscribeChanges;
      const outcome = yield* Effect.exit(
        fixture.store.controlPlanning({
          sessionId: session.id,
          action: "resume",
          expectedVersion: 2,
          now: facts.now,
          context,
          command: { commandId: "malformed-native-resume", receiptId: "malformed-native-resume" },
        }),
      );
      assert.isTrue(Exit.isFailure(outcome));
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([
  ["workflow", false],
  ["planning", false],
  ["workflow", true],
  ["planning", true],
] as const)(
  "current native execution claim observes genuine $0 lease with rearm=$1 without mutation",
  ([kind, rearm]) =>
    Effect.gen(function* () {
      const fixture = yield* nativeExecutionFixture(kind, rearm);
      const { store, sql, claimInput, pendingOperation, input } = fixture;
      assert.strictEqual((yield* store.claim(claimInput)).kind, "claimed");
      const selection = {
        operationId: input.operationId,
        expectedFingerprint: pendingOperation.fingerprint,
        expectedGeneration: rearm ? 2 : 0,
        ...fixture.coordinator,
      };
      const before = yield* nativeBindingState;
      const subscription = yield* store.subscribeChanges;
      const observed = yield* sql.withTransaction(
        store.verifyClaimedNativeOperationInTransaction(selection),
      );
      assert.strictEqual(observed.kind, "current");
      if (observed.kind !== "current") throw new Error("expected current claimed observation");
      assert.strictEqual(observed.stage, "preparing");
      assert.isTrue(Object.isFrozen(observed));
      assert.strictEqual(String(observed.operationId), String(input.operationId));
      assert.strictEqual(String(observed.intentId), String(input.intentId));
      assert.strictEqual(observed.operationFingerprint, pendingOperation.fingerprint);
      assert.strictEqual(observed.generation, rearm ? 2 : 0);
      assert.strictEqual(String(observed.threadId), String(input.threadId));
      assert.strictEqual(observed.nativeThreadId, null);
      assert.strictEqual(observed.checkpointScopeId, null);
      assert.deepStrictEqual(
        yield* sql.withTransaction(store.verifyClaimedNativeOperationInTransaction(selection)),
        observed,
      );
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

const claimCarryCodexSettings = Schema.decodeSync(CodexSettings)({});
const currentNativeExecutionFixture = Effect.fn("test.currentNativeExecutionFixture")(function* (
  kind: "workflow" | "planning",
  rearm = false,
) {
  const fixture = yield* nativeExecutionFixture(kind, rearm);
  const claimed = yield* fixture.store.claim(fixture.claimInput);
  assert.strictEqual(claimed.kind, "claimed");
  if (claimed.kind !== "claimed" || !claimed.operation.claim)
    throw new Error("expected claimed native operation");
  return {
    ...fixture,
    claimedOperation: claimed.operation,
    operationSelection: {
      operationId: claimed.operation.id,
      expectedFingerprint: claimed.operation.fingerprint,
      expectedGeneration: claimed.operation.controlGeneration,
      bootId: String(claimed.operation.claim.bootId),
      workerId: String(claimed.operation.claim.workerId),
    },
  };
});
const readCurrentNativeProjection = Effect.fn("test.readCurrentNativeProjection")(function* (
  fixture: Effect.Success<ReturnType<typeof currentNativeExecutionFixture>>,
) {
  const projections = Context.get(
    yield* Layer.build(
      Layer.fresh(
        ProjectionStore.layer.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, fixture.sql))),
      ),
    ),
    ProjectionStore.ProjectionStoreV2,
  );
  return yield* projections.getRuntimeRecoveryProjection(decodeThreadId(fixture.input.threadId));
});
const setCurrentNativeStage = Effect.fn("test.setCurrentNativeStage")(function* (
  fixture: Effect.Success<ReturnType<typeof currentNativeExecutionFixture>>,
  stage: "starting" | "running",
  loaded = false,
) {
  const projection = yield* readCurrentNativeProjection(fixture);
  const selectedRun = projection.runs[0]!;
  const attempt = projection.attempts[0]!;
  const node = projection.nodes[0]!;
  const provider = projection.providerThreads[0]!;
  const now = yield* DateTime.now;
  const scopeId = CheckpointScopeId.make("current-native-root-scope");
  const sink = Context.get(fixture.runtime, EventSink.EventSinkV2);
  const common = {
    threadId: selectedRun.threadId,
    runId: selectedRun.id,
    nodeId: node.id,
    providerInstanceId: provider.providerInstanceId,
    occurredAt: now,
  };
  yield* sink.write({
    events: [
      {
        ...common,
        id: EventId.make("current-scope"),
        type: "checkpoint-scope.created",
        payload: {
          id: scopeId,
          threadId: selectedRun.threadId,
          runId: selectedRun.id,
          nodeId: node.id,
          parentScopeId: null,
          providerThreadId: provider.id,
          kind: "root_run",
          ordinalWithinParent: 0,
          advancesAppRunCount: true,
          cwd: "/work",
          createdAt: now,
        },
      },
      {
        ...common,
        id: EventId.make("current-provider"),
        type: "provider-thread.updated",
        payload: {
          ...provider,
          status: stage === "running" ? "active" : provider.status,
          nativeThreadRef: loaded
            ? { driver: provider.driver, nativeId: "loaded-current-native", strength: "strong" }
            : provider.nativeThreadRef,
          updatedAt: now,
        },
      },
      {
        ...common,
        id: EventId.make("current-run"),
        type: "run.updated",
        payload: { ...selectedRun, status: stage, startedAt: stage === "running" ? now : null },
      },
      {
        ...common,
        id: EventId.make("current-attempt"),
        type: "run-attempt.updated",
        payload: {
          ...attempt,
          ...(loaded ? { nativeThreadId: "loaded-current-native" } : {}),
          status: stage === "running" ? "running" : "pending",
          startedAt: stage === "running" ? now : null,
        },
      },
      {
        ...common,
        id: EventId.make("current-node"),
        type: "node.updated",
        payload: {
          ...node,
          status: stage === "running" ? "running" : "pending",
          startedAt: stage === "running" ? now : null,
          checkpointScopeId: scopeId,
        },
      },
    ],
  });
  return { selectedRun, attempt, node, provider, scopeId };
});

it.effect.each([
  ["workflow", false, "valid"],
  ["workflow", true, "valid"],
  ["planning", false, "valid"],
  ["planning", true, "valid"],
  ["workflow", false, "extra"],
  ["workflow", false, "missing"],
  ["workflow", false, "blank"],
  ["workflow", false, "hash"],
  ["workflow", false, "negative"],
  ["workflow", false, "unsafe"],
  ["workflow", false, "null"],
  ["workflow", false, "undefined"],
  ["workflow", false, "legacy"],
] as const)(
  "controlled frame pins actual post-CAS $0 claim with rearm=$1 and ref=$2",
  ([kind, rearm, variant]) =>
    Effect.gen(function* () {
      const fixture = yield* currentNativeExecutionFixture(kind, rearm);
      yield* setCurrentNativeStage(fixture, "starting", true);
      const projection = yield* readCurrentNativeProjection(fixture);
      const selectedRun = projection.runs[0]!;
      const attempt = projection.attempts[0]!;
      const root = projection.nodes.find((node) => node.id === attempt.rootNodeId)!;
      const provider = projection.providerThreads[0]!;
      if (provider.providerSessionId === null || provider.providerInstanceId === null)
        throw new Error("selected provider identity missing");
      const operation = fixture.claimedOperation;
      if (!operation.claim) throw new Error("actual claim missing");
      const executionClaim = {
        operationId: operation.id,
        expectedFingerprint: operation.fingerprint,
        expectedGeneration: operation.controlGeneration,
        bootId: operation.claim.bootId,
        workerId: operation.claim.workerId,
      };
      const expected = { ...executionClaim };
      const frames = yield* Queue.unbounded<CodexProtocol.CodexAppServerControlledFrame>();
      let writes = 0;
      const spawner = ChildProcessSpawner.make((command) =>
        Effect.sync(() => {
          assert.strictEqual(command._tag, "StandardCommand");
          if (command._tag === "StandardCommand")
            assert.deepEqual(command.options.stdin, { stream: "pipe", endOnDone: false });
          return ChildProcessSpawner.makeHandle({
            pid: ChildProcessSpawner.ProcessId(999999997),
            exitCode: Effect.never,
            isRunning: Effect.succeed(true),
            kill: () => Effect.void,
            unref: Effect.succeed(Effect.void),
            stdin: Sink.forEach((_: Uint8Array) =>
              Effect.sync(() => {
                writes++;
              }),
            ),
            stdout: Stream.never,
            stderr: Stream.empty,
            all: Stream.empty,
            getInputFd: () => Sink.drain,
            getOutputFd: () => Stream.empty,
          });
        }),
      );
      const openControlled: NonNullable<
        CodexAdapter.CodexAppServerClientFactoryShape["openControlled"]
      > = (input) =>
        CodexClient.makeControlledChildProcessClient(ChildProcess.make("claim-carry-fixture"), {
          wrapper: (frame) =>
            Queue.offer(frames, frame).pipe(
              Effect.andThen(
                Effect.fail(
                  new CodexErrors.CodexAppServerFrameSubmissionError({
                    outcome: "not_submitted",
                    reason: "wrapper",
                  }),
                ),
              ),
            ),
          onOutcome: input.onOutcome,
        }).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.mapError(
            (cause) =>
              new ProviderAdapter.ProviderAdapterOpenSessionError({
                driver: provider.driver,
                providerSessionId: input.providerSessionId,
                cause,
              }),
          ),
        );
      const adapter = yield* CodexAdapter.makeCodexAdapterV2({
        instanceId: provider.providerInstanceId,
        settings: claimCarryCodexSettings,
        environment: {},
        digest: yield* Digest.Digest,
        fileSystem: yield* FileSystem.FileSystem,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: fixture.config,
        continuationRequests: { offer: () => Effect.void },
        clientFactory:
          variant === "legacy"
            ? {
                open: (input) => openControlled({ ...input, onOutcome: () => Effect.void }),
              }
            : {
                open: () => Effect.die("claim carry must use controlled client"),
                openControlled,
              },
      });
      const modelSelection = {
        ...fixture.input.launch.modelSelection,
        instanceId: provider.providerInstanceId,
      };
      const runtimePolicy = ProviderAdapter.ProviderAdapterV2RuntimePolicy.make({
        runtimeMode: "full-access",
        interactionMode: "default",
        cwd: "/work",
      });
      const runtime = yield* adapter.openSession({
        threadId: selectedRun.threadId,
        providerSessionId: provider.providerSessionId,
        modelSelection,
        runtimePolicy,
      });
      const turnInput: ProviderAdapter.ProviderAdapterV2TurnInput = {
        appThread: projection.thread,
        threadId: selectedRun.threadId,
        runId: selectedRun.id,
        runOrdinal: selectedRun.ordinal,
        providerTurnOrdinal: 1,
        attemptId: attempt.id,
        rootNodeId: root.id,
        providerThread: provider,
        message: {
          messageId: MessageId.make("claim-carry-message"),
          text: "claim carry",
          attachments: [],
          createdBy: "agent",
          creationSource: "mcp",
        },
        modelSelection,
        runtimePolicy,
        executionClaim,
      };
      const before = yield* nativeBindingState;
      const subscription = yield* fixture.store.subscribeChanges;
      if (variant === "extra") Object.assign(executionClaim, { extra: true });
      if (variant === "missing") Reflect.deleteProperty(executionClaim, "bootId");
      if (variant === "blank") Object.assign(executionClaim, { bootId: " " });
      if (variant === "hash") Object.assign(executionClaim, { expectedFingerprint: "not-a-hash" });
      if (variant === "negative") Object.assign(executionClaim, { expectedGeneration: -1 });
      if (variant === "unsafe")
        Object.assign(executionClaim, { expectedGeneration: Number.MAX_SAFE_INTEGER + 1 });
      if (variant === "null") Object.assign(turnInput, { executionClaim: null });
      if (variant === "undefined") Object.assign(turnInput, { executionClaim: undefined });
      const pending = runtime.startTurn(turnInput);
      if (variant === "valid")
        Object.assign(executionClaim, {
          expectedGeneration: 999,
          bootId: "mutated-boot",
          workerId: "mutated-worker",
        });
      const denied = yield* pending.pipe(Effect.flip);
      assert.strictEqual(denied._tag, "ProviderAdapterTurnStartError");
      if (denied._tag === "ProviderAdapterTurnStartError")
        assert.isUndefined(denied.submissionState);
      if (variant === "valid") {
        const frame = yield* Queue.take(frames);
        const selection = parse(frame.selection!);
        assert.isObject(selection);
        assert.deepStrictEqual((selection as Record<string, unknown>).expectedClaim, expected);
        assert.strictEqual(expected.expectedGeneration, rearm ? 2 : 0);
        assert.notInclude(frame.encoded, "expectedClaim");
        assert.notInclude(frame.encoded, expected.expectedFingerprint);
      } else assert.strictEqual(yield* Queue.size(frames), 0);
      assert.strictEqual(writes, 0);
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          layer,
          IdAllocator.layer,
          McpProviderSessions.layer,
          NodeServices.layer,
          NodeCrypto.layer,
        ),
      ),
    ),
);

it.effect.each([
  ["workflow", "starting"],
  ["workflow", "running"],
  ["planning", "starting"],
  ["planning", "running"],
] as const)("current native execution claim observes source-backed $0/$1 tuple", ([kind, stage]) =>
  Effect.gen(function* () {
    const fixture = yield* currentNativeExecutionFixture(kind);
    const selected = yield* setCurrentNativeStage(fixture, stage);
    const before = yield* nativeBindingState;
    const subscription = yield* fixture.store.subscribeChanges;
    const result = yield* fixture.sql.withTransaction(
      fixture.store.verifyClaimedNativeOperationInTransaction(fixture.operationSelection),
    );
    assert.strictEqual(result.kind, "current");
    if (result.kind !== "current") throw new Error("expected current native tuple");
    assert.strictEqual(result.stage, stage);
    assert.strictEqual(result.runId, selected.selectedRun.id);
    assert.strictEqual(result.attemptId, selected.attempt.id);
    assert.strictEqual(result.rootNodeId, selected.node.id);
    assert.strictEqual(result.providerThreadId, selected.provider.id);
    assert.strictEqual(result.providerInstanceId, selected.provider.providerInstanceId);
    assert.strictEqual(result.providerSessionId, selected.provider.providerSessionId);
    assert.strictEqual(result.driver, selected.provider.driver);
    assert.strictEqual(result.checkpointScopeId, selected.scopeId);
    assert.deepStrictEqual(
      yield* fixture.sql`SELECT * FROM orchestration_v2_projection_provider_sessions`,
      [],
    );
    assert.deepStrictEqual(yield* nativeBindingState, before);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "current native execution claim denies real interrupted %s run with active owner",
  (kind) =>
    Effect.gen(function* () {
      const fixture = yield* currentNativeExecutionFixture(kind);
      const projection = yield* readCurrentNativeProjection(fixture);
      yield* Context.get(fixture.runtime, Orchestrator.OrchestratorV2).dispatch(
        decodeBindingCommand({
          type: "run.interrupt",
          commandId: "current-native-interrupt",
          threadId: fixture.input.threadId,
          runId: projection.runs[0]!.id,
        }),
      );
      const owners =
        kind === "workflow"
          ? yield* fixture.sql<{ status: string }>`SELECT status FROM workflow_runs`
          : yield* fixture.sql<{ status: string }>`SELECT status FROM workflow_planning_sessions`;
      assert.strictEqual(owners[0]!.status, "active");
      const before = yield* nativeBindingState;
      const result = yield* fixture.sql.withTransaction(
        fixture.store.verifyClaimedNativeOperationInTransaction(fixture.operationSelection),
      );
      assert.notStrictEqual(result.kind, "current");
      assert.deepStrictEqual(yield* nativeBindingState, before);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(
  (["workflow", "planning"] as const).flatMap((kind) =>
    [
      "plain",
      "outside",
      "labels",
      "fingerprint",
      "generation",
      "missing",
      "pending",
      "held",
      "cancel",
      "pause",
      "stop",
      "scope",
      "expiry",
      "revoke",
      "deadline",
      "receipt",
      "proof",
      "intent-labels",
      "native-terminal",
      "mixed",
      "turn",
      "root-identity",
      "provider-identity",
      "provider-row",
      "provider-missing",
      "provider-driver",
      "provider-native-driver",
      "provider-owner",
      "run-row",
      "provider-ordinal",
      "provider-archived",
      "provider-error",
      "owner-generation",
      "claim-label",
      "claim-generation",
      "provider-closed",
      "scope-missing",
      "scope-identity",
      "scope-row",
      "scope-kind",
    ].map((change) => ({ kind, change })),
  ),
)("current native execution claim refuses $kind/$change without mutation", ({ kind, change }) =>
  Effect.gen(function* () {
    const fixture = yield* currentNativeExecutionFixture(kind);
    const { sql, store } = fixture;
    let selected = store;
    const request = {
      ...fixture.operationSelection,
      operationId: String(fixture.operationSelection.operationId),
    };
    if (change.startsWith("scope-") || change === "mixed" || change === "turn")
      yield* setCurrentNativeStage(fixture, "running");
    if (change === "plain") selected = yield* Store.WorkflowRunStore;
    else if (change === "labels") request.workerId = "foreign";
    else if (change === "fingerprint") request.expectedFingerprint = nextHash;
    else if (change === "generation") request.expectedGeneration = 2;
    else if (change === "missing") request.operationId = "absent";
    else if (change === "pending")
      yield* sql`UPDATE workflow_operations SET status='pending',record_json=json_remove(json_set(record_json,'$.status','pending'),'$.claim')`;
    else if (change === "held")
      yield* sql`UPDATE workflow_operations SET held=1,status='pending',record_json=json_remove(json_set(record_json,'$.status','pending'),'$.claim')`;
    else if (change === "cancel") yield* sql`UPDATE workflow_operations SET cancel_requested=1`;
    else if (change === "pause" || change === "stop")
      yield* nativeClaimControl(kind, store, change);
    else if (change === "scope")
      yield* sql`UPDATE auth_sessions SET scopes='["orchestration:read"]'`;
    else if (change === "expiry") yield* sql`UPDATE auth_sessions SET expires_at=${facts.now}`;
    else if (change === "revoke") yield* fixture.sessions.revoke(fixture.authority.sessionId);
    else if (change === "deadline")
      yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-01T00:00:00Z").epochMilliseconds);
    else if (change === "receipt")
      yield* sql`UPDATE workflow_command_receipts SET result_json='{}' WHERE operation_id=${request.operationId}`;
    else if (change === "proof") {
      yield* sql`DROP TRIGGER workflow_native_command_proofs_update_guard`;
      yield* sql`UPDATE workflow_native_command_proofs SET proof_hash=${nextHash} WHERE phase='message'`;
    } else if (change === "intent-labels")
      yield* sql`UPDATE workflow_native_launch_intents SET worker_id='foreign'`;
    else if (change === "native-terminal")
      yield* sql`UPDATE orchestration_v2_projection_runs SET status='interrupted',payload_json=json_set(payload_json,'$.status','interrupted')`;
    else if (change === "mixed")
      yield* sql`UPDATE orchestration_v2_projection_run_attempts SET status='pending',payload_json=json_set(payload_json,'$.status','pending','$.startedAt',NULL)`;
    else if (change === "turn")
      yield* sql`UPDATE orchestration_v2_projection_run_attempts SET provider_turn_id='already-sent',payload_json=json_set(payload_json,'$.providerTurnId','already-sent')`;
    else if (change === "root-identity")
      yield* sql`UPDATE orchestration_v2_projection_nodes SET payload_json=json_set(payload_json,'$.rootNodeId','foreign-root')`;
    else if (change === "provider-identity")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET thread_id='foreign',payload_json=json_set(payload_json,'$.appThreadId','foreign')`;
    else if (change === "provider-row")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET provider_session_id='foreign'`;
    else if (change === "provider-native-driver")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET driver='claude',payload_json=json_set(payload_json,'$.driver','claude','$.nativeThreadRef',json('{"driver":"claude","nativeId":"substituted-native","strength":"strong"}'))`;
    else if (change === "provider-owner")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET owner_node_id='foreign',payload_json=json_set(payload_json,'$.ownerNodeId','foreign')`;
    else if (change === "run-row")
      yield* sql`UPDATE orchestration_v2_projection_runs SET ordinal=2`;
    else if (change === "provider-ordinal")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET last_run_ordinal=2`;
    else if (change === "provider-archived" || change === "provider-error") {
      const status = change === "provider-archived" ? "archived" : "error";
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET status=${status},payload_json=json_set(payload_json,'$.status',${status})`;
    } else if (change === "owner-generation") {
      if (kind === "workflow")
        yield* sql`UPDATE workflow_runs SET control_generation=2,record_json=json_set(record_json,'$.controlGeneration',2)`;
      else
        yield* sql`UPDATE workflow_planning_sessions SET control_generation=2,record_json=json_set(record_json,'$.controlGeneration',2)`;
    } else if (change === "claim-label")
      yield* sql`UPDATE workflow_operations SET record_json=json_set(record_json,'$.claim.workerId','foreign')`;
    else if (change === "claim-generation")
      yield* sql`UPDATE workflow_operations SET record_json=json_set(record_json,'$.claim.generation',2)`;
    else if (change === "provider-driver")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET driver='claude',payload_json=json_set(payload_json,'$.driver','claude')`;
    else if (change === "provider-closed")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET status='closed',payload_json=json_set(payload_json,'$.status','closed')`;
    else if (change === "provider-missing")
      yield* sql`DELETE FROM orchestration_v2_projection_provider_threads`;
    else if (change === "scope-missing")
      yield* sql`DELETE FROM orchestration_v2_projection_checkpoint_scopes`;
    else if (change === "scope-identity")
      yield* sql`UPDATE orchestration_v2_projection_checkpoint_scopes SET node_id='foreign',payload_json=json_set(payload_json,'$.nodeId','foreign')`;
    else if (change === "scope-row")
      yield* sql`UPDATE orchestration_v2_projection_checkpoint_scopes SET run_id='foreign'`;
    else if (change === "scope-kind")
      yield* sql`UPDATE orchestration_v2_projection_checkpoint_scopes SET kind='manual',payload_json=json_set(payload_json,'$.kind','manual')`;
    const before = yield* nativeBindingState;
    const subscription = yield* store.subscribeChanges;
    const read = selected.verifyClaimedNativeOperationInTransaction(request);
    const exit = yield* Effect.exit(change === "outside" ? read : sql.withTransaction(read));
    assert.isTrue(Exit.isFailure(exit) || exit.value.kind !== "current");
    assert.deepStrictEqual(yield* nativeBindingState, before);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
  }).pipe(Effect.provide(layer)),
);

it.effect("current native execution claim owns flat selectors before delayed evaluation", () =>
  Effect.gen(function* () {
    const fixture = yield* currentNativeExecutionFixture("workflow");
    const mutable = { ...fixture.operationSelection };
    const read = fixture.store.verifyClaimedNativeOperationInTransaction(mutable);
    mutable.expectedGeneration = 99;
    mutable.workerId = "mutated";
    mutable.expectedFingerprint = nextHash;
    assert.strictEqual((yield* fixture.sql.withTransaction(read)).kind, "current");
    const invalid = [
      { ...fixture.operationSelection, extra: true },
      { ...fixture.operationSelection, expectedGeneration: -1 },
      { ...fixture.operationSelection, expectedGeneration: 0.5 },
      { ...fixture.operationSelection, expectedFingerprint: "bad" },
      { ...fixture.operationSelection, workerId: " " },
      Object.defineProperty({ ...fixture.operationSelection }, "workerId", {
        get() {
          throw new Error("must not read selector accessor");
        },
        enumerable: true,
      }),
    ];
    for (const input of invalid)
      assert.isTrue(
        Exit.isFailure(
          yield* Effect.exit(
            fixture.sql.withTransaction(
              fixture.store.verifyClaimedNativeOperationInTransaction(input),
            ),
          ),
        ),
      );
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "current native execution claim reads writer-local %s revocation and independent commit order",
  (kind) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-current-order-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        yield* Effect.gen(function* () {
          const fixture = yield* currentNativeExecutionFixture(kind);
          const { sql, store, operationSelection } = fixture;
          yield* sql`PRAGMA journal_mode=WAL`;
          const sqlB = Context.get(
            yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
            SqlClient.SqlClient,
          );
          const other = yield* Store.makeCapturedNativeWorkflowRunStore(
            sqlB,
            fixture.config,
            fixture.path,
            fixture.identity,
            fixture.policy,
            fixture.coordinator,
          );
          const sqlReader = Context.get(
            yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename, readonly: true }))),
            SqlClient.SqlClient,
          );
          const reader = yield* Store.makeCapturedNativeWorkflowRunStore(
            sqlReader,
            fixture.config,
            fixture.path,
            fixture.identity,
            fixture.policy,
            fixture.coordinator,
          );
          const sessionsB = yield* AuthSessions.make.pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          );
          const before = yield* nativeBindingState;
          const subscription = yield* store.subscribeChanges;
          assert.isTrue(
            Exit.isFailure(
              yield* Effect.exit(
                sqlB.withTransaction(
                  store.verifyClaimedNativeOperationInTransaction(operationSelection),
                ),
              ),
            ),
          );
          const local = yield* Effect.exit(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* sql`UPDATE auth_sessions SET scopes='["orchestration:read"]' WHERE session_id=${fixture.authority.sessionId}`;
                assert.notStrictEqual(
                  (yield* store.verifyClaimedNativeOperationInTransaction(operationSelection)).kind,
                  "current",
                );
                assert.strictEqual(
                  (yield* sqlReader.withTransaction(
                    reader.verifyClaimedNativeOperationInTransaction(operationSelection),
                  )).kind,
                  "current",
                );
                return yield* Effect.fail("rollback-local-authority-fixture");
              }),
            ),
          );
          assert.isTrue(Exit.isFailure(local));
          assert.deepStrictEqual(local, Exit.fail("rollback-local-authority-fixture"));
          assert.deepStrictEqual(yield* nativeBindingState, before);
          assert.strictEqual(
            (yield* sql.withTransaction(
              store.verifyClaimedNativeOperationInTransaction(operationSelection),
            )).kind,
            "current",
          );
          yield* sessionsB.revoke({
            sessionId: fixture.authority.sessionId,
            revokedAt: yield* DateTime.now,
          });
          const revoked = yield* nativeBindingState;
          assert.notStrictEqual(
            (yield* sql.withTransaction(
              store.verifyClaimedNativeOperationInTransaction(operationSelection),
            )).kind,
            "current",
          );
          assert.notStrictEqual(
            (yield* sqlB.withTransaction(
              other.verifyClaimedNativeOperationInTransaction(operationSelection),
            )).kind,
            "current",
          );
          assert.deepStrictEqual(yield* nativeBindingState, revoked);
          assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        }).pipe(
          Effect.provide(
            Store.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename }))),
          ),
        );
      }),
    ),
);

it.effect(
  "current native execution claim maps direct SQL failure to persistence without writes",
  () =>
    Effect.gen(function* () {
      const fixture = yield* currentNativeExecutionFixture("workflow");
      const before = yield* nativeBindingState;
      const subscription = yield* fixture.store.subscribeChanges;
      yield* fixture.sql`ALTER TABLE workflow_operations RENAME TO current_fixture_hidden_operations`;
      const result = yield* Effect.result(
        fixture.sql.withTransaction(
          fixture.store.verifyClaimedNativeOperationInTransaction(fixture.operationSelection),
        ),
      );
      yield* fixture.sql`ALTER TABLE current_fixture_hidden_operations RENAME TO workflow_operations`;
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "persistence");
      assert.deepStrictEqual(yield* nativeBindingState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["workflow", "planning"] as const)(
  "current native execution claim returns newly loaded %s native target",
  (kind) =>
    Effect.gen(function* () {
      const fixture = yield* currentNativeExecutionFixture(kind);
      yield* setCurrentNativeStage(fixture, "running", true);
      const result = yield* fixture.sql.withTransaction(
        fixture.store.verifyClaimedNativeOperationInTransaction(fixture.operationSelection),
      );
      assert.strictEqual(result.kind, "current");
      if (result.kind !== "current") throw new Error("expected loaded native observation");
      assert.strictEqual(result.nativeThreadId, "loaded-current-native");
    }).pipe(Effect.provide(layer)),
);

const physicalTransport = (
  openingThreadId: string,
  transportId = "11111111-2222-3333-4444-555555555555",
  providerSessionId = "physical-session",
  providerInstanceId = "provider",
) =>
  encodeSubmissionTransportData({
    context: {
      v: 1,
      environmentId: "environment",
      projectId: "opening-project",
      openingThreadId,
      driver: "codex",
      providerInstanceId,
      providerSessionId,
      scopeNamespace: "app-t3code-agents-123456abcdef.slice",
      hostBootId: "aaaaaaaa-1111-2222-3333-444444444444",
      expectedClaim: null,
    },
    transportId,
    proposedUnit: `t3code-codex-${transportId}.scope`,
    creationInputHash: hash,
    createdAt: facts.now,
  });
const physicalAdmissionKinds = [
  "workflow-native",
  "workflow-legacy",
  "planning-native",
  "planning-legacy",
] as const;
const preparePhysicalAdmission = Effect.fn("test.preparePhysicalAdmission")(function* (
  kind: (typeof physicalAdmissionKinds)[number],
) {
  const store = yield* Store.WorkflowRunStore;
  if (kind === "workflow-native" || kind === "workflow-legacy") {
    yield* setupAdmission;
    const operation = operationFor("physical-op", "physical-attempt");
    const invokeOn = (next: Store.WorkflowRunStore["Service"]) =>
      kind === "workflow-native"
        ? Effect.result(
            next.reserveWorkflowNativeIntent(reservation(operation, "physical-attempt")),
          )
        : Effect.result(
            next.commitTransition({
              runId: run.id,
              command: admission(operation, "physical-attempt"),
              externalFacts: admissionFacts,
              operation,
              receiptId: "physical-receipt",
            }),
          );
    return { threadId: "thread", invoke: invokeOn(store), invokeOn };
  }
  const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
  yield* begin(store, session);
  const input = admissionInput(binding, "physical");
  const invokeOn = (next: Store.WorkflowRunStore["Service"]) =>
    kind === "planning-native"
      ? Effect.result(next.reservePlanningNativeIntent(planningReservation(input)))
      : Effect.result(next.admitPlanningPass(input));
  return { threadId: input.pass.native!.threadId, invoke: invokeOn(store), invokeOn };
});

it.effect.each(physicalAdmissionKinds)(
  "physical submission custody denies fresh %s before charge or publication",
  (kind) =>
    Effect.gen(function* () {
      const prepared = yield* preparePhysicalAdmission(kind);
      const store = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      const row = physicalTransport(prepared.threadId);
      yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
      const subscription = yield* store.subscribeChanges;
      const before = yield* rawReservationState;
      const result = yield* prepared.invoke;
      if (kind === "workflow-native" || kind === "workflow-legacy") {
        assert.isTrue(result._tag === "Success");
        if (result._tag === "Success")
          assert.deepStrictEqual(result.success, { kind: "rejected", reason: "custody" });
      } else {
        assert.isTrue(result._tag === "Failure");
        if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "custody");
      }
      assert.deepStrictEqual(yield* rawReservationState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_v2_events`, []);
    }).pipe(Effect.provide(layerWithOutbox)),
);

const bindPhysicalTransport = Effect.fn("test.bindPhysicalTransport")(function* (
  row: ReturnType<typeof physicalTransport>,
) {
  const sql = yield* SqlClient.SqlClient;
  const binding = {
    unit: row.proposed_unit,
    invocationId: "b".repeat(32),
    controlGroup: `/${row.scope_namespace}/${row.proposed_unit}`,
    pid: 1234,
    pidStartTicks: "900719925474099312345",
  };
  const encoded = encodeScopeBindingData(binding, row);
  yield* sql`UPDATE orchestration_v2_submission_transports SET binding_json=${encoded.json},binding_hash=${encoded.hash},bound_at=${facts.now} WHERE transport_id=${row.transport_id}`;
  return binding;
});
const settlePhysicalTransport = Effect.fn("test.settlePhysicalTransport")(function* (
  row: ReturnType<typeof physicalTransport>,
  binding: Effect.Success<ReturnType<typeof bindPhysicalTransport>>,
  wrongHash = false,
) {
  const sql = yield* SqlClient.SqlClient;
  const encoded = encodeScopeSettlementData({ binding, observation: "removed" }, binding);
  yield* sql`UPDATE orchestration_v2_submission_transports SET settlement_json=${encoded.json},settlement_hash=${wrongHash ? "d".repeat(64) : encoded.hash},settled_at=${facts.now} WHERE transport_id=${row.transport_id}`;
});
const physicalAttempt = (
  parent: ReturnType<typeof physicalTransport>,
  threadId: string,
  providerThreadId = "physical-provider-thread",
  nativeThreadId = "physical-native",
  attemptId = "physical-t3-attempt",
) => {
  const params = { threadId: nativeThreadId, input: [{ type: "text", text: "Work" }] };
  const encoded = `${JSON.stringify({ id: 1, method: "turn/start", params })}\n`;
  return snapshotSubmissionFrame(
    {
      transportId: parent.transport_id,
      environmentId: parent.environment_id,
      projectId: "later-project",
      driver: "codex",
      providerInstanceId: parent.provider_instance_id,
      providerSessionId: parent.provider_session_id,
    },
    {
      encoded,
      byteLength: new TextEncoder().encode(encoded).byteLength,
      method: "turn/start",
      requestId: 1,
      selection: JSON.stringify({
        v: 1,
        kind: "start",
        threadId,
        runId: "physical-t3-run",
        runOrdinal: 1,
        attemptId,
        rootNodeId: "physical-t3-root",
        providerTurnOrdinal: 1,
        providerInstanceId: parent.provider_instance_id,
        providerSessionId: parent.provider_session_id,
        providerThreadId,
        nativeThreadId,
        invocation: 1,
      }),
    },
    facts.now,
  );
};
const physicalProvider = (
  id: string,
  appThreadId: string | null,
  providerSessionId: string | null,
  nativeId: string | null,
  instance = "provider",
  ownerNodeId: string | null = null,
) => ({
  id,
  driver: "codex",
  providerInstanceId: instance,
  providerSessionId,
  appThreadId,
  ownerNodeId,
  nativeThreadRef: nativeId === null ? null : { driver: "codex", nativeId, strength: "strong" },
  nativeConversationHeadRef: null,
  status: "idle",
  firstRunOrdinal: null,
  lastRunOrdinal: null,
  handoffIds: [],
  forkedFrom: null,
  pendingBackgroundTasks: [],
  createdAt: facts.now,
  updatedAt: facts.now,
});
const seedPhysicalProvider = Effect.fn("test.seedPhysicalProvider")(function* (
  value: ReturnType<typeof physicalProvider>,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_v2_projection_provider_threads ${sql.insert({ provider_thread_id: value.id, thread_id: value.appThreadId, owner_node_id: value.ownerNodeId, provider: value.providerInstanceId, driver: value.driver, provider_instance_id: value.providerInstanceId, provider_session_id: value.providerSessionId, status: value.status, first_run_ordinal: value.firstRunOrdinal, last_run_ordinal: value.lastRunOrdinal, updated_at: value.updatedAt, payload_json: json(value) })}`;
});
const seedPhysicalSession = Effect.fn("test.seedPhysicalSession")(function* (
  id: string,
  threadId: string | null = null,
  instance = "provider",
) {
  const sql = yield* SqlClient.SqlClient;
  const value = {
    id,
    driver: "codex",
    providerInstanceId: instance,
    status: "ready",
    cwd: "/workspace",
    model: "model",
    capabilities: CodexProviderCapabilitiesV2,
    createdAt: facts.now,
    updatedAt: facts.now,
    lastError: null,
  };
  yield* sql`INSERT INTO orchestration_v2_projection_provider_sessions ${sql.insert({ provider_session_id: id, thread_id: threadId, provider: instance, driver: "codex", provider_instance_id: instance, status: value.status, updated_at: value.updatedAt, payload_json: json(value) })}`;
});
const seedPhysicalNode = Effect.fn("test.seedPhysicalNode")(function* (
  id: string,
  threadId: string,
  providerThreadId: string | null = null,
) {
  const sql = yield* SqlClient.SqlClient;
  const value = {
    id,
    threadId,
    runId: null,
    parentNodeId: null,
    rootNodeId: id,
    kind: "root_turn",
    status: "completed",
    countsForRun: true,
    providerThreadId,
    providerTurnId: null,
    nativeItemRef: null,
    runtimeRequestId: null,
    checkpointScopeId: null,
    startedAt: null,
    completedAt: null,
  };
  yield* sql`INSERT INTO orchestration_v2_projection_nodes ${sql.insert({ node_id: id, thread_id: threadId, run_id: null, parent_node_id: null, root_node_id: id, kind: value.kind, status: value.status, provider_thread_id: providerThreadId, provider_turn_id: null, runtime_request_id: null, checkpoint_scope_id: null, started_at: null, completed_at: null, payload_json: json(value) })}`;
});
const expectPhysicalRejection = (result: unknown) =>
  assert.deepStrictEqual(result, { kind: "rejected", reason: "custody" });

it.effect.each([
  "shared-session",
  "provider-thread",
  "native-alias",
  "owner-node",
  "subagent",
  "fixed-point",
] as const)("physical custody follows %s exposure without a thread-only exemption", (edge) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const row = physicalTransport("other-opening-thread");
    yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
    if (edge === "shared-session") {
      yield* seedPhysicalSession("physical-session");
      yield* sql`INSERT INTO orchestration_v2_projection_provider_session_bindings VALUES ('physical-session','thread')`;
    } else if (edge === "provider-thread" || edge === "native-alias") {
      yield* bindPhysicalTransport(row);
      const child = physicalAttempt(
        row,
        "other-attempt-thread",
        edge === "provider-thread" ? "selected-provider" : "other-provider",
        "same-native",
      );
      yield* sql`INSERT INTO orchestration_v2_submission_attempts ${sql.insert(child)}`;
      yield* seedPhysicalProvider(
        physicalProvider(
          "selected-provider",
          "thread",
          null,
          edge === "native-alias" ? "same-native" : "other-native",
        ),
      );
    } else if (edge === "owner-node") {
      yield* seedPhysicalSession("physical-session");
      yield* seedPhysicalNode("physical-owner", "thread");
      yield* seedPhysicalProvider(
        physicalProvider(
          "selected-provider",
          null,
          "physical-session",
          "same-native",
          "provider",
          "physical-owner",
        ),
      );
    } else if (edge === "subagent") {
      const value = {
        id: "physical-subagent",
        threadId: row.opening_thread_id,
        runId: null,
        parentNodeId: "physical-parent",
        origin: "provider_native",
        createdBy: "agent",
        driver: "codex",
        providerInstanceId: "provider",
        providerThreadId: null,
        childThreadId: "thread",
        nativeTaskRef: null,
        prompt: "Work",
        title: null,
        model: null,
        status: "completed",
        result: null,
        startedAt: null,
        completedAt: null,
        updatedAt: facts.now,
      };
      yield* seedPhysicalNode("physical-parent", row.opening_thread_id);
      yield* sql`INSERT INTO orchestration_v2_projection_subagents ${sql.insert({ subagent_id: value.id, thread_id: value.threadId, run_id: value.runId, parent_node_id: value.parentNodeId, provider: "provider", driver: "codex", provider_instance_id: "provider", provider_thread_id: null, child_thread_id: "thread", origin: value.origin, status: value.status, started_at: null, completed_at: null, updated_at: value.updatedAt, payload_json: json(value) })}`;
    } else {
      yield* seedPhysicalSession("initial-session");
      yield* seedPhysicalSession("physical-session");
      yield* seedPhysicalProvider(
        physicalProvider("first-provider", "thread", "initial-session", "first-native"),
      );
      yield* seedPhysicalProvider(
        physicalProvider("second-provider", null, "initial-session", "bridge-native"),
      );
      yield* seedPhysicalProvider(
        physicalProvider(
          "alias-provider",
          "other-alias-thread",
          "physical-session",
          "bridge-native",
        ),
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_v2_submission_attempts`, []);
    }
    const subscription = yield* store.subscribeChanges;
    const before = yield* rawReservationState;
    expectPhysicalRejection(
      yield* store.reserveWorkflowNativeIntent(
        reservation(operationFor("edge-op", "edge-attempt"), "edge-attempt"),
      ),
    );
    assert.deepStrictEqual(yield* rawReservationState, before);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each(["not_submitted", "unknown", "submitted"] as const)(
  "physical custody retains a bound parent after child %s",
  (outcome) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const row = physicalTransport("thread");
      yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
      yield* bindPhysicalTransport(row);
      yield* sql`INSERT INTO orchestration_v2_submission_attempts ${sql.insert(physicalAttempt(row, "thread"))}`;
      yield* sql`UPDATE orchestration_v2_submission_attempts SET local_outcome=${outcome},local_reason=${outcome === "submitted" ? "completed" : "wrapper"},local_observed_at=${facts.now}`;
      const before = yield* rawReservationState;
      expectPhysicalRejection(
        yield* store.reserveWorkflowNativeIntent(
          reservation(operationFor("outcome-op", "outcome-attempt"), "outcome-attempt"),
        ),
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each(["valid", "settlement-hash", "selection-hash"] as const)(
  "physical custody validates %s historical release before fresh admission",
  (variant) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const row = physicalTransport("thread");
      yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
      const bound = yield* bindPhysicalTransport(row);
      const child = physicalAttempt(row, "thread");
      yield* sql`INSERT INTO orchestration_v2_submission_attempts ${sql.insert({ ...child, selection_hash: variant === "selection-hash" ? "d".repeat(64) : child.selection_hash })}`;
      yield* sql`UPDATE orchestration_v2_submission_attempts SET local_outcome='unknown',local_reason='sink',local_observed_at=${facts.now}`;
      yield* settlePhysicalTransport(row, bound, variant === "settlement-hash");
      const before = yield* rawReservationState;
      const input = reservation(operationFor("settled-op", "settled-attempt"), "settled-attempt");
      if (variant === "valid")
        assert.strictEqual((yield* store.reserveWorkflowNativeIntent(input)).kind, "reserved");
      else {
        assert.strictEqual(
          (yield* Effect.flip(store.reserveWorkflowNativeIntent(input))).reason,
          "integrity",
        );
        assert.deepStrictEqual(yield* rawReservationState, before);
      }
      assert.strictEqual(
        (yield* sql<{
          local_outcome: string;
        }>`SELECT local_outcome FROM orchestration_v2_submission_attempts`)[0]?.local_outcome,
        "unknown",
      );
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each(physicalAdmissionKinds)(
  "physical custody preserves immutable %s replay without recharging",
  (kind) =>
    Effect.gen(function* () {
      const prepared = yield* preparePhysicalAdmission(kind);
      const store = yield* Store.WorkflowRunStore;
      const sql = yield* SqlClient.SqlClient;
      const original = yield* prepared.invoke;
      if (original._tag !== "Success" || !("receiptId" in original.success))
        throw new Error("expected initial admission");
      yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(physicalTransport(prepared.threadId))}`;
      const subscription = yield* store.subscribeChanges;
      const before = yield* rawReservationState;
      const replay = yield* prepared.invoke;
      assert.strictEqual(replay._tag, "Success");
      if (replay._tag === "Success" && "receiptId" in replay.success) {
        assert.strictEqual(replay.success.kind, "replay");
        assert.strictEqual(replay.success.receiptId, original.success.receiptId);
      } else throw new Error("expected immutable replay");
      assert.deepStrictEqual(yield* rawReservationState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect("physical custody does not equate native IDs across provider instances", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const row = physicalTransport(
      "foreign-opening",
      "11111111-2222-3333-4444-555555555555",
      "foreign-session",
      "foreign-instance",
    );
    yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
    yield* bindPhysicalTransport(row);
    yield* sql`INSERT INTO orchestration_v2_submission_attempts ${sql.insert(physicalAttempt(row, "foreign-thread", "foreign-provider", "same-native"))}`;
    yield* seedPhysicalProvider(
      physicalProvider("selected-provider", "thread", null, "same-native"),
    );
    yield* seedPhysicalProvider(
      physicalProvider(
        "foreign-provider",
        "foreign-thread",
        null,
        "same-native",
        "foreign-instance",
      ),
    );
    assert.strictEqual(
      (yield* store.reserveWorkflowNativeIntent(
        reservation(operationFor("isolation-op", "isolation-attempt"), "isolation-attempt"),
      )).kind,
      "reserved",
    );
    assert.strictEqual(
      (yield* sql`SELECT transport_id FROM orchestration_v2_submission_transports WHERE settlement_json IS NULL`)
        .length,
      1,
    );
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each([
  "allocated",
  "null-native-id",
  "unrelated-malformed",
  "known-malformed",
  "wrong-shadow",
  "wrong-session-shadow",
  "wrong-ref-driver",
] as const)("physical custody handles %s projection evidence without a global scan", (variant) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const row = physicalTransport("unrelated-opening");
    yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
    const bound = yield* bindPhysicalTransport(row);
    yield* settlePhysicalTransport(row, bound);
    const value = physicalProvider(
      "selected-provider",
      "thread",
      variant === "allocated"
        ? "allocated-session"
        : variant === "wrong-session-shadow"
          ? "selected-session"
          : null,
      variant === "allocated" || variant === "null-native-id" ? null : "selected-native",
    );
    if (variant === "allocated") value.status = "not_loaded";
    yield* seedPhysicalProvider(value);
    if (variant === "null-native-id")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json=${json({ ...value, nativeThreadRef: { driver: "codex", nativeId: null, strength: "none" } })} WHERE provider_thread_id='selected-provider'`;
    if (variant === "unrelated-malformed") {
      yield* seedPhysicalProvider(
        physicalProvider("unrelated-provider", "unrelated-thread", null, null),
      );
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json='{malformed' WHERE provider_thread_id='unrelated-provider'`;
    }
    if (variant === "known-malformed")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json='{malformed' WHERE provider_thread_id='selected-provider'`;
    if (variant === "wrong-shadow")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET provider_instance_id='wrong-instance' WHERE provider_thread_id='selected-provider'`;
    if (variant === "wrong-session-shadow") {
      yield* seedPhysicalSession("selected-session");
      yield* sql`UPDATE orchestration_v2_projection_provider_sessions SET driver='claude' WHERE provider_session_id='selected-session'`;
    }
    if (variant === "wrong-ref-driver")
      yield* sql`UPDATE orchestration_v2_projection_provider_threads SET payload_json=${json({ ...value, nativeThreadRef: { driver: "claude", nativeId: "selected-native", strength: "strong" } })} WHERE provider_thread_id='selected-provider'`;
    const input = reservation(operationFor("evidence-op", "evidence-attempt"), "evidence-attempt");
    const before = yield* rawReservationState;
    if (
      variant === "known-malformed" ||
      variant === "wrong-shadow" ||
      variant === "wrong-session-shadow" ||
      variant === "wrong-ref-driver"
    ) {
      assert.strictEqual(
        (yield* Effect.flip(store.reserveWorkflowNativeIntent(input))).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
    } else assert.strictEqual((yield* store.reserveWorkflowNativeIntent(input)).kind, "reserved");
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect("physical custody continues transport history beyond the first128-row page", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    for (let index = 1; index <= 129; index++) {
      const id = `${index.toString(16).padStart(8, "0")}-2222-3333-4444-555555555555`;
      const row = physicalTransport("thread", id);
      yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
      if (index < 129) yield* settlePhysicalTransport(row, yield* bindPhysicalTransport(row));
    }
    const before = yield* rawReservationState;
    expectPhysicalRejection(
      yield* store.reserveWorkflowNativeIntent(
        reservation(operationFor("paged-op", "paged-attempt"), "paged-attempt"),
      ),
    );
    assert.deepStrictEqual(yield* rawReservationState, before);
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect("physical custody validates released children beyond the first128-row page", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const row = physicalTransport("thread");
    yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
    const bound = yield* bindPhysicalTransport(row);
    for (let index = 0; index < 129; index++) {
      const child = physicalAttempt(
        row,
        "thread",
        "old-provider",
        "old-native",
        `attempt-${String(index).padStart(3, "0")}`,
      );
      yield* sql`INSERT INTO orchestration_v2_submission_attempts ${sql.insert({ ...child, selection_hash: index === 128 ? "d".repeat(64) : child.selection_hash })}`;
    }
    yield* settlePhysicalTransport(row, bound);
    const before = yield* rawReservationState;
    assert.strictEqual(
      (yield* Effect.flip(
        store.reserveWorkflowNativeIntent(
          reservation(operationFor("paged-op", "paged-attempt"), "paged-attempt"),
        ),
      )).reason,
      "integrity",
    );
    assert.deepStrictEqual(yield* rawReservationState, before);
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect("physical custody continues native alias candidates beyond the first128-row page", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const row = physicalTransport("other-opening");
    yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
    yield* seedPhysicalSession("physical-session");
    yield* seedPhysicalProvider(physicalProvider("root-provider", "thread", null, "shared-native"));
    for (let index = 0; index < 128; index++)
      yield* seedPhysicalProvider(
        physicalProvider(
          `aa-provider-${String(index).padStart(3, "0")}`,
          `foreign-thread-${index}`,
          null,
          "shared-native",
          "foreign-instance",
        ),
      );
    yield* seedPhysicalProvider(
      physicalProvider("zz-held-provider", "other-alias", "physical-session", "shared-native"),
    );
    const before = yield* rawReservationState;
    expectPhysicalRejection(
      yield* store.reserveWorkflowNativeIntent(
        reservation(operationFor("paged-op", "paged-attempt"), "paged-attempt"),
      ),
    );
    assert.deepStrictEqual(yield* rawReservationState, before);
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each(physicalAdmissionKinds)(
  "physical custody serializes %s with an actual independent ledger writer and reopen",
  (kind) =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "submission-admission-")),
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
            const value = yield* preparePhysicalAdmission(kind);
            const sql = yield* SqlClient.SqlClient;
            yield* sql`PRAGMA busy_timeout=0`;
            const other = yield* Effect.acquireRelease(
              Effect.sync(() => new NodeSqlite.DatabaseSync(filename)),
              (database) => Effect.sync(() => database.close()),
            );
            const row = physicalTransport(value.threadId);
            yield* Effect.sync(() => {
              other.exec("PRAGMA busy_timeout=0; BEGIN IMMEDIATE");
              const keys = Object.keys(row);
              other
                .prepare(
                  `INSERT INTO orchestration_v2_submission_transports (${keys.map((key) => `"${key}"`).join(",")}) VALUES (${keys.map(() => "?").join(",")})`,
                )
                .run(...Object.values(row));
            });
            const before = yield* rawReservationState;
            const blocked = yield* value.invoke;
            assert.strictEqual(blocked._tag, "Failure");
            if (blocked._tag === "Failure")
              assert.strictEqual(
                blocked.failure.reason,
                kind.startsWith("workflow")
                  ? "persistence"
                  : kind === "planning-native"
                    ? "storage"
                    : "conflict",
              );
            assert.deepStrictEqual(yield* rawReservationState, before);
            yield* Effect.sync(() => other.exec("COMMIT"));
            const committed = yield* value.invoke;
            if (kind.startsWith("workflow")) {
              assert.strictEqual(committed._tag, "Success");
              if (committed._tag === "Success") expectPhysicalRejection(committed.success);
            } else {
              assert.strictEqual(committed._tag, "Failure");
              if (committed._tag === "Failure")
                assert.strictEqual(committed.failure.reason, "custody");
            }
            assert.deepStrictEqual(yield* rawReservationState, before);
            return { threadId: value.threadId, invokeOn: value.invokeOn, before };
          }).pipe(Effect.provide(fileLayer())),
        );
        yield* Effect.gen(function* () {
          const store = yield* Store.WorkflowRunStore;
          const result = yield* prepared.invokeOn(store);
          if (kind.startsWith("workflow")) {
            assert.strictEqual(result._tag, "Success");
            if (result._tag === "Success") expectPhysicalRejection(result.success);
          } else {
            assert.strictEqual(result._tag, "Failure");
            if (result._tag === "Failure") assert.strictEqual(result.failure.reason, "custody");
          }
          assert.deepStrictEqual(yield* rawReservationState, prepared.before);
        }).pipe(Effect.provide(fileLayer()));
      }),
    ),
);

it.effect.each(physicalAdmissionKinds)(
  "physical custody maps unavailable lookup storage for %s without writes",
  (kind) =>
    Effect.gen(function* () {
      const prepared = yield* preparePhysicalAdmission(kind);
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const subscription = yield* store.subscribeChanges;
      const before = yield* rawReservationState;
      yield* sql`DROP TABLE orchestration_v2_submission_transports`;
      const result = yield* prepared.invoke;
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure")
        assert.strictEqual(
          result.failure.reason,
          kind.startsWith("workflow") ? "persistence" : "storage",
        );
      assert.deepStrictEqual(yield* rawReservationState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect("physical custody keeps a remote child identity separate from a local alias", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const row = physicalTransport("other-source");
    yield* sql`INSERT INTO orchestration_v2_submission_transports ${sql.insert(row)}`;
    yield* seedPhysicalNode("remote-parent", "other-source");
    const value = {
      id: "remote-subagent",
      threadId: "other-source",
      runId: null,
      parentNodeId: "remote-parent",
      origin: "app_owned",
      createdBy: "agent",
      driver: "codex",
      providerInstanceId: "provider",
      providerThreadId: null,
      childThreadId: null,
      remoteChild: { environmentId: "remote-environment", threadId: "thread", label: "remote" },
      nativeTaskRef: null,
      prompt: "Work",
      title: null,
      model: null,
      status: "completed",
      result: null,
      startedAt: null,
      completedAt: null,
      updatedAt: facts.now,
    };
    yield* sql`INSERT INTO orchestration_v2_projection_subagents ${sql.insert({ subagent_id: value.id, thread_id: value.threadId, run_id: null, parent_node_id: value.parentNodeId, provider: "provider", driver: "codex", provider_instance_id: "provider", provider_thread_id: null, child_thread_id: null, origin: value.origin, status: value.status, started_at: null, completed_at: null, updated_at: value.updatedAt, payload_json: json(value) })}`;
    assert.strictEqual(
      (yield* store.reserveWorkflowNativeIntent(
        reservation(operationFor("remote-op", "remote-attempt"), "remote-attempt"),
      )).kind,
      "reserved",
    );
  }).pipe(Effect.provide(layerWithOutbox)),
);

const prepareLogicalCustodyOwner = Effect.fn("test.prepareLogicalCustodyOwner")(function* (
  owner: "workflow" | "planning",
) {
  const store = yield* Store.WorkflowRunStore;
  if (owner === "workflow") {
    yield* setupAdmission;
    const input = reservation(
      operationFor("logical-owner-op", "logical-owner-attempt"),
      "logical-owner-attempt",
    );
    yield* store.reserveWorkflowNativeIntent(input);
    return input.threadId;
  }
  const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
  yield* begin(store, session);
  const input = planningReservation(admissionInput(binding, "logical-owner"));
  yield* store.reservePlanningNativeIntent(input);
  return input.threadId;
});

it.effect.each(["workflow", "planning"] as const)(
  "ordinary custody finds %s ownership through a native alias with an empty physical ledger",
  (owner) =>
    Effect.gen(function* () {
      const ownedThread = yield* prepareLogicalCustodyOwner(owner);
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* seedPhysicalSession("ordinary-session");
      yield* seedPhysicalSession("owned-session");
      yield* seedPhysicalProvider(
        physicalProvider(
          "ordinary-provider",
          "ordinary-thread",
          "ordinary-session",
          "shared-native",
        ),
      );
      yield* seedPhysicalProvider(
        physicalProvider("owned-provider", ownedThread, "owned-session", "shared-native"),
      );
      assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_v2_submission_transports`, []);
      const subscription = yield* store.subscribeChanges;
      const before = yield* rawReservationState;
      assert.deepStrictEqual(
        yield* sql.withTransaction(
          store.readRecoveryCustodyInTransaction({
            threadId: "ordinary-thread",
            providerSessionIds: [],
            providerThreadIds: ["ordinary-provider"],
          }),
        ),
        { kind: "owned" },
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect("ordinary custody owns its selected seeds before returning an Effect", () =>
  Effect.gen(function* () {
    const ownedThread = yield* prepareLogicalCustodyOwner("workflow");
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    const input = { threadId: ownedThread, providerSessionIds: [] as string[] };
    const observation = store.readRecoveryCustodyInTransaction(input);
    input.threadId = ThreadId.make("ordinary-thread");
    assert.deepStrictEqual(yield* sql.withTransaction(observation), { kind: "owned" });
    yield* seedPhysicalProvider(physicalProvider("owned-provider", ownedThread, null, null));
    const providers = ["owned-provider"];
    const selected = store.readRecoveryCustodyInTransaction({
      threadId: "ordinary-thread",
      providerSessionIds: [],
      providerThreadIds: providers,
    });
    providers.length = 0;
    assert.deepStrictEqual(yield* sql.withTransaction(selected), { kind: "owned" });
  }).pipe(Effect.provide(layerWithOutbox)),
);

const seedPhysicalSubagent = Effect.fn("test.seedPhysicalSubagent")(function* (
  id: string,
  threadId: string,
  parentNodeId: string,
  providerThreadId: string | null,
  childThreadId: string | null = null,
) {
  const sql = yield* SqlClient.SqlClient;
  const value = {
    id,
    threadId,
    runId: null,
    parentNodeId,
    origin: "provider_native",
    createdBy: "agent",
    driver: "codex",
    providerInstanceId: "provider",
    providerThreadId,
    childThreadId,
    nativeTaskRef: null,
    prompt: "Work",
    title: null,
    model: null,
    status: "completed",
    result: null,
    startedAt: null,
    completedAt: null,
    updatedAt: facts.now,
  };
  yield* sql`INSERT INTO orchestration_v2_projection_subagents ${sql.insert({ subagent_id: id, thread_id: threadId, run_id: null, parent_node_id: parentNodeId, provider: "provider", driver: "codex", provider_instance_id: "provider", provider_thread_id: providerThreadId, child_thread_id: childThreadId, origin: value.origin, status: value.status, started_at: null, completed_at: null, updated_at: value.updatedAt, payload_json: json(value) })}`;
});

it.effect("ordinary custody has no physical-table dependency or public mode override", () =>
  Effect.gen(function* () {
    const ownedThread = yield* prepareLogicalCustodyOwner("workflow");
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DROP TABLE orchestration_v2_submission_attempts`;
    yield* sql`DROP TABLE orchestration_v2_submission_transports`;
    const input = {
      threadId: ownedThread,
      providerSessionIds: [],
      purpose: "physical",
      environmentId: "foreign",
    };
    assert.deepStrictEqual(
      yield* sql.withTransaction(store.readRecoveryCustodyInTransaction(input)),
      { kind: "owned" },
    );
    assert.deepStrictEqual(
      yield* sql.withTransaction(
        store.readRecoveryCustodyInTransaction({
          threadId: "ordinary-thread",
          providerSessionIds: [],
        }),
      ),
      { kind: "ordinary" },
    );
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect("ordinary custody rejects non-data seeds synchronously without invoking getters", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const store = yield* Store.WorkflowRunStore;
    const sql = yield* SqlClient.SqlClient;
    for (const explicitUndefined of [false, true]) {
      const input = { threadId: "ordinary-thread", providerSessionIds: [] };
      if (explicitUndefined) Reflect.set(input, "providerThreadIds", undefined);
      assert.deepStrictEqual(
        yield* sql.withTransaction(store.readRecoveryCustodyInTransaction(input)),
        { kind: "ordinary" },
      );
    }
    let calls = 0;
    const getter = Object.defineProperty({ providerSessionIds: [] }, "threadId", {
      enumerable: true,
      get() {
        calls += 1;
        return "thread";
      },
    });
    const cycle: Record<string, unknown> = { threadId: "thread", providerSessionIds: [] };
    cycle.self = cycle;
    const optionalGetter = Object.defineProperty(
      { threadId: "thread", providerSessionIds: [] },
      "providerThreadIds",
      {
        enumerable: true,
        get() {
          calls += 1;
          return undefined;
        },
      },
    );
    for (const input of [
      getter,
      optionalGetter,
      cycle,
      { threadId: "thread", providerSessionIds: [undefined] },
    ]) {
      const effect = store.readRecoveryCustodyInTransaction(
        input as Parameters<typeof store.readRecoveryCustodyInTransaction>[0],
      );
      assert.strictEqual(calls, 0);
      assert.strictEqual((yield* Effect.flip(sql.withTransaction(effect))).reason, "integrity");
    }
    assert.strictEqual(calls, 0);
  }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect(
  "ordinary alias denial reaches the controlled primitive under the actual writer with zero Sink bytes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "ordinary-alias-primitive-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        const fileLayer = Layer.merge(Store.layer, EffectOutbox.layer).pipe(
          Layer.provideMerge(NodeSqliteClient.layer({ filename })),
        );
        yield* Effect.gen(function* () {
          const ownedThread = yield* prepareLogicalCustodyOwner("workflow");
          const store = yield* Store.WorkflowRunStore;
          const sql = yield* SqlClient.SqlClient;
          yield* seedPhysicalSession("ordinary-session");
          yield* seedPhysicalSession("owned-session");
          yield* seedPhysicalProvider(
            physicalProvider(
              "ordinary-provider",
              "ordinary-thread",
              "ordinary-session",
              "shared-native",
            ),
          );
          yield* seedPhysicalProvider(
            physicalProvider("owned-provider", ownedThread, "owned-session", "shared-native"),
          );
          const other = yield* Effect.acquireRelease(
            Effect.sync(() => {
              const db = new NodeSqlite.DatabaseSync(filename);
              db.exec("PRAGMA busy_timeout=0");
              return db;
            }),
            (db) => Effect.sync(() => db.close()),
          );
          const incoming = yield* Queue.unbounded<Uint8Array>();
          const outcomes = yield* Queue.unbounded<CodexProtocol.CodexAppServerFrameOutcome>();
          let writes = 0;
          let checked = false;
          const spawner = ChildProcessSpawner.make(() =>
            Effect.succeed(
              ChildProcessSpawner.makeHandle({
                pid: ChildProcessSpawner.ProcessId(999999996),
                exitCode: Effect.never,
                isRunning: Effect.succeed(true),
                kill: () => Effect.void,
                unref: Effect.succeed(Effect.void),
                stdin: Sink.forEach((bytes: Uint8Array) =>
                  Effect.gen(function* () {
                    writes += 1;
                    const request = JSON.parse(new TextDecoder().decode(bytes));
                    yield* Queue.offer(
                      incoming,
                      new TextEncoder().encode(
                        `${JSON.stringify({ id: request.id, result: {} })}\n`,
                      ),
                    );
                  }),
                ),
                stdout: Stream.fromQueue(incoming),
                stderr: Stream.empty,
                all: Stream.empty,
                getInputFd: () => Sink.drain,
                getOutputFd: () => Stream.empty,
              }),
            ),
          );
          const client = yield* CodexClient.makeControlledChildProcessClient(
            ChildProcess.make("ordinary-alias-fixture"),
            {
              wrapper: (frame, submit) =>
                sql.withTransaction(
                  Effect.gen(function* () {
                    assert.strictEqual(frame.method, "turn/start");
                    assert.strictEqual(
                      (JSON.parse(frame.encoded) as { params: { threadId: string } }).params
                        .threadId,
                      "shared-native",
                    );
                    yield* Effect.sync(() =>
                      assert.throws(() => other.exec("BEGIN IMMEDIATE"), /locked/),
                    );
                    checked = true;
                    const custody = yield* store.readRecoveryCustodyInTransaction({
                      threadId: "ordinary-thread",
                      providerSessionIds: ["ordinary-session"],
                      providerThreadIds: ["ordinary-provider"],
                    });
                    if (custody.kind === "owned")
                      return yield* new CodexErrors.CodexAppServerFrameSubmissionError({
                        outcome: "not_submitted",
                        reason: "wrapper",
                      });
                    return yield* submit();
                  }),
                ),
              onOutcome: (outcome) => Queue.offer(outcomes, outcome).pipe(Effect.asVoid),
            },
          ).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
          const before = yield* rawReservationState;
          const subscription = yield* store.subscribeChanges;
          const result = yield* Effect.result(
            client.raw.request(
              "turn/start",
              { threadId: "shared-native", input: [{ type: "text", text: "Work" }] },
              "ordinary-alias-selection",
            ),
          );
          assert.strictEqual(result._tag, "Failure");
          if (result._tag === "Failure")
            assert.strictEqual(result.failure._tag, "CodexAppServerFrameSubmissionError");
          assert.isTrue(checked);
          assert.strictEqual(writes, 0);
          const outcome = yield* Queue.take(outcomes);
          assert.strictEqual(outcome.outcome, "not_submitted");
          yield* Effect.sync(() => other.exec("BEGIN IMMEDIATE; ROLLBACK"));
          assert.deepStrictEqual(yield* rawReservationState, before);
          assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
        }).pipe(Effect.provide(fileLayer));
      }),
    ),
);

it.effect.each(["node", "subagent", "absent"] as const)(
  "ordinary custody resolves a %s hierarchy parent separately from a required owner",
  (parent) =>
    Effect.gen(function* () {
      const ownedThread = yield* prepareLogicalCustodyOwner("workflow");
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      yield* seedPhysicalNode("local-node", "ordinary-thread");
      yield* sql`UPDATE orchestration_v2_projection_nodes SET parent_node_id='parent-meta',provider_thread_id='historical-provider',payload_json=json_set(payload_json,'$.parentNodeId','parent-meta','$.providerThreadId','historical-provider','$.runtimeRequestId','request','$.checkpointScopeId','checkpoint') WHERE node_id='local-node'`;
      yield* seedPhysicalProvider(
        physicalProvider(
          "selected-provider",
          "ordinary-thread",
          null,
          null,
          "provider",
          "local-node",
        ),
      );
      if (parent === "node") yield* seedPhysicalNode("parent-meta", ownedThread);
      if (parent === "subagent")
        yield* seedPhysicalSubagent("parent-meta", ownedThread, "older-parent", null);
      const before = yield* rawReservationState;
      assert.deepStrictEqual(
        yield* sql.withTransaction(
          store.readRecoveryCustodyInTransaction({
            threadId: "ordinary-thread",
            providerSessionIds: [],
            providerThreadIds: ["selected-provider"],
          }),
        ),
        { kind: parent === "absent" ? "ordinary" : "owned" },
      );
      assert.strictEqual(
        (yield* Effect.flip(
          sql.withTransaction(
            store.readRecoveryCustodyInTransaction({
              threadId: "ordinary-thread",
              providerSessionIds: [],
              providerThreadIds: ["selected-provider", "historical-provider"],
            }),
          ),
        )).reason,
        "integrity",
      );
      assert.deepStrictEqual(yield* rawReservationState, before);
    }).pipe(Effect.provide(layerWithOutbox)),
);

it.effect.each(["workflow", "planning"] as const)(
  "native-bound %s intent claim refuses a different coordinator before writing",
  (kind) =>
    Effect.gen(function* () {
      const fixture = yield* nativeBindingFixture(kind, false, 2, false);
      const selected = { ...fixture.selection, expectedOwnerVersion: 1, ...fixture.coordinator };
      const before = yield* nativeBindingState;
      const subscription = yield* fixture.store.subscribeChanges;
      for (const changed of [{ bootId: "foreign-boot" }, { workerId: "foreign-worker" }]) {
        assert.deepStrictEqual(
          yield* fixture.store.claimNativeIntent({ ...selected, ...changed }),
          { kind: "stale" },
        );
        assert.deepStrictEqual(yield* nativeBindingState, before);
        assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      }
      assert.strictEqual((yield* fixture.store.claimNativeIntent(selected)).kind, "claimed");
    }).pipe(Effect.provide(layer)),
);
