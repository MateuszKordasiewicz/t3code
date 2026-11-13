// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodePathService from "@effect/platform-node/NodePath";
import { assert, it } from "@effect/vitest";
import {
  OrchestrationV2AppThreadJson,
  OrchestrationV2RunJson,
  OrchestrationV2ConversationMessageJson,
  OrchestrationV2DomainEventJson,
  PlanningPassV1,
  PlanningSessionV1,
  upgradeDevelopmentPlanDocumentV1,
  WorkflowOperationV1,
  WorkflowRunV1,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Digest from "effect/crypto/Digest";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { ServerConfig } from "../config.ts";
import { DevelopmentPlanHash } from "../developmentPlans/DevelopmentPlanHash.ts";
import { runMigrations } from "../persistence/Migrations.ts";
import * as Derivation from "./WorkflowNativeLaunchDerivation.ts";
import {
  encodeNativeCommandProof,
  makeNativeCommandProofVerifier,
  NativeCommandProofError,
} from "./WorkflowNativeCommandProof.ts";
import {
  assertMatchesDerived,
  NativeCommandSnapshotError,
  snapshotNativeInput,
} from "./WorkflowNativeCommandSnapshot.ts";
import * as Store from "./WorkflowRunStore.ts";
import { WorkflowCanonicalHash } from "./WorkflowCanonicalHash.ts";
import { WorkflowRunPolicy } from "./WorkflowRunPolicy.ts";

const hash = "a".repeat(64);
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

const testLayer = (filename = ":memory:") => {
  const dependencies = Layer.mergeAll(
    NodeSqliteClient.layer({ filename }),
    NodePathService.layerPosix,
    Layer.succeed(ServerConfig, { baseDir: "/config" } as ServerConfig["Service"]),
  );
  const withStore = Store.layer.pipe(Layer.provideMerge(dependencies));
  return Derivation.layer.pipe(Layer.provideMerge(withStore));
};
const seedProject = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at) VALUES ('project', 'Project', '/old', '[]', '2026-11-01T00:00:00Z', '2026-11-01T00:00:00Z')`;
});
const basicInput = () => reservation(operationFor("operation", "attempt"), "attempt");

type DerivedFixture = Effect.Success<
  ReturnType<Derivation.WorkflowNativeLaunchDerivation["Service"]["deriveInTransaction"]>
>;
type ProofVerifier = Effect.Success<ReturnType<typeof makeNativeCommandProofVerifier>>;
const fixtureTime = "2026-11-01T00:00:00.000Z";
const decodeFixtureThread = Schema.decodeUnknownEffect(OrchestrationV2AppThreadJson);
const encodeFixtureThread = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
const decodeFixtureRun = Schema.decodeUnknownEffect(OrchestrationV2RunJson);
const encodeFixtureRun = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2RunJson));
const decodeFixtureMessage = Schema.decodeUnknownEffect(OrchestrationV2ConversationMessageJson);
const encodeFixtureMessage = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2ConversationMessageJson),
);
const decodeFixtureEvent = Schema.decodeUnknownEffect(OrchestrationV2DomainEventJson);
const encodeFixtureEvent = Schema.encodeEffect(OrchestrationV2DomainEventJson);
const decodeFixtureJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));

const seedNativeProofs = Effect.fn("test.seedNativeProofs")(function* (
  sql: SqlClient.SqlClient,
  verifier: ProofVerifier,
  derived: DerivedFixture,
  phase: "claim" | "message",
) {
  const launch = derived.reservation.material.launch;
  const command = phase === "claim" ? derived.claimCommand : derived.messageCommand;
  const hint = { intentId: derived.reservation.intentId, phase };
  const proof = yield* verifier.prepareInTransaction(command, hint);
  const modelSelection = { instanceId: "provider", model: "model" };
  const thread = {
    id: launch.threadId,
    projectId: launch.projectId,
    title: "Thread",
    providerInstanceId: "provider",
    modelSelection,
    createdBy: "user",
    creationSource: "web",
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: launch.threadId },
    forkedFrom: null,
    createdAt: fixtureTime,
    updatedAt: fixtureTime,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
  const nativeRun = {
    id: "actual-t3-run",
    threadId: launch.threadId,
    ordinal: 7,
    providerInstanceId: "provider",
    modelSelection,
    providerThreadId: "provider-thread",
    userMessageId: launch.initialMessage.messageId,
    rootNodeId: null,
    activeAttemptId: null,
    status: "preparing",
    requestedAt: fixtureTime,
    startedAt: null,
    completedAt: null,
    checkpointId: null,
    contextHandoffId: null,
  };
  const message = {
    createdBy: "user",
    creationSource: "web",
    id: launch.initialMessage.messageId,
    threadId: launch.threadId,
    runId: "actual-t3-run",
    nodeId: null,
    role: "user",
    text: "fixture",
    attachments: [],
    streaming: false,
    createdAt: fixtureTime,
    updatedAt: fixtureTime,
  };
  const threadJson = yield* encodeFixtureThread(yield* decodeFixtureThread(thread));
  const runJson = yield* encodeFixtureRun(yield* decodeFixtureRun(nativeRun));
  const messageJson = yield* encodeFixtureMessage(yield* decodeFixtureMessage(message));
  const events =
    phase === "claim"
      ? [
          {
            sequence: 1,
            id: "claim-event",
            type: command.type === "thread.create" ? "thread.created" : "thread.metadata-updated",
            payload: thread,
          },
        ]
      : [
          { sequence: 2, id: "run-event", type: "run.created", payload: nativeRun },
          { sequence: 3, id: "message-event", type: "message.updated", payload: message },
        ];
  const sequence = phase === "claim" ? 1 : 3;
  yield* sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type) VALUES (${command.commandId},'thread',${launch.threadId},${fixtureTime},${sequence},'accepted',NULL,${command.type})`;
  yield* sql`INSERT INTO workflow_native_command_proofs (command_id,intent_id,phase,proof_json,proof_hash) VALUES (${command.commandId},${hint.intentId},${phase},${proof.proofJson},${proof.proofHash})`;
  if (phase === "claim") {
    yield* sql`INSERT INTO orchestration_v2_projection_threads (thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES (${launch.threadId},${launch.projectId},'Thread','provider','full-access','default',${fixtureTime},${fixtureTime},${threadJson})`;
  } else {
    yield* sql`INSERT INTO orchestration_v2_projection_runs (run_id,thread_id,ordinal,provider,status,requested_at,payload_json) VALUES ('actual-t3-run',${launch.threadId},7,'provider','preparing',${fixtureTime},${runJson})`;
    yield* sql`INSERT INTO orchestration_v2_projection_messages (message_id,thread_id,run_id,role,streaming,created_at,updated_at,payload_json) VALUES (${launch.initialMessage.messageId},${launch.threadId},'actual-t3-run','user',0,${fixtureTime},${fixtureTime},${messageJson})`;
  }
  for (const fixture of events) {
    const decoded = yield* decodeFixtureEvent({
      id: fixture.id,
      threadId: launch.threadId,
      occurredAt: fixtureTime,
      type: fixture.type,
      payload: fixture.payload,
    });
    const encoded = yield* encodeFixtureEvent(decoded);
    yield* sql`INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES (${fixture.sequence},${fixture.id},'thread',${launch.threadId},${fixture.sequence},${fixture.type},${fixtureTime},${command.commandId},'client',${json(encoded.payload)},'{}',2)`;
  }
  return proof;
});

it.effect(
  "derives frozen commands and detached opaque context inside the captured transaction",
  () =>
    Effect.gen(function* () {
      yield* setupAdmission;
      yield* seedProject;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
      const input = basicInput();
      const launch = {
        ...input.launch,
        generateTitle: true,
        modelSelection: {
          ...input.launch.modelSelection,
          options: [{ id: "effort", value: "high" }],
        },
        initialMessage: {
          ...input.launch.initialMessage,
          context: {
            version: 1,
            records: [
              {
                version: 1,
                contextId: "opaque",
                kind: "future-kind",
                label: "Future",
                payload: { nested: ["original"] },
              },
            ],
          },
        },
      };
      assert.strictEqual(
        (yield* store.reserveWorkflowNativeIntent({ ...input, launch })).kind,
        "reserved",
      );
      assert.strictEqual(
        (yield* Effect.flip(derive.deriveInTransaction({ intentId: input.intentId }))).reason,
        "integrity",
      );
      const read = () =>
        sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId }));
      const first = yield* read();
      assert.deepStrictEqual<unknown>(first.normalization, {
        version: 1,
        project: { projectId: "project", workspaceRoot: "/old" },
        scratchRoot: "/config/scratch",
        workspace: {
          requested: launch.workspaceStrategy,
          effective: launch.workspaceStrategy,
          reason: "unchanged",
        },
        reuse: { requested: false, claimKind: "create" },
        attachments: { kind: "none" },
      });
      assert.strictEqual(first.claimCommand.commandId, launch.commandId);
      assert.strictEqual(first.messageCommand.commandId, `${launch.commandId}:initial-message`);
      assert.strictEqual(first.messageCommand.messageId, launch.initialMessage.messageId);
      assert.deepStrictEqual<unknown>(first.messageCommand.context, launch.initialMessage.context);
      assert.strictEqual(first.messageCommand.titleSeed, launch.title);
      assert.strictEqual(
        first.normalizationHash,
        WorkflowCanonicalHash.tagged("t3.workflow-native-normalization.v1", first.normalization),
      );
      assert.strictEqual(
        first.expectedClaimCommandHash,
        WorkflowCanonicalHash.tagged("t3.workflow-native-command.v1", first.claimCommand),
      );
      assert.strictEqual(
        first.expectedMessageCommandHash,
        WorkflowCanonicalHash.tagged("t3.workflow-native-command.v1", first.messageCommand),
      );
      if (first.claimCommand.type !== "thread.create") throw new Error("expected create");
      const claimHint = { intentId: input.intentId, phase: "claim" as const };
      const messageHint = { intentId: input.intentId, phase: "message" as const };
      const claim = snapshotNativeInput(first.claimCommand, claimHint);
      const message = snapshotNativeInput(first.messageCommand, messageHint);
      assertMatchesDerived(claim, first);
      assertMatchesDerived(message, first);
      const reason = (expected: "invalid" | "conflict") => new RegExp(`snapshot ${expected}`);
      assert.throws(
        () =>
          assertMatchesDerived(
            snapshotNativeInput(first.claimCommand, { ...claimHint, intentId: "other" }),
            first,
          ),
        NativeCommandSnapshotError,
        reason("conflict"),
      );
      assert.throws(
        () =>
          assertMatchesDerived(
            snapshotNativeInput({ ...first.claimCommand, commandId: "other" }, claimHint),
            first,
          ),
        NativeCommandSnapshotError,
        reason("conflict"),
      );
      assert.throws(
        () =>
          assertMatchesDerived(
            snapshotNativeInput({ ...first.messageCommand, threadId: "other" }, messageHint),
            first,
          ),
        NativeCommandSnapshotError,
        reason("conflict"),
      );
      assert.throws(
        () => snapshotNativeInput(first.messageCommand, claimHint),
        NativeCommandSnapshotError,
        reason("invalid"),
      );
      assert.throws(
        () => snapshotNativeInput(first.claimCommand, messageHint),
        NativeCommandSnapshotError,
        reason("invalid"),
      );
      assert.throws(
        () =>
          snapshotNativeInput(
            { type: "thread.delete", commandId: "x", threadId: "thread" },
            claimHint,
          ),
        NativeCommandSnapshotError,
        reason("invalid"),
      );
      assert.throws(
        () => snapshotNativeInput(first.claimCommand, { ...claimHint, extra: true }),
        NativeCommandSnapshotError,
        reason("invalid"),
      );
      assert.throws(
        () => snapshotNativeInput(first.claimCommand, { ...claimHint, intentId: " " }),
        NativeCommandSnapshotError,
        reason("invalid"),
      );
      const badCommands = [
        { ...first.claimCommand, extra: true },
        {
          ...first.claimCommand,
          modelSelection: { ...first.claimCommand.modelSelection, options: { effort: "high" } },
        },
        { ...first.claimCommand, title: undefined },
        { ...first.claimCommand, title: NaN },
        {
          ...first.claimCommand,
          modelSelection: { ...first.claimCommand.modelSelection, provider: "legacy" },
        },
        {
          ...first.messageCommand,
          context: {
            version: 1,
            records: [{ version: 1, contextId: "broken", kind: "terminal", label: "Broken" }],
          },
        },
        {
          ...first.messageCommand,
          context: {
            version: 1,
            records: [
              {
                version: 1,
                contextId: "opaque",
                kind: "future-kind",
                label: "Future",
                payload: { bad: undefined },
              },
            ],
          },
        },
      ];
      for (const bad of badCommands) {
        assert.throws(
          () => snapshotNativeInput(bad, bad.type === "message.dispatch" ? messageHint : claimHint),
          NativeCommandSnapshotError,
          reason("invalid"),
        );
      }
      let getterCalls = 0;
      const getterCommand = {
        ...first.claimCommand,
        get title() {
          getterCalls++;
          return "Work";
        },
      };
      assert.throws(
        () => snapshotNativeInput(getterCommand, claimHint),
        NativeCommandSnapshotError,
        reason("invalid"),
      );
      assert.throws(
        () =>
          snapshotNativeInput(first.claimCommand, {
            ...claimHint,
            get extra() {
              getterCalls++;
              return true;
            },
          }),
        NativeCommandSnapshotError,
        reason("invalid"),
      );
      assert.strictEqual(getterCalls, 0);
      const changedExpected = structuredClone(first);
      (
        changedExpected.messageCommand.context!.records[0] as { payload: { nested: string[] } }
      ).payload.nested[0] = "changed";
      assert.throws(
        () => assertMatchesDerived(message, changedExpected),
        NativeCommandSnapshotError,
        reason("conflict"),
      );
      const otherMessage = structuredClone(first.messageCommand);
      (otherMessage.modelSelection!.options![0] as { value: string }).value = "low";
      assert.throws(
        () => assertMatchesDerived(snapshotNativeInput(otherMessage, messageHint), first),
        NativeCommandSnapshotError,
        reason("conflict"),
      );
      const postComparison = structuredClone(first);
      assertMatchesDerived(message, postComparison);
      (
        postComparison.messageCommand.context!.records[0] as { payload: { nested: string[] } }
      ).payload.nested[0] = "later";
      assert.throws(
        () => assertMatchesDerived(message, postComparison),
        NativeCommandSnapshotError,
        reason("conflict"),
      );
      const changedHash = { ...first, expectedMessageCommandHash: "0".repeat(64) };
      assert.throws(
        () => assertMatchesDerived(message, changedHash),
        NativeCommandSnapshotError,
        reason("conflict"),
      );
      const callerCommand = structuredClone(first.messageCommand);
      const callerHint = { ...messageHint };
      const owned = snapshotNativeInput(callerCommand, callerHint);
      const originalBytes = owned.canonicalCommand;
      const originalHash = owned.commandHash;
      (callerCommand.context!.records[0] as { payload: { nested: string[] } }).payload.nested[0] =
        "changed";
      (callerCommand.modelSelection!.options![0] as { value: string }).value = "low";
      callerHint.intentId = "other";
      assertMatchesDerived(owned, first);
      assert.strictEqual(owned.canonicalCommand, originalBytes);
      assert.strictEqual(owned.commandHash, originalHash);
      assert.strictEqual(Object.isFrozen(callerCommand), false);
      assert.strictEqual(Object.isFrozen(callerHint), false);
      const ownedMessage = owned.command;
      if (ownedMessage.type !== "message.dispatch") throw new Error("expected message");
      const nested = (ownedMessage.context!.records[0] as { payload: { nested: string[] } }).payload
        .nested;
      assert.isTrue(Object.isFrozen(nested));
      assert.throws(() => {
        nested[0] = "changed";
      });
      assert.strictEqual(nested[0], "original");
      (changedExpected.messageCommand.modelSelection!.options![0] as { value: string }).value =
        "low";
      assertMatchesDerived(owned, first);
      const records = first.reservation.material.launch.initialMessage.context!.records;
      (records[0] as { payload: { nested: string[] } }).payload.nested[0] = "mutated";
      assert.deepStrictEqual<unknown>(
        (yield* read()).messageCommand.context,
        launch.initialMessage.context,
      );
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each([
  ["missing", null],
  ["deleted", "/deleted"],
  ["empty", ""],
  ["relative", "relative"],
  ["scratch", "/config/other/../scratch"],
] as const)("refuses %s project", ([scenario, root]) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
    const input = basicInput();
    yield* store.reserveWorkflowNativeIntent(input);
    if (root !== null) {
      yield* seedProject;
      yield* sql`UPDATE projection_projects SET workspace_root = ${root}, deleted_at = ${scenario === "deleted" ? "2026-11-02T00:00:00Z" : null}`;
    }
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId })),
      )).reason,
      "integrity",
    );
  }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["attachment", "image", "file"] as const)("refuses %s inputs", (scenario) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    yield* seedProject;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
    const input = basicInput();
    const attachment = {
      type: "file",
      id: "thread-file",
      name: "a.txt",
      mimeType: "text/plain",
      sizeBytes: 1,
    };
    const record = {
      version: 1,
      contextId: "ref",
      kind: scenario,
      label: "ref",
      attachmentId: "thread-file",
      name: "a.txt",
      mimeType: scenario === "image" ? "image/png" : "text/plain",
      sizeBytes: 1,
    };
    const launch = {
      ...input.launch,
      initialMessage: {
        ...input.launch.initialMessage,
        attachments: scenario === "attachment" ? [attachment] : [],
        ...(scenario === "attachment" ? {} : { context: { version: 1, records: [record] } }),
      },
    };
    assert.strictEqual(
      (yield* store.reserveWorkflowNativeIntent({ ...input, launch })).kind,
      "reserved",
    );
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId })),
      )).reason,
      "integrity",
    );
  }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "uses writer-local project state while an independent reader sees committed state, then rolls back",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const directory = yield* Effect.acquireRelease(
          Effect.sync(() =>
            NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-derivation-")),
          ),
          (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
        );
        const filename = NodePath.join(directory, "state.sqlite");
        yield* Effect.gen(function* () {
          yield* setupAdmission;
          yield* seedProject;
          const sql = yield* SqlClient.SqlClient;
          const store = yield* Store.WorkflowRunStore;
          const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
          const input = basicInput();
          yield* store.reserveWorkflowNativeIntent(input);
          const reader = yield* Effect.acquireRelease(
            Effect.sync(() => new NodeSqlite.DatabaseSync(filename, { readOnly: true })),
            (db) => Effect.sync(() => db.close()),
          );
          const externalRoot = () =>
            (
              reader
                .prepare(
                  "SELECT workspace_root FROM projection_projects WHERE project_id = 'project'",
                )
                .get() as { workspace_root: string }
            ).workspace_root;
          assert.strictEqual(externalRoot(), "/old");
          const before = yield* sql.withTransaction(
            derive.deriveInTransaction({ intentId: input.intentId }),
          );
          const result = yield* Effect.result(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* sql`UPDATE projection_projects SET workspace_root = '/writer' WHERE project_id = 'project'`;
                const writerResult = yield* derive.deriveInTransaction({
                  intentId: input.intentId,
                });
                assert.strictEqual(writerResult.normalization.project.workspaceRoot, "/writer");
                assert.notStrictEqual(writerResult.normalizationHash, before.normalizationHash);
                assert.strictEqual(externalRoot(), "/old");
                return yield* Effect.fail("rollback");
              }),
            ),
          );
          assert.strictEqual(result._tag, "Failure");
          assert.strictEqual(externalRoot(), "/old");
          assert.strictEqual(
            (yield* sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId })))
              .normalization.project.workspaceRoot,
            "/old",
          );
        }).pipe(Effect.provide(testLayer(filename)));
      }),
    ),
);

it.effect.each([
  { type: "root", branch: "feature" },
  { type: "worktree", baseRef: "origin/main", branch: "feature", startFromOrigin: true },
  { type: "existing_worktree", worktreePath: "/somewhere", branch: "feature" },
] as const)("preserves requested $type strategy and reuse", (strategy) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    yield* seedProject;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
    const input = basicInput();
    const launch = { ...input.launch, workspaceStrategy: strategy, reuseExistingThread: true };
    yield* store.reserveWorkflowNativeIntent({ ...input, launch });
    const result = yield* sql.withTransaction(
      derive.deriveInTransaction({ intentId: input.intentId }),
    );
    assert.deepStrictEqual(result.normalization.workspace, {
      requested: strategy,
      effective: strategy,
      reason: "unchanged",
    });
    assert.deepStrictEqual(result.normalization.reuse, {
      requested: true,
      claimKind: "metadata-update",
    });
    assert.strictEqual(result.claimCommand.type, "thread.metadata.update");
    const claim = snapshotNativeInput(result.claimCommand, {
      intentId: input.intentId,
      phase: "claim",
    });
    assertMatchesDerived(claim, result);
    const expectedClaim = result.claimCommand;
    if (expectedClaim.type !== "thread.metadata.update") throw new Error("expected reuse");
    assert.throws(
      () =>
        assertMatchesDerived(
          { ...claim, command: { ...expectedClaim, expectedEmpty: false } },
          result,
        ),
      NativeCommandSnapshotError,
      /snapshot conflict/,
    );
    const create = {
      ...result,
      claimCommand: {
        ...result.claimCommand,
        type: "thread.create" as const,
      } as typeof result.claimCommand,
    };
    assert.throws(
      () => assertMatchesDerived(claim, create),
      NativeCommandSnapshotError,
      /snapshot conflict/,
    );
    assert.deepStrictEqual(result.messageCommand.dispatchMode, {
      type: "defer_start",
      workspaceStrategy: strategy,
    });
  }).pipe(Effect.provide(testLayer())),
);

it.effect("classifies malformed project data and SQL failures separately", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    yield* seedProject;
    const sql = yield* SqlClient.SqlClient;
    const store = yield* Store.WorkflowRunStore;
    const derive = yield* Derivation.WorkflowNativeLaunchDerivation;
    const input = basicInput();
    yield* store.reserveWorkflowNativeIntent(input);
    yield* sql`UPDATE projection_projects SET scripts_json = 'not-json'`;
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId })),
      )).reason,
      "integrity",
    );
    yield* sql`DROP TABLE projection_projects`;
    assert.strictEqual(
      (yield* Effect.flip(
        sql.withTransaction(derive.deriveInTransaction({ intentId: input.intentId })),
      )).reason,
      "persistence",
    );
  }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "rejects a foreign client transaction and independently checks the store's captured client",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const derivation = yield* Derivation.WorkflowNativeLaunchDerivation;
      yield* Effect.gen(function* () {
        const foreignSql = yield* SqlClient.SqlClient;
        const foreignStore = yield* Store.WorkflowRunStore;
        assert.notStrictEqual(foreignStore, store);
        assert.strictEqual(
          (yield* Effect.flip(
            foreignSql.withTransaction(derivation.deriveInTransaction({ intentId: "missing" })),
          )).reason,
          "integrity",
        );
        assert.strictEqual(
          (yield* Effect.flip(
            foreignSql.withTransaction(
              store.readNativeReservationInTransaction({ intentId: "missing" }),
            ),
          )).reason,
          "integrity",
        );
        const mismatched = yield* Derivation.WorkflowNativeLaunchDerivation.pipe(
          Effect.provide(Layer.fresh(Derivation.layer)),
          Effect.provideService(Store.WorkflowRunStore, foreignStore),
          Effect.provideService(SqlClient.SqlClient, sql),
        );
        assert.strictEqual(
          (yield* Effect.flip(
            sql.withTransaction(mismatched.deriveInTransaction({ intentId: "missing" })),
          )).reason,
          "integrity",
        );
      }).pipe(
        Effect.provide(
          Layer.fresh(Store.layer).pipe(
            Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
          ),
        ),
      );
    }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "captures command, hint and config and serializes a fresh workflow candidate without writes",
  () =>
    Effect.gen(function* () {
      yield* setupAdmission;
      yield* seedProject;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const derivation = yield* Derivation.WorkflowNativeLaunchDerivation;
      const path = yield* Path.Path;
      const config = { ...(yield* ServerConfig), baseDir: "/config" };
      const input = basicInput();
      const launch = {
        ...input.launch,
        modelSelection: {
          ...input.launch.modelSelection,
          options: [{ id: "effort", value: "high" }],
        },
        initialMessage: {
          ...input.launch.initialMessage,
          context: {
            version: 1,
            records: [
              {
                version: 1,
                contextId: "opaque",
                kind: "custom",
                label: "Opaque",
                payload: { nested: ["original"] },
              },
            ],
          },
        },
      };
      yield* store.reserveWorkflowNativeIntent({ ...input, launch });
      const derived = yield* sql.withTransaction(
        derivation.deriveInTransaction({ intentId: input.intentId }),
      );
      const verifierEffect = makeNativeCommandProofVerifier(sql, config, path);
      config.baseDir = "/mutated";
      const verifier = yield* verifierEffect;
      const command = structuredClone(derived.messageCommand);
      const hint = { intentId: input.intentId, phase: "message" };
      const prepared = verifier.prepareInTransaction(command, hint);
      (command.context!.records[0] as { payload: { nested: string[] } }).payload.nested[0] =
        "changed";
      (command.modelSelection!.options![0] as { value: string }).value = "changed";
      hint.intentId = "changed";
      const result = yield* sql.withTransaction(prepared);
      assert.isTrue(Object.isFrozen(result));
      const proof = JSON.parse(result.proofJson);
      assert.strictEqual(proof.intentId, input.intentId);
      assert.strictEqual(proof.normalization.scratchRoot, "/config/scratch");
      assert.strictEqual(proof.actualCommandHash, derived.expectedMessageCommandHash);
      assert.deepStrictEqual(encodeNativeCommandProof(proof), result);
      assert.strictEqual(
        result.proofHash,
        WorkflowCanonicalHash.tagged("t3.workflow-native-command-proof.v1", proof),
      );
      assert.strictEqual(proof.intentFingerprint, derived.reservation.fingerprint);
      assert.strictEqual(proof.materialHash, derived.reservation.materialHash);
      assert.strictEqual(proof.launchHash, derived.reservation.launchHash);
      assert.strictEqual(proof.commandId, derived.messageCommand.commandId);
      assert.strictEqual(proof.threadId, derived.messageCommand.threadId);
      assert.strictEqual(proof.normalizationHash, derived.normalizationHash);
      assert.strictEqual(
        (yield* Effect.flip(
          verifier.prepareInTransaction(derived.messageCommand, {
            intentId: input.intentId,
            phase: "message",
          }),
        )).reason,
        "integrity",
      );
      assert.throws(() => {
        (result as { proofJson: string }).proofJson = "changed";
      });
      assert.isFalse(Object.isFrozen(command));
      assert.isFalse(Object.isFrozen(hint));
      assert.strictEqual(
        (yield* Effect.flip(
          sql.withTransaction(
            verifier.prepareInTransaction(command, { intentId: input.intentId, phase: "message" }),
          ),
        )).reason,
        "conflict",
      );
      assert.strictEqual(
        (yield* Effect.flip(verifier.prepareInTransaction({}, {}))).reason,
        "invalid",
      );
      const rolledBack = yield* Effect.result(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* verifier.prepareInTransaction(derived.claimCommand, {
              intentId: input.intentId,
              phase: "claim",
            });
            return yield* Effect.fail("rollback");
          }),
        ),
      );
      assert.strictEqual(rolledBack._tag, "Failure");
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_command_proofs`, []);
      yield* sql`DROP TABLE projection_projects`;
      assert.strictEqual(
        (yield* Effect.flip(
          sql.withTransaction(
            verifier.prepareInTransaction(derived.claimCommand, {
              intentId: input.intentId,
              phase: "claim",
            }),
          ),
        )).reason,
        "persistence",
      );
    }).pipe(Effect.provide(testLayer())),
);

it.effect("strictly validates proof candidates without altering raw project roots", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    yield* seedProject;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE projection_projects SET workspace_root = '/old ' WHERE project_id = 'project'`;
    const store = yield* Store.WorkflowRunStore;
    const input = basicInput();
    yield* store.reserveWorkflowNativeIntent(input);
    const derivation = yield* Derivation.WorkflowNativeLaunchDerivation;
    const derived = yield* sql.withTransaction(
      derivation.deriveInTransaction({ intentId: input.intentId }),
    );
    const verifier = yield* makeNativeCommandProofVerifier(
      sql,
      yield* ServerConfig,
      yield* Path.Path,
    );
    const result = yield* sql.withTransaction(
      verifier.prepareInTransaction(derived.claimCommand, {
        intentId: input.intentId,
        phase: "claim",
      }),
    );
    const proof = JSON.parse(result.proofJson);
    assert.strictEqual(proof.normalization.project.workspaceRoot, "/old ");
    const change = (fn: (value: typeof proof) => void) => {
      const v = structuredClone(proof);
      fn(v);
      return v;
    };
    const invalid = [
      change((v) => {
        v.extra = true;
      }),
      change((v) => {
        v.intentId = "";
      }),
      change((v) => {
        v.commandId = " command ";
      }),
      change((v) => {
        v.materialHash = "BAD";
      }),
      change((v) => {
        v.phase = "message";
      }),
      change((v) => {
        v.commandType = "thread.metadata.update";
      }),
      ...["workspaceRoot", "scratchRoot"].map((field) =>
        change((v) => {
          if (field === "workspaceRoot") v.normalization.project.workspaceRoot = "";
          else v.normalization.scratchRoot = "";
          v.normalizationHash = WorkflowCanonicalHash.tagged(
            "t3.workflow-native-normalization.v1",
            v.normalization,
          );
        }),
      ),
      change((v) => {
        v.normalizationHash = hash;
      }),
      change((v) => {
        v.normalization.workspace.effective = { type: "root", branch: "different" };
        v.normalizationHash = WorkflowCanonicalHash.tagged(
          "t3.workflow-native-normalization.v1",
          v.normalization,
        );
      }),
      change((v) => {
        v.normalization.reuse.claimKind = "metadata-update";
        v.normalizationHash = WorkflowCanonicalHash.tagged(
          "t3.workflow-native-normalization.v1",
          v.normalization,
        );
      }),
      change((v) => {
        v.normalization.extra = true;
      }),
      change((v) => {
        v.extra = undefined;
      }),
      change((v) => {
        v.extra = NaN;
      }),
      change((v) => {
        Object.defineProperty(v, "extra", {
          enumerable: true,
          get() {
            throw new Error("getter invoked");
          },
        });
      }),
    ];
    for (const candidate of invalid)
      assert.throws(() => encodeNativeCommandProof(candidate), NativeCommandProofError);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_command_proofs`, []);
  }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "uses only the factory client when independent transactions and ambient services overlap",
  () =>
    Effect.gen(function* () {
      yield* setupAdmission;
      yield* seedProject;
      const sqlA = yield* SqlClient.SqlClient;
      const storeA = yield* Store.WorkflowRunStore;
      const derivationA = yield* Derivation.WorkflowNativeLaunchDerivation;
      const inputA = basicInput();
      yield* storeA.reserveWorkflowNativeIntent(inputA);
      const derivedA = yield* sqlA.withTransaction(
        derivationA.deriveInTransaction({ intentId: inputA.intentId }),
      );
      const verifier = yield* makeNativeCommandProofVerifier(
        sqlA,
        yield* ServerConfig,
        yield* Path.Path,
      );
      yield* Effect.gen(function* () {
        yield* setupAdmission;
        yield* seedProject;
        const sqlB = yield* SqlClient.SqlClient;
        const storeB = yield* Store.WorkflowRunStore;
        assert.notStrictEqual(sqlA, sqlB);
        const inputB = basicInput();
        yield* storeB.reserveWorkflowNativeIntent({
          ...inputB,
          launch: { ...inputB.launch, title: "Foreign" },
        });
        yield* sqlB`UPDATE projection_projects SET workspace_root = '/foreign' WHERE project_id = 'project'`;
        assert.strictEqual(
          (yield* Effect.flip(
            sqlB.withTransaction(
              verifier.readStoredInTransaction(derivedA.claimCommand, {
                intentId: inputA.intentId,
                phase: "claim",
              }),
            ),
          )).reason,
          "integrity",
        );
        const foreign = yield* sqlB.withTransaction(
          (yield* Derivation.WorkflowNativeLaunchDerivation).deriveInTransaction({
            intentId: inputB.intentId,
          }),
        );
        assert.notStrictEqual(foreign.reservation.materialHash, derivedA.reservation.materialHash);
        assert.notStrictEqual(foreign.reservation.launchHash, derivedA.reservation.launchHash);
        const hint = { intentId: inputA.intentId, phase: "claim" };
        assert.strictEqual(
          (yield* Effect.flip(
            sqlB.withTransaction(verifier.prepareInTransaction(derivedA.claimCommand, hint)),
          )).reason,
          "integrity",
        );
        const candidate = yield* sqlA.withTransaction(
          sqlB.withTransaction(verifier.prepareInTransaction(derivedA.claimCommand, hint)),
        );
        const proof = JSON.parse(candidate.proofJson);
        assert.strictEqual(proof.normalization.project.workspaceRoot, "/old");
        assert.strictEqual(proof.materialHash, derivedA.reservation.materialHash);
        assert.strictEqual(proof.launchHash, derivedA.reservation.launchHash);
        assert.deepStrictEqual(yield* sqlB`SELECT * FROM workflow_native_command_proofs`, []);
        assert.deepStrictEqual(yield* sqlA`SELECT * FROM workflow_native_command_proofs`, []);
      }).pipe(Effect.provide(Layer.fresh(testLayer())));
    }).pipe(Effect.provide(testLayer())),
);

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

it.effect("creates candidates from genuine planning material", () =>
  Effect.gen(function* () {
    const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
    yield* seedProject;
    const store = yield* Store.WorkflowRunStore;
    yield* store.beginPlanning({
      session,
      now: facts.now,
      context,
      command: { commandId: "begin", receiptId: "begin-receipt" },
    });
    const admission = admissionInput(binding, "proof");
    const input = planningReservation(admission);
    yield* store.reservePlanningNativeIntent(input);
    const sql = yield* SqlClient.SqlClient;
    const derived = yield* sql.withTransaction(
      (yield* Derivation.WorkflowNativeLaunchDerivation).deriveInTransaction({
        intentId: input.intentId,
      }),
    );
    assert.strictEqual(derived.reservation.owner.kind, "planning_session");
    const verifier = yield* makeNativeCommandProofVerifier(
      sql,
      yield* ServerConfig,
      yield* Path.Path,
    );
    for (const [phase, command] of [
      ["claim", derived.claimCommand],
      ["message", derived.messageCommand],
    ] as const) {
      const candidate = yield* sql.withTransaction(
        verifier.prepareInTransaction(command, { intentId: input.intentId, phase }),
      );
      const proof = JSON.parse(candidate.proofJson);
      assert.strictEqual(proof.materialHash, derived.reservation.materialHash);
      assert.strictEqual(proof.launchHash, derived.reservation.launchHash);
      assert.strictEqual(proof.phase, phase);
    }
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_command_proofs`, []);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* seedNativeProofs(sql, verifier, derived, "claim");
        yield* seedNativeProofs(sql, verifier, derived, "message");
        const result = yield* verifier.readStoredInTransaction(derived.messageCommand, {
          intentId: input.intentId,
          phase: "message",
        });
        assert.strictEqual(result.kind, "verified");
        if (result.kind === "verified") assert.strictEqual(result.actualT3RunId, "actual-t3-run");
      }),
    );
  }).pipe(Effect.provide(testLayer())),
);

it.effect.each([false, true])(
  "verifies stored workflow protocol with reuse=%s and rejects partial histories",
  (reuse) =>
    Effect.gen(function* () {
      yield* setupAdmission;
      yield* seedProject;
      const sql = yield* SqlClient.SqlClient;
      const store = yield* Store.WorkflowRunStore;
      const input = basicInput();
      yield* store.reserveWorkflowNativeIntent({
        ...input,
        launch: { ...input.launch, reuseExistingThread: reuse },
      });
      const derived = yield* sql.withTransaction(
        (yield* Derivation.WorkflowNativeLaunchDerivation).deriveInTransaction({
          intentId: input.intentId,
        }),
      );
      const verifier = yield* makeNativeCommandProofVerifier(
        sql,
        yield* ServerConfig,
        yield* Path.Path,
      );
      const claimHint = { intentId: input.intentId, phase: "claim" } as const;
      const messageHint = { intentId: input.intentId, phase: "message" } as const;
      const readClaim = () => verifier.readStoredInTransaction(derived.claimCommand, claimHint);
      const readMessage = () =>
        verifier.readStoredInTransaction(derived.messageCommand, messageHint);
      assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
      yield* sql`PRAGMA foreign_keys = OFF`;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          assert.deepStrictEqual(yield* readClaim(), { kind: "absent" });
          assert.strictEqual((yield* Effect.flip(readMessage())).reason, "integrity");
          yield* sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type) VALUES (${derived.messageCommand.commandId},'thread',${input.threadId},${fixtureTime},3,'accepted',NULL,'message.dispatch')`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${derived.messageCommand.commandId}`;
          yield* sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type) VALUES (${derived.claimCommand.commandId},'thread',${input.threadId},${fixtureTime},1,'accepted',NULL,${derived.claimCommand.type})`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${derived.claimCommand.commandId}`;
          const claimFixture = yield* seedNativeProofs(sql, verifier, derived, "claim");
          assert.strictEqual((yield* readClaim()).kind, "verified");
          assert.deepStrictEqual(yield* readMessage(), { kind: "absent" });
          yield* seedNativeProofs(sql, verifier, derived, "message");
          const verified = yield* readMessage();
          assert.strictEqual(verified.kind, "verified");
          if (verified.kind === "verified")
            assert.strictEqual(verified.actualT3RunId, "actual-t3-run");
          yield* sql`UPDATE orchestration_events SET sequence = 4 WHERE event_id = 'claim-event'`;
          yield* sql`UPDATE orchestration_command_receipts SET result_sequence = 4 WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readMessage())).reason, "integrity");
          yield* sql`UPDATE orchestration_events SET sequence = 1 WHERE event_id = 'claim-event'`;
          yield* sql`UPDATE orchestration_command_receipts SET result_sequence = 1 WHERE command_id = ${derived.claimCommand.commandId}`;
          const command = structuredClone(derived.messageCommand);
          const hint = { ...messageHint };
          const owned = verifier.readStoredInTransaction(command, hint);
          (command as { threadId: string }).threadId = "changed";
          (hint as { intentId: string }).intentId = "changed";
          assert.strictEqual((yield* owned).kind, "verified");
          assert.strictEqual(
            (yield* Effect.flip(verifier.readStoredInTransaction(command, messageHint))).reason,
            "conflict",
          );
          assert.strictEqual(
            (yield* Effect.flip(verifier.readStoredInTransaction({}, {}))).reason,
            "invalid",
          );
          yield* sql`SAVEPOINT fixture_phase`;
          yield* sql`DROP TRIGGER workflow_native_command_proofs_delete_guard`;
          yield* sql`DROP TRIGGER workflow_native_command_proofs_update_guard`;
          yield* sql`DELETE FROM workflow_native_command_proofs WHERE command_id = ${derived.messageCommand.commandId}`;
          yield* sql`UPDATE workflow_native_command_proofs SET phase = 'message' WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          yield* sql`ROLLBACK TO fixture_phase`;
          yield* sql`RELEASE fixture_phase`;
          yield* sql`UPDATE projection_projects SET workspace_root = '/changed' WHERE project_id = 'project'`;
          assert.strictEqual((yield* Effect.flip(readMessage())).reason, "conflict");
          yield* sql`UPDATE projection_projects SET workspace_root = '/old' WHERE project_id = 'project'`;
          yield* sql`UPDATE orchestration_command_receipts SET status = 'rejected' WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readMessage())).reason, "integrity");
          yield* sql`UPDATE orchestration_command_receipts SET status = 'accepted' WHERE command_id = ${derived.claimCommand.commandId}`;
          yield* sql`UPDATE orchestration_command_receipts SET aggregate_kind = 'project' WHERE command_id = ${derived.messageCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "conflict");
          yield* sql`UPDATE orchestration_command_receipts SET aggregate_kind = 'thread' WHERE command_id = ${derived.messageCommand.commandId}`;
          const guards = yield* sql<{
            name: string;
            sql: string;
          }>`SELECT name, sql FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'workflow_native_command_proofs'`;
          yield* sql`DROP TRIGGER workflow_native_command_proofs_update_guard`;
          yield* sql`DROP TRIGGER workflow_native_command_proofs_delete_guard`;
          yield* sql`UPDATE workflow_native_command_proofs SET command_id = 'unexpected-id' WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          yield* sql`UPDATE workflow_native_command_proofs SET command_id = ${derived.claimCommand.commandId} WHERE command_id = 'unexpected-id'`;
          yield* sql`UPDATE workflow_native_command_proofs SET intent_id = 'other-intent' WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "conflict");
          yield* sql`UPDATE workflow_native_command_proofs SET intent_id = ${input.intentId} WHERE command_id = ${derived.claimCommand.commandId}`;
          yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          yield* sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type) VALUES (${derived.claimCommand.commandId},'thread',${input.threadId},${fixtureTime},1,'accepted',NULL,${derived.claimCommand.type})`;
          yield* sql`UPDATE workflow_native_command_proofs SET proof_hash = ${"b".repeat(64)} WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          yield* sql`UPDATE workflow_native_command_proofs SET proof_hash = ${claimFixture.proofHash} WHERE command_id = ${derived.claimCommand.commandId}`;
          const changedProof = (yield* decodeFixtureJson(claimFixture.proofJson)) as {
            materialHash: string;
            commandId: string;
            normalization: { project: { workspaceRoot: string } };
            normalizationHash: string;
          };
          changedProof.commandId = "unexpected-proof-command";
          const changedId = encodeNativeCommandProof(changedProof);
          yield* sql`UPDATE workflow_native_command_proofs SET proof_json = ${changedId.proofJson}, proof_hash = ${changedId.proofHash} WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          changedProof.commandId = derived.claimCommand.commandId;
          changedProof.materialHash = "b".repeat(64);
          const changedMaterial = encodeNativeCommandProof(changedProof);
          yield* sql`UPDATE workflow_native_command_proofs SET proof_json = ${changedMaterial.proofJson}, proof_hash = ${changedMaterial.proofHash} WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          changedProof.materialHash = derived.reservation.materialHash;
          changedProof.normalization.project.workspaceRoot = "/different";
          changedProof.normalizationHash = WorkflowCanonicalHash.tagged(
            "t3.workflow-native-normalization.v1",
            changedProof.normalization,
          );
          const changedNormalization = encodeNativeCommandProof(changedProof);
          yield* sql`UPDATE workflow_native_command_proofs SET proof_json = ${changedNormalization.proofJson}, proof_hash = ${changedNormalization.proofHash} WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "conflict");
          yield* sql`UPDATE workflow_native_command_proofs SET proof_json = ${claimFixture.proofJson}, proof_hash = ${claimFixture.proofHash} WHERE command_id = ${derived.claimCommand.commandId}`;
          yield* sql`DELETE FROM workflow_native_command_proofs WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${derived.claimCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          yield* sql`DELETE FROM orchestration_command_receipts WHERE command_id = ${derived.messageCommand.commandId}`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "integrity");
          for (const guard of guards.filter(
            (g) => g.name !== "workflow_native_command_proofs_insert_guard",
          ))
            yield* sql.unsafe(guard.sql);
          yield* sql`ALTER TABLE workflow_native_command_proofs RENAME TO fixture_hidden_proofs`;
          assert.strictEqual((yield* Effect.flip(readClaim())).reason, "persistence");
          yield* sql`ALTER TABLE fixture_hidden_proofs RENAME TO workflow_native_command_proofs`;
        }),
      );
      yield* sql`PRAGMA foreign_keys = ON`;
    }).pipe(Effect.provide(testLayer())),
);
