// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodePathService from "@effect/platform-node/NodePath";
import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  MessageId,
  OrchestrationV2Command,
  ProviderInstanceId,
  ProviderDriverKind,
  CommandId,
  ThreadId,
  RunId,
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
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as TestClock from "effect/testing/TestClock";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as ConfigModule from "../config.ts";
import * as SessionStore from "../auth/SessionStore.ts";
import * as ServerSecretStore from "../auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as ExecutionAuthority from "./WorkflowExecutionAuthority.ts";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as Statement from "effect/sql/Statement";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EffectOutbox from "../orchestration-v2/EffectOutbox.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ApplicationEvents from "../persistence/OrchestrationEventStore.ts";
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
import * as NativeLaunch from "../orchestration-v2/WorkflowNativeLaunchService.ts";
import * as PubSub from "effect/PubSub";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as ProviderReplayHarness from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { DispatchModeLimit } from "../orchestration-v2/DispatchModeLimit.ts";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
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
const encodeFixtureThread = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2AppThreadJson),
);
const encodeFixtureRun = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2RunJson));
const encodeFixtureMessage = Schema.encodeEffect(
  Schema.fromJsonString(OrchestrationV2ConversationMessageJson),
);
const decodeFixtureEvent = Schema.decodeUnknownEffect(OrchestrationV2DomainEventJson);
const encodeFixtureEvent = Schema.encodeEffect(OrchestrationV2DomainEventJson);
const decodeFixtureJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeFixtureEffect = Schema.decodeUnknownSync(EffectOutbox.OrchestrationEffectRequestV2);
const decodeOrchestratorFixtureCommand = Schema.decodeUnknownSync(OrchestrationV2Command);
const isOrchestratorDispatchError = Schema.is(Orchestrator.OrchestratorDispatchError);

const nativeDecision = Effect.fn("test.nativeDecision")(function* (
  derived: DerivedFixture,
  phase: "claim" | "message",
) {
  const launch = derived.reservation.material.launch;
  const command = phase === "claim" ? derived.claimCommand : derived.messageCommand;
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
  const payloads =
    phase === "claim"
      ? [
          {
            id: "claim-event",
            type: command.type === "thread.create" ? "thread.created" : "thread.metadata-updated",
            payload: thread,
          },
        ]
      : [
          { id: "run-event", type: "run.created", payload: nativeRun },
          { id: "message-event", type: "message.updated", payload: message },
        ];
  const events = yield* Effect.forEach(payloads, (event) =>
    decodeFixtureEvent({ ...event, threadId: launch.threadId, occurredAt: fixtureTime }),
  );
  return {
    command,
    hint: { intentId: derived.reservation.intentId, phase },
    acceptedAt: DateTime.makeUnsafe(fixtureTime),
    events,
    effects: [] as Array<EffectOutbox.PendingOrchestrationEffectV2>,
  };
});

const seedNativeProofs = Effect.fn("test.seedNativeProofs")(function* (
  sql: SqlClient.SqlClient,
  verifier: ProofVerifier,
  derived: DerivedFixture,
  phase: "claim" | "message",
) {
  const launch = derived.reservation.material.launch;
  const decision = yield* nativeDecision(derived, phase);
  const { command, hint, events } = decision;
  const proof = yield* verifier.prepareInTransaction(command, hint);
  const sequence = phase === "claim" ? 1 : 3;
  yield* sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type) VALUES (${command.commandId},'thread',${launch.threadId},${fixtureTime},${sequence},'accepted',NULL,${command.type})`;
  yield* sql`INSERT INTO workflow_native_command_proofs (command_id,intent_id,phase,proof_json,proof_hash) VALUES (${command.commandId},${hint.intentId},${phase},${proof.proofJson},${proof.proofHash})`;
  for (const [index, event] of events.entries()) {
    if (event.type === "thread.created" || event.type === "thread.metadata-updated") {
      const threadJson = yield* encodeFixtureThread(event.payload);
      yield* sql`INSERT INTO orchestration_v2_projection_threads (thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES (${launch.threadId},${launch.projectId},'Thread','provider','full-access','default',${fixtureTime},${fixtureTime},${threadJson})`;
    } else if (event.type === "run.created") {
      const runJson = yield* encodeFixtureRun(event.payload);
      yield* sql`INSERT INTO orchestration_v2_projection_runs (run_id,thread_id,ordinal,provider,status,requested_at,payload_json) VALUES ('actual-t3-run',${launch.threadId},7,'provider','preparing',${fixtureTime},${runJson})`;
    } else if (event.type === "message.updated") {
      const messageJson = yield* encodeFixtureMessage(event.payload);
      yield* sql`INSERT INTO orchestration_v2_projection_messages (message_id,thread_id,run_id,role,streaming,created_at,updated_at,payload_json) VALUES (${launch.initialMessage.messageId},${launch.threadId},'actual-t3-run','user',0,${fixtureTime},${fixtureTime},${messageJson})`;
    }
    const encoded = yield* encodeFixtureEvent(event);
    const eventSequence = phase === "claim" ? 1 : index + 2;
    yield* sql`INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES (${eventSequence},${event.id},'thread',${launch.threadId},${eventSequence},${event.type},${fixtureTime},${command.commandId},'client',${json(encoded.payload)},'{}',2)`;
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

const nativeWriter = Effect.gen(function* () {
  const captured = yield* EventSink.makeCapturedNativeEventInfrastructure(
    yield* SqlClient.SqlClient,
    yield* ServerConfig,
    yield* Path.Path,
  );
  const sink = Context.get(captured, EventSink.EventSinkV2);
  assert.isDefined(sink.commitNativeCommand);
  assert.isDefined(sink.replayNativeCommand);
  return { captured, sink, commit: sink.commitNativeCommand!, replay: sink.replayNativeCommand! };
});

const workflowDecision = (reuse = false) =>
  Effect.gen(function* () {
    yield* setupAdmission;
    yield* seedProject;
    const input = basicInput();
    yield* (yield* Store.WorkflowRunStore).reserveWorkflowNativeIntent({
      ...input,
      launch: { ...input.launch, reuseExistingThread: reuse },
    });
    const sql = yield* SqlClient.SqlClient;
    const derived = yield* sql.withTransaction(
      (yield* Derivation.WorkflowNativeLaunchDerivation).deriveInTransaction({
        intentId: input.intentId,
      }),
    );
    return {
      derived,
      claim: yield* nativeDecision(derived, "claim"),
      message: yield* nativeDecision(derived, "message"),
    };
  });

it.effect.each([false, true])(
  "atomically commits and hydrates native workflow phases with reuse=%s",
  (reuse) =>
    Effect.gen(function* () {
      const { claim, message } = yield* workflowDecision(reuse);
      const { captured, commit, replay } = yield* nativeWriter;
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(yield* replay(claim), { kind: "absent" });
      assert.instanceOf(yield* Effect.flip(commit(message)), EventSink.EventSinkWriteError);
      const claimed = yield* commit(claim);
      assert.isTrue(claimed.committed);
      const partial = yield* replay(message);
      assert.deepStrictEqual(partial, { kind: "absent" });
      const dispatched = yield* commit(message);
      assert.isTrue(dispatched.committed);
      assert.strictEqual(dispatched.storedEvents.length, 2);
      assert.isTrue(DateTime.isDateTime(dispatched.receipt.acceptedAt));
      const replayed = yield* replay(message);
      assert.deepStrictEqual(replayed, {
        kind: "verified",
        receipt: dispatched.receipt,
        storedEvents: dispatched.storedEvents,
      });
      const retry = yield* commit({
        ...message,
        effects: [
          {
            id: "unused-replay-effect",
            commandId: message.command.commandId,
            threadId: message.command.threadId,
            request: { type: "terminal.cleanup" },
          },
        ],
      });
      assert.deepStrictEqual(retry, { ...dispatched, committed: false });
      assert.lengthOf(yield* sql`SELECT * FROM workflow_native_command_proofs`, 2);
      assert.deepStrictEqual(yield* sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      const projections = Context.get(captured, ProjectionStore.ProjectionStoreV2);
      assert.strictEqual(
        (yield* projections.getThreadProjection(message.command.threadId)).runs[0]?.status,
        "preparing",
      );
      assert.deepStrictEqual(
        yield* Effect.flip(
          replay({ command: { ...message.command, text: "different" }, hint: message.hint }),
        ),
        new EventSink.EventSinkWriteError({
          commandId: message.command.commandId,
          eventCount: 0,
          cause: new NativeCommandProofError({ reason: "integrity" }),
        }),
      );
    }).pipe(Effect.provide(testLayer())),
);

it.effect("commits native proof from genuine planning reservation material", () =>
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
    const input = planningReservation(admissionInput(binding, "native-writer"));
    yield* store.reservePlanningNativeIntent(input);
    const sql = yield* SqlClient.SqlClient;
    const derived = yield* sql.withTransaction(
      (yield* Derivation.WorkflowNativeLaunchDerivation).deriveInTransaction({
        intentId: input.intentId,
      }),
    );
    const { commit, replay } = yield* nativeWriter;
    for (const phase of ["claim", "message"] as const) {
      const plan = yield* nativeDecision(derived, phase);
      const result = yield* commit(plan);
      assert.isTrue(result.committed);
      assert.deepStrictEqual(yield* replay(plan), {
        kind: "verified",
        receipt: result.receipt,
        storedEvents: result.storedEvents,
      });
    }
    assert.deepStrictEqual(
      yield* sql`SELECT phase FROM workflow_native_command_proofs ORDER BY phase`,
      [{ phase: "claim" }, { phase: "message" }],
    );
    assert.strictEqual((yield* store.getPlanning({ sessionId: session.id })).consumed.passes, 1);
  }).pipe(Effect.provide(testLayer())),
);

const nativePublication = (
  captured: Effect.Success<typeof nativeWriter>["captured"],
  afterSequence = 0,
) =>
  Effect.gen(function* () {
    const ready = yield* Deferred.make<void>();
    const observeReplay: Statement.Transformer = (statement) => {
      const [query] = statement.compile();
      return /FROM orchestration_events/.test(query) && /WHERE sequence >/.test(query)
        ? Deferred.succeed(ready, undefined).pipe(Effect.as(statement))
        : Effect.succeed(statement);
    };
    const publication = yield* Context.get(captured, ApplicationEvents.OrchestrationEventStore)
      .streamApplicationEvents({ afterSequence })
      .pipe(
        Stream.take(1),
        Stream.runCollect,
        Effect.provideService(Statement.CurrentTransformer, observeReplay),
        Effect.forkScoped({ startImmediately: true }),
      );
    yield* Deferred.await(ready);
    return publication;
  });

const nativeCounts = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql`SELECT
    (SELECT COUNT(*) FROM workflow_native_command_proofs) AS proofs,
    (SELECT COUNT(*) FROM orchestration_command_receipts) AS receipts,
    (SELECT COUNT(*) FROM orchestration_events) AS events,
    (SELECT COUNT(*) FROM orchestration_v2_projection_threads) AS threads,
    (SELECT COUNT(*) FROM orchestration_v2_projection_runs) AS runs,
    (SELECT COUNT(*) FROM orchestration_v2_projection_messages) AS messages,
    (SELECT COUNT(*) FROM orchestration_v2_effect_outbox) AS effects`;
});

it.effect("rolls back native proof failure without publication, wakeup or cancellation", () =>
  Effect.gen(function* () {
    const { claim, message } = yield* workflowDecision();
    const { captured, commit, replay } = yield* nativeWriter;
    const sql = yield* SqlClient.SqlClient;
    const outbox = Context.get(captured, EffectOutbox.EffectOutboxV2);
    yield* outbox.enqueue([
      {
        id: "pending-cleanup",
        commandId: claim.command.commandId,
        threadId: claim.command.threadId,
        request: { type: "terminal.cleanup" },
      },
    ]);
    const plan = {
      ...claim,
      effects: [
        {
          id: "native-effect",
          commandId: claim.command.commandId,
          threadId: claim.command.threadId,
          request: { type: "thread-title.generate", kind: { type: "regenerate" } },
        } satisfies EffectOutbox.PendingOrchestrationEffectV2,
      ],
      cancelUnsettledEffects: {
        effectTypes: ["terminal.cleanup" as const],
        reason: "native cancellation",
      },
    };
    const before = yield* nativeCounts;
    const publication = yield* nativePublication(captured);
    const wake = yield* outbox.awaitAvailable.pipe(Effect.forkScoped({ startImmediately: true }));
    const cancellation = yield* outbox
      .awaitCancellation("pending-cleanup")
      .pipe(Effect.forkScoped({ startImmediately: true }));
    yield* sql`CREATE TRIGGER injected_proof_failure BEFORE INSERT ON workflow_native_command_proofs BEGIN SELECT RAISE(ABORT, 'injected proof failure'); END`;
    assert.instanceOf(yield* Effect.flip(commit(plan)), EventSink.EventSinkWriteError);
    assert.deepStrictEqual(yield* nativeCounts, before);
    assert.strictEqual(Option.getOrThrow(yield* outbox.get("pending-cleanup")).status, "pending");
    assert.isUndefined(publication.pollUnsafe());
    assert.isUndefined(wake.pollUnsafe());
    assert.isUndefined(cancellation.pollUnsafe());
    yield* sql`DROP TRIGGER injected_proof_failure`;
    const result = yield* commit(plan);
    assert.isTrue(result.committed);
    assert.strictEqual(result.cancelledEffectCount, 1);
    assert.strictEqual(
      (yield* Fiber.join(publication))[0]?.sequence,
      result.receipt.resultSequence,
    );
    yield* Fiber.join(wake);
    yield* Fiber.join(cancellation);
    yield* outbox.awaitAvailable;
    yield* outbox.clearCancellation("pending-cleanup");
    const noCancellation = yield* outbox
      .awaitCancellation("pending-cleanup")
      .pipe(Effect.forkScoped({ startImmediately: true }));
    const noWake = yield* outbox.awaitAvailable.pipe(Effect.forkScoped({ startImmediately: true }));
    const nextPublication = yield* nativePublication(captured, result.receipt.resultSequence);
    assert.deepStrictEqual(yield* replay(claim), {
      kind: "verified",
      receipt: result.receipt,
      storedEvents: result.storedEvents,
    });
    assert.isFalse((yield* commit(plan)).committed);
    assert.isUndefined(noCancellation.pollUnsafe());
    assert.isUndefined(noWake.pollUnsafe());
    assert.isUndefined(nextPublication.pollUnsafe());
    const dispatched = yield* commit(message);
    assert.strictEqual(
      (yield* Fiber.join(nextPublication))[0]?.sequence,
      dispatched.storedEvents[0]?.sequence,
    );
    assert.lengthOf(
      yield* Context.get(captured, EventStore.EventStoreV2)
        .readByCommandId({ commandId: message.command.commandId })
        .pipe(Stream.runCollect),
      2,
    );
  }).pipe(Effect.provide(testLayer())),
);

it.effect("owns native command, hint and complete decision data before execution", () =>
  Effect.gen(function* () {
    const { claim } = yield* workflowDecision();
    const { captured, commit, replay } = yield* nativeWriter;
    const outbox = Context.get(captured, EffectOutbox.EffectOutboxV2);
    yield* outbox.enqueue([
      {
        id: "old-effect",
        commandId: claim.command.commandId,
        threadId: claim.command.threadId,
        request: { type: "terminal.cleanup" },
      },
    ]);
    const actual = { ...claim.command };
    const hint = { ...claim.hint };
    const effect: EffectOutbox.PendingOrchestrationEffectV2 = {
      id: "owned-effect",
      commandId: claim.command.commandId,
      threadId: claim.command.threadId,
      request: { type: "thread-title.generate", kind: { type: "regenerate" } },
      availableAt: DateTime.makeUnsafe(fixtureTime),
    };
    const cancelUnsettledEffects = {
      effectTypes: ["terminal.cleanup" as const],
      reason: "original reason",
    };
    const queued = commit({
      ...claim,
      command: actual,
      hint,
      effects: [effect],
      cancelUnsettledEffects,
    });
    const replayQueued = replay({ command: actual, hint });
    Object.assign(actual, { commandId: "mutated-command", threadId: "mutated-thread" });
    Object.assign(hint, { intentId: "mutated-intent", phase: "message" });
    Object.assign(claim.events[0]!.payload, { title: "mutated title" });
    claim.events.length = 0;
    Object.assign(effect, {
      id: "mutated-effect",
      availableAt: DateTime.makeUnsafe("2026-12-01T00:00:00Z"),
    });
    Object.assign(effect.request, { type: "provider-turn.start", runId: "different-run" });
    cancelUnsettledEffects.effectTypes.length = 0;
    cancelUnsettledEffects.reason = "mutated reason";
    const result = yield* queued;
    assert.isTrue(result.committed);
    assert.strictEqual(result.receipt.commandId, "launch-attempt");
    const event = result.storedEvents[0]?.event;
    assert.strictEqual(event?.type, "thread.created");
    if (event?.type === "thread.created") assert.strictEqual(event.payload.title, "Thread");
    const storedEffect = Option.getOrThrow(yield* outbox.get("owned-effect"));
    assert.strictEqual(storedEffect.request.type, "thread-title.generate");
    assert.strictEqual(storedEffect.availableAt, fixtureTime);
    assert.strictEqual(
      Option.getOrThrow(yield* outbox.get("old-effect")).lastError,
      "original reason",
    );
    assert.deepStrictEqual(yield* replayQueued, {
      kind: "verified",
      receipt: result.receipt,
      storedEvents: result.storedEvents,
    });
    assert.deepStrictEqual(
      yield* (yield* SqlClient.SqlClient)`SELECT command_id FROM workflow_native_command_proofs`,
      [{ command_id: "launch-attempt" }],
    );
  }).pipe(Effect.provide(testLayer())),
);

it.effect("denies native conflicts, invalid decisions and inherited transactions", () =>
  Effect.gen(function* () {
    const { claim } = yield* workflowDecision();
    const { sink, commit, replay, captured } = yield* nativeWriter;
    const sql = yield* SqlClient.SqlClient;
    const original = yield* nativeCounts;
    for (const action of [
      sql.withTransaction(commit(claim)).pipe(Effect.asVoid),
      sql.withTransaction(replay(claim)).pipe(Effect.asVoid),
      sink.withProviderBatch(commit(claim)).pipe(Effect.asVoid),
      sink.withProviderBatch(replay(claim)).pipe(Effect.asVoid),
    ]) {
      assert.instanceOf(yield* Effect.flip(action), EventSink.EventSinkWriteError);
      assert.deepStrictEqual(yield* nativeCounts, original);
    }
    const cleanup: EffectOutbox.PendingOrchestrationEffectV2 = {
      id: "collision",
      commandId: claim.command.commandId,
      threadId: claim.command.threadId,
      request: { type: "terminal.cleanup" },
    };
    const plans = [
      { ...claim, events: [] },
      { ...claim, events: [], effects: [cleanup] },
      { ...claim, effects: [cleanup, cleanup] },
      {
        ...claim,
        events: claim.events.map((event) => ({
          ...event,
          threadId: ThreadId.make("other-thread"),
        })),
      },
      {
        ...claim,
        effects: [
          {
            ...cleanup,
            request: decodeFixtureEffect({
              type: "provider-turn.restart",
              providerSessionId: "session",
              providerThreadId: "thread",
              providerTurnId: "turn",
              interruptedAttemptId: "attempt",
              runId: "run",
            }),
          },
        ],
      },
      { ...claim, effects: [{ ...cleanup, commandId: CommandId.make("other-command") }] },
      { ...claim, effects: [{ ...cleanup, threadId: ThreadId.make("other-thread") }] },
      {
        ...claim,
        effects: [
          {
            ...cleanup,
            request: { type: "provider-turn.start" as const, runId: RunId.make("run") },
          },
        ],
      },
      {
        ...claim,
        effects: [
          {
            ...cleanup,
            request: { type: "provider-runtime.continue" as const, sourceRunId: RunId.make("run") },
          },
        ],
      },
      {
        ...claim,
        events: claim.events.flatMap((event) =>
          event.type === "thread.created"
            ? [{ ...event, type: "thread.metadata-updated" as const }]
            : [],
        ),
      },
    ];
    for (const plan of plans) {
      assert.instanceOf(yield* Effect.flip(commit(plan)), EventSink.EventSinkWriteError);
      assert.deepStrictEqual(yield* nativeCounts, original);
    }
    const outbox = Context.get(captured, EffectOutbox.EffectOutboxV2);
    yield* outbox.enqueue([cleanup]);
    const collisionCounts = yield* nativeCounts;
    assert.instanceOf(
      yield* Effect.flip(commit({ ...claim, effects: [cleanup] })),
      EventSink.EventSinkWriteError,
    );
    assert.deepStrictEqual(yield* nativeCounts, collisionCounts);
    const ordinary = yield* sink.commitCommand({
      commandId: claim.command.commandId,
      threadId: claim.command.threadId,
      commandType: claim.command.type,
      acceptedAt: claim.acceptedAt,
      events: claim.events,
      effects: [],
    });
    assert.isTrue(ordinary.committed);
    assert.instanceOf(yield* Effect.flip(commit(claim)), EventSink.EventSinkWriteError);
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_native_command_proofs`, []);
  }).pipe(Effect.provide(testLayer())),
);

it.effect.each([false, true])(
  "validates native insert-loser protocol with stored proof=%s",
  (complete) =>
    Effect.gen(function* () {
      const { derived, claim } = yield* workflowDecision();
      const { captured, commit } = yield* nativeWriter;
      const sql = yield* SqlClient.SqlClient;
      const verifier = yield* makeNativeCommandProofVerifier(
        sql,
        yield* ServerConfig,
        yield* Path.Path,
      );
      let inserted = false;
      const beforeReserve: Statement.Transformer = (statement) => {
        const [query] = statement.compile();
        if (
          !inserted &&
          /INSERT INTO orchestration_command_receipts/.test(query) &&
          /ON CONFLICT/.test(query)
        ) {
          inserted = true;
          const competitor = complete
            ? seedNativeProofs(sql, verifier, derived, "claim").pipe(Effect.asVoid)
            : sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type) VALUES (${claim.command.commandId},'thread',${claim.command.threadId},${fixtureTime},1,'accepted',NULL,${claim.command.type})`.pipe(
                Effect.asVoid,
              );
          return competitor.pipe(Effect.as(statement), Effect.orDie);
        }
        return Effect.succeed(statement);
      };
      const publication = yield* nativePublication(captured);
      const wake = yield* Context.get(captured, EffectOutbox.EffectOutboxV2).awaitAvailable.pipe(
        Effect.forkScoped({ startImmediately: true }),
      );
      const action = commit(claim).pipe(
        Effect.provideService(Statement.CurrentTransformer, beforeReserve),
      );
      if (complete) {
        const result = yield* action;
        assert.isFalse(result.committed);
        assert.lengthOf(result.storedEvents, 1);
        assert.isTrue(DateTime.isDateTime(result.storedEvents[0]!.event.occurredAt));
        assert.lengthOf(yield* sql`SELECT * FROM workflow_native_command_proofs`, 1);
      } else {
        assert.instanceOf(yield* Effect.flip(action), EventSink.EventSinkWriteError);
        assert.deepStrictEqual(yield* nativeCounts, [
          { proofs: 0, receipts: 0, events: 0, threads: 0, runs: 0, messages: 0, effects: 0 },
        ]);
      }
      assert.isTrue(inserted);
      assert.isUndefined(publication.pollUnsafe());
      assert.isUndefined(wake.pollUnsafe());
    }).pipe(Effect.provide(testLayer())),
);

const proofOrchestrator = Effect.fn("test.proofOrchestrator")(function* (
  nativeCommandProofs = true,
  capabilitiesGate: Effect.Effect<void> = Effect.void,
  nativeWorkflowAuthority?: EventSink.NativeWorkflowAuthority,
  serverConfig?: ServerConfig["Service"],
) {
  const sql = yield* SqlClient.SqlClient;
  const adapter = {
    instanceId: ProviderInstanceId.make("provider"),
    driver: ProviderDriverKind.make("codex"),
    getCapabilities: () => capabilitiesGate.pipe(Effect.as(CodexProviderCapabilitiesV2)),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    openSession: () => Effect.die("Native proof fixture must never open a provider session"),
  } as ProviderAdapter.ProviderAdapterV2["Service"];
  return yield* Layer.build(
    Layer.fresh(
      ProviderReplayHarness.layerWithRegistry(
        { name: "native-command-proofs" },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
        {
          databaseLayer: Layer.succeed(SqlClient.SqlClient, sql),
          nativeCommandProofs,
          ...(serverConfig === undefined ? {} : { serverConfig }),
          ...(nativeWorkflowAuthority === undefined ? {} : { nativeWorkflowAuthority }),
          runEffectWorker: false,
          runContinuationWorker: false,
          recoverOnStartup: false,
        },
      ),
    ),
  );
});

it.effect("refuses a native Orchestrator hint when the sink lacks proof capabilities", () =>
  Effect.gen(function* () {
    const { claim } = yield* workflowDecision();
    const runtime = yield* proofOrchestrator(false);
    const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
    const before = yield* nativeCounts;
    assert.instanceOf(
      yield* Effect.flip(orchestrator.dispatch(claim.command, claim.hint)),
      Orchestrator.OrchestratorDispatchError,
    );
    assert.deepStrictEqual(yield* nativeCounts, before);
  }).pipe(Effect.provide(testLayer())),
);

const existingThreadCommand = (
  derived: DerivedFixture,
  commandId = "existing-thread",
  threadId = derived.claimCommand.threadId,
) => {
  const launch = derived.reservation.material.launch;
  return decodeOrchestratorFixtureCommand({
    type: "thread.create",
    commandId,
    threadId,
    projectId: launch.projectId,
    title: launch.title,
    modelSelection: launch.modelSelection,
    runtimeMode: launch.runtimeMode,
    interactionMode: launch.interactionMode,
    branch: "main",
    worktreePath: null,
    createdBy: launch.createdBy,
    creationSource: launch.creationSource,
  });
};

it.effect.each([false, true])(
  "dispatches real native workflow decisions and replays with reuse=%s",
  (reuse) =>
    Effect.gen(function* () {
      const { derived } = yield* workflowDecision(reuse);
      const runtime = yield* proofOrchestrator();
      const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
      const sink = Context.get(runtime, EventSink.EventSinkV2);
      const sql = yield* SqlClient.SqlClient;
      if (reuse) yield* orchestrator.dispatch(existingThreadCommand(derived));
      const claimHint = { intentId: derived.reservation.intentId, phase: "claim" as const };
      const messageHint = { ...claimHint, phase: "message" as const };
      const claimed = yield* orchestrator.dispatch(derived.claimCommand, claimHint);
      assert.strictEqual(
        claimed.storedEvents[0]?.event.type,
        reuse ? "thread.metadata-updated" : "thread.created",
      );
      const beforeMessage = yield* nativeCounts;
      assert.instanceOf(
        yield* Effect.flip(
          orchestrator.dispatch(derived.messageCommand, messageHint).pipe(
            Effect.provideService(DispatchModeLimit, {
              runtimeMode: "approval-required",
              interactionMode: "default",
            }),
          ),
        ),
        Orchestrator.OrchestratorThreadAboveModeLimitError,
      );
      assert.deepStrictEqual(yield* nativeCounts, beforeMessage);
      const dispatched = yield* orchestrator.dispatch(derived.messageCommand, messageHint);
      const created = dispatched.storedEvents.find(
        (stored) => stored.event.type === "run.created",
      )?.event;
      assert.strictEqual(created?.type, "run.created");
      if (created?.type === "run.created") {
        assert.strictEqual(created.payload.status, "preparing");
        assert.strictEqual(created.payload.ordinal, 1);
        assert.strictEqual(created.payload.userMessageId, derived.messageCommand.messageId);
        assert.notStrictEqual(created.payload.id, "actual-t3-run");
        assert.notStrictEqual(created.payload.id, derived.reservation.intentId);
      }
      assert.deepStrictEqual(yield* orchestrator.dispatch(derived.messageCommand, messageHint), {
        ...dispatched,
        replayed: true,
      });
      assert.deepStrictEqual(yield* orchestrator.dispatch(derived.claimCommand, claimHint), {
        ...claimed,
        replayed: true,
      });
      const replay = yield* sink.replayNativeCommand!({
        command: derived.messageCommand,
        hint: messageHint,
      });
      assert.strictEqual(replay.kind, "verified");
      if (replay.kind === "verified") {
        assert.deepStrictEqual(replay.storedEvents, dispatched.storedEvents);
        assert.isTrue(DateTime.isDateTime(replay.receipt.acceptedAt));
      }
      assert.deepStrictEqual(
        yield* sql`SELECT phase FROM workflow_native_command_proofs ORDER BY phase`,
        [{ phase: "claim" }, { phase: "message" }],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_type IN ('provider-turn.start','provider-turn.restart','provider-runtime.continue')`,
        [],
      );
    }).pipe(Effect.provide(testLayer())),
);

it.effect("dispatches both real native planning phases without charging a second pass", () =>
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
    const input = planningReservation(admissionInput(binding, "orchestrator"));
    yield* store.reservePlanningNativeIntent(input);
    const sql = yield* SqlClient.SqlClient;
    const derived = yield* sql.withTransaction(
      (yield* Derivation.WorkflowNativeLaunchDerivation).deriveInTransaction({
        intentId: input.intentId,
      }),
    );
    const runtime = yield* proofOrchestrator();
    const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
    for (const [phase, command] of [
      ["claim", derived.claimCommand],
      ["message", derived.messageCommand],
    ] as const) {
      const hint = { intentId: input.intentId, phase };
      const first = yield* orchestrator.dispatch(command, hint);
      assert.isAbove(first.storedEvents.length, 0);
      assert.deepStrictEqual(yield* orchestrator.dispatch(command, hint), {
        ...first,
        replayed: true,
      });
    }
    assert.strictEqual((yield* store.getPlanning({ sessionId: session.id })).consumed.passes, 1);
    assert.lengthOf(yield* sql`SELECT * FROM workflow_native_command_proofs`, 2);
  }).pipe(Effect.provide(testLayer())),
);

it.effect("owns hinted commands and hint before returning the dispatch Effect", () =>
  Effect.gen(function* () {
    const { derived } = yield* workflowDecision();
    const runtime = yield* proofOrchestrator();
    const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
    const actual = { ...derived.claimCommand };
    const hint = { intentId: derived.reservation.intentId, phase: "claim" as const };
    const queued = orchestrator.dispatch(actual, hint);
    Object.assign(actual, {
      commandId: "mutated-command",
      threadId: "mutated-thread",
      title: "Mutated",
    });
    Object.assign(hint, { intentId: "mutated-intent", phase: "message" });
    const result = yield* queued;
    const created = result.storedEvents[0]?.event;
    assert.strictEqual(created?.type, "thread.created");
    if (created?.type === "thread.created") {
      assert.strictEqual(created.threadId, derived.claimCommand.threadId);
      assert.strictEqual(created.payload.title, derived.reservation.material.launch.title);
    }
    const before = yield* nativeCounts;
    const invalid = orchestrator.dispatch(derived.claimCommand, {
      intentId: "bad",
      phase: "message",
    });
    assert.instanceOf(yield* Effect.flip(invalid), Orchestrator.OrchestratorDispatchError);
    assert.deepStrictEqual(yield* nativeCounts, before);
  }).pipe(Effect.provide(testLayer())),
);

it.effect("native decision refusal leaves no rejected receipt for an occupied reuse target", () =>
  Effect.gen(function* () {
    const { derived } = yield* workflowDecision(true);
    const runtime = yield* proofOrchestrator();
    const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
    const sql = yield* SqlClient.SqlClient;
    yield* orchestrator.dispatch(existingThreadCommand(derived));
    yield* orchestrator.dispatch({
      ...derived.messageCommand,
      commandId: CommandId.make("existing-message-command"),
      messageId: MessageId.make("existing-message"),
    });
    const before = yield* nativeCounts;
    assert.instanceOf(
      yield* Effect.flip(
        orchestrator.dispatch(derived.claimCommand, {
          intentId: derived.reservation.intentId,
          phase: "claim",
        }),
      ),
      Orchestrator.OrchestratorDispatchError,
    );
    assert.deepStrictEqual(yield* nativeCounts, before);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${derived.claimCommand.commandId}`,
      [],
    );
  }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "native dispatch refuses a global receipt committed by another thread during decision",
  () =>
    Effect.gen(function* () {
      const { derived } = yield* workflowDecision();
      const arrived = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let capabilityCalls = 0;
      const gate = Effect.gen(function* () {
        capabilityCalls += 1;
        yield* Deferred.succeed(arrived, undefined);
        yield* Deferred.await(release);
      });
      const runtime = yield* proofOrchestrator(true, gate);
      const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
      const sql = yield* SqlClient.SqlClient;
      yield* orchestrator.dispatch(derived.claimCommand, {
        intentId: derived.reservation.intentId,
        phase: "claim",
      });
      const hint = { intentId: derived.reservation.intentId, phase: "message" as const };
      const pending = yield* orchestrator
        .dispatch(derived.messageCommand, hint)
        .pipe(Effect.forkScoped({ startImmediately: true }));
      yield* Deferred.await(arrived);
      const otherThread = ThreadId.make("competing-thread");
      const competing = yield* orchestrator.dispatch(
        existingThreadCommand(derived, derived.messageCommand.commandId, otherThread),
      );
      const afterCompeting = yield* nativeCounts;
      yield* Deferred.succeed(release, undefined);
      const failure = yield* Effect.flip(Fiber.join(pending));
      assert.instanceOf(failure, Orchestrator.OrchestratorDispatchError);
      if (isOrchestratorDispatchError(failure))
        assert.instanceOf(failure.cause, EventSink.EventSinkWriteError);
      assert.deepStrictEqual(yield* nativeCounts, afterCompeting);
      assert.deepStrictEqual(
        yield* sql`SELECT aggregate_id, result_sequence, status FROM orchestration_command_receipts WHERE command_id = ${derived.messageCommand.commandId}`,
        [{ aggregate_id: otherThread, result_sequence: competing.sequence, status: "accepted" }],
      );
      assert.deepStrictEqual(yield* sql`SELECT phase FROM workflow_native_command_proofs`, [
        { phase: "claim" },
      ]);
      assert.deepStrictEqual(
        yield* sql`SELECT stream_id, event_type FROM orchestration_events WHERE command_id = ${derived.messageCommand.commandId}`,
        [{ stream_id: otherThread, event_type: "thread.created" }],
      );
      const callsBeforeReplay = capabilityCalls;
      assert.instanceOf(
        yield* Effect.flip(orchestrator.dispatch(derived.messageCommand, hint)),
        Orchestrator.OrchestratorDispatchError,
      );
      assert.strictEqual(capabilityCalls, callsBeforeReplay);
      assert.deepStrictEqual(yield* nativeCounts, afterCompeting);
    }).pipe(Effect.provide(testLayer())),
);

it.effect("real native message decision uses an owned copy of nested caller context", () =>
  Effect.gen(function* () {
    yield* setupAdmission;
    yield* seedProject;
    const input = basicInput();
    const launch = {
      ...input.launch,
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
    yield* (yield* Store.WorkflowRunStore).reserveWorkflowNativeIntent({ ...input, launch });
    const sql = yield* SqlClient.SqlClient;
    const derived = yield* sql.withTransaction(
      (yield* Derivation.WorkflowNativeLaunchDerivation).deriveInTransaction({
        intentId: input.intentId,
      }),
    );
    const runtime = yield* proofOrchestrator();
    const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
    yield* orchestrator.dispatch(derived.claimCommand, {
      intentId: input.intentId,
      phase: "claim",
    });
    const ownedContext = structuredClone(derived.messageCommand.context);
    if (ownedContext === undefined) throw new Error("fixture must have context");
    const actual = { ...derived.messageCommand, context: ownedContext };
    const hint = { intentId: input.intentId, phase: "message" as const };
    const pending = orchestrator.dispatch(actual, hint);
    const record = ownedContext.records[0];
    if (record === undefined || !("payload" in record))
      throw new Error("fixture must have opaque context");
    const payload = record.payload;
    if (typeof payload !== "object" || payload === null)
      throw new Error("fixture must have nested context");
    Object.assign(payload, { nested: ["mutated"] });
    actual.text = "mutated text";
    hint.intentId = "mutated-intent";
    const result = yield* pending;
    const message = result.storedEvents.find(
      (stored) => stored.event.type === "message.updated",
    )?.event;
    assert.strictEqual(message?.type, "message.updated");
    if (message?.type === "message.updated") {
      assert.strictEqual(message.payload.text, launch.initialMessage.text);
      assert.deepStrictEqual<unknown>(message.payload.context, launch.initialMessage.context);
    }
    const before = yield* nativeCounts;
    assert.deepStrictEqual(
      yield* orchestrator.dispatch(derived.messageCommand, {
        intentId: input.intentId,
        phase: "message",
      }),
      { ...result, replayed: true },
    );
    assert.deepStrictEqual(yield* nativeCounts, before);
  }).pipe(Effect.provide(testLayer())),
);

const controlledNativeFixture = Effect.fn("test.controlledNativeFixture")(function* (
  kind: "workflow" | "planning",
  reuse = false,
  claim = true,
  materialVersion: 1 | 2 = 2,
  generateTitle = false,
) {
  assert.isFunction(EventSink.makeCapturedAuthorizedNativeEventInfrastructure);
  yield* TestClock.setTime(DateTime.makeUnsafe(facts.now).epochMilliseconds);
  const binding = yield* seedAdmission.pipe(Effect.provide(Digest.layerWebCrypto));
  yield* seedProject;
  const sql = yield* SqlClient.SqlClient;
  const identity = { getEnvironmentId: Effect.succeed(EnvironmentId.make("environment")) };
  const base = Layer.mergeAll(
    Layer.succeed(SqlClient.SqlClient, sql),
    Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, identity),
    ManagedPolicy.layerTest(),
    ConfigModule.layerTest(process.cwd(), { prefix: "native-controlled-authority-" }),
  ).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));
  const authContext = yield* Layer.build(
    Layer.fresh(
      SessionStore.layer.pipe(
        Layer.provideMerge(ServerSecretStore.layer.pipe(Layer.provideMerge(base))),
      ),
    ),
  );
  const sessions = Context.get(authContext, SessionStore.SessionStore);
  const issued = yield* sessions.issue({
    subject: "human",
    method: "bearer-access-token",
    scopes: ["orchestration:read", "orchestration:operate"],
  });
  const reference = ExecutionAuthority.captureLocalRpcExecutionAuthority(
    yield* sessions.verify(issued.token),
    EnvironmentId.make("environment"),
    "verified-session",
  );
  if (!reference) throw new Error("reference missing");
  const authority = {
    identity,
    policy: Context.get(authContext, ManagedPolicy.ManagedPolicy),
    coordinator: { bootId: "controlled-boot", workerId: "controlled-worker" },
  };
  const config = yield* ProviderReplayHarness.makeReplayServerConfig(
    "native-controlled-authority",
  ).pipe(Effect.provide(NodeServices.layer));
  const runtime = yield* proofOrchestrator(true, Effect.void, authority, config);
  const store = Context.get(runtime, Store.WorkflowRunStore);
  const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
  const requestedInput =
    kind === "workflow"
      ? {
          ...basicInput(),
          materialVersion: 2 as const,
          authority: reference,
          launch: {
            ...basicInput().launch,
            reuseExistingThread: reuse,
            ...(generateTitle ? { generateTitle: true } : {}),
          },
        }
      : (() => {
          const value = planningReservation(admissionInput(binding, "controlled"));
          return {
            ...value,
            materialVersion: 2 as const,
            authority: reference,
            launch: {
              ...value.launch,
              reuseExistingThread: reuse,
              ...(generateTitle ? { generateTitle: true } : {}),
            },
          };
        })();
  const input =
    materialVersion === 2
      ? requestedInput
      : (() => {
          const { authority: _authority, materialVersion: _version, ...base } = requestedInput;
          return { ...base, materialVersion: 1 as const };
        })();
  if (reuse) {
    yield* orchestrator.dispatch(
      decodeOrchestratorFixtureCommand({
        type: "thread.create",
        commandId: "controlled-existing-thread",
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
  }
  let fingerprint: string;
  if (kind === "workflow" && "runId" in input) {
    yield* store.createRun({
      ...run,
      spec: { ...run.spec, plan: binding },
      access: { origins: [destination] },
    });
    const result = yield* store.reserveWorkflowNativeIntent(input);
    if (result.kind !== "reserved") throw new Error("reservation denied");
    fingerprint = result.fingerprint;
  } else if ("sessionId" in input) {
    yield* store.beginPlanning({
      session,
      now: facts.now,
      context,
      command: { commandId: "controlled-begin", receiptId: "controlled-begin" },
    });
    fingerprint = (yield* store.reservePlanningNativeIntent(input)).fingerprint;
  } else throw new Error("wrong fixture owner");
  if (claim)
    assert.strictEqual(
      (yield* store.claimNativeIntent({
        intentId: input.intentId,
        expectedFingerprint: fingerprint,
        expectedOwnerVersion: 1,
        expectedGeneration: 0,
        ...authority.coordinator,
      })).kind,
      "claimed",
    );
  const derivation = yield* Derivation.make.pipe(
    Effect.provideService(Store.WorkflowRunStore, store),
  );
  const derived = yield* sql.withTransaction(
    derivation.deriveInTransaction({ intentId: input.intentId }),
  );
  return {
    kind,
    config,
    sql,
    sessions,
    reference,
    authority,
    runtime,
    store,
    orchestrator,
    input,
    derived,
  };
});

it.effect.each([
  ["workflow", false],
  ["workflow", true],
  ["planning", false],
  ["planning", true],
] as const)("authorized native graph dispatches real $0 phases with reuse=$1", ([kind, reuse]) =>
  Effect.gen(function* () {
    const { orchestrator, derived, input, store } = yield* controlledNativeFixture(kind, reuse);
    const claimHint = { intentId: input.intentId, phase: "claim" } as const;
    const messageHint = { intentId: input.intentId, phase: "message" } as const;
    yield* orchestrator.dispatch(derived.claimCommand, claimHint);
    const result = yield* orchestrator.dispatch(derived.messageCommand, messageHint);
    const created = result.storedEvents.find(({ event }) => event.type === "run.created");
    assert.isDefined(created);
    if (created?.event.type !== "run.created") throw new Error("run missing");
    assert.strictEqual(created.event.payload.userMessageId, input.messageId);
    const sql = yield* SqlClient.SqlClient;
    assert.strictEqual((yield* sql`SELECT * FROM workflow_native_command_proofs`).length, 2);
    assert.deepStrictEqual(
      yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_type IN ('provider-turn.start','provider-turn.restart','provider-runtime.continue')`,
      [],
    );
    assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, []);
    const retained = yield* sql.withTransaction(
      store.readNativeReservationInTransaction({ intentId: input.intentId }),
    );
    assert.strictEqual(retained.state, "dispatching");
    assert.strictEqual(retained.nativeRunId, null);
    assert.strictEqual(retained.material.version, 2);
  }).pipe(Effect.provide(testLayer())),
);

const controlNativeFixture = Effect.fn("test.controlNativeFixture")(function* (
  fixture: Effect.Success<ReturnType<typeof controlledNativeFixture>>,
  action: "pause" | "stop",
) {
  const { store, sql } = fixture;
  if (fixture.kind === "workflow") {
    const row = (yield* sql<{
      record_version: number;
      control_generation: number;
    }>`SELECT record_version,control_generation FROM workflow_runs`)[0]!;
    yield* store.commitTransition({
      runId: run.id,
      command: {
        ...command,
        kind: action,
        commandId: `controlled-${action}`,
        expectedVersion: row.record_version,
        expectedGeneration: row.control_generation,
      },
      externalFacts: admissionFacts,
      receiptId: `controlled-${action}`,
    });
  } else {
    const row = (yield* sql<{
      record_version: number;
    }>`SELECT record_version FROM workflow_planning_sessions`)[0]!;
    yield* store.controlPlanning({
      sessionId: session.id,
      action,
      expectedVersion: row.record_version,
      now: facts.now,
      context,
      command: { commandId: `controlled-${action}`, receiptId: `controlled-${action}` },
    });
  }
});

it.effect.each([
  ["workflow", "revoke"],
  ["planning", "revoke"],
  ["workflow", "pause"],
  ["planning", "pause"],
  ["workflow", "stop"],
  ["planning", "stop"],
  ["workflow", "delete-column"],
  ["planning", "delete-column"],
  ["workflow", "delete-payload"],
  ["planning", "delete-payload"],
  ["workflow", "coordinator"],
  ["planning", "coordinator"],
  ["workflow", "deadline"],
  ["planning", "deadline"],
] as const)(
  "authorized native graph denies new $0 message after $1 and preserves claim replay",
  ([kind, change]) =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture(kind);
      const { orchestrator, derived, input, sessions, reference, sql } = fixture;
      const claimHint = { intentId: input.intentId, phase: "claim" } as const;
      const messageHint = { intentId: input.intentId, phase: "message" } as const;
      const first = yield* orchestrator.dispatch(derived.claimCommand, claimHint);
      let selected = orchestrator;
      if (change === "revoke") yield* sessions.revoke(reference.sessionId);
      else if (change === "pause" || change === "stop")
        yield* controlNativeFixture(fixture, change);
      else if (change === "deadline")
        yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      else if (change === "coordinator")
        selected = Context.get(
          yield* proofOrchestrator(
            true,
            Effect.void,
            {
              ...fixture.authority,
              coordinator: { ...fixture.authority.coordinator, workerId: "other-coordinator" },
            },
            fixture.config,
          ),
          Orchestrator.OrchestratorV2,
        );
      else if (change === "delete-column")
        yield* sql`UPDATE orchestration_v2_projection_threads SET deleted_at = ${facts.now} WHERE thread_id = ${input.threadId}`;
      else
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json = json_set(payload_json,'$.deletedAt',${facts.now}) WHERE thread_id = ${input.threadId}`;
      const before = yield* nativeCounts;
      assert.isTrue(
        isOrchestratorDispatchError(
          yield* Effect.flip(selected.dispatch(derived.messageCommand, messageHint)),
        ),
      );
      assert.deepStrictEqual(yield* nativeCounts, before);
      assert.deepStrictEqual(
        (yield* selected.dispatch(derived.claimCommand, claimHint)).storedEvents,
        first.storedEvents,
      );
      assert.deepStrictEqual(yield* nativeCounts, before);
      assert.strictEqual((yield* sql`SELECT * FROM workflow_native_command_proofs`).length, 1);
      assert.deepStrictEqual(yield* sql`SELECT * FROM workflow_operations`, []);
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["workflow", "planning"] as const)(
  "authorized native graph refuses unclaimed $0 intent without native writes",
  (kind) =>
    Effect.gen(function* () {
      const { orchestrator, derived, input } = yield* controlledNativeFixture(kind, false, false);
      const before = yield* nativeCounts;
      assert.isTrue(
        isOrchestratorDispatchError(
          yield* Effect.flip(
            orchestrator.dispatch(derived.claimCommand, {
              intentId: input.intentId,
              phase: "claim",
            }),
          ),
        ),
      );
      assert.deepStrictEqual(yield* nativeCounts, before);
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["workflow", "planning"] as const)(
  "authorized native graph preserves verified $0 message replay after revocation and stop",
  (kind) =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture(kind);
      const { orchestrator, derived, input, sessions, reference } = fixture;
      yield* orchestrator.dispatch(derived.claimCommand, {
        intentId: input.intentId,
        phase: "claim",
      });
      const hint = { intentId: input.intentId, phase: "message" } as const;
      const first = yield* orchestrator.dispatch(derived.messageCommand, hint);
      yield* sessions.revoke(reference.sessionId);
      yield* controlNativeFixture(fixture, "stop");
      const before = yield* nativeCounts;
      const other = Context.get(
        yield* proofOrchestrator(
          true,
          Effect.void,
          {
            ...fixture.authority,
            coordinator: { bootId: "other-boot", workerId: "other-worker" },
          },
          fixture.config,
        ),
        Orchestrator.OrchestratorV2,
      );
      assert.deepStrictEqual(
        (yield* other.dispatch(derived.messageCommand, hint)).storedEvents,
        first.storedEvents,
      );
      assert.deepStrictEqual(yield* nativeCounts, before);
    }).pipe(Effect.provide(testLayer())),
);

const observeAuthorizedSink = Effect.fn("test.observeAuthorizedSink")(function* (
  sink: EventSink.EventSinkV2["Service"],
  afterSequence = 0,
) {
  const ready = yield* Deferred.make<void>();
  const observe: Statement.Transformer = (statement) => {
    const [query] = statement.compile();
    return /FROM orchestration_events/.test(query)
      ? Deferred.succeed(ready, undefined).pipe(Effect.as(statement))
      : Effect.succeed(statement);
  };
  const publication = yield* sink
    .stream({ afterSequence })
    .pipe(
      Stream.take(1),
      Stream.runCollect,
      Effect.provideService(Statement.CurrentTransformer, observe),
      Effect.forkScoped({ startImmediately: true }),
    );
  yield* Deferred.await(ready);
  return publication;
});

it.effect(
  "authorized native graph owns coordinator/config and shares atomic publication and outbox wakeups",
  () =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow");
      const mutableAuthority = {
        ...fixture.authority,
        coordinator: { ...fixture.authority.coordinator },
      };
      const mutableConfig = { ...fixture.config };
      const pending = EventSink.makeCapturedAuthorizedNativeEventInfrastructure(
        fixture.sql,
        mutableConfig,
        yield* Path.Path,
        mutableAuthority,
      );
      mutableAuthority.coordinator.workerId = "mutated-worker";
      mutableConfig.baseDir = "/mutated-config";
      const graph = yield* pending;
      const sink = Context.get(graph, EventSink.EventSinkV2);
      const outbox = Context.get(graph, EffectOutbox.EffectOutboxV2);
      const selectedStore = Context.get(graph, Store.WorkflowRunStore);
      assert.deepStrictEqual(
        yield* fixture.sql.withTransaction(
          selectedStore.verifyClaimedNativeIntentInTransaction({
            intentId: fixture.input.intentId,
            ...fixture.authority.coordinator,
          }),
        ),
        { kind: "current" },
      );
      const plan = yield* nativeDecision(fixture.derived, "claim");
      const publication = yield* observeAuthorizedSink(sink);
      const wake = yield* outbox.awaitAvailable.pipe(Effect.forkScoped({ startImmediately: true }));
      const before = yield* nativeCounts;
      const unselectedCleanup = {
        ...plan,
        effects: [
          {
            id: "native-unselected-cleanup",
            commandId: plan.command.commandId,
            threadId: fixture.derived.claimCommand.threadId,
            request: { type: "terminal.cleanup" as const },
          },
        ],
      };
      assert.instanceOf(
        yield* Effect.flip(sink.commitNativeCommand!(unselectedCleanup)),
        EventSink.EventSinkWriteError,
      );
      assert.deepStrictEqual(yield* nativeCounts, before);
      assert.isUndefined(publication.pollUnsafe());
      assert.isUndefined(wake.pollUnsafe());
      yield* fixture.sql`CREATE TRIGGER reject_authorized_proof BEFORE INSERT ON workflow_native_command_proofs BEGIN SELECT RAISE(ABORT,'injected authorized proof failure'); END`;
      assert.instanceOf(
        yield* Effect.flip(sink.commitNativeCommand!(plan)),
        EventSink.EventSinkWriteError,
      );
      assert.deepStrictEqual(yield* nativeCounts, before);
      assert.isUndefined(publication.pollUnsafe());
      assert.isUndefined(wake.pollUnsafe());
      yield* fixture.sql`DROP TRIGGER reject_authorized_proof`;
      const committed = yield* sink.commitNativeCommand!(plan);
      assert.isTrue(committed.committed);
      assert.strictEqual(
        (yield* Fiber.join(publication))[0]?.sequence,
        committed.receipt.resultSequence,
      );
      assert.isUndefined(wake.pollUnsafe());
      const ordinaryCommandId = CommandId.make("authorized-ordinary-cleanup");
      const cleanup = yield* sink.commitCommand({
        commandId: ordinaryCommandId,
        threadId: fixture.derived.claimCommand.threadId,
        commandType: "thread.metadata.update",
        acceptedAt: yield* DateTime.now,
        events: [],
        effects: [
          {
            id: "authorized-pending-effect",
            commandId: ordinaryCommandId,
            threadId: fixture.derived.claimCommand.threadId,
            request: { type: "terminal.cleanup" },
          },
        ],
      });
      assert.isTrue(cleanup.committed);
      yield* Fiber.join(wake);
      assert.strictEqual(
        Option.getOrThrow(yield* outbox.get("authorized-pending-effect")).request.type,
        "terminal.cleanup",
      );
      const proof = (yield* fixture.sql<{
        proof_json: string;
      }>`SELECT proof_json FROM workflow_native_command_proofs`)[0]!;
      assert.strictEqual(
        JSON.parse(proof.proof_json).normalization.scratchRoot,
        (yield* Path.Path).resolve(fixture.config.baseDir, "scratch"),
      );
      const nextPublication = yield* observeAuthorizedSink(sink, committed.receipt.resultSequence);
      const nextWake = yield* outbox.awaitAvailable.pipe(
        Effect.forkScoped({ startImmediately: true }),
      );
      yield* fixture.sessions.revoke(fixture.reference.sessionId);
      yield* controlNativeFixture(fixture, "stop");
      assert.isFalse((yield* sink.commitNativeCommand!(plan)).committed);
      assert.strictEqual(
        (yield* sink.replayNativeCommand!({ command: plan.command, hint: plan.hint })).kind,
        "verified",
      );
      assert.isUndefined(nextPublication.pollUnsafe());
      assert.isUndefined(nextWake.pollUnsafe());
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["workflow", "planning"] as const)(
  "authorized native graph denies legacy %s state flags without a grant",
  (kind) =>
    Effect.gen(function* () {
      const { orchestrator, derived, input, sql } = yield* controlledNativeFixture(
        kind,
        false,
        false,
        1,
      );
      yield* sql`UPDATE workflow_native_launch_intents SET state = 'dispatching',boot_id = 'controlled-boot',worker_id = 'controlled-worker'`;
      const before = yield* nativeCounts;
      assert.isTrue(
        isOrchestratorDispatchError(
          yield* Effect.flip(
            orchestrator.dispatch(derived.claimCommand, {
              intentId: input.intentId,
              phase: "claim",
            }),
          ),
        ),
      );
      assert.deepStrictEqual(yield* nativeCounts, before);
    }).pipe(Effect.provide(testLayer())),
);

const decodeProducedOperation = Schema.decodeEffect(Schema.fromJsonString(WorkflowOperationV1));

const nativeLaunchProducer = Effect.fn("test.nativeLaunchProducer")(function* (
  fixture: Effect.Success<ReturnType<typeof controlledNativeFixture>>,
  coordinator = fixture.authority.coordinator,
  store = fixture.store,
  config = fixture.config,
) {
  return yield* NativeLaunch.make(coordinator).pipe(
    Effect.provideService(Store.WorkflowRunStore, store),
    Effect.provide(fixture.runtime),
    Effect.provideService(SqlClient.SqlClient, fixture.sql),
    Effect.provideService(ServerConfig, config),
    Effect.provide(NodePathService.layerPosix),
  );
});
const producerSelection = (
  fixture: Effect.Success<ReturnType<typeof controlledNativeFixture>>,
) => ({
  intentId: fixture.input.intentId,
  expectedFingerprint: fixture.derived.reservation.fingerprint,
  expectedOwnerVersion: 1,
  expectedGeneration: 0,
});

it.effect.each([
  ["workflow", false],
  ["workflow", true],
  ["planning", false],
  ["planning", true],
] as const)(
  "native launch producer owns a real $0 operation claim with reuse=$1",
  ([kind, reuse]) =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture(kind, reuse, false, 2);
      const producer = yield* nativeLaunchProducer(fixture);
      const before = (yield* fixture.sql<{
        sequence: number;
      }>`SELECT COALESCE(MAX(sequence),0) AS sequence FROM orchestration_events`)[0]!.sequence;
      const publication = yield* observeAuthorizedSink(
        Context.get(fixture.runtime, EventSink.EventSinkV2),
        before,
      );
      const result = yield* producer.claimReservedIntent(producerSelection(fixture));
      assert.isTrue(Object.isFrozen(result));
      assert.isTrue(Object.isFrozen(result.native));
      assert.isTrue(Object.isFrozen(result.executionClaim));
      const operations = yield* fixture.sql<{
        record_json: string;
      }>`SELECT record_json FROM workflow_operations`;
      assert.strictEqual(operations.length, 1);
      const operation = yield* decodeProducedOperation(operations[0]!.record_json);
      assert.deepStrictEqual(result.executionClaim, {
        operationId: operation.id,
        expectedFingerprint: operation.fingerprint,
        expectedGeneration: operation.controlGeneration,
        bootId: operation.claim!.bootId,
        workerId: operation.claim!.workerId,
      });
      assert.strictEqual(operation.status, "claimed");
      const jobs = yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`;
      assert.strictEqual(jobs.length, 1);
      assert.strictEqual(result.preparation.job.identityVersion, 3);
      assert.isTrue(Object.isFrozen(result.preparation));
      assert.isTrue(Object.isFrozen(result.preparation.job));
      assert.isTrue(Object.isFrozen(result.preparation.selection));
      assert.deepStrictEqual(result.preparation.job.input.nativeOwner, {
        intentId: fixture.input.intentId,
        owner: operation.owner,
        executionClaim: result.executionClaim,
      });
      assert.strictEqual(result.preparation.job.input.threadId, result.native.threadId);
      assert.strictEqual(result.preparation.job.input.runId, result.native.runId);
      assert.strictEqual(result.preparation.job.state, "active");
      assert.isFalse(result.preparation.job.processExposed);
      assert.strictEqual(result.preparation.job.codeClosedAt, null);
      assert.deepStrictEqual(result.preparation.selection, {
        jobId: result.preparation.job.input.jobId,
        entryKey: result.preparation.job.input.entryKey,
        requestIdentityHash: result.preparation.job.requestIdentityHash,
        serverBootId: result.executionClaim.bootId,
        expectedVersion: 0,
      });
      assert.strictEqual(
        jobs[0]!.request_identity_hash,
        result.preparation.job.requestIdentityHash,
      );

      const intent = yield* fixture.sql.withTransaction(
        fixture.store.readNativeReservationInTransaction({ intentId: fixture.input.intentId }),
      );
      assert.strictEqual(intent.state, "finalized");
      assert.strictEqual(intent.finalizedOperationId, operation.id);
      assert.strictEqual(intent.nativeRunId, result.native.runId);
      assert.strictEqual(result.native.threadId, fixture.input.threadId);
      assert.strictEqual(
        (yield* fixture.sql`SELECT * FROM workflow_native_command_proofs`).length,
        2,
      );
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT status FROM orchestration_v2_projection_runs`,
        [{ status: "preparing" }],
      );
      assert.deepStrictEqual(yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_submission_transports`,
        [],
      );
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_submission_attempts`,
        [],
      );
      const published = yield* Fiber.join(publication);
      assert.isTrue(published.length > 0 && published[0]!.sequence > before);
      if (kind === "workflow")
        assert.strictEqual(
          (yield* fixture.sql<{
            consumed: number;
          }>`SELECT json_extract(record_json,'$.allowance.attemptsConsumed') AS consumed FROM workflow_runs WHERE id='run'`)[0]!
            .consumed,
          1,
        );
      else
        assert.strictEqual(
          (yield* fixture.store.getPlanning({ sessionId: session.id })).consumed.passes,
          1,
        );
      assert.instanceOf(
        yield* Effect.flip(producer.claimReservedIntent(producerSelection(fixture))),
        NativeLaunch.WorkflowNativeLaunchError,
      );
      assert.strictEqual((yield* fixture.sql`SELECT * FROM workflow_operations`).length, 1);
    }).pipe(Effect.provide(testLayer())),
);

const producerDatabaseSnapshot = Effect.fn("test.producerDatabaseSnapshot")(function* (
  sql: SqlClient.SqlClient,
) {
  const tables = yield* sql<{
    name: string;
  }>`SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name`;
  const result: Record<string, readonly unknown[]> = {};
  for (const table of tables)
    result[table.name] = yield* sql.unsafe(`SELECT * FROM "${table.name.replaceAll('"', '""')}"`);
  return result;
});

it.effect.each([
  ["workflow", "revoke"],
  ["planning", "revoke"],
  ["workflow", "pause"],
  ["planning", "stop"],
  ["workflow", "deadline"],
  ["workflow", "material"],
  ["planning", "legacy"],
] as const)("native launch producer preserves $0 state after $1 refusal", ([kind, change]) =>
  Effect.gen(function* () {
    const fixture = yield* controlledNativeFixture(kind, false, false, change === "legacy" ? 1 : 2);
    const producer = yield* nativeLaunchProducer(fixture);
    if (change === "revoke") yield* fixture.sessions.revoke(fixture.reference.sessionId);
    if (change === "pause" || change === "stop") yield* controlNativeFixture(fixture, change);
    if (change === "deadline")
      yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
    if (change === "material")
      yield* fixture.sql`UPDATE workflow_native_launch_intents SET draft_json='{}'`;
    const subscription = yield* fixture.store.subscribeChanges;
    const before = yield* producerDatabaseSnapshot(fixture.sql);
    const sink = Context.get(fixture.runtime, EventSink.EventSinkV2);
    const sequence = (yield* fixture.sql<{
      sequence: number;
    }>`SELECT COALESCE(MAX(sequence),0) AS sequence FROM orchestration_events`)[0]!.sequence;
    const publication = yield* observeAuthorizedSink(sink, sequence);
    const error = yield* Effect.flip(producer.claimReservedIntent(producerSelection(fixture)));
    assert.instanceOf(error, NativeLaunch.WorkflowNativeLaunchError);
    assert.deepStrictEqual(yield* producerDatabaseSnapshot(fixture.sql), before);
    assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    assert.isUndefined(publication.pollUnsafe());
  }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["captured-sql", "captured-batch", "foreign-batch"] as const)(
  "native launch producer refuses inherited %s before any mutation",
  (boundary) =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2);
      const producer = yield* nativeLaunchProducer(fixture);
      const foreign = yield* Layer.build(
        Layer.fresh(NodeSqliteClient.layer({ filename: ":memory:" })),
      );
      const otherSql = Context.get(foreign, SqlClient.SqlClient);
      yield* runMigrations().pipe(Effect.provideService(SqlClient.SqlClient, otherSql));
      const otherGraph = yield* EventSink.makeCapturedEventInfrastructure(otherSql);
      const otherSink = Context.get(otherGraph, EventSink.EventSinkV2);
      const sink = Context.get(fixture.runtime, EventSink.EventSinkV2);
      const subscription = yield* fixture.store.subscribeChanges;
      const before = yield* producerDatabaseSnapshot(fixture.sql);
      const otherBefore = yield* producerDatabaseSnapshot(otherSql);
      const publication = yield* observeAuthorizedSink(sink);
      const action = producer.claimReservedIntent(producerSelection(fixture));
      const result =
        boundary === "captured-sql"
          ? yield* Effect.result(fixture.sql.withTransaction(action))
          : boundary === "captured-batch"
            ? yield* Effect.result(sink.withProviderBatch(action))
            : yield* Effect.result(otherSink.withProviderBatch(action));
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure")
        assert.instanceOf(result.failure, NativeLaunch.WorkflowNativeLaunchError);
      assert.deepStrictEqual(yield* producerDatabaseSnapshot(fixture.sql), before);
      assert.deepStrictEqual(yield* producerDatabaseSnapshot(otherSql), otherBefore);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
      assert.isUndefined(publication.pollUnsafe());
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["workflow", "planning"] as const)(
  "native launch producer permits one concurrent %s claim",
  (kind) =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture(kind, false, false, 2);
      const producer = yield* nativeLaunchProducer(fixture);
      const results = yield* Effect.all(
        [
          Effect.result(producer.claimReservedIntent(producerSelection(fixture))),
          Effect.result(producer.claimReservedIntent(producerSelection(fixture))),
        ],
        { concurrency: "unbounded" },
      );
      assert.strictEqual(results.filter((result) => result._tag === "Success").length, 1);
      assert.strictEqual(results.filter((result) => result._tag === "Failure").length, 1);
      assert.strictEqual(
        (yield* fixture.sql`SELECT * FROM workflow_operations WHERE status='claimed'`).length,
        1,
      );
      assert.strictEqual(
        (yield* fixture.sql`SELECT * FROM workflow_native_command_proofs`).length,
        2,
      );
      assert.deepStrictEqual(yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["message", "finalization", "operation"] as const)(
  "native launch producer retains committed evidence after a real %s failure",
  (phase) =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2);
      const producer = yield* nativeLaunchProducer(fixture);
      if (phase === "message")
        yield* fixture.sql`CREATE TRIGGER reject_producer_phase BEFORE INSERT ON workflow_native_command_proofs WHEN NEW.phase='message' BEGIN SELECT RAISE(ABORT,'injected message proof failure'); END`;
      else if (phase === "finalization")
        yield* fixture.sql`CREATE TRIGGER reject_producer_phase BEFORE INSERT ON workflow_operations BEGIN SELECT RAISE(ABORT,'injected finalization failure'); END`;
      else
        yield* fixture.sql`CREATE TRIGGER reject_producer_phase BEFORE UPDATE ON workflow_operations WHEN NEW.status='claimed' BEGIN SELECT RAISE(ABORT,'injected operation claim failure'); END`;
      const error = yield* Effect.flip(producer.claimReservedIntent(producerSelection(fixture)));
      assert.instanceOf(error, NativeLaunch.WorkflowNativeLaunchError);
      assert.strictEqual(error.stage, phase === "message" ? "message-command" : phase);
      assert.strictEqual(
        (yield* fixture.sql`SELECT * FROM workflow_native_command_proofs`).length,
        phase === "message" ? 1 : 2,
      );
      const intents = yield* fixture.sql<{
        state: string;
        native_run_id: string | null;
      }>`SELECT state,native_run_id FROM workflow_native_launch_intents`;
      assert.strictEqual(intents[0]!.state, phase === "operation" ? "finalized" : "dispatching");
      assert.strictEqual(intents[0]!.native_run_id === null, phase === "message");
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM workflow_operations WHERE status='claimed'`,
        [],
      );
      assert.deepStrictEqual(yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      const retained = yield* producerDatabaseSnapshot(fixture.sql);
      assert.instanceOf(
        yield* Effect.flip(producer.claimReservedIntent(producerSelection(fixture))),
        NativeLaunch.WorkflowNativeLaunchError,
      );
      assert.deepStrictEqual(yield* producerDatabaseSnapshot(fixture.sql), retained);
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["workflow", "planning"] as const)(
  "native launch producer does not upgrade a post-finalization %s owner version",
  (kind) =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture(kind, false, false, 2);
      const observed = yield* Deferred.make<void>();
      const store = {
        ...fixture.store,
        claim: (input: Parameters<typeof fixture.store.claim>[0]) =>
          Effect.gen(function* () {
            assert.strictEqual(input.expectedOwnerVersion, 1);
            assert.strictEqual(input.expectedGeneration, 0);
            if (kind === "workflow")
              yield* fixture.sql`UPDATE workflow_runs SET record_version=record_version+1,record_json=json_set(record_json,'$.recordVersion',record_version+1)`;
            else
              yield* fixture.sql`UPDATE workflow_planning_sessions SET record_version=record_version+1,record_json=json_set(record_json,'$.recordVersion',record_version+1)`;
            yield* Deferred.succeed(observed, undefined);
            return yield* fixture.store.claim(input);
          }),
      };
      const producer = yield* nativeLaunchProducer(fixture, fixture.authority.coordinator, store);
      const error = yield* Effect.flip(producer.claimReservedIntent(producerSelection(fixture)));
      yield* Deferred.await(observed);
      assert.strictEqual(error.stage, "operation");
      assert.strictEqual(error.reason, "stale");
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT status,generation FROM workflow_operations`,
        [{ status: "pending", generation: 0 }],
      );
      assert.deepStrictEqual(yield* fixture.sql`SELECT state FROM workflow_native_launch_intents`, [
        { state: "finalized" },
      ]);
      assert.strictEqual(
        (yield* fixture.sql`SELECT * FROM workflow_native_command_proofs`).length,
        2,
      );
      assert.deepStrictEqual(yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
    }).pipe(Effect.provide(testLayer())),
);

it.effect("native launch producer retains an actual unknown binding after authority changes", () =>
  Effect.gen(function* () {
    const fixture = yield* controlledNativeFixture("workflow", false, false, 2);
    const store = {
      ...fixture.store,
      bindNativeIntent: (input: Parameters<typeof fixture.store.bindNativeIntent>[0]) =>
        fixture.sessions
          .revoke(fixture.reference.sessionId)
          .pipe(Effect.andThen(fixture.store.bindNativeIntent(input))),
    };
    const producer = yield* nativeLaunchProducer(fixture, fixture.authority.coordinator, store);
    const error = yield* Effect.flip(producer.claimReservedIntent(producerSelection(fixture)));
    assert.strictEqual(error.stage, "binding");
    assert.strictEqual(error.reason, "reconciliation_required");
    const intents = yield* fixture.sql<{
      state: string;
      native_run_id: string | null;
    }>`SELECT state,native_run_id FROM workflow_native_launch_intents`;
    assert.strictEqual(intents[0]!.state, "unknown");
    assert.isString(intents[0]!.native_run_id);
    assert.strictEqual(
      (yield* fixture.sql`SELECT * FROM workflow_native_command_proofs`).length,
      2,
    );
    assert.deepStrictEqual(yield* fixture.sql`SELECT * FROM workflow_operations`, []);
    assert.deepStrictEqual(yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
  }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "native launch producer owns its coordinator, graph, normalization and request before execution",
  () =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2);
      const coordinator = { ...fixture.authority.coordinator };
      const config = { ...fixture.config };
      const construction = NativeLaunch.make(coordinator).pipe(
        Effect.provide(fixture.runtime),
        Effect.provideService(SqlClient.SqlClient, fixture.sql),
        Effect.provideService(ServerConfig, config),
        Effect.provide(NodePathService.layerPosix),
      );
      coordinator.workerId = "changed-before-construction-runs";
      const producer = yield* construction;
      config.baseDir = "/changed-after-construction";
      const captured = Context.make(NativeLaunch.WorkflowNativeLaunchService, producer);
      const input = producerSelection(fixture);
      const pending = Context.get(
        captured,
        NativeLaunch.WorkflowNativeLaunchService,
      ).claimReservedIntent(input);
      input.expectedFingerprint = "f".repeat(64);
      input.expectedOwnerVersion = 99;
      input.expectedGeneration = 99;
      input.intentId = "different-intent";
      const result = yield* pending.pipe(
        Effect.provideService(Store.WorkflowRunStore, {
          ...fixture.store,
          claimNativeIntent: () => Effect.die("Ambient Store must not replace captured graph"),
        }),
      );
      assert.deepStrictEqual(
        {
          bootId: result.executionClaim.bootId,
          workerId: result.executionClaim.workerId,
          generation: result.executionClaim.expectedGeneration,
        },
        { ...fixture.authority.coordinator, generation: 0 },
      );
      assert.strictEqual(result.native.threadId, fixture.input.threadId);
      const proofs = yield* fixture.sql<{
        proof_json: string;
      }>`SELECT proof_json FROM workflow_native_command_proofs`;
      for (const row of proofs)
        assert.strictEqual(
          JSON.parse(row.proof_json).normalization.scratchRoot,
          (yield* Path.Path).resolve(fixture.config.baseDir, "scratch"),
        );
    }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "native launch producer refuses malformed selectors without evaluating getters or changing records",
  () =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2);
      const producer = yield* nativeLaunchProducer(fixture);
      let getters = 0;
      const base = producerSelection(fixture);
      const getter = {
        ...base,
        get intentId() {
          getters++;
          return base.intentId;
        },
      };
      const before = yield* producerDatabaseSnapshot(fixture.sql);
      const subscription = yield* fixture.store.subscribeChanges;
      for (const input of [
        getter,
        { ...base, expectedGeneration: -1 },
        { ...base, expectedOwnerVersion: 0.5 },
        { ...base, expectedFingerprint: base.expectedFingerprint + "\n" },
        { ...base, bootId: "forged" },
      ]) {
        const error = yield* Effect.flip(producer.claimReservedIntent(input));
        assert.strictEqual(error.reason, "invalid");
      }
      assert.strictEqual(getters, 0);
      assert.deepStrictEqual(yield* producerDatabaseSnapshot(fixture.sql), before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(testLayer())),
);

it.effect(
  "authoritative native metadata refuses title-model work at the atomic commit boundary",
  () =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, true, 2, true);
      yield* fixture.orchestrator.dispatch(fixture.derived.claimCommand, {
        intentId: fixture.input.intentId,
        phase: "claim",
      });
      const before = yield* producerDatabaseSnapshot(fixture.sql);
      const sink = Context.get(fixture.runtime, EventSink.EventSinkV2);
      const sequence = (yield* fixture.sql<{
        sequence: number;
      }>`SELECT MAX(sequence) AS sequence FROM orchestration_events`)[0]!.sequence;
      const publication = yield* observeAuthorizedSink(sink, sequence);
      const result = yield* Effect.result(
        fixture.orchestrator.dispatch(fixture.derived.messageCommand, {
          intentId: fixture.input.intentId,
          phase: "message",
        }),
      );
      assert.deepStrictEqual(yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.strictEqual(result._tag, "Failure");
      assert.deepStrictEqual(yield* producerDatabaseSnapshot(fixture.sql), before);
      assert.isUndefined(publication.pollUnsafe());
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["workflow", "planning"] as const)(
  "native launch producer refuses requested %s title generation before claiming",
  (kind) =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture(kind, false, false, 2, true);
      const producer = yield* nativeLaunchProducer(fixture);
      const before = yield* producerDatabaseSnapshot(fixture.sql);
      const subscription = yield* fixture.store.subscribeChanges;
      const result = yield* Effect.result(producer.claimReservedIntent(producerSelection(fixture)));
      assert.deepStrictEqual(yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.strictEqual(result._tag, "Failure");
      if (result._tag === "Failure") {
        assert.strictEqual(result.failure.stage, "intent");
        assert.strictEqual(result.failure.reason, "unavailable");
      }
      assert.deepStrictEqual(yield* producerDatabaseSnapshot(fixture.sql), before);
      assert.strictEqual((yield* PubSub.takeUpTo(subscription, 10)).length, 0);
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each(
  (["workflow", "planning"] as const).flatMap((kind) =>
    ["insert-ignore", "revoke-after-cas"].map((change) => ({ kind, change })),
  ),
)(
  "native producer preserves committed $kind work for reconciliation after preparation $change",
  ({ kind, change }) =>
    Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture(kind, false, false, 2);
      if (change === "insert-ignore")
        yield* fixture.sql`CREATE TRIGGER refuse_preparation BEFORE INSERT ON orchestration_v2_preparation_jobs BEGIN SELECT RAISE(IGNORE); END`;
      const store =
        change === "revoke-after-cas"
          ? Store.WorkflowRunStore.of({
              ...fixture.store,
              beginClaimedNativePreparation: (claim) =>
                fixture.sessions
                  .revoke(fixture.reference.sessionId)
                  .pipe(
                    Effect.orDie,
                    Effect.andThen(fixture.store.beginClaimedNativePreparation(claim)),
                  ),
            })
          : fixture.store;
      const producer = yield* nativeLaunchProducer(fixture, fixture.authority.coordinator, store);
      const failure = yield* Effect.flip(producer.claimReservedIntent(producerSelection(fixture)));
      assert.strictEqual(failure.stage, "preparation");
      assert.deepStrictEqual(yield* fixture.sql`SELECT status FROM workflow_operations`, [
        { status: "claimed" },
      ]);
      assert.deepStrictEqual(yield* fixture.sql`SELECT state FROM workflow_native_launch_intents`, [
        { state: "finalized" },
      ]);
      assert.strictEqual(
        (yield* fixture.sql`SELECT * FROM workflow_native_command_proofs`).length,
        2,
      );
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`,
        [],
      );
      assert.deepStrictEqual(yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox`, []);
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_submission_transports`,
        [],
      );
      if (change === "insert-ignore") yield* fixture.sql`DROP TRIGGER refuse_preparation`;
      const before = yield* producerDatabaseSnapshot(fixture.sql);
      const retry = yield* Effect.flip(producer.claimReservedIntent(producerSelection(fixture)));
      assert.strictEqual(retry.reason, "reconciliation_required");
      assert.deepStrictEqual(yield* producerDatabaseSnapshot(fixture.sql), before);
    }).pipe(Effect.provide(testLayer())),
);
