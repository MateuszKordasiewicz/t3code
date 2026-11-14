import { vi } from "vite-plus/test";
import * as FileSystem from "effect/FileSystem";
import { ChildProcessSpawner } from "effect/process";
import { AgentScope, type AgentScopeBinding } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { SpawnExecutableResolution } from "@t3tools/shared/shell";
import * as Cause from "effect/Cause";
import * as Exit from "effect/Exit";
import * as Scope from "effect/Scope";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as PreparationNode from "../preparation/PreparationNodeChild.ts";
import * as PreparationResource from "../preparation/PreparationProcessResource.ts";
import * as PreparationJob from "../preparation/PreparationJob.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as NativeTransport from "../orchestration-v2/WorkflowNativeCodexTransport.ts";
import * as ProviderSessionManager from "../orchestration-v2/ProviderSessionManager.ts";
import * as CommandOutputHub from "../orchestration-v2/CommandOutputHub.ts";
import * as ProviderEventIngestor from "../orchestration-v2/ProviderEventIngestor.ts";
import * as McpSessionRegistryTestkit from "../mcp/McpSessionRegistry.testkit.ts";

import * as ProviderInstances from "../provider/ProviderInstanceRegistry.ts";
import * as CodexInstallation from "../provider/CodexInstallation.ts";
import * as RuntimePolicy from "../orchestration-v2/RuntimePolicy.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import * as Continuations from "@t3tools/provider-core/server/ProviderContinuationRequests";
import type { ProviderInstance } from "@t3tools/provider-core/server/driver";
import type { ServerProvider, ServerProviderCompatibilityAdvisory } from "@t3tools/contracts";
import * as GitVcsDriver from "../vcs/GitVcsDriver.ts";
import { makeGitVcsDriverCore } from "../vcs/GitVcsDriverCore.ts";
import * as Settings from "../serverSettings.ts";
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import * as NodePathService from "@effect/platform-node/NodePath";
import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  EventId,
  MessageId,
  OrchestrationV2Command,
  ProviderInstanceId,
  ProviderDriverKind,
  ProviderSessionId,
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
import * as AuthSessions from "../persistence/AuthSessions.ts";
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
import {
  snapshotSubmissionFrame,
  encodeSubmissionTransportData,
  encodeScopeBindingData,
} from "../orchestration-v2/ProviderSubmissionCodec.ts";
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
const decodePlanningSessionFixture = Schema.decodeUnknownSync(PlanningSessionV1);
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
  const registry = ProviderAdapterRegistry.layerFromAdapters([adapter]);
  return yield* Layer.build(
    Layer.fresh(
      ProviderReplayHarness.layerWithRegistry({ name: "native-command-proofs" }, registry, {
        databaseLayer: Layer.succeed(SqlClient.SqlClient, sql),
        nativeCommandProofs,
        ...(serverConfig === undefined ? {} : { serverConfig }),
        ...(nativeWorkflowAuthority === undefined ? {} : { nativeWorkflowAuthority }),
        runEffectWorker: false,
        runContinuationWorker: false,
        recoverOnStartup: false,
      }).pipe(Layer.provideMerge(registry)),
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

it.effect.each(["before-claim", "after-claim"] as const)(
  "native decision refusal leaves no rejected receipt when occupied %s",
  (stage) =>
    Effect.gen(function* () {
      const { derived } = yield* workflowDecision(true);
      const runtime = yield* proofOrchestrator();
      const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
      const sql = yield* SqlClient.SqlClient;
      yield* orchestrator.dispatch(existingThreadCommand(derived));
      if (stage === "after-claim")
        yield* orchestrator.dispatch(derived.claimCommand, {
          intentId: derived.reservation.intentId,
          phase: "claim",
        });
      yield* orchestrator.dispatch({
        ...derived.messageCommand,
        commandId: CommandId.make("existing-message-command"),
        messageId: MessageId.make("existing-message"),
      });
      const sink = Context.get(runtime, EventSink.EventSinkV2);
      const original = sink.commitNativeCommand;
      if (!original) throw new Error("native writer missing");
      let writes = 0;
      using _writer = vi.spyOn(sink, "commitNativeCommand").mockImplementation((input) => {
        writes++;
        return original(input);
      });
      const command = stage === "before-claim" ? derived.claimCommand : derived.messageCommand;
      const before = yield* nativeCounts;
      assert.instanceOf(
        yield* Effect.flip(
          orchestrator.dispatch(command, {
            intentId: derived.reservation.intentId,
            phase: stage === "before-claim" ? "claim" : "message",
          }),
        ),
        Orchestrator.OrchestratorDispatchError,
      );
      assert.strictEqual(writes, 0);
      assert.deepStrictEqual(yield* nativeCounts, before);
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id = ${command.commandId}`,
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
      let allocationCalls = 0;
      const gate = Effect.gen(function* () {
        allocationCalls += 1;
        yield* Deferred.succeed(arrived, undefined);
        yield* Deferred.await(release);
      });
      const allocator = Context.get(
        yield* Layer.build(IdAllocator.layer),
        IdAllocator.IdAllocatorV2,
      );
      const allocate = allocator.allocate.providerSession;
      using _allocation = vi
        .spyOn(allocator.allocate, "providerSession")
        .mockImplementation((input) => gate.pipe(Effect.andThen(allocate(input))));
      const runtime = yield* proofOrchestrator();
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
      const callsBeforeReplay = allocationCalls;
      assert.instanceOf(
        yield* Effect.flip(orchestrator.dispatch(derived.messageCommand, hint)),
        Orchestrator.OrchestratorDispatchError,
      );
      assert.strictEqual(allocationCalls, callsBeforeReplay);
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
  workspaceStrategy?: ThreadLaunch.ThreadLaunchWorkspaceStrategy,
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
            ...(workspaceStrategy ? { workspaceStrategy } : {}),
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
              ...(workspaceStrategy ? { workspaceStrategy } : {}),
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
    binding,
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
    const providerThread = result.storedEvents.find(
      ({ event }) => event.type === "provider-thread.updated",
    );
    if (providerThread?.event.type !== "provider-thread.updated")
      throw new Error("provider thread missing");
    const allocator = Context.get(yield* Layer.build(IdAllocator.layer), IdAllocator.IdAllocatorV2);
    assert.isString(providerThread.event.payload.providerSessionId);
    assert.notStrictEqual(
      providerThread.event.payload.providerSessionId,
      allocator.derive.providerSession({
        providerInstanceId: ProviderInstanceId.make(input.launch.modelSelection.instanceId),
      }),
    );
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
      assert.isTrue(Object.isFrozen(result.preparation.owner));
      assert.strictEqual(result.preparation.owner.jobId, result.preparation.job.input.jobId);
      assert.strictEqual(result.preparation.owner.threadId, result.native.threadId);
      assert.strictEqual(result.preparation.owner.runId, result.native.runId);
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

const nativeWorkspaceProcesses = Effect.fn("test.nativeWorkspaceProcesses")(function* (
  beforeWrap: Effect.Effect<void> = Effect.void,
  beforeCodex: Effect.Effect<void> = Effect.void,
  onCreate: (image: PreparationNode.PreparationNodeImage) => void = () => {},
  onPayload: (image: PreparationNode.PreparationNodeImage, text: string) => void = () => {},
) {
  const requests: PreparationNode.PreparationNodeImage[] = [];
  const host = {
    hostBootId: "12345678-1234-4234-8234-123456789abc",
    scopeNamespace: "app-t3code-agents-123456789abc.slice",
  };
  type Child = {
    binding: AgentScopeBinding;
    output: Queue.Queue<Uint8Array, Cause.Done>;
    terminal: Deferred.Deferred<{ code: number | null; signal: NodeJS.Signals | null }>;
    running: boolean;
    nonce: string;
    releases: number;
    payloads: string[];
    payloadWritten: Queue.Queue<string>;
    initializeReply: "valid" | "missing" | "invalid" | "already" | "wrong-id";
  };
  const children = new Map<string, Child>();
  const bytes = (text: string) => new TextEncoder().encode(text);
  let wraps = 0;
  const wrapEntered = yield* Deferred.make<void>();
  const scope: typeof AgentScope.Service = {
    ...AgentScope.defaultValue(),
    wrap: (input) =>
      Effect.gen(function* () {
        yield* Deferred.succeed(wrapEntered, undefined);
        yield* beforeWrap;
        if (input.name === "codex") yield* beforeCodex;
        const unit = `t3code-${input.name}-12345678-1234-4234-8234-${String(++wraps).padStart(12, "0")}.scope`;
        const binding = {
          unit,
          invocationId: String(wraps).padStart(32, "a"),
          controlGroup: `/user.slice/${host.scopeNamespace}/${unit}`,
          pid: 6000 + wraps,
          pidStartTicks: String(2000 + wraps),
        };
        const child: Child = {
          binding,
          output: yield* Queue.unbounded<Uint8Array, Cause.Done>(),
          terminal: yield* Deferred.make<{ code: number | null; signal: NodeJS.Signals | null }>(),
          running: false,
          releases: 0,
          payloads: [],
          payloadWritten: yield* Queue.unbounded<string>(),
          initializeReply: "valid",
          nonce: input.args[3]!,
        };
        children.set(child.nonce, child);
        return {
          command: input.command,
          args: input.args,
          unit,
          hostIdentity: host,
          custody: {
            bind: () => Effect.succeed(binding),
            observeRetirement: () =>
              Effect.sync(() =>
                child.running ? undefined : { binding, observation: "empty" as const },
              ),
            stopAndObserve: () =>
              Effect.sync(() => {
                child.running = false;
                Deferred.doneUnsafe(
                  child.terminal,
                  Exit.succeed({ code: null, signal: "SIGTERM" }),
                );
                Queue.endUnsafe(child.output);
                return { binding, observation: "empty" as const };
              }),
          },
        };
      }),
  };
  const node: typeof PreparationNode.PreparationNodeChild.Service = {
    prepare: (image) =>
      Effect.gen(function* () {
        const child = children.get(image.args[3]!)!;
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            child.running = false;
            Deferred.doneUnsafe(child.terminal, Exit.succeed({ code: null, signal: "SIGTERM" }));
            Queue.endUnsafe(child.output);
          }),
        );
        return {
          createLocal: () =>
            Effect.sync(() => {
              requests.push(image);
              onCreate(image);
              child.running = true;
              Queue.offerUnsafe(child.output, bytes(`READY:${child.nonce}:${child.binding.pid}\n`));
              return {
                pid: child.binding.pid,
                awaitSpawn: Effect.void,
                awaitTerminal: Deferred.await(child.terminal),
                exitCode: Deferred.await(child.terminal).pipe(
                  Effect.flatMap((value) =>
                    value.code === null
                      ? Effect.fail(
                          new PreparationNode.PreparationNodeChildError({ reason: "unknown" }),
                        )
                      : Effect.succeed(value.code),
                  ),
                ),
                isRunning: Effect.sync(() => child.running),
                kill: () =>
                  Effect.sync(() => {
                    child.running = false;
                    Deferred.doneUnsafe(
                      child.terminal,
                      Exit.succeed({ code: null, signal: "SIGTERM" }),
                    );
                    return undefined;
                  }),
                stdin: Sink.forEach((value: Uint8Array) =>
                  Effect.sync(() => {
                    const text = new TextDecoder().decode(value);
                    if (child.releases && image.args[4]!.endsWith("/codex")) {
                      child.payloads.push(text);
                      onPayload(image, text);
                      Queue.offerUnsafe(child.payloadWritten, text);
                      const message = JSON.parse(text) as Record<string, unknown>;
                      if (message.method === "initialize" && child.initializeReply !== "missing") {
                        const id =
                          child.initializeReply === "wrong-id"
                            ? Number(message.id) + 1
                            : message.id;
                        const response =
                          child.initializeReply === "already"
                            ? { id, error: { code: -32600, message: "Already initialized" } }
                            : {
                                id,
                                result:
                                  child.initializeReply === "invalid"
                                    ? { userAgent: 42 }
                                    : {
                                        userAgent: "fixture-codex/1",
                                        codexHome: "/fixture-home",
                                        platformFamily: "unix",
                                        platformOs: "linux",
                                      },
                              };
                        Queue.offerUnsafe(child.output, bytes(JSON.stringify(response) + "\n"));
                      }
                      return;
                    }
                    assert.strictEqual(text, `RELEASE:${child.nonce}\n`);
                    child.releases++;
                    const args = image.args.slice(5);
                    const code = args.includes("config") ? 1 : 0;
                    const output = args.includes("--git-common-dir")
                      ? "/old/.git\n"
                      : args.includes("--show-toplevel")
                        ? "/old\n"
                        : args.includes("symbolic-ref")
                          ? "refs/heads/main\n"
                          : args.includes("--symbolic-full-name")
                            ? "refs/heads/main\n"
                            : "";
                    Queue.offerUnsafe(child.output, bytes(`EXEC:${child.nonce}\n${output}`));
                    if (!image.args[4]!.endsWith("/codex")) {
                      Queue.endUnsafe(child.output);
                      child.running = false;
                      Deferred.doneUnsafe(child.terminal, Exit.succeed({ code, signal: null }));
                    }
                  }),
                ),
                stdout: Stream.fromQueue(child.output),
                stderr: Stream.empty,
              };
            }),
        };
      }),
  };
  const services = Layer.mergeAll(
    Layer.succeed(AgentScope, scope),
    Layer.succeed(PreparationNode.PreparationNodeChild, node),
    Layer.succeed(HostProcess.Environment, { PATH: "/fixture-bin", CAPTURED: "yes" }),
    Layer.succeed(HostProcess.WorkingDirectory, "/captured"),
    Layer.succeed(HostProcess.HomeDirectory, "/captured-home"),
    Layer.succeed(HostProcess.Platform, "linux"),
    Layer.succeed(SpawnExecutableResolution, (_command, _platform, env) =>
      env.PATH === "/fixture-bin"
        ? _command.endsWith("codex")
          ? "/fixture-bin/codex"
          : "/fixture-bin/git"
        : undefined,
    ),
  );
  return { requests, services, wrapEntered, children };
});
const nativeWorkspaceEntry = Effect.fn("test.nativeWorkspaceEntry")(function* (
  fixture: Effect.Success<ReturnType<typeof controlledNativeFixture>>,
  fileSystem?: FileSystem.FileSystem,
  settings: Parameters<typeof Settings.layerTest>[0] = {},
  orchestrator: typeof fixture.orchestrator = fixture.orchestrator,
) {
  const coreConstruction = makeGitVcsDriverCore().pipe(
    Effect.provideService(ServerConfig, fixture.config),
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make(() => Effect.die("native raw spawn fallback")),
    ),
  );
  const core = yield* (
    fileSystem
      ? coreConstruction.pipe(Effect.provideService(FileSystem.FileSystem, fileSystem))
      : coreConstruction
  ).pipe(Effect.provide(NodeServices.layer));
  return yield* ThreadLaunch.makeNativeWorkspacePreparation(fixture.authority.coordinator).pipe(
    Effect.provideService(GitVcsDriver.GitVcsDriver, core),
    Effect.provideService(Orchestrator.OrchestratorV2, orchestrator),
    Effect.provide(fixture.runtime),
    Effect.provideService(SqlClient.SqlClient, fixture.sql),
    Effect.provideService(ServerConfig, fixture.config),
    Effect.provide(Layer.mergeAll(Settings.layerTest(settings), NodeServices.layer)),
  );
});

it.effect.each(["workflow", "planning"] as const)(
  "native workspace entry connects real %s producer ownership to actual Core and retained resources",
  (kind) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture(kind, false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const entry = yield* nativeWorkspaceEntry(fixture);
        const result = yield* entry.createReservedWorkspace(producerSelection(fixture)).pipe(
          Effect.provideService(HostProcess.Environment, { PATH: "/foreign" }),
          Effect.provideService(HostProcess.HomeDirectory, "/foreign"),
          Effect.provideService(SpawnExecutableResolution, () => "/foreign/git"),
        );
        assert.strictEqual(result.kind, "worktree-created");
        assert.strictEqual(result.runState, "preparing");
        assert.strictEqual(result.preparation.kind, "retired");
        assert.strictEqual(result.worktree.refName, "native-feature");
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT json_extract(payload_json,'$.branch') AS branch,json_extract(payload_json,'$.worktreePath') AS path FROM orchestration_v2_projection_threads WHERE thread_id=${result.native.threadId}`,
          [{ branch: "native-feature", path: result.worktree.path }],
        );
        assert.strictEqual(result.inventory.inventory, "all-retired");
        assert.isAbove(harness.requests.length, 3);
        for (const image of harness.requests) {
          assert.strictEqual(image.args[4], "/fixture-bin/git");
          assert.strictEqual(image.environment.CAPTURED, "yes");
        }
        const resources =
          yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_process_resources`;
        assert.strictEqual(resources.length, harness.requests.length);
        for (const row of resources)
          assert.strictEqual(PreparationResource.decodeRow(row).state, "settled");
        const job = PreparationJob.decodeRow(
          (yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`)[0],
        );
        assert.strictEqual(job.state, "settled");
        assert.isNotNull(job.codeClosedAt);
        if (kind === "workflow") {
          const workspaceEvents =
            yield* fixture.sql`DELETE FROM orchestration_events WHERE command_id=${`${fixture.input.launchCommandId}:native-workspace`} RETURNING event_id`;
          assert.strictEqual(workspaceEvents.length, 1);
          yield* fixture.sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.branch','later-branch','$.worktreePath','/later') WHERE thread_id=${result.native.threadId}`;
          yield* fixture.sessions.revoke(fixture.reference.sessionId);
          const next = reservation(
            operationFor("post-workspace", "post-workspace-attempt"),
            "post-workspace-attempt",
          );
          assert.strictEqual(
            (yield* Effect.result(fixture.store.reserveWorkflowNativeIntent(next)))._tag,
            "Success",
          );
          const original = resources[0]!;
          yield* fixture.sql`DROP TRIGGER preparation_process_resources_update_guard`;
          yield* fixture.sql`UPDATE orchestration_v2_preparation_process_resources SET settlement_hash=${"f".repeat(64)} WHERE resource_id=${original.resource_id}`;
          const bad = yield* Effect.result(fixture.store.reserveWorkflowNativeIntent(next));
          assert.strictEqual(bad._tag, "Failure");
          if (bad._tag === "Failure") assert.strictEqual(bad.failure.reason, "integrity");
          yield* fixture.sql`UPDATE orchestration_v2_preparation_process_resources SET settlement_hash=${original.settlement_hash} WHERE resource_id=${original.resource_id}`;
        }

        assert.isTrue(job.processExposed);
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT status FROM orchestration_v2_projection_runs`,
          [{ status: "preparing" }],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_type IN ('provider-turn.start','provider-turn.restart','provider-runtime.continue')`,
          [],
        );
        assert.strictEqual(
          (yield* Effect.result(entry.createReservedWorkspace(producerSelection(fixture))))._tag,
          "Failure",
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["root", "generated-branch", "from-origin", "revoked", "setup"] as const)(
  "native workspace entry refuses %s before any owned Git creation",
  (mode) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const strategy: ThreadLaunch.ThreadLaunchWorkspaceStrategy =
          mode === "root"
            ? { type: "root" }
            : mode === "generated-branch"
              ? { type: "worktree", baseRef: "main" }
              : {
                  type: "worktree",
                  baseRef: "main",
                  branch: "native-feature",
                  ...(mode === "from-origin" ? { startFromOrigin: true } : {}),
                };
        const fixture = yield* controlledNativeFixture(
          "workflow",
          false,
          false,
          2,
          false,
          strategy,
        );
        const entry = yield* nativeWorkspaceEntry(
          fixture,
          undefined,
          mode === "setup"
            ? {
                defaultProjectScripts: [
                  {
                    id: "setup",
                    name: "Setup",
                    command: "echo setup",
                    icon: "configure",
                    runOnWorktreeCreate: true,
                  },
                ],
              }
            : {},
        );
        if (mode === "revoked") yield* fixture.sessions.revoke(fixture.reference.sessionId);
        const result = yield* Effect.result(
          entry.createReservedWorkspace(producerSelection(fixture)),
        );
        assert.strictEqual(result._tag, "Failure");
        assert.strictEqual(harness.requests.length, 0);
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_process_resources`,
          [],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_type IN ('provider-turn.start','provider-turn.restart','provider-runtime.continue')`,
          [],
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["revoke", "project"] as const)(
  "native workspace entry rechecks %s changes after exposure and before A",
  (change) =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();
      const harness = yield* nativeWorkspaceProcesses(Deferred.await(release));
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("planning", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const entry = yield* nativeWorkspaceEntry(fixture);
        const creating = yield* entry
          .createReservedWorkspace(producerSelection(fixture))
          .pipe(Effect.result, Effect.forkScoped);
        yield* Deferred.await(harness.wrapEntered);
        yield* Effect.gen(function* () {
          if (change === "revoke") yield* fixture.sessions.revoke(fixture.reference.sessionId);
          else
            yield* fixture.sql`UPDATE projection_projects SET workspace_root='/changed' WHERE project_id='project'`;
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        assert.strictEqual((yield* Fiber.join(creating))._tag, "Failure");
        assert.strictEqual(harness.requests.length, 0);
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_process_resources`,
          [],
        );
        const job = PreparationJob.decodeRow(
          (yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`)[0],
        );
        assert.isTrue(job.processExposed);
        assert.strictEqual(job.codeClosedAt, null);
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect(
  "Store scope teardown joins an actual native entry owned by a separate constructor scope",
  () =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      const storeScope = yield* Scope.make();
      const constructorScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(constructorScope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(storeScope, Exit.void));
      const entered = yield* Deferred.make<void>();
      const unwinding = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        }).pipe(Effect.provideService(Scope.Scope, storeScope));
        const actualFs = yield* FileSystem.FileSystem.pipe(Effect.provide(NodeServices.layer));
        const fs: FileSystem.FileSystem = {
          ...actualFs,
          realPath: (path) =>
            path === "/old"
              ? Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(
                    Deferred.succeed(unwinding, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                    ),
                  ),
                )
              : actualFs.realPath(path),
        };
        const entry = yield* nativeWorkspaceEntry(fixture, fs).pipe(
          Effect.provideService(Scope.Scope, constructorScope),
        );
        const creating = yield* entry
          .createReservedWorkspace(producerSelection(fixture))
          .pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(entered);
        const closing = yield* Scope.close(storeScope, Exit.void).pipe(Effect.forkScoped);
        yield* Deferred.await(unwinding);
        yield* Effect.sync(() => {
          assert.isUndefined(closing.pollUnsafe());
          assert.isUndefined(creating.pollUnsafe());
          assert.isAtLeast(harness.requests.length, 1);
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(closing);
        const exit = yield* Fiber.join(creating);
        assert.isTrue(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause));
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_type IN ('provider-turn.start','provider-turn.restart','provider-runtime.continue')`,
          [],
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["caller", "constructor"] as const)(
  "native workspace entry joins %s cancellation without detaching work",
  (mode) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses(Effect.never);
      const constructorScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(constructorScope, Exit.void));
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("planning", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const entry = yield* nativeWorkspaceEntry(fixture).pipe(
          Effect.provideService(Scope.Scope, constructorScope),
        );
        const creating = yield* entry
          .createReservedWorkspace(producerSelection(fixture))
          .pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(harness.wrapEntered);
        if (mode === "caller") yield* Fiber.interrupt(creating);
        else yield* Scope.close(constructorScope, Exit.void);
        const outer = yield* Fiber.await(creating);
        assert.isTrue(
          Exit.isFailure(outer)
            ? Cause.hasInterrupts(outer.cause)
            : Exit.isFailure(outer.value) && Cause.hasInterrupts(outer.value.cause),
        );
        assert.strictEqual(harness.requests.length, 0);
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_process_resources`,
          [],
        );
        const job = PreparationJob.decodeRow(
          (yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`)[0],
        );
        assert.strictEqual(job.codeClosedAt, null);
        assert.isTrue(job.processExposed);
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each([
  "metadata-rejected",
  "missing-event",
  "wrong-sequence",
  "rollback",
  "lost-return",
  "revoked-after-work",
] as const)("native workspace retirement preserves the actual %s boundary", (mode) =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-feature",
      });
      const actual = fixture.orchestrator;
      const orchestrator: typeof actual = {
        ...actual,
        dispatch: (command, hint) => {
          if (command.type !== "thread.metadata.update") return actual.dispatch(command, hint);
          return Effect.gen(function* () {
            const result = yield* actual.dispatch(
              mode === "metadata-rejected"
                ? { ...command, expectedBranch: "conflicting-branch" }
                : command,
              hint,
            );
            yield* Effect.gen(function* () {
              assert.isAbove(harness.requests.length, 3);
              const resources =
                yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_process_resources`;
              for (const row of resources)
                assert.strictEqual(PreparationResource.decodeRow(row).state, "settled");
              const job = PreparationJob.decodeRow(
                (yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`)[0],
              );
              assert.strictEqual(job.codeClosedAt, null);
              if (mode === "missing-event")
                yield* fixture.sql`DELETE FROM orchestration_events WHERE command_id=${command.commandId}`;
              if (mode === "wrong-sequence")
                yield* fixture.sql`UPDATE orchestration_command_receipts SET result_sequence=result_sequence+1 WHERE command_id=${command.commandId}`;
              if (mode === "rollback")
                yield* fixture.sql`CREATE TRIGGER reject_workspace_retirement BEFORE UPDATE ON orchestration_v2_preparation_jobs WHEN NEW.state='settled' BEGIN SELECT RAISE(ABORT,'fixture retirement rollback'); END`;
              if (mode === "revoked-after-work")
                yield* fixture.sessions.revoke(fixture.reference.sessionId);
            }).pipe(Effect.orDie);
            return result;
          });
        },
      };
      const entry = yield* nativeWorkspaceEntry(fixture, undefined, {}, orchestrator);
      const original = fixture.sql.withTransaction;
      let lost = 0;
      using _return = vi
        .spyOn(fixture.sql, "withTransaction")
        .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) =>
          original(work).pipe(
            Effect.flatMap((value) => {
              if (
                mode === "lost-return" &&
                value &&
                typeof value === "object" &&
                "input" in value &&
                "state" in value &&
                value.state === "settled"
              ) {
                lost++;
                return Effect.interrupt;
              }
              return Effect.succeed(value);
            }),
          ),
        );
      const exit = yield* Effect.exit(entry.createReservedWorkspace(producerSelection(fixture)));
      const job = PreparationJob.decodeRow(
        (yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`)[0],
      );
      for (const row of yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_process_resources`)
        assert.strictEqual(PreparationResource.decodeRow(row).state, "settled");
      if (mode === "lost-return") {
        assert.isTrue(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause));
        assert.strictEqual(lost, 1);
        assert.strictEqual(job.state, "settled");
      } else if (mode === "metadata-rejected") {
        assert.isTrue(Exit.isFailure(exit));
        assert.strictEqual(job.state, "active");
        assert.strictEqual(job.codeClosedAt, null);
      } else {
        assert.isTrue(Exit.isSuccess(exit));
        if (Exit.isSuccess(exit)) {
          assert.strictEqual(
            exit.value.inventory.inventory,
            mode === "revoked-after-work" ? "all-retired" : "held",
          );
          assert.strictEqual(
            exit.value.preparation.kind,
            mode === "revoked-after-work" ? "retired" : "held",
          );
        }
        assert.strictEqual(job.state, mode === "revoked-after-work" ? "settled" : "active");
        if (mode !== "revoked-after-work") assert.strictEqual(job.codeClosedAt, null);
      }
      assert.strictEqual(
        (yield* Effect.result(entry.createReservedWorkspace(producerSelection(fixture))))._tag,
        "Failure",
      );
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_type IN ('provider-turn.start','provider-turn.restart','provider-runtime.continue')`,
        [],
      );
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each([false, true])(
  "native retirement joins a blocked SQL writer under outer mask=%s",
  (masked) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      const constructorScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(constructorScope, Exit.void));
      const entered = yield* Deferred.make<void>();
      const unwinding = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        let armed = false;
        const actual = fixture.orchestrator;
        const orchestrator: typeof actual = {
          ...actual,
          dispatch: (command, hint) =>
            actual.dispatch(command, hint).pipe(
              Effect.tap(() =>
                Effect.sync(() => {
                  if (command.type === "thread.metadata.update") armed = true;
                }),
              ),
            ),
        };
        const entry = yield* nativeWorkspaceEntry(fixture, undefined, {}, orchestrator).pipe(
          Effect.provideService(Scope.Scope, constructorScope),
        );
        const original = fixture.sql.withTransaction;
        using _writer = vi
          .spyOn(fixture.sql, "withTransaction")
          .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) => {
            if (!armed) return original(work);
            armed = false;
            return original(
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(
                  Deferred.succeed(unwinding, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                  ),
                ),
              ),
            );
          });
        const creating = yield* entry
          .createReservedWorkspace(producerSelection(fixture))
          .pipe(
            (work) => (masked ? Effect.uninterruptible(work) : work),
            Effect.exit,
            Effect.forkScoped,
          );
        yield* Deferred.await(entered);
        const closing = yield* Scope.close(constructorScope, Exit.void).pipe(Effect.forkScoped);
        yield* Deferred.await(unwinding);
        yield* Effect.sync(() => {
          assert.isUndefined(closing.pollUnsafe());
          assert.isUndefined(creating.pollUnsafe());
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(closing);
        const outer = yield* Fiber.await(creating);
        assert.isTrue(
          Exit.isFailure(outer)
            ? Cause.hasInterrupts(outer.cause)
            : Exit.isFailure(outer.value) && Cause.hasInterrupts(outer.value.cause),
        );
        const job = PreparationJob.decodeRow(
          (yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`)[0],
        );
        assert.strictEqual(job.state, "active");
        assert.strictEqual(job.codeClosedAt, null);
        assert.strictEqual(
          yield* fixture.sql.withTransaction(Effect.succeed("writer released")),
          "writer released",
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_type IN ('provider-turn.start','provider-turn.restart','provider-runtime.continue')`,
          [],
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

const nativeTransportEntry = Effect.fn("test.nativeTransportEntry")(function* (
  fixture: Effect.Success<ReturnType<typeof controlledNativeFixture>>,
  managedPolicy?: Parameters<typeof Settings.layerTest>[1],
) {
  const providerId = ProviderInstanceId.make("provider");
  const initialSettings = Context.get(
    yield* Layer.build(
      Layer.fresh(
        Settings.layerTest(
          {
            providerInstances: {
              [providerId]: {
                driver: "codex",
                enabled: true,
                config: { setupMode: "existing", binaryPath: "codex" },
              },
            },
          },
          managedPolicy,
        ),
      ),
    ),
    Settings.ServerSettingsService,
  );
  const usage = yield* Queue.unbounded<unknown>();
  const versions = yield* Queue.unbounded<string>();
  const facts = {
    settings: yield* initialSettings.getSettings,
    instance: undefined as ProviderInstance | undefined,
    supportedModes: ["approval-required", "full-access"] as ("approval-required" | "full-access")[],
    beforeUsage: Effect.void as Effect.Effect<void>,
    beforeSnapshot: Effect.void as Effect.Effect<void>,
    beforePolicy: Effect.void as Effect.Effect<void>,
    afterPolicy: Effect.void as Effect.Effect<void>,
    beforeCompatibility: Effect.void as Effect.Effect<void>,
    advisory: undefined as ServerProviderCompatibilityAdvisory | undefined,
    usage,
    versions,
  };
  const adapter = {
    instanceId: providerId,
    driver: ProviderDriverKind.make("codex"),
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
    openSession: () => Effect.die("native fixture must not use the registry Adapter open path"),
  } as ProviderAdapter.ProviderAdapterV2["Service"];
  facts.instance = {
    instanceId: providerId,
    driverKind: ProviderDriverKind.make("codex"),
    enabled: true,
    displayName: undefined,
    continuationIdentity: {
      driverKind: ProviderDriverKind.make("codex"),
      continuationKey: "fixture",
    },
    textGeneration: {
      generateTranscriptionPostProcessing: () => Effect.die("native text generation"),
      generateCommitMessage: () => Effect.die("native text generation"),
      generatePrContent: () => Effect.die("native text generation"),
      generateBranchName: () => Effect.die("native text generation"),
      generateThreadTitle: () => Effect.die("native text generation"),
      generateReviewGuide: () => Effect.die("native text generation"),
    },
    orchestrationAdapter: adapter,
    snapshot: {
      getSnapshot: Effect.suspend(() =>
        facts.beforeSnapshot.pipe(
          Effect.andThen(
            Effect.sync(
              () =>
                ({
                  instanceId: providerId,
                  driver: ProviderDriverKind.make("codex"),
                  supportedRuntimeModes: facts.supportedModes,
                  enabled: true,
                  installed: true,
                  version: "1",
                  status: "ready",
                  auth: { status: "authenticated" },
                  checkedAt: "2026-11-14T00:00:00Z",
                  models: [],
                  slashCommands: [],
                  skills: [],
                }) satisfies ServerProvider,
            ),
          ),
        ),
      ),
      applyUsageLimits: (update: unknown) =>
        facts.beforeUsage.pipe(Effect.andThen(Queue.offer(usage, update)), Effect.asVoid),
      refresh: Effect.die("native fixture must not refresh or probe a provider"),
      resolveMaintenance: () => Effect.die("native fixture must not resolve maintenance"),
      streamChanges: Stream.empty,
    },
  };
  const registry = ProviderInstances.ProviderInstanceRegistry.of({
    getInstance: (id) => Effect.sync(() => (id === providerId ? facts.instance : undefined)),
    listInstances: Effect.sync(() => (facts.instance ? [facts.instance] : [])),
    listUnavailable: Effect.succeed([]),
    streamChanges: Stream.empty,
    subscribeChanges: Effect.never,
  });
  const settings = Settings.ServerSettingsService.of({
    ...initialSettings,
    getSettings: Effect.sync(() => facts.settings),
  });
  const projects = Context.get(
    yield* Layer.build(
      Layer.fresh(
        ProjectStore.layer.pipe(Layer.provide(Layer.succeed(SqlClient.SqlClient, fixture.sql))),
      ),
    ),
    ProjectStore.ProjectStoreV2,
  );
  const policy = Context.get(
    yield* Layer.build(
      RuntimePolicy.layerFromProjectStore.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(ProjectStore.ProjectStoreV2, projects),
            Layer.succeed(ProviderInstances.ProviderInstanceRegistry, registry),
            Layer.succeed(Settings.ServerSettingsService, settings),
          ),
        ),
      ),
    ),
    RuntimePolicy.RuntimePolicyV2,
  );
  const installation = CodexInstallation.CodexInstallation.of({
    managedDirectory: "/unused-installation",
    resolve: () => Effect.die("native fixture must not resolve an installation"),
    acquire: () => Effect.die("native fixture must not acquire an installation"),
    compatibility: (version) =>
      facts.beforeCompatibility.pipe(
        Effect.andThen(Queue.offer(versions, version)),
        Effect.andThen(Effect.sync(() => facts.advisory)),
      ),
    start: Effect.die("native fixture must not start installation"),
    cancel: () => Effect.die("native fixture must not cancel installation"),
    state: Effect.die("native fixture must not read installation state"),
    changes: Stream.empty,
    remove: () => Effect.die("native fixture must not remove installation"),
  });
  const core = yield* makeGitVcsDriverCore().pipe(
    Effect.provideService(ServerConfig, fixture.config),
    Effect.provideService(
      ChildProcessSpawner.ChildProcessSpawner,
      ChildProcessSpawner.make(() => Effect.die("native raw fallback")),
    ),
    Effect.provide(NodeServices.layer),
  );
  const service = yield* NativeTransport.makeNativeCodexTransport(
    fixture.authority.coordinator,
  ).pipe(
    Effect.provideService(GitVcsDriver.GitVcsDriver, core),
    Effect.provide(fixture.runtime),
    Effect.provideService(SqlClient.SqlClient, fixture.sql),
    Effect.provideService(ServerConfig, fixture.config),
    Effect.provideService(Settings.ServerSettingsService, settings),
    Effect.provideService(ProviderInstances.ProviderInstanceRegistry, registry),
    Effect.provideService(CodexInstallation.CodexInstallation, installation),
    Effect.provideService(
      RuntimePolicy.RuntimePolicyV2,
      RuntimePolicy.RuntimePolicyV2.of({
        resolve: (input) =>
          facts.beforePolicy.pipe(
            Effect.andThen(policy.resolve(input)),
            Effect.tap(() => facts.afterPolicy),
          ),
      }),
    ),
    Effect.provide(
      Layer.mergeAll(
        NodeServices.layer,
        IdAllocator.layer,
        McpProviderSessions.layer,
        Continuations.layer,
      ),
    ),
  );
  return { ...service, facts };
});
it.effect.each(["workflow", "planning"] as const)(
  "real native %s transport owns the original retired workspace and denies every SDK frame",
  (kind) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture(kind, false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        assert.isFunction(fixture.store.consumeNativeCodexTransportOwner);
        const service = yield* nativeTransportEntry(fixture);
        const transport = yield* service
          .createReservedTransport(producerSelection(fixture))
          .pipe(Effect.provideService(HostProcess.Environment, { PATH: "/foreign" }));
        assert.strictEqual(transport.kind, "transport-created");
        assert.strictEqual(transport.runState, "preparing");
        assert.strictEqual(transport.preparation.kind, "retired");
        assert.isTrue(yield* transport.client.ownedProcess.isRunning);
        assert.strictEqual(
          harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex").length,
          1,
        );
        const rows = yield* fixture.sql`SELECT * FROM orchestration_v2_submission_transports`;
        assert.strictEqual(rows.length, 1);
        assert.strictEqual(rows[0]!.transport_id, transport.transportId);
        assert.isNotNull(rows[0]!.binding_json);
        assert.strictEqual(rows[0]!.settlement_json, null);
        assert.deepStrictEqual(
          JSON.parse(rows[0]!.creation_context_json as string).expectedClaim,
          transport.executionClaim,
        );
        assert.deepStrictEqual(
          transport.executionClaim,
          transport.preparation.job.input.nativeOwner!.executionClaim,
        );
        const denied = yield* transport.client
          .request("initialize", { clientInfo: { name: "not-started", version: "1" } })
          .pipe(Effect.result);
        assert.strictEqual(denied._tag, "Failure");
        if (denied._tag === "Failure")
          assert.strictEqual(denied.failure._tag, "CodexAppServerFrameSubmissionError");
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_submission_attempts`,
          [],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT status FROM orchestration_v2_projection_runs`,
          [{ status: "preparing" }],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_type IN ('provider-turn.start','provider-turn.restart','provider-runtime.continue')`,
          [],
        );
        assert.strictEqual((yield* transport.close()).kind, "retired");
        assert.strictEqual((yield* transport.close()).kind, "retired");
        assert.isFalse(yield* transport.client.ownedProcess.isRunning);
        assert.isNotNull(
          (yield* fixture.sql`SELECT settlement_json FROM orchestration_v2_submission_transports`)[0]!
            .settlement_json,
        );
        assert.strictEqual(
          (yield* Effect.result(service.createReservedTransport(producerSelection(fixture))))._tag,
          "Failure",
        );
        assert.strictEqual(
          harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex").length,
          1,
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect(
  "parallel parent and explicit transport close join actual SDK shutdown before settlement",
  () =>
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-transport-close-")),
        ),
        (value) => Effect.sync(() => NodeFS.rmSync(value, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      yield* Effect.gen(function* () {
        const parent = yield* Scope.make("parallel");
        yield* Effect.addFinalizer(() => Scope.close(parent, Exit.void));
        const harness = yield* nativeWorkspaceProcesses();
        const entered = yield* Deferred.make<void>();
        const unwinding = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        yield* Effect.gen(function* () {
          const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
            type: "worktree",
            baseRef: "main",
            branch: "native-feature",
          }).pipe(Effect.provideService(Scope.Scope, parent));
          yield* fixture.sql`PRAGMA journal_mode=WAL`;
          const service = yield* nativeTransportEntry(fixture).pipe(
            Effect.provideService(Scope.Scope, parent),
          );
          const transport = yield* service.createReservedTransport(producerSelection(fixture));
          yield* transport.client.handleUnknownServerRequest(() =>
            Deferred.succeed(entered, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(
                Deferred.succeed(unwinding, undefined).pipe(
                  Effect.andThen(Deferred.await(release)),
                ),
              ),
            ),
          );
          const image = harness.requests.find(
            (request) => request.args[4] === "/fixture-bin/codex",
          )!;
          const child = harness.children.get(image.args[3]!)!;
          yield* Queue.offer(
            child.output,
            new TextEncoder().encode(
              JSON.stringify({
                jsonrpc: "2.0",
                id: "blocked-handler",
                method: "fixture/hold",
                params: {},
              }) + "\n",
            ),
          );
          yield* Deferred.await(entered);
          using other = new NodeSqlite.DatabaseSync(filename, { readOnly: true });
          const closing = yield* Scope.close(parent, Exit.void).pipe(Effect.forkScoped);
          yield* Deferred.await(unwinding);
          const explicit = yield* transport
            .close()
            .pipe(Effect.forkScoped({ startImmediately: true }));
          yield* Effect.sync(() => {
            assert.isUndefined(closing.pollUnsafe());
            assert.isUndefined(explicit.pollUnsafe());
            assert.deepStrictEqual(
              {
                ...other
                  .prepare(
                    "SELECT settlement_json FROM orchestration_v2_submission_transports WHERE transport_id=?",
                  )
                  .get(transport.transportId),
              },
              { settlement_json: null },
            );
          }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
          assert.strictEqual((yield* Fiber.join(explicit)).kind, "retired");
          yield* Fiber.join(closing);
          assert.isFalse(yield* transport.client.ownedProcess.isRunning);
          assert.isString(
            other
              .prepare(
                "SELECT settlement_json FROM orchestration_v2_submission_transports WHERE transport_id=?",
              )
              .get(transport.transportId)!.settlement_json,
          );
        }).pipe(Effect.provide(harness.services));
      }).pipe(Effect.provide(Layer.mergeAll(testLayer(filename), Digest.layerWebCrypto)));
    }),
);

it.effect.each([
  "lost-insert",
  "lost-create",
  "lost-binding",
  "lost-release",
  "lost-settlement",
  "revoke-before-create",
  "revoke-before-release",
  "revoke-after-release",
] as const)("native transport preserves the original %s outcome without adoption", (mode) =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-feature",
      });
      const service = yield* nativeTransportEntry(fixture);
      const original = fixture.sql.withTransaction;
      let intercepted = 0;
      using _return = vi
        .spyOn(fixture.sql, "withTransaction")
        .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) =>
          original(work).pipe(
            Effect.flatMap((value) =>
              Effect.gen(function* () {
                const row =
                  value && typeof value === "object" && "transport_id" in value
                    ? (value as Record<string, unknown>)
                    : undefined;
                const image = harness.requests.find(
                  (image) => image.args[4] === "/fixture-bin/codex",
                );
                const child = image ? harness.children.get(image.args[3]!) : undefined;
                const target =
                  ((mode === "lost-insert" || mode === "revoke-before-create") &&
                    row?.binding_json === null) ||
                  ((mode === "lost-binding" || mode === "revoke-before-release") &&
                    row?.binding_json != null &&
                    row.settlement_json === null) ||
                  (mode === "lost-create" &&
                    value === undefined &&
                    child?.running &&
                    child.releases === 0) ||
                  ((mode === "lost-release" || mode === "revoke-after-release") &&
                    value === undefined &&
                    child &&
                    child.releases > 0) ||
                  (mode === "lost-settlement" && row?.settlement_json != null);
                if (!target || intercepted) return value;
                intercepted++;
                if (mode.startsWith("revoke")) {
                  yield* fixture.sessions.revoke(fixture.reference.sessionId);
                  return value;
                }
                return yield* Effect.interrupt;
              }),
            ),
          ),
        );
      const result = yield* Effect.exit(
        service.createReservedTransport(producerSelection(fixture)),
      );
      if (mode === "lost-settlement" || mode === "revoke-after-release") {
        assert.isTrue(Exit.isSuccess(result));
        if (Exit.isSuccess(result)) {
          const close = yield* Effect.exit(result.value.close());
          const again = yield* Effect.exit(result.value.close());
          if (mode === "lost-settlement") {
            assert.isTrue(Exit.isSuccess(close));
            if (Exit.isSuccess(close)) assert.strictEqual(close.value.kind, "held");
            assert.deepStrictEqual(again, close);
          } else {
            assert.isTrue(Exit.isSuccess(close));
            if (Exit.isSuccess(close)) assert.strictEqual(close.value.kind, "retired");
            assert.deepStrictEqual(again, close);
          }
        }
      } else assert.isTrue(Exit.isFailure(result));
      assert.strictEqual(intercepted, 1);
      const rows = yield* fixture.sql`SELECT * FROM orchestration_v2_submission_transports`;
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(
        typeof rows[0]!.settlement_json === "string",
        mode === "lost-settlement" || mode === "revoke-after-release",
      );
      const codex = harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex");
      assert.strictEqual(
        codex.length,
        mode === "lost-insert" || mode === "revoke-before-create" ? 0 : 1,
      );
      if (codex[0]) {
        const child = harness.children.get(codex[0].args[3]!)!;
        assert.isFalse(child.running);
        assert.strictEqual(
          child.releases,
          ["lost-release", "lost-settlement", "revoke-after-release"].includes(mode) ? 1 : 0,
        );
      }
      assert.strictEqual(
        (yield* Effect.result(service.createReservedTransport(producerSelection(fixture))))._tag,
        "Failure",
      );
      assert.strictEqual(
        harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex").length,
        codex.length,
      );
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_submission_attempts`,
        [],
      );
      yield* Effect.log("native transport fault", mode, {
        intercepted,
        creates: codex.length,
        bound: rows[0]!.binding_json !== null,
        settled: rows[0]!.settlement_json !== null,
      });
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect(
  "native transport consumes only the original fresh retired object and owns command bytes",
  () =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const entry = yield* nativeWorkspaceEntry(fixture);
        const prepared = yield* entry.createReservedWorkspace(producerSelection(fixture));
        if (prepared.preparation.kind !== "retired") throw new Error("workspace not retired");
        const job = prepared.preparation.job;
        const command = {
          command: "/fixture-bin/codex",
          args: ["app-server"],
          cwd: prepared.worktree.path,
          env: { PATH: "/fixture-bin", OWNED: "original" },
        };
        const ordinary = yield* Store.make();
        assert.strictEqual(
          (yield* Effect.result(ordinary.consumeNativeCodexTransportOwner(job, command)))._tag,
          "Failure",
        );
        for (const clone of [{ ...job }, structuredClone(job)])
          assert.strictEqual(
            (yield* Effect.result(fixture.store.consumeNativeCodexTransportOwner(clone, command)))
              ._tag,
            "Failure",
          );
        const acquiring = fixture.store.consumeNativeCodexTransportOwner(job, command);
        command.args.push("foreign");
        command.env.OWNED = "mutated";
        const owner = yield* acquiring;
        const managerSource = yield* Store.readNativeManagerOwner(
          fixture.store,
          owner,
          fixture.sql,
        );
        for (const [store, facade, client] of [
          [{ ...fixture.store }, owner, fixture.sql],
          [fixture.store, { ...owner }, fixture.sql],
          [ordinary, owner, fixture.sql],
          [fixture.store, owner, Object.assign(fixture.sql.bind(undefined), fixture.sql)],
        ] as const)
          assert.isTrue(
            Exit.isFailure(yield* Effect.exit(Store.readNativeManagerOwner(store, facade, client))),
          );
        assert.isTrue(Exit.isFailure(yield* Effect.exit(managerSource.readConstruction())));
        assert.strictEqual(
          (yield* Effect.result(fixture.store.consumeNativeCodexTransportOwner(job, command)))._tag,
          "Failure",
        );
        assert.strictEqual((yield* Effect.result(owner.open()))._tag, "Failure");
        const io = yield* Effect.gen(function* () {
          yield* owner.captureConstructor();
          return yield* owner.open();
        }).pipe(Effect.forkScoped, Effect.flatMap(Fiber.join));
        assert.isTrue(yield* io.isRunning);
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        assert.deepStrictEqual(image.args.slice(5), ["app-server"]);
        assert.strictEqual(image.environment.OWNED, "original");
        assert.strictEqual((yield* owner.close()).kind, "retired");
        assert.strictEqual((yield* Effect.result(owner.open()))._tag, "Failure");
        assert.isFalse(yield* io.isRunning);
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["revoke", "pause", "stop", "deadline", "project", "workspace"] as const)(
  "native transport denies %s before durable transport admission",
  (mode) =>
    Effect.gen(function* () {
      let mutate: Effect.Effect<void> = Effect.void;
      const harness = yield* nativeWorkspaceProcesses(
        Effect.void,
        Effect.suspend(() => mutate),
      );
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const service = yield* nativeTransportEntry(fixture);
        mutate = Effect.gen(function* () {
          if (mode === "revoke") yield* fixture.sessions.revoke(fixture.reference.sessionId);
          else if (mode === "pause" || mode === "stop") yield* controlNativeFixture(fixture, mode);
          else if (mode === "deadline")
            yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
          else if (mode === "project")
            yield* fixture.sql`UPDATE projection_projects SET deleted_at=${facts.now} WHERE project_id='project'`;
          else
            yield* fixture.sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.worktreePath','/foreign')`;
        }).pipe(Effect.orDie);
        assert.strictEqual(
          (yield* Effect.result(service.createReservedTransport(producerSelection(fixture))))._tag,
          "Failure",
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_submission_transports`,
          [],
        );
        assert.strictEqual(
          harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex").length,
          0,
        );
        const job = PreparationJob.decodeRow(
          (yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`)[0],
        );
        assert.strictEqual(job.state, "settled");
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["before", "after"] as const)(
  "native transport fences a child attempt inserted %s cleanup",
  (order) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const service = yield* nativeTransportEntry(fixture);
        const transport = yield* service.createReservedTransport(producerSelection(fixture));
        const current = transport.preparation.job.input.request.current as Record<string, string>;
        const encoded =
          JSON.stringify({
            id: 1,
            method: "turn/start",
            params: {
              threadId: "native-future",
              input: [{ type: "text", text: "Work" }],
            },
          }) + "\n";
        const child = snapshotSubmissionFrame(
          {
            transportId: transport.transportId,
            environmentId: "environment",
            projectId: "project",
            driver: "codex",
            providerInstanceId: current.providerInstanceId!,
            providerSessionId: current.providerSessionId!,
          },
          {
            encoded,
            byteLength: new TextEncoder().encode(encoded).byteLength,
            method: "turn/start",
            requestId: 1,
            selection: JSON.stringify({
              v: 1,
              kind: "start",
              threadId: transport.native.threadId,
              runId: transport.native.runId,
              runOrdinal: 1,
              attemptId: "future-attempt",
              rootNodeId: "future-root",
              providerTurnOrdinal: 1,
              providerInstanceId: current.providerInstanceId,
              providerSessionId: current.providerSessionId,
              providerThreadId: current.providerThreadId,
              nativeThreadId: "native-future",
              invocation: 1,
              expectedClaim: transport.executionClaim,
            }),
          },
          facts.now,
        );
        if (order === "before")
          yield* fixture.sql`INSERT INTO orchestration_v2_submission_attempts ${fixture.sql.insert(child)}`;
        assert.strictEqual(
          (yield* transport.close()).kind,
          order === "before" ? "held" : "retired",
        );
        assert.isFalse(yield* transport.client.ownedProcess.isRunning);
        const row = (yield* fixture.sql`SELECT * FROM orchestration_v2_submission_transports`)[0]!;
        assert.strictEqual(row.settlement_json === null, order === "before");
        if (order === "after")
          assert.strictEqual(
            (yield* Effect.result(
              fixture.sql`INSERT INTO orchestration_v2_submission_attempts ${fixture.sql.insert(child)}`,
            ))._tag,
            "Failure",
          );
        assert.strictEqual(
          (yield* transport.close()).kind,
          order === "before" ? "held" : "retired",
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(
  (["create", "release"] as const).flatMap((stage) =>
    (["session", "native", "project", "generation", "deadline", "pause"] as const).map(
      (change) => ({ stage, change }),
    ),
  ),
)("native transport rechecks $change before $stage", ({ stage, change }) =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("planning", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-feature",
      });
      const service = yield* nativeTransportEntry(fixture);
      const original = fixture.sql.withTransaction;
      let changed = false;
      using _boundary = vi
        .spyOn(fixture.sql, "withTransaction")
        .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) =>
          original(work).pipe(
            Effect.flatMap((value) =>
              Effect.gen(function* () {
                if (!value || typeof value !== "object" || !("transport_id" in value) || changed)
                  return value;
                const row = value as Record<string, unknown>;
                if ((row.binding_json !== null) !== (stage === "release")) return value;
                changed = true;
                if (change === "deadline")
                  yield* TestClock.setTime(
                    DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds,
                  );
                else if (change === "pause") yield* controlNativeFixture(fixture, "pause");
                else if (change === "project")
                  yield* fixture.sql`UPDATE projection_projects SET deleted_at=${facts.now} WHERE project_id='project'`;
                else if (change === "generation") {
                  yield* fixture.sql`UPDATE workflow_planning_sessions SET control_generation=2,record_version=3,record_json=json_set(record_json,'$.controlGeneration',2,'$.recordVersion',3)`;
                  yield* fixture.sql`UPDATE workflow_operations SET generation=2,record_json=json_set(record_json,'$.controlGeneration',2,'$.claim.generation',2)`;
                } else if (change === "session")
                  yield* fixture.sql`UPDATE orchestration_v2_projection_provider_threads SET provider_session_id='foreign-session',payload_json=json_set(payload_json,'$.providerSessionId','foreign-session')`;
                else
                  yield* fixture.sql`UPDATE orchestration_v2_projection_provider_threads SET native_thread_id='foreign-native',payload_json=json_set(payload_json,'$.nativeThreadId','foreign-native')`;
                return value;
              }),
            ),
          ),
        );
      assert.strictEqual(
        (yield* Effect.result(service.createReservedTransport(producerSelection(fixture))))._tag,
        "Failure",
      );
      assert.isTrue(changed);
      const images = harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex");
      assert.strictEqual(images.length, stage === "create" ? 0 : 1);
      for (const image of images) {
        const child = harness.children.get(image.args[3]!)!;
        assert.strictEqual(child.releases, 0);
        assert.isFalse(child.running);
      }
      assert.strictEqual(
        (yield* fixture.sql`SELECT settlement_json FROM orchestration_v2_submission_transports`)[0]!
          .settlement_json,
        null,
      );
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

vi.mock("effect/PubSub", { spy: true });
it.effect.each(["insert", "binding", "settlement"] as const)(
  "native transport never retries after lost %s notification",
  (stage) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const actual = yield* Effect.promise(() =>
          vi.importActual<typeof import("effect/PubSub")>("effect/PubSub"),
        );
        let armed = false;
        let losses = 0;
        using _notify = vi.spyOn(PubSub, "publish").mockImplementation((pubsub, value) =>
          Effect.suspend(() => {
            if (!armed) return actual.publish(pubsub, value);
            armed = false;
            losses++;
            return Effect.interrupt;
          }),
        );
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const service = yield* nativeTransportEntry(fixture);
        const original = fixture.sql.withTransaction;
        using _commit = vi
          .spyOn(fixture.sql, "withTransaction")
          .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) =>
            original(work).pipe(
              Effect.tap((value) =>
                Effect.sync(() => {
                  if (losses || !value || typeof value !== "object" || !("transport_id" in value))
                    return;
                  const row = value as Record<string, unknown>;
                  armed =
                    stage === "insert"
                      ? row.binding_json === null
                      : stage === "binding"
                        ? row.binding_json !== null && row.settlement_json === null
                        : row.settlement_json !== null;
                }),
              ),
            ),
          );
        const exit = yield* Effect.exit(
          service.createReservedTransport(producerSelection(fixture)),
        );
        if (stage === "settlement") {
          assert.isTrue(Exit.isSuccess(exit));
          if (Exit.isSuccess(exit)) {
            const close = yield* Effect.exit(exit.value.close());
            assert.isTrue(Exit.isSuccess(close));
            if (Exit.isSuccess(close)) assert.strictEqual(close.value.kind, "held");
          }
        } else assert.isTrue(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause));
        assert.strictEqual(losses, 1);
        const row = (yield* fixture.sql`SELECT * FROM orchestration_v2_submission_transports`)[0]!;
        assert.strictEqual(row.settlement_json !== null, stage === "settlement");
        const images = harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex");
        assert.strictEqual(images.length, stage === "insert" ? 0 : 1);
        for (const image of images) assert.isFalse(harness.children.get(image.args[3]!)!.running);
        assert.strictEqual(
          (yield* Effect.result(service.createReservedTransport(producerSelection(fixture))))._tag,
          "Failure",
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(
  ([false, true] as const).flatMap((masked) =>
    (["caller", "constructor", "store"] as const).map((boundary) => ({ masked, boundary })),
  ),
)(
  "native transport joins SQL unwind at $boundary under outer mask=$masked",
  ({ masked, boundary }) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      const storeScope = yield* Scope.make();
      const constructorScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(constructorScope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(storeScope, Exit.void));
      const entered = yield* Deferred.make<void>();
      const unwinding = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        }).pipe(Effect.provideService(Scope.Scope, storeScope));
        const service = yield* nativeTransportEntry(fixture).pipe(
          Effect.provideService(Scope.Scope, constructorScope),
        );
        const original = fixture.sql.withTransaction;
        let armed = false;
        let intercepted = false;
        using _writer = vi
          .spyOn(fixture.sql, "withTransaction")
          .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) => {
            if (armed) {
              armed = false;
              intercepted = true;
              return original(
                Deferred.succeed(entered, undefined).pipe(
                  Effect.andThen(Effect.never),
                  Effect.ensuring(
                    Deferred.succeed(unwinding, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                    ),
                  ),
                ),
              );
            }
            return original(work).pipe(
              Effect.tap((value) =>
                Effect.sync(() => {
                  if (!intercepted && value && typeof value === "object" && "transport_id" in value)
                    armed = true;
                }),
              ),
            );
          });
        const creating = yield* service
          .createReservedTransport(producerSelection(fixture))
          .pipe(
            (work) => (masked ? Effect.uninterruptible(work) : work),
            Effect.exit,
            Effect.forkScoped,
          );
        yield* Deferred.await(entered);
        const stopping = yield* (
          boundary === "caller"
            ? Fiber.interrupt(creating)
            : Scope.close(boundary === "store" ? storeScope : constructorScope, Exit.void)
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(unwinding);
        yield* Effect.sync(() => {
          assert.isUndefined(stopping.pollUnsafe());
          assert.isUndefined(creating.pollUnsafe());
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(stopping);
        const exit = yield* Fiber.await(creating);
        assert.isTrue(
          Exit.isFailure(exit)
            ? Cause.hasInterrupts(exit.cause)
            : Exit.isFailure(exit.value) && Cause.hasInterrupts(exit.value.cause),
        );
        assert.strictEqual(
          harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex").length,
          0,
        );
        const row = (yield* fixture.sql`SELECT * FROM orchestration_v2_submission_transports`)[0]!;
        assert.strictEqual(row.binding_json, null);
        assert.strictEqual(row.settlement_json, null);
        assert.strictEqual(
          yield* fixture.sql.withTransaction(Effect.succeed("writer released")),
          "writer released",
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["revoke", "release"] as const)(
  "native transport orders independent grant revocation with %s first",
  (first) =>
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-transport-grant-")),
        ),
        (value) => Effect.sync(() => NodeFS.rmSync(value, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      yield* Effect.gen(function* () {
        using competitor = new NodeSqlite.DatabaseSync(filename);
        let probes = 0;
        const harness = yield* nativeWorkspaceProcesses(Effect.void, Effect.void, (image) => {
          if (image.args[4] !== "/fixture-bin/codex") return;
          probes++;
          assert.throws(() => competitor.exec("BEGIN IMMEDIATE"), /locked/);
        });
        yield* Effect.gen(function* () {
          const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
            type: "worktree",
            baseRef: "main",
            branch: "native-feature",
          });
          yield* fixture.sql`PRAGMA journal_mode=WAL`;
          const sqlB = Context.get(
            yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
            SqlClient.SqlClient,
          );
          const sessionsB = yield* AuthSessions.make.pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          );
          const revoke = sessionsB.revoke({
            sessionId: fixture.reference.sessionId,
            revokedAt: yield* DateTime.now,
          });
          const service = yield* nativeTransportEntry(fixture);
          const original = fixture.sql.withTransaction;
          let revoked = false;
          using _order = vi
            .spyOn(fixture.sql, "withTransaction")
            .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) =>
              original(work).pipe(
                Effect.tap((value) =>
                  Effect.gen(function* () {
                    if (
                      first !== "revoke" ||
                      revoked ||
                      !value ||
                      typeof value !== "object" ||
                      !("binding_json" in value) ||
                      value.binding_json === null
                    )
                      return;
                    revoked = true;
                    assert.isTrue(yield* revoke);
                  }),
                ),
              ),
            );
          const result = yield* Effect.result(
            service.createReservedTransport(producerSelection(fixture)),
          );
          assert.strictEqual(result._tag, first === "revoke" ? "Failure" : "Success");
          if (result._tag === "Success") {
            assert.isTrue(yield* revoke);
            assert.strictEqual((yield* result.success.close()).kind, "retired");
          }
          assert.strictEqual(probes, 1);
          const row = (yield* sqlB`SELECT * FROM orchestration_v2_submission_transports`)[0]!;
          assert.isNotNull(row.binding_json);
          assert.strictEqual(row.settlement_json !== null, first === "release");
          const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
          const child = harness.children.get(image.args[3]!)!;
          assert.strictEqual(child.releases, first === "release" ? 1 : 0);
          assert.isFalse(child.running);
          const reopened = yield* proofOrchestrator(
            true,
            Effect.void,
            fixture.authority,
            fixture.config,
          ).pipe(Effect.provideService(SqlClient.SqlClient, sqlB));
          const historical = PreparationJob.decodeRow(
            (yield* sqlB`SELECT * FROM orchestration_v2_preparation_jobs`)[0],
          );
          const freshStore = Context.get(reopened, Store.WorkflowRunStore);
          assert.strictEqual(
            (yield* Effect.result(
              freshStore.consumeNativeCodexTransportOwner(historical, {
                command: "/fixture-bin/codex",
                args: ["app-server"],
                cwd: "/historical",
                env: {},
              }),
            ))._tag,
            "Failure",
          );
          yield* Effect.log("independent native transport ordering", {
            first,
            writerLockProbes: probes,
            releases: child.releases,
            settled: row.settlement_json !== null,
          });
        }).pipe(Effect.provide(harness.services));
      }).pipe(Effect.provide(Layer.mergeAll(testLayer(filename), Digest.layerWebCrypto)));
    }),
);

it.effect.each(["thread", "session", "changed-original"] as const)(
  "native transport own-row exception preserves foreign %s custody",
  (mode) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const service = yield* nativeTransportEntry(fixture);
        const original = fixture.sql.withTransaction;
        let inserted = false;
        using _foreign = vi
          .spyOn(fixture.sql, "withTransaction")
          .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) =>
            original(work).pipe(
              Effect.tap((value) =>
                Effect.gen(function* () {
                  if (inserted || !value || typeof value !== "object" || !("transport_id" in value))
                    return;
                  const row = value as Record<string, unknown>;
                  if (row.binding_json !== null) return;
                  inserted = true;
                  if (mode === "changed-original") {
                    const binding = encodeScopeBindingData(
                      {
                        unit: row.proposed_unit,
                        invocationId: "b".repeat(32),
                        controlGroup: `/user.slice/${row.scope_namespace}/${row.proposed_unit}`,
                        pid: 9999,
                        pidStartTicks: "9999",
                      },
                      row,
                    );
                    yield* fixture.sql`UPDATE orchestration_v2_submission_transports SET binding_json=${binding.json},binding_hash=${binding.hash},bound_at=${facts.now} WHERE transport_id=${row.transport_id}`;
                  } else {
                    const context = JSON.parse(row.creation_context_json as string) as Record<
                      string,
                      unknown
                    >;
                    const foreign = encodeSubmissionTransportData({
                      transportId: "99999999-9999-4999-8999-999999999999",
                      proposedUnit: "t3code-codex-99999999-9999-4999-8999-999999999999.scope",
                      creationInputHash: "f".repeat(64),
                      createdAt: facts.now,
                      context: {
                        ...context,
                        ...(mode === "thread"
                          ? { providerSessionId: "another-session" }
                          : { openingThreadId: "another-thread" }),
                      },
                    });
                    yield* fixture.sql`INSERT INTO orchestration_v2_submission_transports ${fixture.sql.insert(foreign)}`;
                  }
                }),
              ),
            ),
          );
        assert.strictEqual(
          (yield* Effect.result(service.createReservedTransport(producerSelection(fixture))))._tag,
          "Failure",
        );
        assert.isTrue(inserted);
        assert.strictEqual(
          harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex").length,
          0,
        );
        const rows = yield* fixture.sql`SELECT * FROM orchestration_v2_submission_transports`;
        assert.strictEqual(rows.length, mode === "changed-original" ? 1 : 2);
        for (const row of rows) assert.strictEqual(row.settlement_json, null);
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["workflow", "planning"] as const)(
  "native %s bootstrap initializes the same retained client exactly once",
  (kind) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture(kind, false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const service = yield* nativeTransportEntry(fixture);
        const transport = yield* service.createReservedTransport(producerSelection(fixture));
        assert.isFunction(transport.bootstrap);
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        assert.strictEqual(
          (yield* Effect.result(
            transport.client.raw.request("initialize", { clientInfo: { name: "forged" } }),
          ))._tag,
          "Failure",
        );
        assert.strictEqual(child.payloads.length, 0);
        const initialized = yield* transport.bootstrap();
        assert.strictEqual(initialized.kind, "protocol-initialized");
        assert.strictEqual(initialized.transportId, transport.transportId);
        assert.strictEqual(initialized.response.userAgent, "fixture-codex/1");
        assert.strictEqual(child.payloads.length, 2);
        const [initialize, notification] = child.payloads.map((text) => JSON.parse(text));
        assert.strictEqual(initialize.id, 2);
        assert.strictEqual(initialize.method, "initialize");
        assert.deepStrictEqual(initialize.params.capabilities, {
          experimentalApi: true,
          optOutNotificationMethods: ["turn/diff/updated"],
          extensions: {
            "io.modelcontextprotocol/ui": { mimeTypes: ["text/html;profile=mcp-app"] },
          },
        });
        assert.deepStrictEqual(notification, { method: "initialized" });
        assert.isTrue(child.payloads.every((text) => !text.includes("selection")));
        assert.strictEqual((yield* Effect.result(transport.bootstrap()))._tag, "Failure");
        assert.strictEqual(
          (yield* Effect.result(transport.client.request("account/read", {})))._tag,
          "Failure",
        );
        assert.strictEqual(child.payloads.length, 2);
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT status FROM orchestration_v2_projection_runs`,
          [{ status: "preparing" }],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_projection_provider_sessions`,
          [],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_submission_attempts`,
          [],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_effect_outbox WHERE effect_type IN ('provider-turn.start','provider-turn.restart','provider-runtime.continue')`,
          [],
        );
        assert.strictEqual((yield* transport.close()).kind, "retired");
        assert.isFalse(child.running);
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect("native transport refuses handler close before touching the physical owner", () =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-feature",
      });
      const service = yield* nativeTransportEntry(fixture);
      const transport = yield* service.createReservedTransport(producerSelection(fixture));
      const closed =
        yield* Queue.unbounded<
          Exit.Exit<
            Effect.Success<ReturnType<typeof transport.close>>,
            Effect.Error<ReturnType<typeof transport.close>>
          >
        >();
      yield* transport.client.handleUnknownServerRequest(() =>
        transport.close().pipe(
          Effect.exit,
          Effect.flatMap((exit) => Queue.offer(closed, exit)),
          Effect.as({ ok: true }),
        ),
      );
      const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
      const child = harness.children.get(image.args[3]!)!;
      yield* Queue.offer(
        child.output,
        new TextEncoder().encode(
          JSON.stringify({
            id: "self-close",
            method: "fixture/close",
            params: {},
          }) + "\n",
        ),
      );
      const attempted = yield* Queue.take(closed);
      assert.isTrue(Exit.isFailure(attempted));
      if (Exit.isFailure(attempted)) {
        const error = Cause.findErrorOption(attempted.cause);
        assert.isTrue(Option.isSome(error));
        if (Option.isSome(error) && "reason" in error.value)
          assert.strictEqual(error.value.reason, "reentrant");
      }
      assert.isTrue(child.running);
      assert.strictEqual(
        (yield* fixture.sql`SELECT settlement_json FROM orchestration_v2_submission_transports`)[0]!
          .settlement_json,
        null,
      );
      assert.strictEqual((yield* transport.close()).kind, "retired");
      assert.isFalse(child.running);
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["invalid", "already", "missing", "wrong-id"] as const)(
  "native bootstrap refuses %s initialize responses without a second frame",
  (mode) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const service = yield* nativeTransportEntry(fixture);
        const transport = yield* service.createReservedTransport(producerSelection(fixture));
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        child.initializeReply = mode;
        const boot = yield* transport.bootstrap().pipe(Effect.exit, Effect.forkScoped);
        yield* Queue.take(child.payloadWritten);
        if (mode === "missing" || mode === "wrong-id") {
          assert.strictEqual(
            (yield* Effect.result(transport.client.raw.notify("fixture/local-completion-barrier")))
              ._tag,
            "Failure",
          );
          yield* TestClock.adjust("9 seconds");
          assert.isUndefined(boot.pollUnsafe());
          yield* TestClock.adjust("1 second");
        }
        const result = yield* Fiber.join(boot);
        assert.isTrue(Exit.isFailure(result));
        assert.strictEqual(child.payloads.length, 1);
        assert.isFalse(child.running);
        assert.strictEqual((yield* transport.close()).kind, "held");
        assert.strictEqual((yield* Effect.result(transport.bootstrap()))._tag, "Failure");
        assert.strictEqual(child.payloads.length, 1);
        assert.strictEqual(
          (yield* fixture.sql`SELECT settlement_json FROM orchestration_v2_submission_transports`)[0]!
            .settlement_json,
          null,
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each([
  "revoke-before",
  "deadline-before",
  "revoke-after",
  "pause-after",
  "session-after",
  "lost-first-return",
  "lost-second-return",
] as const)("native bootstrap preserves the original %s writer boundary", (mode) =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("planning", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-feature",
      });
      const service = yield* nativeTransportEntry(fixture);
      const transport = yield* service.createReservedTransport(producerSelection(fixture));
      const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
      const child = harness.children.get(image.args[3]!)!;
      if (mode === "revoke-before") yield* fixture.sessions.revoke(fixture.reference.sessionId);
      if (mode === "deadline-before")
        yield* TestClock.setTime(DateTime.makeUnsafe("2026-12-02T00:00:00Z").epochMilliseconds);
      const original = fixture.sql.withTransaction;
      let intercepted = 0;
      using _boundary = vi
        .spyOn(fixture.sql, "withTransaction")
        .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) =>
          original(work).pipe(
            Effect.flatMap((value) =>
              Effect.gen(function* () {
                const target = mode === "lost-second-return" ? 2 : 1;
                if (
                  value !== undefined ||
                  child.payloads.length !== target ||
                  intercepted ||
                  mode.endsWith("before")
                )
                  return value;
                intercepted++;
                if (mode === "revoke-after")
                  yield* fixture.sessions.revoke(fixture.reference.sessionId);
                else if (mode === "pause-after") yield* controlNativeFixture(fixture, "pause");
                else if (mode === "session-after")
                  yield* fixture.sql`UPDATE orchestration_v2_projection_provider_threads SET provider_session_id='changed',payload_json=json_set(payload_json,'$.providerSessionId','changed')`;
                else return yield* Effect.interrupt;
                return value;
              }),
            ),
          ),
        );
      const result = yield* Effect.exit(transport.bootstrap());
      assert.isTrue(Exit.isFailure(result));
      assert.strictEqual(intercepted, mode.endsWith("before") ? 0 : 1);
      assert.strictEqual(
        child.payloads.length,
        mode.endsWith("before") ? 0 : mode === "lost-second-return" ? 2 : 1,
      );
      assert.isFalse(child.running);
      assert.strictEqual((yield* transport.close()).kind, "held");
      const count = child.payloads.length;
      assert.strictEqual((yield* Effect.result(transport.bootstrap()))._tag, "Failure");
      assert.strictEqual(child.payloads.length, count);
      assert.strictEqual(
        (yield* fixture.sql`SELECT settlement_json FROM orchestration_v2_submission_transports`)[0]!
          .settlement_json,
        null,
      );
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each([
  "empty",
  "extra-field",
  "extra-params",
  "null-id",
  "string-id",
  "fraction-id",
  "duplicate-key",
  "whitespace",
  "two-lines",
  "invalid-utf8",
  "oversize",
  "two-chunks",
  "bom",
] as const)("native bootstrap guarded Sink rejects %s before any Gate payload write", (mode) =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-feature",
      });
      const entry = yield* nativeWorkspaceEntry(fixture);
      const prepared = yield* entry.createReservedWorkspace(producerSelection(fixture));
      if (prepared.preparation.kind !== "retired") throw new Error("workspace not retired");
      const owner = yield* fixture.store.consumeNativeCodexTransportOwner(
        prepared.preparation.job,
        {
          command: "/fixture-bin/codex",
          args: ["app-server"],
          cwd: prepared.worktree.path,
          env: { PATH: "/fixture-bin" },
        },
      );
      const io = yield* Effect.gen(function* () {
        yield* owner.captureConstructor();
        return yield* owner.open();
      }).pipe(Effect.forkScoped, Effect.flatMap(Fiber.join));
      const sent = yield* Effect.gen(function* () {
        const profile = yield* owner.captureBootstrap();
        const frame: Record<string, unknown> = {
          id: 5,
          method: "initialize",
          params: profile.params,
        };
        if (mode === "extra-field") frame.extra = true;
        if (mode === "extra-params") frame.params = { ...profile.params, extra: true };
        if (mode === "null-id") frame.id = null;
        if (mode === "string-id") frame.id = "5";
        if (mode === "fraction-id") frame.id = 1.5;
        let text = JSON.stringify(frame) + "\n";
        if (mode === "duplicate-key") text = text.replace('{"id":5,', '{"id":5,"id":5,');
        if (mode === "whitespace") text = " " + text;
        if (mode === "two-lines") text += text;
        const bytes =
          mode === "oversize"
            ? new Uint8Array(1048577)
            : mode === "invalid-utf8"
              ? new Uint8Array([255, 10])
              : mode === "bom"
                ? new Uint8Array([239, 187, 191, ...new TextEncoder().encode(text)])
                : new TextEncoder().encode(text);
        const stream =
          mode === "empty"
            ? Stream.empty
            : mode === "two-chunks"
              ? Stream.make(bytes, bytes)
              : Stream.make(bytes);
        return yield* Effect.exit(Stream.run(stream, io.stdin));
      }).pipe(Effect.forkScoped, Effect.flatMap(Fiber.join));
      assert.isTrue(Exit.isFailure(sent));
      const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
      const child = harness.children.get(image.args[3]!)!;
      assert.deepStrictEqual(child.payloads, []);
      assert.strictEqual((yield* owner.close()).kind, "held");
      assert.isFalse(child.running);
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect("native SQL close refusal leaves SDK output and bootstrap untouched", () =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-feature",
      });
      const service = yield* nativeTransportEntry(fixture);
      const transport = yield* service.createReservedTransport(producerSelection(fixture));
      assert.strictEqual(
        (yield* Effect.result(fixture.sql.withTransaction(transport.close())))._tag,
        "Failure",
      );
      assert.isTrue(yield* transport.client.ownedProcess.isRunning);
      assert.strictEqual((yield* Effect.result(transport.bootstrap()))._tag, "Success");
      assert.strictEqual((yield* transport.close()).kind, "retired");
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(
  ([false, true] as const).flatMap((masked) =>
    (["caller", "constructor", "store"] as const).map((boundary) => ({ masked, boundary })),
  ),
)(
  "native bootstrap joins actual SQL unwind at $boundary under mask=$masked",
  ({ masked, boundary }) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      const storeScope = yield* Scope.make();
      const constructorScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(constructorScope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(storeScope, Exit.void));
      const entered = yield* Deferred.make<void>();
      const unwinding = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        }).pipe(Effect.provideService(Scope.Scope, storeScope));
        const service = yield* nativeTransportEntry(fixture).pipe(
          Effect.provideService(Scope.Scope, constructorScope),
        );
        const transport = yield* service.createReservedTransport(producerSelection(fixture));
        const original = fixture.sql.withTransaction;
        let armed = true;
        using _writer = vi
          .spyOn(fixture.sql, "withTransaction")
          .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) => {
            if (!armed) return original(work);
            armed = false;
            return original(
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(
                  Deferred.succeed(unwinding, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                  ),
                ),
              ),
            );
          });
        const boot = yield* transport
          .bootstrap()
          .pipe(
            (work) => (masked ? Effect.uninterruptible(work) : work),
            Effect.exit,
            Effect.forkScoped,
          );
        yield* Deferred.await(entered);
        const stopping = yield* (
          boundary === "caller"
            ? Fiber.interrupt(boot)
            : Scope.close(boundary === "store" ? storeScope : constructorScope, Exit.void)
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(unwinding);
        yield* Effect.sync(() => {
          assert.isUndefined(stopping.pollUnsafe());
          assert.isUndefined(boot.pollUnsafe());
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(stopping);
        const exit = yield* Fiber.await(boot);
        assert.isTrue(
          Exit.isFailure(exit)
            ? Cause.hasInterrupts(exit.cause)
            : Exit.isFailure(exit.value) && Cause.hasInterrupts(exit.value.cause),
          Exit.isFailure(exit)
            ? Cause.pretty(exit.cause)
            : Exit.isFailure(exit.value)
              ? Cause.pretty(exit.value.cause)
              : "unexpected success",
        );
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        assert.deepStrictEqual(child.payloads, []);
        assert.isFalse(child.running);
        assert.strictEqual(
          (yield* fixture.sql`SELECT settlement_json FROM orchestration_v2_submission_transports`)[0]!
            .settlement_json,
          null,
        );
        assert.strictEqual(
          yield* fixture.sql.withTransaction(Effect.succeed("writer released")),
          "writer released",
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["revoke", "initialized"] as const)(
  "native bootstrap orders an independent grant writer with %s first",
  (first) =>
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-bootstrap-grant-")),
        ),
        (value) => Effect.sync(() => NodeFS.rmSync(value, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      yield* Effect.gen(function* () {
        using competing = new NodeSqlite.DatabaseSync(filename);
        let probes = 0;
        const harness = yield* nativeWorkspaceProcesses(
          Effect.void,
          Effect.void,
          () => {},
          () => {
            probes++;
            assert.throws(() => competing.exec("BEGIN IMMEDIATE"), /locked/);
          },
        );
        yield* Effect.gen(function* () {
          const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
            type: "worktree",
            baseRef: "main",
            branch: "native-feature",
          });
          yield* fixture.sql`PRAGMA journal_mode=WAL`;
          const sqlB = Context.get(
            yield* Layer.build(Layer.fresh(NodeSqliteClient.layer({ filename }))),
            SqlClient.SqlClient,
          );
          const sessionsB = yield* AuthSessions.make.pipe(
            Effect.provideService(SqlClient.SqlClient, sqlB),
          );
          const revoke = sessionsB.revoke({
            sessionId: fixture.reference.sessionId,
            revokedAt: yield* DateTime.now,
          });
          const service = yield* nativeTransportEntry(fixture);
          const transport = yield* service.createReservedTransport(producerSelection(fixture));
          const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
          const child = harness.children.get(image.args[3]!)!;
          const original = fixture.sql.withTransaction;
          let revoked = false;
          using _order = vi
            .spyOn(fixture.sql, "withTransaction")
            .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) =>
              original(work).pipe(
                Effect.tap((value) =>
                  Effect.gen(function* () {
                    if (
                      first !== "revoke" ||
                      revoked ||
                      value !== undefined ||
                      child.payloads.length !== 1
                    )
                      return;
                    revoked = true;
                    assert.isTrue(yield* revoke);
                  }),
                ),
              ),
            );
          const result = yield* Effect.result(transport.bootstrap());
          assert.strictEqual(result._tag, first === "revoke" ? "Failure" : "Success");
          if (first === "initialized") assert.isTrue(yield* revoke);
          assert.strictEqual(
            (yield* transport.close()).kind,
            first === "revoke" ? "held" : "retired",
          );
          assert.strictEqual(probes, first === "revoke" ? 1 : 2);
          assert.strictEqual(child.payloads.length, probes);
          assert.isFalse(child.running);
          assert.strictEqual(
            (yield* sqlB`SELECT settlement_json FROM orchestration_v2_submission_transports`)[0]!
              .settlement_json !== null,
            first === "initialized",
          );
        }).pipe(Effect.provide(harness.services));
      }).pipe(Effect.provide(Layer.mergeAll(testLayer(filename), Digest.layerWebCrypto)));
    }),
);

it.effect.each(["snapshot", "stop-unmasked", "stop-masked"] as const)(
  "native bootstrap input collection preserves its actual %s body boundary",
  (mode) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      const waiting = yield* Deferred.make<void>();
      const unwinding = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const mutable = yield* Deferred.make<Uint8Array>();
      const rest = yield* Queue.unbounded<Uint8Array, Cause.Done>();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const entry = yield* nativeWorkspaceEntry(fixture);
        const prepared = yield* entry.createReservedWorkspace(producerSelection(fixture));
        if (prepared.preparation.kind !== "retired") throw new Error("workspace not retired");
        const owner = yield* fixture.store.consumeNativeCodexTransportOwner(
          prepared.preparation.job,
          {
            command: "/fixture-bin/codex",
            args: ["app-server"],
            cwd: prepared.worktree.path,
            env: { PATH: "/fixture-bin" },
          },
        );
        const io = yield* Effect.gen(function* () {
          yield* owner.captureConstructor();
          return yield* owner.open();
        }).pipe(Effect.forkScoped, Effect.flatMap(Fiber.join));
        const body = yield* Effect.gen(function* () {
          const profile = yield* owner.captureBootstrap();
          const bytes = new TextEncoder().encode(
            JSON.stringify({ id: 7, method: "initialize", params: profile.params }) + "\n",
          );
          yield* Deferred.succeed(mutable, bytes);
          const remaining =
            mode === "snapshot"
              ? Stream.fromQueue(rest)
              : Stream.never.pipe(
                  Stream.ensuring(
                    Deferred.succeed(unwinding, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                    ),
                  ),
                );
          const source = Stream.concat(
            Stream.make(bytes),
            Stream.unwrap(Deferred.succeed(waiting, undefined).pipe(Effect.as(remaining))),
          );
          yield* Stream.run(source, io.stdin);
        }).pipe(
          (work) => (mode === "stop-masked" ? Effect.uninterruptible(work) : work),
          Effect.exit,
          Effect.forkScoped,
        );
        yield* Deferred.await(waiting);
        if (mode === "snapshot") {
          const bytes = yield* Deferred.await(mutable);
          bytes.fill(32);
          yield* Queue.end(rest);
          assert.isTrue(Exit.isSuccess(yield* Fiber.join(body)));
          assert.strictEqual((yield* owner.close()).kind, "held");
        } else {
          const closing = yield* owner.close().pipe(Effect.forkScoped);
          yield* Deferred.await(unwinding);
          yield* Effect.sync(() => {
            assert.isUndefined(closing.pollUnsafe());
            assert.isUndefined(body.pollUnsafe());
          }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
          assert.strictEqual((yield* Fiber.join(closing)).kind, "held");
          const bodyExit = yield* Fiber.await(body);
          assert.isTrue(
            Exit.isFailure(bodyExit)
              ? Cause.hasInterrupts(bodyExit.cause)
              : Exit.isFailure(bodyExit.value) && Cause.hasInterrupts(bodyExit.value.cause),
          );
        }
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        assert.isFalse(child.running);
        assert.strictEqual(child.payloads.length, mode === "snapshot" ? 1 : 0);
        if (mode === "snapshot")
          assert.strictEqual(JSON.parse(child.payloads[0]!).method, "initialize");
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["workflow", "planning"] as const)(
  "native %s constructs its real unpublished Adapter on the original client",
  (kind) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture(kind, false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const service = yield* nativeTransportEntry(fixture);
        const transport = yield* service.createReservedTransport(producerSelection(fixture));
        const bootstrap = yield* transport.bootstrap();
        Object.assign(bootstrap.response, { userAgent: "public-mutation/999" });
        assert.isFunction(transport.createAdapterRuntime);
        const initializedAgain = yield* Effect.result(transport.bootstrap());
        assert.strictEqual(initializedAgain._tag, "Failure");
        assert.isTrue(Object.isFrozen(transport.client));
        assert.isTrue(Object.isFrozen(transport.client.raw));
        assert.strictEqual(
          (yield* Effect.result(fixture.sql.withTransaction(transport.createAdapterRuntime())))
            ._tag,
          "Failure",
        );
        const result = yield* transport.createAdapterRuntime();
        assert.strictEqual(result.kind, "adapter-runtime-created");
        assert.strictEqual(result.transportId, transport.transportId);
        assert.strictEqual(
          result.runtime.providerSession.id,
          (transport.preparation.job.input.request.current as Record<string, unknown>)
            .providerSessionId,
        );
        assert.isTrue(Object.isFrozen(result.runtime));
        assert.isTrue(Object.isFrozen(result.runtime.providerSession));
        assert.isFalse("input" in result);
        assert.strictEqual(yield* Queue.take(service.facts.versions), "1");
        assert.strictEqual(
          (yield* Effect.result(transport.client.request("account/read", {})))._tag,
          "Failure",
        );
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        assert.strictEqual(child.payloads.length, 2);
        yield* Queue.offer(
          child.output,
          new TextEncoder().encode(
            JSON.stringify({
              method: "account/rateLimits/updated",
              params: {
                rateLimits: {
                  limitId: "codex",
                  primary: { usedPercent: 20, resetsAt: 2000100000, windowDurationMins: 300 },
                },
              },
            }) + "\n",
          ),
        );
        assert.isObject(yield* Queue.take(service.facts.usage));
        assert.strictEqual(
          (yield* Effect.result(transport.createAdapterRuntime()))._tag,
          "Failure",
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_projection_provider_sessions`,
          [],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT status FROM orchestration_v2_projection_runs`,
          [{ status: "preparing" }],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_submission_attempts`,
          [],
        );
        assert.strictEqual((yield* transport.close()).kind, "retired");
        assert.isFalse(child.running);
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(
  (["before-process", "before-adapter", "during-compatibility"] as const).flatMap((stage) =>
    (["missing", "replaced", "disabled", "auth", "config", "supported-modes"] as const).map(
      (change) => ({ stage, change }),
    ),
  ),
)("native Adapter refuses $change profile $stage", ({ stage, change }) =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-feature",
      });
      const service = yield* nativeTransportEntry(fixture);
      const mutate = Effect.sync(() => {
        const instance = service.facts.instance!;
        if (change === "missing") service.facts.instance = undefined;
        else if (change === "replaced") service.facts.instance = { ...instance };
        else if (change === "disabled") service.facts.instance = { ...instance, enabled: false };
        else if (change === "auth")
          service.facts.instance = {
            ...instance,
            auth: {
              start: () => Effect.die("native auth must not start"),
              complete: () => Effect.die("native auth must not complete"),
              cancel: () => Effect.die("native auth must not cancel"),
              logout: () => Effect.die("native auth must not logout"),
              withAccess: () => Effect.die("native auth must not acquire"),
              subscribe: () => Stream.die("native auth must not subscribe"),
            },
          };
        else if (change === "supported-modes") service.facts.supportedModes = ["approval-required"];
        else {
          const id = instance.instanceId;
          const selected = service.facts.settings.providerInstances[id]!;
          service.facts.settings = {
            ...service.facts.settings,
            providerInstances: {
              ...service.facts.settings.providerInstances,
              [id]: { ...selected, config: { setupMode: "managed", binaryPath: "codex" } },
            },
          };
        }
      });
      if (stage === "before-process") {
        yield* mutate;
        const result = yield* Effect.result(
          service.createReservedTransport(producerSelection(fixture)),
        );
        // Replacement and capability snapshots are captured at this boundary.
        assert.strictEqual(
          result._tag,
          change === "replaced" || change === "supported-modes" ? "Success" : "Failure",
        );
        if (result._tag === "Success") yield* result.success.close();
        else
          assert.strictEqual(
            harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex").length,
            0,
          );
        return;
      }
      const transport = yield* service.createReservedTransport(producerSelection(fixture));
      yield* transport.bootstrap();
      if (stage === "before-adapter") yield* mutate;
      else service.facts.beforeCompatibility = mutate;
      assert.isTrue(Exit.isFailure(yield* Effect.exit(transport.createAdapterRuntime())));
      const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
      const child = harness.children.get(image.args[3]!)!;
      assert.strictEqual(child.payloads.length, 2);
      assert.isFalse(child.running);
      assert.strictEqual((yield* transport.close()).kind, "held");
      assert.strictEqual((yield* Effect.result(transport.createAdapterRuntime()))._tag, "Failure");
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_projection_provider_sessions`,
        [],
      );
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each([
  "revoke",
  "pause",
  "generation",
  "workspace",
  "mode",
  "model",
  "project",
  "broken",
  "ceiling",
  "dead",
  "lost-return",
] as const)("native Adapter holds its original owner after %s during construction", (change) =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("planning", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-feature",
      });
      const service = yield* nativeTransportEntry(
        fixture,
        change === "ceiling"
          ? {
              organization: "Fixture",
              lockedSettings: [],
              limits: { maxRuntimeMode: "approval-required" },
            }
          : undefined,
      );
      const transport = yield* service.createReservedTransport(producerSelection(fixture));
      assert.strictEqual((yield* Effect.result(transport.createAdapterRuntime()))._tag, "Failure");
      yield* transport.bootstrap();
      const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
      const child = harness.children.get(image.args[3]!)!;
      service.facts.beforeCompatibility = Effect.gen(function* () {
        if (change === "revoke") yield* fixture.sessions.revoke(fixture.reference.sessionId);
        else if (change === "pause") yield* controlNativeFixture(fixture, "pause");
        else if (change === "generation") {
          yield* fixture.sql`UPDATE workflow_planning_sessions SET control_generation=2,record_version=3,record_json=json_set(record_json,'$.controlGeneration',2,'$.recordVersion',3)`;
          yield* fixture.sql`UPDATE workflow_operations SET generation=2,record_json=json_set(record_json,'$.controlGeneration',2,'$.claim.generation',2)`;
        } else if (change === "workspace")
          yield* fixture.sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.worktreePath','/foreign')`;
        else if (change === "mode")
          yield* fixture.sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.runtimeMode','approval-required')`;
        else if (change === "model")
          yield* fixture.sql`UPDATE orchestration_v2_projection_runs SET payload_json=json_set(payload_json,'$.modelSelection.model','foreign')`;
        else if (change === "project")
          yield* fixture.sql`UPDATE projection_projects SET deleted_at=${facts.now} WHERE project_id='project'`;
        else if (change === "dead") {
          child.running = false;
          yield* Deferred.succeed(child.terminal, { code: 1, signal: null });
          yield* Queue.end(child.output);
        }
      }).pipe(Effect.orDie);
      if (change === "broken")
        service.facts.advisory = {
          status: "broken",
          message: null,
          recommendedVersion: "1.0.0",
          recommendedRange: null,
        };
      const original = fixture.sql.withTransaction;
      let lost = false;
      using _writer = vi
        .spyOn(fixture.sql, "withTransaction")
        .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) =>
          original(work).pipe(
            Effect.flatMap((value) => {
              if (
                change !== "lost-return" ||
                lost ||
                !value ||
                typeof value !== "object" ||
                !("key" in value)
              )
                return Effect.succeed(value);
              lost = true;
              return Effect.interrupt;
            }),
          ),
        );
      assert.isTrue(Exit.isFailure(yield* Effect.exit(transport.createAdapterRuntime())));
      assert.strictEqual(lost, change === "lost-return");
      assert.strictEqual(child.payloads.length, 2);
      assert.isFalse(child.running);
      assert.strictEqual((yield* transport.close()).kind, "held");
      assert.strictEqual((yield* Effect.result(transport.createAdapterRuntime()))._tag, "Failure");
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_projection_provider_sessions`,
        [],
      );
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_submission_attempts`,
        [],
      );
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(
  ([false, true] as const).flatMap((masked) =>
    (["caller", "constructor", "store"] as const).flatMap((boundary) =>
      (["policy", "compatibility", "sql"] as const).map((stage) => ({ masked, boundary, stage })),
    ),
  ),
)(
  "native Adapter joins $stage unwind at $boundary under mask=$masked",
  ({ masked, boundary, stage }) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      const storeScope = yield* Scope.make();
      const constructorScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(constructorScope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(storeScope, Exit.void));
      const entered = yield* Deferred.make<void>();
      const unwinding = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        }).pipe(Effect.provideService(Scope.Scope, storeScope));
        const service = yield* nativeTransportEntry(fixture).pipe(
          Effect.provideService(Scope.Scope, constructorScope),
        );
        const transport = yield* service.createReservedTransport(producerSelection(fixture));
        yield* transport.bootstrap();
        const blocked = Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Deferred.succeed(unwinding, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        );
        if (stage === "policy") service.facts.beforePolicy = blocked;
        if (stage === "compatibility") service.facts.beforeCompatibility = blocked;
        const original = fixture.sql.withTransaction;
        let armed = stage === "sql";
        using _writer = vi
          .spyOn(fixture.sql, "withTransaction")
          .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) => {
            if (!armed) return original(work);
            armed = false;
            return original(blocked);
          });
        const constructing = yield* transport
          .createAdapterRuntime()
          .pipe(
            (work) => (masked ? Effect.uninterruptible(work) : work),
            Effect.exit,
            Effect.forkScoped,
          );
        yield* Deferred.await(entered);
        const stopping = yield* (
          boundary === "caller"
            ? Fiber.interrupt(constructing)
            : Scope.close(boundary === "store" ? storeScope : constructorScope, Exit.void)
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(unwinding);
        yield* Effect.sync(() => {
          assert.isUndefined(stopping.pollUnsafe());
          assert.isUndefined(constructing.pollUnsafe());
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(stopping);
        const exit = yield* Fiber.await(constructing);
        assert.isTrue(
          Exit.isFailure(exit)
            ? Cause.hasInterrupts(exit.cause)
            : Exit.isFailure(exit.value) && Cause.hasInterrupts(exit.value.cause),
        );
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        assert.strictEqual(child.payloads.length, 2);
        assert.isFalse(child.running);
        assert.strictEqual(
          (yield* fixture.sql`SELECT settlement_json FROM orchestration_v2_submission_transports`)[0]!
            .settlement_json,
          null,
        );
        assert.strictEqual(
          yield* fixture.sql.withTransaction(Effect.succeed("writer released")),
          "writer released",
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["policy", "compatibility", "final-profile", "final-current"] as const)(
  "native Adapter times out its original %s without retry",
  (stage) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("planning", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const service = yield* nativeTransportEntry(fixture);
        const transport = yield* service.createReservedTransport(producerSelection(fixture));
        yield* transport.bootstrap();
        const blocked = Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        );
        if (stage === "policy") service.facts.beforePolicy = blocked;
        else if (stage === "compatibility") service.facts.beforeCompatibility = blocked;
        let policies = 0;
        let writes = 0;
        if (stage === "final-profile" || stage === "final-current")
          service.facts.afterPolicy = Effect.sync(() => {
            if (++policies === 2 && stage === "final-profile")
              service.facts.beforeSnapshot = blocked;
          });
        const original = fixture.sql.withTransaction;
        using _writer = vi
          .spyOn(fixture.sql, "withTransaction")
          .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) => {
            if (stage === "final-current" && policies === 2 && ++writes === 2)
              return original(blocked);
            return original(work);
          });
        const constructing = yield* transport
          .createAdapterRuntime()
          .pipe(Effect.exit, Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* TestClock.adjust("10 seconds");
        yield* Deferred.succeed(release, undefined);
        assert.isTrue(Exit.isFailure(yield* Fiber.join(constructing)));
        assert.strictEqual((yield* transport.close()).kind, "held");
        assert.strictEqual(
          (yield* Effect.result(transport.createAdapterRuntime()))._tag,
          "Failure",
        );
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        assert.strictEqual(child.payloads.length, 2);
        assert.isFalse(child.running);
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["workflow", "planning"] as const)(
  "native %s runtime joins the actual Adapter notification handler before settlement",
  (kind) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      const entered = yield* Deferred.make<void>();
      const unwinding = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture(kind, false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-feature",
        });
        const service = yield* nativeTransportEntry(fixture);
        const transport = yield* service.createReservedTransport(producerSelection(fixture));
        yield* transport.bootstrap();
        yield* transport.createAdapterRuntime();
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        service.facts.beforeUsage = Effect.gen(function* () {
          const reentrant = yield* Effect.result(transport.close());
          assert.strictEqual(reentrant._tag, "Failure");
          yield* Deferred.succeed(entered, undefined);
          return yield* Effect.never;
        }).pipe(
          Effect.ensuring(
            Deferred.succeed(unwinding, undefined).pipe(Effect.andThen(Deferred.await(release))),
          ),
        );
        yield* Queue.offer(
          child.output,
          new TextEncoder().encode(
            JSON.stringify({
              method: "account/rateLimits/updated",
              params: {
                rateLimits: {
                  limitId: "codex",
                  primary: { usedPercent: 20, resetsAt: 2000100000, windowDurationMins: 300 },
                },
              },
            }) + "\n",
          ),
        );
        yield* Deferred.await(entered);
        const closing = yield* transport.close().pipe(Effect.forkScoped);
        yield* Deferred.await(unwinding);
        yield* Effect.gen(function* () {
          assert.isUndefined(closing.pollUnsafe());
          assert.strictEqual(
            (yield* fixture.sql`SELECT settlement_json FROM orchestration_v2_submission_transports`)[0]!
              .settlement_json,
            null,
          );
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        assert.strictEqual((yield* Fiber.join(closing)).kind, "retired");
        assert.isFalse(child.running);
        assert.strictEqual(child.payloads.length, 2);
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect(
  "native workflow and planning transports keep independent sessions and workspaces live together",
  () =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-overlap-workflow",
        });
        const siblingSession = decodePlanningSessionFixture({
          ...session,
          id: "native-overlap-planning",
        });
        yield* fixture.store.beginPlanning({
          session: siblingSession,
          now: facts.now,
          context,
          command: { commandId: "begin-native-overlap", receiptId: "begin-native-overlap" },
        });
        const reservationInput = planningReservation(
          admissionInput(fixture.binding, "overlap", siblingSession),
        );
        const input = {
          ...reservationInput,
          materialVersion: 2 as const,
          authority: fixture.reference,
          launch: {
            ...reservationInput.launch,
            reuseExistingThread: false,
            workspaceStrategy: {
              type: "worktree" as const,
              baseRef: "main",
              branch: "native-overlap-planning",
            },
          },
        };
        yield* fixture.store.reservePlanningNativeIntent(input);
        const derivation = yield* Derivation.make.pipe(
          Effect.provideService(Store.WorkflowRunStore, fixture.store),
        );
        const derived = yield* fixture.sql.withTransaction(
          derivation.deriveInTransaction({ intentId: input.intentId }),
        );
        const sibling = { ...fixture, kind: "planning" as const, input, derived };
        const service = yield* nativeTransportEntry(fixture);
        const first = yield* service.createReservedTransport(producerSelection(fixture));
        yield* first.bootstrap();
        const firstRuntime = yield* first.createAdapterRuntime();
        const second = yield* service.createReservedTransport(producerSelection(sibling));
        yield* second.bootstrap();
        const secondRuntime = yield* second.createAdapterRuntime();
        assert.notStrictEqual(
          firstRuntime.runtime.providerSession.id,
          secondRuntime.runtime.providerSession.id,
        );
        assert.notStrictEqual(
          firstRuntime.runtime.providerSession.cwd,
          secondRuntime.runtime.providerSession.cwd,
        );
        assert.isTrue(yield* first.client.ownedProcess.isRunning);
        assert.isTrue(yield* second.client.ownedProcess.isRunning);
        const paths = (yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`).map(
          (row) => {
            const job = PreparationJob.decodeRow(row);
            if (job.codeClosedAt === null) throw new Error("workspace evidence missing");
            return PreparationJob.ownWorkspaceSettlement(
              job.settlement,
              job.input,
              job.recordVersion - 1,
              job.codeClosedAt,
            ).workspace.path;
          },
        );
        assert.strictEqual(new Set(paths).size, 2);
        assert.deepStrictEqual(
          paths.toSorted(),
          [
            firstRuntime.runtime.providerSession.cwd,
            secondRuntime.runtime.providerSession.cwd,
          ].toSorted(),
        );
        const children = harness.requests
          .filter((image) => image.args[4] === "/fixture-bin/codex")
          .map((image) => harness.children.get(image.args[3]!)!);
        assert.strictEqual(children.length, 2);
        assert.isTrue(children.every((child) => child.running && child.payloads.length === 2));
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT status FROM orchestration_v2_projection_runs ORDER BY run_id`,
          [{ status: "preparing" }, { status: "preparing" }],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_projection_provider_sessions`,
          [],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_submission_attempts`,
          [],
        );
        assert.strictEqual((yield* first.close()).kind, "retired");
        assert.isTrue(yield* second.client.ownedProcess.isRunning);
        assert.strictEqual((yield* second.close()).kind, "retired");
        assert.isTrue(children.every((child) => !child.running));
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect(
  "native allocation preserves ordinary sharing and exact historical shared-session replay after reopen",
  () =>
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.sync(() =>
          NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-session-replay-")),
        ),
        (value) => Effect.sync(() => NodeFS.rmSync(value, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      const saved = yield* Effect.scoped(
        Effect.gen(function* () {
          const fixture = yield* controlledNativeFixture("workflow");
          const decisions = yield* Effect.gen(function* () {
            yield* runMigrations();
            yield* seedProject;
            const ordinary = Context.get(
              yield* proofOrchestrator(false, Effect.void, undefined, fixture.config),
              Orchestrator.OrchestratorV2,
            );
            const claim = yield* ordinary.dispatch(fixture.derived.claimCommand);
            const message = yield* ordinary.dispatch(fixture.derived.messageCommand);
            const otherThread = ThreadId.make("ordinary-sharing-control");
            yield* ordinary.dispatch(
              existingThreadCommand(fixture.derived, "ordinary-sharing-create", otherThread),
            );
            const other = yield* ordinary.dispatch({
              ...fixture.derived.messageCommand,
              threadId: otherThread,
              commandId: CommandId.make("ordinary-sharing-message"),
              messageId: MessageId.make("ordinary-sharing-message"),
            });
            const sessionOf = (result: typeof message) => {
              const changed = result.storedEvents.find(
                ({ event }) => event.type === "provider-thread.updated",
              );
              if (changed?.event.type !== "provider-thread.updated")
                throw new Error("provider thread missing");
              return changed.event.payload.providerSessionId;
            };
            const shared = Context.get(
              yield* Layer.build(IdAllocator.layer),
              IdAllocator.IdAllocatorV2,
            ).derive.providerSession({ providerInstanceId: ProviderInstanceId.make("provider") });
            assert.strictEqual(sessionOf(message), shared);
            assert.strictEqual(sessionOf(other), shared);
            assert.deepStrictEqual(
              yield* (yield* SqlClient.SqlClient)`SELECT * FROM orchestration_v2_effect_outbox`,
              [],
            );
            return { claim, message, shared };
          }).pipe(Effect.provide(Layer.fresh(testLayer())));
          const commit = Context.get(fixture.runtime, EventSink.EventSinkV2).commitNativeCommand;
          if (!commit) throw new Error("native writer missing");
          for (const [command, phase, result] of [
            [fixture.derived.claimCommand, "claim", decisions.claim],
            [fixture.derived.messageCommand, "message", decisions.message],
          ] as const) {
            const events = result.storedEvents.map((stored) => stored.event);
            yield* commit({
              command,
              hint: { intentId: fixture.input.intentId, phase },
              acceptedAt: events.at(-1)!.occurredAt,
              events,
              effects: [],
            });
          }
          const rows = {
            receipts:
              yield* fixture.sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
            proofs:
              yield* fixture.sql`SELECT * FROM workflow_native_command_proofs ORDER BY command_id`,
            events: yield* fixture.sql`SELECT * FROM orchestration_events ORDER BY sequence`,
          };
          assert.strictEqual(rows.proofs.length, 2);
          return {
            config: fixture.config,
            derived: fixture.derived,
            intentId: fixture.input.intentId,
            rows,
            shared: decisions.shared,
          };
        }).pipe(Effect.provide(testLayer(filename))),
      );
      yield* Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const runtime = yield* proofOrchestrator(true, Effect.void, undefined, saved.config);
        const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
        assert.isTrue(
          (yield* orchestrator.dispatch(saved.derived.claimCommand, {
            intentId: saved.intentId,
            phase: "claim",
          })).replayed,
        );
        const replay = yield* orchestrator.dispatch(saved.derived.messageCommand, {
          intentId: saved.intentId,
          phase: "message",
        });
        assert.isTrue(replay.replayed);
        const changed = replay.storedEvents.find(
          ({ event }) => event.type === "provider-thread.updated",
        );
        if (changed?.event.type !== "provider-thread.updated")
          throw new Error("provider thread missing");
        assert.strictEqual(changed.event.payload.providerSessionId, saved.shared);
        assert.deepStrictEqual(
          {
            receipts: yield* sql`SELECT * FROM orchestration_command_receipts ORDER BY command_id`,
            proofs: yield* sql`SELECT * FROM workflow_native_command_proofs ORDER BY command_id`,
            events: yield* sql`SELECT * FROM orchestration_events ORDER BY sequence`,
          },
          saved.rows,
        );
      }).pipe(Effect.provide(testLayer(filename)));
    }),
);

it.effect(
  "native allocation preserves an empty imported provider thread while replacing its prior session",
  () =>
    Effect.gen(function* () {
      const { derived } = yield* workflowDecision(true);
      const runtime = yield* proofOrchestrator();
      const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
      const sink = Context.get(runtime, EventSink.EventSinkV2);
      const allocator = Context.get(
        yield* Layer.build(IdAllocator.layer),
        IdAllocator.IdAllocatorV2,
      );
      const created = yield* orchestrator.dispatch(
        decodeOrchestratorFixtureCommand({
          ...existingThreadCommand(derived),
          importedNativeThread: {
            ref: { driver: "codex", nativeId: "retained-native-thread", strength: "strong" },
          },
        }),
      );
      const imported = created.storedEvents.find(
        ({ event }) => event.type === "provider-thread.updated",
      );
      if (imported?.event.type !== "provider-thread.updated")
        throw new Error("imported provider thread missing");
      const shared = allocator.derive.providerSession({
        providerInstanceId: ProviderInstanceId.make("provider"),
      });
      yield* sink.write({
        events: [
          {
            ...imported.event,
            id: yield* allocator.allocate.event({
              threadId: derived.claimCommand.threadId,
              commandId: CommandId.make("imported-session-reference"),
            }),
            payload: { ...imported.event.payload, providerSessionId: shared },
          },
        ],
      });
      yield* orchestrator.dispatch(derived.claimCommand, {
        intentId: derived.reservation.intentId,
        phase: "claim",
      });
      const result = yield* orchestrator.dispatch(derived.messageCommand, {
        intentId: derived.reservation.intentId,
        phase: "message",
      });
      const changed = result.storedEvents.find(
        ({ event }) => event.type === "provider-thread.updated",
      );
      if (changed?.event.type !== "provider-thread.updated")
        throw new Error("provider thread missing");
      assert.strictEqual(changed.event.payload.id, imported.event.payload.id);
      assert.deepStrictEqual(
        changed.event.payload.nativeThreadRef,
        imported.event.payload.nativeThreadRef,
      );
      assert.deepStrictEqual(
        changed.event.payload.nativeMetadata,
        imported.event.payload.nativeMetadata,
      );
      assert.isString(changed.event.payload.providerSessionId);
      assert.notStrictEqual(changed.event.payload.providerSessionId, shared);
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["assistant-message", "fork", "merge_back", "provider-switch"] as const)(
  "native initial message refuses %s before planning a writer",
  (mode) =>
    Effect.gen(function* () {
      const { derived } = yield* workflowDecision(true);
      const runtime = yield* proofOrchestrator();
      const orchestrator = Context.get(runtime, Orchestrator.OrchestratorV2);
      const sink = Context.get(runtime, EventSink.EventSinkV2);
      yield* orchestrator.dispatch(existingThreadCommand(derived));
      yield* orchestrator.dispatch(derived.claimCommand, {
        intentId: derived.reservation.intentId,
        phase: "claim",
      });
      const threadId = derived.claimCommand.threadId;
      const event = yield* decodeFixtureEvent({
        id: `native-late-${mode}`,
        threadId,
        occurredAt: facts.now,
        ...(mode === "assistant-message"
          ? {
              type: "message.updated",
              payload: {
                id: "late-assistant",
                threadId,
                runId: null,
                nodeId: null,
                role: "assistant",
                text: "Imported history",
                attachments: [],
                streaming: false,
                createdBy: "agent",
                creationSource: "mcp",
                createdAt: facts.now,
                updatedAt: facts.now,
              },
            }
          : mode === "provider-switch"
            ? {
                type: "provider-thread.updated",
                driver: "codex",
                providerInstanceId: "other",
                payload: {
                  id: "foreign-provider-thread",
                  driver: "codex",
                  providerInstanceId: "other",
                  providerSessionId: "ordinary-old-session",
                  appThreadId: threadId,
                  ownerNodeId: null,
                  nativeThreadRef: {
                    driver: "codex",
                    nativeId: "foreign-native",
                    strength: "strong",
                  },
                  nativeConversationHeadRef: null,
                  status: "not_loaded",
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  forkedFrom: null,
                  createdAt: facts.now,
                  updatedAt: facts.now,
                },
              }
            : {
                type: "context-transfer.created",
                providerInstanceId: "provider",
                payload: {
                  id: `late-${mode}`,
                  type: mode,
                  sourceThreadId: threadId,
                  targetThreadId: threadId,
                  sourcePoint: { threadId },
                  basePoint: null,
                  sourceProviderInstanceId: "provider",
                  targetProviderInstanceId: "provider",
                  targetRunId: null,
                  status: "pending",
                  resolution: null,
                  createdBy: "agent",
                  error: null,
                  createdAt: facts.now,
                  updatedAt: facts.now,
                  consumedAt: null,
                },
              }),
      });
      yield* sink.write({ events: [event] });
      const original = sink.commitNativeCommand;
      if (!original) throw new Error("native writer missing");
      let writes = 0;
      using _writer = vi.spyOn(sink, "commitNativeCommand").mockImplementation((input) => {
        writes++;
        return original(input);
      });
      const before = yield* nativeCounts;
      const error = yield* Effect.flip(
        orchestrator.dispatch(derived.messageCommand, {
          intentId: derived.reservation.intentId,
          phase: "message",
        }),
      );
      assert.instanceOf(error, Orchestrator.OrchestratorDispatchError);
      assert.isString(error.cause);
      assert.strictEqual(writes, 0);
      assert.deepStrictEqual(yield* nativeCounts, before);
      const sql = yield* SqlClient.SqlClient;
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id=${derived.messageCommand.commandId}`,
        [],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT * FROM workflow_native_command_proofs WHERE phase='message'`,
        [],
      );
    }).pipe(Effect.provide(testLayer())),
);

it.effect.each(["workflow", "planning"] as const)(
  "native %s publishes its original runtime into the real Manager and joins release",
  (kind) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture(kind, false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-manager-owned",
        });
        const manager = Context.get(
          fixture.runtime,
          ProviderSessionManager.ProviderSessionManagerV2,
        );
        const service = yield* nativeTransportEntry(fixture);
        const transport = yield* service.createReservedTransport(producerSelection(fixture));
        yield* transport.client.assertCanQuiesceOutput;
        yield* transport.bootstrap();
        const constructed = yield* transport.createAdapterRuntime();
        const sessionId = constructed.runtime.providerSessionId;
        assert.isFalse(yield* manager.isLive(sessionId));
        assert.isFunction(transport.publishRuntime);
        const published = yield* transport.publishRuntime();
        assert.strictEqual(published.kind, "runtime-published");
        assert.strictEqual(published.transportId, transport.transportId);
        assert.strictEqual(published.runtime.providerSessionId, sessionId);
        const live = yield* manager.get(sessionId);
        assert.isTrue(Option.isSome(live));
        if (Option.isSome(live)) assert.strictEqual(live.value, published.runtime);
        assert.notStrictEqual(published.runtime, constructed.runtime);
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT status FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id=${sessionId}`,
          [{ status: "ready" }],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT thread_id FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id=${sessionId}`,
          [{ thread_id: fixture.input.threadId }],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT event_type FROM orchestration_events WHERE event_type='provider-session.attached'`,
          [{ event_type: "provider-session.attached" }],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT status FROM orchestration_v2_projection_runs`,
          [{ status: "preparing" }],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_submission_attempts`,
          [],
        );
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        assert.strictEqual(child.payloads.length, 2);
        yield* manager.close(sessionId);
        assert.isFalse(child.running);
        assert.isFalse(yield* manager.isLive(sessionId));
        assert.isTrue(Option.isNone(yield* manager.get(sessionId)));
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT status FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id=${sessionId}`,
          [{ status: "stopped" }],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id=${sessionId}`,
          [],
        );
        assert.strictEqual((yield* transport.close()).kind, "retired");
        assert.strictEqual(child.payloads.length, 2);
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

const nativeManagerOrdinaryAdapter = Effect.fn("test.nativeManagerOrdinaryAdapter")(function* (
  fixture: Effect.Success<ReturnType<typeof controlledNativeFixture>>,
) {
  const adapter = yield* Context.get(
    fixture.runtime,
    ProviderAdapterRegistry.ProviderAdapterRegistryV2,
  ).get(ProviderInstanceId.make("provider"));
  const state = {
    beforeOpen: Effect.void as Effect.Effect<void>,
    beforeClose: Effect.void as Effect.Effect<void>,
    closed: 0,
  };
  const spy = vi.spyOn(adapter, "openSession").mockImplementation((input) =>
    Effect.gen(function* () {
      yield* Effect.addFinalizer(() =>
        state.beforeClose.pipe(
          Effect.andThen(
            Effect.sync(() => {
              state.closed++;
            }),
          ),
        ),
      );
      yield* state.beforeOpen;
      const now = yield* DateTime.now;
      const unused = () => Effect.die("unused ordinary fixture method");
      return {
        instanceId: ProviderInstanceId.make("provider"),
        driver: ProviderDriverKind.make("codex"),
        providerSessionId: input.providerSessionId,
        providerSession: {
          id: input.providerSessionId,
          driver: ProviderDriverKind.make("codex"),
          providerInstanceId: ProviderInstanceId.make("provider"),
          status: "ready" as const,
          cwd: "/ordinary",
          model: "model",
          capabilities: CodexProviderCapabilitiesV2,
          createdAt: now,
          updatedAt: now,
          lastError: null,
        },
        events: Stream.never,
        ensureThread: unused,
        resumeThread: unused,
        startTurn: unused,
        steerTurn: unused,
        interruptTurn: unused,
        respondToRuntimeRequest: unused,
        readThreadSnapshot: unused,
        rollbackThread: unused,
        forkThread: unused,
      } satisfies ProviderAdapter.ProviderAdapterV2SessionRuntime;
    }),
  );
  return { state, spy };
});
const ordinaryNativeManagerInput = (threadId: ThreadId, providerSessionId: ProviderSessionId) => ({
  threadId,
  providerSessionId,
  modelSelection: { instanceId: ProviderInstanceId.make("provider"), model: "model" },
  runtimePolicy: {
    cwd: null,
    runtimeMode: "full-access" as const,
    interactionMode: "default" as const,
  },
});

it.effect.each([
  "same-session",
  "same-thread",
  "detached-thread",
  "pending-open",
  "close-debt",
] as const)("native Manager refuses ordinary custody at %s without evicting its runtime", (mode) =>
  Effect.gen(function* () {
    let codexWraps = 0;
    const harness = yield* nativeWorkspaceProcesses(
      Effect.void,
      Effect.sync(() => {
        codexWraps++;
      }),
    );
    const testScope = yield* Effect.scope;
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-manager-custody",
      });
      const manager = Context.get(fixture.runtime, ProviderSessionManager.ProviderSessionManagerV2);
      const ordinary = yield* nativeManagerOrdinaryAdapter(fixture);
      using _adapter = ordinary.spy;
      const sid = ProviderSessionId.make("ordinary-manager-custody");
      const tid =
        mode === "same-session" || mode === "detached-thread"
          ? ThreadId.make("other-thread")
          : ThreadId.make(fixture.input.threadId);
      if (tid !== fixture.input.threadId)
        yield* fixture.orchestrator.dispatch(
          decodeOrchestratorFixtureCommand({
            type: "thread.create",
            commandId: "ordinary-other-thread",
            threadId: tid,
            projectId: fixture.input.projectId,
            title: "Other",
            modelSelection: fixture.input.launch.modelSelection,
            runtimeMode: "full-access",
            interactionMode: "default",
            branch: "main",
            worktreePath: null,
            createdBy: "agent",
            creationSource: "mcp",
          }),
        );
      const allocator = Context.get(
        yield* Layer.build(IdAllocator.layer),
        IdAllocator.IdAllocatorV2,
      );
      using _id = vi
        .spyOn(allocator.allocate, "providerSession")
        .mockImplementation(() =>
          Effect.succeed(
            mode === "same-session" ? sid : ProviderSessionId.make("new-native-session"),
          ),
        );
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const closed = yield* Deferred.make<void>();
      if (mode === "pending-open")
        ordinary.state.beforeOpen = Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
        );
      if (mode === "close-debt")
        ordinary.state.beforeClose = Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Deferred.await(release)),
          Effect.andThen(Deferred.succeed(closed, undefined)),
        );
      let opening:
        | Fiber.Fiber<
            ProviderAdapter.ProviderAdapterV2SessionRuntime,
            ProviderSessionManager.ProviderSessionManagerV2Error
          >
        | undefined;
      let reached = 0;
      let consumed = 0;
      const originalConsume = fixture.store.consumeNativeCodexTransportOwner;
      using _consume = vi
        .spyOn(fixture.store, "consumeNativeCodexTransportOwner")
        .mockImplementation((job, command) =>
          Effect.gen(function* () {
            reached++;
            const retired = PreparationJob.decodeRow(
              (yield* fixture.sql`SELECT * FROM orchestration_v2_preparation_jobs`)[0],
            );
            assert.strictEqual(retired.input.threadId, fixture.input.threadId);
            assert.strictEqual(
              (yield* fixture.sql`SELECT state FROM orchestration_v2_preparation_jobs`)[0]!.state,
              "settled",
            );
            opening = yield* manager
              .open(ordinaryNativeManagerInput(tid, sid))
              .pipe(Effect.forkIn(testScope));
            if (mode === "pending-open") yield* Deferred.await(entered);
            else yield* Fiber.join(opening);
            if (mode === "detached-thread") {
              yield* manager.open(
                ordinaryNativeManagerInput(ThreadId.make(fixture.input.threadId), sid),
              );
              yield* manager.detach({
                providerSessionId: sid,
                threadId: ThreadId.make(fixture.input.threadId),
              });
            }
            if (mode === "close-debt") {
              const closing = yield* manager.close(sid).pipe(Effect.forkIn(testScope));
              yield* Deferred.await(entered);
              yield* TestClock.adjust("30 seconds");
              yield* Fiber.join(closing);
              assert.strictEqual(ordinary.state.closed, 0);
            }
          }).pipe(
            Effect.orDie,
            Effect.andThen(originalConsume(job, command)),
            Effect.tap(() =>
              Effect.sync(() => {
                consumed++;
              }),
            ),
          ),
        );
      const service = yield* nativeTransportEntry(fixture);
      const denied = yield* service
        .createReservedTransport(producerSelection(fixture))
        .pipe(Effect.exit, Effect.ensuring(Deferred.succeed(release, undefined)));
      assert.strictEqual(reached, 1);
      assert.strictEqual(consumed, 1);
      assert.isTrue(Exit.isFailure(denied));
      assert.strictEqual(codexWraps, 0);
      assert.strictEqual(
        harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex").length,
        0,
      );
      if (mode === "pending-open" && opening) yield* Fiber.join(opening);
      if (mode === "close-debt") {
        yield* Deferred.await(closed);
        assert.strictEqual(ordinary.state.closed, 1);
      } else {
        assert.isTrue(yield* manager.isLive(sid));
        assert.strictEqual(ordinary.state.closed, 0);
        yield* manager.close(sid);
        assert.strictEqual(ordinary.state.closed, 1);
      }
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["manager", "sink", "store"] as const)(
  "native Manager rejects a copied %s service before Codex birth",
  (mode) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-cloned-service",
        });
        const altered =
          mode === "manager"
            ? Context.add(fixture.runtime, ProviderSessionManager.ProviderSessionManagerV2, {
                ...Context.get(fixture.runtime, ProviderSessionManager.ProviderSessionManagerV2),
              })
            : mode === "sink"
              ? Context.add(fixture.runtime, EventSink.EventSinkV2, {
                  ...Context.get(fixture.runtime, EventSink.EventSinkV2),
                })
              : Context.add(fixture.runtime, Store.WorkflowRunStore, { ...fixture.store });
        const attempted = yield* Effect.gen(function* () {
          const service = yield* nativeTransportEntry({ ...fixture, runtime: altered });
          return yield* service.createReservedTransport(producerSelection(fixture));
        }).pipe(Effect.exit);
        assert.isTrue(Exit.isFailure(attempted));
        assert.strictEqual(
          harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex").length,
          0,
        );
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each([
  "close-instance",
  "detach",
  "release",
  "release-unless-busy",
  "shutdown",
  "transport",
] as const)("native Manager joins original cleanup through %s", (mode) =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("planning", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-manager-release",
      });
      const manager = Context.get(fixture.runtime, ProviderSessionManager.ProviderSessionManagerV2);
      const service = yield* nativeTransportEntry(fixture);
      const transport = yield* service.createReservedTransport(producerSelection(fixture));
      yield* transport.bootstrap();
      yield* transport.createAdapterRuntime();
      const published = yield* transport.publishRuntime();
      const sid = published.runtime.providerSessionId;
      if (mode === "close-instance")
        yield* manager.closeInstance(ProviderInstanceId.make("provider"));
      else if (mode === "detach")
        yield* manager.detach({
          providerSessionId: sid,
          threadId: ThreadId.make(fixture.input.threadId),
        });
      else if (mode === "release")
        yield* manager.release({ providerSessionId: sid, reason: "manual_shutdown" });
      else if (mode === "release-unless-busy")
        assert.isTrue(
          yield* manager.releaseUnlessBusy({
            providerSessionId: sid,
            reason: "manual_shutdown",
            ownProviderThreadIds: [],
          }),
        );
      else if (mode === "shutdown") yield* manager.shutdown;
      else assert.strictEqual((yield* transport.close()).kind, "retired");
      assert.isFalse(yield* manager.isLive(sid));
      assert.isFalse(yield* transport.client.ownedProcess.isRunning);
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT status FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id=${sid}`,
        [{ status: "stopped" }],
      );
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id=${sid}`,
        [],
      );
      assert.strictEqual((yield* transport.close()).kind, "retired");
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each([
  "lost-attachment-return",
  "projection-replaced",
  "attachment-aba",
  "grant-revoked",
  "grant-after-attachment",
  "profile-after-attachment",
  "input-after-attachment",
  "nested-mutation",
  "callback-close",
  "callback-shutdown",
  "callback-close-instance",
  "foreign-thread",
  "sdk-death",
] as const)("native Manager preserves original ownership at %s", (mode) =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-manager-boundary",
      });
      const manager = Context.get(fixture.runtime, ProviderSessionManager.ProviderSessionManagerV2);
      const sink = Context.get(fixture.runtime, EventSink.EventSinkV2);
      const service = yield* nativeTransportEntry(fixture);
      const transport = yield* service.createReservedTransport(producerSelection(fixture));
      yield* transport.bootstrap();
      const raw = yield* transport.createAdapterRuntime();
      const sid = raw.runtime.providerSessionId;
      if (mode === "grant-revoked") yield* fixture.sessions.revoke(fixture.reference.sessionId);
      const originalAttach = sink.attachNativeSession!;
      using _write = vi.spyOn(sink, "attachNativeSession").mockImplementation((store, owner) =>
        originalAttach(store, owner).pipe(
          Effect.tap(
            Effect.gen(function* () {
              if (mode === "grant-after-attachment")
                yield* fixture.sessions.revoke(fixture.reference.sessionId);
              else if (mode === "input-after-attachment")
                yield* fixture.sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.worktreePath','/foreign')`;
              else if (mode === "profile-after-attachment") {
                const id = ProviderInstanceId.make("provider");
                service.facts.settings = {
                  ...service.facts.settings,
                  providerInstances: {
                    ...service.facts.settings.providerInstances,
                    [id]: { ...service.facts.settings.providerInstances[id]!, enabled: false },
                  },
                };
              }
            }).pipe(
              Effect.mapError(
                (cause) => new EventSink.EventSinkWriteError({ eventCount: 1, cause }),
              ),
            ),
          ),
          Effect.andThen(
            mode === "lost-attachment-return"
              ? Effect.fail(new EventSink.EventSinkWriteError({ eventCount: 1 }))
              : Effect.succeed(raw.runtime.providerSession),
          ),
        ),
      );
      if (
        mode === "grant-revoked" ||
        mode === "lost-attachment-return" ||
        mode === "grant-after-attachment" ||
        mode === "profile-after-attachment" ||
        mode === "input-after-attachment"
      ) {
        assert.isTrue(Exit.isFailure(yield* Effect.exit(transport.publishRuntime())));
        assert.isFalse(yield* manager.isLive(sid));
        assert.isFalse(yield* transport.client.ownedProcess.isRunning);
        if (mode !== "grant-revoked") {
          assert.deepStrictEqual(
            yield* fixture.sql`SELECT status FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id=${sid}`,
            [{ status: "error" }],
          );
          assert.deepStrictEqual(
            yield* fixture.sql`SELECT * FROM orchestration_v2_projection_provider_session_bindings WHERE provider_session_id=${sid}`,
            [],
          );
        }
        return;
      }
      const published = yield* transport.publishRuntime();
      assert.isTrue(Exit.isFailure(yield* Effect.exit(transport.publishRuntime())));
      if (mode === "projection-replaced") {
        yield* fixture.sql`UPDATE orchestration_v2_projection_provider_sessions SET driver='foreign' WHERE provider_session_id=${sid}`;
        assert.isTrue(Exit.isFailure(yield* Effect.exit(manager.close(sid))));
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT driver,status FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id=${sid}`,
          [{ driver: "foreign", status: "ready" }],
        );
        return;
      }
      if (mode === "attachment-aba") {
        yield* sink.write({
          events: [
            {
              id: EventId.make("foreign-native-update"),
              type: "provider-session.updated",
              threadId: ThreadId.make(fixture.input.threadId),
              occurredAt: yield* DateTime.now,
              payload: published.runtime.providerSession,
            },
          ],
        });
        assert.isTrue(Exit.isFailure(yield* Effect.exit(manager.close(sid))));
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT status FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id=${sid}`,
          [{ status: "ready" }],
        );
        return;
      }
      if (mode === "nested-mutation") {
        const caps = published.runtime.providerSession.capabilities.tools;
        const original = caps.supportsMcpTools;
        Object.assign(caps, { supportsMcpTools: !original });
        yield* manager
          .close(sid)
          .pipe(
            Effect.ensuring(Effect.sync(() => Object.assign(caps, { supportsMcpTools: original }))),
          );
        const row = (yield* fixture.sql<{
          payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id=${sid}`)[0]!;
        assert.strictEqual(
          JSON.parse(row.payload_json).capabilities.tools.supportsMcpTools,
          original,
        );
        return;
      }
      if (
        mode === "callback-close" ||
        mode === "callback-shutdown" ||
        mode === "callback-close-instance"
      ) {
        const attempts =
          yield* Queue.unbounded<
            Exit.Exit<void, ProviderSessionManager.ProviderSessionManagerV2Error>
          >();
        yield* transport.client.handleUnknownServerRequest(() =>
          (mode === "callback-shutdown"
            ? manager.shutdown
            : mode === "callback-close-instance"
              ? manager.closeInstance(ProviderInstanceId.make("provider"))
              : manager.close(sid)
          ).pipe(
            Effect.exit,
            Effect.flatMap((exit) => Queue.offer(attempts, exit)),
            Effect.as({ ok: true }),
          ),
        );
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        yield* Queue.offer(
          child.output,
          new TextEncoder().encode(
            JSON.stringify({ id: "manager-close", method: "fixture/close", params: {} }) + "\n",
          ),
        );
        const attempted = yield* Queue.take(attempts);
        assert.strictEqual(Exit.isSuccess(attempted), mode === "callback-shutdown");
        assert.isTrue(yield* manager.isLive(sid));
        assert.isTrue(child.running);
      }
      if (mode === "foreign-thread") {
        const projections = Context.get(
          yield* Layer.build(
            ProjectionStore.layer.pipe(
              Layer.provide(Layer.succeed(SqlClient.SqlClient, fixture.sql)),
            ),
          ),
          ProjectionStore.ProjectionStoreV2,
        );
        const records = yield* projections.getThreadRecords(ThreadId.make(fixture.input.threadId), [
          "providerThreads",
        ]);
        const providerThread = records.providerThreads[0]!;
        const foreign = ThreadId.make("foreign-thread");
        const modelSelection = { instanceId: ProviderInstanceId.make("provider"), model: "model" };
        const runtimePolicy = {
          cwd: "/foreign",
          runtimeMode: "full-access" as const,
          interactionMode: "default" as const,
        };
        for (const action of [
          published.runtime.ensureThread({ threadId: foreign, modelSelection, runtimePolicy }),
          published.runtime.resumeThread({
            providerThread: { ...providerThread, appThreadId: null },
          }),
          published.runtime.resumeThread({ providerThread, threadId: foreign }),
          published.runtime.forkThread({
            sourceProviderThread: providerThread,
            targetThreadId: foreign,
            modelSelection,
            runtimePolicy,
          }),
        ]) {
          const error = yield* Effect.flip(action);
          assert.strictEqual(error._tag, "ProviderAdapterProtocolError");
        }
      }
      if (mode === "sdk-death") {
        const subscription = yield* published.runtime.subscribeEvents!;
        const ended = yield* subscription.events.pipe(
          Stream.runDrain,
          Effect.exit,
          Effect.forkScoped,
        );
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        child.running = false;
        yield* Deferred.succeed(child.terminal, { code: 1, signal: null });
        yield* Queue.end(child.output);
        assert.isTrue(Exit.isFailure(yield* Fiber.join(ended)));
      }
      yield* manager.close(sid);
      assert.isFalse(yield* manager.isLive(sid));
      assert.isFalse(yield* transport.client.ownedProcess.isRunning);
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT status FROM orchestration_v2_projection_runs`,
        [{ status: "preparing" }],
      );
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT * FROM orchestration_v2_submission_attempts`,
        [],
      );
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(
  ([false, true] as const).flatMap((masked) =>
    (["caller", "factory", "store", "manager", "deadline"] as const).map((boundary) => ({
      masked,
      boundary,
    })),
  ),
)(
  "native Manager joins blocked publication at $boundary under mask=$masked",
  ({ masked, boundary }) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      const storeScope = yield* Scope.make();
      const factoryScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(factoryScope, Exit.void));
      yield* Effect.addFinalizer(() => Scope.close(storeScope, Exit.void));
      const entered = yield* Deferred.make<void>();
      const unwinding = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-manager-publish-unwind",
        }).pipe(Effect.provideService(Scope.Scope, storeScope));
        const manager = Context.get(
          fixture.runtime,
          ProviderSessionManager.ProviderSessionManagerV2,
        );
        const sink = Context.get(fixture.runtime, EventSink.EventSinkV2);
        const service = yield* nativeTransportEntry(fixture).pipe(
          Effect.provideService(Scope.Scope, factoryScope),
        );
        const prepared = yield* Queue.unbounded<ProviderSessionId>();
        const originalWriter = fixture.sql.withTransaction;
        const originalAttach = sink.attachNativeSession!;
        let armed = false;
        using _writer = vi
          .spyOn(fixture.sql, "withTransaction")
          .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) => {
            if (!armed) return originalWriter(work);
            armed = false;
            return originalWriter(
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(
                  Deferred.succeed(unwinding, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                  ),
                ),
                Effect.interruptible,
              ),
            );
          });
        using _attach = vi.spyOn(sink, "attachNativeSession").mockImplementation((store, owner) =>
          Effect.suspend(() => {
            armed = true;
            return originalAttach(store, owner);
          }),
        );
        const working = yield* Effect.gen(function* () {
          const transport = yield* service.createReservedTransport(producerSelection(fixture));
          yield* transport.bootstrap();
          const constructed = yield* transport.createAdapterRuntime();
          yield* Queue.offer(prepared, constructed.runtime.providerSessionId);
          return yield* transport.publishRuntime();
        }).pipe(
          (work) => (masked ? Effect.uninterruptible(work) : work),
          Effect.exit,
          Effect.forkScoped,
        );
        const sid = yield* Queue.take(prepared);
        yield* Deferred.await(entered);
        assert.isFalse(yield* manager.isLive(sid));
        const stopping = yield* (
          boundary === "caller"
            ? Fiber.interrupt(working)
            : boundary === "factory"
              ? Scope.close(factoryScope, Exit.void)
              : boundary === "store"
                ? Scope.close(storeScope, Exit.void)
                : boundary === "manager"
                  ? manager.shutdown
                  : TestClock.adjust("10 seconds")
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(unwinding);
        yield* Effect.sync(() => {
          assert.isUndefined(working.pollUnsafe());
          if (boundary !== "deadline") assert.isUndefined(stopping.pollUnsafe());
        }).pipe(Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(stopping);
        const result = yield* Fiber.await(working);
        assert.isTrue(Exit.isFailure(result) || Exit.isFailure(result.value));
        assert.isFalse(yield* manager.isLive(sid));
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_projection_provider_sessions`,
          [],
        );
        assert.deepStrictEqual(
          yield* fixture.sql`SELECT * FROM orchestration_v2_submission_attempts`,
          [],
        );
        const image = harness.requests.find((image) => image.args[4] === "/fixture-bin/codex")!;
        const child = harness.children.get(image.args[3]!)!;
        assert.isFalse(child.running);
        assert.strictEqual(child.payloads.length, 2);
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect.each(["configured", "retained-config", "idle-release"] as const)(
  "native Manager enforces original MCP and idle profile at %s",
  (mode) =>
    Effect.gen(function* () {
      const harness = yield* nativeWorkspaceProcesses();
      yield* Effect.gen(function* () {
        const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
          type: "worktree",
          baseRef: "main",
          branch: "native-manager-profile",
        });
        const mcp = Context.get(
          yield* Layer.build(Layer.fresh(McpProviderSessions.layer)),
          McpProviderSessions.McpProviderSessions,
        );
        const selected = {
          environmentId: EnvironmentId.make("environment"),
          threadId: ThreadId.make(fixture.input.threadId),
          providerSessionId: "retained",
          providerInstanceId: ProviderInstanceId.make("provider"),
          endpoint: "http://unused",
          authorizationHeader: "fixture",
          browserToolsAvailable: false,
        };
        if (mode === "retained-config") yield* mcp.set(selected);
        const dependencies = Layer.mergeAll(
          NodeServices.layer,
          IdAllocator.layer,
          McpSessionRegistryTestkit.layer,
          Layer.succeed(McpProviderSessions.McpProviderSessions, mcp),
          Layer.succeed(EventSink.EventSinkV2, Context.get(fixture.runtime, EventSink.EventSinkV2)),
          Layer.succeed(
            ProviderAdapterRegistry.ProviderAdapterRegistryV2,
            Context.get(fixture.runtime, ProviderAdapterRegistry.ProviderAdapterRegistryV2),
          ),
          ProjectionStore.layer.pipe(
            Layer.provide(Layer.succeed(SqlClient.SqlClient, fixture.sql)),
          ),
          Layer.succeed(CommandOutputHub.CommandOutputHub, {
            append: () => Effect.die("native output not qualified"),
            finish: () => Effect.die("native output not qualified"),
            subscribe: () => Stream.die("native output not qualified"),
          }),
          Layer.succeed(ProviderEventIngestor.ProviderEventIngestorV2, {
            normalize: () => Effect.die("native events not qualified"),
            ingestNormalized: () => Effect.die("native events not qualified"),
          }),
        );
        const manager = Context.get(
          yield* Layer.build(
            Layer.fresh(
              ProviderSessionManager.layerWithOptions({
                configureMcp: mode === "configured",
                idleTimeoutMs: 1,
              }).pipe(Layer.provide(dependencies)),
            ),
          ),
          ProviderSessionManager.ProviderSessionManagerV2,
        );
        const result = yield* Effect.gen(function* () {
          const service = yield* nativeTransportEntry({
            ...fixture,
            runtime: Context.add(
              fixture.runtime,
              ProviderSessionManager.ProviderSessionManagerV2,
              manager,
            ),
          });
          const transport = yield* service.createReservedTransport(producerSelection(fixture));
          if (mode !== "idle-release") return yield* Effect.die("MCP profile was accepted");
          yield* transport.bootstrap();
          yield* transport.createAdapterRuntime();
          const published = yield* transport.publishRuntime();
          const subscription = yield* published.runtime.subscribeEvents!;
          const ended = yield* subscription.events.pipe(
            Stream.runDrain,
            Effect.exit,
            Effect.forkScoped,
          );
          yield* TestClock.adjust(1);
          assert.isTrue(Exit.isFailure(yield* Fiber.join(ended)));
          yield* manager.close(published.runtime.providerSessionId);
          assert.isFalse(yield* manager.isLive(published.runtime.providerSessionId));
          assert.strictEqual((yield* transport.close()).kind, "retired");
        }).pipe(Effect.exit);
        if (mode === "idle-release")
          assert.isTrue(
            Exit.isSuccess(result),
            Exit.isFailure(result) ? Cause.pretty(result.cause) : "success",
          );
        else {
          assert.isTrue(Exit.isFailure(result));
          assert.strictEqual(
            harness.requests.filter((image) => image.args[4] === "/fixture-bin/codex").length,
            0,
          );
          assert.deepStrictEqual(
            yield* mcp.read(selected.threadId),
            mode === "retained-config" ? selected : undefined,
          );
        }
      }).pipe(Effect.provide(harness.services));
    }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect("native Manager rechecks its profile after the observed final acceptance writer", () =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-manager-final-profile",
      });
      const manager = Context.get(fixture.runtime, ProviderSessionManager.ProviderSessionManagerV2);
      const sink = Context.get(fixture.runtime, EventSink.EventSinkV2);
      const service = yield* nativeTransportEntry(fixture);
      const transport = yield* service.createReservedTransport(producerSelection(fixture));
      yield* transport.bootstrap();
      const raw = yield* transport.createAdapterRuntime();
      let armed = false;
      let intercepted = false;
      const originalAttach = sink.attachNativeSession!;
      const originalWriter = fixture.sql.withTransaction;
      using _attach = vi.spyOn(sink, "attachNativeSession").mockImplementation((store, owner) =>
        originalAttach(store, owner).pipe(
          Effect.tap(() =>
            Effect.sync(() => {
              armed = true;
            }),
          ),
        ),
      );
      using _writer = vi
        .spyOn(fixture.sql, "withTransaction")
        .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) => {
          if (!armed) return originalWriter(work);
          armed = false;
          return originalWriter(work).pipe(
            Effect.tap(() =>
              Effect.sync(() => {
                intercepted = true;
                const id = ProviderInstanceId.make("provider");
                service.facts.settings = {
                  ...service.facts.settings,
                  providerInstances: {
                    ...service.facts.settings.providerInstances,
                    [id]: { ...service.facts.settings.providerInstances[id]!, enabled: false },
                  },
                };
              }),
            ),
          );
        });
      const result = yield* Effect.exit(transport.publishRuntime());
      assert.isTrue(intercepted);
      assert.isTrue(Exit.isFailure(result));
      assert.isFalse(yield* manager.isLive(raw.runtime.providerSessionId));
      assert.isFalse(yield* transport.client.ownedProcess.isRunning);
      assert.deepStrictEqual(
        yield* fixture.sql`SELECT status FROM orchestration_v2_projection_provider_sessions`,
        [{ status: "error" }],
      );
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);

it.effect("native Manager owns attachment payload before its SQL writer waits", () =>
  Effect.gen(function* () {
    const harness = yield* nativeWorkspaceProcesses();
    yield* Effect.gen(function* () {
      const fixture = yield* controlledNativeFixture("workflow", false, false, 2, false, {
        type: "worktree",
        baseRef: "main",
        branch: "native-manager-payload",
      });
      const manager = Context.get(fixture.runtime, ProviderSessionManager.ProviderSessionManagerV2);
      const sink = Context.get(fixture.runtime, EventSink.EventSinkV2);
      const service = yield* nativeTransportEntry(fixture);
      const transport = yield* service.createReservedTransport(producerSelection(fixture));
      yield* transport.bootstrap();
      const raw = yield* transport.createAdapterRuntime();
      const caps = raw.runtime.providerSession.capabilities.tools;
      const original = caps.supportsMcpTools;
      let armed = false;
      let intercepted = false;
      const originalAttach = sink.attachNativeSession!;
      const originalWriter = fixture.sql.withTransaction;
      using _attach = vi.spyOn(sink, "attachNativeSession").mockImplementation((store, owner) =>
        Effect.suspend(() => {
          armed = true;
          return originalAttach(store, owner);
        }),
      );
      using _writer = vi
        .spyOn(fixture.sql, "withTransaction")
        .mockImplementation(<A, E, R>(work: Effect.Effect<A, E, R>) => {
          if (!armed) return originalWriter(work);
          armed = false;
          return Effect.sync(() => {
            intercepted = true;
            Object.assign(caps, { supportsMcpTools: !original });
          }).pipe(Effect.andThen(originalWriter(work)));
        });
      yield* Effect.gen(function* () {
        const published = yield* transport.publishRuntime();
        assert.isTrue(intercepted);
        const row = (yield* fixture.sql<{
          payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_provider_sessions WHERE provider_session_id=${published.runtime.providerSessionId}`)[0]!;
        assert.strictEqual(
          JSON.parse(row.payload_json).capabilities.tools.supportsMcpTools,
          original,
        );
        yield* manager.close(published.runtime.providerSessionId);
      }).pipe(
        Effect.ensuring(Effect.sync(() => Object.assign(caps, { supportsMcpTools: original }))),
      );
    }).pipe(Effect.provide(harness.services));
  }).pipe(Effect.provide(Layer.mergeAll(testLayer(), Digest.layerWebCrypto))),
);
