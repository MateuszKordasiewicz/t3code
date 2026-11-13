import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as CodexReplay from "effect-codex-app-server/replay";
import {
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
  WorkflowRunV1,
  WorkflowOperationV1,
  ProviderDriverKind,
} from "@t3tools/contracts";

import {
  ClaudeOrchestratorReplayHarness,
  makeClaudeRestartReplayHarness,
} from "../Adapters/ClaudeAdapterV2.testkit.ts";
import { CodexOrchestratorReplayHarness } from "../Adapters/CodexAdapterV2.testkit.ts";
import * as CodexAdapterV2Testkit from "../Adapters/CodexAdapterV2.testkit.ts";
import {
  type CursorAgentSdkReplayTranscript,
  CursorOrchestratorReplayHarness,
  makeCursorAgentSdkReplayRunner,
} from "../Adapters/CursorAdapterV2.testkit.ts";
import * as CursorAdapterV2Testkit from "../Adapters/CursorAdapterV2.testkit.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as EffectWorker from "../EffectWorker.ts";
import * as EventSink from "../EventSink.ts";
import { WorkflowCanonicalHash } from "../../workflowRuns/WorkflowCanonicalHash.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import { provideDeterministicTestRuntime } from "./DeterministicRuntime.ts";
import {
  CLAUDE_MODEL_SELECTION,
  CODEX_MODEL_SELECTION,
  CURSOR_MODEL_SELECTION,
  materializeFixtureInput,
  type MaterializedOrchestratorFixtureInput,
  PROVIDER_THREAD_RESUME_FIRST_PROMPT,
  PROVIDER_THREAD_RESUME_SECOND_PROMPT,
} from "./fixtures/shared.ts";
import {
  assertAssistantTextIncludes,
  assertBaseProjection,
  assertConversationMessageRoles,
  assertRunOrdinals,
  assertSemanticProjectionIntegrity,
  assertTurnItemTypes,
  assertUserMessagesInclude,
  projectionFor,
} from "./fixtures/shared.ts";
import {
  runOrchestratorV2Scenario,
  type OrchestratorV2ScenarioResult,
} from "./OrchestratorScenario.ts";
import {
  layerProviderReplay,
  runOrchestratorV2ProviderReplayScenario,
} from "./ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import { materializeReplayTranscriptRuntimeInstructions } from "./ReplayRuntimeInstructions.ts";
import {
  materializeReplayTranscriptWorkspace,
  readProviderReplayTranscript,
} from "@t3tools/provider-testing/replayTranscript";

const decodeRecoveryWorkflowRun = Schema.decodeEffect(WorkflowRunV1);
const decodeRecoveryWorkflowOperation = Schema.decodeEffect(WorkflowOperationV1);

const seedValidWorkflowOperation = Effect.fn("test.seedValidWorkflowOperation")(function* (
  threadId: ThreadId,
  nativeRunId: RunId,
) {
  const sql = yield* SqlClient.SqlClient;
  const hash = "a".repeat(64);
  const run = yield* decodeRecoveryWorkflowRun({
    schemaVersion: 1,
    id: `workflow:${threadId}`,
    spec: {
      ref: { kind: "configuration", id: "spec", sha256: hash },
      plan: {
        planId: `plan:${threadId}`,
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
  const draft = yield* decodeRecoveryWorkflowOperation({
    schemaVersion: 1,
    id: `operation:${threadId}`,
    owner: { kind: "workflow_run", workflowRunId: run.id },
    kind: "stage_launch",
    parameters: {
      kind: "stage_launch",
      stage: "implementation",
      context: {
        specHash: hash,
        policyHash: hash,
        itemId: "item",
        attemptId: "attempt",
        candidateHash: hash,
        configurationHash: hash,
      },
      provider: { providerInstanceId: "provider", model: "model" },
      nativeThreadId: threadId,
      nativeRunId,
    },
    fingerprint: hash,
    origin: run.origin,
    destination: { environmentId: "environment", projectId: "project" },
    controlGeneration: 0,
    status: "pending",
    access: { origins: [] },
  });
  const operation = { ...draft, fingerprint: WorkflowCanonicalHash.operation(draft) };
  yield* sql`INSERT INTO workflow_plan_identities VALUES (${run.spec.plan.planId})`;
  yield* sql`INSERT INTO workflow_revision_identities VALUES (${run.spec.plan.planId}, 'revision', ${hash}, ${hash})`;
  yield* sql`INSERT INTO workflow_runs VALUES (${run.id}, ${run.spec.plan.planId}, 'revision', ${hash}, ${hash}, ${hash}, 'active', 0, 0, ${JSON.stringify(run)})`;
  yield* sql`INSERT INTO workflow_operations (id, workflow_run_id, kind, fingerprint, status, generation, record_json) VALUES (${operation.id}, ${run.id}, 'stage_launch', ${operation.fingerprint}, 'pending', 0, ${JSON.stringify(operation)})`;
});

const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = CODEX_MODEL_SELECTION;

function makeThread(threadId: ThreadId, now: DateTime.Utc): OrchestrationV2AppThread {
  return {
    createdBy: "user",
    creationSource: "web",
    id: threadId,
    projectId: ProjectId.make(`project:${threadId}`),
    title: `Thread ${threadId}`,
    providerInstanceId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    activeProviderThreadId: null,
    lineage: {
      parentThreadId: null,
      relationshipToParent: null,
      rootThreadId: threadId,
    },
    forkedFrom: null,
    createdAt: now,
    updatedAt: now,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    lastVisitedAt: null,
    deletedAt: null,
  };
}

const FIRST_FINAL = "provider thread resume fixture first turn complete";
const SECOND_FINAL = "provider thread resume fixture second turn complete";

const decodeCodexTranscript = Schema.decodeUnknownEffect(
  CodexReplay.CodexAppServerReplayTranscript,
);
const readRawTranscript = Effect.fn("readRecoveryTranscript")(function* (file: URL) {
  return yield* readProviderReplayTranscript(file);
});
const readCodexTranscript = Effect.fn("readCodexRecoveryTranscript")(function* (workspace: string) {
  const transcript = yield* readRawTranscript(
    new URL("./fixtures/provider_thread_resume/codex_transcript.ndjson", import.meta.url),
  );
  return yield* decodeCodexTranscript(materializeReplayTranscriptWorkspace(transcript, workspace));
});
const decodePromptPair = Schema.decodeUnknownEffect(Schema.Tuple([Schema.String, Schema.String]));
const readClaudeSubagentResumeTranscript = Effect.fn("readClaudeSubagentResumeTranscript")(
  function* () {
    return yield* ClaudeOrchestratorReplayHarness.decodeTranscript(
      yield* readRawTranscript(
        new URL(
          "./fixtures/claude_subagent_resume_after_restart/claude_transcript.ndjson",
          import.meta.url,
        ),
      ),
    );
  },
);
/** Remove the subagent's first run so restart cancels it after root settlement; resume carries the restart note. */
const readClaudeCancelledSubagentResumeTranscript = Effect.fn(
  "readClaudeCancelledSubagentResumeTranscript",
)(function* () {
  const recorded = yield* readRawTranscript(
    new URL(
      "./fixtures/claude_subagent_resume_after_restart/claude_transcript.ndjson",
      import.meta.url,
    ),
  );
  const frameField = (index: number, key: string): unknown => {
    const entry = recorded.entries[index];
    return entry?.type === "emit_inbound" && typeof entry.frame === "object" && entry.frame !== null
      ? Reflect.get(entry.frame, key)
      : undefined;
  };
  const indices = recorded.entries.map((_, index) => index);
  const firstSubagentFrame = indices.find(
    (index) => typeof frameField(index, "parent_tool_use_id") === "string",
  );
  const firstOutcome = indices.find(
    (index) => frameField(index, "subtype") === "task_notification",
  );
  const firstExit = recorded.entries.findIndex((entry) => entry.type === "runtime_exit");
  if (firstSubagentFrame === undefined || firstOutcome === undefined || firstExit < 0) {
    throw new Error("Expected the recorded subagent's first run.");
  }
  const entries = recorded.entries
    // A CLI exit settles the task; hold at query.open:2 until the crash snapshot instead.
    .filter(
      (_, index) => (index < firstSubagentFrame || index > firstOutcome) && index !== firstExit,
    )
    .map((entry) => {
      if (entry.type !== "expect_outbound" || entry.label !== "prompt.offer:2") return entry;
      const frame = entry.frame as {
        readonly message: { readonly message: { readonly content: string } };
      };
      return {
        ...entry,
        frame: {
          ...frame,
          message: {
            ...frame.message,
            message: {
              ...frame.message.message,
              content: [
                "Note: the T3 server restarted, and this background work was cancelled before it finished. It will not report back on its own, so resume or restart what you still need and wait for its result:",
                "- subagent: Audit src/math.ts (id a44c0ed810da9e4f3)",
                "",
                "User message:",
                frame.message.message.content,
              ].join("\n"),
            },
          },
        },
      };
    });
  return yield* ClaudeOrchestratorReplayHarness.decodeTranscript({ ...recorded, entries });
});
const readCursorTranscript = Effect.fn("readCursorRecoveryTranscript")(function* () {
  const transcript = yield* readRawTranscript(
    new URL("./fixtures/provider_thread_resume/cursor_transcript.ndjson", import.meta.url),
  );
  return yield* CursorOrchestratorReplayHarness.decodeTranscript(
    materializeReplayTranscriptRuntimeInstructions(transcript, {
      driver: ProviderDriverKind.make("cursor"),
      model: CURSOR_MODEL_SELECTION.model,
    }),
  );
});

function splitAfterFirstIdle(materialized: MaterializedOrchestratorFixtureInput) {
  const splitIndex = materialized.steps.findIndex((step) => step.type === "await_thread_idle");
  if (splitIndex < 0) {
    throw new Error("Expected fixture to contain await_thread_idle after the first turn.");
  }

  const phase1Steps = materialized.steps.slice(0, splitIndex + 1);
  const phase2Steps = materialized.steps.slice(splitIndex + 1);
  return {
    phase1Steps,
    phase2Steps,
    phase1Commands: phase1Steps.flatMap((step) => (step.type === "dispatch" ? [step.command] : [])),
    phase2Commands: phase2Steps.flatMap((step) => (step.type === "dispatch" ? [step.command] : [])),
  };
}

const runCursorRecovery = Effect.fn("runCursorRecovery")(function* (input: {
  readonly transcript: CursorAgentSdkReplayTranscript;
  readonly runner: ReturnType<typeof makeCursorAgentSdkReplayRunner>;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const tempDir = yield* Effect.acquireRelease(
    fs.makeTempDirectory({
      prefix: "t3-orchestration-v2-cursor-recovery-",
    }),
    (directory) => fs.remove(directory, { recursive: true, force: true }).pipe(Effect.orDie),
  );
  yield* fs.makeDirectory(tempDir, { recursive: true });
  const dbPath = path.join(tempDir, "state.sqlite");
  const materialized = yield* materializeFixtureInput({
    scenario: "provider_thread_resume",
    fixtureInput: {
      steps: [
        { type: "message", text: PROVIDER_THREAD_RESUME_FIRST_PROMPT },
        { type: "message", text: PROVIDER_THREAD_RESUME_SECOND_PROMPT },
      ],
    },
    driver: ProviderDriverKind.make("cursor"),
    modelSelection: CURSOR_MODEL_SELECTION,
  });
  const { phase1Commands, phase1Steps, phase2Commands, phase2Steps } =
    splitAfterFirstIdle(materialized);
  const options = {
    databaseLayer: SqlitePersistence.layerFromPath(dbPath).pipe(
      Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
    ),
  };
  const harness = {
    ...CursorOrchestratorReplayHarness,
    makeProviderAdapterRegistryLayer: () =>
      CursorAdapterV2Testkit.layer(input.transcript, {
        runner: input.runner,
        assertCompleteOnFinalize: false,
      }),
  };

  yield* Effect.scoped(
    runOrchestratorV2ProviderReplayScenario(
      {
        name: "provider_thread_resume/cursor:first-runtime",
        transcript: input.transcript,
        commands: phase1Commands,
        steps: phase1Steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { cwd: tempDir },
      },
      harness,
      options,
    ),
  );

  const result = yield* Effect.scoped(
    runOrchestratorV2ProviderReplayScenario(
      {
        name: "provider_thread_resume/cursor:second-runtime",
        transcript: input.transcript,
        commands: phase2Commands,
        steps: phase2Steps,
        projectionThreadIds: materialized.projectionThreadIds,
        runtimePolicyOverride: { cwd: tempDir },
      },
      harness,
      // A real restart requeues effects, like checkpoint capture, the first runtime left running.
      { ...options, recoverOnStartup: true },
    ),
  );

  assertBaseProjection({
    result,
    transcript: input.transcript,
    runCount: 2,
    runStatuses: ["completed", "completed"],
  });
  const projection = projectionFor(result, input.transcript.scenario);
  assertSemanticProjectionIntegrity(projection);
  assertRunOrdinals(projection, [1, 2]);
  assertConversationMessageRoles(projection, ["user", "assistant", "user", "assistant"]);
  assertTurnItemTypes(projection, ["user_message", "assistant_message"]);
  assertUserMessagesInclude(projection, [
    PROVIDER_THREAD_RESUME_FIRST_PROMPT,
    PROVIDER_THREAD_RESUME_SECOND_PROMPT,
  ]);
  assertAssistantTextIncludes(projection, FIRST_FINAL);
  assertAssistantTextIncludes(projection, SECOND_FINAL);
  assert.lengthOf(projection.providerThreads, 1);
});

describe("orchestrator replay recovery", () => {
  it.effect(
    "startup replay preserves workflow-owned native work while recovering ordinary work",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const workspace = yield* checkpointWorkspace("provider_thread_resume");
          const transcript = yield* readCodexTranscript(workspace);
          const driver = yield* CodexReplay.makeReplayDriver(transcript);
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const dir = yield* fs.makeTempDirectoryScoped({ prefix: "t3-owned-recovery-" });
          const databaseLayer = SqlitePersistence.layerFromPath(
            path.join(dir, "state.sqlite"),
          ).pipe(Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));
          const harness = {
            ...CodexOrchestratorReplayHarness,
            makeProviderAdapterRegistryLayer: () =>
              CodexAdapterV2Testkit.layer({ transcript, driver }),
          };
          const owned = ThreadId.make("thread:replay-owned");
          const ordinary = ThreadId.make("thread:replay-ordinary");
          const ownedRun = RunId.make("run:replay-owned");
          const ordinaryRun = RunId.make("run:replay-ordinary");
          const scenario = {
            name: "workflow-owned-recovery",
            transcript,
            commands: [],
            steps: [],
            projectionThreadIds: [owned, ordinary],
            runtimePolicyOverride: { cwd: workspace },
          };
          const before = yield* Effect.scoped(
            Effect.gen(function* () {
              const sink = yield* EventSink.EventSinkV2;
              const sql = yield* SqlClient.SqlClient;
              const now = yield* DateTime.now;
              yield* sink.write({
                events: [
                  [owned, ownedRun],
                  [ordinary, ordinaryRun],
                ].flatMap(([id, run]) => {
                  const threadId = id as ThreadId;
                  const runId = run as RunId;
                  return [
                    {
                      id: EventId.make(`create:${threadId}`),
                      type: "thread.created",
                      threadId,
                      occurredAt: now,
                      payload: makeThread(threadId, now),
                    },
                    {
                      id: EventId.make(`create:${runId}`),
                      type: "run.created",
                      threadId,
                      runId,
                      providerInstanceId,
                      occurredAt: now,
                      payload: {
                        id: runId,
                        threadId,
                        ordinal: 1,
                        providerInstanceId,
                        modelSelection,
                        providerThreadId: null,
                        userMessageId: MessageId.make(`message:${runId}`),
                        rootNodeId: null,
                        activeAttemptId: null,
                        status: "running",
                        queuePosition: null,
                        requestedAt: now,
                        startedAt: now,
                        completedAt: null,
                        checkpointId: null,
                        contextHandoffId: null,
                      },
                    },
                  ] satisfies OrchestrationV2DomainEvent[];
                }),
              });
              yield* seedValidWorkflowOperation(owned, ownedRun);
              const commandId = CommandId.make("command:owned-existing-continuation");
              yield* sink.writeWithEffects({
                commandId,
                events: [],
                effects: [
                  {
                    id: `effect:restart-continuation:${ownedRun}`,
                    commandId,
                    threadId: owned,
                    request: { type: "provider-runtime.continue", sourceRunId: ownedRun },
                  },
                ],
              });
              return {
                journal: yield* sql`SELECT * FROM workflow_operations`,
                ownedEvents:
                  yield* sql`SELECT * FROM orchestration_events WHERE aggregate_kind = 'thread' AND stream_id = ${owned} ORDER BY sequence`,
              };
            }).pipe(
              Effect.provide(
                Layer.merge(
                  layerProviderReplay(scenario, harness, { databaseLayer, runEffectWorker: false }),
                  databaseLayer,
                ),
              ),
            ),
          );
          const after = yield* Effect.scoped(
            Effect.gen(function* () {
              const result = yield* runOrchestratorV2Scenario(scenario);
              const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
              const executed = yield* worker.drain();
              const sql = yield* SqlClient.SqlClient;
              return {
                result,
                executed,
                journal: yield* sql`SELECT * FROM workflow_operations`,
                ownedEvents:
                  yield* sql`SELECT * FROM orchestration_events WHERE aggregate_kind = 'thread' AND stream_id = ${owned} ORDER BY sequence`,
                continuation:
                  yield* sql`SELECT * FROM orchestration_v2_effect_outbox WHERE thread_id = ${owned} AND effect_id LIKE 'effect:restart-continuation:%'`,
              };
            }).pipe(
              Effect.provide(
                Layer.merge(
                  layerProviderReplay(scenario, harness, {
                    databaseLayer,
                    recoverOnStartup: true,
                    continueThreadsAfterServerUpdate: true,
                  }),
                  databaseLayer,
                ),
              ),
            ),
          );
          assert.equal(after.result.projections.get(owned)?.runs[0]?.status, "running");
          assert.equal(after.result.projections.get(ordinary)?.runs[0]?.status, "cancelled");
          assert.deepEqual(after.journal, before.journal);
          assert.isAbove(before.ownedEvents.length, 0);
          assert.deepEqual(after.ownedEvents, before.ownedEvents);
          assert.equal(after.executed, 0);
          assert.lengthOf(after.continuation, 1);
          assert.equal(after.continuation[0]?.status, "pending");
        }).pipe(
          provideDeterministicTestRuntime,
          Effect.provide(
            Layer.merge(IdAllocator.layer, Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
          ),
        ),
      ),
  );

  it.effect(
    "resumes a provider-native Codex thread after recreating the orchestrator runtime",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          // Use a throwaway checkpoint workspace; cold full-repo capture exceeds CI wait budgets.
          const workspace = yield* checkpointWorkspace("provider_thread_resume");
          const transcript = yield* readCodexTranscript(workspace);
          const fs = yield* FileSystem.FileSystem;
          const path = yield* Path.Path;
          const tempDir = yield* Effect.acquireRelease(
            fs.makeTempDirectory({
              prefix: "t3-orchestration-v2-recovery-",
            }),
            (directory) =>
              fs.remove(directory, { recursive: true, force: true }).pipe(Effect.orDie),
          );
          yield* fs.makeDirectory(tempDir, { recursive: true });
          const dbPath = path.join(tempDir, "state.sqlite");
          const driver = yield* CodexReplay.makeReplayDriver(transcript);
          const materialized = yield* materializeFixtureInput({
            scenario: "provider_thread_resume",
            fixtureInput: {
              steps: [
                { type: "message", text: PROVIDER_THREAD_RESUME_FIRST_PROMPT },
                { type: "message", text: PROVIDER_THREAD_RESUME_SECOND_PROMPT },
              ],
            },
            driver: ProviderDriverKind.make("codex"),
            modelSelection: CODEX_MODEL_SELECTION,
          });
          const { phase1Commands, phase1Steps, phase2Commands, phase2Steps } =
            splitAfterFirstIdle(materialized);

          const harness = {
            ...CodexOrchestratorReplayHarness,
            makeProviderAdapterRegistryLayer: () =>
              CodexAdapterV2Testkit.layer({ transcript, driver }),
          };
          const options = {
            databaseLayer: SqlitePersistence.layerFromPath(dbPath).pipe(
              Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
            ),
          };

          yield* runOrchestratorV2ProviderReplayScenario(
            {
              name: "provider_thread_resume/codex:first-runtime",
              transcript,
              commands: phase1Commands,
              steps: phase1Steps,
              projectionThreadIds: materialized.projectionThreadIds,
              runtimePolicyOverride: { cwd: workspace },
            },
            harness,
            options,
          );

          const result = yield* runOrchestratorV2ProviderReplayScenario(
            {
              name: "provider_thread_resume/codex:second-runtime",
              transcript,
              commands: phase2Commands,
              steps: phase2Steps,
              projectionThreadIds: materialized.projectionThreadIds,
              runtimePolicyOverride: { cwd: workspace },
            },
            harness,
            { ...options, recoverOnStartup: true },
          );

          assertBaseProjection({
            result,
            transcript,
            runCount: 2,
            runStatuses: ["completed", "completed"],
          });
          const projection = projectionFor(result, transcript.scenario);
          assertSemanticProjectionIntegrity(projection);
          assertRunOrdinals(projection, [1, 2]);
          assertConversationMessageRoles(projection, ["user", "assistant", "user", "assistant"]);
          assertTurnItemTypes(projection, ["user_message", "assistant_message"]);
          assertUserMessagesInclude(projection, [
            PROVIDER_THREAD_RESUME_FIRST_PROMPT,
            PROVIDER_THREAD_RESUME_SECOND_PROMPT,
          ]);
          assertAssistantTextIncludes(projection, FIRST_FINAL);
          assertAssistantTextIncludes(projection, SECOND_FINAL);
          assert.lengthOf(projection.providerThreads, 1);
        }).pipe(
          provideDeterministicTestRuntime,
          Effect.provide(
            Layer.merge(IdAllocator.layer, Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
          ),
        ),
      ),
  );

  it.effect(
    "resumes a provider-native Cursor thread after recreating the orchestrator runtime",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const transcript = yield* readCursorTranscript();
          const runner = makeCursorAgentSdkReplayRunner(transcript);
          yield* runCursorRecovery({ transcript, runner });
          yield* runner.assertComplete;
        }).pipe(
          provideDeterministicTestRuntime,
          Effect.provide(
            Layer.merge(IdAllocator.layer, Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
          ),
        ),
      ),
  );
  it.effect("keeps a Claude subagent resumed after a server restart in its own thread", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = yield* readClaudeSubagentResumeTranscript();
        const [launchPrompt, resumePrompt] = yield* decodePromptPair(transcript.metadata?.prompts);
        const workspace = yield* checkpointWorkspace(transcript.scenario);
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* Effect.acquireRelease(
          fs.makeTempDirectory({ prefix: "t3-orchestration-v2-claude-recovery-" }),
          (directory) => fs.remove(directory, { recursive: true, force: true }).pipe(Effect.orDie),
        );
        const materialized = yield* materializeFixtureInput({
          scenario: transcript.scenario,
          fixtureInput: {
            steps: [
              { type: "message", text: launchPrompt },
              { type: "message", text: resumePrompt },
            ],
          },
          driver: ProviderDriverKind.make("claudeAgent"),
          modelSelection: CLAUDE_MODEL_SELECTION,
        });
        const { phase1Commands, phase1Steps, phase2Commands, phase2Steps } =
          splitAfterFirstIdle(materialized);
        const { harness, assertComplete } = makeClaudeRestartReplayHarness(transcript);
        const options = {
          databaseLayer: SqlitePersistence.layerFromPath(path.join(tempDir, "state.sqlite")).pipe(
            Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
          ),
        };

        yield* Effect.scoped(
          runOrchestratorV2ProviderReplayScenario(
            {
              name: `${transcript.scenario}:first-runtime`,
              transcript,
              commands: phase1Commands,
              steps: phase1Steps,
              projectionThreadIds: materialized.projectionThreadIds,
              runtimePolicyOverride: { cwd: workspace },
            },
            harness,
            options,
          ),
        );
        const result = yield* Effect.scoped(
          runOrchestratorV2ProviderReplayScenario(
            {
              name: `${transcript.scenario}:second-runtime`,
              transcript,
              commands: phase2Commands,
              steps: phase2Steps,
              projectionThreadIds: materialized.projectionThreadIds,
              runtimePolicyOverride: { cwd: workspace },
            },
            harness,
            { ...options, recoverOnStartup: true },
          ),
        );
        yield* assertComplete;

        const projection = projectionFor(result, transcript.scenario);
        assertSemanticProjectionIntegrity(projection);
        assert.lengthOf(projection.subagents, 1);
        const subagent = projection.subagents[0];
        assert.equal(subagent?.status, "completed");
        const childThreads = [...result.projections.values()].filter(
          (candidate) => candidate.thread.lineage.parentThreadId === projection.thread.id,
        );
        assert.lengthOf(childThreads, 1);
        const child = childThreads[0];
        assert.equal(child?.thread.id, subagent?.childThreadId);
        assert.deepEqual(child?.thread.modelSelection, {
          instanceId: CLAUDE_MODEL_SELECTION.instanceId,
          model: "claude-sonnet-5",
          options: [{ id: "effort", value: "high" }],
        });

        const conversation = (child?.turnItems ?? [])
          .toSorted((left, right) => left.ordinal - right.ordinal)
          .flatMap((item) =>
            item.type === "user_message" || item.type === "assistant_message"
              ? [`${item.type === "user_message" ? "user" : "assistant"}:${item.text}`]
              : [],
          );
        const launchTask = conversation[0];
        assert.deepEqual(
          conversation.map((entry) => entry.slice(0, entry.indexOf(":"))),
          ["user", "assistant", "user", "assistant"],
        );
        assert.include(launchTask, "Your job is just the file");
        assert.include(conversation[1], "is 1 line long");
        assert.include(conversation[2], "Look again at");
        assert.include(conversation[3], "Bug/edge case found and fixed");
        const childCommands = (child?.turnItems ?? []).filter(
          (item) => item.type === "command_execution",
        );
        assert.lengthOf(childCommands, 3);
        assert.isFalse(
          projection.turnItems.some(
            (item) =>
              item.type === "assistant_message" && item.text.includes("Bug/edge case found"),
          ),
          "the resumed subagent's reply leaked into the parent thread",
        );
      }).pipe(
        provideDeterministicTestRuntime,
        Effect.provide(
          Layer.merge(IdAllocator.layer, Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        ),
      ),
    ),
  );
  it.effect("reopens the thread of a Claude subagent the restart cancelled when resumed", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const transcript = yield* readClaudeCancelledSubagentResumeTranscript();
        const [launchPrompt, resumePrompt] = yield* decodePromptPair(transcript.metadata?.prompts);
        const workspace = yield* checkpointWorkspace(transcript.scenario);
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const tempDir = yield* Effect.acquireRelease(
          fs.makeTempDirectory({ prefix: "t3-orchestration-v2-claude-recovery-" }),
          (directory) => fs.remove(directory, { recursive: true, force: true }).pipe(Effect.orDie),
        );
        const materialized = yield* materializeFixtureInput({
          scenario: transcript.scenario,
          fixtureInput: {
            steps: [
              { type: "message", text: launchPrompt },
              { type: "message", text: resumePrompt },
            ],
          },
          driver: ProviderDriverKind.make("claudeAgent"),
          modelSelection: CLAUDE_MODEL_SELECTION,
        });
        const { phase1Commands, phase1Steps, phase2Commands, phase2Steps } =
          splitAfterFirstIdle(materialized);
        const { harness, assertComplete } = makeClaudeRestartReplayHarness(transcript);
        const options = {
          databaseLayer: SqlitePersistence.layerFromPath(path.join(tempDir, "state.sqlite")).pipe(
            Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
          ),
        };
        const childThreadOf = (result: OrchestratorV2ScenarioResult, parentThreadId: ThreadId) =>
          [...result.projections.values()].find(
            (candidate) => candidate.thread.lineage.parentThreadId === parentThreadId,
          );
        const childRootStatus = (child: OrchestrationV2ThreadProjection | undefined) =>
          child?.nodes.find((node) => node.kind === "root_turn" && node.runId === null)?.status;

        const snapshotPath = path.join(tempDir, "restart-state.sqlite");
        const beforeScenario = {
          name: `${transcript.scenario}:cancelled-first-runtime`,
          transcript,
          commands: phase1Commands,
          steps: phase1Steps,
          projectionThreadIds: materialized.projectionThreadIds,
          runtimePolicyOverride: { cwd: workspace },
        };
        const before = yield* Effect.scoped(
          Effect.gen(function* () {
            const result = yield* runOrchestratorV2Scenario(beforeScenario);
            const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
            yield* worker.drain();
            // Preserve the orphan before scoped teardown settles its provider work.
            const sql = yield* SqlClient.SqlClient;
            yield* sql`VACUUM INTO ${snapshotPath}`;
            return result;
          }).pipe(
            Effect.provide(
              Layer.merge(
                layerProviderReplay(beforeScenario, harness, options),
                options.databaseLayer,
              ),
            ),
          ),
        );
        const settled = projectionFor(before, transcript.scenario);
        assert.equal(settled.subagents[0]?.status, "running");
        assert.equal(childRootStatus(childThreadOf(before, settled.thread.id)), "running");

        const result = yield* Effect.scoped(
          runOrchestratorV2ProviderReplayScenario(
            {
              name: `${transcript.scenario}:cancelled-second-runtime`,
              transcript,
              commands: phase2Commands,
              steps: phase2Steps,
              projectionThreadIds: materialized.projectionThreadIds,
              runtimePolicyOverride: { cwd: workspace },
            },
            harness,
            {
              databaseLayer: SqlitePersistence.layerFromPath(snapshotPath).pipe(
                Layer.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
              ),
              recoverOnStartup: true,
              continueThreadsAfterServerUpdate: false,
            },
          ),
        );
        yield* assertComplete;

        const projection = projectionFor(result, transcript.scenario);
        assertSemanticProjectionIntegrity(projection);
        assert.equal(projection.subagents[0]?.status, "completed");
        const child = childThreadOf(result, projection.thread.id);
        assert.equal(child?.thread.id, projection.subagents[0]?.childThreadId);
        assert.equal(childRootStatus(child), "completed");
        const conversation = (child?.turnItems ?? [])
          .toSorted((left, right) => left.ordinal - right.ordinal)
          .flatMap((item) =>
            item.type === "user_message" || item.type === "assistant_message"
              ? [`${item.type === "user_message" ? "user" : "assistant"}:${item.text}`]
              : [],
          );
        assert.deepEqual(
          conversation.map((entry) => entry.slice(0, entry.indexOf(":"))),
          ["user", "user", "assistant"],
        );
        assert.include(conversation[0], "Your job is just the file");
        assert.include(conversation[1], "Look again at");
        assert.include(conversation[2], "Bug/edge case found and fixed");
        assert.lengthOf(
          (child?.turnItems ?? []).filter((item) => item.type === "command_execution"),
          2,
        );
      }).pipe(
        provideDeterministicTestRuntime,
        Effect.provide(
          Layer.merge(IdAllocator.layer, Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
        ),
      ),
    ),
  );
});
