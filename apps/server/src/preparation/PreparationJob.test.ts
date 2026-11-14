import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { stableStringify } from "@t3tools/shared/relaySigning";
import migration from "../persistence/Migrations/090_ThreadPreparationJobs.ts";
import { WorkflowCanonicalHash } from "../workflowRuns/WorkflowCanonicalHash.ts";
import * as Job from "./PreparationJob.ts";

const at = "2026-11-14T00:00:00.000Z";
const hash = "a".repeat(64);
const launch = {
  commandId: "launch",
  threadId: "thread",
  projectId: "project",
  title: "Work",
  modelSelection: { instanceId: "instance", model: "model" },
  runtimeMode: "full-access",
  interactionMode: "default",
  workspaceStrategy: { type: "worktree", baseRef: "main", branch: "feature" },
  initialMessage: { messageId: "message", text: "Work", attachments: [] },
  createdBy: "agent",
  creationSource: "mcp",
};
const input = Job.ownInput({
  jobId: "job",
  entryKey: "entry",
  kind: "launch",
  environmentId: "environment",
  projectId: "project",
  threadId: "thread",
  runId: "run",
  originatingLaunchJobId: null,
  parentSelection: null,
  serverBootId: "boot",
  nativeSelection: null,
  nativeOwner: {
    intentId: "intent",
    owner: { kind: "workflow_run", workflowRunId: "workflow" },
    executionClaim: {
      operationId: "operation",
      expectedFingerprint: hash,
      expectedGeneration: 0,
      bootId: "boot",
      workerId: "worker",
    },
  },
  request: {
    phase: "native-preparation",
    materialHash: hash,
    launchHash: WorkflowCanonicalHash.tagged("t3.workflow-native-reservation-launch.v1", launch),
    launch,
    current: {
      driver: "codex",
      providerInstanceId: "instance",
      providerThreadId: "provider-thread",
      providerSessionId: null,
      nativeThreadId: null,
    },
  },
});
const identity = Job.identity(input);
const evidence = () => ({
  v: 1,
  kind: "owned_resources",
  jobId: "job",
  entryKey: "entry",
  priorVersion: 1,
  resources: {
    v: 1,
    entries: [
      {
        resourceId: "11111111-1111-4111-8111-111111111111",
        contextHash: "c".repeat(64),
        recordVersion: 4,
        settlementHash: "d".repeat(64),
      },
    ],
  },
  workspace: {
    v: 1,
    profile: "native-local-worktree-v1",
    path: "/workspace/feature",
    refName: "feature",
    command: {
      commandId: "launch:native-workspace",
      threadId: "thread",
      commandType: "thread.metadata.update",
      acceptedAt: at,
      resultSequence: 1,
      status: "accepted",
      error: null,
      eventId: "event",
      eventType: "thread.metadata-updated",
      occurredAt: at,
    },
  },
});
const row = (settlement: object = evidence()) => ({
  format_version: 1,
  job_id: input.jobId,
  entry_key: input.entryKey,
  kind: input.kind,
  environment_id: input.environmentId,
  project_id: input.projectId,
  thread_id: input.threadId,
  run_id: input.runId,
  originating_launch_job_id: null,
  server_boot_id: input.serverBootId,
  request_identity_json: identity.text,
  request_identity_hash: identity.hash,
  created_at: at,
  record_version: 2,
  state: "settled",
  process_exposed: 1,
  code_closed_at: at,
  settlement_json: stableStringify(settlement),
  settlement_hash: Job.settlementHash(identity.hash, at, settlement),
  settled_at: at,
});

it("decodes an owned native workspace manifest without changing the historical identity", () => {
  const decoded = Job.decodeRow(row());
  assert.deepStrictEqual(decoded.settlement, evidence());
  assert.strictEqual(decoded.requestIdentityHash, identity.hash);
  assert.isTrue(Object.isFrozen(decoded.settlement));
  assert.isTrue(
    Object.isFrozen(
      Job.ownWorkspaceSettlement(decoded.settlement, input, 1, at).resources.entries[0],
    ),
  );
  const malformed: ((value: ReturnType<typeof evidence>) => void)[] = [
    (v) => {
      v.jobId = "foreign";
    },
    (v) => {
      v.entryKey = "foreign";
    },
    (v) => {
      v.priorVersion = 0;
    },
    (v) => {
      v.resources.entries = [];
    },
    (v) => {
      v.resources.entries.push({ ...v.resources.entries[0]! });
    },
    (v) => {
      v.resources.entries[0]!.resourceId = "00000000-0000-0000-0000-000000000000";
    },
    (v) => {
      v.resources.entries[0]!.contextHash = "C".repeat(64);
    },
    (v) => {
      v.resources.entries[0]!.recordVersion = 2;
    },
    (v) => {
      v.workspace.path = "/workspace/../feature";
    },
    (v) => {
      v.workspace.path = "/workspace//feature";
    },
    (v) => {
      v.workspace.refName = "foreign";
    },
    (v) => {
      v.workspace.command.commandId = "foreign:native-workspace";
    },
    (v) => {
      v.workspace.command.threadId = "foreign";
    },
    (v) => {
      v.workspace.command.status = "rejected";
    },
    (v) => {
      v.workspace.command.resultSequence = Number.MAX_SAFE_INTEGER + 1;
    },
    (v) => {
      v.workspace.command.acceptedAt = "2026-11-15T00:00:00.000Z";
    },
    (v) => {
      v.workspace.command.occurredAt = "2026-11-15T00:00:00.000Z";
    },
    (v) => {
      Object.assign(v.workspace.command, { forged: true });
    },
  ];
  for (const change of malformed) {
    const value = evidence();
    change(value);
    assert.throws(() => Job.decodeRow(row(value)));
  }
  assert.throws(() => Job.decodeRow({ ...row(), settlement_hash: "0".repeat(64) }));
  assert.throws(() => Job.decodeRow({ ...row(), process_exposed: 0 }));
});

it("keeps historical owned-resource labels distinct from a complete manifest", () => {
  const legacy = {
    v: 1,
    kind: "owned_resources",
    jobId: "job",
    entryKey: "entry",
    priorVersion: 1,
  };
  assert.deepStrictEqual(Job.decodeRow(row(legacy)).settlement, legacy);
});

it.effect("existing job storage accepts the new immutable envelope in one positive CAS", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* migration;
    const final = row();
    yield* sql`INSERT INTO orchestration_v2_preparation_jobs ${sql.insert({ ...final, record_version: 0, state: "active", process_exposed: 0, code_closed_at: null, settlement_json: null, settlement_hash: null, settled_at: null })}`;
    yield* sql`UPDATE orchestration_v2_preparation_jobs SET process_exposed=1,record_version=1 WHERE job_id='job'`;
    const stored =
      yield* sql`UPDATE orchestration_v2_preparation_jobs SET code_closed_at=${at},state='settled',settlement_json=${final.settlement_json},settlement_hash=${final.settlement_hash},settled_at=${at},record_version=2 WHERE job_id='job' AND record_version=1 RETURNING *`;
    assert.deepStrictEqual(Job.decodeRow(stored[0]), Job.decodeRow(final));
    assert.strictEqual(
      (yield* Effect.result(
        sql`UPDATE orchestration_v2_preparation_jobs SET record_version=3 WHERE job_id='job'`,
      ))._tag,
      "Failure",
    );
  }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
);
