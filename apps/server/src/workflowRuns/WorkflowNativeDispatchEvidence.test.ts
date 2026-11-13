// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import { assert, it } from "@effect/vitest";
import {
  OrchestrationV2AppThreadJson,
  OrchestrationV2RunJson,
  OrchestrationV2ConversationMessageJson,
  OrchestrationV2DomainEventJson,
  RunId,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { runMigrations } from "../persistence/Migrations.ts";
import {
  readNativeDispatchEvidence,
  readNativeEvidenceStrict,
} from "./WorkflowNativeDispatchEvidence.ts";

const ids = {
  launchCommandId: "launch",
  threadId: "thread",
  projectId: "project",
  messageId: "message",
};
const commandId = "launch:initial-message";
const time = "2026-11-01T00:00:00.000Z";
const modelSelection = { instanceId: "codex", model: "model" };
const thread = {
  id: "thread",
  projectId: "project",
  title: "Thread",
  providerInstanceId: "codex",
  modelSelection,
  createdBy: "user",
  creationSource: "web",
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: "thread" },
  forkedFrom: null,
  createdAt: time,
  updatedAt: time,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
};
const run = {
  id: "t3-run",
  threadId: "thread",
  ordinal: 1,
  providerInstanceId: "codex",
  modelSelection,
  providerThreadId: "provider-thread",
  userMessageId: "message",
  rootNodeId: null,
  activeAttemptId: null,
  status: "preparing",
  requestedAt: time,
  startedAt: null,
  completedAt: null,
  checkpointId: null,
  contextHandoffId: null,
};
const message = {
  createdBy: "user",
  creationSource: "web",
  id: "message",
  threadId: "thread",
  runId: "t3-run",
  nodeId: null,
  role: "user",
  text: "hello",
  attachments: [],
  streaming: false,
  createdAt: time,
  updatedAt: time,
};
const json = JSON.stringify;
const threadJson = Schema.encodeSync(Schema.fromJsonString(OrchestrationV2AppThreadJson))(
  Schema.decodeUnknownSync(OrchestrationV2AppThreadJson)(thread),
);
const runJson = Schema.encodeSync(Schema.fromJsonString(OrchestrationV2RunJson))(
  Schema.decodeUnknownSync(OrchestrationV2RunJson)(run),
);
const messageJson = Schema.encodeSync(
  Schema.fromJsonString(OrchestrationV2ConversationMessageJson),
)(Schema.decodeUnknownSync(OrchestrationV2ConversationMessageJson)(message));
const plan = {
  id: "plan",
  threadId: "foreign-thread",
  runId: "prior-run",
  nodeId: "prior-node",
  status: "completed",
  kind: "proposed_plan",
  markdown: "Plan",
};
const providerTurn = {
  id: "provider-turn-id",
  providerThreadId: "provider-thread",
  nodeId: "node",
  runAttemptId: null,
  nativeTurnRef: null,
  ordinal: 1,
  status: "completed",
  startedAt: time,
  completedAt: time,
};
const decodeFixtureEvent = Schema.decodeUnknownSync(OrchestrationV2DomainEventJson);
const encodeFixtureEvent = Schema.encodeSync(OrchestrationV2DomainEventJson);
const encodedEvents = [
  {
    id: "event-run",
    threadId: "thread",
    runId: "t3-run",
    occurredAt: time,
    type: "run.created",
    payload: run,
  },
  {
    id: "event-message",
    threadId: "thread",
    runId: "t3-run",
    occurredAt: time,
    type: "message.updated",
    payload: message,
  },
  {
    id: "event-plan",
    threadId: "foreign-thread",
    runId: "prior-run",
    occurredAt: time,
    type: "plan.updated",
    payload: plan,
  },
  {
    id: "event-turn",
    threadId: "thread",
    runId: "t3-run",
    occurredAt: time,
    type: "provider-turn.updated",
    payload: providerTurn,
  },
].map((fixture) => encodeFixtureEvent(decodeFixtureEvent(fixture)));
const seed = Effect.fn("test.seed")(function* (sql: SqlClient.SqlClient) {
  yield* sql`INSERT INTO projection_projects (project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES ('project','Project','/work','[]',${time},${time})`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads (thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES ('thread','project','Thread','codex','full-access','default',${time},${time},${threadJson})`;
  yield* sql`INSERT INTO orchestration_v2_projection_runs (run_id,thread_id,ordinal,provider,status,requested_at,payload_json) VALUES ('t3-run','thread',1,'codex','preparing',${time},${runJson})`;
  yield* sql`INSERT INTO orchestration_v2_projection_messages (message_id,thread_id,run_id,role,streaming,created_at,updated_at,payload_json) VALUES ('message','thread','t3-run','user',0,${time},${time},${messageJson})`;
  yield* sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type) VALUES (${commandId},'thread','thread',${time},5,'accepted',NULL,'message.dispatch')`;
  yield* sql`INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES (1,'event-run','thread','thread',1,'run.created',${time},${commandId},'client',${json(encodedEvents[0]!.payload)},${json({ applicationEventVersion: 2, runId: "t3-run" })},2)`;
  yield* sql`INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES (2,'event-turn','thread','thread',2,'provider-turn.updated',${time},'other-command','client',${json(encodedEvents[3]!.payload)},${json({ applicationEventVersion: 2, runId: "t3-run" })},2)`;
  yield* sql`INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES (3,'other-turn','thread','thread',3,'provider-turn.updated',${time},'other-command','client',${json(encodedEvents[3]!.payload)},${json({ applicationEventVersion: 2, runId: "t3-run" })},2)`;
  yield* sql`INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES (4,'event-message','thread','thread',4,'message.updated',${time},${commandId},'client',${json(encodedEvents[1]!.payload)},${json({ applicationEventVersion: 2, runId: "t3-run" })},2)`;
  yield* sql`INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES (5,'event-plan','thread','foreign-thread',0,'plan.updated',${time},${commandId},'client',${json(encodedEvents[2]!.payload)},${json({ applicationEventVersion: 2, runId: "prior-run" })},2)`;
});
const withDatabase = <E>(
  f: (sql: SqlClient.SqlClient, filename: string) => Effect.Effect<void, E>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const dir = yield* Effect.acquireRelease(
        Effect.sync(() => NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "native-evidence-"))),
        (path) => Effect.sync(() => NodeFS.rmSync(path, { recursive: true, force: true })),
      );
      const filename = NodePath.join(dir, "test.sqlite");
      yield* Effect.scoped(
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* runMigrations();
          yield* f(sql, filename);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename, readerWorker: true }))),
      );
    }),
  );

it.live(
  "reads writer-local evidence, permits foreign prior-plan events and rolls everything back",
  () =>
    withDatabase((sql, filename) =>
      Effect.scoped(
        Effect.gen(function* () {
          const reader = yield* SqlClient.SqlClient;
          const count = NodeSqliteClient.readOnly(
            reader<{ count: number }>`SELECT COUNT(*) AS count FROM orchestration_events`,
          );
          assert.strictEqual((yield* count)[0]?.count, 0);
          const rollback = yield* Effect.flip(
            sql.withTransaction(
              Effect.gen(function* () {
                yield* seed(sql);
                assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), {
                  kind: "verified",
                  actualT3RunId: RunId.make("t3-run"),
                  receiptSequence: 5,
                });
                assert.deepStrictEqual(
                  yield* NodeSqliteClient.readOnly(readNativeDispatchEvidence(sql, ids)),
                  { kind: "verified", actualT3RunId: RunId.make("t3-run"), receiptSequence: 5 },
                );
                assert.strictEqual((yield* count)[0]?.count, 0);
                return yield* Effect.fail("rollback");
              }),
            ),
          );
          assert.strictEqual(rollback, "rollback");
          const rows = yield* sql<{
            count: number;
          }>`SELECT (SELECT COUNT(*) FROM orchestration_events) + (SELECT COUNT(*) FROM orchestration_command_receipts) + (SELECT COUNT(*) FROM projection_projects) + (SELECT COUNT(*) FROM orchestration_v2_projection_threads) + (SELECT COUNT(*) FROM orchestration_v2_projection_runs) + (SELECT COUNT(*) FROM orchestration_v2_projection_messages) AS count`;
          assert.strictEqual(rows[0]?.count, 0);
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename, readerWorker: true }))),
      ),
    ),
);

it.live("fails closed on missing, pruned and contradictory evidence", () =>
  withDatabase((sql) =>
    Effect.gen(function* () {
      assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "conflict" });
      yield* sql.withTransaction(
        Effect.gen(function* () {
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "missing" });
          yield* seed(sql);
          yield* sql`UPDATE orchestration_command_receipts SET accepted_at = 'invalid timestamp'`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "conflict" });
          yield* sql`UPDATE orchestration_command_receipts SET accepted_at = ${time}`;
          const terminalRun = json({ ...run, status: "completed", completedAt: time });
          const changedThread = json({ ...thread, deletedAt: time });
          const changedMessage = json({ ...message, text: "edited" });
          yield* sql`UPDATE orchestration_v2_projection_runs SET status = 'completed', payload_json = ${terminalRun}`;
          yield* sql`UPDATE orchestration_v2_projection_threads SET deleted_at = ${time}, payload_json = ${changedThread}`;
          yield* sql`UPDATE orchestration_v2_projection_messages SET payload_json = ${changedMessage}`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), {
            kind: "verified",
            actualT3RunId: RunId.make("t3-run"),
            receiptSequence: 5,
          });
          yield* sql`UPDATE orchestration_v2_projection_messages SET run_id = 'provider-turn-id'`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "conflict" });
          yield* sql`UPDATE orchestration_v2_projection_messages SET run_id = 't3-run'`;
          yield* sql`UPDATE orchestration_command_receipts SET status = 'rejected'`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "conflict" });
          yield* sql`UPDATE orchestration_command_receipts SET status = 'accepted'`;
          yield* sql`UPDATE orchestration_events SET payload_json = 'not json' WHERE event_id = 'event-message'`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "conflict" });
          yield* sql`UPDATE orchestration_events SET payload_json = ${messageJson} WHERE event_id = 'event-message'`;
          yield* sql`UPDATE orchestration_events SET event_type = 'run.created', payload_json = ${runJson} WHERE event_id = 'event-plan'`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "conflict" });
          yield* sql`UPDATE orchestration_events SET event_type = 'plan.updated', payload_json = ${json(plan)} WHERE event_id = 'event-plan'`;
          yield* sql`DELETE FROM orchestration_v2_projection_messages WHERE message_id = 'message'`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "missing" });
          yield* sql`UPDATE orchestration_v2_projection_threads SET project_id = 'wrong-project'`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "conflict" });
          yield* sql`UPDATE orchestration_v2_projection_threads SET project_id = 'project'`;
          yield* sql`DELETE FROM orchestration_events WHERE event_id = 'event-plan'`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "missing" });
          yield* sql`UPDATE orchestration_command_receipts SET result_sequence = 3 WHERE command_id = ${commandId}`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "conflict" });
          yield* sql`UPDATE orchestration_command_receipts SET result_sequence = 5 WHERE command_id = ${commandId}`;
          yield* sql`UPDATE orchestration_events SET metadata_json = ${json({ applicationEventVersion: 2, runId: "provider-turn-id" })} WHERE event_id = 'event-run'`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "conflict" });
          yield* sql`UPDATE orchestration_events SET metadata_json = ${json({ applicationEventVersion: 2, runId: "t3-run" })} WHERE event_id = 'event-run'`;
          yield* sql`UPDATE orchestration_command_receipts SET result_sequence = 4`;
          yield* sql`DELETE FROM orchestration_events WHERE event_id = 'event-run'`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "missing" });
          yield* sql`INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES (1,'event-run','thread','thread',1,'run.created',${time},${commandId},'client',${runJson},${json({ applicationEventVersion: 2, runId: "t3-run" })},2)`;
          yield* sql`DROP TABLE projection_projects`;
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), { kind: "conflict" });
        }),
      );
    }),
  ),
);

it.live("rejects another client's transaction and invalid retained ids", () =>
  withDatabase((sql, filename) =>
    Effect.gen(function* () {
      yield* Effect.scoped(
        Effect.gen(function* () {
          const other = yield* SqlClient.SqlClient;
          yield* other.withTransaction(
            Effect.gen(function* () {
              assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), {
                kind: "conflict",
              });
            }),
          );
        }).pipe(Effect.provide(NodeSqliteClient.layer({ filename, readerWorker: true }))),
      );
      yield* sql.withTransaction(
        Effect.gen(function* () {
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, { ...ids, threadId: "" }), {
            kind: "conflict",
          });
        }),
      );
    }),
  ),
);

it.live(
  "strictly verifies claim events and preserves SQL errors while the legacy reader fails closed",
  () =>
    withDatabase((sql) =>
      sql.withTransaction(
        Effect.gen(function* () {
          yield* seed(sql);
          assert.strictEqual(
            (yield* readNativeEvidenceStrict(sql, { ...ids, phase: "message" })).kind,
            "verified",
          );
          yield* sql`UPDATE orchestration_command_receipts SET command_type = 'thread.create' WHERE command_id = ${commandId}`;
          assert.strictEqual(
            (yield* readNativeEvidenceStrict(sql, { ...ids, phase: "message" })).kind,
            "conflict",
          );
          yield* sql`UPDATE orchestration_command_receipts SET command_type = 'message.dispatch', result_sequence = 4 WHERE command_id = ${commandId}`;
          assert.strictEqual(
            (yield* readNativeEvidenceStrict(sql, { ...ids, phase: "message" })).kind,
            "conflict",
          );
          yield* sql`UPDATE orchestration_command_receipts SET result_sequence = 5 WHERE command_id = ${commandId}`;
          const selection = {
            ...ids,
            phase: "claim" as const,
            claimType: "thread.create" as const,
          };
          yield* sql`INSERT INTO orchestration_command_receipts (command_id,aggregate_kind,aggregate_id,accepted_at,result_sequence,status,error,command_type) VALUES ('launch','thread','thread',${time},6,'accepted',NULL,'thread.create')`;
          yield* sql`INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version) VALUES (6,'claim-event','thread','thread',5,'thread.created',${time},'launch','client',${threadJson},'{}',2)`;
          assert.strictEqual((yield* readNativeEvidenceStrict(sql, selection)).kind, "verified");
          yield* sql`UPDATE orchestration_events SET event_type = 'thread.metadata-updated' WHERE event_id = 'claim-event'`;
          assert.strictEqual((yield* readNativeEvidenceStrict(sql, selection)).kind, "missing");
          yield* sql`UPDATE orchestration_command_receipts SET command_type = 'thread.metadata.update' WHERE command_id = 'launch'`;
          assert.strictEqual(
            (yield* readNativeEvidenceStrict(sql, {
              ...selection,
              claimType: "thread.metadata.update",
            })).kind,
            "verified",
          );
          yield* sql`UPDATE orchestration_events SET stream_id = 'foreign-thread' WHERE event_id = 'claim-event'`;
          assert.strictEqual(
            (yield* readNativeEvidenceStrict(sql, {
              ...selection,
              claimType: "thread.metadata.update",
            })).kind,
            "conflict",
          );
          yield* sql`DROP TABLE orchestration_events`;
          assert.isTrue(
            (yield* Effect.result(readNativeEvidenceStrict(sql, { ...ids, phase: "message" })))
              ._tag === "Failure",
          );
          assert.deepStrictEqual(yield* readNativeDispatchEvidence(sql, ids), {
            kind: "conflict",
          });
        }),
      ),
    ),
);
