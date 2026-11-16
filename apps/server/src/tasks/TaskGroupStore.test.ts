import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as HashMap from "effect/HashMap";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as RpcTest from "effect/rpc/RpcTest";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import {
  OrchestrationV2DomainEvent,
  WorkTaskPersistedEvent,
  WorkTaskPersistedGroupControlEvent,
  WorkTaskPersistedMembershipEvent,
} from "@t3tools/contracts";
import { runMigrations } from "../persistence/Migrations.ts";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSessionId,
  EnvironmentId,
  WS_METHODS,
  WsRpcGroup,
  WorkTaskCreateInitialGroupInput,
  WorkTaskId,
  WorkTaskManagerGroupId,
  ThreadId,
  CommandId,
  RunId,
} from "@t3tools/contracts";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import * as TaskService from "./TaskService.ts";
import { makeTaskSessionService } from "./TaskSessionService.ts";
import * as TaskStore from "./TaskStore.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import { persistInitialGroup } from "./TaskGroupStore.ts";
import {
  readNativeLaunchReservation,
  reconcileNativeLaunch,
  reserveNativeLaunch,
} from "./TaskNativeLaunchBinding.ts";
import {
  emptyTaskGroupControl,
  reduceTaskGroupControl,
  reduceTaskMembership,
} from "./TaskGroup.ts";
const databaseLayer = NodeSqliteClient.layer({ filename: ":memory:" });
const layer = TaskStore.layer.pipe(Layer.provideMerge(databaseLayer));
const environmentLayer = Layer.succeed(
  ServerEnvironment,
  ServerEnvironment.of({
    getEnvironmentId: Effect.succeed(EnvironmentId.make("env")),
    getDescriptor: Effect.die("unused in task group tests"),
  }),
);
const sessionLayer = TaskService.layer.pipe(
  Layer.provideMerge(layer),
  Layer.provideMerge(environmentLayer),
);
const nativeStores = Layer.mergeAll(TaskStore.layer, EventStore.layer, ProjectionStore.layer).pipe(
  Layer.provideMerge(databaseLayer),
);
const nativeIntegrationLayer = Layer.mergeAll(TaskService.layer, EventSink.layer).pipe(
  Layer.provideMerge(nativeStores),
  Layer.provideMerge(environmentLayer),
);
const decode = Schema.decodeUnknownEffect(WorkTaskPersistedEvent);
const decodeNativeEvent = Schema.decodeEffect(OrchestrationV2DomainEvent);
const decodeControl = Schema.decodeUnknownEffect(WorkTaskPersistedGroupControlEvent);
const decodeRoot = Schema.decodeUnknownEffect(WorkTaskPersistedMembershipEvent);
const decodeGroupInput = Schema.decodeUnknownEffect(WorkTaskCreateInitialGroupInput);
const time = "2026-11-16T00:00:00.000Z";
const definition = {
  revision: 1,
  title: "Outcome",
  objective: "Deliver",
  criteria: [{ id: "criterion", condition: "Done", verification: "Inspect" }],
  actionPolicy: { version: 1, rules: [] },
};
const seed = Effect.fn("TaskGroupStoreTest.seed")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO orchestration_v2_projection_threads (thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES ('manager','project','Manager','codex','full-access','default',${time},${time},'{}')`;
  const store = yield* TaskStore.TaskStore;
  for (const [sequence, details] of [
    [
      1,
      { type: "task.created", attribution: { type: "server" }, projectId: "project", definition },
    ],
    [
      2,
      {
        type: "task.definition-accepted",
        attribution: { type: "user", subject: "owner", sessionId: "session" },
        definition,
      },
    ],
  ] as const) {
    const event = {
      id: `task-${sequence}`,
      taskId: "task",
      occurredAt: time,
      definitionRevision: 1,
      lifecycleRevision: 1,
      completionPolicyRevision: 1,
      actionPolicyRevision: 1,
      ...details,
    };
    const recorded = yield* decode({
      event,
      context: {
        sequence,
        eventId: event.id,
        occurredAt: time,
        commandId: `task-command-${sequence}`,
      },
    });
    yield* sql`INSERT INTO orchestration_events (sequence,event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
      VALUES (${sequence},${event.id},'task','task',${sequence - 1},${event.type},${time},${recorded.context.commandId},'system',${JSON.stringify(event)},'{}',2)`;
    yield* store.applyFoundation(recorded);
  }
});
it.effect("requires an authenticated operator and replays only the same group command", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* seed();
    const session = {
      sessionId: AuthSessionId.make("session"),
      subject: "owner",
      method: "browser-session-cookie" as const,
      scopes: [AuthOrchestrationOperateScope, AuthOrchestrationReadScope],
    };
    const group = {
      taskId: WorkTaskId.make("task"),
      groupId: WorkTaskManagerGroupId.make("group"),
      managerThreadId: ThreadId.make("manager"),
      commandId: CommandId.make("join"),
    };
    const sql = yield* SqlClient.SqlClient;
    const service = yield* TaskService.TaskService;
    const serviceDenied = yield* service
      .createInitialGroup({ ...group, environmentId: "env" }, { type: "server" })
      .pipe(Effect.flip);
    assert.strictEqual(serviceDenied._tag, "WorkTaskError");
    if (serviceDenied._tag === "WorkTaskError")
      assert.strictEqual(serviceDenied.code, "unsupported");
    const denied = yield* makeTaskSessionService({ ...session, method: "bearer-access-token" });
    assert.strictEqual(
      (yield* denied.createInitialGroup(group).pipe(Effect.flip)).code,
      "unsupported",
    );
    const reader = yield* makeTaskSessionService({
      ...session,
      scopes: [AuthOrchestrationReadScope],
    });
    assert.strictEqual(
      (yield* reader.createInitialGroup(group).pipe(Effect.flip)).code,
      "unsupported",
    );
    assert.strictEqual((yield* sql`SELECT * FROM work_task_manager_groups`).length, 0);
    const operator = yield* makeTaskSessionService(session);
    const absent = yield* operator
      .createInitialGroup({ ...group, managerThreadId: ThreadId.make("missing") })
      .pipe(Effect.flip);
    assert.strictEqual(absent.code, "not_found");
    yield* sql`INSERT INTO orchestration_v2_projection_threads (thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES ('other','different','Other','codex','full-access','default',${time},${time},'{}')`;
    assert.strictEqual(
      (yield* operator
        .createInitialGroup({ ...group, managerThreadId: ThreadId.make("other") })
        .pipe(Effect.flip)).code,
      "not_found",
    );
    yield* sql`CREATE TRIGGER reject_group BEFORE INSERT ON work_task_manager_groups BEGIN SELECT RAISE(ABORT,'blocked'); END`;
    assert.isTrue(yield* operator.createInitialGroup(group).pipe(Effect.isFailure));
    assert.strictEqual((yield* sql`SELECT * FROM orchestration_events`).length, 2);
    yield* sql`DROP TRIGGER reject_group`;
    const committed = yield* operator.createInitialGroup(group);
    assert.deepStrictEqual(yield* operator.createInitialGroup(group), committed);
    assert.strictEqual((yield* sql`SELECT * FROM orchestration_events`).length, 6);
    const different = yield* operator
      .createInitialGroup({ ...group, managerThreadId: ThreadId.make("other") })
      .pipe(Effect.flip);
    assert.strictEqual(different.code, "revision_conflict");
    const otherSession = yield* makeTaskSessionService({
      ...session,
      sessionId: AuthSessionId.make("other"),
    });
    assert.strictEqual(
      (yield* otherSession.createInitialGroup(group).pipe(Effect.flip)).code,
      "revision_conflict",
    );
  }).pipe(Effect.provide(sessionLayer)),
);
it.effect("creates a group through authenticated typed RPC and returns its persisted state", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* seed();
    const session = {
      sessionId: AuthSessionId.make("session"),
      subject: "owner",
      method: "browser-session-cookie" as const,
      scopes: [AuthOrchestrationOperateScope, AuthOrchestrationReadScope],
    };
    const handlers = yield* makeTaskSessionService(session);
    const group = WsRpcGroup.omit(
      ...[...WsRpcGroup.requests.keys()].filter(
        (
          tag,
        ): tag is Exclude<
          keyof typeof RpcAuthorization.RPC_REQUIRED_SCOPES,
          typeof WS_METHODS.workTasksCreateInitialGroup
        > => tag !== WS_METHODS.workTasksCreateInitialGroup,
      ),
    );
    const client = yield* RpcTest.makeClient(group).pipe(
      Effect.provide(
        Layer.mergeAll(
          group.toLayerHandler(WS_METHODS.workTasksCreateInitialGroup, handlers.createInitialGroup),
          RpcAuthorization.layer(session.scopes),
        ),
      ),
    );
    const input = yield* decodeGroupInput({
      taskId: "task",
      groupId: "group",
      managerThreadId: "manager",
      commandId: "join",
      subject: "forged",
    });
    const created = yield* client[WS_METHODS.workTasksCreateInitialGroup](input);
    assert.deepStrictEqual(yield* client[WS_METHODS.workTasksCreateInitialGroup](input), created);
    assert.strictEqual(created.control.currentManager?.threadId, "manager");
    assert.strictEqual(created.membership.currentMembership?.task.taskId, "task");
    const sql = yield* SqlClient.SqlClient;
    assert.strictEqual(
      (yield* sql<{
        sequence: number;
      }>`SELECT MAX(sequence) sequence FROM orchestration_events`)[0]!.sequence,
      created.sequence,
    );
    const tokenHandlers = yield* makeTaskSessionService({
      ...session,
      method: "bearer-access-token",
    });
    const denied = yield* tokenHandlers
      .createInitialGroup({
        ...input,
        commandId: "token-command" as typeof input.commandId,
      })
      .pipe(Effect.flip);
    assert.strictEqual(denied.code, "unsupported");
  }).pipe(Effect.provide(sessionLayer)),
);
it.effect("reserves the native message once and recovers its exact run and terminal evidence", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* seed();
    yield* persistInitialGroup(input);
    const intent = {
      environmentId: "env",
      taskId: "task",
      groupId: "group",
      managerThreadId: "manager",
      commandId: "native-message",
      messageId: "native-message-id",
    };
    const browser = yield* makeTaskSessionService({
      sessionId: AuthSessionId.make("session"),
      subject: "owner",
      method: "browser-session-cookie",
      scopes: [AuthOrchestrationOperateScope, AuthOrchestrationReadScope],
    });
    const token = yield* makeTaskSessionService({
      sessionId: AuthSessionId.make("session"),
      subject: "owner",
      method: "bearer-access-token",
      scopes: [AuthOrchestrationOperateScope, AuthOrchestrationReadScope],
    });
    const sql = yield* SqlClient.SqlClient;
    const denied = yield* token.reserveLaunch(intent).pipe(Effect.flip);
    assert.strictEqual(denied.code, "unsupported");
    assert.strictEqual((yield* sql`SELECT * FROM work_task_native_launch_intents`).length, 0);
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error)
      VALUES ('used-native','thread','manager','message.dispatch',${time},0,'accepted',NULL)`;
    const used = yield* reserveNativeLaunch({
      ...intent,
      commandId: "used-native",
      messageId: "used-native-message",
    }).pipe(Effect.flip);
    assert.strictEqual(used._tag, "WorkTaskError");
    if (used._tag === "WorkTaskError") assert.strictEqual(used.code, "revision_conflict");
    const missing = yield* reconcileNativeLaunch(intent.commandId).pipe(Effect.flip);
    assert.strictEqual(missing._tag, "WorkTaskError");
    if (missing._tag === "WorkTaskError") assert.strictEqual(missing.code, "not_found");
    const unconfigured = yield* reserveNativeLaunch(intent).pipe(Effect.flip);
    assert.strictEqual(unconfigured._tag, "WorkTaskError");
    if (unconfigured._tag === "WorkTaskError")
      assert.strictEqual(unconfigured.code, "invalid_state");
    yield* sql`INSERT INTO work_task_allowances VALUES('task',1,0)`;
    yield* sql`UPDATE work_task_current_state SET desired_mode='closed',execution_mode='idle'
      WHERE task_id='task'`;
    const stopped = yield* reserveNativeLaunch(intent).pipe(Effect.flip);
    assert.strictEqual(stopped._tag, "WorkTaskError");
    if (stopped._tag === "WorkTaskError") assert.strictEqual(stopped.code, "invalid_state");
    yield* sql`UPDATE work_task_current_state SET desired_mode='active',execution_mode='idle'
      WHERE task_id='task'`;
    yield* sql`UPDATE work_task_allowances SET consumed=1 WHERE task_id='task'`;
    const exhausted = yield* reserveNativeLaunch(intent).pipe(Effect.flip);
    assert.strictEqual(exhausted._tag, "WorkTaskError");
    if (exhausted._tag === "WorkTaskError") assert.strictEqual(exhausted.code, "invalid_state");
    yield* sql`UPDATE work_task_allowances SET consumed=0 WHERE task_id='task'`;
    yield* sql`CREATE TRIGGER reject_native_claim BEFORE INSERT ON work_task_native_launch_claims
      BEGIN SELECT RAISE(ABORT,'claim write failed'); END`;
    assert.isTrue(yield* reserveNativeLaunch(intent).pipe(Effect.isFailure));
    assert.strictEqual((yield* sql`SELECT * FROM work_task_native_launch_intents`).length, 0);
    yield* sql`DROP TRIGGER reject_native_claim`;
    const reserved = yield* browser.reserveLaunch(intent);
    assert.strictEqual(reserved.manager_generation, 1);
    const nativeTaskLaunch = yield* readNativeLaunchReservation(intent.commandId);
    assert.strictEqual(nativeTaskLaunch?.allowanceUnit, 1);
    assert.deepStrictEqual(yield* reserveNativeLaunch(intent), reserved);
    const claims = yield* sql<{
      allowance_unit: number;
    }>`SELECT allowance_unit FROM work_task_native_launch_claims`;
    assert.deepStrictEqual(
      claims.map((row) => row.allowance_unit),
      [1],
    );
    const competing = yield* reserveNativeLaunch({
      ...intent,
      commandId: "second-message",
      messageId: "second-message-id",
    }).pipe(Effect.flip);
    assert.strictEqual(competing._tag, "WorkTaskError");
    if (competing._tag === "WorkTaskError") assert.strictEqual(competing.code, "invalid_state");
    assert.strictEqual((yield* sql`SELECT * FROM work_task_native_launch_intents`).length, 1);
    assert.strictEqual((yield* reconcileNativeLaunch(intent.commandId)).state, "reserved");
    const reused = yield* reserveNativeLaunch({ ...intent, messageId: "different" }).pipe(
      Effect.flip,
    );
    assert.strictEqual(reused._tag, "WorkTaskError");
    if (reused._tag === "WorkTaskError") assert.strictEqual(reused.code, "revision_conflict");
    yield* sql`INSERT INTO orchestration_v2_command_receipts VALUES(${intent.commandId},'manager','message.dispatch',${time},1,'accepted',NULL)`;
    assert.strictEqual((yield* reconcileNativeLaunch(intent.commandId)).state, "uncertain");
    yield* sql`UPDATE work_task_current_state SET desired_mode='paused' WHERE task_id='task'`;
    assert.isTrue(
      yield* sql`INSERT INTO orchestration_v2_events
      (sequence,event_id,command_id,thread_id,run_id,event_type,occurred_at,payload_json)
      VALUES (1,'paused-run',${intent.commandId},'manager','native-run','run.created',${time},
        ${JSON.stringify({ type: "run.created", payload: { id: "native-run", userMessageId: intent.messageId, nativeTaskLaunch } })})`.pipe(
        Effect.isFailure,
      ),
    );
    assert.strictEqual((yield* sql`SELECT * FROM orchestration_v2_events`).length, 0);
    yield* sql`UPDATE work_task_current_state SET desired_mode='active' WHERE task_id='task'`;
    assert.isTrue(
      yield* sql`INSERT INTO orchestration_v2_events
      (sequence,event_id,command_id,thread_id,run_id,event_type,occurred_at,payload_json)
      VALUES (1,'wrong-run-message',${intent.commandId},'manager','native-run','run.created',${time},
        ${JSON.stringify({ type: "run.created", payload: { id: "native-run", userMessageId: "other", nativeTaskLaunch } })})`.pipe(
        Effect.isFailure,
      ),
    );
    yield* sql`INSERT INTO orchestration_v2_events
      (sequence,event_id,command_id,thread_id,run_id,event_type,occurred_at,payload_json)
      VALUES (1,'run-created',${intent.commandId},'manager','native-run','run.created',${time},
        ${JSON.stringify({ type: "run.created", payload: { id: "native-run", userMessageId: intent.messageId, nativeTaskLaunch } })})`;
    const bound = yield* reconcileNativeLaunch(intent.commandId);
    assert.strictEqual(bound.state, "bound");
    assert.deepStrictEqual(yield* reconcileNativeLaunch(intent.commandId), bound);
    yield* sql`INSERT INTO orchestration_v2_events
      (sequence,event_id,command_id,thread_id,run_id,event_type,occurred_at,payload_json)
      VALUES (2,'wrong-message','completion','manager','native-run','run.updated',${time},
        ${JSON.stringify({ type: "run.updated", payload: { id: "native-run", userMessageId: "other", status: "completed" } })})`;
    assert.deepStrictEqual(yield* reconcileNativeLaunch(intent.commandId), bound);
    yield* sql`INSERT INTO orchestration_v2_events
      (sequence,event_id,command_id,thread_id,run_id,event_type,occurred_at,payload_json)
      VALUES (3,'run-terminal','completion','manager','native-run','run.updated',${time},
        ${JSON.stringify({ type: "run.updated", payload: { id: "native-run", userMessageId: intent.messageId, status: "completed" } })})`;
    const terminal = yield* reconcileNativeLaunch(intent.commandId);
    assert.strictEqual(terminal.state, "terminal");
    assert.strictEqual(terminal.sequence, 3);
    assert.deepStrictEqual(yield* reconcileNativeLaunch(intent.commandId), terminal);
    assert.strictEqual((yield* sql`SELECT * FROM work_task_native_launch_bindings`).length, 1);
    assert.strictEqual(
      (yield* sql`SELECT * FROM work_task_native_terminal_observations`).length,
      1,
    );
    yield* sql`INSERT INTO work_task_native_launch_intents
      (command_id,environment_id,task_id,group_id,manager_thread_id,manager_generation,message_id,created_at)
      VALUES ('legacy-command','env','task','group','manager',1,'legacy-message',${time})`;
    assert.strictEqual((yield* reconcileNativeLaunch("legacy-command")).state, "reserved");
    yield* sql`INSERT INTO orchestration_command_receipts
      (command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status,error)
      VALUES ('legacy-command','thread','manager','message.dispatch',${time},3,'accepted',NULL)`;
    assert.strictEqual((yield* reconcileNativeLaunch("legacy-command")).state, "uncertain");
    const legacyReplay = yield* reserveNativeLaunch({
      ...intent,
      commandId: "legacy-command",
      messageId: "legacy-message",
    }).pipe(Effect.flip);
    assert.strictEqual(legacyReplay._tag, "WorkTaskError");
    if (legacyReplay._tag === "WorkTaskError")
      assert.strictEqual(legacyReplay.code, "invalid_state");
  }).pipe(Effect.provide(sessionLayer)),
);
it.effect("binds a reserved launch in the native event and receipt transaction", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* seed();
    yield* persistInitialGroup(input);
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO work_task_allowances VALUES('task',1,0)`;
    yield* sql`UPDATE work_task_current_state SET desired_mode='active',execution_mode='idle'
      WHERE task_id='task'`;
    const intent = {
      environmentId: "env",
      taskId: "task",
      groupId: "group",
      managerThreadId: "manager",
      commandId: "native-integration",
      messageId: "native-integration-message",
    };
    yield* reserveNativeLaunch(intent);
    const now = yield* DateTime.now;
    const event = yield* decodeNativeEvent({
      id: "native-integration-run-event",
      type: "run.created",
      threadId: "manager",
      runId: "native-integration-run",
      providerInstanceId: "codex",
      occurredAt: now,
      payload: {
        id: "native-integration-run",
        threadId: "manager",
        ordinal: 1,
        providerInstanceId: "codex",
        modelSelection: { instanceId: "codex", model: "gpt-6-luna" },
        providerThreadId: null,
        userMessageId: intent.messageId,
        rootNodeId: null,
        activeAttemptId: null,
        status: "starting",
        queuePosition: null,
        requestedAt: now,
        startedAt: null,
        completedAt: null,
        checkpointId: null,
        contextHandoffId: null,
      },
    });
    const sink = yield* EventSink.EventSinkV2;
    const command = {
      commandId: CommandId.make(intent.commandId),
      threadId: ThreadId.make(intent.managerThreadId),
      commandType: "message.dispatch",
      acceptedAt: now,
      events: [event],
      effects: [],
    };
    yield* sql`UPDATE work_task_current_state SET desired_mode='paused' WHERE task_id='task'`;
    assert.isTrue(yield* sink.commitCommand(command).pipe(Effect.isFailure));
    assert.strictEqual(
      (yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id=${intent.commandId}`)
        .length,
      0,
    );
    yield* sql`UPDATE work_task_current_state SET desired_mode='active' WHERE task_id='task'`;
    if (event.type !== "run.created") return yield* Effect.die("Expected native run event");
    assert.isTrue(
      yield* sql`INSERT INTO orchestration_events
        (event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,command_id,actor_kind,payload_json,metadata_json,application_event_version)
        VALUES ('unbound-live','thread','manager',0,'run.created',${time},${intent.commandId},'server',${JSON.stringify(event.payload)},'{}',2)`.pipe(
        Effect.isFailure,
      ),
    );
    assert.isTrue(
      yield* sink
        .commitCommand({
          ...command,
          effects: [
            {
              id: "native-integration-effect",
              commandId: command.commandId,
              threadId: command.threadId,
              request: { type: "provider-turn.start", runId: RunId.make("native-integration-run") },
            },
          ],
        })
        .pipe(Effect.isFailure),
    );
    assert.strictEqual(
      (yield* sql`SELECT * FROM orchestration_command_receipts WHERE command_id=${intent.commandId}`)
        .length,
      0,
    );
    assert.strictEqual(
      (yield* sql`SELECT * FROM orchestration_events WHERE command_id=${intent.commandId}`).length,
      0,
    );
    const committed = yield* sink.commitCommand(command);
    assert.isTrue(committed.committed);
    const run = committed.storedEvents[0]?.event;
    assert.strictEqual(run?.type, "run.created");
    if (run?.type === "run.created") {
      assert.strictEqual(run.payload.nativeTaskLaunch?.commandId, intent.commandId);
      assert.strictEqual(run.payload.nativeTaskLaunch?.allowanceUnit, 1);
    }
    yield* sql`UPDATE orchestration_events
      SET payload_json=json_remove(payload_json,'$.nativeTaskLaunch')
      WHERE event_id='native-integration-run-event'`;
    const missingClaim = yield* reconcileNativeLaunch(intent.commandId).pipe(Effect.flip);
    assert.strictEqual(missingClaim._tag, "WorkTaskError");
    if (missingClaim._tag === "WorkTaskError")
      assert.strictEqual(missingClaim.code, "invalid_state");
    assert.strictEqual((yield* sql`SELECT * FROM work_task_native_live_bindings`).length, 0);
    yield* sql`UPDATE orchestration_events
      SET payload_json=json_set(payload_json,'$.nativeTaskLaunch',json(${JSON.stringify(run?.type === "run.created" ? { ...run.payload.nativeTaskLaunch, allowanceUnit: 2 } : null)}))
      WHERE event_id='native-integration-run-event'`;
    const changedClaim = yield* reconcileNativeLaunch(intent.commandId).pipe(Effect.flip);
    assert.strictEqual(changedClaim._tag, "WorkTaskError");
    if (changedClaim._tag === "WorkTaskError")
      assert.strictEqual(changedClaim.code, "invalid_state");
    assert.strictEqual((yield* sql`SELECT * FROM work_task_native_live_bindings`).length, 0);
    yield* sql`UPDATE orchestration_events
      SET payload_json=json_set(payload_json,'$.nativeTaskLaunch',json(${JSON.stringify(run?.type === "run.created" ? run.payload.nativeTaskLaunch : null)}))
      WHERE event_id='native-integration-run-event'`;
    assert.strictEqual((yield* reconcileNativeLaunch(intent.commandId)).state, "bound");
    assert.isFalse((yield* sink.commitCommand(command)).committed);
    assert.strictEqual(
      (yield* sql`SELECT * FROM orchestration_events WHERE command_id=${intent.commandId}`).length,
      1,
    );
    if (run?.type !== "run.created") return yield* Effect.die("Expected native run");
    const terminalEvent = yield* decodeNativeEvent({
      ...run,
      id: "native-integration-terminal-event",
      type: "run.updated",
      payload: { ...run.payload, status: "completed", completedAt: now },
    });
    yield* sink.commitCommand({
      commandId: CommandId.make("native-integration-complete"),
      threadId: command.threadId,
      commandType: "run.complete",
      acceptedAt: now,
      events: [terminalEvent],
      effects: [],
    });
    const terminal = yield* reconcileNativeLaunch(intent.commandId);
    assert.strictEqual(terminal.state, "terminal");
    assert.deepStrictEqual(yield* reconcileNativeLaunch(intent.commandId), terminal);
    assert.strictEqual((yield* sql`SELECT * FROM work_task_native_live_terminals`).length, 1);
  }).pipe(Effect.provide(nativeIntegrationLayer)),
);
const input = {
  taskId: "task",
  environmentId: "env",
  groupId: "group",
  managerThreadId: "manager",
  subject: "owner",
  sessionId: "session",
  commandId: "join",
};
it.effect("commits one group, manager and membership root with its companion atomically", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* seed();
    const sql = yield* SqlClient.SqlClient;
    const result = yield* persistInitialGroup(input);
    assert.deepStrictEqual(
      result.events.map((event) => event.type),
      [
        "task-group.created",
        "task-group.manager-changed",
        "task.membership-changed",
        "task-group.membership-changed",
      ],
    );
    const source = yield* sql<{
      sequence: number;
      event_id: string;
      event_type: string;
      aggregate_kind: string;
      occurred_at: string;
      command_id: string;
      payload_json: string;
    }>`SELECT sequence,event_id,event_type,aggregate_kind,occurred_at,command_id,payload_json FROM orchestration_events WHERE sequence>2 ORDER BY sequence`;
    assert.deepStrictEqual(
      source.map((row) => row.event_type),
      result.events.map((event) => event.type),
    );
    assert.deepStrictEqual(
      source.map((row) => row.aggregate_kind),
      ["task-group", "task-group", "task-membership", "task-group"],
    );
    const recorded = source.map((row) => ({
      event: JSON.parse(row.payload_json),
      context: {
        sequence: row.sequence,
        eventId: row.event_id,
        commandId: row.command_id,
        occurredAt: row.occurred_at,
      },
    }));
    const first = yield* decodeControl(recorded[0]);
    const second = yield* decodeControl(recorded[1]);
    const root = yield* decodeRoot(recorded[2]);
    const companion = yield* decodeControl(recorded[3]);
    assert.strictEqual(companion.event.type, "task-group.membership-changed");
    if (companion.event.type !== "task-group.membership-changed") return;
    const initial = emptyTaskGroupControl(first.event.environmentId);
    const withManager = reduceTaskGroupControl(reduceTaskGroupControl(initial, first), second);
    const replayed = reduceTaskMembership(withManager, {
      root,
      companions: [{ event: companion.event, context: companion.context }],
    });
    assert.strictEqual(HashMap.size(replayed.groups), 1);
    assert.strictEqual(HashMap.size(replayed.tasks), 1);
    assert.strictEqual((yield* sql`SELECT * FROM work_task_group_memberships`).length, 1);
    assert.strictEqual((yield* sql`SELECT * FROM work_task_membership_companions`).length, 1);
    assert.strictEqual((yield* sql`SELECT * FROM work_task_group_managers`).length, 1);
    const store = yield* TaskStore.TaskStore;
    assert.isTrue((yield* store.readFoundation("task"))?.sourceVerified);
    assert.deepStrictEqual(yield* persistInitialGroup(input), result);
    assert.strictEqual((yield* sql`SELECT * FROM orchestration_events`).length, 6);
  }).pipe(Effect.provide(layer)),
);
it.effect("rolls back native source if a companion projection fails", () =>
  Effect.gen(function* () {
    yield* runMigrations();
    yield* seed();
    const sql = yield* SqlClient.SqlClient;
    yield* sql`CREATE TRIGGER fail_companion BEFORE INSERT ON work_task_membership_companions BEGIN SELECT RAISE(ABORT,'blocked'); END`;
    assert.isTrue(yield* persistInitialGroup(input).pipe(Effect.isFailure));
    assert.strictEqual((yield* sql`SELECT * FROM orchestration_events`).length, 2);
    assert.strictEqual((yield* sql`SELECT * FROM work_task_manager_groups`).length, 0);
    assert.strictEqual((yield* sql`SELECT * FROM work_task_group_memberships`).length, 0);
  }).pipe(Effect.provide(layer)),
);
