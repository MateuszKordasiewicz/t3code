import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as HashMap from "effect/HashMap";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as RpcTest from "effect/rpc/RpcTest";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import {
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
} from "@t3tools/contracts";
import * as RpcAuthorization from "../auth/RpcAuthorization.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import * as TaskService from "./TaskService.ts";
import { makeTaskSessionService } from "./TaskSessionService.ts";
import * as TaskStore from "./TaskStore.ts";
import { persistInitialGroup } from "./TaskGroupStore.ts";
import {
  emptyTaskGroupControl,
  reduceTaskGroupControl,
  reduceTaskMembership,
} from "./TaskGroup.ts";
const layer = TaskStore.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const sessionLayer = TaskService.layer.pipe(
  Layer.provideMerge(layer),
  Layer.provideMerge(
    Layer.succeed(
      ServerEnvironment,
      ServerEnvironment.of({
        getEnvironmentId: Effect.succeed(EnvironmentId.make("env")),
        getDescriptor: Effect.die("unused in task group tests"),
      }),
    ),
  ),
);
const decode = Schema.decodeUnknownEffect(WorkTaskPersistedEvent);
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
