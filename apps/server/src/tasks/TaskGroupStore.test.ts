import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as HashMap from "effect/HashMap";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import {
  WorkTaskPersistedEvent,
  WorkTaskPersistedGroupControlEvent,
  WorkTaskPersistedMembershipEvent,
} from "@t3tools/contracts";
import { runMigrations } from "../persistence/Migrations.ts";
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
const decode = Schema.decodeUnknownEffect(WorkTaskPersistedEvent);
const decodeControl = Schema.decodeUnknownEffect(WorkTaskPersistedGroupControlEvent);
const decodeRoot = Schema.decodeUnknownEffect(WorkTaskPersistedMembershipEvent);
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
    assert.isTrue(yield* persistInitialGroup(input).pipe(Effect.isFailure));
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
