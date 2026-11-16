import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  WorkTaskMembershipSnapshot,
  WorkTaskMembershipEventBatch,
  WorkTaskPersistedGroupControlEvent,
  WorkTaskPersistedMembershipEvent,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../persistence/Migrations.ts";
import {
  emptyTaskGroupControl,
  reduceTaskGroupControl,
  reduceTaskMembership,
  GroupReplayConflict,
} from "./TaskGroup.ts";

const decodeEnvironment = Schema.decodeUnknownSync(EnvironmentId);
const decodeControl = Schema.decodeUnknownSync(WorkTaskPersistedGroupControlEvent);
const decodeRoot = Schema.decodeUnknownSync(WorkTaskPersistedMembershipEvent);
const decodeBatch = Schema.decodeUnknownSync(WorkTaskMembershipEventBatch);
const decodeSnapshot = Schema.decodeUnknownSync(WorkTaskMembershipSnapshot);
const env = decodeEnvironment("env");
const time = "2026-11-16T00:00:00.0001Z";
const fields = (id: string) => ({
  id,
  environmentId: env,
  commandId: `command-${id}`,
  occurredAt: time,
  performedBy: { subject: "owner", sessionId: "session" },
});
const context = (event: ReturnType<typeof fields>, sequence: number) => ({
  eventId: event.id,
  commandId: event.commandId,
  occurredAt: event.occurredAt,
  sequence,
});
const created = (groupId: string, sequence: number) => {
  const event = {
    ...fields(`created-${groupId}`),
    type: "task-group.created",
    control: {
      group: { environmentId: env, groupId },
      controlRevision: 0,
      membershipRevision: 0,
      highestManagerGeneration: 0,
      currentManager: null,
      dispatchHold: "none",
    },
  };
  return decodeControl({ event, context: context(event, sequence) });
};
type Control = WorkTaskPersistedGroupControlEvent["event"]["control"];
const controlChange = (
  previous: Control,
  id: string,
  sequence: number,
  patch: object,
  type = "task-group.manager-changed",
) => {
  const event = {
    ...fields(id),
    type,
    previousControl: previous,
    control: { ...previous, controlRevision: previous.controlRevision + 1, ...patch },
  };
  return decodeControl({ event, context: context(event, sequence) });
};
const initialTask = (taskId = "task") =>
  decodeSnapshot({
    task: { environmentId: env, taskId },
    highestMembershipGeneration: 0,
    currentMembership: null,
    taskRevisions: {
      definitionRevision: 1,
      acceptedDefinitionRevision: 1,
      lifecycleRevision: 1,
      actionPolicyRevision: 1,
      completionPolicyRevision: 1,
    },
  });
const membership = (
  previousSnapshot: WorkTaskMembershipSnapshot,
  controls: readonly Control[],
  destination: string | null,
  id: string,
  rootSequence: number,
  companionSequences: readonly number[],
) => {
  const base = fields(id);
  const provenance = {
    eventId: id,
    commandId: base.commandId,
    occurredAt: time,
    performedBy: base.performedBy,
  };
  const current = previousSnapshot.currentMembership;
  const groups = [current?.group.groupId, destination].filter(
    (g): g is string => g !== undefined && g !== null,
  );
  const transitions = groups.map((groupId) => {
    const previousControl = controls.find((c) => c.group.groupId === groupId)!;
    return {
      companionEventId: `${id}-${groupId}`,
      previousControl,
      control: { ...previousControl, membershipRevision: previousControl.membershipRevision + 1 },
    };
  });
  const target =
    destination === null ? null : transitions.find((t) => t.control.group.groupId === destination)!;
  const joined =
    target === null
      ? null
      : {
          task: previousSnapshot.task,
          group: target.control.group,
          membershipGeneration: previousSnapshot.highestMembershipGeneration + 1,
          acceptedDefinitionRevisionAtJoin:
            previousSnapshot.taskRevisions.acceptedDefinitionRevision,
          joinedMembershipRevision: target.control.membershipRevision,
          joined: provenance,
          state: "current",
        };
  const batch = decodeBatch({
    event: {
      ...base,
      type: "task.membership-changed",
      previousSnapshot,
      snapshot: {
        ...previousSnapshot,
        currentMembership: joined,
        highestMembershipGeneration:
          previousSnapshot.highestMembershipGeneration + (joined === null ? 0 : 1),
      },
      retiredMembership:
        current === null
          ? null
          : {
              ...current,
              state: "retired",
              retiredMembershipRevision: transitions.find(
                (t) => t.control.group.groupId === current.group.groupId,
              )!.control.membershipRevision,
              retired: provenance,
            },
      groupTransitions: transitions,
    },
    companions: transitions.map((t) => ({
      ...base,
      id: t.companionEventId,
      type: "task-group.membership-changed",
      rootEventId: id,
      task: previousSnapshot.task,
      previousControl: t.previousControl,
      control: t.control,
    })),
  });
  return {
    root: decodeRoot({ event: batch.event, context: context(batch.event, rootSequence) }),
    companions: batch.companions.map((event, n) => {
      const record = decodeControl({ event, context: context(event, companionSequences[n]!) });
      if (record.event.type !== "task-group.membership-changed") throw new Error("companion");
      return { event: record.event, context: record.context };
    }),
  };
};
const get = <K, V>(map: HashMap.HashMap<K, V>, key: K) => Option.getOrThrow(HashMap.get(map, key));
const freeze = <T>(value: T): T => {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};

it("retains independent control and manager history without granting work authority", () => {
  const one = created("g1", 1),
    two = created("g2", 2);
  const empty = emptyTaskGroupControl(env);
  let state = reduceTaskGroupControl(reduceTaskGroupControl(empty, one), two);
  const bound = freeze(
    controlChange(one.event.control, "bound", 3, {
      highestManagerGeneration: 1,
      currentManager: { threadId: "manager", managerGeneration: 1 },
    }),
  );
  state = reduceTaskGroupControl(state, bound);
  const paused = controlChange(
    bound.event.control,
    "paused",
    4,
    { dispatchHold: "paused" },
    "task-group.hold-changed",
  );
  state = reduceTaskGroupControl(state, paused);
  assert.strictEqual(get(state.groups, "g2").control.dispatchHold, "none");
  assert.strictEqual(HashMap.size(state.tasks), 0);
  const conflict = controlChange(two.event.control, "duplicate-manager", 5, {
    highestManagerGeneration: 1,
    currentManager: { threadId: "manager", managerGeneration: 1 },
  });
  assert.throws(() => reduceTaskGroupControl(state, conflict), GroupReplayConflict);
  const replaced = controlChange(paused.event.control, "replace", 5, {
    highestManagerGeneration: 2,
    currentManager: { threadId: "other", managerGeneration: 2 },
  });
  state = reduceTaskGroupControl(state, replaced);
  assert.strictEqual(get(state.managers, JSON.stringify(["g1", 1])).retired?.event.id, "replace");
  const retired = controlChange(replaced.event.control, "retire", 6, { currentManager: null });
  state = reduceTaskGroupControl(state, retired);
  assert.strictEqual(get(state.groups, "g1").control.highestManagerGeneration, 2);
  const rebound = controlChange(retired.event.control, "rebind", 7, {
    highestManagerGeneration: 3,
    currentManager: { threadId: "manager", managerGeneration: 3 },
  });
  state = reduceTaskGroupControl(state, rebound);
  for (const record of [one, two, bound, paused, replaced, retired, rebound])
    assert.strictEqual(reduceTaskGroupControl(state, record), state);
  assert.strictEqual(HashMap.size(empty.groups), 0);
  assert.throws(
    () =>
      reduceTaskGroupControl(
        state,
        controlChange(
          paused.event.control,
          "stale",
          8,
          { dispatchHold: "none" },
          "task-group.hold-changed",
        ),
      ),
    GroupReplayConflict,
  );
  assert.throws(
    () =>
      reduceTaskGroupControl(
        state,
        controlChange(
          rebound.event.control,
          "old-position",
          6,
          { dispatchHold: "none" },
          "task-group.hold-changed",
        ),
      ),
    GroupReplayConflict,
  );
  assert.throws(() => reduceTaskGroupControl(state, created("g1", 20)), GroupReplayConflict);
  assert.throws(
    () => reduceTaskGroupControl(state, { ...two, context: { ...two.context, sequence: 50 } }),
    GroupReplayConflict,
  );
  assert.throws(
    () => reduceTaskGroupControl(emptyTaskGroupControl(decodeEnvironment("other")), one),
    GroupReplayConflict,
  );
});

it("applies complete membership changes atomically with stable history and independent task revisions", () => {
  const g1 = created("g1", 1),
    g2 = created("g2", 2);
  const initial = reduceTaskGroupControl(
    reduceTaskGroupControl(emptyTaskGroupControl(env), g1),
    g2,
  );
  const join = freeze(membership(initialTask(), [g1.event.control], "g1", "join", 10, [12]));
  const joined = reduceTaskMembership(initial, join);
  const newer = decodeSnapshot({
    ...join.root.event.snapshot,
    taskRevisions: {
      ...join.root.event.snapshot.taskRevisions,
      definitionRevision: 3,
      acceptedDefinitionRevision: 2,
      actionPolicyRevision: 2,
    },
  });
  const move = membership(
    newer,
    [get(joined.groups, "g1").control, g2.event.control],
    "g2",
    "move",
    15,
    [14, 16],
  );
  const unrelatedHold = controlChange(
    g2.event.control,
    "intervening-hold",
    13,
    { dispatchHold: "paused" },
    "task-group.hold-changed",
  );
  const changedDestination = reduceTaskGroupControl(joined, unrelatedHold);
  assert.throws(() => reduceTaskMembership(changedDestination, move), GroupReplayConflict);
  assert.strictEqual(get(changedDestination.groups, "g1").sequence, 12);
  assert.strictEqual(get(changedDestination.tasks, "task").highestMembershipGeneration, 1);
  const moved = reduceTaskMembership(joined, move);
  assert.strictEqual(get(moved.tasks, "task").highestMembershipGeneration, 2);
  assert.strictEqual(get(moved.tasks, "task").currentMembership?.group.groupId, "g2");
  assert.strictEqual(
    get(moved.memberships, JSON.stringify(["task", 1])).retired?.retired.eventId,
    "move",
  );
  assert.strictEqual(
    get(moved.memberships, JSON.stringify(["task", 1])).joined.acceptedDefinitionRevisionAtJoin,
    1,
  );
  assert.strictEqual(
    reduceTaskMembership(moved, { ...move, companions: move.companions.toReversed() }),
    moved,
  );
  const remove = membership(
    move.root.event.snapshot,
    [get(moved.groups, "g2").control],
    null,
    "remove",
    18,
    [17],
  );
  const removed = reduceTaskMembership(moved, remove);
  assert.isNull(get(removed.tasks, "task").currentMembership);
  assert.strictEqual(get(removed.tasks, "task").highestMembershipGeneration, 2);
  const rejoin = membership(
    remove.root.event.snapshot,
    [get(removed.groups, "g1").control],
    "g1",
    "rejoin",
    19,
    [20],
  );
  const final = reduceTaskMembership(removed, rejoin);
  assert.strictEqual(get(final.tasks, "task").highestMembershipGeneration, 3);
  for (const batch of [join, move, remove, rejoin])
    assert.strictEqual(reduceTaskMembership(final, batch), final);
  assert.throws(
    () =>
      reduceTaskMembership(final, {
        ...join,
        root: { ...join.root, context: { ...join.root.context, sequence: 99 } },
      }),
    GroupReplayConflict,
  );
  assert.strictEqual(HashMap.size(initial.tasks), 0);
  assert.strictEqual(get(joined.tasks, "task").currentMembership?.group.groupId, "g1");
  assert.throws(
    () => reduceTaskMembership(joined, { ...move, companions: move.companions.slice(0, 1) }),
    GroupReplayConflict,
  );
  const wrong = {
    ...move,
    companions: move.companions.map((c, n) =>
      n ? { ...c, context: { ...c.context, sequence: 12 } } : c,
    ),
  };
  assert.throws(() => reduceTaskMembership(joined, wrong), GroupReplayConflict);
  assert.strictEqual(get(joined.groups, "g1").control.membershipRevision, 1);
  assert.throws(() => reduceTaskGroupControl(joined, join.companions[0]!), GroupReplayConflict);
  const regressed = decodeSnapshot({
    ...move.root.event.snapshot,
    taskRevisions: { ...move.root.event.snapshot.taskRevisions, actionPolicyRevision: 1 },
  });
  assert.throws(
    () =>
      reduceTaskMembership(
        moved,
        membership(regressed, [get(moved.groups, "g2").control], null, "regress", 22, [23]),
      ),
    GroupReplayConflict,
  );
  const wrongPrevious = membership(
    initialTask(),
    [get(joined.groups, "g1").control],
    "g1",
    "pretend-first",
    30,
    [31],
  );
  assert.throws(() => reduceTaskMembership(joined, wrongPrevious), GroupReplayConflict);
});

it("allows unrelated interleaving and separates known references from owned replay", () => {
  const g1 = created("g1", 1),
    g2 = created("g2", 2);
  const initial = reduceTaskGroupControl(
    reduceTaskGroupControl(emptyTaskGroupControl(env), g1),
    g2,
  );
  const join = membership(
    initialTask("task\u0000\ud800"),
    [g1.event.control],
    "g1",
    "join\u0000\ud800",
    10,
    [12],
  );
  const hold = controlChange(
    g2.event.control,
    "hold",
    11,
    { dispatchHold: "paused" },
    "task-group.hold-changed",
  );
  const left = reduceTaskMembership(reduceTaskGroupControl(initial, hold), join);
  const right = reduceTaskGroupControl(reduceTaskMembership(initial, join), hold);
  for (const id of ["g1", "g2"])
    assert.deepStrictEqual(get(left.groups, id), get(right.groups, id));
  assert.strictEqual(left.observedMaximum, 12);
  assert.strictEqual(right.observedMaximum, 12);
  const refs = {
    ...initial,
    references: HashMap.set(initial.references, join.root.event.id, {
      sequence: 10,
      kind: "task.membership-changed",
      taskId: join.root.event.snapshot.task.taskId,
    }),
    referenceSequences: HashMap.set(initial.referenceSequences, 10, join.root.event.id),
  };
  assert.strictEqual(HashMap.size(reduceTaskMembership(refs, join).tasks), 1);
  const incompatible = {
    ...refs,
    references: HashMap.set(refs.references, join.root.event.id, { sequence: 9 }),
  };
  assert.throws(() => reduceTaskMembership(incompatible, join), GroupReplayConflict);
  const partial = {
    ...initial,
    events: HashMap.set(initial.events, join.root.event.id, join.root),
  };
  assert.throws(() => reduceTaskMembership(partial, join), GroupReplayConflict);
  assert.strictEqual(HashMap.size(initial.events), 2);
  const old = membership(
    join.root.event.snapshot,
    [get(left.groups, "g1").control],
    null,
    "old-companion",
    14,
    [9],
  );
  assert.throws(() => reduceTaskMembership(left, old), GroupReplayConflict);
});

const db = NodeSqliteClient.layer({ filename: ":memory:" });
const writeControl = Effect.fn("TaskGroupTest.writeControl")(function* (
  record: WorkTaskPersistedGroupControlEvent,
) {
  const sql = yield* SqlClient.SqlClient;
  const event = record.event,
    encoded = JSON.stringify(event);
  if (event.type === "task-group.created") {
    yield* sql`INSERT INTO work_task_manager_groups (environment_id,group_id,control_revision,membership_revision,highest_manager_generation,current_manager_generation,current_manager_thread_id,dispatch_hold,created_event_id,created_sequence,created_event_json,control_event_id,control_sequence,control_event_json)
    SELECT json_extract(value,'$.environmentId'),json_extract(value,'$.control.group.groupId'),0,0,0,NULL,NULL,'none',json_extract(value,'$.id'),${record.context.sequence},value,json_extract(value,'$.id'),${record.context.sequence},value FROM (SELECT ${encoded} value)`;
  } else {
    yield* sql`UPDATE work_task_manager_groups SET control_revision=json_extract(${encoded},'$.control.controlRevision'),membership_revision=json_extract(${encoded},'$.control.membershipRevision'),dispatch_hold=json_extract(${encoded},'$.control.dispatchHold'),control_event_id=json_extract(${encoded},'$.id'),control_sequence=${record.context.sequence},control_event_json=${encoded},latest_membership_event_id=CASE WHEN json_extract(${encoded},'$.type')='task-group.membership-changed' THEN json_extract(${encoded},'$.id') ELSE latest_membership_event_id END WHERE group_id=json_extract(${encoded},'$.control.group.groupId')`;
  }
});
const writeJoin = Effect.fn("TaskGroupTest.writeJoin")(function* (
  batch: ReturnType<typeof membership>,
) {
  const sql = yield* SqlClient.SqlClient;
  const root = batch.root.event,
    joined = root.snapshot.currentMembership!;
  for (const companion of batch.companions) {
    yield* writeControl(companion);
    const encoded = JSON.stringify(companion.event);
    yield* sql`INSERT INTO work_task_membership_companions (event_id,sequence,environment_id,group_id,task_id,membership_generation,role,root_event_id,root_sequence,root_event_json,event_json,joined_generation,retired_generation,peer_event_id)
      SELECT json_extract(value,'$.id'),${companion.context.sequence},json_extract(value,'$.environmentId'),json_extract(value,'$.control.group.groupId'),json_extract(value,'$.task.taskId'),${joined.membershipGeneration},'join',json_extract(value,'$.rootEventId'),${batch.root.context.sequence},${JSON.stringify(root)},value,${joined.membershipGeneration},NULL,NULL FROM (SELECT ${encoded} value)`;
    const canonical = JSON.stringify(joined);
    yield* sql`INSERT INTO work_task_group_memberships (task_id,membership_generation,environment_id,group_id,accepted_definition_revision,joined_membership_revision,joined_event_id,joined_record_json,joined_companion_id)
      SELECT json_extract(value,'$.task.taskId'),json_extract(value,'$.membershipGeneration'),json_extract(value,'$.task.environmentId'),json_extract(value,'$.group.groupId'),json_extract(value,'$.acceptedDefinitionRevisionAtJoin'),json_extract(value,'$.joinedMembershipRevision'),json_extract(value,'$.joined.eventId'),value,json_extract(${encoded},'$.id') FROM (SELECT ${canonical} value)`;
  }
});
it.effect.each([false, true])("matches actual SQL domain order with holdFirst=%s", (holdFirst) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    for (const task of ["task\u0000\ud800", "second"]) {
      const encoded = JSON.stringify(task);
      yield* sql`INSERT INTO work_task_projections VALUES (json_extract(${encoded},'$'),NULL,1,1,1,1,'{}','1','1')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES (json_extract(${encoded},'$'),1,1,'{}','{}',${`definition-${task === "second" ? "2" : "1"}`},'1')`;
    }
    const g1 = created("g1", 1),
      g2 = created("g2", 2);
    let state = emptyTaskGroupControl(env);
    for (const record of [g1, g2]) {
      yield* writeControl(record);
      state = reduceTaskGroupControl(state, record);
    }
    const join = membership(
      initialTask("task\u0000\ud800"),
      [g1.event.control],
      "g1",
      "join\u0000\ud800",
      10,
      [12],
    );
    const hold = controlChange(
      g2.event.control,
      "hold",
      11,
      { dispatchHold: "paused" },
      "task-group.hold-changed",
    );
    if (holdFirst) {
      yield* writeControl(hold);
      state = reduceTaskGroupControl(state, hold);
    }
    yield* sql.withTransaction(writeJoin(join));
    state = reduceTaskMembership(state, join);
    if (!holdFirst) {
      yield* writeControl(hold);
      state = reduceTaskGroupControl(state, hold);
    }
    const second = membership(
      initialTask("second"),
      [get(state.groups, "g1").control],
      "g1",
      "second-join",
      14,
      [13],
    );
    yield* sql.withTransaction(writeJoin(second));
    state = reduceTaskMembership(state, second);
    const rows = yield* sql<{
      control_event_json: string;
      control_sequence: number;
    }>`SELECT control_event_json,control_sequence FROM work_task_manager_groups`;
    for (const row of rows) {
      const event = JSON.parse(row.control_event_json);
      const group = get(state.groups, event.control.group.groupId);
      assert.deepStrictEqual(group.control, event.control);
      assert.strictEqual(group.sequence, row.control_sequence);
    }
    const histories = yield* sql<{
      joined_record_json: string;
      root_event_json: string;
      root_sequence: number;
      sequence: number;
    }>`SELECT m.joined_record_json,c.root_event_json,c.root_sequence,c.sequence FROM work_task_group_memberships m JOIN work_task_membership_companions c ON c.event_id=m.joined_companion_id`;
    assert.lengthOf(histories, 2);
    for (const row of histories) {
      const joined = JSON.parse(row.joined_record_json),
        root = JSON.parse(row.root_event_json);
      assert.deepStrictEqual(get(state.tasks, joined.task.taskId).currentMembership, joined);
      assert.strictEqual(get(state.events, root.id).context.sequence, row.root_sequence);
      assert.strictEqual(get(state.groups, "g1").sequence >= row.sequence, true);
    }
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);
