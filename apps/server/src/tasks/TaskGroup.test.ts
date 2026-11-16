import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  WorkTaskPersistedEvent,
  WorkTaskPersistedCoordinationEvent,
  WorkTaskPersistedAcceptedRunOwnershipEvent,
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
  reduceTaskRunOwnership,
  reduceTaskWork,
  reconcileTaskWorkReleaseEvidence,
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
    yield* sql`UPDATE work_task_manager_groups SET highest_manager_generation=json_extract(${encoded},'$.control.highestManagerGeneration'),current_manager_generation=json_extract(${encoded},'$.control.currentManager.managerGeneration'),current_manager_thread_id=json_extract(${encoded},'$.control.currentManager.threadId'),control_revision=json_extract(${encoded},'$.control.controlRevision'),membership_revision=json_extract(${encoded},'$.control.membershipRevision'),dispatch_hold=json_extract(${encoded},'$.control.dispatchHold'),control_event_id=json_extract(${encoded},'$.id'),control_sequence=${record.context.sequence},control_event_json=${encoded},latest_membership_event_id=CASE WHEN json_extract(${encoded},'$.type')='task-group.membership-changed' THEN json_extract(${encoded},'$.id') ELSE latest_membership_event_id END WHERE group_id=json_extract(${encoded},'$.control.group.groupId')`;
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

const decodeWork = Schema.decodeUnknownSync(WorkTaskPersistedCoordinationEvent);
const decodeOwner = Schema.decodeUnknownEffect(WorkTaskPersistedAcceptedRunOwnershipEvent);
const ownerRecord = Schema.decodeUnknownSync(WorkTaskPersistedAcceptedRunOwnershipEvent);
const decodeDebit = Schema.decodeUnknownSync(WorkTaskPersistedEvent);
const releaseFixture = (settled = false, interrupt = false) => {
  const binding = {
    version: 2,
    workClaimId: "claim",
    task: { environmentId: env, taskId: "task" },
    group: { environmentId: env, groupId: "g" },
    manager: { managerGeneration: 1, threadId: "manager" },
    membershipGeneration: 1,
    membershipRevision: 1,
    controlRevision: 1,
    graphRevision: 0,
    projectId: "project",
    providerInstanceId: "codex",
    commandId: "activate",
    runId: "run",
    messageId: "message",
    effectId: "effect",
    taskRevisions: {
      definitionRevision: 1,
      acceptedDefinitionRevision: 1,
      lifecycleRevision: 1,
      completionPolicyRevision: 1,
      actionPolicyRevision: 1,
    },
  };
  const run = {
    type: "run.updated",
    eventId: "native-terminal",
    sequence: 32,
    runId: "run",
    threadId: "manager",
    status: settled ? "rolled_back" : "cancelled",
    occurredAt: time,
  };
  const attempt = {
    type: "run-attempt.updated",
    eventId: "attempt-terminal",
    sequence: 31,
    runId: "run",
    attemptId: "attempt",
    providerThreadId: "provider-thread",
    providerTurnId: null,
    status: "superseded",
    occurredAt: time,
  };
  const interruption = {
    type: "turn-item.updated",
    eventId: "interrupt",
    sequence: 33,
    runId: "run",
    threadId: "manager",
    turnItemId: "item",
    itemType: "run_interrupt_result",
    occurredAt: time,
  };
  const event = {
    id: "release",
    type: "task-group.work-released",
    environmentId: env,
    commandId: "settle",
    occurredAt: time,
    attribution: { type: "server" },
    claim: { binding, acceptedEventId: "accepted", acceptedSequence: 30, acceptedAt: time },
    observedSequence: 33,
    releaseEvidence: settled
      ? {
          type: "run_settled",
          terminalRunEvent: { type: run.type, eventId: run.eventId, sequence: run.sequence },
          attempt: {
            attemptId: attempt.attemptId,
            providerThreadId: attempt.providerThreadId,
            providerTurnId: null,
            terminalAttemptEvent: {
              type: attempt.type,
              eventId: attempt.eventId,
              sequence: attempt.sequence,
            },
          },
          ...(interrupt
            ? {
                interruptResult: {
                  type: interruption.type,
                  eventId: interruption.eventId,
                  sequence: interruption.sequence,
                  turnItemId: interruption.turnItemId,
                },
              }
            : {}),
        }
      : {
          type: "not_started",
          terminalRunEvent: { type: run.type, eventId: run.eventId, sequence: run.sequence },
          settlementCommandId: "settle",
        },
  };
  const recorded = decodeWork({
    event,
    context: { eventId: event.id, commandId: event.commandId, occurredAt: time, sequence: 34 },
  });
  if (recorded.event.type !== "task-group.work-released") throw new Error("Expected release");
  return {
    recorded: { ...recorded, event: recorded.event },
    native: {
      run,
      ...(settled ? { attempt } : {}),
      ...(interrupt ? { interrupt: interruption } : {}),
    },
  };
};
it("reconciles retained native release evidence without claiming physical custody", () => {
  for (const input of [releaseFixture(), releaseFixture(true), releaseFixture(true, true)]) {
    const before = structuredClone(input);
    const result = reconcileTaskWorkReleaseEvidence(input.recorded, input.native);
    assert.deepStrictEqual<unknown>(result.native, input.native);
    assert.deepStrictEqual(result.recorded, input.recorded);
    assert.deepStrictEqual(input, before);
    for (const patch of [
      { run: { ...input.native.run, runId: "other" } },
      { run: { ...input.native.run, threadId: "other" } },
      { run: { ...input.native.run, eventId: "other" } },
      { run: { ...input.native.run, sequence: 30 } },
      { run: { ...input.native.run, status: "running" } },
      { run: { ...input.native.run, occurredAt: "not-time" } },
      { run: { ...input.native.run, extra: true } },
      { run: { ...input.native.run, occurredAt: "2026-11-16T00:00:00." + "1".repeat(1980) + "Z" } },
      { run: { ...input.native.run, eventId: "\t" } },
      { run: { ...input.native.run, sequence: "32" } },
      { extra: true },
    ])
      assert.throws(
        () => reconcileTaskWorkReleaseEvidence(input.recorded, { ...input.native, ...patch }),
        GroupReplayConflict,
      );
  }
  for (const occurredAt of [
    "0001-01-01T00:00:00.00000001+23:59",
    "2026-11-16T00:00:00." + "1".repeat(1979) + "Z",
  ]) {
    const input = releaseFixture();
    const native = { run: { ...input.native.run, occurredAt } };
    assert.strictEqual(
      reconcileTaskWorkReleaseEvidence(input.recorded, native).native.run.occurredAt,
      occurredAt,
    );
  }
  const input = releaseFixture(true, true);
  for (const patch of [
    { attempt: undefined },
    { interrupt: undefined },
    { attempt: { ...input.native.attempt, providerTurnId: undefined } },
    { attempt: { ...input.native.attempt, providerThreadId: "other" } },
    { attempt: { ...input.native.attempt, sequence: 29 } },
    { interrupt: { ...input.native.interrupt, sequence: 34 } },
    { interrupt: { ...input.native.interrupt, turnItemId: "other" } },
  ])
    assert.throws(
      () => reconcileTaskWorkReleaseEvidence(input.recorded, { ...input.native, ...patch }),
      GroupReplayConflict,
    );
  assert.throws(
    () => reconcileTaskWorkReleaseEvidence(releaseFixture().recorded, input.native),
    GroupReplayConflict,
  );
});

const acceptedRunPaths = {
  run_id: "ownership.run.runId",
  message_id: "ownership.run.messageId",
  environment_id: "environmentId",
  task_id: "ownership.run.task.taskId",
  group_id: "ownership.run.group.groupId",
  manager_generation: "ownership.run.manager.managerGeneration",
  manager_thread_id: "ownership.run.manager.threadId",
  membership_generation: "ownership.run.membershipGeneration",
  project_id: "ownership.run.projectId",
  provider_instance_id: "ownership.run.providerInstanceId",
  definition_revision: "ownership.run.taskRevisions.definitionRevision",
  accepted_definition_revision: "ownership.run.taskRevisions.acceptedDefinitionRevision",
  lifecycle_revision: "ownership.run.taskRevisions.lifecycleRevision",
  completion_policy_revision: "ownership.run.taskRevisions.completionPolicyRevision",
  action_policy_revision: "ownership.run.taskRevisions.actionPolicyRevision",
  graph_revision: "ownership.run.graphRevision",
  control_revision: "ownership.run.controlRevision",
  membership_revision: "ownership.run.membershipRevision",
  recorded_event_id: "id",
  recorded_sequence: "ownership.recorded.sequence",
  recorded_command_id: "commandId",
  recorded_at: "occurredAt",
  native_run_event_id: "ownership.nativeAcceptance.runEvent.eventId",
  native_run_sequence: "ownership.nativeAcceptance.runEvent.sequence",
  native_message_event_id: "ownership.nativeAcceptance.messageEvent.eventId",
  native_message_sequence: "ownership.nativeAcceptance.messageEvent.sequence",
  native_receipt_command_id: "ownership.nativeAcceptance.receipt.commandId",
  native_receipt_result_sequence: "ownership.nativeAcceptance.receipt.resultSequence",
  origin_type: "ownership.origin.type",
  reservation_id: "ownership.origin.reservationId",
  reservation_command_id: "ownership.origin.reservationCommandId",
  allowance_unit: "ownership.origin.allowanceUnit",
  debit_event_id: "ownership.origin.debitEvent.eventId",
  debit_sequence: "ownership.origin.debitEvent.sequence",
};
const insertAcceptedRun = Effect.fn("TaskGroupTest.insertAcceptedRun")(function* (event: unknown) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    `INSERT INTO work_task_accepted_runs (${Object.keys(acceptedRunPaths).join(",")},event_json)
    SELECT ${Object.values(acceptedRunPaths)
      .map((p) => `json_extract(value,'$.${p}')`)
      .join(",")},value FROM (SELECT ? AS value)`,
    [JSON.stringify(event)],
  );
});

const insertPhysicalWork = Effect.fn("TaskGroupTest.insertPhysicalWork")(function* (
  event: unknown,
) {
  const sql = yield* SqlClient.SqlClient;
  const columns = {
    work_claim_id: "claim.binding.workClaimId",
    run_id: "claim.binding.runId",
    environment_id: "environmentId",
    task_id: "claim.binding.task.taskId",
    group_id: "claim.binding.group.groupId",
    manager_generation: "claim.binding.manager.managerGeneration",
    manager_thread_id: "claim.binding.manager.threadId",
    membership_generation: "claim.binding.membershipGeneration",
    accepted_event_id: "id",
    accepted_sequence: "claim.acceptedSequence",
    effect_id: "claim.binding.effectId",
    accepted_definition_revision: "claim.binding.taskRevisions.acceptedDefinitionRevision",
  };
  yield* sql.unsafe(
    `INSERT INTO work_task_executable_claims (${Object.keys(columns).join(",")},event_json)
    SELECT ${Object.values(columns)
      .map((path) => `json_extract(value,'$.${path}')`)
      .join(",")},value FROM (SELECT ? value)`,
    [JSON.stringify(event)],
  );
});
const seedReleaseClaim = Effect.fn("TaskGroupTest.seedReleaseClaim")(function* (
  input: ReturnType<typeof releaseFixture>,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* runMigrations();
  yield* sql`INSERT INTO work_task_projections VALUES ('task',NULL,1,1,1,1,'{}','raw','raw')`;
  yield* sql`INSERT INTO work_task_accepted_definitions VALUES ('task',1,1,'{}','{}','definition','raw')`;
  const group = created("g", 1);
  yield* writeControl(group);
  const join = membership(initialTask(), [group.event.control], "g", "join", 2, [3]);
  yield* sql.withTransaction(writeJoin(join));
  const manager = controlChange(join.companions[0]!.event.control, "manager-bound", 4, {
    highestManagerGeneration: 1,
    currentManager: { managerGeneration: 1, threadId: "manager" },
  });
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* writeControl(manager);
      const encoded = JSON.stringify(manager.event);
      yield* sql`INSERT INTO work_task_group_managers (environment_id,group_id,generation,manager_thread_id,bound_event_id,bound_sequence,bound_event_json,current_generation,current_manager_thread_id)
      SELECT json_extract(value,'$.environmentId'),json_extract(value,'$.control.group.groupId'),1,'manager',json_extract(value,'$.id'),4,value,1,'manager' FROM (SELECT ${encoded} value)`;
    }),
  );
  const recorded = { eventId: "owner", sequence: 20, commandId: "record-owner", occurredAt: time };
  const origin = {
    type: "initial_start",
    startCommandId: "start",
    startEventId: "start-event",
    startSequence: 10,
  };
  const owner = yield* decodeOwner({
    context: recorded,
    event: {
      id: recorded.eventId,
      type: "task.run-ownership-accepted",
      environmentId: env,
      commandId: recorded.commandId,
      occurredAt: time,
      attribution: { type: "server" },
      ownership: {
        recorded,
        run: {
          version: 2,
          task: input.recorded.event.claim.binding.task,
          group: input.recorded.event.claim.binding.group,
          manager: input.recorded.event.claim.binding.manager,
          membershipGeneration: 1,
          membershipRevision: 1,
          controlRevision: 1,
          graphRevision: 0,
          projectId: "project",
          providerInstanceId: "codex",
          runId: "run",
          messageId: "message",
          taskRevisions: input.recorded.event.claim.binding.taskRevisions,
        },
        nativeAcceptance: {
          receipt: {
            commandId: "native",
            threadId: "manager",
            commandType: "thread.message.send",
            acceptedAt: time,
            resultSequence: 12,
            status: "accepted",
            error: null,
          },
          runEvent: { type: "run.created", eventId: "native-run", sequence: 11 },
          messageEvent: { type: "message.updated", eventId: "native-message", sequence: 12 },
          runStatus: "queued",
        },
        sourceEventId: "start-event",
        sourceSequence: 10,
        observedWorkSequence: 10,
        prerequisites: [],
        origin,
      },
    },
  });
  yield* insertAcceptedRun(owner.event);
  const claim = decodeWork({
    context: { eventId: "accepted", sequence: 30, commandId: "activate", occurredAt: time },
    event: {
      id: "accepted",
      type: "task-group.work-accepted",
      environmentId: env,
      commandId: "activate",
      occurredAt: time,
      attribution: { type: "server" },
      previousActiveClaimId: null,
      claim: {
        ...input.recorded.event.claim,
        sourceEventId: "owner",
        sourceSequence: 20,
        observedWorkSequence: 20,
        prerequisites: [],
        launchOrigin: origin,
      },
      control: manager.event.control,
      membership: join.root.event.snapshot,
    },
  });
  yield* insertPhysicalWork(claim.event);
  if (
    owner.event.type !== "task.run-ownership-accepted" ||
    claim.event.type !== "task-group.work-accepted"
  )
    throw new Error("Expected acceptance");
  return {
    group,
    join,
    manager,
    owner: { ...owner, event: owner.event },
    claim: { ...claim, event: claim.event },
  };
});
const writeReleaseEvidence = Effect.fn("TaskGroupTest.writeReleaseEvidence")(function* (
  recorded: ReturnType<typeof releaseFixture>["recorded"],
  native: unknown,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql.unsafe(
    `INSERT INTO work_task_claim_releases(event_id,sequence,work_claim_id,run_id,effect_id,terminal_event_id,terminal_sequence,event_json,native_json)
    SELECT json_extract(value,'$.id'),?,json_extract(value,'$.claim.binding.workClaimId'),json_extract(value,'$.claim.binding.runId'),json_extract(value,'$.claim.binding.effectId'),json_extract(value,'$.releaseEvidence.terminalRunEvent.eventId'),json_extract(value,'$.releaseEvidence.terminalRunEvent.sequence'),value,? FROM (SELECT ? value)`,
    [recorded.context.sequence, JSON.stringify(native), JSON.stringify(recorded.event)],
  );
  yield* sql`UPDATE work_task_executable_claims SET release_event_id=json_extract(${JSON.stringify(recorded.event.id)},'$') WHERE work_claim_id=json_extract(${JSON.stringify(recorded.event.claim.binding.workClaimId)},'$')`;
});
it.effect.each([false, true])(
  "matches actual retained release evidence with settled=%s",
  (settled) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const input = releaseFixture(settled, settled);
      yield* seedReleaseClaim(input);
      const changes: readonly unknown[] = [
        { ...input.native, run: { ...input.native.run, runId: "other" } },
        { ...input.native, run: { ...input.native.run, threadId: "other" } },
        { ...input.native, run: { ...input.native.run, status: "running" } },
        { ...input.native, run: { ...input.native.run, sequence: 30 } },
        { ...input.native, run: { ...input.native.run, occurredAt: "bad" } },
        { ...input.native, run: { ...input.native.run, eventId: " padded " } },
        { ...input.native, run: { ...input.native.run, extra: null } },
        ...(settled
          ? [
              { ...input.native, attempt: { ...input.native.attempt, providerTurnId: "changed" } },
              { ...input.native, attempt: { ...input.native.attempt, providerTurnId: undefined } },
              { ...input.native, attempt: { ...input.native.attempt, sequence: 29 } },
              { ...input.native, interrupt: { ...input.native.interrupt, turnItemId: "other" } },
            ]
          : [{ ...input.native, attempt: releaseFixture(true).native.attempt }]),
      ];
      for (const native of changes) {
        assert.throws(
          () => reconcileTaskWorkReleaseEvidence(input.recorded, native),
          GroupReplayConflict,
        );
        yield* sql`BEGIN`;
        const failed = yield* writeReleaseEvidence(input.recorded, native).pipe(Effect.flip);
        assert.isDefined(failed);
        yield* sql`ROLLBACK`;
      }
      const result = reconcileTaskWorkReleaseEvidence(input.recorded, input.native);
      yield* sql.withTransaction(writeReleaseEvidence(result.recorded, result.native));
      const [row] = yield* sql<{
        native_json: string;
        event_json: string;
      }>`SELECT native_json,event_json FROM work_task_claim_releases`;
      assert.deepStrictEqual(JSON.parse(row!.native_json), result.native);
      assert.deepStrictEqual(JSON.parse(row!.event_json), result.recorded.event);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      assert.deepStrictEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(db)),
);

const workRecord = (input: unknown) => {
  const value = decodeWork(input);
  if (
    value.event.type !== "task-group.work-accepted" &&
    value.event.type !== "task-group.work-uncertain" &&
    value.event.type !== "task-group.work-released"
  )
    throw new Error("Expected physical fact");
  return { ...value, event: value.event };
};
const ownerRef = (record: ReturnType<typeof ownerRecord>) => {
  if (record.event.type !== "task.run-ownership-accepted") throw new Error("Expected owner");
  const owner = record.event.ownership,
    origin = owner.origin;
  return {
    run: owner.run,
    recorded: owner.recorded,
    nativeRunEvent: owner.nativeAcceptance.runEvent,
    origin:
      origin.type === "initial_start"
        ? origin
        : {
            type: origin.type,
            reservationId: origin.reservationId,
            reservationCommandId: origin.reservationCommandId,
            allowanceUnit: origin.allowanceUnit,
            debitEvent: origin.debitEvent,
          },
  };
};
const ownerObservation = (
  owner: ReturnType<typeof ownerRecord>,
  id: string,
  sequence: number,
  patch: object,
) =>
  ownerRecord({
    event: {
      id,
      environmentId: env,
      commandId: id,
      occurredAt: time,
      attribution: { type: "server" },
      ownership: ownerRef(owner),
      ...patch,
    },
    context: { eventId: id, commandId: id, occurredAt: time, sequence },
  });
const activateAgain = (record: ReturnType<typeof workRecord>, id: string, sequence: number) => {
  if (record.event.type !== "task-group.work-accepted") throw new Error("Expected claim");
  const claim = record.event.claim;
  return workRecord({
    event: {
      ...record.event,
      id,
      commandId: id,
      claim: {
        ...claim,
        acceptedEventId: id,
        acceptedSequence: sequence,
        binding: { ...claim.binding, workClaimId: id, effectId: id, commandId: id },
      },
    },
    context: { eventId: id, commandId: id, occurredAt: time, sequence },
  });
};
const releaseFor = (claim: ReturnType<typeof workRecord>, sequence: number) => {
  if (claim.event.type !== "task-group.work-accepted") throw new Error("Expected claim");
  const input = releaseFixture(),
    id = `release-${claim.event.id}`,
    terminal = {
      type: "run.updated",
      eventId: `terminal-${claim.event.id}`,
      sequence: sequence - 2,
    };
  const recorded = workRecord({
    event: {
      ...input.recorded.event,
      id,
      claim: {
        binding: claim.event.claim.binding,
        acceptedEventId: claim.event.id,
        acceptedSequence: claim.context.sequence,
        acceptedAt: time,
      },
      observedSequence: sequence - 1,
      releaseEvidence: {
        type: "not_started",
        terminalRunEvent: terminal,
        settlementCommandId: "settle",
      },
    },
    context: { ...input.recorded.context, eventId: id, sequence },
  });
  if (recorded.event.type !== "task-group.work-released") throw new Error("Expected release");
  return {
    recorded: { ...recorded, event: recorded.event },
    native: {
      run: {
        ...input.native.run,
        ...terminal,
        runId: claim.event.claim.binding.runId,
        threadId: claim.event.claim.binding.manager.threadId,
      },
    },
  };
};
const terminateAfter = (
  owner: ReturnType<typeof ownerRecord>,
  release: Pick<ReturnType<typeof releaseFor>, "recorded">,
  id: string,
  sequence: number,
  status = "cancelled",
) => {
  if (release.recorded.event.type !== "task-group.work-released")
    throw new Error("Expected release");
  return ownerObservation(owner, id, sequence, {
    type: "task.run-ownership-terminated",
    observedSequence: sequence - 1,
    terminationEvidence: {
      kind: "after_executable_claim",
      terminalRunEvent: release.recorded.event.releaseEvidence.terminalRunEvent,
      terminalStatus: status,
      workClaimId: release.recorded.event.claim.binding.workClaimId,
      workReleaseEvent: {
        type: "task-group.work-released",
        eventId: release.recorded.event.id,
        sequence: release.recorded.context.sequence,
      },
    },
  });
};
const pureSeed = (fixture: Effect.Success<ReturnType<typeof seedReleaseClaim>>) =>
  reduceTaskGroupControl(
    reduceTaskMembership(
      reduceTaskGroupControl(emptyTaskGroupControl(env), fixture.group),
      fixture.join,
    ),
    fixture.manager,
  );
const insertOwnerObservation = Effect.fn("TaskGroupTest.insertOwnerObservation")(function* (
  record: ReturnType<typeof ownerRecord>,
) {
  const sql = yield* SqlClient.SqlClient;
  const columns = {
    event_id: "id",
    run_id: "ownership.run.runId",
    environment_id: "environmentId",
    task_id: "ownership.run.task.taskId",
    group_id: "ownership.run.group.groupId",
    manager_thread_id: "ownership.run.manager.threadId",
    kind: "type",
    command_id: "commandId",
    occurred_at: "occurredAt",
    source_event_id: "sourceEventId",
    source_sequence: "sourceSequence",
    observed_sequence: "observedSequence",
    terminal_event_id: "terminationEvidence.terminalRunEvent.eventId",
    terminal_sequence: "terminationEvidence.terminalRunEvent.sequence",
    terminal_status: "terminationEvidence.terminalStatus",
    settlement_command_id: "terminationEvidence.settlementCommandId",
  };
  yield* sql.unsafe(
    `INSERT INTO work_task_run_ownership_observations(${Object.keys(columns).join(",")},sequence,event_json,after_release_companion_id) SELECT ${Object.values(
      columns,
    )
      .map((p) => `json_extract(value,'$.${p}')`)
      .join(
        ",",
      )},?,value,CASE WHEN json_extract(value,'$.terminationEvidence.kind')='after_executable_claim' THEN json_extract(value,'$.id') END FROM(SELECT ? value)`,
    [record.context.sequence, JSON.stringify(record.event)],
  );
});
const insertOwnerBridge = Effect.fn("TaskGroupTest.insertOwnerBridge")(function* (
  record: ReturnType<typeof ownerRecord>,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO work_task_run_ownership_release_bridges SELECT json_extract(value,'$.id'),json_extract(value,'$.terminationEvidence.workClaimId'),json_extract(value,'$.terminationEvidence.workReleaseEvent.eventId'),json_extract(value,'$.ownership.run.runId'),json_extract(value,'$.terminationEvidence.terminalRunEvent.eventId'),json_extract(value,'$.terminationEvidence.terminalRunEvent.sequence') FROM(SELECT ${JSON.stringify(record.event)} value)`;
});
it.effect.each([false, true])(
  "retains complete custody and actual closure with bridgeFirst=%s",
  (bridgeFirst) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient,
        release = releaseFixture();
      const fixture = yield* seedReleaseClaim(release);
      let state = reduceTaskRunOwnership(pureSeed(fixture), fixture.owner);
      const beforePhysical = state;
      state = reduceTaskWork(state, fixture.claim);
      const uncertain = workRecord({
        event: {
          ...release.recorded.event,
          id: "uncertain",
          type: "task-group.work-uncertain",
          observedSequence: undefined,
          releaseEvidence: undefined,
          sourceEventId: "owner",
          sourceSequence: 20,
          reason: "lost reply",
        },
        context: { ...release.recorded.context, eventId: "uncertain", sequence: 31 },
      });
      state = reduceTaskWork(state, uncertain);
      const occupied = state,
        closure = terminateAfter(fixture.owner, release, "owner-terminal", 35);
      assert.throws(() => reduceTaskRunOwnership(state, closure), GroupReplayConflict);
      assert.strictEqual(reduceTaskWork(state, fixture.claim), state);
      state = reduceTaskWork(state, release);
      assert.throws(
        () =>
          reduceTaskRunOwnership(
            state,
            terminateAfter(fixture.owner, release, "wrong-status", 35, "failed"),
          ),
        GroupReplayConflict,
      );
      state = reduceTaskRunOwnership(state, closure);
      assert.strictEqual(reduceTaskRunOwnership(state, fixture.owner), state);
      assert.strictEqual(reduceTaskWork(state, release), state);
      assert.strictEqual(HashMap.size(occupied.activeWork), 3);
      assert.strictEqual(HashMap.size(state.activeWork), 0);
      assert.strictEqual(HashMap.size(beforePhysical.claims), 0);
      assert.strictEqual(HashMap.size(state.claims), 1);
      assert.throws(
        () =>
          reduceTaskWork(state, {
            ...release,
            native: { run: { ...release.native.run, status: "failed" } },
          }),
        GroupReplayConflict,
      );
      assert.throws(
        () => reduceTaskWork(state, activateAgain(fixture.claim, "after-terminal", 40)),
        GroupReplayConflict,
      );
      yield* sql.withTransaction(writeReleaseEvidence(release.recorded, release.native));
      yield* sql.withTransaction(insertOwnerObservation(closure)).pipe(Effect.flip);
      assert.deepStrictEqual(
        yield* sql`SELECT event_id FROM work_task_run_ownership_observations`,
        [],
      );
      yield* sql.withTransaction(
        Effect.gen(function* () {
          if (bridgeFirst) yield* insertOwnerBridge(closure);
          yield* insertOwnerObservation(closure);
          if (!bridgeFirst) yield* insertOwnerBridge(closure);
        }),
      );
      const [row] = yield* sql<{
        event_json: string;
      }>`SELECT event_json FROM work_task_run_ownership_observations`;
      assert.deepStrictEqual(JSON.parse(row!.event_json), closure.event);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);
it.effect("rejects stale same-run closure and preserves historical basis and source roles", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const fixture = yield* seedReleaseClaim(releaseFixture());
    const base = reduceTaskRunOwnership(pureSeed(fixture), fixture.owner);
    const first = reduceTaskWork(base, fixture.claim),
      r1 = releaseFor(fixture.claim, 40);
    const closed1 = reduceTaskWork(first, r1),
      c2 = activateAgain(fixture.claim, "claim2", 50),
      r2 = releaseFor(c2, 60);
    const active2 = reduceTaskWork(closed1, c2);
    yield* sql.withTransaction(writeReleaseEvidence(r1.recorded, r1.native));
    yield* insertPhysicalWork(c2.event);
    yield* insertOwnerBridge(terminateAfter(fixture.owner, r1, "sql-stale-active", 70)).pipe(
      Effect.flip,
    );
    assert.throws(
      () => reduceTaskRunOwnership(active2, terminateAfter(fixture.owner, r1, "stale", 70)),
      GroupReplayConflict,
    );
    const closed2 = reduceTaskWork(active2, r2);
    yield* sql.withTransaction(writeReleaseEvidence(r2.recorded, r2.native));
    yield* insertOwnerBridge(terminateAfter(fixture.owner, r1, "sql-stale-closed", 70)).pipe(
      Effect.flip,
    );
    const finalRecord = terminateAfter(fixture.owner, r2, "sql-final", 70);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* insertOwnerObservation(finalRecord);
        yield* insertOwnerBridge(finalRecord);
      }),
    );
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    assert.throws(
      () => reduceTaskRunOwnership(closed2, terminateAfter(fixture.owner, r1, "stale", 70)),
      GroupReplayConflict,
    );
    const done = reduceTaskRunOwnership(closed2, terminateAfter(fixture.owner, r2, "done", 70));
    assert.strictEqual(HashMap.size(done.claims), 2);
    assert.throws(
      () => reduceTaskWork(closed1, activateAgain(fixture.claim, "overlap", 35)),
      GroupReplayConflict,
    );
    assert.throws(() => reduceTaskWork(first, c2), GroupReplayConflict);
    const held = controlChange(
      fixture.manager.event.control,
      "hold",
      100,
      { dispatchHold: "paused" },
      "task-group.hold-changed",
    );
    const moved = membership(
      fixture.join.root.event.snapshot,
      [held.event.control],
      null,
      "remove",
      110,
      [111],
    );
    const later = reduceTaskMembership(reduceTaskGroupControl(base, held), moved);
    const historical = reduceTaskWork(later, fixture.claim);
    assert.deepStrictEqual(historical.groups, later.groups);
    assert.deepStrictEqual(historical.tasks, later.tasks);
    assert.strictEqual(HashMap.size(historical.activeWork), 3);
    const ownerUncertain = ownerObservation(fixture.owner, "owner-uncertain", 25, {
      type: "task.run-ownership-uncertain",
      sourceEventId: "owner",
      sourceSequence: 20,
      reason: "pending",
    });
    const observed = reduceTaskRunOwnership(later, ownerUncertain);
    assert.strictEqual(get(observed.owners, "run").latestSequence, 25);
    assert.strictEqual(observed.observedMaximum, 111);
    const sourceOwner = ownerRecord({
      ...fixture.owner,
      event: {
        ...fixture.owner.event,
        ownership: {
          ...fixture.owner.event.ownership,
          sourceEventId: "future-control",
          origin: {
            type: "initial_start",
            startCommandId: "start",
            startEventId: "future-control",
            startSequence: 10,
          },
        },
      },
    });
    const sourceState = reduceTaskRunOwnership(pureSeed(fixture), sourceOwner);
    const futureControl = controlChange(
      fixture.manager.event.control,
      "future-control",
      10,
      { dispatchHold: "paused" },
      "task-group.hold-changed",
    );
    const enriched = reduceTaskGroupControl(sourceState, futureControl);
    assert.strictEqual(get(enriched.references, "future-control").taskId, "task");
    assert.strictEqual(get(enriched.references, "future-control").kind, "task-group.hold-changed");
  }).pipe(Effect.provide(db)),
);

it.effect(
  "preserves original automatic debit and before-claim termination without a new charge",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient,
        fixture = yield* seedReleaseClaim(releaseFixture());
      const debit = decodeDebit({
        event: {
          id: "debit",
          type: "task.allowance-debited",
          taskId: "task",
          commandId: "reservation-command",
          occurredAt: time,
          definitionRevision: 1,
          lifecycleRevision: 1,
          completionPolicyRevision: 1,
          actionPolicyRevision: 1,
          attribution: { type: "server" },
          previous: { type: "configured", cap: 1, consumed: 0 },
          resulting: { type: "configured", cap: 1, consumed: 1 },
          allowanceUnit: 1,
        },
        context: {
          eventId: "debit",
          commandId: "reservation-command",
          occurredAt: time,
          sequence: 23,
        },
      });
      const original = fixture.owner.event.ownership;
      const owner = ownerRecord({
        event: {
          ...fixture.owner.event,
          id: "automatic-owner",
          commandId: "record-auto",
          ownership: {
            ...original,
            run: { ...original.run, runId: "auto-run", messageId: "auto-message" },
            recorded: {
              eventId: "automatic-owner",
              commandId: "record-auto",
              occurredAt: time,
              sequence: 25,
            },
            nativeAcceptance: {
              ...original.nativeAcceptance,
              receipt: {
                ...original.nativeAcceptance.receipt,
                commandId: "auto-native",
                resultSequence: 22,
              },
              runEvent: { type: "run.created", eventId: "auto-run-event", sequence: 21 },
              messageEvent: {
                type: "message.updated",
                eventId: "auto-message-event",
                sequence: 22,
              },
            },
            origin: {
              type: "automatic_followup",
              reservationId: "reservation",
              reservationCommandId: "reservation-command",
              allowanceUnit: 1,
              observedDirtyGeneration: 1,
              acknowledgedGeneration: 0,
              pending: { sources: [], overflowThroughGeneration: null },
              debitEvent: {
                type: "task.allowance-debited",
                eventId: "debit",
                sequence: 23,
                commandId: "reservation-command",
                taskId: "task",
                definitionRevision: 1,
                lifecycleRevision: 1,
                completionPolicyRevision: 1,
                actionPolicyRevision: 1,
                allowanceUnit: 1,
              },
            },
          },
        },
        context: {
          eventId: "automatic-owner",
          commandId: "record-auto",
          occurredAt: time,
          sequence: 25,
        },
      });
      if (owner.event.type !== "task.run-ownership-accepted") throw new Error("Expected owner");
      const input = freeze(owner),
        snapshot = structuredClone(input);
      let state = reduceTaskRunOwnership(
        reduceTaskRunOwnership(pureSeed(fixture), fixture.owner),
        input,
      );
      assert.deepStrictEqual(input, snapshot);
      assert.strictEqual(HashMap.size(state.activeWork), 0);
      const duplicate = ownerRecord({
        event: {
          ...owner.event,
          id: "second-owner",
          commandId: "second-owner",
          ownership: {
            ...owner.event.ownership,
            recorded: {
              eventId: "second-owner",
              commandId: "second-owner",
              occurredAt: time,
              sequence: 29,
            },
            run: { ...owner.event.ownership.run, runId: "second-run", messageId: "second-message" },
            nativeAcceptance: {
              ...owner.event.ownership.nativeAcceptance,
              runEvent: { type: "run.created", eventId: "second-run-event", sequence: 26 },
              messageEvent: {
                type: "message.updated",
                eventId: "second-message-event",
                sequence: 27,
              },
              receipt: { ...owner.event.ownership.nativeAcceptance.receipt, resultSequence: 27 },
            },
          },
        },
        context: {
          eventId: "second-owner",
          commandId: "second-owner",
          occurredAt: time,
          sequence: 29,
        },
      });
      assert.throws(() => reduceTaskRunOwnership(state, duplicate), GroupReplayConflict);
      const before = ownerObservation(owner, "before-terminal", 29, {
        type: "task.run-ownership-terminated",
        observedSequence: 28,
        terminationEvidence: {
          kind: "before_executable_claim",
          terminalRunEvent: { type: "run.updated", eventId: "auto-terminal", sequence: 24 },
          terminalStatus: "cancelled",
          settlementCommandId: "settle",
        },
      });
      const terminated = reduceTaskRunOwnership(state, before);
      assert.strictEqual(HashMap.size(terminated.claims), 0);
      assert.strictEqual(HashMap.size(state.terminalOwners), 0);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* insertAcceptedRun(owner.event);
          yield* sql`INSERT INTO work_task_accepted_run_debits(event_id,run_id,sequence,task_id,command_id,allowance_unit,definition_revision,lifecycle_revision,completion_policy_revision,action_policy_revision,occurred_at,event_json)
      SELECT json_extract(value,'$.id'),'auto-run',23,json_extract(value,'$.taskId'),json_extract(value,'$.commandId'),json_extract(value,'$.allowanceUnit'),1,1,1,1,json_extract(value,'$.occurredAt'),value FROM(SELECT ${JSON.stringify(debit.event)} value)`;
        }),
      );
      yield* insertOwnerObservation(before);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
      const autoClaim = workRecord({
        event: {
          ...fixture.claim.event,
          id: "auto-claim",
          commandId: "activate-auto",
          claim: {
            ...fixture.claim.event.claim,
            acceptedEventId: "auto-claim",
            acceptedSequence: 40,
            sourceEventId: "automatic-owner",
            sourceSequence: 25,
            observedWorkSequence: 25,
            binding: {
              ...fixture.claim.event.claim.binding,
              runId: "auto-run",
              messageId: "auto-message",
              workClaimId: "auto-claim",
              effectId: "auto-effect",
              commandId: "activate-auto",
            },
            launchOrigin: {
              type: "automatic_followup",
              reservationId: "reservation",
              reservationCommandId: "reservation-command",
              allowanceUnit: 1,
              observedDirtyGeneration: 1,
              acknowledgedGeneration: 0,
            },
          },
        },
        context: {
          eventId: "auto-claim",
          commandId: "activate-auto",
          occurredAt: time,
          sequence: 40,
        },
      });
      assert.throws(() => reduceTaskWork(terminated, autoClaim), GroupReplayConflict);
      state = reduceTaskWork(state, autoClaim);
      assert.strictEqual(HashMap.size(state.activeWork), 3);
      assert.strictEqual(
        get(state.owners, "auto-run").record.event.ownership.origin.type,
        "automatic_followup",
      );
      assert.throws(() => reduceTaskRunOwnership(state, before), GroupReplayConflict);
      const knownSource = ownerObservation(fixture.owner, "known-source", 60, {
        type: "task.run-ownership-uncertain",
        sourceEventId: "future-hold",
        sourceSequence: 50,
        reason: "source only",
      });
      const known = reduceTaskRunOwnership(state, knownSource);
      const hold = controlChange(
        fixture.manager.event.control,
        "future-hold",
        50,
        { dispatchHold: "paused" },
        "task-group.hold-changed",
      );
      const learned = reduceTaskGroupControl(known, hold);
      assert.strictEqual(get(learned.references, "future-hold").kind, "task-group.hold-changed");
      assert.strictEqual(reduceTaskRunOwnership(learned, knownSource), learned);
    }).pipe(Effect.provide(db)),
);
