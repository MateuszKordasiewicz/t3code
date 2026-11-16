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
  reduceTaskReadiness,
  recordedTaskGroupCandidate,
  compareRecordedTaskCandidates,
  reduceTaskGroup,
  reduceTaskReconciliation,
  compareTaskIdentifiers,
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
              observedDirtyGeneration: 34,
              acknowledgedGeneration: 0,
              pending: {
                sources: Array.from({ length: 32 }, (_, i) => ({
                  kind: "result",
                  sourceId: `pending-${i}`,
                  resultRef: null,
                })),
                overflowThroughGeneration: 34,
              },
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
              observedDirtyGeneration: 34,
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
      let combined = reduceTaskGroup(learned, owner);
      for (let i = 0; i < 34; i++) {
        const dirty = dirtyFact(
          "task",
          `combined-dirty-${i}`,
          1000 + i,
          i,
          `combined-source-${i}`,
          500 + i,
        );
        combined = reduceTaskGroup(combined, dirty);
        yield* writeReconciliation(dirty);
      }
      const partial = ackFact("combined-ack", 1100, 0, 32, 33, 34);
      combined = reduceTaskGroup(combined, partial);
      yield* writeReconciliation(partial);
      assert.strictEqual(get(combined.dirty, "task").event.dirtyGeneration, 34);
      assert.strictEqual(get(combined.dirty, "task").event.acknowledgedGeneration, 32);
      const origin = get(combined.owners, "auto-run").record.event.ownership.origin;
      if (origin.type !== "automatic_followup") throw new Error("automatic origin");
      assert.strictEqual(origin.pending.sources.length, 32);
      assert.strictEqual(origin.pending.overflowThroughGeneration, 34);
      assert.strictEqual(origin.allowanceUnit, 1);
      assert.strictEqual(HashMap.size(combined.ready), 0);
      assert.strictEqual(HashMap.size(combined.claims), HashMap.size(learned.claims));
      assert.strictEqual(
        (yield* sql`SELECT count(*) AS n FROM work_task_accepted_run_debits`)[0]!.n,
        1,
      );
      const secondTask = initialTask("second-auto-task");
      const secondJoin = membership(
        secondTask,
        [get(combined.groups, "g").control],
        "g",
        "second-auto-join",
        1200,
        [1201],
      );
      combined = reduceTaskGroup(combined, secondJoin);
      const secondContext = {
        eventId: "second-auto-owner",
        commandId: "record-second-auto",
        occurredAt: time,
        sequence: 1300,
      };
      const secondOwner = ownerRecord({
        context: secondContext,
        event: {
          ...owner.event,
          id: secondContext.eventId,
          commandId: secondContext.commandId,
          ownership: {
            ...owner.event.ownership,
            recorded: secondContext,
            run: {
              ...owner.event.ownership.run,
              task: secondTask.task,
              membershipRevision: 2,
              taskRevisions: { ...secondTask.taskRevisions, actionPolicyRevision: 2 },
              runId: "second-auto-run",
              messageId: "second-auto-message",
            },
            sourceEventId: "second-auto-source",
            sourceSequence: 1202,
            observedWorkSequence: 1202,
            nativeAcceptance: {
              ...owner.event.ownership.nativeAcceptance,
              receipt: {
                ...owner.event.ownership.nativeAcceptance.receipt,
                commandId: "second-auto-native",
                resultSequence: 1252,
              },
              runEvent: { type: "run.created", eventId: "second-auto-native-run", sequence: 1251 },
              messageEvent: {
                type: "message.updated",
                eventId: "second-auto-native-message",
                sequence: 1252,
              },
            },
            origin: {
              ...origin,
              reservationId: "second-auto-reservation",
              reservationCommandId: "second-auto-reservation-command",
              debitEvent: {
                ...origin.debitEvent,
                eventId: "second-auto-debit",
                sequence: 1253,
                commandId: "second-auto-reservation-command",
                taskId: "second-auto-task",
                actionPolicyRevision: 2,
              },
            },
          },
        },
      });
      combined = reduceTaskGroup(combined, secondOwner);
      const firstRun = get(combined.owners, "auto-run").record.event.ownership;
      const secondRun = get(combined.owners, "second-auto-run").record.event.ownership;
      assert.deepStrictEqual(firstRun.run.manager, secondRun.run.manager);
      assert.notStrictEqual(firstRun.run.task.taskId, secondRun.run.task.taskId);
      assert.strictEqual(firstRun.run.taskRevisions.actionPolicyRevision, 1);
      assert.strictEqual(secondRun.run.taskRevisions.actionPolicyRevision, 2);
      assert.strictEqual(HashMap.size(get(combined.unclosedOwners, "second-auto-task")), 1);
      assert.strictEqual(HashMap.size(combined.activeWork), 3);
      assert.strictEqual(HashMap.size(combined.ready), 0);
    }).pipe(Effect.provide(db)),
);

const dirtyFact = (
  taskId: string,
  id: string,
  sequence: number,
  previous = 0,
  source = "source",
  sourceSequence = 40,
) => {
  const event = {
    ...fields(id),
    type: "task.dirty-source-recorded",
    attribution: { type: "server" },
    task: { environmentId: env, taskId },
    membership: null,
    sourceEventId: source,
    sourceSequence,
    previousDirtyGeneration: previous,
    dirtyGeneration: previous + 1,
    acknowledgedGeneration: 0,
  };
  const record = decodeWork({ event, context: context(event, sequence) });
  if (record.event.type !== "task.dirty-source-recorded") throw new Error("dirty fixture");
  return { ...record, event: record.event };
};
const graphProgress = (
  id: string,
  sequence: number,
  visited: readonly string[],
  patch: object = {},
) => {
  const event = {
    ...fields(id),
    type: "task.reconciliation-progressed",
    attribution: { type: "server" },
    scan: {
      kind: "graph_dependents",
      blocker: { environmentId: env, taskId: "blocker" },
      sourceEventId: "source",
      sourceSequence: 40,
      capturedRevision: 1,
    },
    observedRevision: 1,
    previousPosition: null,
    position: visited.length
      ? { dependentTaskId: visited[visited.length - 1], edgeId: "edge" }
      : null,
    visited: visited.map((taskId) => ({ environmentId: env, taskId })),
    completed: true,
    ...patch,
  };
  const record = decodeWork({ event, context: context(event, sequence) });
  if (record.event.type !== "task.reconciliation-progressed") throw new Error("scan fixture");
  return { ...record, event: record.event };
};
it("closes actual dirty sources atomically without treating references as records", () => {
  const empty = emptyTaskGroupControl(env),
    dirty = dirtyFact("task", "dirty", 60),
    progress = graphProgress("progress", 50, ["task"]);
  assert.throws(() => reduceTaskReconciliation(empty, [progress]), GroupReplayConflict);
  const result = reduceTaskReconciliation(empty, [progress, dirty]);
  assert.strictEqual(HashMap.size(empty.events), 0);
  assert.strictEqual(HashMap.size(result.events), 2);
  assert.deepStrictEqual(reduceTaskReconciliation(empty, [dirty, progress]), result);
  assert.strictEqual(reduceTaskReconciliation(result, [progress, dirty]), result);
  assert.strictEqual(compareTaskIdentifiers("\uE000", "\u{10000}"), -1);
});

const ackFact = (
  id: string,
  sequence: number,
  previous: number,
  acknowledged: number,
  observed: number,
  dirty: number,
) => {
  const event = {
    ...fields(id),
    type: "task.dirty-acknowledged",
    attribution: { type: "server" },
    task: { environmentId: env, taskId: "task" },
    membership: null,
    sourceEventId: "ack-source",
    sourceSequence: 41,
    previousAcknowledgedGeneration: previous,
    acknowledgedGeneration: acknowledged,
    observedDirtyGeneration: observed,
    dirtyGeneration: dirty,
  };
  const record = decodeWork({ event, context: context(event, sequence) });
  if (record.event.type !== "task.dirty-acknowledged") throw new Error("ack fixture");
  return { ...record, event: record.event };
};
const writeReconciliation = Effect.fn("TaskGroupTest.writeReconciliation")(function* (
  record: Parameters<typeof reduceTaskReconciliation>[1][number],
) {
  const sql = yield* SqlClient.SqlClient,
    encoded = JSON.stringify(record.event);
  if (record.event.type === "task.reconciliation-progressed")
    yield* sql`INSERT INTO work_task_reconciliation_events(event_id,sequence,event_json) SELECT json_extract(${encoded},'$.id'),${record.context.sequence},${encoded}`;
  else
    yield* sql`INSERT INTO work_task_dirty_events(event_id,sequence,event_json) SELECT json_extract(${encoded},'$.id'),${record.context.sequence},${encoded}`;
});
it("preserves dirty generations across observed acknowledgements and rejects conflicting source replay", () => {
  const empty = emptyTaskGroupControl(env),
    first = dirtyFact("task", "dirty-1", 60),
    second = dirtyFact("task", "dirty-2", 61, 1, "source-2", 42);
  const two = reduceTaskReconciliation(empty, [first, second]);
  const ack = ackFact("ack", 62, 0, 1, 1, 2),
    partial = reduceTaskReconciliation(two, [ack]);
  assert.strictEqual(get(partial.dirty, "task").event.dirtyGeneration, 2);
  assert.strictEqual(get(partial.dirty, "task").event.acknowledgedGeneration, 1);
  assert.throws(
    () => reduceTaskReconciliation(two, [dirtyFact("task", "duplicate", 63, 2)]),
    GroupReplayConflict,
  );
  assert.throws(
    () => reduceTaskReconciliation(partial, [ackFact("stale", 63, 0, 2, 2, 2)]),
    GroupReplayConflict,
  );
  const finished = reduceTaskReconciliation(partial, [ackFact("finish", 64, 1, 2, 2, 2)]);
  assert.strictEqual(get(two.dirty, "task").event.acknowledgedGeneration, 0);
  assert.strictEqual(reduceTaskReconciliation(finished, [first, ack]), finished);
  const independent = reduceTaskReconciliation(finished, [dirtyFact("other", "other-dirty", 59)]);
  assert.strictEqual(get(independent.dirty, "other").event.dirtyGeneration, 1);
  assert.strictEqual(get(independent.dirty, "task").event.acknowledgedGeneration, 2);
  const unfulfilled = graphProgress("ack-not-dirty", 70, ["task"], {
    scan: {
      kind: "graph_dependents",
      blocker: { environmentId: env, taskId: "blocker" },
      sourceEventId: "ack-source",
      sourceSequence: 41,
      capturedRevision: 1,
    },
  });
  assert.throws(() => reduceTaskReconciliation(finished, [unfulfilled]), GroupReplayConflict);
  const changed = dirtyFact("task", "dirty-1", 60, 0, "changed", 42);
  assert.throws(() => reduceTaskReconciliation(finished, [changed]), GroupReplayConflict);
});
it.effect.each([false, true])(
  "matches actual deferred scan closure with progressFirst=%s",
  (progressFirst) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const ids = ["a\0\ud800", "b"],
        group = created("g-scan", 1);
      yield* writeControl(group);
      let state = reduceTaskGroupControl(emptyTaskGroupControl(env), group);
      for (const [i, id] of [...ids, "blocker"].entries()) {
        const encoded = JSON.stringify(id);
        yield* sql`INSERT INTO work_task_projections VALUES(json_extract(${encoded},'$'),NULL,1,1,1,1,'{}','raw','raw')`;
        yield* sql`INSERT INTO work_task_accepted_definitions VALUES(json_extract(${encoded},'$'),1,1,'{}','{}',${`definition-scan-${i}`},'raw')`;
        if (id === "blocker") continue;
        const join = membership(
          initialTask(id),
          [get(state.groups, "g-scan").control],
          "g-scan",
          `scan-join-${i}`,
          2 + i * 2,
          [3 + i * 2],
        );
        yield* sql.withTransaction(writeJoin(join));
        state = reduceTaskMembership(state, join);
      }
      const original = state,
        dirty = ids.map((id, i) => dirtyFact(id, `source-${i}`, 60 + i));
      const progress = graphProgress("members-progress", 50, ids, {
        scan: {
          kind: "group_members",
          group: { environmentId: env, groupId: "g-scan" },
          sourceEventId: "source",
          sourceSequence: 40,
          capturedRevision: 2,
        },
        observedRevision: 2,
        position: { taskId: "b" },
        visited: ids.map((taskId) => ({
          task: { environmentId: env, taskId },
          membershipGeneration: 1,
        })),
        completed: false,
      });
      assert.throws(() => reduceTaskReconciliation(state, [progress]), GroupReplayConflict);
      const failure = yield* Effect.exit(sql.withTransaction(writeReconciliation(progress)));
      assert.strictEqual(failure._tag, "Failure");
      assert.deepStrictEqual(yield* sql`SELECT event_id FROM work_task_reconciliation_events`, []);
      const batch = progressFirst ? [progress, ...dirty] : [...dirty, progress];
      yield* sql.withTransaction(Effect.forEach(batch, writeReconciliation, { discard: true }));
      state = reduceTaskReconciliation(state, batch);
      assert.strictEqual(HashMap.size(original.dirtySources), 0);
      const rows = yield* sql<{
        event_json: string;
      }>`SELECT event_json FROM work_task_dirty_events ORDER BY sequence`;
      assert.deepStrictEqual(
        rows.map((row) => JSON.parse(row.event_json)),
        dirty.map((record) => record.event),
      );
      assert.strictEqual(
        (yield* sql<{ n: number }>`SELECT count(*) n FROM work_task_reconciliation_visits`)[0]!.n,
        2,
      );
      const complete = graphProgress("members-complete", 51, [], {
        scan: progress.event.scan,
        previousPosition: progress.event.position,
        position: progress.event.position,
        observedRevision: 3,
      });
      yield* writeReconciliation(complete);
      state = reduceTaskReconciliation(state, [...dirty, complete]);
      assert.strictEqual(reduceTaskReconciliation(state, batch), state);
      const resumed = graphProgress("members-resume", 52, [], {
        scan: progress.event.scan,
        previousPosition: progress.event.position,
        position: progress.event.position,
        observedRevision: 3,
      });
      assert.throws(() => reduceTaskReconciliation(state, [resumed]), GroupReplayConflict);
      assert.strictEqual((yield* Effect.exit(writeReconciliation(resumed)))._tag, "Failure");
      const graph = graphProgress("graph-progress", 53, ids);
      yield* writeReconciliation(graph);
      state = reduceTaskReconciliation(state, [graph]);
      const histories = yield* sql<{
        event_json: string;
      }>`SELECT event_json FROM work_task_reconciliation_events ORDER BY sequence`;
      for (const row of histories) {
        const event = JSON.parse(row.event_json);
        assert.deepStrictEqual(get(state.events, event.id).event, event);
      }
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);
it.effect("matches SQLite lossless identifier order and source-backed graph cursor rules", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* runMigrations();
    const ids = ["x", "x\0", "\ud800", "\udfff", "\ue000", "\u{10000}", "\0x"];
    const ordered = [...ids].sort(compareTaskIdentifiers);
    const rows = yield* sql<{
      value: string;
    }>`SELECT value FROM json_each(${JSON.stringify(ids.map((id) => JSON.stringify(id)))}) ORDER BY json_extract(value,'$') COLLATE BINARY`;
    assert.deepStrictEqual(
      rows.map((row) => JSON.parse(row.value)),
      ordered,
    );
    for (const [i, id] of [...ids, "blocker"].entries()) {
      yield* sql`INSERT INTO work_task_projections VALUES(json_extract(${JSON.stringify(id)},'$'),NULL,1,1,1,1,'{}','raw','raw')`;
      if (id !== "blocker") yield* writeReconciliation(dirtyFact(id, `order-dirty-${i}`, 60 + i));
    }
    let state = reduceTaskReconciliation(
      emptyTaskGroupControl(env),
      ids.map((id, i) => dirtyFact(id, `order-dirty-${i}`, 60 + i)),
    );
    const progress = graphProgress("ordered", 50, ordered, { completed: false });
    yield* writeReconciliation(progress);
    state = reduceTaskReconciliation(state, [progress]);
    const sameTaskNextEdge = graphProgress("next-edge", 51, [ordered[ordered.length - 1]!], {
      previousPosition: progress.event.position,
      position: { dependentTaskId: ordered[ordered.length - 1], edgeId: "edge-z" },
      completed: false,
    });
    yield* writeReconciliation(sameTaskNextEdge);
    state = reduceTaskReconciliation(state, [sameTaskNextEdge]);
    const bad = graphProgress("reverse-edge", 52, [ordered[ordered.length - 1]!], {
      previousPosition: sameTaskNextEdge.event.position,
      position: { dependentTaskId: ordered[ordered.length - 1], edgeId: "edge-a" },
    });
    assert.throws(() => reduceTaskReconciliation(state, [bad]), GroupReplayConflict);
    assert.strictEqual((yield* Effect.exit(writeReconciliation(bad)))._tag, "Failure");
    const unordered = graphProgress("unordered", 49, [...ordered].reverse());
    assert.throws(
      () => reduceTaskReconciliation(emptyTaskGroupControl(env), [unordered]),
      GroupReplayConflict,
    );
    assert.strictEqual((yield* Effect.exit(writeReconciliation(unordered)))._tag, "Failure");
    const changedCaptured = graphProgress("changed-captured", 53, [], {
      scan: { ...sameTaskNextEdge.event.scan, capturedRevision: 2 },
      observedRevision: 2,
      previousPosition: sameTaskNextEdge.event.position,
      position: sameTaskNextEdge.event.position,
    });
    assert.throws(() => reduceTaskReconciliation(state, [changedCaptured]), GroupReplayConflict);
    assert.strictEqual((yield* Effect.exit(writeReconciliation(changedCaptured)))._tag, "Failure");
    assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
  }).pipe(Effect.provide(db)),
);

const readyFact = (
  claim: ReturnType<typeof workRecord>,
  id: string,
  sequence: number,
  kind: "qualified" | "accepted" | "settled" | "disqualified",
  previous: number | null,
  patch: object = {},
) => {
  const binding = claim.event.claim.binding;
  const event = {
    ...fields(id),
    type: "task.ready-sequence-updated",
    attribution: { type: "server" },
    task: binding.task,
    group: binding.group,
    membershipGeneration: binding.membershipGeneration,
    basis: {
      taskRevisions: binding.taskRevisions,
      graphRevision: binding.graphRevision,
      controlRevision: binding.controlRevision,
      membershipRevision: binding.membershipRevision,
      managerGeneration: binding.manager.managerGeneration,
    },
    kind,
    previousOldestReadySequence: previous,
    oldestReadySequence:
      kind === "qualified" ? (previous ?? sequence) : kind === "settled" ? sequence : null,
    sourceEventId: kind === "accepted" ? claim.event.claim.acceptedEventId : "ready-source",
    sourceSequence: kind === "accepted" ? claim.event.claim.acceptedSequence : 9,
    ...(kind === "accepted" || kind === "settled"
      ? {
          claim: {
            binding,
            acceptedEventId: claim.event.claim.acceptedEventId,
            acceptedSequence: claim.event.claim.acceptedSequence,
            acceptedAt: claim.event.claim.acceptedAt,
          },
        }
      : {}),
    ...(kind === "disqualified" ? { reason: "task_pause" } : {}),
    ...patch,
  };
  const record = decodeWork({ event, context: context(event, sequence) });
  if (record.event.type !== "task.ready-sequence-updated") throw new Error("ready fixture");
  return { ...record, event: record.event };
};
const writeReady = Effect.fn("TaskGroupTest.writeReady")(function* (
  record: ReturnType<typeof readyFact>,
) {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO work_task_ready_events(event_id,sequence,event_json) VALUES (${record.event.id},${record.context.sequence},${JSON.stringify(record.event)})`;
});
it.effect("uses historical custody for readiness and retains consumed age until settlement", () =>
  Effect.gen(function* () {
    const fixture = yield* seedReleaseClaim(releaseFixture());
    let state = reduceTaskWork(
      reduceTaskRunOwnership(pureSeed(fixture), fixture.owner),
      fixture.claim,
    );
    const sql = yield* SqlClient.SqlClient;
    const apply = Effect.fn("TaskGroupTest.applyReady")(function* (
      record: ReturnType<typeof readyFact>,
    ) {
      yield* writeReady(record);
      state = reduceTaskReadiness(state, record);
      const ready = get(state.ready, JSON.stringify(["task", 1]));
      const [row] =
        yield* sql`SELECT * FROM work_task_ready_state WHERE task_id='task' AND membership_generation=1`;
      assert.strictEqual(row!.latest_event_id, ready.record.event.id);
      assert.strictEqual(row!.latest_sequence, ready.record.context.sequence);
      assert.strictEqual(row!.oldest_ready_sequence, ready.record.event.oldestReadySequence);
      assert.strictEqual(row!.epoch_start_event_id, ready.epoch ?? null);
      assert.strictEqual(row!.pending_claim_id, ready.pending ?? null);
      assert.strictEqual(row!.last_served_claim_id, ready.served?.claimId ?? null);
      assert.strictEqual(row!.last_served_sequence, ready.served?.sequence ?? null);
      assert.deepStrictEqual(JSON.parse(String(row!.basis_json)), ready.record.event.basis);
    });
    const qualified = readyFact(fixture.claim, "qualified", 25, "qualified", null);
    yield* apply(qualified);
    assert.strictEqual(
      recordedTaskGroupCandidate(state, "g")?.ready.record.event.oldestReadySequence,
      25,
    );
    yield* apply(
      readyFact(fixture.claim, "refresh", 26, "qualified", 25, {
        basis: { ...qualified.event.basis, controlRevision: 5 },
      }),
    );
    yield* apply(readyFact(fixture.claim, "consumed", 31, "accepted", 25));
    yield* apply(readyFact(fixture.claim, "paused", 32, "disqualified", null));
    assert.throws(
      () => reduceTaskReadiness(state, readyFact(fixture.claim, "bypass", 33, "qualified", null)),
      GroupReplayConflict,
    );
    const release = releaseFor(fixture.claim, 70);
    yield* sql.withTransaction(writeReleaseEvidence(release.recorded, release.native));
    state = reduceTaskGroup(state, release);
    yield* apply(
      readyFact(fixture.claim, "settled", 71, "settled", null, {
        releaseEventId: release.recorded.event.id,
        releaseSequence: 70,
        sourceEventId: release.recorded.event.id,
        sourceSequence: 70,
      }),
    );
    assert.strictEqual(
      recordedTaskGroupCandidate(state, "g")?.ready.record.event.oldestReadySequence,
      71,
    );
    assert.strictEqual(reduceTaskGroup(state, qualified), state);
    const before = state;
    const wrongCas = readyFact(fixture.claim, "wrong-cas", 72, "qualified", 25);
    assert.throws(() => reduceTaskReadiness(state, wrongCas), GroupReplayConflict);
    assert.strictEqual((yield* Effect.exit(writeReady(wrongCas)))._tag, "Failure");
    yield* apply(readyFact(fixture.claim, "post-settlement-pause", 72, "disqualified", 71));
    const duplicateRelease = readyFact(fixture.claim, "duplicate-settlement", 73, "settled", null, {
      releaseEventId: release.recorded.event.id,
      releaseSequence: 70,
      sourceEventId: release.recorded.event.id,
      sourceSequence: 70,
    });
    assert.throws(() => reduceTaskReadiness(state, duplicateRelease), GroupReplayConflict);
    assert.strictEqual((yield* Effect.exit(writeReady(duplicateRelease)))._tag, "Failure");
    assert.strictEqual(recordedTaskGroupCandidate(before, "g")?.oldestReadySequence, 71);
    assert.strictEqual(recordedTaskGroupCandidate(state, "g"), undefined);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "resolves delayed readiness against historical intervals rather than current custody",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const fixture = yield* seedReleaseClaim(releaseFixture());
      let state = reduceTaskGroup(reduceTaskGroup(pureSeed(fixture), fixture.owner), fixture.claim);
      const release = releaseFor(fixture.claim, 80);
      yield* sql.withTransaction(writeReleaseEvidence(release.recorded, release.native));
      state = reduceTaskGroup(state, release);
      const tooEarly = readyFact(fixture.claim, "early", 70, "qualified", null);
      assert.throws(() => reduceTaskReadiness(state, tooEarly), GroupReplayConflict);
      assert.strictEqual((yield* Effect.exit(writeReady(tooEarly)))._tag, "Failure");
      const next = activateAgain(fixture.claim, "later-claim", 100);
      yield* insertPhysicalWork(next.event);
      const future = reduceTaskWork(state, next);
      const middle = readyFact(fixture.claim, "middle", 90, "settled", null, {
        sourceEventId: release.recorded.event.id,
        sourceSequence: 80,
        releaseEventId: release.recorded.event.id,
        releaseSequence: 80,
      });
      yield* writeReady(middle);
      const delayed = reduceTaskReadiness(future, freeze(middle));
      assert.strictEqual(recordedTaskGroupCandidate(delayed, "g")?.ready.served?.sequence, 30);
      assert.strictEqual(recordedTaskGroupCandidate(delayed, "g")?.physicalClaims.length, 1);
      assert.strictEqual(recordedTaskGroupCandidate(delayed, "g")?.unclosedOwners.length, 1);
      assert.strictEqual(HashMap.size(future.ready), 0);
      assert.strictEqual(reduceTaskGroup(delayed, middle), delayed);
      const [row] =
        yield* sql`SELECT last_served_sequence,oldest_ready_sequence FROM work_task_ready_state`;
      assert.strictEqual(row!.last_served_sequence, 30);
      assert.strictEqual(row!.oldest_ready_sequence, 90);
      for (const q of [25, 30, 31, 79, 80, 81, 99, 100, 101]) {
        const claims =
          yield* sql`SELECT work_claim_id FROM work_task_executable_claims WHERE task_id='task' AND accepted_sequence<${q} ORDER BY accepted_sequence DESC LIMIT 1`;
        assert.strictEqual(
          claims[0]?.work_claim_id,
          q <= 30
            ? undefined
            : q <= 100
              ? fixture.claim.event.claim.binding.workClaimId
              : next.event.claim.binding.workClaimId,
        );
        const occupied =
          yield* sql`SELECT EXISTS(SELECT 1 FROM work_task_executable_claims c WHERE c.task_id='task' AND c.accepted_sequence<${q} AND NOT EXISTS(SELECT 1 FROM work_task_claim_releases r WHERE r.event_id=c.release_event_id AND r.work_claim_id=c.work_claim_id AND r.sequence<${q})) AS occupied`;
        assert.strictEqual(occupied[0]!.occupied, (q > 30 && q <= 80) || q > 100 ? 1 : 0);
      }
      const plan =
        yield* sql`EXPLAIN QUERY PLAN SELECT work_claim_id FROM work_task_executable_claims WHERE task_id='task' AND accepted_sequence<90 ORDER BY accepted_sequence DESC LIMIT 1`;
      assert.match(JSON.stringify(plan), /SEARCH.*INDEX/);
      assert.notMatch(JSON.stringify(plan), /TEMP B-TREE|SCAN work_task_executable_claims/);
    }).pipe(Effect.provide(db)),
);

it.effect.each(["qualified", "settled", "disqualified"] as const)(
  "matches the physical reverse guard after a later %s fact",
  (kind) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const fixture = yield* seedReleaseClaim(releaseFixture());
      const release = releaseFor(fixture.claim, 80);
      yield* sql.withTransaction(writeReleaseEvidence(release.recorded, release.native));
      let state = reduceTaskGroup(
        reduceTaskGroup(reduceTaskGroup(pureSeed(fixture), fixture.owner), fixture.claim),
        release,
      );
      const observation = readyFact(
        fixture.claim,
        "future-ready",
        95,
        kind,
        null,
        kind === "settled"
          ? {
              releaseEventId: release.recorded.event.id,
              releaseSequence: 80,
              sourceEventId: release.recorded.event.id,
              sourceSequence: 80,
            }
          : {},
      );
      yield* writeReady(observation);
      state = reduceTaskReadiness(state, observation);
      const later = activateAgain(fixture.claim, "retroactive", 90);
      if (kind === "disqualified") {
        state = reduceTaskWork(state, later);
        yield* insertPhysicalWork(later.event);
        assert.strictEqual(reduceTaskWork(state, later), state);
      } else {
        assert.throws(() => reduceTaskWork(state, later), GroupReplayConflict);
        assert.strictEqual((yield* Effect.exit(insertPhysicalWork(later.event)))._tag, "Failure");
        const cleared = readyFact(fixture.claim, "cleared", 96, "disqualified", 95);
        yield* writeReady(cleared);
        state = reduceTaskReadiness(state, cleared);
        assert.throws(() => reduceTaskWork(state, later), GroupReplayConflict);
        assert.strictEqual((yield* Effect.exit(insertPhysicalWork(later.event)))._tag, "Failure");
      }
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);

it.effect("keeps retired membership debt separate from the new member queue age", () =>
  Effect.gen(function* () {
    const fixture = yield* seedReleaseClaim(releaseFixture());
    let state = reduceTaskGroup(emptyTaskGroupControl(env), fixture.group);
    assert.throws(() => reduceTaskGroup(state, fixture.join.root), GroupReplayConflict);
    assert.throws(() => reduceTaskGroup(state, fixture.join.companions[0]!), GroupReplayConflict);
    state = reduceTaskGroup(reduceTaskGroup(state, fixture.join), fixture.manager);
    state = reduceTaskGroup(reduceTaskGroup(state, fixture.owner), fixture.claim);
    state = reduceTaskGroup(state, readyFact(fixture.claim, "old-ready", 25, "qualified", null));
    state = reduceTaskGroup(state, readyFact(fixture.claim, "old-consumed", 31, "accepted", 25));
    const removed = membership(
      fixture.join.root.event.snapshot,
      [get(state.groups, "g").control],
      null,
      "remove-ready",
      40,
      [41],
    );
    state = reduceTaskGroup(state, removed);
    assert.strictEqual(recordedTaskGroupCandidate(state, "g"), undefined);
    const rejoined = membership(
      removed.root.event.snapshot,
      [get(state.groups, "g").control],
      "g",
      "rejoin-ready",
      50,
      [51],
    );
    state = reduceTaskGroup(state, rejoined);
    const early = readyFact(fixture.claim, "new-early", 60, "qualified", null, {
      membershipGeneration: 2,
    });
    assert.throws(() => reduceTaskGroup(state, early), GroupReplayConflict);
    const release = releaseFor(fixture.claim, 70);
    state = reduceTaskGroup(state, release);
    const newReady = readyFact(fixture.claim, "new-ready", 80, "qualified", null, {
      membershipGeneration: 2,
    });
    state = reduceTaskGroup(state, newReady);
    assert.strictEqual(
      get(state.ready, JSON.stringify(["task", 1])).pending,
      fixture.claim.event.claim.binding.workClaimId,
    );
    assert.strictEqual(recordedTaskGroupCandidate(state, "g")?.generation, 2);
    assert.strictEqual(recordedTaskGroupCandidate(state, "g")?.oldestReadySequence, 80);
    const held = controlChange(
      get(state.groups, "g").control,
      "ready-hold",
      90,
      { dispatchHold: "paused" },
      "task-group.hold-changed",
    );
    state = reduceTaskGroup(state, held);
    assert.strictEqual(recordedTaskGroupCandidate(state, "g")?.control.dispatchHold, "paused");
    assert.strictEqual(recordedTaskGroupCandidate(state, "g")?.oldestReadySequence, 80);
    assert.strictEqual(recordedTaskGroupCandidate(state, "g")?.unclosedOwners.length, 1);
    const closed = reduceTaskRunOwnership(
      state,
      terminateAfter(fixture.owner, release, "closed-owner", 100),
    );
    assert.strictEqual(recordedTaskGroupCandidate(closed, "g")?.unclosedOwners.length, 0);
    assert.strictEqual(recordedTaskGroupCandidate(state, "g")?.unclosedOwners.length, 1);
    assert.strictEqual(HashMap.size(closed.claims), 1);
  }).pipe(Effect.provide(db)),
);

it.effect(
  "selects recorded current members with byte-safe identities and indexed SQL ordering",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const fixture = yield* seedReleaseClaim(releaseFixture());
      let state = pureSeed(fixture);
      const ids = ["task", "second\u0000\ud800"];
      const second = ids[1]!;
      yield* sql`INSERT INTO work_task_projections VALUES (json_extract(${JSON.stringify(second)},'$'),NULL,1,1,1,1,'{}','1','1')`;
      yield* sql`INSERT INTO work_task_accepted_definitions VALUES (json_extract(${JSON.stringify(second)},'$'),1,1,'{}','{}','second-definition','1')`;
      const joined = membership(
        initialTask(second),
        [get(state.groups, "g").control],
        "g",
        "second-join",
        5,
        [6],
      );
      yield* sql.withTransaction(writeJoin(joined));
      state = reduceTaskGroup(state, joined);
      for (const [i, taskId] of ids.entries()) {
        const record = readyFact(fixture.claim, `candidate-${i}`, 25 + i, "qualified", null, {
          task: { environmentId: env, taskId },
        });
        state = reduceTaskGroup(state, record);
        yield* writeReady(record);
      }
      assert.strictEqual(recordedTaskGroupCandidate(state, "g")?.taskId, "task");
      const paused = readyFact(fixture.claim, "candidate-paused", 27, "disqualified", 25);
      state = reduceTaskGroup(state, paused);
      yield* writeReady(paused);
      const candidate = recordedTaskGroupCandidate(state, "g")!;
      assert.strictEqual(candidate.taskId, second);
      assert.strictEqual(candidate.oldestReadySequence, 26);
      const query = `SELECT hex(s.task_id) AS task_hex FROM work_task_ready_state s JOIN work_task_group_memberships m ON m.task_id=s.task_id AND m.membership_generation=s.membership_generation WHERE s.environment_id='env' AND s.group_id='g' AND s.oldest_ready_sequence IS NOT NULL AND m.retired_event_id IS NULL ORDER BY s.oldest_ready_sequence,s.task_id COLLATE BINARY,s.membership_generation LIMIT 1`;
      const rows = yield* sql.unsafe<{ task_hex: string }>(query);
      const expected =
        yield* sql`SELECT hex(json_extract(${JSON.stringify(second)},'$')) AS task_hex`;
      assert.strictEqual(rows[0]!.task_hex, expected[0]!.task_hex);
      const plan = yield* sql.unsafe(`EXPLAIN QUERY PLAN ${query}`);
      assert.match(JSON.stringify(plan), /work_task_ready_selection_idx/);
      assert.notMatch(JSON.stringify(plan), /TEMP B-TREE/);
      const tuples = ["z", "a", "\u0000", "\ud800", "\ue000", "\ud800\udc00"].map((taskId) => ({
        taskId,
        oldestReadySequence: 10,
      }));
      tuples.sort(compareRecordedTaskCandidates);
      const sqlTies =
        yield* sql`SELECT hex(json_extract(value,'$.taskId')) AS id FROM json_each(${JSON.stringify(tuples.toReversed())}) ORDER BY json_extract(value,'$.oldestReadySequence'),json_extract(value,'$.taskId') COLLATE BINARY`;
      for (const [i, tuple] of tuples.entries()) {
        const expectedTie =
          yield* sql`SELECT hex(json_extract(${JSON.stringify(tuple.taskId)},'$')) AS id`;
        assert.strictEqual(sqlTies[i]!.id, expectedTie[0]!.id);
      }
      assert.strictEqual(recordedTaskGroupCandidate(state, "unknown"), undefined);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);
