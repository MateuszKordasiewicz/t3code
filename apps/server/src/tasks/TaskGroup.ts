import * as NodeUtil from "node:util";
import {
  WorkTaskMembershipEventBatch,
  WorkTaskPersistedGroupControlEvent,
  WorkTaskPersistedMembershipEvent,
  type EnvironmentId,
  type WorkTaskMembershipSnapshot,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";

type ControlRecord = WorkTaskPersistedGroupControlEvent;
type RootRecord = WorkTaskPersistedMembershipEvent;
type Record = ControlRecord | RootRecord;
type Companion = ControlRecord & {
  readonly event: Extract<ControlRecord["event"], { type: "task-group.membership-changed" }>;
};
type Batch = { readonly root: RootRecord; readonly companions: readonly Companion[] };
type CurrentMember = NonNullable<WorkTaskMembershipSnapshot["currentMembership"]>;
type RetiredMember = NonNullable<RootRecord["event"]["retiredMembership"]>;
type Reference = { readonly sequence: number; readonly kind?: string; readonly taskId?: string };
type Group = {
  readonly control: ControlRecord["event"]["control"];
  readonly sequence: number;
  readonly history: HashMap.HashMap<string, ControlRecord>;
};
type Manager = {
  readonly threadId: string;
  readonly bound: ControlRecord;
  readonly retired?: ControlRecord;
};
type State = {
  readonly environmentId: EnvironmentId;
  readonly groups: HashMap.HashMap<string, Group>;
  readonly managers: HashMap.HashMap<string, Manager>;
  readonly currentThreads: HashMap.HashMap<string, string>;
  readonly tasks: HashMap.HashMap<string, WorkTaskMembershipSnapshot>;
  readonly memberships: HashMap.HashMap<
    string,
    { readonly joined: CurrentMember; readonly retired?: RetiredMember }
  >;
  readonly groupMembers: HashMap.HashMap<string, HashMap.HashMap<string, number>>;
  readonly events: HashMap.HashMap<string, Record>;
  readonly references: HashMap.HashMap<string, Reference>;
  readonly referenceSequences: HashMap.HashMap<number, string>;
  readonly observedMaximum: number | undefined;
};

export class GroupReplayConflict extends Schema.TaggedError<GroupReplayConflict>()(
  "GroupReplayConflict",
  {
    code: Schema.Literals([
      "envelope",
      "environment",
      "event_identity",
      "reference",
      "partial_batch",
      "incomplete_batch",
      "group",
      "sequence",
      "manager",
      "membership",
      "task_revision",
    ]),
    eventId: Schema.String,
  },
) {}
const conflict = (code: GroupReplayConflict["code"], eventId: string): never => {
  throw new GroupReplayConflict({ code, eventId });
};
const get = <K, V>(map: HashMap.HashMap<K, V>, key: K) =>
  Option.getOrUndefined(HashMap.get(map, key));
const key = (id: string, generation: number) => JSON.stringify([id, generation]);
const decodeControl = Schema.decodeUnknownSync(WorkTaskPersistedGroupControlEvent);
const decodeRoot = Schema.decodeUnknownSync(WorkTaskPersistedMembershipEvent);
const decodeBatch = Schema.decodeUnknownSync(WorkTaskMembershipEventBatch);

export const emptyTaskGroupControl = (environmentId: EnvironmentId): State => ({
  environmentId,
  groups: HashMap.empty(),
  managers: HashMap.empty(),
  currentThreads: HashMap.empty(),
  tasks: HashMap.empty(),
  memberships: HashMap.empty(),
  groupMembers: HashMap.empty(),
  events: HashMap.empty(),
  references: HashMap.empty(),
  referenceSequences: HashMap.empty(),
  observedMaximum: undefined,
});

const preflight = (state: State, records: readonly Record[]) => {
  let known = 0;
  const ids = new Set<string>(),
    sequences = new Set<number>();
  for (const record of records) {
    const { event, context } = record;
    if (event.environmentId !== state.environmentId) conflict("environment", event.id);
    if (ids.has(event.id) || sequences.has(context.sequence)) conflict("reference", event.id);
    ids.add(event.id);
    sequences.add(context.sequence);
    const prior = get(state.events, event.id);
    if (prior) {
      if (!NodeUtil.isDeepStrictEqual(prior, record)) conflict("event_identity", event.id);
      known++;
    }
  }
  if (known > 0 && known !== records.length) conflict("partial_batch", records[0]!.event.id);
  return known === records.length;
};

const recordFacts = (state: State, records: readonly Record[]): State => {
  let events = state.events,
    references = state.references,
    referenceSequences = state.referenceSequences;
  let observedMaximum = state.observedMaximum;
  for (const record of records) {
    const { event, context } = record;
    const taskId =
      event.type === "task.membership-changed"
        ? event.snapshot.task.taskId
        : event.type === "task-group.membership-changed"
          ? event.task.taskId
          : undefined;
    const prior = get(references, event.id),
      alias = get(referenceSequences, context.sequence);
    if (
      (prior &&
        (prior.sequence !== context.sequence ||
          (prior.kind !== undefined && prior.kind !== event.type) ||
          (prior.taskId !== undefined && prior.taskId !== taskId))) ||
      (alias !== undefined && alias !== event.id)
    )
      conflict("reference", event.id);
    references = HashMap.set(references, event.id, {
      sequence: context.sequence,
      kind: event.type,
      ...(taskId === undefined ? {} : { taskId }),
    });
    referenceSequences = HashMap.set(referenceSequences, context.sequence, event.id);
    events = HashMap.set(events, event.id, record);
    observedMaximum = Math.max(observedMaximum ?? context.sequence, context.sequence);
  }
  return { ...state, events, references, referenceSequences, observedMaximum };
};

const applyControl = (state: State, record: ControlRecord): State => {
  const { event, context } = record;
  const control = event.control,
    groupId = control.group.groupId;
  const prior = get(state.groups, groupId);
  if (event.type === "task-group.created") {
    if (prior) conflict("group", event.id);
  } else {
    if (!prior || !NodeUtil.isDeepStrictEqual(prior.control, event.previousControl))
      return conflict("group", event.id);
    if (context.sequence <= prior.sequence) conflict("sequence", event.id);
  }
  let managers = state.managers,
    currentThreads = state.currentThreads;
  if (event.type === "task-group.manager-changed") {
    const previous = event.previousControl.currentManager,
      current = control.currentManager;
    if (previous !== null) {
      const managerKey = key(groupId, previous.managerGeneration),
        manager = get(managers, managerKey);
      if (
        !manager ||
        manager.retired ||
        manager.threadId !== previous.threadId ||
        get(currentThreads, previous.threadId) !== managerKey
      )
        return conflict("manager", event.id);
      managers = HashMap.set(managers, managerKey, { ...manager, retired: record });
      currentThreads = HashMap.remove(currentThreads, previous.threadId);
    }
    if (current !== null) {
      const managerKey = key(groupId, current.managerGeneration);
      if (HashMap.has(managers, managerKey) || HashMap.has(currentThreads, current.threadId))
        conflict("manager", event.id);
      managers = HashMap.set(managers, managerKey, { threadId: current.threadId, bound: record });
      currentThreads = HashMap.set(currentThreads, current.threadId, managerKey);
    }
  }
  const history = HashMap.set(
    prior?.history ?? HashMap.empty<string, ControlRecord>(),
    event.id,
    record,
  );
  return {
    ...state,
    managers,
    currentThreads,
    groups: HashMap.set(state.groups, groupId, { control, sequence: context.sequence, history }),
  };
};

export const reduceTaskGroupControl = (state: State, input: ControlRecord): State => {
  let record: ControlRecord;
  try {
    record = decodeControl(input);
  } catch {
    return conflict("envelope", input.context.eventId);
  }
  if (record.event.type === "task-group.membership-changed")
    conflict("incomplete_batch", record.event.id);
  if (preflight(state, [record])) return state;
  const candidate = recordFacts(state, [record]);
  return applyControl(candidate, record);
};

export const reduceTaskMembership = (state: State, input: Batch): State => {
  let root: RootRecord, companions: Companion[];
  try {
    root = decodeRoot(input.root);
    companions = input.companions.map((entry) => {
      const record = decodeControl(entry);
      if (record.event.type !== "task-group.membership-changed")
        return conflict("incomplete_batch", root.event.id);
      return { event: record.event, context: record.context };
    });
    decodeBatch({ event: root.event, companions: companions.map((entry) => entry.event) });
  } catch {
    return conflict("incomplete_batch", input.root.context.eventId);
  }
  const records = [root, ...companions];
  if (preflight(state, records)) return state;
  let candidate = recordFacts(state, records);
  const event = root.event,
    previous = event.previousSnapshot,
    next = event.snapshot,
    taskId = next.task.taskId;
  const prior = get(state.tasks, taskId);
  if (
    prior === undefined
      ? previous.highestMembershipGeneration !== 0 || previous.currentMembership !== null
      : prior.highestMembershipGeneration !== previous.highestMembershipGeneration ||
        !NodeUtil.isDeepStrictEqual(prior.currentMembership, previous.currentMembership)
  )
    conflict("membership", event.id);
  if (prior) {
    for (const field of [
      "definitionRevision",
      "lifecycleRevision",
      "actionPolicyRevision",
      "completionPolicyRevision",
    ] as const)
      if (previous.taskRevisions[field] < prior.taskRevisions[field])
        conflict("task_revision", event.id);
    if (
      (previous.taskRevisions.acceptedDefinitionRevision ?? 0) <
      (prior.taskRevisions.acceptedDefinitionRevision ?? 0)
    )
      conflict("task_revision", event.id);
  }
  for (const companion of companions) candidate = applyControl(candidate, companion);
  let memberships = candidate.memberships,
    groupMembers = candidate.groupMembers;
  const source = previous.currentMembership;
  if (source !== null) {
    const historyKey = key(taskId, source.membershipGeneration),
      history = get(memberships, historyKey);
    if (
      !history ||
      history.retired ||
      !NodeUtil.isDeepStrictEqual(history.joined, source) ||
      event.retiredMembership === null
    )
      return conflict("membership", event.id);
    memberships = HashMap.set(memberships, historyKey, {
      ...history,
      retired: event.retiredMembership,
    });
    const members = get(groupMembers, source.group.groupId);
    if (!members || get(members, taskId) !== source.membershipGeneration)
      return conflict("membership", event.id);
    groupMembers = HashMap.set(groupMembers, source.group.groupId, HashMap.remove(members, taskId));
  }
  const destination = next.currentMembership;
  if (destination !== null) {
    const historyKey = key(taskId, destination.membershipGeneration);
    const members = get(groupMembers, destination.group.groupId) ?? HashMap.empty<string, number>();
    if (HashMap.has(memberships, historyKey) || HashMap.has(members, taskId))
      conflict("membership", event.id);
    memberships = HashMap.set(memberships, historyKey, { joined: destination });
    groupMembers = HashMap.set(
      groupMembers,
      destination.group.groupId,
      HashMap.set(members, taskId, destination.membershipGeneration),
    );
  }
  return {
    ...candidate,
    memberships,
    groupMembers,
    tasks: HashMap.set(candidate.tasks, taskId, next),
  };
};
