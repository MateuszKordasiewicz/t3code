import * as NodeUtil from "node:util";
import {
  WorkTaskPersistedAcceptedRunOwnershipEvent,
  type WorkTaskAcceptedRunIdentityV2,
  WorkTaskAcceptedWorkClaim,
  WorkTaskPersistedCoordinationEvent,
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
type OwnerRecord = WorkTaskPersistedAcceptedRunOwnershipEvent;
type OwnerAccepted = OwnerRecord & {
  readonly event: Extract<OwnerRecord["event"], { type: "task.run-ownership-accepted" }>;
};
type OwnerTerminal = OwnerRecord & {
  readonly event: Extract<OwnerRecord["event"], { type: "task.run-ownership-terminated" }>;
};
type WorkRecord = WorkTaskPersistedCoordinationEvent & {
  readonly event: Extract<
    WorkTaskPersistedCoordinationEvent["event"],
    { type: "task-group.work-accepted" | "task-group.work-uncertain" | "task-group.work-released" }
  >;
};
type WorkAccepted = WorkRecord & {
  readonly event: Extract<WorkRecord["event"], { type: "task-group.work-accepted" }>;
};
type ReleaseFact = ReturnType<typeof reconcileTaskWorkReleaseEvidence>;
type Owner = {
  readonly record: OwnerAccepted;
  readonly latestSequence: number;
  readonly terminal?: OwnerTerminal;
};
type Claim = {
  readonly record: WorkAccepted;
  readonly latestSequence: number;
  readonly release?: ReleaseFact;
};
type ReconciliationRecord = WorkTaskPersistedCoordinationEvent & {
  readonly event: Extract<
    WorkTaskPersistedCoordinationEvent["event"],
    {
      type:
        | "task.dirty-source-recorded"
        | "task.dirty-acknowledged"
        | "task.reconciliation-progressed";
    }
  >;
};
type DirtyRecord = ReconciliationRecord & {
  readonly event: Extract<
    ReconciliationRecord["event"],
    { type: "task.dirty-source-recorded" | "task.dirty-acknowledged" }
  >;
};
type ScanRecord = ReconciliationRecord & {
  readonly event: Extract<
    ReconciliationRecord["event"],
    { type: "task.reconciliation-progressed" }
  >;
};
type ReadyRecord = WorkTaskPersistedCoordinationEvent & {
  readonly event: Extract<
    WorkTaskPersistedCoordinationEvent["event"],
    { type: "task.ready-sequence-updated" }
  >;
};
type Ready = {
  readonly record: ReadyRecord;
  readonly epoch: string | undefined;
  readonly pending: string | undefined;
  readonly served: { readonly claimId: string; readonly sequence: number } | undefined;
};
type Record =
  | ControlRecord
  | RootRecord
  | OwnerRecord
  | WorkRecord
  | ReconciliationRecord
  | ReadyRecord;
type Companion = ControlRecord & {
  readonly event: Extract<ControlRecord["event"], { type: "task-group.membership-changed" }>;
};
type Batch = { readonly root: RootRecord; readonly companions: readonly Companion[] };
type CurrentMember = NonNullable<WorkTaskMembershipSnapshot["currentMembership"]>;
type RetiredMember = NonNullable<RootRecord["event"]["retiredMembership"]>;
type Reference = {
  readonly sequence: number;
  readonly kind?: string | undefined;
  readonly taskId?: string | undefined;
};
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
  readonly ready: HashMap.HashMap<string, Ready>;
  readonly readyMaximum: HashMap.HashMap<string, number>;
  readonly consumedClaims: HashMap.HashMap<string, string>;
  readonly settledReleases: HashMap.HashMap<string, string>;
  readonly claimRanks: HashMap.HashMap<
    string,
    { readonly count: number; readonly byRank: HashMap.HashMap<number, string> }
  >;
  readonly unclosedOwners: HashMap.HashMap<string, HashMap.HashMap<string, true>>;
  readonly dirty: HashMap.HashMap<string, DirtyRecord>;
  readonly dirtySources: HashMap.HashMap<string, DirtyRecord>;
  readonly scans: HashMap.HashMap<string, ScanRecord>;
  readonly owners: HashMap.HashMap<string, Owner>;
  readonly ownerAliases: HashMap.HashMap<string, string>;
  readonly claims: HashMap.HashMap<string, Claim>;
  readonly effects: HashMap.HashMap<string, string>;
  readonly activeWork: HashMap.HashMap<string, string>;
  readonly latestWork: HashMap.HashMap<string, string>;
  readonly latestRunWork: HashMap.HashMap<string, string>;
  readonly terminalOwners: HashMap.HashMap<string, string>;
  readonly nativeEvidence: HashMap.HashMap<string, unknown>;
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
      "readiness",
      "reconciliation",
      "ownership",
      "custody",
      "phase",
      "native_evidence",
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
  ready: HashMap.empty(),
  readyMaximum: HashMap.empty(),
  consumedClaims: HashMap.empty(),
  settledReleases: HashMap.empty(),
  claimRanks: HashMap.empty(),
  unclosedOwners: HashMap.empty(),
  dirty: HashMap.empty(),
  dirtySources: HashMap.empty(),
  scans: HashMap.empty(),
  owners: HashMap.empty(),
  ownerAliases: HashMap.empty(),
  claims: HashMap.empty(),
  effects: HashMap.empty(),
  activeWork: HashMap.empty(),
  latestWork: HashMap.empty(),
  latestRunWork: HashMap.empty(),
  terminalOwners: HashMap.empty(),
  nativeEvidence: HashMap.empty(),
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

type SourceReference = readonly [string, Reference];
const recordFacts = (
  state: State,
  records: readonly Record[],
  extra: readonly SourceReference[] = [],
): State => {
  let events = state.events,
    references = state.references,
    referenceSequences = state.referenceSequences;
  let observedMaximum = state.observedMaximum;
  const own: SourceReference[] = records.map(({ event, context }) => {
    const taskId =
      "ownership" in event
        ? event.ownership.run.task.taskId
        : "task" in event
          ? event.task.taskId
          : "claim" in event
            ? event.claim.binding.task.taskId
            : event.type === "task.membership-changed"
              ? event.snapshot.task.taskId
              : undefined;
    return [
      event.id,
      { sequence: context.sequence, kind: event.type, ...(taskId === undefined ? {} : { taskId }) },
    ];
  });
  for (const [id, ref] of [...own, ...extra]) {
    const prior = get(references, id),
      alias = get(referenceSequences, ref.sequence);
    if (
      (prior &&
        (prior.sequence !== ref.sequence ||
          (prior.kind !== undefined && ref.kind !== undefined && prior.kind !== ref.kind) ||
          (prior.taskId !== undefined &&
            ref.taskId !== undefined &&
            prior.taskId !== ref.taskId))) ||
      (alias !== undefined && alias !== id)
    )
      conflict("reference", id);
    references = HashMap.set(references, id, {
      ...prior,
      ...ref,
      kind: ref.kind ?? prior?.kind,
      taskId: ref.taskId ?? prior?.taskId,
    });
    referenceSequences = HashMap.set(referenceSequences, ref.sequence, id);
  }
  for (const record of records) {
    events = HashMap.set(events, record.event.id, record);
    observedMaximum = Math.max(observedMaximum ?? record.context.sequence, record.context.sequence);
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

const nativeText = Schema.String.check(
  Schema.makeFilter((value) => value.length > 0 && value.length <= 2000 && value === value.trim()),
);
const nativeSequence = Schema.Number.check(
  Schema.isInt(),
  Schema.isGreaterThanOrEqualTo(0),
  Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER),
);
const nativeFields = {
  eventId: nativeText,
  sequence: nativeSequence,
  runId: nativeText,
  occurredAt: WorkTaskAcceptedWorkClaim.fields.acceptedAt,
};
const nativeRun = Schema.Struct({
  ...nativeFields,
  type: Schema.Literal("run.updated"),
  threadId: nativeText,
  status: Schema.Literals(["completed", "cancelled", "failed", "interrupted", "rolled_back"]),
});
const nativeAttempt = Schema.Struct({
  ...nativeFields,
  type: Schema.Literal("run-attempt.updated"),
  attemptId: nativeText,
  providerThreadId: nativeText,
  providerTurnId: Schema.NullOr(nativeText),
  status: Schema.Literals(["completed", "interrupted", "failed", "cancelled", "superseded"]),
});
const nativeInterrupt = Schema.Struct({
  ...nativeFields,
  type: Schema.Literal("turn-item.updated"),
  threadId: nativeText,
  turnItemId: nativeText,
  itemType: Schema.Literal("run_interrupt_result"),
});
const nativeRelease = Schema.Struct({
  run: nativeRun,
  attempt: Schema.optional(nativeAttempt),
  interrupt: Schema.optional(nativeInterrupt),
});
const decodeNativeRelease = Schema.decodeUnknownSync(nativeRelease, { onExcessProperty: "error" });
const decodeCoordination = Schema.decodeUnknownSync(WorkTaskPersistedCoordinationEvent);
type WorkReleaseRecord = WorkTaskPersistedCoordinationEvent & {
  readonly event: Extract<
    WorkTaskPersistedCoordinationEvent["event"],
    { type: "task-group.work-released" }
  >;
};

export const reconcileTaskWorkReleaseEvidence = (
  input: WorkReleaseRecord,
  nativeInput: unknown,
) => {
  let recorded: WorkTaskPersistedCoordinationEvent;
  let native: typeof nativeRelease.Type;
  try {
    recorded = decodeCoordination(input);
    native = decodeNativeRelease(nativeInput);
  } catch {
    return conflict("native_evidence", input.context.eventId);
  }
  if (recorded.event.type !== "task-group.work-released")
    return conflict("native_evidence", recorded.context.eventId);
  const event = recorded.event,
    evidence = event.releaseEvidence,
    claim = event.claim;
  const matches = (
    actual: {
      readonly type: string;
      readonly eventId: string;
      readonly sequence: number;
      readonly runId: string;
    },
    reference: { readonly type: string; readonly eventId: string; readonly sequence: number },
  ) =>
    actual.type === reference.type &&
    actual.eventId === reference.eventId &&
    actual.sequence === reference.sequence &&
    actual.runId === claim.binding.runId &&
    actual.sequence >= claim.acceptedSequence &&
    actual.sequence <= event.observedSequence;
  if (
    !matches(native.run, evidence.terminalRunEvent) ||
    native.run.sequence <= claim.acceptedSequence ||
    native.run.threadId !== claim.binding.manager.threadId ||
    event.observedSequence >= recorded.context.sequence
  )
    return conflict("native_evidence", event.id);
  if (evidence.type === "not_started") {
    if (
      native.attempt !== undefined ||
      native.interrupt !== undefined ||
      !["cancelled", "failed", "interrupted"].includes(native.run.status)
    )
      return conflict("native_evidence", event.id);
  } else {
    const attempt = native.attempt;
    if (
      !attempt ||
      !matches(attempt, evidence.attempt.terminalAttemptEvent) ||
      attempt.attemptId !== evidence.attempt.attemptId ||
      attempt.providerThreadId !== evidence.attempt.providerThreadId ||
      attempt.providerTurnId !== evidence.attempt.providerTurnId
    )
      return conflict("native_evidence", event.id);
    if (evidence.interruptResult) {
      const interruption = native.interrupt;
      if (
        !interruption ||
        !matches(interruption, evidence.interruptResult) ||
        interruption.threadId !== claim.binding.manager.threadId ||
        interruption.turnItemId !== evidence.interruptResult.turnItemId
      )
        return conflict("native_evidence", event.id);
    } else if (native.interrupt !== undefined) return conflict("native_evidence", event.id);
  }
  return { recorded: { ...recorded, event }, native };
};

const decodeOwnership = Schema.decodeUnknownSync(WorkTaskPersistedAcceptedRunOwnershipEvent);
const equal = NodeUtil.isDeepStrictEqual;
const ref = (
  eventId: string,
  sequence: number,
  kind?: string,
  taskId?: string,
): SourceReference => [eventId, { sequence, kind, taskId }];
const journalRef = (
  value: { readonly eventId: string; readonly sequence: number; readonly type: string },
  taskId: string,
) => ref(value.eventId, value.sequence, value.type, taskId);
const ownerReference = (owner: OwnerAccepted["event"]["ownership"]) => {
  const origin = owner.origin;
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
const ownershipRefs = (record: OwnerRecord): SourceReference[] => {
  const event = record.event,
    value = event.ownership,
    task = value.run.task.taskId,
    origin = value.origin;
  const refs: SourceReference[] = [
    ref(value.recorded.eventId, value.recorded.sequence, "task.run-ownership-accepted", task),
  ];
  refs.push(
    journalRef(
      event.type === "task.run-ownership-accepted"
        ? event.ownership.nativeAcceptance.runEvent
        : event.ownership.nativeRunEvent,
      task,
    ),
  );
  refs.push(
    origin.type === "initial_start"
      ? ref(origin.startEventId, origin.startSequence, undefined, task)
      : journalRef(origin.debitEvent, task),
  );
  if (event.type === "task.run-ownership-accepted") {
    refs.push(
      journalRef(event.ownership.nativeAcceptance.messageEvent, task),
      ref(event.ownership.sourceEventId, event.ownership.sourceSequence),
    );
    for (const witness of event.ownership.prerequisites)
      refs.push(
        ref(
          witness.completionEventId,
          witness.completionSequence,
          "task.completed",
          witness.blocker.taskId,
        ),
      );
  } else if (event.type === "task.run-ownership-uncertain")
    refs.push(ref(event.sourceEventId, event.sourceSequence));
  else {
    refs.push(journalRef(event.terminationEvidence.terminalRunEvent, task));
    if (event.terminationEvidence.kind === "after_executable_claim")
      refs.push(journalRef(event.terminationEvidence.workReleaseEvent, task));
  }
  return refs;
};
const retainedBasis = (state: State, run: WorkTaskAcceptedRunIdentityV2, id: string) => {
  const manager = get(state.managers, key(run.group.groupId, run.manager.managerGeneration));
  const member = get(state.memberships, key(run.task.taskId, run.membershipGeneration));
  if (
    !manager ||
    manager.threadId !== run.manager.threadId ||
    !member ||
    !equal(member.joined.group, run.group)
  )
    return conflict("ownership", id);
  return member.joined;
};
const originalAliases = (owner: OwnerAccepted["event"]["ownership"]) => {
  const native = owner.nativeAcceptance,
    origin = owner.origin;
  return [
    JSON.stringify(["message", owner.run.messageId]),
    JSON.stringify(["native_run", native.runEvent.eventId]),
    JSON.stringify(["native_message", native.messageEvent.eventId]),
    ...(origin.type === "initial_start"
      ? []
      : [
          JSON.stringify(["reservation", origin.reservationId]),
          JSON.stringify(["reservation_command", origin.reservationCommandId]),
          JSON.stringify(["debit", origin.debitEvent.eventId]),
          JSON.stringify(["unit", owner.run.task.taskId, origin.allowanceUnit]),
        ]),
  ];
};
export const reduceTaskRunOwnership = (state: State, input: OwnerRecord): State => {
  let record: OwnerRecord;
  try {
    record = decodeOwnership(input);
  } catch {
    return conflict("envelope", input.context.eventId);
  }
  if (preflight(state, [record])) return state;
  let candidate = recordFacts(state, [record], ownershipRefs(record));
  const event = record.event,
    runId = event.ownership.run.runId,
    owner = get(state.owners, runId);
  if (event.type === "task.run-ownership-accepted") {
    if (owner) return conflict("ownership", event.id);
    retainedBasis(state, event.ownership.run, event.id);
    let aliases = state.ownerAliases;
    for (const alias of originalAliases(event.ownership)) {
      if (HashMap.has(aliases, alias)) conflict("ownership", event.id);
      aliases = HashMap.set(aliases, alias, runId);
    }
    return {
      ...candidate,
      ownerAliases: aliases,
      unclosedOwners: HashMap.set(
        state.unclosedOwners,
        event.ownership.run.task.taskId,
        HashMap.set(
          get(state.unclosedOwners, event.ownership.run.task.taskId) ?? HashMap.empty(),
          runId,
          true,
        ),
      ),
      owners: HashMap.set(state.owners, runId, {
        record: { ...record, event },
        latestSequence: record.context.sequence,
      }),
    };
  }
  if (
    !owner ||
    owner.terminal ||
    record.context.sequence <= owner.latestSequence ||
    !equal(event.ownership, ownerReference(owner.record.event.ownership))
  )
    return conflict("ownership", event.id);
  if (event.type === "task.run-ownership-uncertain")
    return {
      ...candidate,
      owners: HashMap.set(state.owners, runId, {
        ...owner,
        latestSequence: record.context.sequence,
      }),
    };
  const evidence = event.terminationEvidence,
    latestId = get(state.latestRunWork, runId);
  if (HashMap.has(state.terminalOwners, evidence.terminalRunEvent.eventId))
    return conflict("ownership", event.id);
  if (evidence.kind === "before_executable_claim") {
    if (latestId !== undefined) return conflict("custody", event.id);
  } else {
    const latest = latestId === undefined ? undefined : get(state.claims, latestId),
      release = latest?.release;
    if (
      !latest ||
      latestId !== evidence.workClaimId ||
      !release ||
      release.recorded.event.id !== evidence.workReleaseEvent.eventId ||
      release.recorded.context.sequence !== evidence.workReleaseEvent.sequence ||
      !equal(release.recorded.event.releaseEvidence.terminalRunEvent, evidence.terminalRunEvent) ||
      release.native.run.status !== evidence.terminalStatus ||
      resources(latest.record.event.claim.binding).some(
        (resource) => get(state.activeWork, resource) === latestId,
      )
    )
      return conflict("custody", event.id);
  }
  candidate = {
    ...candidate,
    terminalOwners: HashMap.set(state.terminalOwners, evidence.terminalRunEvent.eventId, runId),
  };
  return {
    ...candidate,
    owners: HashMap.set(state.owners, runId, {
      ...owner,
      latestSequence: record.context.sequence,
      terminal: { ...record, event },
    }),
    unclosedOwners: HashMap.set(
      state.unclosedOwners,
      event.ownership.run.task.taskId,
      HashMap.remove(
        get(state.unclosedOwners, event.ownership.run.task.taskId) ?? HashMap.empty(),
        runId,
      ),
    ),
  };
};
const resources = (binding: WorkAccepted["event"]["claim"]["binding"]) => [
  JSON.stringify(["group", binding.group.groupId]),
  JSON.stringify(["task", binding.task.taskId]),
  JSON.stringify(["thread", binding.manager.threadId]),
];
const claimReference = (claim: WorkAccepted["event"]["claim"]) => ({
  binding: claim.binding,
  acceptedEventId: claim.acceptedEventId,
  acceptedSequence: claim.acceptedSequence,
  acceptedAt: claim.acceptedAt,
});
const physicalRefs = (record: WorkRecord, owner: Owner): SourceReference[] => {
  const event = record.event,
    task = event.claim.binding.task.taskId;
  const refs: SourceReference[] = [
    ref(
      event.claim.acceptedEventId,
      event.claim.acceptedSequence,
      "task-group.work-accepted",
      task,
    ),
  ];
  if (event.type === "task-group.work-accepted") {
    refs.push(
      ref(event.claim.sourceEventId, event.claim.sourceSequence),
      ref(
        owner.record.event.id,
        owner.record.context.sequence,
        "task.run-ownership-accepted",
        task,
      ),
    );
    const origin = owner.record.event.ownership.origin;
    refs.push(
      origin.type === "initial_start"
        ? ref(origin.startEventId, origin.startSequence, undefined, task)
        : journalRef(origin.debitEvent, task),
    );
    for (const witness of event.claim.prerequisites)
      refs.push(
        ref(
          witness.completionEventId,
          witness.completionSequence,
          "task.completed",
          witness.blocker.taskId,
        ),
      );
  } else if (event.type === "task-group.work-uncertain")
    refs.push(ref(event.sourceEventId, event.sourceSequence));
  else {
    refs.push(journalRef(event.releaseEvidence.terminalRunEvent, task));
    if (event.releaseEvidence.type === "run_settled") {
      refs.push(journalRef(event.releaseEvidence.attempt.terminalAttemptEvent, task));
      if (event.releaseEvidence.interruptResult)
        refs.push(journalRef(event.releaseEvidence.interruptResult, task));
    }
  }
  return refs;
};
export const reduceTaskWork = (
  state: State,
  input: WorkRecord | { readonly recorded: WorkRecord; readonly native: unknown },
): State => {
  const source = "recorded" in input ? input.recorded : input;
  let record: WorkRecord, release: ReleaseFact | undefined;
  try {
    const decoded = decodeCoordination(source);
    if (
      decoded.event.type !== "task-group.work-accepted" &&
      decoded.event.type !== "task-group.work-uncertain" &&
      decoded.event.type !== "task-group.work-released"
    )
      return conflict("envelope", source.context.eventId);
    record = { ...decoded, event: decoded.event };
    if (decoded.event.type === "task-group.work-released") {
      if (!("native" in input)) return conflict("native_evidence", source.context.eventId);
      release = reconcileTaskWorkReleaseEvidence(
        { ...decoded, event: decoded.event },
        input.native,
      );
    }
  } catch {
    return conflict("envelope", source.context.eventId);
  }
  const event = record.event,
    binding = event.claim.binding,
    existing = get(state.claims, binding.workClaimId);
  if (preflight(state, [record])) {
    if (release && !equal(existing?.release, release)) conflict("native_evidence", event.id);
    return state;
  }
  const owner = get(state.owners, binding.runId);
  if (!owner) return conflict("ownership", event.id);
  let candidate = recordFacts(state, [record], physicalRefs(record, owner));
  if (event.type === "task-group.work-accepted") {
    if (
      existing ||
      (get(state.readyMaximum, binding.task.taskId) ?? -1) > record.context.sequence ||
      HashMap.has(state.effects, binding.effectId) ||
      owner.terminal ||
      owner.record.context.sequence >= record.context.sequence
    )
      return conflict("custody", event.id);
    const original = owner.record.event.ownership;
    for (const field of [
      "version",
      "task",
      "group",
      "manager",
      "membershipGeneration",
      "projectId",
      "providerInstanceId",
      "runId",
      "messageId",
    ] as const)
      if (!equal(binding[field], original.run[field])) conflict("ownership", event.id);
    const origin = original.origin;
    const expected =
      origin.type === "initial_start"
        ? origin
        : {
            type: origin.type,
            reservationId: origin.reservationId,
            reservationCommandId: origin.reservationCommandId,
            allowanceUnit: origin.allowanceUnit,
            observedDirtyGeneration: origin.observedDirtyGeneration,
            acknowledgedGeneration: origin.acknowledgedGeneration,
          };
    if (!equal(event.claim.launchOrigin, expected)) return conflict("ownership", event.id);
    const member = retainedBasis(state, original.run, event.id);
    if (
      !equal(member, event.membership.currentMembership) ||
      member.acceptedDefinitionRevisionAtJoin > binding.taskRevisions.acceptedDefinitionRevision ||
      event.membership.highestMembershipGeneration !== binding.membershipGeneration
    )
      return conflict("membership", event.id);
    let active = state.activeWork,
      latest = state.latestWork;
    for (const resource of resources(binding)) {
      const previousId = get(latest, resource),
        previous = previousId === undefined ? undefined : get(state.claims, previousId);
      if (
        HashMap.has(active, resource) ||
        (previous &&
          (!previous.release ||
            previous.release.recorded.context.sequence >= record.context.sequence ||
            previous.record.context.sequence >= record.context.sequence))
      )
        return conflict("custody", event.id);
      active = HashMap.set(active, resource, binding.workClaimId);
      latest = HashMap.set(latest, resource, binding.workClaimId);
    }
    const ranks = get(state.claimRanks, binding.task.taskId) ?? {
      count: 0,
      byRank: HashMap.empty<number, string>(),
    };
    return {
      ...candidate,
      claimRanks: HashMap.set(state.claimRanks, binding.task.taskId, {
        count: ranks.count + 1,
        byRank: HashMap.set(ranks.byRank, ranks.count, binding.workClaimId),
      }),
      activeWork: active,
      latestWork: latest,
      latestRunWork: HashMap.set(state.latestRunWork, binding.runId, binding.workClaimId),
      effects: HashMap.set(state.effects, binding.effectId, binding.workClaimId),
      claims: HashMap.set(state.claims, binding.workClaimId, {
        record: { ...record, event },
        latestSequence: record.context.sequence,
      }),
    };
  }
  if (
    !existing ||
    existing.release ||
    record.context.sequence <= existing.latestSequence ||
    !equal(event.claim, claimReference(existing.record.event.claim))
  )
    return conflict("phase", event.id);
  if (event.type === "task-group.work-uncertain")
    return {
      ...candidate,
      claims: HashMap.set(state.claims, binding.workClaimId, {
        ...existing,
        latestSequence: record.context.sequence,
      }),
    };
  if (!release) return conflict("native_evidence", event.id);
  let active = state.activeWork,
    native = state.nativeEvidence;
  for (const value of [release.native.run, release.native.attempt, release.native.interrupt])
    if (value) {
      const prior = get(native, value.eventId);
      if (prior && !equal(prior, value)) conflict("native_evidence", event.id);
      native = HashMap.set(native, value.eventId, value);
    }
  for (const resource of resources(binding)) {
    if (get(active, resource) !== binding.workClaimId) conflict("custody", event.id);
    active = HashMap.remove(active, resource);
  }
  candidate = { ...candidate, activeWork: active, nativeEvidence: native };
  return {
    ...candidate,
    claims: HashMap.set(state.claims, binding.workClaimId, {
      ...existing,
      latestSequence: record.context.sequence,
      release,
    }),
  };
};

export const compareTaskIdentifiers = (left: string, right: string) => {
  let a = 0,
    b = 0;
  while (a < left.length && b < right.length) {
    const x = left.codePointAt(a)!,
      y = right.codePointAt(b)!;
    if (x !== y) return x < y ? -1 : 1;
    a += x > 0xffff ? 2 : 1;
    b += y > 0xffff ? 2 : 1;
  }
  return a < left.length ? 1 : b < right.length ? -1 : 0;
};
const sourceKey = (task: string, source: string) => JSON.stringify([task, source]);
const retainedMembership = (
  state: State,
  task: string,
  generation: number,
  group: string,
  eventId: string,
) => {
  const member = get(state.memberships, key(task, generation));
  if (!member || member.joined.group.groupId !== group) conflict("membership", eventId);
};
const scanTasks = (event: ScanRecord["event"]) => {
  const tasks: string[] = [];
  for (const row of event.visited) tasks.push("task" in row ? row.task.taskId : row.taskId);
  return tasks;
};
const scanPosition = (position: ScanRecord["event"]["position"]) =>
  position === null
    ? null
    : "taskId" in position
      ? [position.taskId]
      : [position.dependentTaskId, position.edgeId];
const comparePosition = (a: readonly string[], b: readonly string[]) =>
  compareTaskIdentifiers(a[0]!, b[0]!) || compareTaskIdentifiers(a[1] ?? "", b[1] ?? "");
export const reduceTaskReconciliation = (
  state: State,
  inputs: readonly ReconciliationRecord[],
): State => {
  let candidate = state;
  const visits: ScanRecord[] = [];
  for (const input of inputs) {
    let record: ReconciliationRecord;
    try {
      const decoded = decodeCoordination(input);
      if (
        decoded.event.type !== "task.dirty-source-recorded" &&
        decoded.event.type !== "task.dirty-acknowledged" &&
        decoded.event.type !== "task.reconciliation-progressed"
      )
        return conflict("envelope", input.context.eventId);
      record = { ...decoded, event: decoded.event };
    } catch {
      return conflict("envelope", input.context.eventId);
    }
    if (preflight(candidate, [record])) continue;
    const event = record.event;
    if (event.type !== "task.reconciliation-progressed") {
      const task = event.task.taskId,
        prior = get(candidate.dirty, task),
        currentDirty = prior?.event.dirtyGeneration ?? 0,
        currentAck = prior?.event.acknowledgedGeneration ?? 0;
      if (prior && prior.context.sequence >= record.context.sequence)
        conflict("sequence", event.id);
      if (event.membership !== null)
        retainedMembership(
          candidate,
          task,
          event.membership.membershipGeneration,
          event.membership.group.groupId,
          event.id,
        );
      const source = sourceKey(task, event.sourceEventId);
      if (event.type === "task.dirty-source-recorded") {
        if (
          event.previousDirtyGeneration !== currentDirty ||
          event.acknowledgedGeneration !== currentAck ||
          HashMap.has(candidate.dirtySources, source)
        )
          conflict("reconciliation", event.id);
      } else if (
        event.previousAcknowledgedGeneration !== currentAck ||
        event.dirtyGeneration !== currentDirty
      )
        conflict("reconciliation", event.id);
      candidate = recordFacts(
        candidate,
        [record],
        [ref(event.sourceEventId, event.sourceSequence)],
      );
      const narrowed = { ...record, event };
      candidate = {
        ...candidate,
        dirty: HashMap.set(candidate.dirty, task, narrowed),
        dirtySources:
          event.type === "task.dirty-source-recorded"
            ? HashMap.set(candidate.dirtySources, source, narrowed)
            : candidate.dirtySources,
      };
      continue;
    }
    const scan = event.scan,
      target = "group" in scan ? scan.group.groupId : scan.blocker.taskId;
    const job = JSON.stringify([scan.kind, event.environmentId, target, scan.sourceEventId]),
      prior = get(candidate.scans, job);
    if (
      prior &&
      (prior.context.sequence >= record.context.sequence ||
        prior.event.completed ||
        !equal(prior.event.scan, scan) ||
        prior.event.observedRevision > event.observedRevision)
    )
      conflict("reconciliation", event.id);
    if (!equal(prior?.event.position ?? null, event.previousPosition))
      conflict("reconciliation", event.id);
    const tasks = scanTasks(event),
      previous = scanPosition(event.previousPosition),
      position = scanPosition(event.position);
    for (let i = 1; i < tasks.length; i++)
      if (compareTaskIdentifiers(tasks[i - 1]!, tasks[i]!) >= 0)
        conflict("reconciliation", event.id);
    if (
      tasks.length &&
      previous &&
      (!position ||
        comparePosition(position, previous) <= 0 ||
        compareTaskIdentifiers(tasks[0]!, previous[0]!) < (scan.kind === "group_members" ? 1 : 0))
    )
      conflict("reconciliation", event.id);
    if ("group" in scan) {
      if (!HashMap.has(candidate.groups, scan.group.groupId)) conflict("group", event.id);
      for (const row of event.visited) {
        if (!("task" in row)) return conflict("envelope", event.id);
        retainedMembership(
          candidate,
          row.task.taskId,
          row.membershipGeneration,
          scan.group.groupId,
          event.id,
        );
      }
    }
    const narrowed = { ...record, event };
    candidate = recordFacts(candidate, [record], [ref(scan.sourceEventId, scan.sourceSequence)]);
    candidate = { ...candidate, scans: HashMap.set(candidate.scans, job, narrowed) };
    visits.push(narrowed);
  }
  for (const { event } of visits)
    for (const task of scanTasks(event)) {
      const source = get(candidate.dirtySources, sourceKey(task, event.scan.sourceEventId));
      if (!source || source.event.sourceSequence !== event.scan.sourceSequence)
        conflict("reconciliation", event.id);
    }
  return candidate;
};

const predecessorClaim = (state: State, task: string, sequence: number) => {
  const ranks = get(state.claimRanks, task);
  let low = 0,
    high = ranks?.count ?? 0;
  while (low < high) {
    const middle = Math.floor((low + high) / 2),
      claim = get(state.claims, get(ranks!.byRank, middle)!)!;
    if (claim.record.context.sequence < sequence) low = middle + 1;
    else high = middle;
  }
  return low === 0 ? undefined : get(state.claims, get(ranks!.byRank, low - 1)!);
};
export const reduceTaskReadiness = (state: State, input: ReadyRecord): State => {
  let record: ReadyRecord;
  try {
    const decoded = decodeCoordination(input);
    if (decoded.event.type !== "task.ready-sequence-updated")
      return conflict("envelope", input.context.eventId);
    record = { ...decoded, event: decoded.event };
  } catch {
    return conflict("envelope", input.context.eventId);
  }
  if (preflight(state, [record])) return state;
  const event = record.event,
    task = event.task.taskId,
    id = key(task, event.membershipGeneration),
    prior = get(state.ready, id);
  retainedMembership(state, task, event.membershipGeneration, event.group.groupId, event.id);
  if (
    event.basis.managerGeneration !== null &&
    !HashMap.has(state.managers, key(event.group.groupId, event.basis.managerGeneration))
  )
    conflict("manager", event.id);
  if (
    (prior && prior.record.context.sequence >= record.context.sequence) ||
    (prior?.record.event.oldestReadySequence ?? null) !== event.previousOldestReadySequence
  )
    conflict("readiness", event.id);
  const predecessor = predecessorClaim(state, task, record.context.sequence);
  const occupied =
    predecessor !== undefined &&
    (!predecessor.release ||
      predecessor.release.recorded.context.sequence >= record.context.sequence);
  let pending = prior?.pending,
    served = prior?.served,
    epoch = prior?.epoch,
    consumed = state.consumedClaims,
    settled = state.settledReleases;
  const refs: SourceReference[] = [ref(event.sourceEventId, event.sourceSequence)];
  if ((event.kind === "qualified" || event.kind === "accepted") && pending !== undefined)
    conflict("readiness", event.id);
  if ((event.kind === "qualified" || event.kind === "settled") && occupied)
    conflict("custody", event.id);
  if (event.kind === "accepted" || event.kind === "settled") {
    const claim = get(state.claims, event.claim.binding.workClaimId);
    if (
      !claim ||
      claim !== predecessor ||
      !equal(event.claim, claimReference(claim.record.event.claim))
    )
      return conflict("readiness", event.id);
    const binding = claim.record.event.claim.binding,
      owner = get(state.owners, binding.runId)!;
    refs.push(
      ref(claim.record.event.id, claim.record.context.sequence, "task-group.work-accepted", task),
      ref(
        owner.record.event.id,
        owner.record.context.sequence,
        "task.run-ownership-accepted",
        task,
      ),
    );
    if (event.kind === "accepted") {
      const basis = {
        taskRevisions: binding.taskRevisions,
        graphRevision: binding.graphRevision,
        controlRevision: binding.controlRevision,
        membershipRevision: binding.membershipRevision,
        managerGeneration: binding.manager.managerGeneration,
      };
      if (!equal(event.basis, basis) || HashMap.has(consumed, binding.workClaimId))
        conflict("readiness", event.id);
      consumed = HashMap.set(consumed, binding.workClaimId, event.id);
      pending = binding.workClaimId;
      epoch = undefined;
    } else {
      const release = claim.release;
      if (
        !release ||
        release.recorded.context.sequence >= record.context.sequence ||
        release.recorded.event.id !== event.releaseEventId ||
        release.recorded.context.sequence !== event.releaseSequence ||
        (pending !== undefined && pending !== binding.workClaimId) ||
        HashMap.has(settled, event.releaseEventId)
      )
        return conflict("readiness", event.id);
      settled = HashMap.set(settled, event.releaseEventId, event.id);
      refs.push(ref(event.releaseEventId, event.releaseSequence, "task-group.work-released", task));
      pending = undefined;
      epoch = event.id;
    }
    served = { claimId: binding.workClaimId, sequence: claim.record.context.sequence };
  } else if (event.kind === "qualified")
    epoch = event.previousOldestReadySequence === null ? event.id : epoch;
  else epoch = undefined;
  const candidate = recordFacts(state, [record], refs);
  return {
    ...candidate,
    ready: HashMap.set(state.ready, id, { record, epoch, pending, served }),
    consumedClaims: consumed,
    settledReleases: settled,
    readyMaximum:
      event.kind === "disqualified"
        ? state.readyMaximum
        : HashMap.set(
            state.readyMaximum,
            task,
            Math.max(get(state.readyMaximum, task) ?? -1, record.context.sequence),
          ),
  };
};
export const compareRecordedTaskCandidates = (
  left: { readonly oldestReadySequence: number; readonly taskId: string },
  right: { readonly oldestReadySequence: number; readonly taskId: string },
) =>
  left.oldestReadySequence - right.oldestReadySequence ||
  compareTaskIdentifiers(left.taskId, right.taskId);
export const recordedTaskGroupCandidate = (state: State, groupId: string) => {
  const group = get(state.groups, groupId),
    members = get(state.groupMembers, groupId);
  if (!group || !members) return undefined;
  let selected:
    | { taskId: string; generation: number; ready: Ready; oldestReadySequence: number }
    | undefined;
  for (const [taskId, generation] of members) {
    const ready = get(state.ready, key(taskId, generation)),
      oldestReadySequence = ready?.record.event.oldestReadySequence;
    if (
      !ready ||
      oldestReadySequence === undefined ||
      oldestReadySequence === null ||
      ready.record.event.group.groupId !== groupId
    )
      continue;
    const candidate = { taskId, generation, ready, oldestReadySequence };
    if (!selected || compareRecordedTaskCandidates(candidate, selected) < 0) selected = candidate;
  }
  if (!selected) return undefined;
  const resourceKeys = [
    JSON.stringify(["task", selected.taskId]),
    JSON.stringify(["group", groupId]),
    ...(group.control.currentManager
      ? [JSON.stringify(["thread", group.control.currentManager.threadId])]
      : []),
  ];
  const physical = new Map<string, Claim>();
  for (const resource of resourceKeys) {
    const claimId = get(state.activeWork, resource);
    if (claimId !== undefined) physical.set(claimId, get(state.claims, claimId)!);
  }
  return {
    ...selected,
    control: group.control,
    physicalClaims: [...physical.values()],
    unclosedOwners: [
      ...(get(state.unclosedOwners, selected.taskId) ?? HashMap.empty<string, true>()),
    ].map(([run]) => get(state.owners, run)!),
  };
};
type GroupInput =
  | Record
  | Batch
  | { readonly recorded: WorkRecord; readonly native: unknown }
  | readonly ReconciliationRecord[];
const isReconciliationBatch = (input: GroupInput): input is readonly ReconciliationRecord[] =>
  Array.isArray(input);
export const reduceTaskGroup = (state: State, input: GroupInput): State => {
  if (isReconciliationBatch(input)) return reduceTaskReconciliation(state, input);
  if ("root" in input) return reduceTaskMembership(state, input);
  if ("recorded" in input) return reduceTaskWork(state, input);
  const event = input.event;
  switch (event.type) {
    case "task-group.created":
    case "task-group.manager-changed":
    case "task-group.hold-changed":
      return reduceTaskGroupControl(state, { ...input, event });
    case "task.membership-changed":
    case "task-group.membership-changed":
      return conflict("incomplete_batch", event.id);
    case "task.run-ownership-accepted":
    case "task.run-ownership-uncertain":
    case "task.run-ownership-terminated":
      return reduceTaskRunOwnership(state, { ...input, event });
    case "task-group.work-accepted":
    case "task-group.work-uncertain":
    case "task-group.work-released":
      return reduceTaskWork(state, { ...input, event });
    case "task.dirty-source-recorded":
    case "task.dirty-acknowledged":
    case "task.reconciliation-progressed":
      return reduceTaskReconciliation(state, [{ ...input, event }]);
    case "task.ready-sequence-updated":
      return reduceTaskReadiness(state, { ...input, event });
    default: {
      const unsupported: never = event;
      return conflict("envelope", String(unsupported));
    }
  }
};
