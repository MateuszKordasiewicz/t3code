import * as NodeUtil from "node:util";
import type { WorkTaskEvent, WorkTaskPersistedEventContext } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";
import * as Chunk from "effect/Chunk";

type FoundationType =
  | "task.created"
  | "task.definition-updated"
  | "task.definition-accepted"
  | "task.thread-linked"
  | "task.thread-unlinked"
  | "task.configuration-updated"
  | "task.execution-updated"
  | "task.wait-updated"
  | "task.result-recorded"
  | "task.continuation-accepted"
  | "task.completion-submitted"
  | "task.completed"
  | "task.reopened"
  | "task.archived";

type FoundationEvent = Extract<WorkTaskEvent, { readonly type: FoundationType }>;
type EventOf<T extends FoundationType> = Extract<FoundationEvent, { readonly type: T }>;
type Recorded<E extends FoundationEvent = FoundationEvent> = {
  readonly event: E;
  readonly context: WorkTaskPersistedEventContext;
};
type LinkEvent = EventOf<"task.thread-linked" | "task.thread-unlinked">;
type Link = { readonly linked: Recorded<LinkEvent>; readonly unlinked?: Recorded<LinkEvent> };
type Wait = Recorded<EventOf<"task.wait-updated">>;
type Claim = Recorded<EventOf<"task.completion-submitted">>;

type FoundationState = {
  readonly taskId: FoundationEvent["taskId"];
  readonly created: Recorded<EventOf<"task.created">>;
  readonly draft: Recorded<EventOf<"task.created" | "task.definition-updated">>;
  readonly definition: Recorded<
    EventOf<"task.created" | "task.definition-updated" | "task.definition-accepted">
  >;
  readonly acceptedDefinitions: Chunk.Chunk<Recorded<EventOf<"task.definition-accepted">>>;
  readonly acceptedByRevision: HashMap.HashMap<
    number,
    Recorded<EventOf<"task.definition-accepted">>
  >;
  readonly acceptanceCommands: HashMap.HashMap<
    string,
    Recorded<EventOf<"task.definition-accepted">>
  >;
  readonly policies: HashMap.HashMap<
    number,
    EventOf<"task.configuration-updated">["completionPolicy"]
  >;
  readonly managers: HashMap.HashMap<number, string>;
  readonly highestManagerGeneration: number | undefined;
  readonly revisions: Pick<
    FoundationEvent,
    "definitionRevision" | "lifecycleRevision" | "completionPolicyRevision" | "actionPolicyRevision"
  >;
  readonly configuration?: Recorded<EventOf<"task.configuration-updated">>;
  readonly execution?: Recorded<EventOf<"task.execution-updated">>;
  readonly links: HashMap.HashMap<string, Link>;
  readonly activeLinks: HashMap.HashMap<string, string>;
  readonly waits: HashMap.HashMap<string, Wait>;
  readonly results: Chunk.Chunk<Recorded<EventOf<"task.result-recorded">>>;
  readonly continuations: Chunk.Chunk<Recorded<EventOf<"task.continuation-accepted">>>;
  readonly launches: HashMap.HashMap<string, EventOf<"task.continuation-accepted">>;
  readonly claims: Chunk.Chunk<Claim>;
  readonly claimIds: HashMap.HashMap<string, Claim>;
  readonly evidenceIds: HashMap.HashMap<string, Claim>;
  readonly completion?: Recorded<EventOf<"task.completed" | "task.reopened">>;
  readonly archive?: Recorded<EventOf<"task.archived">>;
  readonly events: HashMap.HashMap<string, Recorded>;
  readonly sequences: HashMap.HashMap<number, Recorded>;
  readonly lastSequence: number;
};

export class FoundationReplayConflict extends Schema.TaggedError<FoundationReplayConflict>()(
  "FoundationReplayConflict",
  {
    code: Schema.Literals([
      "envelope",
      "task_identity",
      "event_identity",
      "sequence",
      "revision",
      "definition",
      "link",
      "claim",
      "completion",
      "creation",
      "unsupported",
      "policy",
      "manager",
      "wait",
      "continuation",
      "command",
    ]),
    eventId: Schema.String,
  },
) {}

const conflict = (
  code: FoundationReplayConflict["code"],
  event: { readonly id: string },
): never => {
  throw new FoundationReplayConflict({ code, eventId: event.id });
};

const lookup = <K, V>(map: HashMap.HashMap<K, V>, key: K) =>
  Option.getOrUndefined(HashMap.get(map, key));
const launchIdentity = (event: EventOf<"task.continuation-accepted">) => ({
  taskId: event.taskId,
  managerThreadId: event.managerThreadId,
  managerGeneration: event.managerGeneration,
  runId: event.runId,
  messageId: event.messageId,
  effectId: event.effectId,
});

/** Reduces only the foundation event family; other task event families must be composed separately. */
export function reduceTaskFoundation(
  state: FoundationState | undefined,
  recorded: Recorded,
): FoundationState {
  const { event, context } = recorded;
  if (
    event.id !== context.eventId ||
    event.occurredAt !== context.occurredAt ||
    (event.type === "task.definition-accepted" && context.commandId === null)
  )
    conflict("envelope", event);
  if (!state) {
    if (event.type !== "task.created") return conflict("creation", event);
    if (event.definitionRevision !== event.definition.revision)
      return conflict("definition", event);
    const created = { event, context };
    return {
      taskId: event.taskId,
      created,
      draft: created,
      definition: created,
      acceptedDefinitions: Chunk.empty(),
      acceptedByRevision: HashMap.empty(),
      acceptanceCommands: HashMap.empty(),
      policies: HashMap.empty(),
      managers: HashMap.empty(),
      highestManagerGeneration: undefined,
      revisions: {
        definitionRevision: event.definitionRevision,
        lifecycleRevision: event.lifecycleRevision,
        completionPolicyRevision: event.completionPolicyRevision,
        actionPolicyRevision: event.actionPolicyRevision,
      },
      links: HashMap.empty(),
      activeLinks: HashMap.empty(),
      waits: HashMap.empty(),
      results: Chunk.empty(),
      continuations: Chunk.empty(),
      launches: HashMap.empty(),
      claims: Chunk.empty(),
      claimIds: HashMap.empty(),
      evidenceIds: HashMap.empty(),
      events: HashMap.set(HashMap.empty<string, Recorded>(), event.id, recorded),
      sequences: HashMap.set(HashMap.empty<number, Recorded>(), context.sequence, recorded),
      lastSequence: context.sequence,
    };
  }
  if (event.taskId !== state.taskId) return conflict("task_identity", event);
  const existing = lookup(state.events, event.id) ?? lookup(state.sequences, context.sequence);
  if (existing) {
    if (NodeUtil.isDeepStrictEqual(existing, recorded)) return state;
    return conflict(existing.event.id === event.id ? "event_identity" : "sequence", event);
  }
  if (context.sequence <= state.lastSequence) return conflict("sequence", event);
  for (const key of [
    "definitionRevision",
    "lifecycleRevision",
    "completionPolicyRevision",
    "actionPolicyRevision",
  ] as const) {
    if (event[key] < state.revisions[key]) return conflict("revision", event);
  }
  const manager =
    event.type === "task.execution-updated"
      ? event.manager
      : "managerThreadId" in event
        ? event
        : null;
  if (manager) {
    const known = lookup(state.managers, manager.managerGeneration);
    if (
      (known !== undefined && known !== manager.managerThreadId) ||
      (event.type === "task.execution-updated" &&
        state.highestManagerGeneration !== undefined &&
        manager.managerGeneration < state.highestManagerGeneration)
    )
      conflict("manager", event);
  }
  const next = {
    ...state,
    revisions: {
      definitionRevision: event.definitionRevision,
      lifecycleRevision: event.lifecycleRevision,
      completionPolicyRevision: event.completionPolicyRevision,
      actionPolicyRevision: event.actionPolicyRevision,
    },
    events: HashMap.set(state.events, event.id, recorded),
    sequences: HashMap.set(state.sequences, context.sequence, recorded),
    lastSequence: context.sequence,
    managers: manager
      ? HashMap.set(state.managers, manager.managerGeneration, manager.managerThreadId)
      : state.managers,
    highestManagerGeneration: manager
      ? Math.max(state.highestManagerGeneration ?? 0, manager.managerGeneration)
      : state.highestManagerGeneration,
  };
  switch (event.type) {
    case "task.created":
      return conflict("creation", event);
    case "task.definition-updated": {
      if (event.definitionRevision !== event.definition.revision)
        return conflict("definition", event);
      const previous = state.draft.event.definition;
      const accepted = lookup(state.acceptedByRevision, event.definitionRevision);
      if (
        (previous.revision === event.definitionRevision &&
          !NodeUtil.isDeepStrictEqual(previous, event.definition)) ||
        (accepted && !NodeUtil.isDeepStrictEqual(accepted.event.definition, event.definition))
      )
        return conflict("definition", event);
      return { ...next, draft: { event, context }, definition: { event, context } };
    }
    case "task.definition-accepted": {
      const command = context.commandId;
      if (command === null) return conflict("envelope", event);
      if (HashMap.has(state.acceptanceCommands, command)) return conflict("command", event);

      if (
        event.definitionRevision !== event.definition.revision ||
        HashMap.has(state.acceptedByRevision, event.definitionRevision)
      )
        return conflict("definition", event);
      if (
        state.draft.event.definition.revision === event.definitionRevision &&
        !NodeUtil.isDeepStrictEqual(state.draft.event.definition, event.definition)
      )
        return conflict("definition", event);
      return {
        ...next,
        definition: { event, context },
        acceptedDefinitions: Chunk.append(state.acceptedDefinitions, { event, context }),
        acceptedByRevision: HashMap.set(state.acceptedByRevision, event.definitionRevision, {
          event,
          context,
        }),
        acceptanceCommands: HashMap.set(state.acceptanceCommands, command, { event, context }),
      };
    }
    case "task.configuration-updated": {
      const policy = lookup(state.policies, event.completionPolicyRevision);
      if (policy && !NodeUtil.isDeepStrictEqual(policy, event.completionPolicy))
        return conflict("policy", event);
      return {
        ...next,
        configuration: { event, context },
        policies: HashMap.set(
          state.policies,
          event.completionPolicyRevision,
          event.completionPolicy,
        ),
      };
    }
    case "task.execution-updated":
      return { ...next, execution: { event, context } };
    case "task.thread-linked": {
      if (HashMap.has(state.activeLinks, event.threadId)) return conflict("link", event);
      return {
        ...next,
        links: HashMap.set(state.links, event.id, { linked: { event, context } }),
        activeLinks: HashMap.set(state.activeLinks, event.threadId, event.id),
      };
    }
    case "task.thread-unlinked": {
      const key = lookup(state.activeLinks, event.threadId);
      if (key === undefined) return conflict("link", event);
      const link = HashMap.getUnsafe(state.links, key);
      return {
        ...next,
        links: HashMap.set(state.links, key, { ...link, unlinked: { event, context } }),
        activeLinks: HashMap.remove(state.activeLinks, event.threadId),
      };
    }
    case "task.wait-updated": {
      const wait = lookup(state.waits, event.waitId);
      if (
        (wait && !NodeUtil.isDeepStrictEqual(wait.event.target, event.target)) ||
        (!wait && event.state === "cleared")
      )
        return conflict("wait", event);
      return { ...next, waits: HashMap.set(state.waits, event.waitId, { event, context }) };
    }
    case "task.result-recorded":
      return { ...next, results: Chunk.append(state.results, { event, context }) };
    case "task.continuation-accepted": {
      const keys = [`run:${event.runId}`, `message:${event.messageId}`, `effect:${event.effectId}`];
      for (const key of keys) {
        const launch = lookup(state.launches, key);
        if (launch && !NodeUtil.isDeepStrictEqual(launchIdentity(launch), launchIdentity(event)))
          return conflict("continuation", event);
      }
      return {
        ...next,
        continuations: Chunk.append(state.continuations, { event, context }),
        launches: keys.reduce((map, key) => HashMap.set(map, key, event), state.launches),
      };
    }
    case "task.completion-submitted": {
      if (
        !HashMap.has(state.acceptedByRevision, event.definitionRevision) ||
        HashMap.has(state.claimIds, event.claimId) ||
        HashMap.has(state.evidenceIds, event.evidenceId)
      )
        return conflict("claim", event);
      return {
        ...next,
        claims: Chunk.append(state.claims, { event, context }),
        claimIds: HashMap.set(state.claimIds, event.claimId, { event, context }),
        evidenceIds: HashMap.set(state.evidenceIds, event.evidenceId, { event, context }),
      };
    }
    case "task.completed": {
      const claim =
        lookup(state.claimIds, event.claimId) ?? lookup(state.evidenceIds, event.evidenceId);
      if (
        event.attribution.type === "manager" ||
        event.attribution.type === "agent" ||
        (claim !== undefined &&
          (claim.event.claimId !== event.claimId ||
            claim.event.evidenceId !== event.evidenceId ||
            claim.event.managerThreadId !== event.managerThreadId ||
            claim.event.managerGeneration !== event.managerGeneration ||
            claim.event.observedWorkSequence !== event.observedWorkSequence ||
            claim.event.definitionRevision !== event.definitionRevision ||
            claim.event.completionPolicyRevision !== event.completionPolicyRevision))
      )
        return conflict("completion", event);
      return { ...next, completion: { event, context } };
    }
    case "task.reopened":
      return { ...next, completion: { event, context } };
    case "task.archived":
      return { ...next, archive: { event, context } };
    default:
      return conflict("unsupported", event);
  }
}

type CustodyType =
  | "task.manager-bound"
  | "task.manager-retired"
  | "task.allowance-configured"
  | "task.allowance-topped-up"
  | "task.allowance-debited"
  | "task.wake-source-recorded"
  | "task.wake-reserved"
  | "task.wake-claimed"
  | "task.wake-claim-uncertain"
  | "task.wake-acknowledged";
type CustodyEvent = Extract<WorkTaskEvent, { readonly type: CustodyType }>;
type CustodyOf<T extends CustodyType> = Extract<CustodyEvent, { readonly type: T }>;
type CustodyRecorded = {
  readonly event: CustodyEvent;
  readonly context: WorkTaskPersistedEventContext;
};
type ManagerBinding = {
  readonly bound: CustodyOf<"task.manager-bound">;
  readonly retired?: CustodyOf<"task.manager-retired">;
};
type WakeReservation = {
  readonly reserved: CustodyOf<"task.wake-reserved">;
  readonly claimed?: CustodyOf<"task.wake-claimed">;
  readonly uncertain?: CustodyOf<"task.wake-claim-uncertain">;
  readonly acknowledged?: CustodyOf<"task.wake-acknowledged">;
};
type SourceRecord = CustodyOf<"task.wake-source-recorded">;
type CustodyState = {
  readonly taskId: CustodyEvent["taskId"];
  readonly managers: HashMap.HashMap<number, ManagerBinding>;
  readonly threads: HashMap.HashMap<string, number>;
  readonly currentManagerGeneration: number | undefined;
  readonly highestManagerGeneration: number;
  readonly allowance: CustodyOf<"task.allowance-configured">["previous"];
  readonly allowanceCommands: HashMap.HashMap<string, CustodyRecorded>;
  readonly debits: HashMap.HashMap<number, CustodyOf<"task.allowance-debited">>;
  readonly reservations: HashMap.HashMap<string, WakeReservation>;
  readonly reservationCommands: HashMap.HashMap<string, string>;
  readonly units: HashMap.HashMap<number, string>;
  readonly pendingReservations: HashMap.HashMap<number, string>;
  readonly launches: HashMap.HashMap<string, string>;
  readonly sources: HashMap.HashMap<string, SourceRecord>;
  readonly pendingSources: HashMap.HashMap<string, SourceRecord>;
  readonly overflowThroughGeneration: number | null;
  readonly dirtyGeneration: number;
  readonly acknowledgedGeneration: number;
  readonly events: HashMap.HashMap<string, CustodyRecorded>;
  readonly sequences: HashMap.HashMap<number, CustodyRecorded>;
  readonly lastSequence: number;
  readonly revisions: FoundationState["revisions"];
};

export class CustodyReplayConflict extends Schema.TaggedError<CustodyReplayConflict>()(
  "CustodyReplayConflict",
  {
    code: Schema.Literals([
      "envelope",
      "task_identity",
      "event_identity",
      "sequence",
      "revision",
      "manager",
      "allowance",
      "command",
      "unit",
      "source",
      "reservation",
      "phase",
      "generation",
      "acknowledgement",
      "unsupported",
    ]),
    eventId: Schema.String,
  },
) {}

const custodyConflict = (code: CustodyReplayConflict["code"], event: CustodyEvent): never => {
  throw new CustodyReplayConflict({ code, eventId: event.id });
};
const sourceKey = (source: SourceRecord["source"]) =>
  JSON.stringify([source.kind, source.sourceId]);
const wakeIdentity = (
  event: CustodyOf<
    | "task.wake-reserved"
    | "task.wake-claimed"
    | "task.wake-claim-uncertain"
    | "task.wake-acknowledged"
  >,
) => ({
  reservationId: event.reservationId,
  reservationCommandId: event.reservationCommandId,
  managerGeneration: event.managerGeneration,
  allowanceUnit: event.allowanceUnit,
});

/** Reduces custody facts only; execution eligibility and cross-family composition remain separate. */
export function reduceTaskCustody(
  state: CustodyState | undefined,
  recorded: CustodyRecorded,
): CustodyState {
  const { event, context } = recorded;
  const fail = (code: CustodyReplayConflict["code"]): never => custodyConflict(code, event);
  if (
    event.id !== context.eventId ||
    event.occurredAt !== context.occurredAt ||
    ("commandId" in event && event.commandId !== context.commandId)
  )
    return fail("envelope");
  const previous: CustodyState = state ?? {
    taskId: event.taskId,
    managers: HashMap.empty(),
    threads: HashMap.empty(),
    currentManagerGeneration: undefined,
    highestManagerGeneration: 0,
    allowance: { type: "unconfigured", consumed: 0 },
    allowanceCommands: HashMap.empty(),
    debits: HashMap.empty(),
    reservations: HashMap.empty(),
    reservationCommands: HashMap.empty(),
    units: HashMap.empty(),
    pendingReservations: HashMap.empty(),
    launches: HashMap.empty(),
    sources: HashMap.empty(),
    pendingSources: HashMap.empty(),
    overflowThroughGeneration: null,
    dirtyGeneration: 0,
    acknowledgedGeneration: 0,
    events: HashMap.empty(),
    sequences: HashMap.empty(),
    lastSequence: 0,
    revisions: {
      definitionRevision: 0,
      lifecycleRevision: 0,
      completionPolicyRevision: 0,
      actionPolicyRevision: 0,
    },
  };
  if (event.taskId !== previous.taskId) return fail("task_identity");
  const duplicate =
    lookup(previous.events, event.id) ?? lookup(previous.sequences, context.sequence);
  if (duplicate) {
    if (NodeUtil.isDeepStrictEqual(duplicate, recorded)) return previous;
    return fail(duplicate.event.id === event.id ? "event_identity" : "sequence");
  }
  if (context.sequence <= previous.lastSequence) return fail("sequence");
  for (const key of [
    "definitionRevision",
    "lifecycleRevision",
    "completionPolicyRevision",
    "actionPolicyRevision",
  ] as const)
    if (event[key] < previous.revisions[key]) return fail("revision");
  const next: CustodyState = {
    ...previous,
    events: HashMap.set(previous.events, event.id, recorded),
    sequences: HashMap.set(previous.sequences, context.sequence, recorded),
    lastSequence: context.sequence,
    revisions: {
      definitionRevision: event.definitionRevision,
      lifecycleRevision: event.lifecycleRevision,
      completionPolicyRevision: event.completionPolicyRevision,
      actionPolicyRevision: event.actionPolicyRevision,
    },
  };
  switch (event.type) {
    case "task.manager-bound":
      if (
        previous.currentManagerGeneration !== undefined ||
        event.managerGeneration <= previous.highestManagerGeneration ||
        HashMap.has(previous.threads, event.managerThreadId)
      )
        return fail("manager");
      return {
        ...next,
        managers: HashMap.set(previous.managers, event.managerGeneration, { bound: event }),
        threads: HashMap.set(previous.threads, event.managerThreadId, event.managerGeneration),
        currentManagerGeneration: event.managerGeneration,
        highestManagerGeneration: event.managerGeneration,
      };
    case "task.manager-retired": {
      const binding = lookup(previous.managers, event.managerGeneration);
      if (
        !binding ||
        binding.retired ||
        binding.bound.managerThreadId !== event.managerThreadId ||
        previous.currentManagerGeneration !== event.managerGeneration
      )
        return fail("manager");
      return {
        ...next,
        managers: HashMap.set(previous.managers, event.managerGeneration, {
          ...binding,
          retired: event,
        }),
        currentManagerGeneration: undefined,
      };
    }
    case "task.allowance-configured":
    case "task.allowance-topped-up":
    case "task.allowance-debited": {
      if (HashMap.has(previous.allowanceCommands, event.commandId)) return fail("command");
      if (!NodeUtil.isDeepStrictEqual(previous.allowance, event.previous)) return fail("allowance");
      if (
        event.resulting.consumed !==
        previous.allowance.consumed + (event.type === "task.allowance-debited" ? 1 : 0)
      )
        return fail("allowance");
      if (
        event.type === "task.allowance-debited" &&
        HashMap.has(previous.debits, event.allowanceUnit)
      )
        return fail("unit");
      return {
        ...next,
        allowance: event.resulting,
        allowanceCommands: HashMap.set(previous.allowanceCommands, event.commandId, recorded),
        debits:
          event.type === "task.allowance-debited"
            ? HashMap.set(previous.debits, event.allowanceUnit, event)
            : previous.debits,
      };
    }
    case "task.wake-source-recorded": {
      const key = sourceKey(event.source);
      const known = lookup(previous.sources, key);
      if (known) {
        if (
          !NodeUtil.isDeepStrictEqual(known.source, event.source) ||
          known.reservationId !== event.reservationId ||
          known.dirtyGeneration !== event.dirtyGeneration
        )
          return fail("source");
        return next;
      }
      if (event.dirtyGeneration !== previous.dirtyGeneration + 1) return fail("generation");
      if (event.reservationId !== null && !HashMap.has(previous.reservations, event.reservationId))
        return fail("reservation");
      const fits = HashMap.size(previous.pendingSources) < 32;
      return {
        ...next,
        sources: HashMap.set(previous.sources, key, event),
        dirtyGeneration: event.dirtyGeneration,
        pendingSources: fits
          ? HashMap.set(previous.pendingSources, key, event)
          : previous.pendingSources,
        overflowThroughGeneration: fits
          ? previous.overflowThroughGeneration
          : event.dirtyGeneration,
      };
    }
    case "task.wake-reserved":
    case "task.wake-claimed":
    case "task.wake-claim-uncertain":
    case "task.wake-acknowledged": {
      if (
        event.dirtyGeneration !== previous.dirtyGeneration ||
        event.acknowledgedGeneration > previous.dirtyGeneration
      )
        return fail("generation");
      const reservation = lookup(previous.reservations, event.reservationId);
      if (event.type === "task.wake-reserved") {
        if (
          reservation ||
          HashMap.has(previous.reservationCommands, event.reservationCommandId) ||
          HashMap.has(previous.units, event.allowanceUnit) ||
          HashMap.has(previous.pendingReservations, event.managerGeneration)
        )
          return fail("reservation");
        if (event.managerGeneration !== previous.currentManagerGeneration) return fail("manager");
        if (!HashMap.has(previous.debits, event.allowanceUnit)) return fail("unit");
      } else if (
        !reservation ||
        !NodeUtil.isDeepStrictEqual(wakeIdentity(reservation.reserved), wakeIdentity(event))
      )
        return fail("reservation");
      if (
        event.type !== "task.wake-acknowledged" &&
        event.acknowledgedGeneration !== previous.acknowledgedGeneration
      )
        return fail("acknowledgement");
      if ("pending" in event) {
        const batch = HashMap.fromIterable(
          event.pending.sources.map((source) => [sourceKey(source), source] as const),
        );
        for (const source of event.pending.sources) {
          const known = lookup(previous.sources, sourceKey(source));
          if (
            !known ||
            known.dirtyGeneration <= previous.acknowledgedGeneration ||
            !NodeUtil.isDeepStrictEqual(known.source, source)
          )
            return fail("source");
        }
        for (const [key, source] of previous.pendingSources)
          if (source.dirtyGeneration > previous.acknowledgedGeneration && !HashMap.has(batch, key))
            return fail("source");
        if (event.pending.overflowThroughGeneration !== previous.overflowThroughGeneration)
          return fail("generation");
      }
      if (event.type === "task.wake-reserved")
        return {
          ...next,
          reservations: HashMap.set(previous.reservations, event.reservationId, {
            reserved: event,
          }),
          reservationCommands: HashMap.set(
            previous.reservationCommands,
            event.reservationCommandId,
            event.reservationId,
          ),
          units: HashMap.set(previous.units, event.allowanceUnit, event.reservationId),
          pendingReservations: HashMap.set(
            previous.pendingReservations,
            event.managerGeneration,
            event.reservationId,
          ),
        };
      if (!reservation) return fail("reservation");
      if (event.type === "task.wake-claimed") {
        if (reservation.acknowledged) return fail("phase");
        if (
          reservation.claimed &&
          (reservation.claimed.messageId !== event.messageId ||
            reservation.claimed.runId !== event.runId ||
            event.dirtyGeneration <= reservation.claimed.dirtyGeneration)
        )
          return fail("phase");
        const keys = [`message:${event.messageId}`, `run:${event.runId}`];
        if (
          keys.some((key) => {
            const owner = lookup(previous.launches, key);
            return owner !== undefined && owner !== event.reservationId;
          })
        )
          return fail("reservation");
        return {
          ...next,
          reservations: HashMap.set(previous.reservations, event.reservationId, {
            ...reservation,
            claimed: event,
          }),
          launches: keys.reduce(
            (map, key) => HashMap.set(map, key, event.reservationId),
            previous.launches,
          ),
          pendingReservations: HashMap.remove(
            previous.pendingReservations,
            event.managerGeneration,
          ),
        };
      }
      if (event.type === "task.wake-claim-uncertain") {
        if (reservation.uncertain || reservation.acknowledged) return fail("phase");
        return {
          ...next,
          reservations: HashMap.set(previous.reservations, event.reservationId, {
            ...reservation,
            uncertain: event,
          }),
        };
      }
      if (
        !reservation.claimed ||
        event.previousAcknowledgedGeneration !== previous.acknowledgedGeneration ||
        event.acknowledgedGeneration <= previous.acknowledgedGeneration ||
        event.acknowledgedGeneration > reservation.claimed.dirtyGeneration ||
        event.observedDirtyGeneration !== reservation.claimed.dirtyGeneration
      )
        return fail("acknowledgement");
      return {
        ...next,
        reservations: HashMap.set(previous.reservations, event.reservationId, {
          ...reservation,
          acknowledged: event,
        }),
        acknowledgedGeneration: event.acknowledgedGeneration,
        pendingSources: HashMap.filter(
          previous.pendingSources,
          (source) => source.dirtyGeneration > event.acknowledgedGeneration,
        ),
        overflowThroughGeneration:
          previous.overflowThroughGeneration !== null &&
          previous.overflowThroughGeneration <= event.acknowledgedGeneration
            ? null
            : previous.overflowThroughGeneration,
      };
    }
    default:
      return fail("unsupported");
  }
}
