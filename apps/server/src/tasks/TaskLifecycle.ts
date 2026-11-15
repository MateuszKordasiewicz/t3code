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
    if (!historicalFoundation(event) && event[key] < state.revisions[key])
      return conflict("revision", event);
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
      ...revisionHighWater(state.revisions, event),
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

type ValidationType =
  | "task.validation-reserved"
  | "task.validation-started"
  | "task.validation-result"
  | "task.validation-uncertain";
type ValidationEvent = Extract<WorkTaskEvent, { readonly type: ValidationType }>;
type ValidationOf<T extends ValidationType> = Extract<ValidationEvent, { readonly type: T }>;
type ValidationRecorded = {
  readonly event: ValidationEvent;
  readonly context: WorkTaskPersistedEventContext;
};
type ValidationAttempt = {
  readonly reserved: ValidationOf<"task.validation-reserved">;
  readonly started?: ValidationOf<"task.validation-started">;
  readonly uncertain?: ValidationOf<"task.validation-uncertain">;
  readonly result?: ValidationOf<"task.validation-result">;
};
type ValidationState = {
  readonly taskId: ValidationEvent["taskId"];
  readonly attempts: HashMap.HashMap<string, ValidationAttempt>;
  readonly units: HashMap.HashMap<number, string>;
  readonly runs: HashMap.HashMap<string, string>;
  readonly commands: HashMap.HashMap<string, string>;
  readonly events: HashMap.HashMap<string, ValidationRecorded>;
  readonly sequences: HashMap.HashMap<number, ValidationRecorded>;
  readonly lastSequence: number;
};
export class ValidationReplayConflict extends Schema.TaggedError<ValidationReplayConflict>()(
  "ValidationReplayConflict",
  {
    code: Schema.Literals([
      "envelope",
      "task_identity",
      "event_identity",
      "sequence",
      "binding",
      "phase",
      "unit",
      "run",
      "command",
      "unsupported",
    ]),
    eventId: Schema.String,
  },
) {}
const validationIdentity = (event: ValidationEvent) => ({
  definitionRevision: event.definitionRevision,
  completionPolicyRevision: event.completionPolicyRevision,
  attemptId: event.attemptId,
  managerGeneration: event.managerGeneration,
  claimId: event.claimId,
  evidenceId: event.evidenceId,
  workWatermark: event.workWatermark,
  validator: event.validator,
  allowanceUnit: event.allowanceUnit,
  createdAt: event.createdAt,
});

/** Retains recorded validation phases; it neither launches a validator nor authorizes completion. */
export function reduceTaskValidation(
  state: ValidationState | undefined,
  recorded: ValidationRecorded,
): ValidationState {
  const { event, context } = recorded;
  const fail = (code: ValidationReplayConflict["code"]): never => {
    throw new ValidationReplayConflict({ code, eventId: event.id });
  };
  if (
    event.id !== context.eventId ||
    event.occurredAt !== context.occurredAt ||
    event.commandId !== context.commandId
  )
    return fail("envelope");
  const previous: ValidationState = state ?? {
    taskId: event.taskId,
    attempts: HashMap.empty(),
    units: HashMap.empty(),
    runs: HashMap.empty(),
    commands: HashMap.empty(),
    events: HashMap.empty(),
    sequences: HashMap.empty(),
    lastSequence: 0,
  };
  if (event.taskId !== previous.taskId) return fail("task_identity");
  const duplicate =
    lookup(previous.events, event.id) ?? lookup(previous.sequences, context.sequence);
  if (duplicate) {
    if (NodeUtil.isDeepStrictEqual(duplicate, recorded)) return previous;
    return fail(duplicate.event.id === event.id ? "event_identity" : "sequence");
  }
  if (context.sequence <= previous.lastSequence) return fail("sequence");
  const commandKey = JSON.stringify([event.type, event.commandId]);
  if (HashMap.has(previous.commands, commandKey)) return fail("command");
  const next: ValidationState = {
    ...previous,
    events: HashMap.set(previous.events, event.id, recorded),
    sequences: HashMap.set(previous.sequences, context.sequence, recorded),
    lastSequence: context.sequence,
    commands: HashMap.set(previous.commands, commandKey, event.attemptId),
  };
  const attempt = lookup(previous.attempts, event.attemptId);
  if (event.type === "task.validation-reserved") {
    if (attempt) return fail("phase");
    if (HashMap.has(previous.units, event.allowanceUnit)) return fail("unit");
    return {
      ...next,
      attempts: HashMap.set(previous.attempts, event.attemptId, { reserved: event }),
      units: HashMap.set(previous.units, event.allowanceUnit, event.attemptId),
    };
  }
  if (!attempt) return fail("phase");
  if (!NodeUtil.isDeepStrictEqual(validationIdentity(attempt.reserved), validationIdentity(event)))
    return fail("binding");
  const runOwner = lookup(previous.runs, event.runId);
  if (runOwner !== undefined && runOwner !== event.attemptId) return fail("run");
  if (event.type === "task.validation-started") {
    if (attempt.started || attempt.result || attempt.uncertain) return fail("phase");
    return {
      ...next,
      attempts: HashMap.set(previous.attempts, event.attemptId, { ...attempt, started: event }),
      runs: HashMap.set(previous.runs, event.runId, event.attemptId),
    };
  }
  if (
    !attempt.started ||
    event.runId !== attempt.started.runId ||
    event.startedAt !== attempt.started.startedAt
  )
    return fail("binding");
  switch (event.type) {
    case "task.validation-uncertain":
      if (attempt.uncertain || attempt.result) return fail("phase");
      return {
        ...next,
        attempts: HashMap.set(previous.attempts, event.attemptId, { ...attempt, uncertain: event }),
      };
    case "task.validation-result":
      if (attempt.result) return fail("phase");
      return {
        ...next,
        attempts: HashMap.set(previous.attempts, event.attemptId, { ...attempt, result: event }),
      };
    default:
      return fail("unsupported");
  }
}

type ReviewType =
  | "task.action-review-requested"
  | "task.action-review-result-recorded"
  | "task.action-review-invalidated"
  | "task.action-review-cancelled"
  | "task.action-review-continuation-released";
type ReviewEvent = Extract<WorkTaskEvent, { readonly type: ReviewType }>;
type ReviewOf<T extends ReviewType> = Extract<ReviewEvent, { readonly type: T }>;
type ReviewRecorded<E extends ReviewEvent = ReviewEvent> = {
  readonly event: E;
  readonly context: WorkTaskPersistedEventContext;
};
type ReviewRecord = {
  readonly request: ReviewRecorded<ReviewOf<"task.action-review-requested">>;
  readonly decision?: ReviewRecorded<ReviewOf<"task.action-review-result-recorded">>;
  readonly outcomes: Chunk.Chunk<ReviewRecorded>;
  readonly release?: ReviewRecorded<ReviewOf<"task.action-review-continuation-released">>;
  readonly status:
    | "pending"
    | "approved"
    | "rejected"
    | "changes_requested"
    | "invalidated"
    | "cancelled";
};
type ReviewState = {
  readonly taskId: ReviewEvent["taskId"];
  readonly reviews: HashMap.HashMap<string, ReviewRecord>;
  readonly runtimeRequests: HashMap.HashMap<string, string>;
  readonly continuations: HashMap.HashMap<string, string>;
  readonly requestCommands: HashMap.HashMap<string, string>;
  readonly events: HashMap.HashMap<string, ReviewRecorded>;
  readonly sequences: HashMap.HashMap<number, ReviewRecorded>;
  readonly lastSequence: number;
};
export class ReviewReplayConflict extends Schema.TaggedError<ReviewReplayConflict>()(
  "ReviewReplayConflict",
  {
    code: Schema.Literals([
      "envelope",
      "task_identity",
      "event_identity",
      "sequence",
      "request",
      "binding",
      "decision",
      "release",
      "phase",
      "unsupported",
    ]),
    eventId: Schema.String,
  },
) {}
const requestedReviewBinding = (review: ReviewOf<"task.action-review-requested">["review"]) => ({
  taskId: review.taskId,
  reviewId: review.reviewId,
  reviewRevision: review.revision,
  definitionRevision: review.definitionRevision,
  actionPolicyRevision: review.actionPolicyRevision,
  payloadIdentity: {
    action: review.action,
    target: review.target,
    fingerprint: review.fingerprint,
  },
  ruleId: review.ruleId,
  runtimeRequestId: review.runtimeRequestId,
  providerSessionId: review.providerSessionId,
  managerThreadId: review.managerThreadId,
  managerGeneration: review.managerGeneration,
  runId: review.runId,
});

/** Retains immutable review evidence; live human authorization and dispatch remain service guards. */
export function reduceTaskReviews(
  state: ReviewState | undefined,
  recorded: ReviewRecorded,
): ReviewState {
  const { event, context } = recorded;
  const fail = (code: ReviewReplayConflict["code"]): never => {
    throw new ReviewReplayConflict({ code, eventId: event.id });
  };
  if (event.id !== context.eventId || event.occurredAt !== context.occurredAt)
    return fail("envelope");
  const previous: ReviewState = state ?? {
    taskId: event.taskId,
    reviews: HashMap.empty(),
    runtimeRequests: HashMap.empty(),
    continuations: HashMap.empty(),
    requestCommands: HashMap.empty(),
    events: HashMap.empty(),
    sequences: HashMap.empty(),
    lastSequence: -1,
  };
  if (event.taskId !== previous.taskId) return fail("task_identity");
  const duplicate =
    lookup(previous.events, event.id) ?? lookup(previous.sequences, context.sequence);
  if (duplicate) {
    if (NodeUtil.isDeepStrictEqual(duplicate, recorded)) return previous;
    return fail(duplicate.event.id === event.id ? "event_identity" : "sequence");
  }
  if (context.sequence <= previous.lastSequence) return fail("sequence");
  const next: ReviewState = {
    ...previous,
    events: HashMap.set(previous.events, event.id, recorded),
    sequences: HashMap.set(previous.sequences, context.sequence, recorded),
    lastSequence: context.sequence,
  };
  if (event.type === "task.action-review-requested") {
    const review = event.review;
    if (
      !review.runtimeRequestId ||
      context.commandId === null ||
      review.createdSequence !== context.sequence ||
      review.taskId !== event.taskId ||
      review.state.type !== "pending" ||
      HashMap.has(previous.reviews, review.reviewId) ||
      HashMap.has(previous.runtimeRequests, review.runtimeRequestId) ||
      HashMap.has(previous.requestCommands, context.commandId) ||
      (review.continuationId !== undefined &&
        HashMap.has(previous.continuations, review.continuationId))
    )
      return fail("request");
    return {
      ...next,
      reviews: HashMap.set(previous.reviews, review.reviewId, {
        request: { event, context },
        outcomes: Chunk.empty(),
        status: "pending",
      }),
      runtimeRequests: HashMap.set(
        previous.runtimeRequests,
        review.runtimeRequestId,
        review.reviewId,
      ),
      requestCommands: HashMap.set(previous.requestCommands, context.commandId, review.reviewId),
      continuations:
        review.continuationId === undefined
          ? previous.continuations
          : HashMap.set(previous.continuations, review.continuationId, review.reviewId),
    };
  }
  const reviewId = "binding" in event ? event.binding.reviewId : event.reviewId;
  const known = lookup(previous.reviews, reviewId);
  if (!known) return fail("request");
  const binding = requestedReviewBinding(known.request.event.review);
  if (event.type === "task.action-review-result-recorded") {
    if (
      event.reviewRevision !== binding.reviewRevision ||
      event.definitionRevision !== binding.definitionRevision ||
      event.actionPolicyRevision !== binding.actionPolicyRevision ||
      event.managerThreadId !== binding.managerThreadId ||
      event.managerGeneration !== binding.managerGeneration ||
      event.runId !== binding.runId ||
      !NodeUtil.isDeepStrictEqual(event.payloadIdentity, binding.payloadIdentity)
    )
      return fail("binding");
    if (known.decision || !Chunk.isEmpty(known.outcomes)) return fail("decision");
    const status =
      event.decision === "approve"
        ? "approved"
        : event.decision === "reject"
          ? "rejected"
          : "changes_requested";
    return {
      ...next,
      reviews: HashMap.set(previous.reviews, reviewId, {
        ...known,
        decision: { event, context },
        status,
      }),
    };
  }
  if (!NodeUtil.isDeepStrictEqual(event.binding, binding)) return fail("binding");
  switch (event.type) {
    case "task.action-review-invalidated":
    case "task.action-review-cancelled": {
      if (
        event.type === "task.action-review-cancelled" &&
        context.commandId !== event.source.commandId
      )
        return fail("envelope");
      return {
        ...next,
        reviews: HashMap.set(previous.reviews, reviewId, {
          ...known,
          outcomes: Chunk.append(known.outcomes, recorded),
          status: event.type === "task.action-review-invalidated" ? "invalidated" : "cancelled",
        }),
      };
    }
    case "task.action-review-continuation-released": {
      if (
        !known.decision ||
        known.release ||
        !Chunk.isEmpty(known.outcomes) ||
        known.decision.event.id !== event.decisionEventId ||
        known.decision.event.decision !== event.decision
      )
        return fail("release");
      const requested = known.request.event.review.continuationId;
      const owner = lookup(previous.continuations, event.continuationId);
      if (
        (requested !== undefined && requested !== event.continuationId) ||
        (owner !== undefined && owner !== reviewId)
      )
        return fail("release");
      return {
        ...next,
        reviews: HashMap.set(previous.reviews, reviewId, { ...known, release: { event, context } }),
        continuations: HashMap.set(previous.continuations, event.continuationId, reviewId),
      };
    }
    default:
      return fail("unsupported");
  }
}

type TaskRecorded = {
  readonly event: WorkTaskEvent;
  readonly context: WorkTaskPersistedEventContext;
};
type ManagerReference = {
  readonly managerThreadId: string;
  readonly managerGeneration: number;
  readonly messageId?: string;
  readonly effectId?: string;
};
type ManagerLaunch = ManagerReference & {
  readonly type: "manager";
  readonly runId: string;
  readonly messageId: string;
};
type RunOwner = ManagerLaunch | { readonly type: "validation"; readonly attemptId: string };
type TaskState = {
  readonly foundation: FoundationState;
  readonly custody?: CustodyState;
  readonly validation?: ValidationState;
  readonly reviews?: ReviewState;
  readonly revisions: FoundationState["revisions"];
  readonly actionPolicies: HashMap.HashMap<
    number,
    EventOf<"task.definition-accepted">["definition"]["actionPolicy"]
  >;
  readonly definitionPolicies: HashMap.HashMap<number, number>;
  readonly managers: HashMap.HashMap<number, string>;
  readonly threads: HashMap.HashMap<string, number>;
  readonly runOwners: HashMap.HashMap<string, RunOwner>;
  readonly runReferences: HashMap.HashMap<string, ManagerReference>;
  readonly messages: HashMap.HashMap<string, ManagerLaunch>;
  readonly effects: HashMap.HashMap<string, ManagerLaunch>;
  readonly units: HashMap.HashMap<number, string>;
  readonly archives: HashMap.HashMap<string, EventOf<"task.archived">>;
  readonly cancellationSources: HashMap.HashMap<
    string,
    ReviewOf<"task.action-review-cancelled">["source"]["human"]
  >;
  readonly events: HashMap.HashMap<string, TaskRecorded>;
  readonly sequences: HashMap.HashMap<number, TaskRecorded>;
  readonly lastSequence: number;
};
export class TaskReplayConflict extends Schema.TaggedError<TaskReplayConflict>()(
  "TaskReplayConflict",
  {
    code: Schema.Literals([
      "creation",
      "envelope",
      "task_identity",
      "event_identity",
      "sequence",
      "revision",
      "policy",
      "definition",
      "manager",
      "claim",
      "unit",
      "run",
      "message",
      "effect",
      "archive",
      "unsupported",
    ]),
    eventId: Schema.String,
  },
) {}
const taskConflict = (code: TaskReplayConflict["code"], event: { readonly id: string }): never => {
  throw new TaskReplayConflict({ code, eventId: event.id });
};
const historicalFoundation = (event: FoundationEvent) =>
  event.type === "task.result-recorded" ||
  event.type === "task.wait-updated" ||
  event.type === "task.continuation-accepted" ||
  event.type === "task.completion-submitted";
const revisionHighWater = (
  left: FoundationState["revisions"],
  right: FoundationState["revisions"],
) => ({
  definitionRevision: Math.max(left.definitionRevision, right.definitionRevision),
  lifecycleRevision: Math.max(left.lifecycleRevision, right.lifecycleRevision),
  completionPolicyRevision: Math.max(left.completionPolicyRevision, right.completionPolicyRevision),
  actionPolicyRevision: Math.max(left.actionPolicyRevision, right.actionPolicyRevision),
});

/** Composes every recorded task event; service authorization and external execution are separate. */
export function reduceTaskLifecycle(
  state: TaskState | undefined,
  recorded: TaskRecorded,
): TaskState {
  const { event, context } = recorded;
  const fail = (code: TaskReplayConflict["code"]): never => taskConflict(code, event);
  if (event.id !== context.eventId || event.occurredAt !== context.occurredAt)
    return fail("envelope");
  if (!state) {
    if (event.type !== "task.created") return fail("creation");
    const foundation = reduceTaskFoundation(undefined, { event, context });
    return {
      foundation,
      revisions: foundation.revisions,
      actionPolicies: HashMap.empty(),
      definitionPolicies: HashMap.empty(),
      managers: HashMap.empty(),
      threads: HashMap.empty(),
      runOwners: HashMap.empty(),
      runReferences: HashMap.empty(),
      messages: HashMap.empty(),
      effects: HashMap.empty(),
      units: HashMap.empty(),
      archives: HashMap.empty(),
      cancellationSources: HashMap.empty(),
      events: HashMap.set(HashMap.empty<string, TaskRecorded>(), event.id, recorded),
      sequences: HashMap.set(HashMap.empty<number, TaskRecorded>(), context.sequence, recorded),
      lastSequence: context.sequence,
    };
  }
  if (event.taskId !== state.foundation.taskId) return fail("task_identity");
  const duplicate = lookup(state.events, event.id) ?? lookup(state.sequences, context.sequence);
  if (duplicate) {
    if (NodeUtil.isDeepStrictEqual(duplicate, recorded)) return state;
    return fail(duplicate.event.id === event.id ? "event_identity" : "sequence");
  }
  if (context.sequence <= state.lastSequence) return fail("sequence");
  const custodyMutation = [
    "task.manager-bound",
    "task.manager-retired",
    "task.allowance-configured",
    "task.allowance-topped-up",
    "task.allowance-debited",
    "task.wake-reserved",
  ].includes(event.type);
  if (custodyMutation)
    for (const key of [
      "definitionRevision",
      "lifecycleRevision",
      "completionPolicyRevision",
      "actionPolicyRevision",
    ] as const)
      if (event[key] < state.revisions[key]) return fail("revision");
  let next: TaskState = {
    ...state,
    revisions: revisionHighWater(state.revisions, event),
    events: HashMap.set(state.events, event.id, recorded),
    sequences: HashMap.set(state.sequences, context.sequence, recorded),
    lastSequence: context.sequence,
  };
  const manager =
    "binding" in event
      ? event.binding
      : "review" in event
        ? event.review
        : "managerThreadId" in event
          ? event
          : "manager" in event
            ? event.manager
            : null;
  if (manager) {
    const knownThread = lookup(state.managers, manager.managerGeneration);
    const knownGeneration = lookup(state.threads, manager.managerThreadId);
    if (
      (knownThread !== undefined && knownThread !== manager.managerThreadId) ||
      (knownGeneration !== undefined && knownGeneration !== manager.managerGeneration)
    )
      return fail("manager");
    next = {
      ...next,
      managers: HashMap.set(next.managers, manager.managerGeneration, manager.managerThreadId),
      threads: HashMap.set(next.threads, manager.managerThreadId, manager.managerGeneration),
    };
  }
  const referenceRun =
    "binding" in event
      ? event.binding.runId
      : "review" in event
        ? event.review.runId
        : "runId" in event && manager
          ? event.runId
          : undefined;
  if (manager && referenceRun !== undefined) {
    const known = lookup(next.runReferences, referenceRun);
    const messageId = "messageId" in event ? event.messageId : undefined;
    const effectId = "effectId" in event ? event.effectId : undefined;
    const reference = {
      ...known,
      managerThreadId: manager.managerThreadId,
      managerGeneration: manager.managerGeneration,
      ...(messageId === undefined ? {} : { messageId }),
      ...(effectId === undefined ? {} : { effectId }),
    };
    const owner = lookup(next.runOwners, referenceRun);
    if (
      (known &&
        (known.managerThreadId !== reference.managerThreadId ||
          known.managerGeneration !== reference.managerGeneration)) ||
      (owner &&
        (owner.type !== "manager" ||
          owner.managerThreadId !== reference.managerThreadId ||
          owner.managerGeneration !== reference.managerGeneration))
    )
      return fail("run");
    if (
      messageId !== undefined &&
      ((known?.messageId !== undefined && known.messageId !== messageId) ||
        (owner?.type === "manager" && owner.messageId !== messageId))
    )
      return fail("message");
    if (effectId !== undefined && known?.effectId !== undefined && known.effectId !== effectId)
      return fail("effect");
    next = { ...next, runReferences: HashMap.set(next.runReferences, referenceRun, reference) };
  }
  const claimUnit = (unit: number, owner: string) => {
    const known = lookup(next.units, unit);
    if (
      !state.custody ||
      !HashMap.has(state.custody.debits, unit) ||
      (known !== undefined && known !== owner)
    )
      return fail("unit");
    next = { ...next, units: HashMap.set(next.units, unit, owner) };
  };
  const managerLaunch = (launch: ManagerLaunch, effectId?: string) => {
    const run = lookup(next.runOwners, launch.runId);
    const message = lookup(next.messages, launch.messageId);
    const reference = lookup(next.runReferences, launch.runId);
    if (
      (run && !NodeUtil.isDeepStrictEqual(run, launch)) ||
      (reference &&
        (reference.managerThreadId !== launch.managerThreadId ||
          reference.managerGeneration !== launch.managerGeneration))
    )
      return fail("run");
    if (reference?.messageId !== undefined && reference.messageId !== launch.messageId)
      return fail("message");
    if (
      effectId !== undefined &&
      reference?.effectId !== undefined &&
      reference.effectId !== effectId
    )
      return fail("effect");
    if (message && !NodeUtil.isDeepStrictEqual(message, launch)) return fail("message");
    if (effectId !== undefined) {
      const effect = lookup(next.effects, effectId);
      if (effect && !NodeUtil.isDeepStrictEqual(effect, launch)) return fail("effect");
    }
    next = {
      ...next,
      runOwners: HashMap.set(next.runOwners, launch.runId, launch),
      messages: HashMap.set(next.messages, launch.messageId, launch),
      effects: effectId === undefined ? next.effects : HashMap.set(next.effects, effectId, launch),
    };
  };
  switch (event.type) {
    case "task.created":
      return fail("creation");
    case "task.definition-accepted": {
      const policy = lookup(state.actionPolicies, event.actionPolicyRevision);
      if (policy && !NodeUtil.isDeepStrictEqual(policy, event.definition.actionPolicy))
        return fail("policy");
      next = {
        ...next,
        actionPolicies: HashMap.set(
          next.actionPolicies,
          event.actionPolicyRevision,
          event.definition.actionPolicy,
        ),
        definitionPolicies: HashMap.set(
          next.definitionPolicies,
          event.definitionRevision,
          event.actionPolicyRevision,
        ),
      };
      break;
    }
    case "task.action-review-requested": {
      const review = event.review;
      const accepted = lookup(state.foundation.acceptedByRevision, review.definitionRevision);
      const policy = lookup(state.actionPolicies, review.actionPolicyRevision);
      if (
        !accepted ||
        !policy ||
        lookup(state.definitionPolicies, review.definitionRevision) !== review.actionPolicyRevision
      )
        return fail("definition");
      const rule = policy.rules.find((rule) => rule.id === review.ruleId);
      if (!rule || rule.action !== review.action || rule.target !== review.target)
        return fail("policy");
      break;
    }
    case "task.wake-reserved":
      claimUnit(event.allowanceUnit, `wake:${event.reservationId}`);
      break;
    case "task.wake-claimed": {
      const thread = lookup(next.managers, event.managerGeneration);
      if (!thread) return fail("manager");
      managerLaunch({
        type: "manager",
        managerThreadId: thread,
        managerGeneration: event.managerGeneration,
        runId: event.runId,
        messageId: event.messageId,
      });
      break;
    }
    case "task.continuation-accepted":
      managerLaunch(
        {
          type: "manager",
          managerThreadId: event.managerThreadId,
          managerGeneration: event.managerGeneration,
          runId: event.runId,
          messageId: event.messageId,
        },
        event.effectId,
      );
      break;
    case "task.validation-reserved":
    case "task.validation-started":
    case "task.validation-result":
    case "task.validation-uncertain": {
      const claim = lookup(state.foundation.claimIds, event.claimId);
      if (
        !claim ||
        claim.event.evidenceId !== event.evidenceId ||
        claim.event.definitionRevision !== event.definitionRevision ||
        claim.event.completionPolicyRevision !== event.completionPolicyRevision ||
        claim.event.managerGeneration !== event.managerGeneration ||
        !HashMap.has(state.foundation.acceptedByRevision, event.definitionRevision)
      )
        return fail("claim");
      if (!HashMap.has(state.managers, event.managerGeneration)) return fail("manager");
      claimUnit(event.allowanceUnit, `validation:${event.attemptId}`);
      if ("runId" in event && event.runId !== undefined) {
        const owner = { type: "validation" as const, attemptId: event.attemptId };
        const known = lookup(next.runOwners, event.runId);
        if (
          HashMap.has(next.runReferences, event.runId) ||
          (known && !NodeUtil.isDeepStrictEqual(known, owner))
        )
          return fail("run");
        next = { ...next, runOwners: HashMap.set(next.runOwners, event.runId, owner) };
      }
      break;
    }
    case "task.archived": {
      if (context.commandId !== null) {
        if (HashMap.has(state.archives, context.commandId)) return fail("archive");
        const cancellation = lookup(state.cancellationSources, context.commandId);
        if (
          cancellation &&
          (!event.archived ||
            event.attribution.type !== "user" ||
            event.attribution.subject !== cancellation.subject ||
            event.attribution.sessionId !== cancellation.sessionId)
        )
          return fail("archive");
        next = { ...next, archives: HashMap.set(next.archives, context.commandId, event) };
      }
      break;
    }
    case "task.action-review-cancelled": {
      const archive = lookup(state.archives, event.source.commandId);
      if (
        archive &&
        (!archive.archived ||
          archive.attribution.type !== "user" ||
          archive.attribution.subject !== event.source.human.subject ||
          archive.attribution.sessionId !== event.source.human.sessionId)
      )
        return fail("archive");
      const known = lookup(state.cancellationSources, event.source.commandId);
      if (known && !NodeUtil.isDeepStrictEqual(known, event.source.human)) return fail("archive");
      next = {
        ...next,
        cancellationSources: HashMap.set(
          next.cancellationSources,
          event.source.commandId,
          event.source.human,
        ),
      };
      break;
    }
  }
  switch (event.type) {
    case "task.definition-updated":
    case "task.definition-accepted":
    case "task.thread-linked":
    case "task.thread-unlinked":
    case "task.configuration-updated":
    case "task.execution-updated":
    case "task.wait-updated":
    case "task.result-recorded":
    case "task.continuation-accepted":
    case "task.completion-submitted":
    case "task.completed":
    case "task.reopened":
    case "task.archived": {
      if (!historicalFoundation(event))
        for (const key of [
          "definitionRevision",
          "lifecycleRevision",
          "completionPolicyRevision",
          "actionPolicyRevision",
        ] as const)
          if (event[key] < state.revisions[key]) return fail("revision");
      return { ...next, foundation: reduceTaskFoundation(state.foundation, { event, context }) };
    }
    case "task.manager-bound":
    case "task.manager-retired":
    case "task.allowance-configured":
    case "task.allowance-topped-up":
    case "task.allowance-debited":
    case "task.wake-source-recorded":
    case "task.wake-reserved":
    case "task.wake-claimed":
    case "task.wake-claim-uncertain":
    case "task.wake-acknowledged":
      return { ...next, custody: reduceTaskCustody(state.custody, { event, context }) };
    case "task.validation-reserved":
    case "task.validation-started":
    case "task.validation-result":
    case "task.validation-uncertain":
      return { ...next, validation: reduceTaskValidation(state.validation, { event, context }) };
    case "task.action-review-requested":
    case "task.action-review-result-recorded":
    case "task.action-review-invalidated":
    case "task.action-review-cancelled":
    case "task.action-review-continuation-released":
      return { ...next, reviews: reduceTaskReviews(state.reviews, { event, context }) };
    default:
      return taskConflict("unsupported", event satisfies never);
  }
}
