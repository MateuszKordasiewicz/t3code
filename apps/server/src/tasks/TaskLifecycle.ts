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
