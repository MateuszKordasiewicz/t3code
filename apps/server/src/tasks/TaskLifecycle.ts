import * as NodeUtil from "node:util";
import type { WorkTaskEvent, WorkTaskPersistedEventContext } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

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
  readonly acceptedDefinitions: ReadonlyArray<Recorded<EventOf<"task.definition-accepted">>>;
  readonly revisions: Pick<
    FoundationEvent,
    "definitionRevision" | "lifecycleRevision" | "completionPolicyRevision" | "actionPolicyRevision"
  >;
  readonly configuration?: Recorded<EventOf<"task.configuration-updated">>;
  readonly execution?: Recorded<EventOf<"task.execution-updated">>;
  readonly links: ReadonlyArray<Link>;
  readonly waits: ReadonlyArray<Wait>;
  readonly results: ReadonlyArray<Recorded<EventOf<"task.result-recorded">>>;
  readonly continuations: ReadonlyArray<Recorded<EventOf<"task.continuation-accepted">>>;
  readonly claims: ReadonlyArray<Claim>;
  readonly completion?: Recorded<EventOf<"task.completed" | "task.reopened">>;
  readonly archive?: Recorded<EventOf<"task.archived">>;
  readonly events: ReadonlyArray<Recorded>;
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
      acceptedDefinitions: [],
      revisions: {
        definitionRevision: event.definitionRevision,
        lifecycleRevision: event.lifecycleRevision,
        completionPolicyRevision: event.completionPolicyRevision,
        actionPolicyRevision: event.actionPolicyRevision,
      },
      links: [],
      waits: [],
      results: [],
      continuations: [],
      claims: [],
      events: [recorded],
    };
  }
  if (event.taskId !== state.taskId) return conflict("task_identity", event);
  const existing = state.events.find(
    (item) => item.event.id === event.id || item.context.sequence === context.sequence,
  );
  if (existing) {
    if (NodeUtil.isDeepStrictEqual(existing, recorded)) return state;
    return conflict(existing.event.id === event.id ? "event_identity" : "sequence", event);
  }
  if (context.sequence <= state.events[state.events.length - 1]!.context.sequence)
    return conflict("sequence", event);
  for (const key of [
    "definitionRevision",
    "lifecycleRevision",
    "completionPolicyRevision",
    "actionPolicyRevision",
  ] as const) {
    if (event[key] < state.revisions[key]) return conflict("revision", event);
  }
  const next = {
    ...state,
    revisions: {
      definitionRevision: event.definitionRevision,
      lifecycleRevision: event.lifecycleRevision,
      completionPolicyRevision: event.completionPolicyRevision,
      actionPolicyRevision: event.actionPolicyRevision,
    },
    events: [...state.events, recorded],
  };
  switch (event.type) {
    case "task.created":
      return conflict("creation", event);
    case "task.definition-updated": {
      if (event.definitionRevision !== event.definition.revision)
        return conflict("definition", event);
      const previous = state.draft.event.definition;
      const accepted = state.acceptedDefinitions.find(
        (item) => item.event.definitionRevision === event.definitionRevision,
      );
      if (
        (previous.revision === event.definitionRevision &&
          !NodeUtil.isDeepStrictEqual(previous, event.definition)) ||
        (accepted && !NodeUtil.isDeepStrictEqual(accepted.event.definition, event.definition))
      )
        return conflict("definition", event);
      return { ...next, draft: { event, context }, definition: { event, context } };
    }
    case "task.definition-accepted": {
      if (
        event.definitionRevision !== event.definition.revision ||
        state.acceptedDefinitions.some(
          (item) => item.event.definitionRevision === event.definitionRevision,
        )
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
        acceptedDefinitions: [...state.acceptedDefinitions, { event, context }],
      };
    }
    case "task.configuration-updated":
      return { ...next, configuration: { event, context } };
    case "task.execution-updated":
      return { ...next, execution: { event, context } };
    case "task.thread-linked": {
      if (
        state.links.some((link) => link.linked.event.threadId === event.threadId && !link.unlinked)
      )
        return conflict("link", event);
      return { ...next, links: [...state.links, { linked: { event, context } }] };
    }
    case "task.thread-unlinked": {
      const index = state.links.findIndex(
        (link) => link.linked.event.threadId === event.threadId && !link.unlinked,
      );
      if (index < 0) return conflict("link", event);
      return {
        ...next,
        links: state.links.map((link, i) =>
          i === index ? { ...link, unlinked: { event, context } } : link,
        ),
      };
    }
    case "task.wait-updated":
      return {
        ...next,
        waits: [
          ...state.waits.filter((wait) => wait.event.waitId !== event.waitId),
          { event, context },
        ],
      };
    case "task.result-recorded":
      return { ...next, results: [...state.results, { event, context }] };
    case "task.continuation-accepted":
      return { ...next, continuations: [...state.continuations, { event, context }] };
    case "task.completion-submitted": {
      if (
        state.claims.some(
          (claim) =>
            claim.event.claimId === event.claimId || claim.event.evidenceId === event.evidenceId,
        )
      )
        return conflict("claim", event);
      return { ...next, claims: [...state.claims, { event, context }] };
    }
    case "task.completed": {
      const claim = state.claims.find(
        (item) =>
          item.event.claimId === event.claimId || item.event.evidenceId === event.evidenceId,
      );
      if (
        event.attribution.type === "manager" ||
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
