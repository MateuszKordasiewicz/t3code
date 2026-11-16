import * as NodeUtil from "node:util";
import type { EnvironmentId, WorkTaskPersistedGraphEvent } from "@t3tools/contracts";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

type Recorded = WorkTaskPersistedGraphEvent;
type Event = Recorded["event"];
type Adoption = Recorded & { readonly event: Extract<Event, { type: "task.graph-edge-adopted" }> };
type Removal = Recorded & { readonly event: Extract<Event, { type: "task.graph-edge-removed" }> };
type Proposal = Extract<Event, { type: "task.graph-proposal-recorded" }>["proposal"];
type History = { readonly adopted: Adoption; readonly removed?: Removal };
type GraphState = {
  readonly environmentId: EnvironmentId;
  readonly graphRevision: number;
  readonly lastSequence: number | undefined;
  readonly edges: HashMap.HashMap<string, History>;
  readonly activePairs: HashMap.HashMap<string, string>;
  readonly proposals: HashMap.HashMap<string, Proposal>;
  readonly events: HashMap.HashMap<string, Recorded>;
  readonly sequences: HashMap.HashMap<number, string>;
};

export class GraphReplayConflict extends Schema.TaggedError<GraphReplayConflict>()(
  "GraphReplayConflict",
  {
    code: Schema.Literals([
      "envelope",
      "environment",
      "event_identity",
      "sequence",
      "revision",
      "edge",
      "pair",
      "proposal",
      "unsupported",
    ]),
    eventId: Schema.String,
  },
) {}

const conflict = (code: GraphReplayConflict["code"], eventId: string): never => {
  throw new GraphReplayConflict({ code, eventId });
};
const lookup = <K, V>(map: HashMap.HashMap<K, V>, key: K) =>
  Option.getOrUndefined(HashMap.get(map, key));
const pairKey = (edge: Adoption["event"]["edge"]) =>
  JSON.stringify([edge.dependent.taskId, edge.blocker.taskId]);

export const emptyTaskGraph = (environmentId: EnvironmentId): GraphState => ({
  environmentId,
  graphRevision: 0,
  lastSequence: undefined,
  edges: HashMap.empty(),
  activePairs: HashMap.empty(),
  proposals: HashMap.empty(),
  events: HashMap.empty(),
  sequences: HashMap.empty(),
});

export const reduceTaskGraph = (state: GraphState, recorded: Recorded): GraphState => {
  const { event, context } = recorded;
  if (
    event.id !== context.eventId ||
    event.commandId !== context.commandId ||
    event.occurredAt !== context.occurredAt ||
    !Number.isSafeInteger(context.sequence) ||
    context.sequence < 0
  )
    conflict("envelope", event.id);
  if (event.environmentId !== state.environmentId) conflict("environment", event.id);
  const existing = lookup(state.events, event.id);
  if (existing) {
    if (!NodeUtil.isDeepStrictEqual(existing, recorded)) conflict("event_identity", event.id);
    return state;
  }
  if (
    HashMap.has(state.sequences, context.sequence) ||
    (state.lastSequence !== undefined && context.sequence <= state.lastSequence)
  )
    conflict("sequence", event.id);
  let edges = state.edges;
  let activePairs = state.activePairs;
  let proposals = state.proposals;
  let graphRevision = state.graphRevision;
  switch (event.type) {
    case "task.graph-proposal-recorded": {
      if (
        event.proposal.dependent.environmentId !== state.environmentId ||
        event.proposal.proposedAt !== event.occurredAt ||
        event.proposal.observedGraphRevision > event.graphRevision
      )
        conflict("proposal", event.id);
      const prior = lookup(proposals, event.proposal.proposalId);
      if (prior && !NodeUtil.isDeepStrictEqual(prior, event.proposal))
        conflict("proposal", event.id);
      proposals = HashMap.set(proposals, event.proposal.proposalId, event.proposal);
      break;
    }
    case "task.graph-edge-adopted":
    case "task.graph-edge-removed": {
      if (
        event.previousGraphRevision !== graphRevision ||
        event.graphRevision !== graphRevision + 1 ||
        !Number.isSafeInteger(event.graphRevision)
      )
        conflict("revision", event.id);
      const edge = event.edge;
      if (
        edge.dependent.environmentId !== state.environmentId ||
        edge.blocker.environmentId !== state.environmentId ||
        edge.dependent.taskId === edge.blocker.taskId
      )
        conflict("edge", event.id);
      const provenance =
        event.type === "task.graph-edge-adopted" ? edge.provenance : event.provenance;
      if (
        provenance.eventId !== event.id ||
        provenance.commandId !== context.commandId ||
        provenance.occurredAt !== event.occurredAt
      )
        conflict("envelope", event.id);
      const key = pairKey(edge);
      const prior = lookup(edges, edge.edgeId);
      if (event.type === "task.graph-edge-adopted") {
        if (prior || edge.createdGraphRevision !== event.graphRevision) conflict("edge", event.id);
        if (HashMap.has(activePairs, key)) conflict("pair", event.id);
        edges = HashMap.set(edges, edge.edgeId, { adopted: { event, context } });
        activePairs = HashMap.set(activePairs, key, edge.edgeId);
      } else {
        if (!prior || prior.removed || !NodeUtil.isDeepStrictEqual(prior.adopted.event.edge, edge))
          return conflict("edge", event.id);
        if (lookup(activePairs, key) !== edge.edgeId) conflict("pair", event.id);
        edges = HashMap.set(edges, edge.edgeId, { ...prior, removed: { event, context } });
        activePairs = HashMap.remove(activePairs, key);
      }
      graphRevision = event.graphRevision;
      break;
    }
    default:
      return conflict("unsupported", context.eventId);
  }
  return {
    ...state,
    graphRevision,
    lastSequence: context.sequence,
    edges,
    activePairs,
    proposals,
    events: HashMap.set(state.events, event.id, recorded),
    sequences: HashMap.set(state.sequences, context.sequence, event.id),
  };
};
