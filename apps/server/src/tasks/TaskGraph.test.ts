import { assert, it } from "@effect/vitest";
import { WorkTaskPersistedGraphEvent, EnvironmentId } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as HashMap from "effect/HashMap";
import * as Option from "effect/Option";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../persistence/Migrations.ts";
import { emptyTaskGraph, reduceTaskGraph, GraphReplayConflict } from "./TaskGraph.ts";

const decode = Schema.decodeUnknownSync(WorkTaskPersistedGraphEvent);
const decodeEnvironment = Schema.decodeUnknownSync(EnvironmentId);
const env = decodeEnvironment("env");
const time = "2026-11-16T00:00:00.0001Z";
const provenance = (id: string) => ({
  eventId: id,
  commandId: "command",
  occurredAt: time,
  adoptedBy: { subject: "owner", sessionId: "session" },
});
const adopted = (
  revision: number,
  edgeId = `edge-${revision}`,
  dependent = "a",
  blocker = "b",
  sequence = revision * 10,
) => {
  const id = `adopt-${edgeId}`;
  return decode({
    event: {
      type: "task.graph-edge-adopted",
      id,
      environmentId: env,
      occurredAt: time,
      commandId: "command",
      previousGraphRevision: revision - 1,
      graphRevision: revision,
      edge: {
        edgeId,
        dependent: { environmentId: env, taskId: dependent },
        blocker: { environmentId: env, taskId: blocker },
        createdGraphRevision: revision,
        provenance: provenance(id),
      },
    },
    context: { eventId: id, sequence, occurredAt: time, commandId: "command" },
  });
};
const removed = (
  original: ReturnType<typeof adopted>,
  revision: number,
  sequence = revision * 10,
) => {
  if (original.event.type !== "task.graph-edge-adopted") throw new Error("adoption fixture");
  const id = `remove-${original.event.edge.edgeId}`;
  return decode({
    event: {
      ...original.event,
      type: "task.graph-edge-removed",
      id,
      previousGraphRevision: revision - 1,
      graphRevision: revision,
      provenance: provenance(id),
    },
    context: { ...original.context, eventId: id, sequence },
  });
};
const proposal = (sequence: number, graphRevision = 0, foreign = false) => {
  const id = `proposal-event-${sequence}`;
  return decode({
    event: {
      type: "task.graph-proposal-recorded",
      id,
      environmentId: env,
      occurredAt: time,
      commandId: "command",
      graphRevision,
      proposal: {
        proposalId: "proposal",
        dependent: { environmentId: env, taskId: "a" },
        blocker: { environmentId: foreign ? "foreign" : env, taskId: "b" },
        observedGraphRevision: graphRevision,
        proposedAt: time,
        proposedBy: {
          type: "agent",
          threadId: "thread",
          providerSessionId: "session",
          runId: "run",
        },
      },
    },
    context: { eventId: id, sequence, occurredAt: time, commandId: "command" },
  });
};
const changed = (record: WorkTaskPersistedGraphEvent, patch: object, context: object = {}) =>
  decode({ event: { ...record.event, ...patch }, context: { ...record.context, ...context } });
const get = <K, V>(map: HashMap.HashMap<K, V>, key: K) => Option.getOrThrow(HashMap.get(map, key));
const freeze = <T>(value: T): T => {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) freeze(child);
    Object.freeze(value);
  }
  return value;
};

it("replays immutable graph history, branches and rejects conflicting facts", () => {
  const first = freeze(adopted(1));
  const second = adopted(2, "second", "c", "b");
  const removal = removed(first, 3);
  const readopt = adopted(4, "fresh");
  const records = [first, second, removal, readopt, removed(second, 5), removed(readopt, 6)];
  const empty = emptyTaskGraph(env);
  const state = records.reduce(reduceTaskGraph, empty);
  assert.strictEqual(state.graphRevision, 6);
  assert.strictEqual(HashMap.size(state.activePairs), 0);
  assert.strictEqual(HashMap.size(state.edges), 3);
  for (const record of records) assert.strictEqual(reduceTaskGraph(state, record), state);
  assert.deepStrictEqual(get(state.edges, "edge-1").adopted, first);
  assert.deepStrictEqual(get(state.edges, "edge-1").removed, removal);
  const initial = reduceTaskGraph(empty, first);
  const left = reduceTaskGraph(initial, removed(first, 2));
  const right = reduceTaskGraph(initial, second);
  assert.strictEqual(HashMap.size(initial.activePairs), 1);
  assert.strictEqual(HashMap.size(left.activePairs), 0);
  assert.strictEqual(HashMap.size(right.activePairs), 2);
  assert.strictEqual(empty.graphRevision, 0);
  const reject = (record: WorkTaskPersistedGraphEvent, source = initial) =>
    assert.throws(() => reduceTaskGraph(source, record), GraphReplayConflict);
  reject(adopted(3, "skipped", "c", "b"));
  reject(adopted(2, "edge-1", "c", "b"));
  reject(adopted(2, "duplicate"));
  reject(adopted(2, "sequence", "c", "b", 10));
  reject(adopted(2, "older", "c", "b", 9));
  reject(removed(adopted(1, "unknown"), 2));
  if (first.event.type !== "task.graph-edge-adopted") throw new Error("adoption");
  reject(
    changed(first, { edge: { ...first.event.edge, blocker: { environmentId: env, taskId: "c" } } }),
  );
  reject(changed(first, {}, { sequence: 11 }));
  const otherCommand = { ...first.event.edge.provenance, commandId: "different" };
  reject(
    changed(
      first,
      { commandId: "different", edge: { ...first.event.edge, provenance: otherCommand } },
      { commandId: "different" },
    ),
  );
  const otherTime = "2026-11-16T00:00:00.0002Z";
  reject(
    changed(
      first,
      {
        occurredAt: otherTime,
        edge: {
          ...first.event.edge,
          provenance: { ...first.event.edge.provenance, occurredAt: otherTime },
        },
      },
      { occurredAt: otherTime },
    ),
  );
  if (removal.event.type !== "task.graph-edge-removed") throw new Error("removal");
  reject(
    changed(removed(first, 2), {
      edge: {
        ...first.event.edge,
        provenance: {
          ...first.event.edge.provenance,
          adoptedBy: { subject: "other", sessionId: "session" },
        },
      },
    }),
  );
  reject(removed(first, 3), left);
  reject(adopted(3, "edge-1"), left);
  assert.throws(
    () => reduceTaskGraph(emptyTaskGraph(decodeEnvironment("other")), first),
    GraphReplayConflict,
  );
  // Structural object ordering is not a distinct replay identity.
  assert.strictEqual(
    reduceTaskGraph(initial, { context: first.context, event: { ...first.event } }),
    initial,
  );
});

it("keeps proposals inert and binds their canonical identity without claiming adoption", () => {
  const observation = proposal(1, 20, true);
  const empty = emptyTaskGraph(env);
  const state = reduceTaskGraph(empty, observation);
  assert.strictEqual(state.graphRevision, 0);
  assert.strictEqual(HashMap.size(state.edges), 0);
  const first = adopted(1);
  const accepted = reduceTaskGraph(state, first);
  assert.strictEqual(accepted.graphRevision, 1);
  assert.strictEqual(HashMap.size(accepted.proposals), 1);
  const repeat = changed(
    observation,
    { id: "repeat", commandId: "another" },
    { eventId: "repeat", commandId: "another", sequence: 11 },
  );
  const repeated = reduceTaskGraph(accepted, repeat);
  assert.deepStrictEqual(
    get(repeated.proposals, "proposal"),
    observation.event.type === "task.graph-proposal-recorded"
      ? observation.event.proposal
      : undefined,
  );
  const behind = proposal(12);
  if (behind.event.type !== "task.graph-proposal-recorded") throw new Error("proposal");
  const local = changed(behind, {
    proposal: {
      ...behind.event.proposal,
      proposalId: "local",
      proposedBy: { type: "user", subject: "owner", sessionId: "session" },
    },
  });
  const localState = reduceTaskGraph(repeated, local);
  assert.strictEqual(localState.graphRevision, 1);
  assert.strictEqual(HashMap.size(localState.activePairs), 1);
  assert.throws(() => reduceTaskGraph(repeated, behind), GraphReplayConflict);
  if (observation.event.type !== "task.graph-proposal-recorded") throw new Error("proposal");
  const originalProposal = observation.event.proposal;
  const refreshed = "2026-11-16T00:00:01Z";
  assert.throws(
    () =>
      reduceTaskGraph(
        repeated,
        changed(
          observation,
          {
            id: "refresh",
            occurredAt: refreshed,
            proposal: { ...originalProposal, proposedAt: refreshed },
          },
          { eventId: "refresh", sequence: 12, occurredAt: refreshed },
        ),
      ),
    GraphReplayConflict,
  );
  assert.strictEqual(reduceTaskGraph(localState, observation), localState);
});

it("preserves indexed histories through long replay and independent branches", () => {
  let state = emptyTaskGraph(env);
  const first = adopted(1, "first", "a\u0000b", "c");
  state = reduceTaskGraph(state, first);
  const branch = state;
  for (let n = 2; n <= 1000; n++)
    state = reduceTaskGraph(state, adopted(n, `e${n}`, `task${n}`, "blocker"));
  assert.strictEqual(HashMap.size(state.edges), 1000);
  assert.strictEqual(reduceTaskGraph(state, first), state);
  assert.strictEqual(HashMap.size(branch.edges), 1);
  assert.strictEqual(reduceTaskGraph(branch, removed(first, 2)).graphRevision, 2);
  assert.strictEqual(HashMap.size(state.activePairs), 1000);
});

const db = NodeSqliteClient.layer({ filename: ":memory:" });
const persist = Effect.fn("TaskGraphTest.persist")(function* (record: WorkTaskPersistedGraphEvent) {
  const sql = yield* SqlClient.SqlClient;
  const event = record.event;
  if (event.type === "task.graph-proposal-recorded") return;
  const encoded = JSON.stringify(event.edge);
  if (event.type === "task.graph-edge-adopted") {
    yield* sql`WITH input(value) AS (VALUES (${encoded})) INSERT INTO work_task_graph_edges
      (environment_id,edge_id,dependent_task_id,blocker_task_id,created_graph_revision,adopted_event_id,adopted_sequence,edge_json)
      SELECT json_extract(value,'$.dependent.environmentId'),json_extract(value,'$.edgeId'),json_extract(value,'$.dependent.taskId'),json_extract(value,'$.blocker.taskId'),json_extract(value,'$.createdGraphRevision'),json_extract(value,'$.provenance.eventId'),${record.context.sequence},value FROM input`;
  } else {
    yield* sql`UPDATE work_task_graph_edges SET removed_graph_revision=${event.graphRevision},removed_event_id=json_extract(${JSON.stringify(event.id)},'$'),removed_sequence=${record.context.sequence},removal_provenance_json=${JSON.stringify(event.provenance)}
      WHERE environment_id=json_extract(${JSON.stringify(env)},'$') AND edge_id=json_extract(${JSON.stringify(event.edge.edgeId)},'$')`;
  }
});

it.effect(
  "matches actual SQL graph history after every mutation and retries from a pure snapshot",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const unusual = "a\u0000\ud800😀";
      for (const id of [unusual, "b", "c"])
        yield* sql`INSERT INTO work_task_projections VALUES (json_extract(${JSON.stringify(id)},'$'),NULL,1,1,1,1,'{}','1','1')`;
      const first = adopted(1, "edge\u0000\ud800", unusual);
      const second = adopted(2, "second", "c", "b");
      const third = adopted(4, "fresh", unusual);
      const log = [
        proposal(1, 10, true),
        first,
        second,
        removed(first, 3),
        third,
        removed(second, 5),
        removed(third, 6),
      ];
      let state = emptyTaskGraph(env);
      const compare = Effect.fn("TaskGraphTest.compare")(function* (
        current: ReturnType<typeof emptyTaskGraph>,
      ) {
        const [head] = yield* sql<{
          revision: number;
        }>`SELECT max(coalesce((SELECT max(created_graph_revision) FROM work_task_graph_edges WHERE environment_id='env'),0),coalesce((SELECT max(removed_graph_revision) FROM work_task_graph_edges WHERE environment_id='env'),0)) AS revision`;
        assert.strictEqual(head!.revision, current.graphRevision);
        const rows = yield* sql<{
          edge_json: string;
          adopted_sequence: number;
          removed_graph_revision: number | null;
          removed_sequence: number | null;
          removal_provenance_json: string | null;
        }>`SELECT edge_json,adopted_sequence,removed_graph_revision,removed_sequence,removal_provenance_json FROM work_task_graph_edges`;
        assert.strictEqual(rows.length, HashMap.size(current.edges));
        for (const row of rows) {
          const edge = JSON.parse(row.edge_json);
          const history = get(current.edges, edge.edgeId);
          assert.deepStrictEqual(
            history.adopted.event.type === "task.graph-edge-adopted"
              ? history.adopted.event.edge
              : undefined,
            edge,
          );
          assert.strictEqual(history.adopted.context.sequence, row.adopted_sequence);
          assert.strictEqual(
            history.removed?.event.graphRevision ?? null,
            row.removed_graph_revision,
          );
          assert.strictEqual(history.removed?.context.sequence ?? null, row.removed_sequence);
          assert.deepStrictEqual(
            history.removed?.event.type === "task.graph-edge-removed"
              ? history.removed.event.provenance
              : null,
            row.removal_provenance_json === null ? null : JSON.parse(row.removal_provenance_json),
          );
        }
      });
      yield* compare(state);
      for (const record of log) {
        const next = reduceTaskGraph(state, record);
        yield* persist(record);
        state = next;
        yield* compare(state);
      }
      const retry = adopted(7, "retry", "c", unusual);
      const snapshot = state;
      const failure = yield* Effect.exit(
        sql.withTransaction(
          Effect.gen(function* () {
            yield* persist(retry);
            return yield* Effect.fail("injected");
          }),
        ),
      );
      assert.isTrue(Exit.isFailure(failure));
      yield* compare(snapshot);
      yield* persist(retry);
      state = reduceTaskGraph(snapshot, retry);
      yield* compare(state);
      assert.strictEqual(snapshot.graphRevision, 6);
      assert.deepStrictEqual(yield* sql`PRAGMA foreign_key_check`, []);
    }).pipe(Effect.provide(db)),
);
